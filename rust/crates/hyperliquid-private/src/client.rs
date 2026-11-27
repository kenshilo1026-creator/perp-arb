use crate::{Error, Result};
use perp_arb_private_events::{
    account, actor::Coordinator, hyperliquid, OrderKey, OrderSnapshot, Scope,
};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::Mutex,
    time::{sleep_until, timeout, Instant},
};

#[derive(Clone)]
pub struct Client(Arc<Inner>);
struct Inner {
    user: String,
    http: reqwest::Client,
    info: String,
    ws: String,
    network: perp_arb_private_events::orders::Network,
    budget: Mutex<Budget>,
    gate: Mutex<()>,
}

struct Budget {
    sent: VecDeque<Instant>,
    next: Instant,
    blocked: Instant,
    forever: bool,
}
/// Explicitly authenticated trading handle. Does not retry ambiguous submissions.
pub struct TradingClient {
    client: Client,
    credentials: crate::signing::Credentials,
}
impl TradingClient {
    pub async fn configure_isolated(
        &self,
        order: &perp_arb_private_events::orders::Order,
        leverage: u32,
    ) -> Result<()> {
        order.validate()?;
        if order.venue != "hyperliquid"
            || order.request()?.scope != self.client.scope()
            || order.network != self.client.network()
            || !(1..=125).contains(&leverage)
        {
            return Err(Error::Configuration);
        }
        timeout(Duration::from_secs(30), async {
            let _gate = self.client.0.gate.lock().await;
            self.client.reserve(1).await;
            let body = self.credentials.isolated_leverage(
                order.asset.ok_or(Error::Configuration)?,
                leverage,
                order.network,
            )?;
            let url = self
                .client
                .0
                .info
                .strip_suffix("/info")
                .ok_or(Error::Configuration)?
                .to_owned()
                + "/exchange";
            let raw = self.client.response_at(&url, body, 65536).await?;
            let v: serde_json::Value = serde_json::from_str(&raw).map_err(|_| Error::Protocol)?;
            if v["status"] != "ok" || v["response"]["type"] != "default" {
                return Err(Error::Protocol);
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    async fn exchange(
        &self,
        order: &perp_arb_private_events::orders::Order,
        cancel: bool,
    ) -> Result<serde_json::Value> {
        order.validate()?;
        if order.venue != "hyperliquid"
            || order.request()?.scope != self.client.scope()
            || order.network != self.client.network()
        {
            return Err(Error::Configuration);
        }
        timeout(Duration::from_secs(30), async {
            let _gate = self.client.0.gate.lock().await;
            self.client.reserve(1).await;
            let body = if cancel {
                self.credentials.cancel(order)?
            } else {
                self.credentials.order(order)?
            };
            let url = self
                .client
                .0
                .info
                .strip_suffix("/info")
                .ok_or(Error::Configuration)?
                .to_owned()
                + "/exchange";
            let raw = self.client.response_at(&url, body, 65_536).await?;
            let v: serde_json::Value = serde_json::from_str(&raw).map_err(|_| Error::Protocol)?;
            if v["status"] != "ok"
                || v["response"]["type"] != if cancel { "cancel" } else { "order" }
            {
                return Err(Error::Protocol);
            }
            let statuses = v["response"]["data"]["statuses"]
                .as_array()
                .filter(|a| a.len() == 1)
                .ok_or(Error::Protocol)?;
            Ok(statuses[0].clone())
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub async fn submit_limit(
        &self,
        order: &perp_arb_private_events::orders::Order,
    ) -> Result<perp_arb_private_events::orders::Accepted> {
        let s = self.exchange(order, false).await?;
        // Do not treat a "filled" ACK as reconciled; the journal still queries cumulative status.
        let id = s
            .get("resting")
            .or_else(|| s.get("filled"))
            .and_then(|s| s["oid"].as_u64())
            .ok_or(Error::Protocol)?;
        let cumulative = s
            .get("filled")
            .map(|filled| {
                let q = filled["totalSz"].as_str().ok_or(Error::Protocol)?;
                let amount = perp_arb_core::decimal(q).map_err(|_| Error::Protocol)?;
                if amount <= perp_arb_core::Decimal::ZERO || amount > order.quantity()? {
                    return Err(Error::Protocol);
                }
                Ok(amount.to_string())
            })
            .transpose()?;
        Ok(perp_arb_private_events::orders::Accepted {
            order_id: id.to_string(),
            cumulative,
        })
    }
    pub async fn cancel_limit(&self, order: &perp_arb_private_events::orders::Order) -> Result<()> {
        if self.exchange(order, true).await? != "success" {
            return Err(Error::Protocol);
        }
        Ok(())
    }
}
impl Client {
    pub fn testnet(user: &str) -> Result<Self> {
        Self::build(
            user,
            "https://api.hyperliquid-testnet.xyz/info".into(),
            "wss://api.hyperliquid-testnet.xyz/ws".into(),
        )
    }
    pub fn network(&self) -> perp_arb_private_events::orders::Network {
        self.0.network
    }
    pub fn new(user: &str) -> Result<Self> {
        Self::build(
            user,
            "https://api.hyperliquid.xyz/info".into(),
            "wss://api.hyperliquid.xyz/ws".into(),
        )
    }
    fn build(user: &str, info: String, ws: String) -> Result<Self> {
        let user = hyperliquid::address(user).map_err(|_| Error::Configuration)?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::Configuration)?;
        let now = Instant::now();
        let network = if info == "https://api.hyperliquid-testnet.xyz/info" {
            perp_arb_private_events::orders::Network::Testnet
        } else {
            perp_arb_private_events::orders::Network::Mainnet
        };
        Ok(Self(Arc::new(Inner {
            user,
            http,
            info,
            ws,
            network,
            budget: Mutex::new(Budget {
                sent: VecDeque::new(),
                next: now,
                blocked: now,
                forever: false,
            }),
            gate: Mutex::new(()),
        })))
    }
    #[cfg(test)]
    pub(crate) fn local(user: &str, info: String, ws: String) -> Result<Self> {
        Self::build(user, info, ws)
    }
    pub fn scope(&self) -> Scope {
        Scope {
            venue: "hyperliquid".into(),
            account: self.0.user.clone(),
        }
    }
    pub(crate) fn user(&self) -> &str {
        &self.0.user
    }
    pub(crate) fn ws(&self) -> &str {
        &self.0.ws
    }

    async fn reserve(&self, weight: usize) {
        loop {
            let mut b = self.0.budget.lock().await;
            let now = Instant::now();
            while b
                .sent
                .front()
                .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
            {
                b.sent.pop_front();
            }
            if b.forever {
                drop(b);
                std::future::pending::<()>().await;
                unreachable!();
            }
            let mut due = b.next.max(b.blocked);
            // All supported endpoints share <=240 weighted units/minute.
            if b.sent.len() + weight > 240 {
                due = due.max(b.sent[b.sent.len() + weight - 241] + Duration::from_secs(60));
            }
            if due <= now {
                b.sent.extend(std::iter::repeat_n(now, weight));
                b.next = now + Duration::from_millis(100);
                return;
            }
            drop(b);
            sleep_until(due).await;
        }
    }
    async fn response(&self, payload: serde_json::Value, max_bytes: usize) -> Result<String> {
        self.response_at(
            &self.0.info,
            serde_json::to_string(&payload).map_err(|_| Error::Protocol)?,
            max_bytes,
        )
        .await
    }
    async fn response_at(&self, url: &str, body: String, max_bytes: usize) -> Result<String> {
        let mut response = self
            .0
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    Error::Timeout
                } else {
                    Error::Network
                }
            })?;
        let status = response.status().as_u16();
        if status == 429
            || status == 418
            || (status == 503 && response.headers().contains_key("retry-after"))
        {
            let duration = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| {
                    v.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
                        httpdate::parse_http_date(v)
                            .ok()
                            .map(|v| v.duration_since(SystemTime::now()).unwrap_or_default())
                    })
                })
                .unwrap_or(Duration::from_secs(if status == 418 { 180 } else { 60 }))
                .max(Duration::from_secs(1));
            let mut b = self.0.budget.lock().await;
            if let Some(until) = Instant::now().checked_add(duration) {
                b.blocked = b.blocked.max(until);
            } else {
                b.forever = true;
            }
        }
        if !(200..300).contains(&status) {
            return Err(Error::Http(status));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Network)? {
            if bytes.len() + chunk.len() > max_bytes {
                return Err(Error::Capacity);
            }
            bytes.extend_from_slice(&chunk);
        }
        String::from_utf8(bytes).map_err(|_| Error::Protocol)
    }
    /// Binds a signing key to this exact account before enabling /exchange.
    /// Vault/subaccount actions require separate explicit support and are rejected.
    pub async fn trading(&self, credentials: crate::signing::Credentials) -> Result<TradingClient> {
        timeout(Duration::from_secs(30), async {
            let _gate = self.0.gate.lock().await;
            self.reserve(60).await;
            let raw = self
                .response(
                    json!({"type":"userRole","user":credentials.address()}),
                    65_536,
                )
                .await?;
            let v: serde_json::Value = serde_json::from_str(&raw).map_err(|_| Error::Protocol)?;
            let matched = if credentials.address() == self.user() {
                v["role"] == "user"
            } else {
                v["role"] == "agent"
                    && v["data"]["user"]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(self.user()))
            };
            if !matched {
                return Err(Error::Credentials);
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(TradingClient {
            client: self.clone(),
            credentials,
        })
    }
    pub async fn metadata(&self) -> Result<serde_json::Value> {
        timeout(Duration::from_secs(30), async {
            let _gate = self.0.gate.lock().await;
            self.reserve(20).await;
            serde_json::from_str(
                &self
                    .response(json!({"type":"meta"}), 2 * 1024 * 1024)
                    .await?,
            )
            .map_err(|_| Error::Protocol)
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    /// `oid` accepts an exchange u64 or the persisted 128-bit hexadecimal cloid.
    pub async fn lookup_submission(
        &self,
        request: &perp_arb_private_events::lookup::Request,
    ) -> Result<perp_arb_private_events::lookup::Outcome> {
        use perp_arb_private_events::lookup::{self, Selector};
        if request.scope != self.scope() {
            return Err(Error::Configuration);
        }
        request.validate()?;
        let oid = match &request.selector {
            Selector::OrderId(id) => json!(id.parse::<u64>().map_err(|_| Error::Configuration)?),
            Selector::ClientId(id) => json!(id),
        };
        let raw = timeout(Duration::from_secs(30), async {
            let _gate = self.0.gate.lock().await;
            self.reserve(2).await;
            self.response(
                json!({"type":"orderStatus","user":self.0.user,"oid":oid}),
                65_536,
            )
            .await
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(lookup::hyperliquid_response(request, &raw)?)
    }
    async fn inventory_sample(
        &self,
        dexes: &[String],
        spot: bool,
    ) -> Result<(account::Inventory, u64)> {
        let mut perps = Vec::new();
        let mut watermark = u64::MAX;
        for dex in dexes {
            self.reserve(2).await;
            let state: serde_json::Value = serde_json::from_str(
                &self
                    .response(
                        json!({"type":"clearinghouseState","user":self.0.user,"dex":dex}),
                        2 * 1024 * 1024,
                    )
                    .await?,
            )
            .map_err(|_| Error::Protocol)?;
            watermark = watermark.min(
                state
                    .get("time")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(Error::Protocol)?,
            );
            self.reserve(20).await;
            let orders = serde_json::from_str(
                &self
                    .response(
                        json!({"type":"openOrders","user":self.0.user,"dex":dex}),
                        2 * 1024 * 1024,
                    )
                    .await?,
            )
            .map_err(|_| Error::Protocol)?;
            perps.push((dex.clone(), state, orders));
        }
        let balances = if spot {
            self.reserve(2).await;
            Some(
                serde_json::from_str(
                    &self
                        .response(
                            json!({"type":"spotClearinghouseState","user":self.0.user}),
                            2 * 1024 * 1024,
                        )
                        .await?,
                )
                .map_err(|_| Error::Protocol)?,
            )
        } else {
            None
        };
        Ok((account::hyperliquid(&perps, balances.as_ref())?, watermark))
    }
    /// Two equal inventories plus a WS revision fence. Not an atomic exchange snapshot.
    pub async fn check_position(&self, c: &Coordinator, symbol: &str) -> Result<account::Report> {
        if c.scope() != &self.scope() {
            return Err(Error::Configuration);
        }
        let (ticket, snapshot) = timeout(Duration::from_secs(30), async {
            let _gate = self.0.gate.lock().await;
            let ticket = c.begin_account_query().await?;
            let started_at = std::time::Instant::now();
            self.reserve(2).await;
            let state: serde_json::Value = serde_json::from_str(
                &self
                    .response(
                        json!({"type":"clearinghouseState","user":self.0.user,"dex":""}),
                        2 * 1024 * 1024,
                    )
                    .await?,
            )
            .map_err(|_| Error::Protocol)?;
            let time = state["time"].as_u64().ok_or(Error::Protocol)?;
            let inventory = account::hyperliquid_position(&state, symbol)?;
            Ok::<_, Error>((
                ticket,
                account::Snapshot {
                    scope: self.scope(),
                    inventory,
                    started_at,
                    completed_at: std::time::Instant::now(),
                    exchange_time_ms: Some(time),
                },
            ))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(c.apply_position_snapshot(ticket, snapshot, symbol).await?)
    }
    /// Expected inventory must come from an independent ledger, not this response.
    pub async fn audit_account(
        &self,
        c: &Coordinator,
        dexes: &[String],
        spot: bool,
        expected: Option<account::Inventory>,
    ) -> Result<account::Report> {
        if c.scope() != &self.scope()
            || dexes.is_empty()
            || dexes.len() > 4
            || dexes
                .iter()
                .any(|d| d.len() > 64 || !d.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'))
        {
            return Err(Error::Configuration);
        }
        let (ticket, snapshot) = timeout(Duration::from_secs(30), async {
            let _gate = self.0.gate.lock().await;
            let ticket = c.begin_account_query().await?;
            let started_at = std::time::Instant::now();
            let (first, first_time) = self.inventory_sample(dexes, spot).await?;
            let (second, second_time) = self.inventory_sample(dexes, spot).await?;
            if first != second || second_time < first_time {
                return Err(Error::Protocol);
            }
            Ok::<_, Error>((
                ticket,
                account::Snapshot {
                    scope: self.scope(),
                    inventory: second,
                    started_at,
                    completed_at: std::time::Instant::now(),
                    exchange_time_ms: Some(first_time),
                },
            ))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(c.apply_account_snapshot(ticket, snapshot, expected).await?)
    }
    pub async fn reconcile(&self, c: &Coordinator, key: OrderKey) -> Result<OrderSnapshot> {
        if c.scope() != &self.scope() {
            return Err(Error::Configuration);
        }
        let oid = key
            .order_id
            .parse::<u64>()
            .map_err(|_| Error::Configuration)?;
        let response = timeout(Duration::from_secs(30), async {
            let _gate = self.0.gate.lock().await;
            self.reserve(2).await;
            let ticket = c.begin_hyperliquid_query(key.clone()).await?;
            Ok::<_, Error>((
                ticket,
                self.response(
                    json!({"type":"orderStatus","user":self.0.user,"oid":oid}),
                    65_536,
                )
                .await?,
            ))
        })
        .await
        .map_err(|_| Error::Timeout)
        .and_then(|v| v);
        let result = match response {
            Ok((ticket, raw)) => c
                .ingest_hyperliquid_rest(ticket, &raw)
                .await
                .map_err(Error::from),
            Err(e) => Err(e),
        };
        if result.is_err() {
            // Unknown fill-only orders remain in the coordinator's pending set.
            match c.require_reconciliation(key).await {
                Ok(()) | Err(perp_arb_private_events::Error::UnknownOrder) => (),
                Err(e) => return Err(e.into()),
            }
        }
        result
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn account_and_order_queries_consume_endpoint_weights_in_one_window() {
        let c = Client::new("0x0000000000000000000000000000000000000001").unwrap();
        for _ in 0..12 {
            c.reserve(20).await;
        }
        assert_eq!(c.0.budget.lock().await.sent.len(), 240);
        let started = Instant::now();
        c.reserve(2).await;
        assert!(started.elapsed() >= Duration::from_secs(58));
        assert_eq!(c.0.budget.lock().await.sent.len(), 222);
    }
}
