use crate::{auth::Credentials, Error, Result};
use futures_util::StreamExt;
use perp_arb_private_events::{account, actor::Coordinator, OrderKey, OrderSnapshot, Scope};
use reqwest::{Client, Method};
use serde_json::Value;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, SystemTime},
};
use tokio::{
    sync::Mutex,
    time::{sleep_until, timeout, Instant},
};

#[derive(Clone, Debug)]
pub struct HttpPolicy {
    /// Weighted request units per minute (legacy field name).
    pub requests_per_minute: usize,
    pub minimum_spacing: Duration,
    pub request_timeout: Duration,
    pub total_timeout: Duration,
    pub max_body_bytes: usize,
}
impl Default for HttpPolicy {
    fn default() -> Self {
        Self {
            requests_per_minute: 120,
            minimum_spacing: Duration::from_millis(100),
            request_timeout: Duration::from_secs(10),
            total_timeout: Duration::from_secs(30),
            max_body_bytes: 65_536,
        }
    }
}

struct Budget {
    sent: VecDeque<Instant>,
    next: Instant,
    blocked: Instant,
    blocked_forever: bool,
}

pub(crate) struct Inner {
    http: Client,
    credentials: Credentials,
    policy: HttpPolicy,
    budget: Mutex<Budget>,
    gate: Mutex<()>,
    pub(crate) active: AtomicBool,
    base: String,
    pub(crate) ws_base: String,
}

#[derive(Clone)]
pub struct PrivateHttp(pub(crate) Arc<Inner>);

/// Deliberately no Debug/Display: a listenKey is a bearer credential.
pub(crate) struct ListenKey(String);
impl ListenKey {
    pub(crate) fn socket_url(&self, base: &str) -> String {
        format!("{base}/ws/{}", self.0)
    }
}

impl PrivateHttp {
    /// Symbol-scoped setup, only called before opening a new paired maker.
    pub async fn configure_isolated(
        &self,
        order: &perp_arb_private_events::orders::Order,
        leverage: u32,
    ) -> Result<()> {
        order.validate()?;
        if order.request()?.scope != self.scope()
            || order.venue != "aster"
            || !(1..=125).contains(&leverage)
        {
            return Err(Error::Configuration);
        }
        timeout(self.0.policy.total_timeout, async {
            let _gate = self.0.gate.lock().await;
            self.reserve(1).await?;
            match self
                .send(
                    Method::POST,
                    "/fapi/v3/marginType",
                    vec![
                        ("symbol".into(), order.symbol.clone()),
                        ("marginType".into(), "ISOLATED".into()),
                    ],
                    65536,
                )
                .await
            {
                Ok(raw) => {
                    let v: Value = serde_json::from_str(&raw).map_err(|_| Error::InvalidPayload)?;
                    if v["code"] != 200 {
                        return Err(Error::InvalidPayload);
                    }
                }
                Err(Error::Exchange(-4046)) => (),
                Err(e) => return Err(e),
            }
            self.reserve(1).await?;
            let raw = self
                .send(
                    Method::POST,
                    "/fapi/v3/leverage",
                    vec![
                        ("symbol".into(), order.symbol.clone()),
                        ("leverage".into(), leverage.to_string()),
                    ],
                    65536,
                )
                .await?;
            let v: Value = serde_json::from_str(&raw).map_err(|_| Error::InvalidPayload)?;
            if v["symbol"] != order.symbol || v["leverage"].as_u64() != Some(leverage as u64) {
                return Err(Error::InvalidPayload);
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    /// Explicit trade entry; caller must persist an unknown submission before awaiting.
    /// Exactly one request; errors never cause an automatic order retry.
    pub async fn submit_limit(
        &self,
        order: &perp_arb_private_events::orders::Order,
    ) -> Result<perp_arb_private_events::orders::Accepted> {
        if order.tif == perp_arb_private_events::orders::TimeInForce::Market {
            return Err(Error::Configuration);
        }
        self.submit_order(order).await
    }
    pub async fn submit_market(
        &self,
        order: &perp_arb_private_events::orders::Order,
    ) -> Result<perp_arb_private_events::orders::Accepted> {
        if order.tif != perp_arb_private_events::orders::TimeInForce::Market {
            return Err(Error::Configuration);
        }
        self.submit_order(order).await
    }
    async fn submit_order(
        &self,
        order: &perp_arb_private_events::orders::Order,
    ) -> Result<perp_arb_private_events::orders::Accepted> {
        use perp_arb_private_events::orders::{Accepted, TimeInForce};
        order.validate()?;
        if order.venue != "aster" || order.request()?.scope != self.scope() {
            return Err(Error::Configuration);
        }
        let raw = timeout(self.0.policy.total_timeout, async {
            let _gate = self.0.gate.lock().await;
            self.reserve(1).await?;
            let mut params = vec![
                ("symbol".into(), order.symbol.clone()),
                ("side".into(), order.side.clone()),
                (
                    "type".into(),
                    if order.tif == TimeInForce::Market {
                        "MARKET"
                    } else {
                        "LIMIT"
                    }
                    .into(),
                ),
                ("positionSide".into(), "BOTH".into()),
                ("quantity".into(), order.quantity()?.normalize().to_string()),
                ("reduceOnly".into(), order.reduce_only.to_string()),
                ("newClientOrderId".into(), order.client_id.clone()),
                ("newOrderRespType".into(), "ACK".into()),
            ];
            if order.tif != TimeInForce::Market {
                params.push(("price".into(), order.price()?.normalize().to_string()));
                params.push((
                    "timeInForce".into(),
                    if order.tif == TimeInForce::PostOnly {
                        "GTX"
                    } else {
                        "IOC"
                    }
                    .into(),
                ));
            }
            self.send(Method::POST, "/fapi/v3/order", params, 65_536)
                .await
        })
        .await
        .map_err(|_| Error::Timeout)??;
        let v: Value = serde_json::from_str(&raw).map_err(|_| Error::InvalidPayload)?;
        if v["symbol"] != order.symbol || v["clientOrderId"] != order.client_id {
            return Err(Error::InvalidPayload);
        }
        let id = v["orderId"]
            .as_u64()
            .map(|n| n.to_string())
            .or_else(|| {
                v["orderId"]
                    .as_str()
                    .filter(|s| s.parse::<u64>().is_ok())
                    .map(str::to_owned)
            })
            .ok_or(Error::InvalidPayload)?;
        let cumulative = v
            .get("executedQty")
            .map(|value| {
                let q = value.as_str().ok_or(Error::InvalidPayload)?;
                let amount = perp_arb_core::decimal(q).map_err(|_| Error::InvalidPayload)?;
                if amount < perp_arb_core::Decimal::ZERO || amount > order.quantity()? {
                    return Err(Error::InvalidPayload);
                }
                Ok(amount.to_string())
            })
            .transpose()?;
        Ok(Accepted {
            order_id: id,
            cumulative,
        })
    }
    /// Cancel only the persisted client ID, never an account-wide cancel.
    /// Cancel ACK is not proof of terminal status; query the order afterwards.
    pub async fn cancel_limit(&self, order: &perp_arb_private_events::orders::Order) -> Result<()> {
        order.validate()?;
        if order.venue != "aster" || order.request()?.scope != self.scope() {
            return Err(Error::Configuration);
        }
        timeout(self.0.policy.total_timeout, async {
            let _gate = self.0.gate.lock().await;
            self.reserve(1).await?;
            let raw = self
                .send(
                    Method::DELETE,
                    "/fapi/v3/order",
                    vec![
                        ("symbol".into(), order.symbol.clone()),
                        ("origClientOrderId".into(), order.client_id.clone()),
                    ],
                    65_536,
                )
                .await?;
            let v: Value = serde_json::from_str(&raw).map_err(|_| Error::InvalidPayload)?;
            if v["symbol"] != order.symbol || v["clientOrderId"] != order.client_id {
                return Err(Error::InvalidPayload);
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub fn new(credentials: Credentials, policy: HttpPolicy) -> Result<Self> {
        Self::build(
            credentials,
            policy,
            "https://fapi.asterdex.com".into(),
            "wss://fstream.asterdex.com".into(),
        )
    }
    fn build(
        credentials: Credentials,
        policy: HttpPolicy,
        base: String,
        ws_base: String,
    ) -> Result<Self> {
        if policy.requests_per_minute == 0
            || policy.requests_per_minute > 1200
            || policy.minimum_spacing.is_zero()
            || policy.minimum_spacing > Duration::from_secs(60)
            || policy.request_timeout.is_zero()
            || policy.total_timeout < policy.request_timeout
            || policy.total_timeout > Duration::from_secs(120)
            || policy.max_body_bytes == 0
            || policy.max_body_bytes > 1_048_576
        {
            return Err(Error::Configuration);
        }
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(policy.request_timeout)
            .pool_idle_timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| Error::Configuration)?;
        let now = Instant::now();
        Ok(Self(Arc::new(Inner {
            http,
            credentials,
            policy,
            base,
            ws_base,
            budget: Mutex::new(Budget {
                sent: VecDeque::new(),
                next: now,
                blocked: now,
                blocked_forever: false,
            }),
            gate: Mutex::new(()),
            active: AtomicBool::new(false),
        })))
    }
    #[cfg(test)]
    pub(crate) fn local(
        credentials: Credentials,
        policy: HttpPolicy,
        base: String,
        ws_base: String,
    ) -> Result<Self> {
        Self::build(credentials, policy, base, ws_base)
    }
    pub fn scope(&self) -> Scope {
        Scope {
            venue: "aster".into(),
            account: self.0.credentials.user().into(),
        }
    }

    async fn reserve(&self, weight: usize) -> Result<()> {
        if weight > self.0.policy.requests_per_minute {
            return Err(Error::Configuration);
        }
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
            let mut ready = b.next.max(b.blocked);
            if b.blocked_forever {
                drop(b);
                std::future::pending::<()>().await;
                unreachable!();
            }
            if b.sent.len() + weight > self.0.policy.requests_per_minute {
                ready = ready.max(
                    b.sent[b.sent.len() + weight - self.0.policy.requests_per_minute - 1]
                        + Duration::from_secs(60),
                );
            }
            if ready <= now {
                b.sent.extend(std::iter::repeat_n(now, weight));
                b.next = now + self.0.policy.minimum_spacing;
                return Ok(());
            }
            drop(b);
            sleep_until(ready).await;
        }
    }

    async fn cooldown(&self, status: u16, header: Option<&str>) {
        let fallback = if status == 418 {
            Duration::from_secs(180)
        } else {
            Duration::from_secs(60)
        };
        let duration = header
            .and_then(|s| {
                s.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
                    httpdate::parse_http_date(s)
                        .ok()
                        .map(|t| t.duration_since(SystemTime::now()).unwrap_or_default())
                })
            })
            .unwrap_or(fallback)
            .max(Duration::from_secs(1));
        let mut budget = self.0.budget.lock().await;
        if let Some(until) = Instant::now().checked_add(duration) {
            budget.blocked = budget.blocked.max(until);
        } else {
            budget.blocked_forever = true;
        }
    }

    /// Caller holds the gate and budget reservation. Parameters are signed only
    /// after quota waits, so queued requests don't carry an expired nonce.
    async fn send(
        &self,
        method: Method,
        path: &'static str,
        params: Vec<(String, String)>,
        max_body_bytes: usize,
    ) -> Result<String> {
        let order_lookup = method == Method::GET && path == "/fapi/v3/order";
        let margin_setup = method == Method::POST && path == "/fapi/v3/marginType";
        let encoded = self.0.credentials.sign(params)?;
        let request = if method == Method::GET || method == Method::DELETE {
            self.0
                .http
                .request(method, format!("{}{path}?{encoded}", self.0.base))
        } else {
            self.0
                .http
                .request(method, format!("{}{path}", self.0.base))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(encoded)
        };
        let response = request.send().await.map_err(|e| {
            if e.is_timeout() {
                Error::Timeout
            } else {
                Error::Network
            }
        })?;
        let status = response.status().as_u16();
        if matches!(status, 429 | 418)
            || (status == 503 && response.headers().contains_key("Retry-After"))
        {
            self.cooldown(
                status,
                response
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok()),
            )
            .await;
        }
        if status != 200 && !(status == 400 && (order_lookup || margin_setup)) {
            return Err(Error::Http(status));
        }
        if response
            .content_length()
            .is_some_and(|size| size > max_body_bytes as u64)
        {
            return Err(Error::BodyLimit);
        }
        let mut body = Vec::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| Error::Network)?;
            if body.len().saturating_add(chunk.len()) > max_body_bytes {
                return Err(Error::BodyLimit);
            }
            body.extend_from_slice(&chunk);
        }
        let raw = String::from_utf8(body).map_err(|_| Error::InvalidPayload)?;
        let value: Value = serde_json::from_str(&raw).map_err(|_| {
            if status == 400 {
                Error::Http(400)
            } else {
                Error::InvalidPayload
            }
        })?;
        // The documented missing-order code may arrive with HTTP 400. No other
        // non-success body is promoted to order data or a terminal rejection.
        if status == 400 {
            return Err(
                if value.get("code").and_then(Value::as_i64) == Some(-2013) {
                    Error::Exchange(-2013)
                } else if path == "/fapi/v3/marginType"
                    && value.get("code").and_then(Value::as_i64) == Some(-4046)
                {
                    Error::Exchange(-4046)
                } else {
                    Error::Http(400)
                },
            );
        }
        if let Some(code) = value.get("code").and_then(Value::as_i64) {
            if code != 0 && code != 200 {
                return Err(Error::Exchange(code));
            }
        }
        Ok(raw)
    }

    async fn request(&self, method: Method, path: &'static str) -> Result<String> {
        timeout(self.0.policy.total_timeout, async {
            let _guard = self.0.gate.lock().await;
            self.reserve(1).await?;
            self.send(method, path, vec![], self.0.policy.max_body_bytes)
                .await
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub(crate) async fn listen_key(&self) -> Result<ListenKey> {
        let raw = self.request(Method::POST, "/fapi/v3/listenKey").await?;
        let value: Value = serde_json::from_str(&raw).map_err(|_| Error::InvalidPayload)?;
        let key = value
            .get("listenKey")
            .and_then(Value::as_str)
            .ok_or(Error::InvalidPayload)?;
        if key.is_empty()
            || key.len() > 512
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(Error::InvalidPayload);
        }
        Ok(ListenKey(key.into()))
    }
    pub(crate) async fn renew(&self) -> Result<()> {
        let raw = self.request(Method::PUT, "/fapi/v3/listenKey").await?;
        let value: Value = serde_json::from_str(&raw).map_err(|_| Error::InvalidPayload)?;
        if !value.is_object() {
            return Err(Error::InvalidPayload);
        }
        Ok(())
    }

    /// Read-only lookup by persisted client ID or known exchange ID; never submit/retry.
    pub async fn lookup_submission(
        &self,
        request: &perp_arb_private_events::lookup::Request,
    ) -> Result<perp_arb_private_events::lookup::Outcome> {
        use perp_arb_private_events::lookup::{self, Outcome, Selector};
        if request.scope != self.scope() {
            return Err(Error::Configuration);
        }
        request.validate()?;
        let result = timeout(self.0.policy.total_timeout, async {
            let _gate = self.0.gate.lock().await;
            self.reserve(1).await?;
            let mut params = vec![("symbol".into(), request.symbol.clone())];
            match &request.selector {
                Selector::OrderId(id) => params.push(("orderId".into(), id.clone())),
                Selector::ClientId(id) => params.push(("origClientOrderId".into(), id.clone())),
            };
            self.send(
                Method::GET,
                "/fapi/v3/order",
                params,
                self.0.policy.max_body_bytes,
            )
            .await
        })
        .await
        .map_err(|_| Error::Timeout)?;
        match result {
            Ok(raw) => Ok(lookup::aster_response(request, &raw)?),
            Err(Error::Exchange(-2013)) => Ok(Outcome::NotFound),
            Err(e) => Err(e),
        }
    }
    async fn inventory_sample(&self) -> Result<account::Inventory> {
        let mut values = Vec::new();
        for (path, weight) in [
            ("/fapi/v3/positionRisk", 5),
            ("/fapi/v3/openOrders", 40),
            ("/fapi/v3/balance", 5),
        ] {
            self.reserve(weight).await?;
            let raw = self
                .send(Method::GET, path, vec![], 2 * 1024 * 1024)
                .await?;
            values.push(serde_json::from_str(&raw).map_err(|_| Error::InvalidPayload)?);
        }
        Ok(account::aster_with_balances(
            &values[0], &values[1], &values[2],
        )?)
    }
    /// Account queries share the listen-key/order-query weighted budget.
    pub async fn check_position(&self, c: &Coordinator, symbol: &str) -> Result<account::Report> {
        if c.scope() != &self.scope() {
            return Err(Error::Configuration);
        }
        let (ticket, snapshot) = timeout(self.0.policy.total_timeout, async {
            let _gate = self.0.gate.lock().await;
            let ticket = c.begin_account_query().await?;
            let started_at = std::time::Instant::now();
            self.reserve(5).await?;
            let raw = self
                .send(
                    Method::GET,
                    "/fapi/v3/positionRisk",
                    vec![("symbol".into(), symbol.into())],
                    2 * 1024 * 1024,
                )
                .await?;
            let rows: Value = serde_json::from_str(&raw).map_err(|_| Error::InvalidPayload)?;
            let selected = rows
                .as_array()
                .ok_or(Error::InvalidPayload)?
                .iter()
                .filter(|p| p["symbol"].as_str() == Some(symbol))
                .cloned()
                .collect::<Vec<_>>();
            let inventory = account::aster(&Value::Array(selected), &serde_json::json!([]))?
                .for_position(symbol)?;
            Ok::<_, Error>((
                ticket,
                account::Snapshot {
                    scope: self.scope(),
                    inventory,
                    started_at,
                    completed_at: std::time::Instant::now(),
                    exchange_time_ms: None,
                },
            ))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(c.apply_position_snapshot(ticket, snapshot, symbol).await?)
    }
    pub async fn audit_account(
        &self,
        c: &Coordinator,
        expected: Option<account::Inventory>,
    ) -> Result<account::Report> {
        if c.scope() != &self.scope() {
            return Err(Error::Configuration);
        }
        let (ticket, snapshot) = timeout(self.0.policy.total_timeout, async {
            let _guard = self.0.gate.lock().await;
            let ticket = c.begin_account_query().await?;
            let started_at = std::time::Instant::now();
            let first = self.inventory_sample().await?;
            let second = self.inventory_sample().await?;
            if first != second {
                return Err(Error::InvalidPayload);
            }
            Ok::<_, Error>((
                ticket,
                account::Snapshot {
                    scope: self.scope(),
                    inventory: second,
                    started_at,
                    completed_at: std::time::Instant::now(),
                    exchange_time_ms: None,
                },
            ))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(c.apply_account_snapshot(ticket, snapshot, expected).await?)
    }

    /// Order-status GET errors preserve unknown exposure and
    /// request reconciliation; they never manufacture a rejected/zero-fill order.
    pub async fn reconcile_order(
        &self,
        coordinator: &Coordinator,
        key: OrderKey,
    ) -> Result<OrderSnapshot> {
        if coordinator.scope() != &self.scope() {
            return Err(Error::Configuration);
        }
        let result = timeout(self.0.policy.total_timeout, async {
            let _guard = self.0.gate.lock().await;
            self.reserve(1).await?;
            let ticket = coordinator.begin_query(key.clone()).await?;
            let params = vec![
                ("symbol".into(), key.symbol.clone()),
                ("orderId".into(), key.order_id.clone()),
            ];
            let raw = self
                .send(
                    Method::GET,
                    "/fapi/v3/order",
                    params,
                    self.0.policy.max_body_bytes,
                )
                .await?;
            Ok::<_, Error>((ticket, raw))
        })
        .await
        .map_err(|_| Error::Timeout)
        .and_then(|r| r);
        // Once a reply is read, admission/application is not canceled by the
        // HTTP timeout. It may contain a newly known fill.
        let result = match result {
            Ok((ticket, raw)) => coordinator
                .ingest_aster_rest(ticket, &raw)
                .await
                .map_err(Error::from),
            Err(error) => Err(error),
        };
        if result.is_err() {
            coordinator.require_reconciliation(key).await?;
        }
        result
    }
}
