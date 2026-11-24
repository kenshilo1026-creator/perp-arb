use crate::{
    auth::Credentials,
    events::{self, OrderEvidence, Trade},
    orders::Intent,
    wall_ms, Error, Result,
};
use futures_util::StreamExt;
use perp_arb_core::Decimal;
use perp_arb_private_events::{
    lookup::{Outcome, Request, Selector},
    Scope,
};
use reqwest::{Client as Http, Method};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct Clock {
    offset: i64,
    synced: Instant,
}
struct Budget {
    sent: VecDeque<(Instant, u32)>,
    blocked: Option<Instant>,
}
pub(crate) struct Inner {
    pub credentials: Credentials,
    http: Http,
    base: String,
    pub ws_url: String,
    clock: Mutex<Option<Clock>>,
    budget: Mutex<Budget>,
    submitted: Mutex<BTreeSet<String>>,
    timeout: Duration,
}
#[derive(Clone)]
pub struct Client(pub(crate) Arc<Inner>);
#[derive(Debug, Clone)]
pub struct Account {
    pub can_trade: bool,
    pub balances: BTreeMap<String, (Decimal, Decimal)>,
}
impl Client {
    pub fn new(credentials: Credentials) -> Result<Self> {
        Self::build(
            credentials,
            "https://api.binance.com".into(),
            "wss://ws-api.binance.com:443/ws-api/v3".into(),
            Duration::from_secs(10),
        )
    }
    fn build(
        credentials: Credentials,
        base: String,
        ws_url: String,
        timeout: Duration,
    ) -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = Http::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(timeout)
            .pool_idle_timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| Error::Configuration)?;
        Ok(Self(Arc::new(Inner {
            credentials,
            http,
            base,
            ws_url,
            clock: Mutex::new(None),
            budget: Mutex::new(Budget {
                sent: VecDeque::new(),
                blocked: None,
            }),
            submitted: Mutex::new(BTreeSet::new()),
            timeout,
        })))
    }
    #[cfg(test)]
    pub(crate) fn fixture(
        credentials: Credentials,
        base: String,
        ws_url: String,
        timeout: Duration,
    ) -> Result<Self> {
        Self::build(credentials, base, ws_url, timeout)
    }
    pub fn scope(&self) -> Scope {
        Scope {
            venue: "binance_spot".into(),
            account: self.0.credentials.account().into(),
        }
    }
    pub(crate) fn timestamp(&self) -> Result<u64> {
        let clock = self.0.clock.lock().map_err(|_| Error::Clock)?;
        let c = clock
            .as_ref()
            .filter(|c| c.synced.elapsed() < Duration::from_secs(1800))
            .ok_or(Error::Clock)?;
        let ms = i64::try_from(wall_ms())
            .map_err(|_| Error::Clock)?
            .checked_add(c.offset)
            .ok_or(Error::Clock)?;
        u64::try_from(ms).map_err(|_| Error::Clock)
    }
    // No sleep or network await under this mutex. Reserve headroom for hedge/cancel.
    fn reserve(&self, weight: u32, mutation: bool) -> Result<()> {
        let now = Instant::now();
        let mut b = self.0.budget.lock().map_err(|_| Error::RateLimited)?;
        if b.blocked.is_some_and(|t| t > now) {
            return Err(Error::RateLimited);
        }
        while b
            .sent
            .front()
            .is_some_and(|(t, _)| now.duration_since(*t) >= Duration::from_secs(60))
        {
            b.sent.pop_front();
        }
        let used: u32 = b.sent.iter().map(|(_, w)| *w).sum();
        if used + weight > if mutation { 600 } else { 500 } {
            return Err(Error::RateLimited);
        }
        b.sent.push_back((now, weight));
        Ok(())
    }
    fn cooldown(&self, seconds: u64) -> Result<()> {
        let until = Instant::now() + Duration::from_secs(seconds.clamp(1, 86400));
        let mut budget = self.0.budget.lock().map_err(|_| Error::RateLimited)?;
        budget.blocked = Some(budget.blocked.map_or(until, |old| old.max(until)));
        Ok(())
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        mut params: BTreeMap<String, String>,
        signed: bool,
        weight: u32,
    ) -> Result<Value> {
        self.reserve(weight, method != Method::GET)?;
        let request = self
            .0
            .http
            .request(method, format!("{}{path}", self.0.base));
        if signed {
            params.insert("timestamp".into(), self.timestamp()?.to_string());
            params.insert("recvWindow".into(), "5000".into());
            let payload = self.0.credentials.signed(&params)?;
            // Already canonical and ASCII; no second URL/form encoder after signing.
            let mut built = request
                .header("X-MBX-APIKEY", self.0.credentials.key())
                .build()
                .map_err(|_| Error::Configuration)?;
            built.url_mut().set_query(Some(&payload));
            return self.response(built).await;
        }
        if !params.is_empty() {
            return Err(Error::Configuration);
        }
        self.response(request.build().map_err(|_| Error::Configuration)?)
            .await
    }
    async fn response(&self, request: reqwest::Request) -> Result<Value> {
        tokio::time::timeout(self.0.timeout, async {
            let response = self.0.http.execute(request).await.map_err(|e| {
                if e.is_timeout() {
                    Error::Timeout
                } else {
                    Error::Transport
                }
            })?;
            let status = response.status().as_u16();
            if matches!(status, 418 | 429) {
                let seconds = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60);
                self.cooldown(seconds)?;
            }
            // Honor account/IP pressure reported by the exchange as well as our local budget.
            if response
                .headers()
                .get("x-mbx-used-weight-1m")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|n| n >= 6000)
            {
                self.cooldown(60)?;
            }
            let mut bytes = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(part) = stream.next().await {
                let part = part.map_err(|_| Error::Transport)?;
                if bytes.len() + part.len() > 2_000_000 {
                    return Err(Error::Capacity);
                }
                bytes.extend_from_slice(&part);
            }
            let v: Value = serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)?;
            let code = v["code"].as_i64();
            if status != 200 || code.is_some_and(|c| c != 0) {
                return Err(Error::Api { status, code });
            }
            Ok(v)
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub async fn sync_time(&self) -> Result<()> {
        let start = wall_ms();
        let mono = Instant::now();
        let v = self
            .request(Method::GET, "/api/v3/time", BTreeMap::new(), false, 1)
            .await?;
        let elapsed = mono.elapsed().as_millis() as u64;
        if elapsed > 2000 {
            return Err(Error::Clock);
        }
        let server = v["serverTime"].as_i64().ok_or(Error::Clock)?;
        let mid = i64::try_from(start + elapsed / 2).map_err(|_| Error::Clock)?;
        let offset = server.checked_sub(mid).ok_or(Error::Clock)?;
        if offset.unsigned_abs() > 60_000 {
            return Err(Error::Clock);
        }
        *self.0.clock.lock().map_err(|_| Error::Clock)? = Some(Clock {
            offset,
            synced: Instant::now(),
        });
        Ok(())
    }
    pub async fn account(&self) -> Result<Account> {
        let v = self
            .request(Method::GET, "/api/v3/account", BTreeMap::new(), true, 20)
            .await?;
        if v["accountType"] != "SPOT" {
            return Err(Error::Protocol);
        }
        let can_trade = v["canTrade"].as_bool().ok_or(Error::Protocol)?;
        let mut balances = BTreeMap::new();
        for row in v["balances"].as_array().ok_or(Error::Protocol)? {
            let asset = events::text(row, "asset")?;
            let amounts = (events::amount(row, "free")?, events::amount(row, "locked")?);
            if balances.insert(asset, amounts).is_some() {
                return Err(Error::Protocol);
            }
        }
        Ok(Account {
            can_trade,
            balances,
        })
    }
    fn selector(&self, request: &Request) -> Result<BTreeMap<String, String>> {
        request.validate().map_err(|_| Error::Configuration)?;
        if request.scope != self.scope() || !crate::orders::symbol(&request.symbol) {
            return Err(Error::Identity);
        }
        let (key, value) = match &request.selector {
            Selector::OrderId(id) => ("orderId", id),
            Selector::ClientId(id) => ("origClientOrderId", id),
        };
        Ok(BTreeMap::from([
            ("symbol".into(), request.symbol.clone()),
            (key.into(), value.clone()),
        ]))
    }
    pub async fn query(&self, request: &Request) -> Result<Option<OrderEvidence>> {
        match self
            .request(
                Method::GET,
                "/api/v3/order",
                self.selector(request)?,
                true,
                4,
            )
            .await
        {
            Ok(v) => events::rest(request, &v, Instant::now()).map(Some),
            Err(Error::Api {
                status: 400,
                code: Some(-2013),
            }) => Ok(None), // Absence never authorizes resend.
            Err(e) => Err(e),
        }
    }
    pub async fn lookup(&self, request: &Request) -> Result<Outcome> {
        Ok(self
            .query(request)
            .await?
            .map_or(Outcome::NotFound, |e| Outcome::Found(e.found)))
    }
    /// Single attempt per client ID per client instance. A durable caller must
    /// additionally fence process restarts: Binance may reuse IDs after a fill.
    pub async fn submit(&self, permit: crate::journal::Permit) -> Result<OrderEvidence> {
        if permit.scope != self.scope() {
            return Err(Error::Identity);
        }
        self.submit_once(&permit.intent).await
    }
    pub(crate) async fn submit_once(&self, intent: &Intent) -> Result<OrderEvidence> {
        let params = intent.params()?;
        {
            let mut ids = self
                .0
                .submitted
                .lock()
                .map_err(|_| Error::AlreadySubmitted)?;
            if ids.contains(&intent.client_id) {
                return Err(Error::AlreadySubmitted);
            }
            if ids.len() >= 10000 {
                return Err(Error::Capacity);
            }
            ids.insert(intent.client_id.clone());
        }
        let v = self
            .request(Method::POST, "/api/v3/order", params, true, 1)
            .await?;
        events::rest(&intent.request(self.scope())?, &v, Instant::now())
    }
    pub async fn cancel(&self, request: &Request) -> Result<OrderEvidence> {
        // A failed cancel (including unknown order) must be followed by query;
        // never infer zero fill or terminal state from an error response.
        let v = self
            .request(
                Method::DELETE,
                "/api/v3/order",
                self.selector(request)?,
                true,
                1,
            )
            .await?;
        events::rest(request, &v, Instant::now())
    }
    pub async fn trades(&self, request: &Request) -> Result<Vec<Trade>> {
        if !matches!(&request.selector, Selector::OrderId(_)) {
            return Err(Error::Configuration);
        }
        let mut params = self.selector(request)?;
        params.insert("limit".into(), "1000".into());
        let v = self
            .request(Method::GET, "/api/v3/myTrades", params, true, 20)
            .await?;
        let rows = v.as_array().ok_or(Error::Protocol)?;
        // A full page is not evidence of completeness. Bounded batch rejects it
        // rather than dropping unknown trades/fees; later pagination is explicit.
        if rows.len() >= 1000 {
            return Err(Error::Capacity);
        }
        rows.iter().map(|v| events::trade(request, v)).collect()
    }
}
