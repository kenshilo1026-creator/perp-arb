//! Public metadata only. Clone one PublicHttp per runtime/IP budget; independent
//! instances/processes do NOT share limits. No credentials, signing or orders.
//! Resolve from memory on the decision path, refresh in a separate task. These
//! eligibility checks must never become a gate on hedging an existing exposure.
use crate::{
    catalog::{resolve_aster, resolve_hyperliquid, ResolvedInstrument, SymbolMappings},
    Venue,
};
use serde_json::{json, Value};
use std::{
    collections::{HashSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{watch, Mutex},
    time::{sleep_until, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    AsterV1,
    AsterV3,
    Hyperliquid,
}
impl Source {
    pub fn venue(self) -> Venue {
        match self {
            Self::Hyperliquid => Venue::Hyperliquid,
            _ => Venue::Aster,
        }
    }
    pub fn url(self) -> &'static str {
        match self {
            Self::AsterV1 => "https://fapi.asterdex.com/fapi/v1/exchangeInfo",
            Self::AsterV3 => "https://fapi.asterdex.com/fapi/v3/exchangeInfo",
            Self::Hyperliquid => "https://api.hyperliquid.xyz/info",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::AsterV1 => "aster_v1",
            Self::AsterV3 => "aster_v3",
            Self::Hyperliquid => "hyperliquid",
        }
    }
    fn weight(self) -> u32 {
        if self == Self::Hyperliquid {
            20
        } else {
            1
        }
    }
    fn index(self) -> usize {
        match self {
            Self::AsterV1 => 0,
            Self::AsterV3 => 1,
            Self::Hyperliquid => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataError {
    Configuration(String),
    Transport(String),
    Http(u16),
    BodyTooLarge,
    Invalid(String),
    Timeout,
    Unavailable(String),
}
impl std::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for MetadataError {}
type Result<T> = std::result::Result<T, MetadataError>;

#[derive(Clone, Debug)]
pub struct HttpPolicy {
    /// Conservative local allocations, not a claim to own the IP's entire quota.
    pub aster_weight_per_minute: u32,
    pub hyperliquid_weight_per_minute: u32,
    pub minimum_spacing: Duration,
    pub request_timeout: Duration,
    /// Includes queueing, limiter waits, retries and reading bodies.
    pub total_timeout: Duration,
    pub max_attempts: u32,
    pub max_body_bytes: usize,
    pub retry_backoff: Duration,
}
impl Default for HttpPolicy {
    fn default() -> Self {
        Self {
            aster_weight_per_minute: 120,
            hyperliquid_weight_per_minute: 600,
            minimum_spacing: Duration::from_millis(100),
            request_timeout: Duration::from_secs(15),
            total_timeout: Duration::from_secs(45),
            max_attempts: 3,
            max_body_bytes: 8_000_000,
            retry_backoff: Duration::from_secs(1),
        }
    }
}

struct BudgetState {
    requests: VecDeque<(Instant, u32)>,
    blocked_until: Instant,
    next_start: Instant,
    blocked_forever: bool,
}
struct Budget {
    capacity: u32,
    spacing: Duration,
    state: Mutex<BudgetState>,
    // Serial per venue so a 429 is observed before another local request starts.
    request: Mutex<()>,
}
impl Budget {
    fn new(capacity: u32, spacing: Duration) -> Self {
        let now = Instant::now();
        Self {
            capacity,
            spacing,
            state: Mutex::new(BudgetState {
                requests: VecDeque::new(),
                blocked_until: now,
                next_start: now,
                blocked_forever: false,
            }),
            request: Mutex::new(()),
        }
    }
    async fn acquire(&self, weight: u32) {
        loop {
            let mut state = self.state.lock().await;
            if state.blocked_forever {
                drop(state);
                std::future::pending::<()>().await;
                unreachable!();
            }
            let now = Instant::now();
            while state
                .requests
                .front()
                .is_some_and(|(t, _)| now.duration_since(*t) >= Duration::from_secs(60))
            {
                state.requests.pop_front();
            }
            let mut until = state.blocked_until.max(state.next_start);
            let mut used: u32 = state.requests.iter().map(|(_, w)| *w).sum();
            for (t, w) in &state.requests {
                if used + weight <= self.capacity {
                    break;
                }
                until = until.max(*t + Duration::from_secs(60));
                used -= w;
            }
            if until <= now {
                state.requests.push_back((now, weight));
                state.next_start = now + self.spacing;
                return;
            }
            drop(state);
            sleep_until(until).await;
        }
    }
    async fn defer(&self, delay: Duration) {
        let mut state = self.state.lock().await;
        // Never shorten server cooldowns, even when the duration overflows the
        // platform clock. A timed-out caller must not clear this shared state.
        if let Some(until) = Instant::now().checked_add(delay) {
            state.blocked_until = state.blocked_until.max(until);
        } else {
            state.blocked_forever = true;
        }
    }
}

struct HttpInner {
    client: reqwest::Client,
    policy: HttpPolicy,
    aster: Budget,
    hl: Budget,
    urls: [String; 3],
    attempts: [AtomicU64; 3],
}
#[derive(Clone)]
pub struct PublicHttp(Arc<HttpInner>);
impl PublicHttp {
    #[cfg(test)]
    pub(crate) fn local_test(url: &str) -> Self {
        let mut http = Self::new(HttpPolicy {
            max_attempts: 1,
            minimum_spacing: Duration::from_millis(1),
            ..HttpPolicy::default()
        })
        .unwrap();
        Arc::get_mut(&mut http.0).unwrap().urls = [url.into(), url.into(), url.into()];
        http
    }
    pub fn new(policy: HttpPolicy) -> Result<Self> {
        if policy.aster_weight_per_minute == 0
            || policy.aster_weight_per_minute > 2400
            || policy.hyperliquid_weight_per_minute < 20
            || policy.hyperliquid_weight_per_minute > 1200
            || policy.minimum_spacing.is_zero()
            || policy.minimum_spacing > Duration::from_secs(60)
            || policy.request_timeout.is_zero()
            || policy.request_timeout > Duration::from_secs(300)
            || policy.total_timeout.is_zero()
            || policy.total_timeout > Duration::from_secs(3600)
            || !(1..=5).contains(&policy.max_attempts)
            || !(1..=8_000_000).contains(&policy.max_body_bytes)
            || policy.retry_backoff.is_zero()
            || policy.retry_backoff > Duration::from_secs(60)
        {
            return Err(MetadataError::Configuration("invalid HTTP limits".into()));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .timeout(policy.request_timeout)
            .connect_timeout(policy.request_timeout)
            .user_agent("perp-arb-rust-public-metadata/0.1")
            .build()
            .map_err(|e| MetadataError::Configuration(e.to_string()))?;
        Ok(Self(Arc::new(HttpInner {
            client,
            aster: Budget::new(policy.aster_weight_per_minute, policy.minimum_spacing),
            hl: Budget::new(policy.hyperliquid_weight_per_minute, policy.minimum_spacing),
            policy,
            urls: [
                Source::AsterV1.url().into(),
                Source::AsterV3.url().into(),
                Source::Hyperliquid.url().into(),
            ],
            attempts: std::array::from_fn(|_| AtomicU64::new(0)),
        })))
    }
    /// Counts explicit send attempts (including network failures), not fills or
    /// a claim that the remote server received the request.
    pub fn request_attempts(&self, source: Source) -> u64 {
        self.0.attempts[source.index()].load(Ordering::Relaxed)
    }
    fn budget(&self, source: Source) -> &Budget {
        if source.venue() == Venue::Aster {
            &self.0.aster
        } else {
            &self.0.hl
        }
    }
    pub async fn fetch(&self, source: Source) -> Result<Value> {
        tokio::time::timeout(self.0.policy.total_timeout, self.fetch_inner(source))
            .await
            .map_err(|_| MetadataError::Timeout)?
    }
    async fn fetch_inner(&self, source: Source) -> Result<Value> {
        let budget = self.budget(source);
        for attempt in 0..self.0.policy.max_attempts {
            let _guard = budget.request.lock().await;
            budget.acquire(source.weight()).await;
            let url = &self.0.urls[source.index()];
            let request = if source == Source::Hyperliquid {
                self.0.client.post(url).json(&json!({"type":"meta"}))
            } else {
                self.0.client.get(url)
            };
            self.0.attempts[source.index()].fetch_add(1, Ordering::Relaxed);
            let response = request.send().await;
            let mut response = match response {
                Ok(r) => r,
                Err(e) => {
                    budget
                        .defer(self.0.policy.retry_backoff * (1 << attempt))
                        .await;
                    if attempt + 1 == self.0.policy.max_attempts {
                        return Err(MetadataError::Transport(e.to_string()));
                    }
                    continue;
                }
            };
            let status = response.status().as_u16();
            // External traffic may share this IP. At our local allowance, pause
            // conservatively for a full rolling minute; never increase budgets.
            if source.venue() == Venue::Aster
                && response
                    .headers()
                    .get("x-mbx-used-weight-1m")
                    .and_then(|s| s.to_str().ok())
                    .and_then(|s| s.parse::<u32>().ok())
                    .is_some_and(|v| v >= budget.capacity)
            {
                budget.defer(Duration::from_secs(60)).await;
            }
            if !(200..300).contains(&status) {
                if status == 429 || status == 418 || status == 503 {
                    let fallback = if status == 418 {
                        Duration::from_secs(180)
                    } else if status == 429 {
                        Duration::from_secs(60)
                    } else {
                        self.0.policy.retry_backoff * (1 << attempt)
                    };
                    let delay = retry_after(
                        response
                            .headers()
                            .get("retry-after")
                            .and_then(|s| s.to_str().ok()),
                        SystemTime::now(),
                    )
                    .unwrap_or(fallback);
                    budget.defer(delay.max(self.0.policy.retry_backoff)).await;
                } else if (500..600).contains(&status) {
                    budget
                        .defer(self.0.policy.retry_backoff * (1 << attempt))
                        .await;
                }
                if status == 418
                    || !(status == 429 || (500..600).contains(&status))
                    || attempt + 1 == self.0.policy.max_attempts
                {
                    return Err(MetadataError::Http(status));
                }
                continue;
            }
            if response
                .content_length()
                .is_some_and(|n| n > self.0.policy.max_body_bytes as u64)
            {
                return Err(MetadataError::BodyTooLarge);
            }
            let mut raw = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| MetadataError::Transport(e.to_string()))?
            {
                if chunk.len() > self.0.policy.max_body_bytes - raw.len() {
                    return Err(MetadataError::BodyTooLarge);
                }
                raw.extend_from_slice(&chunk);
            }
            let value =
                serde_json::from_slice(&raw).map_err(|e| MetadataError::Invalid(e.to_string()))?;
            validate_catalog(source, &value)?;
            return Ok(value);
        }
        unreachable!("nonzero attempt limit");
    }
}

fn retry_after(header: Option<&str>, now: SystemTime) -> Option<Duration> {
    let raw = header?.trim();
    if !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()) {
        return Some(Duration::from_secs(raw.parse::<u64>().unwrap_or(u64::MAX)));
    }
    httpdate::parse_http_date(raw)
        .ok()
        .map(|t| t.duration_since(now).unwrap_or_default())
}

/// Validate the envelope and identity globally. Unsupported/malformed individual
/// instruments stay in the snapshot and fail closed on resolution, so a new
/// product type cannot keep valid delisting updates from reaching other symbols.
fn validate_catalog(source: Source, value: &Value) -> Result<()> {
    let (field, name) = if source == Source::Hyperliquid {
        ("universe", "name")
    } else {
        ("symbols", "symbol")
    };
    let rows = value[field]
        .as_array()
        .filter(|r| !r.is_empty())
        .ok_or_else(|| MetadataError::Invalid(format!("missing/nonempty {field}")))?;
    if source == Source::Hyperliquid
        && value
            .get("collateralToken")
            .is_some_and(|v| v.as_u64() != Some(0))
    {
        return Err(MetadataError::Invalid("unsupported collateral".into()));
    }
    let mut names = HashSet::new();
    for row in rows {
        let symbol = row[name]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| MetadataError::Invalid("missing instrument identity".into()))?;
        if !names.insert(symbol.to_ascii_uppercase()) {
            return Err(MetadataError::Invalid(
                "duplicate instrument identity".into(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct CachePolicy {
    pub refresh_after: Duration,
    pub max_age: Duration,
    pub failure_retry: Duration,
}
impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            refresh_after: Duration::from_secs(300),
            max_age: Duration::from_secs(900),
            failure_retry: Duration::from_secs(5),
        }
    }
}
#[derive(Clone)]
pub struct MetadataSnapshot {
    pub generation: u64,
    pub payload: Arc<Value>,
    pub received_at: Instant,
}
struct CacheState {
    snapshot: Option<MetadataSnapshot>,
    revision: u64,
    last_attempt: Option<Instant>,
    error: Option<MetadataError>,
}
struct CacheInner {
    http: PublicHttp,
    source: Source,
    policy: CachePolicy,
    state: RwLock<CacheState>,
    refresh: Mutex<()>,
}
#[derive(Clone)]
pub struct MetadataCache(Arc<CacheInner>);
#[derive(Debug, Clone)]
pub struct CacheHealth {
    pub generation: u64,
    pub age: Option<Duration>,
    pub usable: bool,
    pub error: Option<MetadataError>,
}
impl MetadataCache {
    pub fn source(&self) -> Source {
        self.0.source
    }
    pub fn new(http: PublicHttp, source: Source, policy: CachePolicy) -> Result<Self> {
        if policy.refresh_after.is_zero()
            || policy.max_age < policy.refresh_after
            || policy.max_age > Duration::from_secs(86400)
            || policy.failure_retry.is_zero()
            || policy.failure_retry > policy.max_age
        {
            return Err(MetadataError::Configuration("invalid cache policy".into()));
        }
        Ok(Self(Arc::new(CacheInner {
            http,
            source,
            policy,
            state: RwLock::new(CacheState {
                snapshot: None,
                revision: 0,
                last_attempt: None,
                error: None,
            }),
            refresh: Mutex::new(()),
        })))
    }
    pub fn health(&self) -> CacheHealth {
        let state = self.0.state.read().expect("metadata lock poisoned");
        let age = state.snapshot.as_ref().map(|s| s.received_at.elapsed());
        let error = state.error.clone().or_else(|| match age {
            None => Some(MetadataError::Unavailable("not loaded".into())),
            Some(age) if age >= self.0.policy.max_age => {
                Some(MetadataError::Unavailable("metadata expired".into()))
            }
            _ => None,
        });
        CacheHealth {
            generation: state.revision,
            age,
            usable: error.is_none(),
            error,
        }
    }
    /// Recheck a previously resolved generation immediately before using it to
    /// add risk. This is a point-in-time check, not a cross-exchange order lock.
    pub fn is_current_generation(&self, generation: u64) -> bool {
        let health = self.health();
        health.usable && health.generation == generation
    }
    /// Memory-only, checks monotonic expiry every time. Do not retain a snapshot
    /// as perpetual trading authorization; recheck health/generation at use.
    pub fn current(&self) -> Result<MetadataSnapshot> {
        let state = self.0.state.read().expect("metadata lock poisoned");
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        let snapshot = state
            .snapshot
            .as_ref()
            .ok_or_else(|| MetadataError::Unavailable("not loaded".into()))?;
        if snapshot.received_at.elapsed() >= self.0.policy.max_age {
            return Err(MetadataError::Unavailable("metadata expired".into()));
        }
        Ok(snapshot.clone())
    }
    pub fn resolve(
        &self,
        symbol: &str,
        maps: &SymbolMappings,
    ) -> Result<(u64, ResolvedInstrument)> {
        let snapshot = self.current()?;
        let instrument = match self.0.source.venue() {
            Venue::Aster => resolve_aster(&snapshot.payload, symbol, maps),
            Venue::Hyperliquid => resolve_hyperliquid(&snapshot.payload, symbol, maps),
        }
        .map_err(MetadataError::Invalid)?;
        Ok((snapshot.generation, instrument))
    }
    /// Overlapping callers coalesce, including failed attempts. Cancelled fetches
    /// release the mutex without publishing a partial result or sticking a flag.
    /// force bypasses the refresh age, NEVER the failure backoff or HTTP limiter.
    pub async fn refresh(&self, force: bool) -> Result<MetadataSnapshot> {
        let revision = self
            .0
            .state
            .read()
            .expect("metadata lock poisoned")
            .revision;
        let _guard = self.0.refresh.lock().await;
        {
            let state = self.0.state.read().expect("metadata lock poisoned");
            if state.revision != revision {
                drop(state);
                return self.current();
            }
            if let Some(error) = &state.error {
                if state
                    .last_attempt
                    .is_some_and(|t| t.elapsed() < self.0.policy.failure_retry)
                {
                    return Err(error.clone());
                }
            } else if !force
                && state
                    .snapshot
                    .as_ref()
                    .is_some_and(|s| s.received_at.elapsed() < self.0.policy.refresh_after)
            {
                drop(state);
                return self.current();
            }
        }
        let result = self.0.http.fetch(self.0.source).await;
        let now = Instant::now();
        let mut state = self.0.state.write().expect("metadata lock poisoned");
        state.revision += 1;
        state.last_attempt = Some(now);
        match result {
            Ok(payload) => {
                let snapshot = MetadataSnapshot {
                    generation: state.revision,
                    payload: Arc::new(payload),
                    received_at: now,
                };
                state.snapshot = Some(snapshot.clone());
                state.error = None;
                Ok(snapshot)
            }
            Err(error) => {
                state.error = Some(error.clone());
                Err(error)
            }
        }
    }
    /// Immediate initial load, then age-based refresh. Sender drop/true stops and
    /// cancels any public read in progress. Caller owns and awaits this task.
    pub async fn run(&self, mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            tokio::select! {
                biased;
                _ = stop.changed() => { if *stop.borrow() || stop.has_changed().is_err() { return; } }
                _ = self.refresh(false) => {}
            }
            let delay = {
                let state = self.0.state.read().expect("metadata lock poisoned");
                if state.error.is_some() {
                    self.0.policy.failure_retry
                } else {
                    state
                        .snapshot
                        .as_ref()
                        .map(|s| {
                            self.0
                                .policy
                                .refresh_after
                                .saturating_sub(s.received_at.elapsed())
                        })
                        .unwrap_or(self.0.policy.failure_retry)
                }
            }
            .max(Duration::from_millis(1));
            tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = stop.changed() => { if *stop.borrow() || stop.has_changed().is_err() { return; } } }
        }
    }
}

#[cfg(test)]
#[path = "metadata_tests.rs"]
mod tests;
