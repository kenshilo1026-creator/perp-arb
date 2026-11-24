//! Variational v2 public indicative quotes ONLY. Not a firm RFQ, visible depth,
//! tick/lot metadata or execution permission. Do not route these through the
//! order-book hub: the public API may cache a quote for up to 600 seconds.
use chrono::DateTime;
use perp_arb_core::{
    decimal,
    market::{Depth, Freshness, FreshnessPolicy, Quote},
    Decimal,
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};

pub const STATS_URL: &str =
    "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/v2/stats";
const MAX_BODY: usize = 8_000_000;
const SPACING: Duration = Duration::from_secs(2);
const TIMEOUT: Duration = Duration::from_secs(15);
type Result<T> = std::result::Result<T, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractType {
    Perpetual,
    Swap,
}

#[derive(Debug)]
pub struct Listing {
    pub ticker: String,
    pub contract_type: ContractType,
    // Keep absent/invalid quotes local to this market, never reuse its old quote.
    quotes: Result<Quotes>,
}

#[derive(Debug)]
struct Quotes {
    source_ms: i64,
    tiers: BTreeMap<&'static str, Result<(Decimal, Decimal)>>,
}

#[derive(Debug)]
pub struct Stats {
    pub received_ms: i64,
    pub listings: BTreeMap<String, Listing>,
}

/// Intentionally offers no executable(), depth or order-sizing method.
#[derive(Debug, Clone)]
pub struct IndicativeQuote {
    pub ticker: String,
    pub tier: &'static str,
    pub tier_notional_usdc: Decimal,
    pub requested_notional_usdc: Decimal,
    pub bid: Decimal,
    pub ask: Decimal,
    pub source_ms: i64,
    pub received_ms: i64,
}

impl IndicativeQuote {
    pub fn freshness(&self, now_ms: i64, policy: FreshnessPolicy) -> Freshness {
        // Unsized is used privately only for the shared freshness semantics.
        // Callers never receive a Quote that could price unlimited liquidity.
        Quote::new(
            self.bid,
            self.ask,
            self.received_ms,
            Some(self.source_ms),
            Depth::Unsized,
            Depth::Unsized,
        )
        .map_or(Freshness::Disconnected, |q| {
            q.freshness(now_ms, true, policy)
        })
    }
}

fn price(value: &Value) -> Result<Decimal> {
    let p = decimal(value.as_str().ok_or("price must be a decimal string")?)
        .map_err(|_| "invalid decimal price")?;
    if p <= Decimal::ZERO {
        return Err("price must be positive".into());
    }
    Ok(p)
}

fn quotes(value: &Value) -> Result<Quotes> {
    let raw = value["updated_at"]
        .as_str()
        .ok_or("missing quote timestamp")?;
    let source_ms = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| "invalid quote timestamp or missing timezone")?
        .timestamp_millis();
    if source_ms <= 0 {
        return Err("invalid quote timestamp".into());
    }
    let mut tiers = BTreeMap::new();
    for name in ["base", "size_1k", "size_10k", "size_100k", "size_1m"] {
        if let Some(row) = value.get(name).filter(|v| !v.is_null()) {
            let pair = (|| {
                let bid = price(&row["bid"])?;
                let ask = price(&row["ask"])?;
                if bid > ask {
                    return Err("crossed indicative quote".into());
                }
                Ok((bid, ask))
            })();
            tiers.insert(name, pair);
        }
    }
    Ok(Quotes { source_ms, tiers })
}

impl Stats {
    pub fn parse(bytes: &[u8], received_ms: i64) -> Result<Self> {
        if bytes.len() > MAX_BODY || received_ms <= 0 {
            return Err("invalid body size or receipt time".into());
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| "invalid stats JSON")?;
        let rows = value["listings"].as_array().ok_or("missing listings")?;
        let mut listings = BTreeMap::new();
        for row in rows {
            let ticker = row["ticker"].as_str().ok_or("missing ticker")?;
            if ticker.is_empty() || ticker.trim() != ticker || ticker.len() > 128 {
                return Err("invalid ticker".into());
            }
            let (contract_type, suffix) = match row["contract_type"].as_str() {
                Some("perpetual") => (ContractType::Perpetual, "-PERP"),
                Some("swap") => (ContractType::Swap, "-SWAP"),
                _ => return Err("unsupported or missing contract_type (v2 required)".into()),
            };
            if !ticker.ends_with(suffix) || ticker.len() == suffix.len() {
                return Err("ticker/contract_type mismatch".into());
            }
            let key = ticker.to_uppercase();
            let listing = Listing {
                ticker: ticker.to_owned(),
                contract_type,
                quotes: quotes(&row["quotes"]),
            };
            if listings.insert(key, listing).is_some() {
                return Err("duplicate market ticker".into());
            }
        }
        Ok(Self {
            received_ms,
            listings,
        })
    }

    /// Select the smallest published numbered tier at least as large as the
    /// requested USDC notional. Never fall back to base (unknown minimum size)
    /// or extrapolate above the largest published tier. This is a reference
    /// for that tier, NOT a firm quote for the requested size.
    pub fn perpetual_quote(&self, symbol: &str, notional_usdc: Decimal) -> Result<IndicativeQuote> {
        if notional_usdc <= Decimal::ZERO {
            return Err("notional must be positive".into());
        }
        let symbol = symbol.trim().to_uppercase();
        let key = if symbol.ends_with("-PERP") || symbol.ends_with("-SWAP") {
            symbol
        } else {
            format!("{symbol}-PERP")
        };
        let listing = self
            .listings
            .get(&key)
            .ok_or("perpetual market not found")?;
        if listing.contract_type != ContractType::Perpetual {
            return Err("swap is not a perpetual".into());
        }
        let quotes = listing.quotes.as_ref().map_err(Clone::clone)?;
        for (tier, amount) in [
            ("size_1k", 1_000),
            ("size_10k", 10_000),
            ("size_100k", 100_000),
            ("size_1m", 1_000_000),
        ] {
            let amount = Decimal::from(amount);
            if notional_usdc <= amount {
                if let Some(pair) = quotes.tiers.get(tier) {
                    let &(bid, ask) = pair.as_ref().map_err(Clone::clone)?;
                    return Ok(IndicativeQuote {
                        ticker: listing.ticker.clone(),
                        tier,
                        tier_notional_usdc: amount,
                        requested_notional_usdc: notional_usdc,
                        bid,
                        ask,
                        source_ms: quotes.source_ms,
                        received_ms: self.received_ms,
                    });
                }
            }
        }
        Err("no published numbered quote tier covering requested notional".into())
    }
}

struct State {
    cached: Option<(Instant, Arc<Stats>)>,
    next_request: Instant,
    failures: u32,
    blocked: bool,
}

/// Clone one client for all symbols/consumers. One all-market request at a time,
/// 2-second cache/spacing and shared cooldown; no background task or retry loop.
/// Independent clients/processes (including Python) do not share this IP budget.
#[derive(Clone)]
pub struct PublicClient {
    http: reqwest::Client,
    url: String,
    state: Arc<Mutex<State>>,
}

pub fn now_ms() -> Result<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "clock before epoch")?
        .as_millis()
        .try_into()
        .map_err(|_| "clock overflow".into())
}

impl PublicClient {
    pub fn new() -> Result<Self> {
        Self::with_url(STATS_URL.to_owned())
    }

    fn with_url(url: String) -> Result<Self> {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(TIMEOUT)
            .build()
            .map_err(|_| "HTTP client initialization failed")?;
        Ok(Self {
            http,
            url,
            state: Arc::new(Mutex::new(State {
                cached: None,
                next_request: Instant::now(),
                failures: 0,
                blocked: false,
            })),
        })
    }

    pub async fn snapshot(&self) -> Result<Arc<Stats>> {
        tokio::time::timeout(TIMEOUT, self.snapshot_inner())
            .await
            .map_err(|_| "stats timeout (includes queue/cooldown wait)".to_owned())?
    }

    async fn snapshot_inner(&self) -> Result<Arc<Stats>> {
        let mut state = self.state.lock().await;
        if state.blocked {
            return Err("server cooldown overflow; client blocked".into());
        }
        if let Some((received, stats)) = &state.cached {
            if received.elapsed() < SPACING {
                return Ok(stats.clone());
            }
        }
        // Invalidate before any await. A failed/cancelled refresh never serves
        // the old snapshot as healthy; caller-held snapshots retain source time.
        state.cached = None;
        tokio::time::sleep_until(state.next_request).await;
        state.failures = state.failures.saturating_add(1);
        let backoff = Duration::from_secs(2u64.pow(state.failures.min(5)));
        state.next_request = Instant::now() + backoff;
        let mut response = self
            .http
            .get(&self.url)
            .send()
            .await
            .map_err(|_| "stats HTTP transport failed")?;
        let status = response.status();
        if status.as_u16() == 429 || status.as_u16() == 418 {
            // Protect cancellation/body failures before reading Retry-After JSON.
            state.next_request = state
                .next_request
                .max(Instant::now() + Duration::from_secs(30));
        }
        if let Some(raw) = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
        {
            let delay = raw
                .parse::<u64>()
                .ok()
                .map(Duration::from_secs)
                .or_else(|| {
                    httpdate::parse_http_date(raw)
                        .ok()
                        .and_then(|t| t.duration_since(std::time::SystemTime::now()).ok())
                });
            if let Some(delay) = delay {
                defer(&mut state, delay)?;
            }
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_BODY as u64)
        {
            return Err("stats body too large".into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "stats body read failed")?
        {
            if bytes.len() + chunk.len() > MAX_BODY {
                return Err("stats body too large".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            if let Ok(body) = serde_json::from_slice::<Value>(&bytes) {
                if let Some(raw) = body.get("retry_after_seconds") {
                    let text = raw
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| raw.to_string());
                    if let Ok(seconds) = decimal(&text) {
                        if seconds > Decimal::ZERO {
                            // Overflow must not shorten a server's cooldown.
                            let seconds = seconds
                                .ceil()
                                .to_string()
                                .parse::<u64>()
                                .unwrap_or(u64::MAX);
                            defer(&mut state, Duration::from_secs(seconds))?;
                        }
                    }
                }
            }
            return Err(format!("stats HTTP {}", status.as_u16()));
        }
        let stats = Arc::new(Stats::parse(&bytes, now_ms()?)?);
        let received = Instant::now();
        state.cached = Some((received, stats.clone()));
        state.next_request = state.next_request.max(received + SPACING);
        state.failures = 0;
        Ok(stats)
    }
}

fn defer(state: &mut State, delay: Duration) -> Result<()> {
    // An unrepresentable delay blocks the shared client, never shortens cooldown.
    match Instant::now().checked_add(delay) {
        Some(until) => state.next_request = state.next_request.max(until),
        None => {
            state.blocked = true;
            return Err("server cooldown overflow; stop this client".into());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "variational_tests.rs"]
mod tests;
