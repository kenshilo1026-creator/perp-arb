//! Bounded public GETs and two independent public WS feeds; never loads credentials.
use crate::{
    d, metadata,
    policy::{Book, Venue},
    Result,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{
    future::Future,
    pin::Pin,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};

pub fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub fn client() -> Result<reqwest::Client> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(12))
        .pool_idle_timeout(Duration::from_secs(120))
        .build()
        .map_err(|_| "public client initialization failed".into())
}
async fn get(client: &reqwest::Client, url: &str) -> Result<Value> {
    request(client.get(url)).await
}
async fn request(builder: reqwest::RequestBuilder) -> Result<Value> {
    let response = builder
        .send()
        .await
        .map_err(|_| "public HTTP transport/timeout".to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "public HTTP {}; no retry burst",
            response.status().as_u16()
        ));
    }
    response_json(response).await
}
async fn response_json(response: reqwest::Response) -> Result<Value> {
    let mut bytes = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| "public response truncated")?;
        if bytes.len() + chunk.len() > 8_000_000 {
            return Err("public response too large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid public JSON".into())
}
pub async fn instruments(
    client: &reqwest::Client,
    base: &str,
    venue: Venue,
) -> Result<(metadata::Instrument, metadata::Instrument)> {
    validate_base(base)?;
    let spot_url = format!("https://api.binance.com/api/v3/exchangeInfo?symbol={base}USDT");
    let perp = async {
        match venue {
            Venue::Aster => metadata::parse(
                &get(client, "https://fapi.asterdex.com/fapi/v3/exchangeInfo").await?,
                base,
                false,
            ),
            Venue::Hyperliquid => metadata::hyperliquid(
                &request(
                    client
                        .post("https://api.hyperliquid.xyz/info")
                        .json(&serde_json::json!({"type":"meta"})),
                )
                .await?,
                base,
            ),
            Venue::Lighter => metadata::lighter(
                &get(
                    client,
                    "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails",
                )
                .await?,
                base,
            ),
            Venue::BinanceSpot => Err("Binance Spot is not a perp venue".into()),
        }
    };
    let (spot, perp) = tokio::join!(get(client, &spot_url), perp);
    Ok((metadata::parse(&spot?, base, true)?, perp?))
}
pub fn validate_base(base: &str) -> Result<()> {
    if base.is_empty()
        || base.len() > 24
        || !base
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err("use exact uppercase ASCII base symbol; aliases unsupported".into());
    }
    Ok(())
}
#[derive(Debug, Clone)]
pub struct Quote {
    pub book: Book,
    pub version: u64,
    pub received: Instant,
    pub source_ms: u64,
    pub snapshot: bool,
    pub rest: bool,
}
impl Quote {
    pub fn aged(&self) -> Book {
        let mut book = self.book.clone();
        let local = self.received.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let source = if self.source_ms == 0 {
            0
        } else {
            wall_ms().abs_diff(self.source_ms)
        };
        book.age_ms = local.max(source);
        book
    }
}
fn string(v: &Value, key: &str) -> Result<String> {
    v[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("missing decimal field {key}"))
}
fn valid(book: &Book) -> Result<()> {
    if d(&book.bid)? <= perp_arb_core::Decimal::ZERO
        || d(&book.ask)? <= d(&book.bid)?
        || d(&book.bid_size)? < perp_arb_core::Decimal::ZERO
        || d(&book.ask_size)? < perp_arb_core::Decimal::ZERO
    {
        return Err("invalid quote".into());
    }
    Ok(())
}
pub fn decode(v: &Value, venue: Venue, symbol: &str) -> Result<Option<Quote>> {
    if !matches!(venue, Venue::BinanceSpot | Venue::Aster) {
        return Err("use the venue-specific public quote decoder".into());
    }
    if v.get("error").is_some() || v.get("code").is_some() {
        return Err("public WS exchange error".into());
    }
    let data = v.get("data").unwrap_or(v);
    let stream = v["stream"].as_str().unwrap_or("");
    let snapshot = data.get("lastUpdateId").is_some() || data["e"] == "depthUpdate";
    let (bid, ask, bq, aq, version) = if snapshot {
        if venue == Venue::BinanceSpot
            && stream != format!("{}@depth20@100ms", symbol.to_lowercase())
        {
            return Ok(None);
        }
        if venue == Venue::Aster && data["s"] != symbol {
            return Ok(None);
        }
        let bk = if venue == Venue::BinanceSpot {
            "bids"
        } else {
            "b"
        };
        let ak = if venue == Venue::BinanceSpot {
            "asks"
        } else {
            "a"
        };
        let level = |k: &str| -> Result<(String, String)> {
            let rows = data[k].as_array().ok_or("missing depth levels")?;
            let row = rows
                .first()
                .ok_or("empty depth")?
                .as_array()
                .ok_or("invalid level")?;
            if row.len() != 2 {
                return Err("invalid level width".into());
            }
            Ok((
                row[0].as_str().ok_or("invalid depth price")?.into(),
                row[1].as_str().ok_or("invalid depth quantity")?.into(),
            ))
        };
        let (b, bq) = level(bk)?;
        let (a, aq) = level(ak)?;
        (
            b,
            a,
            bq,
            aq,
            data[if venue == Venue::BinanceSpot {
                "lastUpdateId"
            } else {
                "u"
            }]
            .as_u64()
            .ok_or("missing depth sequence")?,
        )
    } else if data.get("b").is_some() && data.get("B").is_some() {
        if data["s"] != symbol {
            return Ok(None);
        }
        (
            string(data, "b")?,
            string(data, "a")?,
            string(data, "B")?,
            string(data, "A")?,
            data["u"].as_u64().ok_or("missing BBO sequence")?,
        )
    } else {
        return Ok(None);
    };
    let book = Book {
        bid,
        ask,
        bid_size: bq,
        ask_size: aq,
        age_ms: 0,
    };
    valid(&book)?;
    Ok(Some(Quote {
        book,
        version,
        received: Instant::now(),
        source_ms: data["T"]
            .as_u64()
            .or_else(|| data["E"].as_u64())
            .unwrap_or(0),
        snapshot,
        rest: false,
    }))
}
#[derive(Default)]
pub struct Cache {
    pub quote: Option<Quote>,
}
impl Cache {
    pub fn update(&mut self, quote: Quote) -> Result<bool> {
        if let Some(old) = &self.quote {
            if quote.version < old.version {
                return Ok(false);
            }
            if quote.version == old.version {
                let a = &old.book;
                let b = &quote.book;
                if d(&a.bid)? != d(&b.bid)?
                    || d(&a.ask)? != d(&b.ask)?
                    || d(&a.bid_size)? != d(&b.bid_size)?
                    || d(&a.ask_size)? != d(&b.ask_size)?
                {
                    self.quote = None;
                    return Err("conflicting quotes for same update ID".into());
                }
                if !quote.snapshot {
                    return Ok(false);
                }
                // A REST snapshot is aged from request START, not response arrival.
                // Same-version WS evidence received during that GET is newer.
                if quote.received < old.received {
                    return Ok(false);
                }
            }
        }
        self.quote = Some(quote);
        Ok(true)
    }
}
pub struct Event {
    pub venue: Venue,
    pub quote: Option<Quote>,
    pub error: Option<String>,
}

struct RefreshError {
    reason: String,
    backoff: Duration,
}
type RefreshFuture = Pin<Box<dyn Future<Output = std::result::Result<Quote, RefreshError>> + Send>>;
// Shared across public subscriptions/reconnects in this process. A reconnect
// must not bypass Retry-After or create a REST request burst.
static BINANCE_REFRESH_AFTER: std::sync::Mutex<Option<Instant>> = std::sync::Mutex::new(None);
fn reserve_refresh(now: Instant) -> bool {
    let Ok(mut after) = BINANCE_REFRESH_AFTER.lock() else {
        return false;
    };
    if after.is_some_and(|t| now < t) {
        return false;
    }
    *after = Some(now + Duration::from_millis(500));
    true
}
fn defer_refresh(delay: Duration) {
    if let Ok(mut after) = BINANCE_REFRESH_AFTER.lock() {
        let until = Instant::now() + delay;
        *after = Some(after.map_or(until, |t| t.max(until)));
    }
}
fn refresh_backoff(status: u16, retry_after: Option<&str>) -> Duration {
    if matches!(status, 418 | 429) {
        Duration::from_secs(
            retry_after
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60)
                .max(60),
        )
    } else {
        Duration::from_secs(5)
    }
}
fn rest_snapshot(data: Value, symbol: &str, started: Instant) -> Result<Quote> {
    let wrapper =
        serde_json::json!({"stream":format!("{}@depth20@100ms",symbol.to_lowercase()),"data":data});
    let mut q = decode(&wrapper, Venue::BinanceSpot, symbol)?.ok_or("missing REST depth")?;
    if !q.snapshot {
        return Err("REST depth must be a full snapshot".into());
    }
    q.received = started;
    q.rest = true;
    Ok(q)
}
async fn quiet_snapshot(
    http: reqwest::Client,
    symbol: String,
) -> std::result::Result<Quote, RefreshError> {
    let started = Instant::now();
    let attempt = async {
        let response = http
            .get(format!(
                "https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5"
            ))
            .send()
            .await
            .map_err(|_| RefreshError {
                reason: "quiet depth transport error".into(),
                backoff: Duration::from_secs(5),
            })?;
        if !response.status().is_success() {
            return Err(RefreshError {
                reason: format!("quiet depth HTTP {}", response.status().as_u16()),
                backoff: refresh_backoff(
                    response.status().as_u16(),
                    response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok()),
                ),
            });
        }
        let result = match response_json(response).await {
            Ok(v) => rest_snapshot(v, &symbol, started),
            Err(e) => Err(e),
        };
        result.map_err(|reason| RefreshError {
            reason,
            backoff: Duration::from_secs(5),
        })
    };
    tokio::time::timeout(Duration::from_millis(750), attempt)
        .await
        .unwrap_or_else(|_| {
            Err(RefreshError {
                reason: "quiet depth timeout".into(),
                backoff: Duration::from_secs(5),
            })
        })
}

fn refresh_due(cache: &Cache, now: Instant, not_before: Instant) -> bool {
    now >= not_before
        && cache
            .quote
            .as_ref()
            .is_none_or(|q| now.saturating_duration_since(q.received) >= Duration::from_millis(500))
}
pub fn subscribe(
    venue: Venue,
    instrument: metadata::Instrument,
    tx: mpsc::Sender<Event>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let s = instrument.symbol.to_lowercase();
        let url = match venue {
            Venue::BinanceSpot => format!(
                "wss://stream.binance.com:9443/stream?streams={s}@bookTicker/{s}@depth20@100ms"
            ),
            Venue::Aster => format!(
                "wss://fstream.asterdex.com/stream?streams={s}@bookTicker/{s}@depth20@100ms"
            ),
            Venue::Hyperliquid => "wss://api.hyperliquid.xyz/ws".into(),
            Venue::Lighter => "wss://mainnet.zklighter.elliot.ai/stream?readonly=true".into(),
        };
        let mut retry = 1;
        loop {
            let result = connection(&url, venue, &instrument, &tx).await;
            if tx
                .send(Event {
                    venue,
                    quote: None,
                    error: Some(result.err().unwrap_or_else(|| "public stream ended".into())),
                })
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(retry)).await;
            retry = (retry * 2).min(8);
        }
    })
}
async fn connection(
    url: &str,
    venue: Venue,
    instrument: &metadata::Instrument,
    tx: &mpsc::Sender<Event>,
) -> Result<()> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(1_048_576))
        .max_frame_size(Some(1_048_576));
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async_with_config(url, Some(config), true),
    )
    .await
    .map_err(|_| "public WS connection timeout")?
    .map_err(|_| "public WS connection failed")?;
    for subscription in crate::perp_feed::subscriptions(venue, instrument)? {
        ws.send(Message::Text(subscription.to_string().into()))
            .await
            .map_err(|_| "public WS subscribe failed")?;
    }
    // Quiet Binance books can have >1s between WS publications. Confirm them
    // independently, as the Python owner does; never poll on fill -> hedge.
    // One request at a time, at most two starts/second, bounded RTT and backoff.
    let http = client()?;
    let mut latest = Cache::default();
    let mut refresh: Option<RefreshFuture> = None;
    let mut next_refresh = Instant::now() + Duration::from_millis(500);
    let mut refresh_tick = tokio::time::interval(Duration::from_millis(100));
    let mut last_ws = tokio::time::Instant::now();
    loop {
        let msg = tokio::select! {
            _=tokio::time::sleep_until(last_ws+Duration::from_secs(30))=>return Err("public WS inactive".into()),
            _=refresh_tick.tick(),if venue==Venue::BinanceSpot=>{
                let now=Instant::now();
                if refresh.is_none() && refresh_due(&latest,now,next_refresh) && reserve_refresh(now) {
                    next_refresh=now+Duration::from_millis(500);
                    refresh=Some(Box::pin(quiet_snapshot(http.clone(),instrument.symbol.clone())));
                }
                continue;
            },
            result=async {refresh.as_mut().expect("guarded refresh").await},if refresh.is_some()=>{
                refresh=None;
                match result {
                    Ok(q)=>match latest.update(q.clone()) {
                        Ok(true)=>tx.send(Event {venue,quote:Some(q),error:None}).await.map_err(|_|"consumer closed")?,
                        Ok(false)=>(),
                        Err(error)=>tx.send(Event {venue,quote:None,error:Some(error)}).await.map_err(|_|"consumer closed")?,
                    },
                    Err(error)=>{next_refresh=Instant::now()+error.backoff;defer_refresh(error.backoff);eprintln!("{}; backing off {}s; existing quote still expires normally",error.reason,error.backoff.as_secs());}
                }
                continue;
            },
            msg=ws.next()=>msg.ok_or("public WS ended")?.map_err(|_|"public WS read failed")?,
        };
        last_ws = tokio::time::Instant::now();
        let received = Instant::now();
        match msg {
            Message::Text(text) => {
                let v: Value = serde_json::from_str(&text).map_err(|_| "invalid public WS JSON")?;
                if venue == Venue::Lighter && v["type"] == "ping" {
                    ws.send(Message::Text("{\"type\":\"pong\"}".into()))
                        .await
                        .map_err(|_| "Lighter heartbeat failed")?;
                    continue;
                }
                let quote = match venue {
                    Venue::BinanceSpot | Venue::Aster => decode(&v, venue, &instrument.symbol)?,
                    Venue::Hyperliquid | Venue::Lighter => {
                        crate::perp_feed::decode(&v, venue, instrument)?
                    }
                };
                if let Some(mut quote) = quote {
                    quote.received = received;
                    let _ = latest.update(quote.clone());
                    tx.send(Event {
                        venue,
                        quote: Some(quote),
                        error: None,
                    })
                    .await
                    .map_err(|_| "consumer closed")?;
                }
            }
            Message::Ping(p) => ws
                .send(Message::Pong(p))
                .await
                .map_err(|_| "public WS heartbeat failed")?,
            Message::Close(_) => return Err("public WS closed".into()),
            _ => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn frame(id: u64) -> Value {
        json!({"s":"SYNUSDT","u":id,"b":"0.1","a":"0.11","B":"10","A":"11"})
    }
    fn depth(id: u64) -> Value {
        json!({"lastUpdateId":id,"bids":[["0.1","10"]],"asks":[["0.11","11"]]})
    }
    #[test]
    fn quiet_snapshot_confirms_unchanged_book_without_overwriting_newer_ws() {
        let start = Instant::now() - Duration::from_millis(100);
        let mut c = Cache::default();
        let ws = decode(&frame(2), Venue::BinanceSpot, "SYNUSDT")
            .unwrap()
            .unwrap();
        let ws_time = ws.received;
        c.update(ws).unwrap();
        assert!(!c
            .update(rest_snapshot(depth(1), "SYNUSDT", start).unwrap())
            .unwrap());
        assert!(!c
            .update(rest_snapshot(depth(2), "SYNUSDT", start).unwrap())
            .unwrap());
        assert_eq!(c.quote.as_ref().unwrap().received, ws_time);
        // A later GET confirms an unchanged complete book with the same ID.
        let later = Instant::now();
        assert!(c
            .update(rest_snapshot(depth(2), "SYNUSDT", later).unwrap())
            .unwrap());
        assert_eq!(c.quote.as_ref().unwrap().received, later);
        assert!(c.quote.as_ref().unwrap().rest);
        let mut conflict = depth(2);
        conflict["bids"][0][1] = json!("99");
        assert!(c
            .update(rest_snapshot(conflict, "SYNUSDT", Instant::now()).unwrap())
            .is_err());
        assert!(c.quote.is_none());
    }
    #[test]
    fn quiet_refresh_is_paced_and_failed_refresh_never_renews_quote() {
        let start = Instant::now();
        let mut c = Cache::default();
        c.update(rest_snapshot(depth(1), "SYNUSDT", start).unwrap())
            .unwrap();
        assert!(!refresh_due(&c, start + Duration::from_millis(499), start));
        assert!(refresh_due(&c, start + Duration::from_millis(500), start));
        assert!(!refresh_due(
            &c,
            start + Duration::from_secs(1),
            start + Duration::from_secs(60)
        ));
        assert_eq!(refresh_backoff(429, Some("120")), Duration::from_secs(120));
        assert_eq!(refresh_backoff(418, None), Duration::from_secs(60));
        assert_eq!(refresh_backoff(503, None), Duration::from_secs(5));
        assert!(rest_snapshot(json!({"code":-1}), "SYNUSDT", Instant::now()).is_err());
        assert_eq!(c.quote.as_ref().unwrap().received, start);
        let slow =
            rest_snapshot(depth(2), "SYNUSDT", Instant::now() - Duration::from_secs(2)).unwrap();
        assert!(
            slow.aged().age_ms >= 2000,
            "RTT must count towards quote age"
        );
    }
    #[test]
    fn rejects_wrong_symbol_crossed_and_nondecimal() {
        assert!(decode(&frame(1), Venue::BinanceSpot, "ETHUSDT")
            .unwrap()
            .is_none());
        let mut f = frame(1);
        f["a"] = json!("0.09");
        assert!(decode(&f, Venue::BinanceSpot, "SYNUSDT").is_err());
        f["a"] = json!("NaN");
        assert!(decode(&f, Venue::BinanceSpot, "SYNUSDT").is_err());
    }
    #[test]
    fn never_mix_old_depth_with_new_bbo() {
        let mut c = Cache::default();
        assert!(c
            .update(
                decode(&frame(2), Venue::BinanceSpot, "SYNUSDT")
                    .unwrap()
                    .unwrap()
            )
            .unwrap());
        assert!(!c
            .update(
                decode(&frame(1), Venue::BinanceSpot, "SYNUSDT")
                    .unwrap()
                    .unwrap()
            )
            .unwrap());
        let mut conflict = frame(2);
        conflict["B"] = json!("99");
        assert!(c
            .update(
                decode(&conflict, Venue::BinanceSpot, "SYNUSDT")
                    .unwrap()
                    .unwrap()
            )
            .is_err());
        assert!(c.quote.is_none());
    }
    #[test]
    fn duplicate_bbo_does_not_refresh_age() {
        let mut c = Cache::default();
        let q = decode(&frame(1), Venue::BinanceSpot, "SYNUSDT")
            .unwrap()
            .unwrap();
        c.update(q.clone()).unwrap();
        assert!(!c.update(q).unwrap());
    }

    #[test]
    fn equal_numeric_snapshot_can_refresh_but_is_not_a_conflict() {
        let mut c = Cache::default();
        c.update(
            decode(&frame(1), Venue::BinanceSpot, "SYNUSDT")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let snapshot = json!({"stream":"synusdt@depth20@100ms", "data":{
            "lastUpdateId":1,"bids":[["0.1000","10.00"]],"asks":[["0.1100","11.00"]]}});
        assert!(c
            .update(
                decode(&snapshot, Venue::BinanceSpot, "SYNUSDT")
                    .unwrap()
                    .unwrap()
            )
            .unwrap());
        assert!(c.quote.is_some());
    }
}
