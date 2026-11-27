//! Public partial snapshots ONLY: never use the Aster diff-depth stream here.
#![forbid(unsafe_code)]

pub mod catalog;
pub mod hub;
pub mod lighter;
pub mod metadata;
pub mod recorder;
pub mod shared;
pub mod variational;

use perp_arb_core::{
    decimal,
    market::{Depth, Freshness, FreshnessPolicy, Level, Quote},
    pricing::Side,
    Decimal,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Aster,
    Hyperliquid,
    Lighter,
}

impl Venue {
    pub fn from_name(name: &str) -> Result<Self, String> {
        match name {
            "aster" => Ok(Self::Aster),
            "hyperliquid" => Ok(Self::Hyperliquid),
            "lighter" => Ok(Self::Lighter),
            _ => Err("unsupported execution venue".into()),
        }
    }
    pub fn endpoint(self, symbol: &str) -> Result<String, String> {
        if self == Self::Lighter {
            if symbol.is_empty()
                || symbol.len() > 80
                || !symbol
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            {
                return Err("invalid native symbol".into());
            }
            return Ok(lighter::WS.into());
        }
        if symbol.is_empty()
            || symbol.len() > 80
            || !symbol
                .chars()
                .all(|c| c.is_alphanumeric() || (self == Self::Hyperliquid && ":_-".contains(c)))
        {
            return Err("invalid exchange symbol".into());
        }
        Ok(match self {
            Self::Aster => format!(
                "wss://fstream.asterdex.com/ws/{}@depth10@100ms",
                aster_stream_symbol(symbol)
            ),
            Self::Hyperliquid => "wss://api.hyperliquid.xyz/ws".into(),
            _ => unreachable!(),
        })
    }
    pub fn subscription(self, symbol: &str) -> Option<Value> {
        (self == Self::Hyperliquid).then(|| json!({"method":"subscribe", "subscription":{"type":"l2Book", "coin":symbol, "fast":true}}))
    }
}

/// Stream names in URLs must encode UTF-8 (Aster lists non-ASCII native symbols).
/// Delimiters are added by us, never accepted from a symbol supplied by a caller.
pub(crate) fn aster_stream_symbol(symbol: &str) -> String {
    let mut result = String::new();
    for byte in symbol.to_ascii_lowercase().bytes() {
        if byte.is_ascii_alphanumeric() {
            result.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(result, "%{byte:02X}").expect("string write");
        }
    }
    result
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub symbol: String,
    pub sequence: Option<u64>,
    pub quote: Quote,
}

impl Snapshot {
    pub fn normalized(&self) -> Value {
        let rows = |side| {
            self.quote
                .levels(side)
                .unwrap_or_default()
                .iter()
                .map(|l| {
                    [
                        l.price.normalize().to_string(),
                        l.size.normalize().to_string(),
                    ]
                })
                .collect::<Vec<_>>()
        };
        json!({"symbol":self.symbol, "source_ms":self.quote.source_ms(),
            "bid":self.quote.bid().normalize().to_string(), "ask":self.quote.ask().normalize().to_string(),
            "bids":rows(Side::Sell), "asks":rows(Side::Buy)})
    }
}

fn price(value: &Value) -> Result<Decimal, String> {
    // Both subscribed exchange protocols publish prices/quantities as strings.
    // Reject JSON floats rather than allowing a binary-float round trip.
    decimal(
        value
            .as_str()
            .ok_or("price/size must be a decimal string")?,
    )
    .map_err(|e| e.to_string())
}

fn levels(value: &Value, objects: bool) -> Result<Vec<Level>, String> {
    let rows = value.as_array().ok_or("missing depth array")?;
    if rows.len() > 20 {
        return Err("unexpected depth size for partial snapshot".into());
    }
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let (p, s) = if objects {
            (&row["px"], &row["sz"])
        } else {
            let pair = row.as_array().ok_or("invalid depth row")?;
            if pair.len() != 2 {
                return Err("invalid depth row length".into());
            }
            (&pair[0], &pair[1])
        };
        let (p, s) = (price(p)?, price(s)?);
        if p <= Decimal::ZERO || s < Decimal::ZERO {
            return Err("nonpositive price or negative size".into());
        }
        if s > Decimal::ZERO {
            output.push(Level { price: p, size: s });
        }
    }
    if output.is_empty() {
        return Err("empty visible side".into());
    }
    Ok(output)
}

pub fn parse_snapshot(
    venue: Venue,
    expected: &str,
    raw: &str,
    received_ms: i64,
) -> Result<Option<Snapshot>, String> {
    let payload = parse_payload(raw)?;
    parse_depth_value(venue, expected, &payload, received_ms)
}

fn parse_payload(raw: &str) -> Result<Value, String> {
    if raw.len() > 1_048_576 {
        return Err("oversized market message".into());
    }
    let payload: Value = serde_json::from_str(raw).map_err(|_| "invalid market JSON")?;
    if !payload.is_object() {
        return Err("market message must be an object".into());
    }
    if payload.get("error").is_some()
        || payload.get("code").is_some()
        || payload["channel"] == "error"
    {
        return Err("exchange error response".into());
    }
    Ok(payload)
}

fn parse_depth_value(
    venue: Venue,
    expected: &str,
    payload: &Value,
    received_ms: i64,
) -> Result<Option<Snapshot>, String> {
    let (symbol, source, sequence, bids, asks) = match venue {
        Venue::Lighter => return Err("Lighter requires its stateful nonce-checked book".into()),
        Venue::Aster => {
            let data = if let Some(stream) = payload.get("stream") {
                let expected_stream = format!("{}@depth10@100ms", expected.to_ascii_lowercase());
                if stream.as_str() != Some(&expected_stream) {
                    return Err("wrong Aster stream; partial depth required".into());
                }
                &payload["data"]
            } else {
                payload
            };
            if data["e"] != "depthUpdate" {
                return Ok(None);
            }
            let symbol = data["s"].as_str().ok_or("missing symbol")?;
            if !symbol.eq_ignore_ascii_case(expected) {
                return Err("wrong symbol".into());
            }
            let source = data["T"]
                .as_i64()
                .filter(|n| *n > 0)
                .or_else(|| data["E"].as_i64())
                .ok_or("missing source time")?;
            (
                symbol.to_ascii_uppercase(),
                source,
                data["u"].as_u64(),
                levels(&data["b"], false)?,
                levels(&data["a"], false)?,
            )
        }
        Venue::Hyperliquid => {
            if payload["channel"] != "l2Book" {
                return Ok(None);
            }
            let data = &payload["data"];
            let symbol = data["coin"].as_str().ok_or("missing coin")?;
            if symbol != expected {
                return Err("wrong coin or spelling".into());
            }
            let sides = data["levels"].as_array().ok_or("missing sides")?;
            if sides.len() != 2 {
                return Err("expected two book sides".into());
            }
            (
                symbol.to_string(),
                data["time"].as_i64().ok_or("missing source time")?,
                None,
                levels(&sides[0], true)?,
                levels(&sides[1], true)?,
            )
        }
    };
    if source < 0 {
        return Err("negative source time".into());
    }
    let quote = Quote::new(
        bids[0].price,
        asks[0].price,
        received_ms,
        Some(source),
        Depth::Visible(bids),
        Depth::Visible(asks),
    )
    .map_err(|e| e.to_string())?;
    Ok(Some(Snapshot {
        symbol,
        sequence,
        quote,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Update {
    Accepted,
    Ignored,
    OutOfOrder,
}

#[derive(Default)]
pub struct FeedState {
    latest: Option<Snapshot>,
}
impl FeedState {
    pub fn latest(&self) -> Option<&Snapshot> {
        self.latest.as_ref()
    }
    pub fn disconnect(&mut self) {
        self.latest = None;
    }
    pub fn fresh(&self, now: i64) -> Option<&Snapshot> {
        self.latest.as_ref().filter(|s| {
            s.quote.freshness(now, true, FreshnessPolicy::default()) == Freshness::Fresh
        })
    }
    pub fn ingest(
        &mut self,
        venue: Venue,
        symbol: &str,
        raw: &str,
        received: i64,
    ) -> Result<Update, String> {
        let snapshot = match parse_snapshot(venue, symbol, raw, received) {
            Ok(Some(s)) => s,
            Ok(None) => return Ok(Update::Ignored),
            Err(e) => {
                self.disconnect();
                return Err(e);
            }
        };
        if snapshot
            .quote
            .freshness(received, true, FreshnessPolicy::default())
            == Freshness::Future
        {
            self.disconnect();
            return Err("source time exceeds future tolerance".into());
        }
        if let Some(previous) = &self.latest {
            let old_time = snapshot.quote.source_ms() < previous.quote.source_ms();
            let old_sequence = matches!((snapshot.sequence, previous.sequence), (Some(new), Some(old)) if new <= old);
            if old_time || old_sequence {
                return Ok(Update::OutOfOrder);
            }
        }
        // Every message on these streams replaces the whole visible snapshot.
        // In particular, a 5-level update must remove the previous 20-level tail.
        self.latest = Some(snapshot);
        Ok(Update::Accepted)
    }
}
