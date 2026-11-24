use crate::Result;
use chrono::DateTime;
use perp_arb_core::decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_FRAME: usize = 1_048_576;
const WS_ROOT: &str = "wss://omni-ws-server.prod.ap-northeast-1.variational.io";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Private,
    Reference,
    Command,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub signed_quantity: String,
}

/// Fields are deliberately optional. A trade ID is NOT an order ID, quantity
/// may be cumulative or incremental, and a trade is NOT an order terminal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillCandidate {
    pub symbol: Option<String>,
    pub side: Option<String>,
    pub order_id: Option<String>,
    pub trade_id: Option<String>,
    pub quantity: Option<String>,
    pub quote_amount: Option<String>,
    pub price: Option<String>,
    pub status: Option<String>,
    pub trade_type: Option<String>,
    pub instrument: Option<crate::native::Instrument>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Observation {
    Heartbeat,
    Connected,
    Disconnected,
    Gap {
        reason: String,
    },
    Portfolio {
        positions: Vec<Position>,
        #[serde(default)]
        wallet_balance: Option<String>,
    },
    FillCandidate {
        candidate: Box<FillCandidate>,
    },
    /// No response headers, cookies, tokens, URLs or arbitrary account body.
    ReferenceObserved,
    Unclassified {
        keys: Vec<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub channel: Channel,
    pub generation: u64,
    pub received_ms: i64,
    pub captured_ms: Option<i64>,
    /// Relative monotonic time within this Bridge instance; never exchange time.
    pub received_elapsed_us: u64,
    pub parsed_elapsed_us: u64,
    #[serde(flatten)]
    pub observation: Observation,
}

pub fn now_ms() -> Result<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "clock before epoch")?
        .as_millis()
        .try_into()
        .map_err(|_| "clock overflow".into())
}

pub fn symbol(raw: &str) -> Result<String> {
    let mut s = raw.trim().to_uppercase();
    // Strip only the known perpetual suffix. Do not conflate USDT, swaps or
    // multipliers with base tickers by a best-effort string replacement.
    if let Some(base) = s.strip_suffix("-PERP") {
        s = base.to_owned();
    }
    if s.is_empty()
        || s.len() > 80
        || s.ends_with("-SWAP")
        || !s.chars().all(|c| c.is_alphanumeric() || "_-.".contains(c))
    {
        return Err("invalid perpetual symbol".into());
    }
    Ok(s)
}

fn id(v: Option<&Value>) -> Option<String> {
    let value = v?;
    let s = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|n| n.to_string()))?;
    (!s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control)).then_some(s)
}

fn amount(value: &Value, positive: bool) -> Result<String> {
    // Protocol may encode integers as JSON numbers; fractional JSON floats are
    // rejected rather than silently making them binary-float fill quantities.
    let raw = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|n| n.to_string()))
        .ok_or("quantity needs decimal string")?;
    let n = decimal(&raw).map_err(|_| "invalid exact quantity")?;
    if positive && n <= perp_arb_core::Decimal::ZERO {
        return Err("non-positive fill".into());
    }
    Ok(n.normalize().to_string())
}

fn portfolio(v: &Value) -> Result<Vec<Position>> {
    let rows = v["positions"]
        .as_array()
        .ok_or("portfolio positions missing")?;
    if rows.len() > 8192 {
        return Err("portfolio too large".into());
    }
    let mut positions = std::collections::BTreeMap::new();
    for row in rows {
        let info = row.get("position_info").unwrap_or(row);
        let instrument = &info["instrument"];
        let raw = instrument
            .as_str()
            .or_else(|| instrument["underlying"].as_str())
            .or_else(|| instrument["symbol"].as_str())
            .ok_or("position instrument missing")?;
        let s = symbol(raw)?;
        let quantity = amount(&info["qty"], false)?;
        if positions.insert(s, quantity).is_some() {
            return Err("ambiguous duplicate position".into());
        }
    }
    Ok(positions
        .into_iter()
        .map(|(symbol, signed_quantity)| Position {
            symbol,
            signed_quantity,
        })
        .collect())
}

fn candidate(v: &Value) -> Result<FillCandidate> {
    // Existing Python fixtures also contain a single data object on /portfolio.
    // Select one object; never merge outer IDs with a nested trade's quantity.
    let v = if let Some(data) = v.get("data") {
        if !data.is_object()
            || [
                "symbol",
                "underlying",
                "instrument",
                "side",
                "order_id",
                "source_rfq",
                "trade_id",
                "id",
                "qty",
                "base_amount",
                "filled_base_amount",
                "quote_amount",
                "filled_quote_amount",
            ]
            .iter()
            .any(|key| v.get(*key).is_some())
        {
            return Err("ambiguous nested trade layout".into());
        }
        data
    } else {
        v
    };
    let raw_symbol = v.get("symbol").or_else(|| v.get("underlying")).or_else(|| {
        v.get("instrument").and_then(|i| {
            if i.is_string() {
                Some(i)
            } else {
                i.get("underlying").or_else(|| i.get("symbol"))
            }
        })
    });
    let symbol = raw_symbol.and_then(Value::as_str).map(symbol).transpose()?;
    let side = v
        .get("side")
        .and_then(Value::as_str)
        .map(|s| match s.to_uppercase().as_str() {
            "BUY" | "LONG" | "BID" => Ok("BUY".to_owned()),
            "SELL" | "SHORT" | "ASK" => Ok("SELL".to_owned()),
            _ => Err("unknown trade side"),
        })
        .transpose()?;
    let quantity = v
        .get("filled_base_amount")
        .or_else(|| v.get("base_amount"))
        .or_else(|| v.get("qty"))
        .map(|v| amount(v, true))
        .transpose()?;
    let quote_amount = v
        .get("filled_quote_amount")
        .or_else(|| v.get("quote_amount"))
        .map(|v| amount(v, true))
        .transpose()?;
    let mut order_id = id(v.get("order_id"));
    let mut trade_id = id(v.get("trade_id"));
    // Verified REST schema uses source_rfq -> rfq_id; id denotes a TRADE.
    // WS messages remain candidates until session/confirmation semantics and
    // persistent per-trade dedup are checked by the execution owner.
    if v.get("source_rfq").is_some() {
        let rfq = id(v.get("source_rfq")).ok_or("invalid source RFQ")?;
        let trade = id(v.get("id")).ok_or("RFQ trade missing trade ID")?;
        if order_id.as_ref().is_some_and(|s| s != &rfq)
            || trade_id.as_ref().is_some_and(|s| s != &trade)
        {
            return Err("conflicting RFQ/trade identities".into());
        }
        order_id = Some(rfq);
        trade_id = Some(trade);
    }
    Ok(FillCandidate {
        symbol,
        side,
        order_id,
        trade_id,
        quantity,
        quote_amount,
        price: v.get("price").map(|p| amount(p, true)).transpose()?,
        status: v["status"]
            .as_str()
            .filter(|s| s.len() <= 32)
            .map(str::to_owned),
        trade_type: v["trade_type"]
            .as_str()
            .filter(|s| s.len() <= 32)
            .map(str::to_owned),
        instrument: v
            .get("instrument")
            .filter(|i| i.is_object())
            .and_then(|i| serde_json::from_value(i.clone()).ok()),
    })
}

/// Normalize known envelopes only. No recursive search mixing fields from
/// different nested trades. Unknown shapes stay unclassified and cannot hedge.
pub fn decode(channel: Channel, raw: &str, received_ms: i64) -> Result<Option<(i64, Observation)>> {
    if raw.len() > MAX_FRAME {
        return Err("frame too large".into());
    }
    let outer: Value = serde_json::from_str(raw).map_err(|_| "invalid envelope JSON")?;
    let kind = outer["kind"].as_str().ok_or("missing envelope kind")?;
    if channel == Channel::Command {
        return Err("command is not a forwarded frame".into());
    }
    if kind == "ws_frame" && outer["direction"] != "received" {
        return Ok(None);
    }
    let captured = outer["timestamp"]
        .as_str()
        .ok_or("missing capture timestamp")?;
    let captured_ms = DateTime::parse_from_rfc3339(captured)
        .map_err(|_| "invalid capture timestamp")?
        .timestamp_millis();
    let age = i128::from(received_ms) - i128::from(captured_ms);
    if !(-1000..=5000).contains(&age) {
        return Err("queued or future browser capture; fresh baseline required".into());
    }
    let url = outer["url"]
        .as_str()
        .ok_or("missing source URL")?
        .split('?')
        .next()
        .unwrap_or("");
    if channel == Channel::Reference {
        if url != "https://omni.variational.io/api/quotes/indicative" {
            return Err("unexpected reference source".into());
        }
        if kind == "rest_response_error" {
            return Err("reference capture failed".into());
        }
        if kind != "rest_response" || outer["status"].as_u64() != Some(200) {
            return Err("invalid reference response".into());
        }
        return Ok(Some((captured_ms, Observation::ReferenceObserved)));
    }
    let portfolio_url = format!("{WS_ROOT}/portfolio");
    if url != portfolio_url && url != format!("{WS_ROOT}/events") {
        return Err("unexpected private source".into());
    }
    if matches!(kind, "ws_closed" | "ws_frame_error") {
        return Err("upstream private socket lost".into());
    }
    if kind != "ws_frame" {
        return Ok(None);
    }
    if outer["opcode"].as_u64() != Some(1) {
        return Err("unsupported private frame opcode".into());
    }
    let payload = outer["payloadData"]
        .as_str()
        .ok_or("missing private payload")?;
    let v: Value = serde_json::from_str(payload).map_err(|_| "invalid private payload JSON")?;
    let event = if v["type"] == "heartbeat"
        && v.as_object().is_some_and(|o| {
            o.keys()
                .all(|k| ["type", "timestamp"].contains(&k.as_str()))
        }) {
        Observation::Heartbeat
    } else if url == portfolio_url && v.get("positions").is_some() {
        Observation::Portfolio {
            positions: portfolio(&v)?,
            wallet_balance: v["pool_portfolio_result"]
                .get("balance")
                .map(|b| amount(b, false))
                .transpose()?,
        }
    } else if matches!(v["type"].as_str(), Some("trade" | "trade_update")) {
        Observation::FillCandidate {
            candidate: Box::new(candidate(&v)?),
        }
    } else {
        let keys = v
            .as_object()
            .map(|o| {
                o.keys()
                    .take(32)
                    .map(|s| s.chars().take(64).collect())
                    .collect()
            })
            .unwrap_or_default();
        Observation::Unclassified { keys }
    };
    Ok(Some((captured_ms, event)))
}
