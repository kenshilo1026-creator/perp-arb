//! Aster cumulative order normalization. Transport/account authentication is
//! supplied by a future adapter; this module never opens a socket.
use crate::{Error, OrderKey, Result, Status, Update};
use perp_arb_core::{decimal, Decimal};
use serde_json::Value;

pub const MAX_FRAME_BYTES: usize = 65_536;

fn text<'a>(v: &'a Value, field: &str) -> Result<&'a str> {
    v.get(field)
        .and_then(Value::as_str)
        .ok_or(Error::InvalidInput)
}
fn quantity(v: &Value, field: &str) -> Result<Decimal> {
    // Decimal JSON strings only: never route money through binary float.
    decimal(text(v, field)?).map_err(|_| Error::InvalidQuantity)
}
fn order_id(v: &Value, field: &str) -> Result<String> {
    match v.get(field) {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        Some(Value::Number(n)) if n.as_u64().is_some() => Ok(n.to_string()),
        _ => Err(Error::InvalidInput),
    }
}
fn status(s: &str) -> Result<Status> {
    match s {
        "NEW" => Ok(Status::New),
        "PARTIALLY_FILLED" => Ok(Status::PartiallyFilled),
        "FILLED" => Ok(Status::Filled),
        "CANCELED" => Ok(Status::Canceled),
        "EXPIRED" | "EXPIRED_IN_MATCH" => Ok(Status::Expired),
        "REJECTED" => Ok(Status::Rejected),
        _ => Err(Error::InvalidInput),
    }
}
fn parse(raw: &str) -> Result<Value> {
    if raw.len() > MAX_FRAME_BYTES {
        return Err(Error::InvalidInput);
    }
    let v: Value = serde_json::from_str(raw).map_err(|_| Error::InvalidInput)?;
    if !v.is_object() {
        return Err(Error::InvalidInput);
    }
    Ok(v)
}

/// Unrelated account events are ignored; malformed order events are errors,
/// not empty/no-fill responses. A subscription acknowledgement is not a fill.
pub fn decode_ws(raw: &str) -> Result<Option<Update>> {
    let v = parse(raw)?;
    match text(&v, "e")? {
        "listenKeyExpired" => return Err(Error::StreamExpired),
        "ORDER_TRADE_UPDATE" => (),
        _ => return Ok(None),
    }
    let o = v.get("o").ok_or(Error::InvalidInput)?;
    let update = Update {
        key: OrderKey {
            symbol: text(o, "s")?.into(),
            order_id: order_id(o, "i")?,
        },
        original: quantity(o, "q")?,
        cumulative: quantity(o, "z")?,
        status: status(text(o, "X")?)?,
        event_ms: Some(
            v.get("E")
                .and_then(Value::as_u64)
                .ok_or(Error::InvalidInput)?,
        ),
    };
    update.validate()?;
    Ok(Some(update))
}

/// A full cumulative order-status reply, not an order submission/cancel ACK.
/// Missing fields and HTTP error bodies must never become zero-fill records.
pub fn decode_rest(raw: &str) -> Result<Update> {
    let v = parse(raw)?;
    let event_ms = match v.get("updateTime") {
        Some(value) => Some(value.as_u64().ok_or(Error::InvalidInput)?),
        None => None,
    };
    let update = Update {
        key: OrderKey {
            symbol: text(&v, "symbol")?.into(),
            order_id: order_id(&v, "orderId")?,
        },
        original: quantity(&v, "origQty")?,
        cumulative: quantity(&v, "executedQty")?,
        status: status(text(&v, "status")?)?,
        event_ms,
    };
    update.validate()?;
    Ok(update)
}
