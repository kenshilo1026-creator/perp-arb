use crate::{number, Error, Result};
use perp_arb_core::Decimal;
use perp_arb_private_events::{
    lookup::{Found, Request},
    OrderKey, Scope, Status, Update,
};
use serde_json::Value;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub symbol: String,
    pub order_id: String,
    pub id: u64,
    pub side: String,
    pub quantity: Decimal,
    pub price: Decimal,
    pub quote: Decimal,
    pub commission: Decimal,
    pub commission_asset: Option<String>,
}
#[derive(Debug, Clone)]
pub struct OrderEvidence {
    pub found: Found,
    pub quote: Decimal,
    pub trade: Option<Trade>,
    pub received_at: Instant,
}
#[derive(Debug, Clone)]
pub enum Event {
    Order(Box<OrderEvidence>),
    AccountChanged,
}
pub(crate) fn text(v: &Value, key: &str) -> Result<String> {
    v[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or(Error::Protocol)
}
pub(crate) fn amount(v: &Value, key: &str) -> Result<Decimal> {
    let n = number(v[key].as_str().ok_or(Error::Protocol)?)?;
    if n < Decimal::ZERO {
        return Err(Error::Protocol);
    }
    Ok(n)
}
fn id(v: &Value, key: &str) -> Result<String> {
    v[key]
        .as_u64()
        .map(|n| n.to_string())
        .ok_or(Error::Protocol)
}
fn status(s: &str) -> Result<Status> {
    Ok(match s {
        "PENDING_NEW" => Status::Pending,
        "NEW" => Status::New,
        "PARTIALLY_FILLED" => Status::PartiallyFilled,
        "FILLED" => Status::Filled,
        "CANCELED" => Status::Canceled,
        "REJECTED" => Status::Rejected,
        "EXPIRED" | "EXPIRED_IN_MATCH" => Status::Expired,
        _ => return Err(Error::Protocol),
    })
}
#[allow(clippy::too_many_arguments)] // Both fixed wire schemas normalize into the same checked evidence.
fn found(
    scope: &Scope,
    symbol: String,
    order_id: String,
    side: String,
    client: String,
    original: Decimal,
    cumulative: Decimal,
    state: &str,
    time: u64,
) -> Result<Found> {
    if scope.venue != "binance_spot"
        || !crate::orders::symbol(&symbol)
        || !matches!(side.as_str(), "BUY" | "SELL")
        || client.is_empty()
    {
        return Err(Error::Identity);
    }
    let order = Update {
        key: OrderKey { symbol, order_id },
        original,
        cumulative,
        status: status(state)?,
        event_ms: Some(time),
    };
    order.validate().map_err(|_| Error::Protocol)?;
    Ok(Found {
        scope: scope.clone(),
        side,
        client_id: Some(client),
        order,
    })
}
pub fn rest(request: &Request, v: &Value, received_at: Instant) -> Result<OrderEvidence> {
    let client = v
        .get("origClientOrderId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or(text(v, "clientOrderId")?);
    let f = found(
        &request.scope,
        text(v, "symbol")?,
        id(v, "orderId")?,
        text(v, "side")?,
        client,
        amount(v, "origQty")?,
        amount(v, "executedQty")?,
        v["status"].as_str().ok_or(Error::Protocol)?,
        v["updateTime"]
            .as_u64()
            .or_else(|| v["transactTime"].as_u64())
            .ok_or(Error::Protocol)?,
    )?;
    request.verify(&f).map_err(|_| Error::Identity)?;
    let quote = amount(v, "cummulativeQuoteQty")?;
    if (f.order.cumulative == Decimal::ZERO) != (quote == Decimal::ZERO) {
        return Err(Error::Protocol);
    }
    Ok(OrderEvidence {
        found: f,
        quote,
        trade: None,
        received_at,
    })
}
pub fn stream(scope: &Scope, subscription: u64, v: &Value, received_at: Instant) -> Result<Event> {
    if v["subscriptionId"].as_u64() != Some(subscription) {
        return Err(Error::Identity);
    }
    let e = &v["event"];
    match e["e"].as_str() {
        Some("executionReport") => {
            let client = e["C"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .unwrap_or(text(e, "c")?);
            let f = found(
                scope,
                text(e, "s")?,
                id(e, "i")?,
                text(e, "S")?,
                client,
                amount(e, "q")?,
                amount(e, "z")?,
                e["X"].as_str().ok_or(Error::Protocol)?,
                e["T"].as_u64().ok_or(Error::Protocol)?,
            )?;
            let quote = amount(e, "Z")?;
            if (f.order.cumulative == Decimal::ZERO) != (quote == Decimal::ZERO) {
                return Err(Error::Protocol);
            }
            let trade = if e["x"] == "TRADE" {
                let quantity = amount(e, "l")?;
                let price = amount(e, "L")?;
                let commission = amount(e, "n")?;
                let asset = e["N"].as_str().filter(|s| !s.is_empty()).map(str::to_owned);
                if quantity <= Decimal::ZERO
                    || price <= Decimal::ZERO
                    || quantity > f.order.cumulative
                    || commission > Decimal::ZERO && asset.is_none()
                {
                    return Err(Error::Protocol);
                }
                Some(Trade {
                    symbol: f.order.key.symbol.clone(),
                    order_id: f.order.key.order_id.clone(),
                    id: e["t"].as_u64().ok_or(Error::Protocol)?,
                    side: f.side.clone(),
                    quantity,
                    price,
                    quote: amount(e, "Y")?,
                    commission,
                    commission_asset: asset,
                })
            } else {
                None
            };
            Ok(Event::Order(Box::new(OrderEvidence {
                found: f,
                quote,
                trade,
                received_at,
            })))
        }
        Some("eventStreamTerminated") => Err(Error::StreamEnded),
        Some(_) => Ok(Event::AccountChanged), // Unknown account events invalidate account proof.
        None => Err(Error::Protocol),
    }
}
pub fn trade(request: &Request, v: &Value) -> Result<Trade> {
    if v["symbol"] != request.symbol || v["isBuyer"].as_bool() != Some(request.side == "BUY") {
        return Err(Error::Identity);
    }
    let order_id = id(v, "orderId")?;
    if let perp_arb_private_events::lookup::Selector::OrderId(expected) = &request.selector {
        if &order_id != expected {
            return Err(Error::Identity);
        }
    } else {
        return Err(Error::Configuration);
    }
    let result = Trade {
        symbol: request.symbol.clone(),
        order_id,
        id: v["id"].as_u64().ok_or(Error::Protocol)?,
        side: request.side.clone(),
        quantity: amount(v, "qty")?,
        price: amount(v, "price")?,
        quote: amount(v, "quoteQty")?,
        commission: amount(v, "commission")?,
        commission_asset: Some(text(v, "commissionAsset")?),
    };
    if result.quantity <= Decimal::ZERO
        || result.price <= Decimal::ZERO
        || result.quote <= Decimal::ZERO
    {
        return Err(Error::Protocol);
    }
    Ok(result)
}
