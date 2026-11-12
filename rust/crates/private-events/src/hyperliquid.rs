//! Hyperliquid order snapshots and unaggregated trades have different semantics.
//! Trade totals are observations, never added to a REST cumulative baseline.
use crate::{Error, OrderKey, Result, Status, Update};
use perp_arb_core::{decimal, Decimal};
use serde_json::Value;
use std::collections::BTreeMap;

pub const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_ITEMS: usize = 2000;

pub fn address(user: &str) -> Result<String> {
    if user.len() != 42
        || !user.starts_with("0x")
        || !user[2..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(Error::InvalidInput);
    }
    Ok(user.to_ascii_lowercase())
}
fn text<'a>(v: &'a Value, field: &str) -> Result<&'a str> {
    v.get(field)
        .and_then(Value::as_str)
        .ok_or(Error::InvalidInput)
}
fn integer(v: &Value, field: &str) -> Result<u64> {
    v.get(field)
        .and_then(Value::as_u64)
        .ok_or(Error::InvalidInput)
}
fn quantity(v: &Value, field: &str) -> Result<Decimal> {
    decimal(text(v, field)?).map_err(|_| Error::InvalidQuantity)
}
fn key(v: &Value) -> Result<OrderKey> {
    let symbol = text(v, "coin")?;
    if symbol.is_empty() || symbol.len() > 128 || symbol.chars().any(char::is_control) {
        return Err(Error::InvalidInput);
    }
    Ok(OrderKey {
        symbol: symbol.into(),
        order_id: integer(v, "oid")?.to_string(),
    })
}
pub fn parse(raw: &str) -> Result<Value> {
    if raw.len() > MAX_FRAME_BYTES {
        return Err(Error::Capacity);
    }
    let v: Value = serde_json::from_str(raw).map_err(|_| Error::InvalidInput)?;
    if !v.is_object() {
        return Err(Error::InvalidInput);
    }
    Ok(v)
}
fn order(v: &Value) -> Result<Update> {
    let o = v.get("order").ok_or(Error::InvalidInput)?;
    let original = quantity(o, "origSz")?;
    let remaining = quantity(o, "sz")?;
    if remaining < Decimal::ZERO || remaining > original {
        return Err(Error::InvalidQuantity);
    }
    let cumulative = original.checked_sub(remaining).ok_or(Error::Arithmetic)?;
    if original.checked_sub(cumulative) != Some(remaining) {
        return Err(Error::Arithmetic);
    }
    let status = match text(v, "status")? {
        "open" | "triggered" if cumulative == Decimal::ZERO => Status::New,
        "open" | "triggered" => Status::PartiallyFilled,
        "filled" => Status::Filled,
        "canceled"
        | "marginCanceled"
        | "vaultWithdrawalCanceled"
        | "openInterestCapCanceled"
        | "selfTradeCanceled"
        | "reduceOnlyCanceled"
        | "siblingFilledCanceled"
        | "delistedCanceled"
        | "liquidatedCanceled"
        | "scheduledCancel" => Status::Canceled,
        "rejected"
        | "tickRejected"
        | "minTradeNtlRejected"
        | "perpMarginRejected"
        | "reduceOnlyRejected"
        | "badAloPxRejected"
        | "iocCancelRejected"
        | "badTriggerPxRejected"
        | "marketOrderNoLiquidityRejected"
        | "positionIncreaseAtOpenInterestCapRejected"
        | "positionFlipAtOpenInterestCapRejected"
        | "tooAggressiveAtOpenInterestCapRejected"
        | "openInterestIncreaseRejected"
        | "insufficientSpotBalanceRejected"
        | "oracleRejected"
        | "perpMaxPositionRejected" => Status::Rejected,
        _ => return Err(Error::InvalidInput),
    };
    let update = Update {
        key: key(o)?,
        original,
        cumulative,
        status,
        event_ms: Some(integer(v, "statusTimestamp")?),
    };
    update.validate()?;
    Ok(update)
}
pub fn decode_rest(raw: &str) -> Result<Update> {
    let v = parse(raw)?;
    match text(&v, "status")? {
        "unknownOid" => Err(Error::UnknownOrder),
        "order" => order(v.get("order").ok_or(Error::InvalidInput)?),
        _ => Err(Error::InvalidInput),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalKind {
    Liquidation,
    AdlHint,
    SettlementHint,
    SpotDustHint,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    pub key: OrderKey,
    pub trade_id: u64,
    pub quantity: Decimal,
    pub price: Decimal,
    pub side: String,
    pub time_ms: u64,
    pub hash: String,
    /// Routing hint only: never authorizes dropping exposure or hedging.
    pub external_kind: Option<ExternalKind>,
}
impl Fill {
    fn decode(v: &Value) -> Result<Self> {
        let fill = Self {
            key: key(v)?,
            trade_id: integer(v, "tid")?,
            quantity: quantity(v, "sz")?,
            price: quantity(v, "px")?,
            side: text(v, "side")?.into(),
            time_ms: integer(v, "time")?,
            hash: text(v, "hash")?.into(),
            external_kind: match v.get("dir").and_then(Value::as_str) {
                Some("Auto-Deleveraging") => Some(ExternalKind::AdlHint),
                Some("Settlement") => Some(ExternalKind::SettlementHint),
                Some("Spot Dust Conversion") => Some(ExternalKind::SpotDustHint),
                _ if v.get("liquidation").is_some_and(Value::is_object) => {
                    Some(ExternalKind::Liquidation)
                }
                _ => None,
            },
        };
        if fill.quantity <= Decimal::ZERO
            || fill.price <= Decimal::ZERO
            || !matches!(fill.side.as_str(), "A" | "B")
            || fill.hash.len() != 66
            || !fill.hash.starts_with("0x")
            || !fill.hash[2..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(Error::InvalidInput);
        }
        Ok(fill)
    }
}
#[derive(Debug, Clone)]
pub enum Event {
    Orders(Vec<Update>),
    Fills { snapshot: bool, fills: Vec<Fill> },
    AccountChange,
    Other,
}
pub fn decode_ws(raw: &str, user: &str) -> Result<Event> {
    let v = parse(raw)?;
    match text(&v, "channel")? {
        "orderUpdates" => {
            let items = v
                .get("data")
                .and_then(Value::as_array)
                .ok_or(Error::InvalidInput)?;
            if items.len() > MAX_ITEMS {
                return Err(Error::Capacity);
            }
            Ok(Event::Orders(
                items.iter().map(order).collect::<Result<_>>()?,
            ))
        }
        "userFills" => {
            let data = v.get("data").ok_or(Error::InvalidInput)?;
            if address(text(data, "user")?)? != address(user)? {
                return Err(Error::WrongSession);
            }
            let snapshot = match data.get("isSnapshot") {
                None => false,
                Some(v) => v.as_bool().ok_or(Error::InvalidInput)?,
            };
            let items = data
                .get("fills")
                .and_then(Value::as_array)
                .ok_or(Error::InvalidInput)?;
            if items.len() > MAX_ITEMS {
                return Err(Error::Capacity);
            }
            Ok(Event::Fills {
                snapshot,
                fills: items.iter().map(Fill::decode).collect::<Result<_>>()?,
            })
        }
        "userFundings" | "userNonFundingLedgerUpdates" => {
            let data = v
                .get("data")
                .filter(|v| v.is_object())
                .ok_or(Error::InvalidInput)?;
            if address(text(data, "user")?)? != address(user)? {
                return Err(Error::WrongSession);
            }
            // Contents remain evidence for a later inventory query, never order fills.
            let field = if text(&v, "channel")? == "userFundings" {
                "fundings"
            } else {
                "nonFundingLedgerUpdates"
            };
            let rows = data
                .get(field)
                .and_then(Value::as_array)
                .ok_or(Error::InvalidInput)?;
            if rows.len() > MAX_ITEMS {
                return Err(Error::Capacity);
            }
            if rows.iter().any(|r| !r.is_object()) {
                return Err(Error::InvalidInput);
            }
            Ok(Event::AccountChange)
        }
        "error" => Err(Error::InvalidInput),
        _ => Ok(Event::Other),
    }
}

#[derive(Debug, Clone, Default)]
pub struct FillSummary {
    /// All unique observed trades, including historical snapshots. Not an
    /// executable hedge delta and not necessarily complete order history.
    pub observed_quantity: Decimal,
    pub unique_fills: usize,
    pub last_time_ms: u64,
    pub external_kind: Option<ExternalKind>,
}
#[derive(Debug, Clone, Default)]
pub struct IngestReport {
    pub new_fills: usize,
    pub duplicate_fills: usize,
    pub order_updates: usize,
    pub snapshot: bool,
}
#[derive(Debug, Clone)]
pub struct QueryTicket {
    pub(crate) inner: crate::QueryTicket,
    pub(crate) key: OrderKey,
    pub(crate) fill_count: usize,
}
pub(crate) struct FillBook {
    capacity: usize,
    fills: BTreeMap<(OrderKey, u64, u64), Fill>,
    summaries: BTreeMap<OrderKey, FillSummary>,
}
impl FillBook {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            fills: BTreeMap::new(),
            summaries: BTreeMap::new(),
        }
    }
    pub fn summary(&self, key: &OrderKey) -> Option<FillSummary> {
        self.summaries.get(key).cloned()
    }
    pub fn insert(&mut self, fill: Fill) -> Result<bool> {
        let id = (fill.key.clone(), fill.time_ms, fill.trade_id);
        if let Some(old) = self.fills.get(&id) {
            return if old == &fill {
                Ok(false)
            } else {
                Err(Error::IdentityConflict)
            };
        }
        if self.fills.len() >= self.capacity {
            return Err(Error::Capacity);
        }
        let mut summary = self.summaries.get(&fill.key).cloned().unwrap_or_default();
        if summary.unique_fills > 0 && summary.external_kind != fill.external_kind {
            return Err(Error::IdentityConflict);
        }
        summary.external_kind = fill.external_kind.clone();
        let total = summary
            .observed_quantity
            .checked_add(fill.quantity)
            .ok_or(Error::Arithmetic)?;
        if total.checked_sub(summary.observed_quantity) != Some(fill.quantity) {
            return Err(Error::Arithmetic);
        }
        summary.observed_quantity = total;
        summary.unique_fills += 1;
        summary.last_time_ms = summary.last_time_ms.max(fill.time_ms);
        self.summaries.insert(fill.key.clone(), summary);
        self.fills.insert(id, fill);
        Ok(true)
    }
}
