//! Explicitly scoped inventory reconciliation; a snapshot is not an expected ledger.
use crate::{Error, OrderKey, Result, Scope};
use perp_arb_core::{decimal, Decimal};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PositionKey {
    pub symbol: String,
    pub side: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOrder {
    pub side: String,
    pub remaining: Decimal,
    pub price: Decimal,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    pub total: Decimal,
    pub held: Decimal,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Inventory {
    /// Empty string is HL's main perp dex; Aster uses only the empty string.
    pub perp_dexes: BTreeSet<String>,
    pub includes_spot: bool,
    pub includes_wallets: bool,
    pub wallets: BTreeMap<String, Decimal>,
    pub positions: BTreeMap<PositionKey, Decimal>,
    pub balances: BTreeMap<String, Balance>,
    pub open_orders: BTreeMap<OrderKey, OpenOrder>,
}
impl Inventory {
    pub fn validate(&self) -> Result<()> {
        if self.perp_dexes.is_empty()
            || self.perp_dexes.len() > 32
            || self.positions.len() > 8192
            || self.wallets.len() > 8192
            || self.balances.len() > 8192
            || self.open_orders.len() > 8192
        {
            return Err(Error::Capacity);
        }
        if self
            .perp_dexes
            .iter()
            .any(|d| d.len() > 64 || d.chars().any(char::is_control))
        {
            return Err(Error::InvalidInput);
        }
        for (k, q) in &self.positions {
            id(&k.symbol)?;
            if !matches!(k.side.as_str(), "BOTH" | "LONG" | "SHORT")
                || *q == Decimal::ZERO
                || (k.side == "LONG" && *q < Decimal::ZERO)
                || (k.side == "SHORT" && *q > Decimal::ZERO)
            {
                return Err(Error::InvalidQuantity);
            }
            if !self.covers(&k.symbol) {
                return Err(Error::InvalidInput);
            }
        }
        if !self.includes_wallets && !self.wallets.is_empty() {
            return Err(Error::InvalidInput);
        }
        if !self.includes_spot && !self.balances.is_empty() {
            return Err(Error::InvalidInput);
        }
        for asset in self.wallets.keys() {
            id(asset)?;
        }
        for (asset, b) in &self.balances {
            id(asset)?;
            if b.total < Decimal::ZERO || b.held < Decimal::ZERO || b.held > b.total {
                return Err(Error::InvalidQuantity);
            }
        }
        for (k, v) in &self.open_orders {
            id(&k.symbol)?;
            id(&k.order_id)?;
            if !self.covers(&k.symbol)
                || !matches!(v.side.as_str(), "BUY" | "SELL")
                || v.remaining <= Decimal::ZERO
                || v.price < Decimal::ZERO
            {
                return Err(Error::InvalidInput);
            }
        }
        Ok(())
    }
    pub fn covers(&self, symbol: &str) -> bool {
        if symbol.starts_with('@') || symbol.contains('/') {
            self.includes_spot
        } else {
            self.perp_dexes
                .contains(symbol.split_once(':').map_or("", |(dex, _)| dex))
        }
    }
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub scope: Scope,
    pub inventory: Inventory,
    pub started_at: Instant,
    pub completed_at: Instant,
    pub exchange_time_ms: Option<u64>,
}
#[derive(Debug, Clone)]
pub struct Ticket {
    pub(crate) scope: Scope,
    pub(crate) instance: u64,
    pub(crate) generation: u64,
    pub(crate) revision: u64,
    pub(crate) started_at: Instant,
}
#[derive(Debug, Clone)]
pub struct Report {
    pub instance: u64,
    pub scope: Scope,
    pub generation: u64,
    pub revision: u64,
    pub observed_at: Instant,
    pub expected_supplied: bool,
    pub position_differences: usize,
    pub balance_differences: usize,
    pub wallet_differences: usize,
    pub order_differences: usize,
    pub coverage_matches: bool,
    pub external_orders_accounted: usize,
    pub exchange_time_ms: Option<u64>,
    pub(crate) inventory: Inventory,
}
impl Report {
    pub fn matches_expected(&self) -> bool {
        self.expected_supplied
            && self.coverage_matches
            && self.position_differences == 0
            && self.balance_differences == 0
            && self.wallet_differences == 0
            && self.order_differences == 0
    }
    pub fn inventory(&self) -> &Inventory {
        &self.inventory
    }
    pub fn usable(
        &self,
        scope: &Scope,
        health: &crate::Health,
        now: Instant,
        max_age: Duration,
    ) -> bool {
        health.ready_for_new_risk()
            && self.matches_expected()
            && &self.scope == scope
            && self.instance == health.instance
            && self.generation == health.generation
            && self.revision == health.account_revision
            && now
                .checked_duration_since(self.observed_at)
                .is_some_and(|age| age <= max_age)
    }
}
fn differences<K: Ord, V: PartialEq>(a: &BTreeMap<K, V>, b: &BTreeMap<K, V>) -> usize {
    a.keys()
        .chain(b.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| a.get(*key) != b.get(*key))
        .count()
}
pub(crate) fn reconcile(
    ticket: Ticket,
    snapshot: Snapshot,
    expected: Option<Inventory>,
    scope: &Scope,
    instance: u64,
    generation: u64,
    revision: u64,
) -> Result<Report> {
    snapshot.inventory.validate()?;
    let now = Instant::now();
    if &ticket.scope != scope
        || &snapshot.scope != scope
        || ticket.instance != instance
        || ticket.generation != generation
        || ticket.revision != revision
        || snapshot.started_at < ticket.started_at
        || snapshot.completed_at < snapshot.started_at
        || snapshot.completed_at > now
        || now.duration_since(ticket.started_at) > Duration::from_secs(30)
    {
        return Err(Error::WrongSession);
    }
    let mut report = Report {
        scope: scope.clone(),
        instance,
        generation,
        revision,
        observed_at: snapshot.started_at,
        expected_supplied: expected.is_some(),
        position_differences: 0,
        balance_differences: 0,
        wallet_differences: 0,
        order_differences: 0,
        coverage_matches: false,
        external_orders_accounted: 0,
        exchange_time_ms: snapshot.exchange_time_ms,
        inventory: snapshot.inventory,
    };
    if let Some(expected) = expected {
        expected.validate()?;
        report.coverage_matches = expected.perp_dexes == report.inventory.perp_dexes
            && expected.includes_spot == report.inventory.includes_spot
            && expected.includes_wallets == report.inventory.includes_wallets;
        report.position_differences = differences(&expected.positions, &report.inventory.positions);
        report.wallet_differences = differences(&expected.wallets, &report.inventory.wallets);
        report.balance_differences = differences(&expected.balances, &report.inventory.balances);
        report.order_differences =
            differences(&expected.open_orders, &report.inventory.open_orders);
    }
    Ok(report)
}
fn id(s: &str) -> Result<()> {
    if s.is_empty() || s.len() > 128 || s.chars().any(char::is_control) {
        Err(Error::InvalidInput)
    } else {
        Ok(())
    }
}
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v.get(k).and_then(Value::as_str).ok_or(Error::InvalidInput)
}
fn qty(v: &Value, k: &str) -> Result<Decimal> {
    decimal(text(v, k)?).map_err(|_| Error::InvalidQuantity)
}
fn array(v: &Value) -> Result<&Vec<Value>> {
    let rows = v.as_array().ok_or(Error::InvalidInput)?;
    if rows.len() > 8192 {
        return Err(Error::Capacity);
    }
    Ok(rows)
}
fn oid(v: &Value, k: &str) -> Result<String> {
    match v.get(k) {
        Some(Value::Number(n)) if n.as_u64().is_some() => Ok(n.to_string()),
        Some(Value::String(s)) if s.parse::<u64>().is_ok() => Ok(s.clone()),
        _ => Err(Error::InvalidInput),
    }
}
fn insert_position(
    inv: &mut Inventory,
    seen: &mut BTreeSet<PositionKey>,
    key: PositionKey,
    q: Decimal,
) -> Result<()> {
    id(&key.symbol)?;
    if !matches!(key.side.as_str(), "BOTH" | "LONG" | "SHORT")
        || (key.side == "LONG" && q < Decimal::ZERO)
        || (key.side == "SHORT" && q > Decimal::ZERO)
    {
        return Err(Error::InvalidQuantity);
    }
    if !seen.insert(key.clone()) {
        return Err(Error::IdentityConflict);
    }
    if q != Decimal::ZERO {
        inv.positions.insert(key, q);
    }
    Ok(())
}
pub fn aster(positions: &Value, orders: &Value) -> Result<Inventory> {
    let mut inv = Inventory {
        perp_dexes: BTreeSet::from([String::new()]),
        ..Inventory::default()
    };
    let mut seen = BTreeSet::new();
    for p in array(positions)? {
        insert_position(
            &mut inv,
            &mut seen,
            PositionKey {
                symbol: text(p, "symbol")?.into(),
                side: text(p, "positionSide")?.into(),
            },
            qty(p, "positionAmt")?,
        )?;
    }
    for o in array(orders)? {
        if !matches!(text(o, "status")?, "NEW" | "PARTIALLY_FILLED") {
            return Err(Error::InvalidInput);
        }
        let original = qty(o, "origQty")?;
        let filled = qty(o, "executedQty")?;
        let remaining = original.checked_sub(filled).ok_or(Error::Arithmetic)?;
        if original <= Decimal::ZERO
            || filled < Decimal::ZERO
            || original.checked_sub(remaining) != Some(filled)
        {
            return Err(Error::InvalidQuantity);
        }
        let key = OrderKey {
            symbol: text(o, "symbol")?.into(),
            order_id: oid(o, "orderId")?,
        };
        let value = OpenOrder {
            side: text(o, "side")?.into(),
            remaining,
            price: qty(o, "price")?,
        };
        if inv.open_orders.insert(key, value).is_some() {
            return Err(Error::IdentityConflict);
        }
    }
    inv.validate()?;
    Ok(inv)
}
pub fn aster_with_balances(
    positions: &Value,
    orders: &Value,
    balances: &Value,
) -> Result<Inventory> {
    let mut inv = aster(positions, orders)?;
    inv.includes_wallets = true;
    for b in array(balances)? {
        let asset = text(b, "asset")?;
        if inv
            .wallets
            .insert(asset.into(), qty(b, "balance")?)
            .is_some()
        {
            return Err(Error::IdentityConflict);
        }
    }
    inv.validate()?;
    Ok(inv)
}
/// Caller fetches every explicitly requested dex; missing coverage is an error.
pub fn hyperliquid(perps: &[(String, Value, Value)], spot: Option<&Value>) -> Result<Inventory> {
    let mut inv = Inventory {
        includes_spot: spot.is_some(),
        includes_wallets: true,
        ..Inventory::default()
    };
    let mut seen = BTreeSet::new();
    for (dex, state, orders) in perps {
        if !inv.perp_dexes.insert(dex.clone()) {
            return Err(Error::IdentityConflict);
        }
        let margin = state.get("marginSummary").ok_or(Error::InvalidInput)?;
        inv.wallets
            .insert(format!("perp:{dex}:rawUsd"), qty(margin, "totalRawUsd")?);
        for item in array(state.get("assetPositions").ok_or(Error::InvalidInput)?)? {
            let p = item.get("position").ok_or(Error::InvalidInput)?;
            let coin = text(p, "coin")?;
            if coin.split_once(':').map_or("", |(d, _)| d) != dex {
                return Err(Error::IdentityConflict);
            }
            insert_position(
                &mut inv,
                &mut seen,
                PositionKey {
                    symbol: coin.into(),
                    side: "BOTH".into(),
                },
                qty(p, "szi")?,
            )?;
        }
        for o in array(orders)? {
            let coin = text(o, "coin")?;
            if coin.starts_with('@') || coin.contains('/') {
                if !dex.is_empty() {
                    return Err(Error::IdentityConflict);
                }
            } else if coin.split_once(':').map_or("", |(d, _)| d) != dex {
                return Err(Error::IdentityConflict);
            }
            let key = OrderKey {
                symbol: coin.into(),
                order_id: oid(o, "oid")?,
            };
            let side = match text(o, "side")? {
                "A" => "SELL",
                "B" => "BUY",
                _ => return Err(Error::InvalidInput),
            };
            if inv
                .open_orders
                .insert(
                    key,
                    OpenOrder {
                        side: side.into(),
                        remaining: qty(o, "sz")?,
                        price: qty(o, "limitPx")?,
                    },
                )
                .is_some()
            {
                return Err(Error::IdentityConflict);
            }
        }
    }
    if let Some(spot) = spot {
        for b in array(spot.get("balances").ok_or(Error::InvalidInput)?)? {
            // Numeric token ID distinguishes same-name tokens; do not net by ticker.
            let token = oid(b, "token")?;
            let total = qty(b, "total")?;
            let hold = qty(b, "hold")?;
            if total < Decimal::ZERO || hold < Decimal::ZERO || hold > total {
                return Err(Error::InvalidQuantity);
            }
            if inv
                .balances
                .insert(token, Balance { total, held: hold })
                .is_some()
            {
                return Err(Error::IdentityConflict);
            }
        }
    }
    inv.validate()?;
    Ok(inv)
}
