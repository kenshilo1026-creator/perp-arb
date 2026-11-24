//! Per-order trade-ID dedup and spot inventory delta, separately from gross fills.
use crate::{events::Trade, Error, Result};
use perp_arb_core::Decimal;
use std::collections::BTreeMap;
pub struct Fees {
    symbol: String,
    order_id: String,
    side: String,
    trades: BTreeMap<u64, Trade>,
    faulted: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Totals {
    pub gross: Decimal,
    pub quote: Decimal,
    pub base_delta: Decimal,
    pub commissions: BTreeMap<String, Decimal>,
}
impl Fees {
    pub fn new(symbol: &str, order_id: &str, side: &str) -> Result<Self> {
        if !crate::orders::symbol(symbol)
            || order_id.parse::<u64>().is_err()
            || !matches!(side, "BUY" | "SELL")
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            symbol: symbol.into(),
            order_id: order_id.into(),
            side: side.into(),
            trades: BTreeMap::new(),
            faulted: false,
        })
    }
    pub fn ingest(&mut self, t: Trade) -> Result<bool> {
        if self.faulted {
            return Err(Error::Poisoned);
        }
        let result = self.ingest_checked(t);
        if result.is_err() {
            self.faulted = true;
        }
        result
    }
    fn ingest_checked(&mut self, t: Trade) -> Result<bool> {
        if t.symbol != self.symbol || t.order_id != self.order_id || t.side != self.side {
            return Err(Error::Identity);
        }
        if t.quantity <= Decimal::ZERO
            || t.price <= Decimal::ZERO
            || t.quote <= Decimal::ZERO
            || t.commission < Decimal::ZERO
            || t.commission > Decimal::ZERO && t.commission_asset.is_none()
        {
            return Err(Error::Protocol);
        }
        if let Some(old) = self.trades.get(&t.id) {
            return if old == &t {
                Ok(false)
            } else {
                Err(Error::Identity)
            };
        }
        if self.trades.len() >= 10000 {
            return Err(Error::Capacity);
        }
        self.trades.insert(t.id, t);
        Ok(true)
    }
    /// Requires exact gross and quote coverage. Missing commission evidence must
    /// never silently become zero fee (REST order responses contain no fees).
    pub fn reconcile(&self, gross: Decimal, quote: Decimal) -> Result<Totals> {
        if self.faulted {
            return Err(Error::Poisoned);
        }
        let mut q = Decimal::ZERO;
        let mut value = Decimal::ZERO;
        let mut commissions = BTreeMap::new();
        for t in self.trades.values() {
            q = exact_add(q, t.quantity)?;
            value = exact_add(value, t.quote)?;
            if let Some(asset) = &t.commission_asset {
                let sum = commissions.entry(asset.clone()).or_insert(Decimal::ZERO);
                *sum = exact_add(*sum, t.commission)?;
            }
        }
        if q != gross || value != quote {
            return Err(Error::Protocol);
        }
        let base = self
            .symbol
            .strip_suffix("USDT")
            .ok_or(Error::Configuration)?;
        let fee = commissions.get(base).copied().unwrap_or(Decimal::ZERO);
        let delta = if self.side == "BUY" { q } else { -q };
        Ok(Totals {
            gross: q,
            quote: value,
            base_delta: exact_add(delta, -fee)?,
            commissions,
        })
    }
}
fn exact_add(a: Decimal, b: Decimal) -> Result<Decimal> {
    let result = a.checked_add(b).ok_or(Error::Arithmetic)?;
    if result.checked_sub(a) != Some(b) || result.checked_sub(b) != Some(a) {
        return Err(Error::Arithmetic);
    }
    Ok(result)
}
