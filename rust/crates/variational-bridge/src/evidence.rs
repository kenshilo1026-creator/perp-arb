//! Conservative observation state. Portfolio is diagnostic, not account/order
//! ownership proof. Raw fill candidates never automatically authorize a hedge.
use crate::{
    wire::{Channel, Event, FillCandidate, Observation, Position},
    Result,
};
use perp_arb_core::{decimal, Decimal};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct Evidence {
    generation: u64,
    portfolio: Option<(i64, Vec<Position>)>,
    last_capture_ms: Option<i64>,
    pub fill_candidates: u64,
    pub unclassified: u64,
}
impl Evidence {
    pub fn apply(&mut self, e: &Event) -> Result<()> {
        if e.channel != Channel::Private {
            return Ok(());
        }
        if e.generation < self.generation {
            return Err("old bridge connection generation".into());
        }
        if e.generation > self.generation {
            self.generation = e.generation;
            self.portfolio = None;
            self.last_capture_ms = None;
        }
        match &e.observation {
            Observation::Connected | Observation::Disconnected | Observation::Gap { .. } => {
                self.portfolio = None;
                self.last_capture_ms = None;
            }
            Observation::Portfolio { positions, .. } => {
                let captured = e.captured_ms.ok_or("missing capture time")?;
                if self.last_capture_ms.is_some_and(|last| captured < last) {
                    self.portfolio = None;
                    return Err("portfolio capture moved backwards".into());
                }
                if self.last_capture_ms == Some(captured)
                    && self
                        .portfolio
                        .as_ref()
                        .is_some_and(|(_, old)| old != positions)
                {
                    self.portfolio = None;
                    return Err("conflicting portfolio at same capture time".into());
                }
                self.last_capture_ms = Some(captured);
                self.portfolio = Some((captured, positions.clone()));
            }
            Observation::FillCandidate { .. } => {
                self.fill_candidates += 1;
            }
            Observation::Unclassified { .. } => {
                self.unclassified += 1;
            }
            _ => (),
        }
        Ok(())
    }
    pub fn portfolio(&self, now_ms: i64) -> Option<&[Position]> {
        let (captured, positions) = self.portfolio.as_ref()?;
        let age = i128::from(now_ms) - i128::from(*captured);
        (0..=5000).contains(&age).then_some(positions.as_slice())
    }
}

/// Offline binding primitive for a future verified trade stream. Caller must
/// establish that the feed belongs to the right account and quantities are
/// incremental executions. The live probe intentionally never calls this.
pub struct BoundTrades {
    symbol: String,
    side: String,
    order_id: String,
    requested: Decimal,
    cumulative: Decimal,
    trades: BTreeMap<String, FillCandidate>,
    faulted: bool,
}
impl BoundTrades {
    pub fn new(symbol: &str, side: &str, order_id: &str, quantity: &str) -> Result<Self> {
        let requested = decimal(quantity).map_err(|_| "invalid requested quantity")?;
        if requested <= Decimal::ZERO
            || !matches!(side, "BUY" | "SELL")
            || order_id.is_empty()
            || order_id.len() > 128
            || order_id.chars().any(char::is_control)
        {
            return Err("invalid binding".into());
        }
        Ok(Self {
            symbol: crate::wire::symbol(symbol)?,
            side: side.into(),
            order_id: order_id.into(),
            requested,
            cumulative: Decimal::ZERO,
            trades: BTreeMap::new(),
            faulted: false,
        })
    }
    pub fn cumulative(&self) -> Decimal {
        self.cumulative
    }
    pub fn requires_reconciliation(&self) -> bool {
        self.faulted
    }
    pub fn apply_verified_incremental(&mut self, fill: FillCandidate) -> Result<Decimal> {
        if self.faulted {
            return Err("trade binding requires reconciliation".into());
        }
        let result = self.apply(fill);
        if result.is_err() {
            self.faulted = true;
        }
        result
    }
    fn apply(&mut self, fill: FillCandidate) -> Result<Decimal> {
        if fill.symbol.as_deref() != Some(&self.symbol)
            || fill.side.as_deref() != Some(&self.side)
            || fill.order_id.as_deref() != Some(&self.order_id)
        {
            return Err("trade identity missing or mismatched".into());
        }
        let id = fill
            .trade_id
            .clone()
            .filter(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control))
            .ok_or("unique trade ID required")?;
        if let Some(old) = self.trades.get(&id) {
            return if old == &fill {
                Ok(Decimal::ZERO)
            } else {
                Err("conflicting duplicate trade".into())
            };
        }
        let quantity = decimal(fill.quantity.as_deref().ok_or("trade quantity missing")?)
            .map_err(|_| "invalid trade quantity")?;
        let next = self
            .cumulative
            .checked_add(quantity)
            .ok_or("quantity overflow")?;
        if quantity <= Decimal::ZERO
            || next > self.requested
            || next.checked_sub(self.cumulative) != Some(quantity)
        {
            return Err("trade overfill or invalid quantity".into());
        }
        if self.trades.len() >= 8192 {
            return Err("trade dedup capacity reached; do not evict IDs".into());
        }
        self.trades.insert(id, fill);
        self.cumulative = next;
        Ok(quantity)
    }
}
