//! Paper execution coordination, with opt-in durable journal. No broker or signing.
//! A reserved intent is an unknown submission until cumulative private evidence arrives.
#![forbid(unsafe_code)]
pub mod durable;
pub mod selection;
use perp_arb_core::Decimal;
use perp_arb_private_events::{
    account::Report, actor::Coordinator, Health, OrderKey, OrderSnapshot, Scope, Update,
};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}
impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Leg {
    pub scope: Scope,
    pub symbol: String,
    pub side: Side,
}
#[derive(Debug, Clone)]
pub struct Plan {
    /// Run identifier. DurableEngine additionally namespaces client IDs with a persisted nonce.
    pub run_id: String,
    pub maker: Leg,
    pub hedge: Leg,
    pub maker_order: OrderKey,
    /// Both legs must be expressed in the same base units (no contract multiplier).
    pub quantity: Decimal,
    pub step: Decimal,
    pub minimum: Decimal,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    NewRiskBlocked,
    Identity,
    InvalidEvidence,
    Capacity,
    Arithmetic,
    Faulted,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub id: String,
    pub quantity: Decimal,
    pub side: Side,
}
#[derive(Clone)]
struct Hedge {
    intent: Intent,
    key: Option<OrderKey>,
    cumulative: Decimal,
    revision: Option<u64>,
    terminal: bool,
}
#[derive(Clone)]
pub struct Engine {
    plan: Plan,
    maker: Decimal,
    maker_revision: Option<u64>,
    maker_terminal: bool,
    hedges: BTreeMap<String, Hedge>,
    sequence: u64,
    fault: bool,
}

fn add(a: Decimal, b: Decimal) -> Result<Decimal> {
    let n = a.checked_add(b).ok_or(Error::Arithmetic)?;
    if n.checked_sub(a) != Some(b) {
        return Err(Error::Arithmetic);
    }
    Ok(n)
}
fn sub(a: Decimal, b: Decimal) -> Result<Decimal> {
    let n = a.checked_sub(b).ok_or(Error::Arithmetic)?;
    if n.checked_add(b) != Some(a) {
        return Err(Error::Arithmetic);
    }
    Ok(n)
}
fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn gate(leg: &Leg, r: &Report, h: &Health, now: Instant, age: Duration) -> bool {
    r.usable(&leg.scope, h, now, age)
        && match &r.position_only {
            Some(symbol) => symbol == &leg.symbol,
            None => r.inventory().includes_wallets,
        }
        && r.inventory().covers(&leg.symbol)
        && r.inventory().open_orders.is_empty()
}
impl Engine {
    /// Price/account gates apply before new maker risk, never to a known hedge obligation.
    pub fn arm(
        plan: Plan,
        maker: (&Report, &Health),
        hedge: (&Report, &Health),
        price_ok: bool,
        max_age: Duration,
    ) -> Result<Self> {
        Self::validate_plan(&plan)?;
        if max_age.is_zero() || max_age > Duration::from_secs(30) {
            return Err(Error::Configuration);
        }
        let now = Instant::now();
        if !price_ok
            || !gate(&plan.maker, maker.0, maker.1, now, max_age)
            || !gate(&plan.hedge, hedge.0, hedge.1, now, max_age)
        {
            return Err(Error::NewRiskBlocked);
        }
        Ok(Self {
            plan,
            maker: Decimal::ZERO,
            maker_revision: None,
            maker_terminal: false,
            hedges: BTreeMap::new(),
            sequence: 0,
            fault: false,
        })
    }
    fn validate_plan(plan: &Plan) -> Result<()> {
        if !valid_id(&plan.run_id)
            || plan.maker.scope == plan.hedge.scope
            || plan.maker.symbol != plan.maker_order.symbol
            || plan.maker.symbol.is_empty()
            || plan.hedge.symbol.is_empty()
            || plan.maker_order.order_id.is_empty()
            || plan.maker.side.opposite() != plan.hedge.side
            || plan.quantity <= Decimal::ZERO
            || plan.step <= Decimal::ZERO
            || plan.minimum <= Decimal::ZERO
            || plan.quantity < plan.minimum
            || plan.quantity.checked_rem(plan.step) != Some(Decimal::ZERO)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    fn replay_new(plan: Plan) -> Result<Self> {
        Self::validate_plan(&plan)?;
        Ok(Self {
            plan,
            maker: Decimal::ZERO,
            maker_revision: None,
            maker_terminal: false,
            hedges: BTreeMap::new(),
            sequence: 0,
            fault: false,
        })
    }
    /// Uses the same cumulative snapshots delivered by the live private coordinator.
    /// No submission occurs here; the caller must retain the returned receiver.
    pub async fn maker_stream(
        &self,
        c: &Coordinator,
    ) -> Result<tokio::sync::watch::Receiver<OrderSnapshot>> {
        if c.scope() != &self.plan.maker.scope {
            return Err(Error::Identity);
        }
        c.subscribe(self.plan.maker_order.clone())
            .await
            .map_err(|_| Error::InvalidEvidence)
    }
    pub async fn consume_maker(&mut self, c: &Coordinator) -> Result<()> {
        if c.scope() != &self.plan.maker.scope {
            return Err(Error::Identity);
        }
        let mut stream = c
            .subscribe(self.plan.maker_order.clone())
            .await
            .map_err(|_| Error::InvalidEvidence)?;
        let snapshot = stream.borrow_and_update().clone();
        self.observe_maker(&snapshot)
    }
    /// The hedge has the same private cumulative watch path after secure ACK correlation.
    pub async fn hedge_stream(
        &self,
        id: &str,
        c: &Coordinator,
    ) -> Result<tokio::sync::watch::Receiver<OrderSnapshot>> {
        if c.scope() != &self.plan.hedge.scope {
            return Err(Error::Identity);
        }
        let key = self
            .hedges
            .get(id)
            .and_then(|h| h.key.clone())
            .ok_or(Error::Identity)?;
        c.subscribe(key).await.map_err(|_| Error::InvalidEvidence)
    }
    pub fn observe_maker(&mut self, s: &OrderSnapshot) -> Result<()> {
        if s.scope != self.plan.maker.scope
            || s.key != self.plan.maker_order
            || s.original != self.plan.quantity
        {
            return Err(Error::Identity);
        }
        validate(s)?;
        if self.maker_revision.is_some_and(|r| s.revision < r) {
            return Ok(());
        }
        if s.cumulative < self.maker {
            self.fault = true;
            return Err(Error::InvalidEvidence);
        }
        self.maker = s.cumulative;
        self.maker_revision = Some(s.revision);
        self.maker_terminal = s.status.terminal() && !s.requires_reconciliation;
        Ok(())
    }
    pub fn maker_terminal_confirmed(&self) -> bool {
        self.maker_terminal
    }
    pub fn confirmed_hedge(&self) -> Result<Decimal> {
        self.hedges
            .values()
            .try_fold(Decimal::ZERO, |a, h| add(a, h.cumulative))
    }
    pub fn in_flight(&self) -> Result<Decimal> {
        self.hedges.values().try_fold(Decimal::ZERO, |a, h| {
            add(
                a,
                if h.terminal {
                    Decimal::ZERO
                } else {
                    sub(h.intent.quantity, h.cumulative)?
                },
            )
        })
    }
    /// Signed residual: negative means overhedged, and must not be hidden as zero.
    pub fn exposure(&self) -> Result<Decimal> {
        sub(self.maker, self.confirmed_hedge()?)
    }
    /// Reserve BEFORE awaiting any future broker. Unknown/timeout keeps this amount reserved.
    /// No price/quote-health argument: a filled maker must remain hedgeable.
    pub fn reserve_hedge(&mut self) -> Result<Option<Intent>> {
        if self.fault {
            return Err(Error::Faulted);
        }
        let available = sub(self.exposure()?, self.in_flight()?)?;
        if available < Decimal::ZERO {
            self.fault = true;
            return Err(Error::InvalidEvidence);
        }
        let remainder = available
            .checked_rem(self.plan.step)
            .ok_or(Error::Arithmetic)?;
        let quantity = sub(available, remainder)?;
        if quantity < self.plan.minimum || quantity == Decimal::ZERO {
            return Ok(None);
        }
        if self.hedges.len() >= 4096 {
            return Err(Error::Capacity);
        }
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Arithmetic)?;
        let intent = Intent {
            id: format!("{}-{}", self.plan.run_id, self.sequence),
            quantity,
            side: self.plan.hedge.side,
        };
        self.hedges.insert(
            intent.id.clone(),
            Hedge {
                intent: intent.clone(),
                key: None,
                cumulative: Decimal::ZERO,
                revision: None,
                terminal: false,
            },
        );
        Ok(Some(intent))
    }
    /// Submission ACK only binds identity; it never counts a fill or releases quantity.
    pub fn acknowledge(&mut self, id: &str, key: OrderKey) -> Result<()> {
        if key.symbol != self.plan.hedge.symbol
            || key.order_id.is_empty()
            || self
                .hedges
                .iter()
                .any(|(other, h)| other != id && h.key.as_ref() == Some(&key))
        {
            return Err(Error::Identity);
        }
        let h = self.hedges.get_mut(id).ok_or(Error::Identity)?;
        if h.key.as_ref().is_some_and(|old| old != &key) {
            return Err(Error::Identity);
        }
        h.key = Some(key);
        Ok(())
    }
    /// Supports fill-before-ACK only after the caller has securely correlated intent/order.
    /// A terminal cumulative snapshot, not a cancel ACK, releases unfilled reservation.
    pub fn observe_hedge(&mut self, id: &str, s: &OrderSnapshot) -> Result<()> {
        if s.scope != self.plan.hedge.scope {
            return Err(Error::Identity);
        }
        validate(s)?;
        let original = self.hedges.get(id).ok_or(Error::Identity)?.intent.quantity;
        if s.original != original {
            return Err(Error::Identity);
        }
        self.acknowledge(id, s.key.clone())?;
        let h = self.hedges.get_mut(id).ok_or(Error::Identity)?;
        if h.revision.is_some_and(|r| s.revision < r) {
            return Ok(());
        }
        if s.cumulative < h.cumulative {
            self.fault = true;
            return Err(Error::InvalidEvidence);
        }
        // Preserve corrected fill evidence even if it exposes a terminal-state contradiction.
        let changed_after_terminal =
            h.terminal && (s.cumulative != h.cumulative || !s.status.terminal());
        h.cumulative = s.cumulative;
        h.revision = Some(s.revision);
        h.terminal = s.status.terminal() && !s.requires_reconciliation;
        if changed_after_terminal || sub(self.exposure()?, self.in_flight()?)? < Decimal::ZERO {
            self.fault = true;
            return Err(Error::InvalidEvidence);
        }
        Ok(())
    }
}
fn validate(s: &OrderSnapshot) -> Result<()> {
    Update {
        key: s.key.clone(),
        original: s.original,
        cumulative: s.cumulative,
        status: s.status,
        event_ms: s.event_ms,
    }
    .validate()
    .map_err(|_| Error::InvalidEvidence)
}
