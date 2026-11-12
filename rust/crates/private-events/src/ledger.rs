use crate::{Error, Result};
use perp_arb_core::Decimal;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);

fn next_instance() -> Result<u64> {
    let mut current = NEXT_INSTANCE.load(Ordering::Relaxed);
    loop {
        let next = current.checked_add(1).ok_or(Error::Arithmetic)?;
        match NEXT_INSTANCE.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(value) => return Ok(value),
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub venue: String,
    /// An application account label, not a credential.
    pub account: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OrderKey {
    pub symbol: String,
    pub order_id: String,
}

fn valid_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pending,
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Expired,
    Rejected,
}
impl Status {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Expired | Self::Rejected
        )
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::New => "NEW",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
            Self::Expired => "EXPIRED",
            Self::Rejected => "REJECTED",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Update {
    pub key: OrderKey,
    pub original: Decimal,
    pub cumulative: Decimal,
    pub status: Status,
    pub event_ms: Option<u64>,
}
impl Update {
    pub fn validate(&self) -> Result<()> {
        if !valid_id(&self.key.symbol) || !valid_id(&self.key.order_id) {
            return Err(Error::InvalidInput);
        }
        if self.original <= Decimal::ZERO
            || self.cumulative < Decimal::ZERO
            || self.cumulative > self.original
            || (self.status == Status::Filled && self.cumulative != self.original)
            || (matches!(
                self.status,
                Status::New | Status::Pending | Status::Rejected
            ) && self.cumulative != Decimal::ZERO)
            || (self.status == Status::PartiallyFilled
                && (self.cumulative == Decimal::ZERO || self.cumulative == self.original))
        {
            return Err(Error::InvalidQuantity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    instance: u64,
    scope: Scope,
    generation: u64,
}
impl Session {
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Capture immediately before starting a REST query. A reply from an older
/// session/revision may contribute fills but cannot certify recovery.
#[derive(Debug, Clone)]
pub struct QueryTicket {
    session: Session,
    key: OrderKey,
    revision: u64,
}

#[derive(Debug, Clone)]
pub enum Evidence {
    Ws(Session),
    Rest(QueryTicket),
}

#[derive(Debug, Clone)]
pub struct OrderSnapshot {
    pub scope: Scope,
    pub key: OrderKey,
    pub original: Decimal,
    pub cumulative: Decimal,
    pub status: Status,
    pub event_ms: Option<u64>,
    pub revision: u64,
    pub requires_reconciliation: bool,
    /// Receipt time on the local monotonic clock; never subtract exchange time.
    pub observed_at: Instant,
    pub applied_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Health {
    pub instance: u64,
    pub generation: u64,
    pub connected: bool,
    pub reconcile_orders: usize,
    /// Hyperliquid trade observations awaiting cumulative order reconciliation.
    pub unresolved_fill_orders: usize,
    pub unresolved_external_orders: usize,
    pub account_revision: u64,
    /// Sticky on malformed input/overflow. Requires account-wide recovery,
    /// which is intentionally outside this in-memory batch.
    pub fault: Option<Error>,
}
impl Health {
    /// Necessary condition for the tracked orders only. Account inventory,
    /// risk checks and restart reconciliation require a separate account report.
    pub fn ready_for_new_risk(&self) -> bool {
        self.connected
            && self.reconcile_orders == 0
            && self.unresolved_fill_orders == 0
            && self.unresolved_external_orders == 0
            && self.fault.is_none()
    }
}

pub struct Ledger {
    instance: u64,
    scope: Scope,
    generation: u64,
    connected: bool,
    fault: Option<Error>,
    capacity: usize,
    reconcile_orders: usize,
    orders: BTreeMap<OrderKey, OrderSnapshot>,
}
impl Ledger {
    pub fn new(scope: Scope, capacity: usize) -> Result<Self> {
        if !valid_id(&scope.venue) || !valid_id(&scope.account) || capacity == 0 {
            return Err(Error::InvalidInput);
        }
        Ok(Self {
            instance: next_instance()?,
            scope,
            generation: 0,
            connected: false,
            fault: None,
            capacity,
            reconcile_orders: 0,
            orders: BTreeMap::new(),
        })
    }

    pub fn health(&self) -> Health {
        Health {
            instance: self.instance,
            generation: self.generation,
            connected: self.connected,
            fault: self.fault,
            reconcile_orders: self.reconcile_orders,
            unresolved_fill_orders: 0,
            unresolved_external_orders: 0,
            account_revision: 0,
        }
    }

    pub fn snapshot(&self, key: &OrderKey) -> Result<OrderSnapshot> {
        self.orders.get(key).cloned().ok_or(Error::UnknownOrder)
    }
    pub(crate) fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn snapshots(&self) -> Vec<OrderSnapshot> {
        self.orders.values().cloned().collect()
    }

    pub fn require_reconciliation(&mut self, key: &OrderKey) -> Result<()> {
        let order = self.orders.get_mut(key).ok_or(Error::UnknownOrder)?;
        order.revision = order.revision.checked_add(1).ok_or(Error::Arithmetic)?;
        if !order.requires_reconciliation {
            self.reconcile_orders += 1;
        }
        order.requires_reconciliation = true;
        Ok(())
    }

    /// Transport owner shutdown, including failure before a Session exists.
    pub fn transport_lost(&mut self) -> Result<()> {
        self.generation = self.generation.checked_add(1).ok_or(Error::Arithmetic)?;
        self.close();
        Ok(())
    }

    fn mark_gap(&mut self) {
        self.reconcile_orders = self.orders.len();
        for order in self.orders.values_mut() {
            order.requires_reconciliation = true;
        }
    }

    /// Call only after the private transport's subscription is acknowledged.
    /// A new session cannot erase a disconnected interval.
    pub fn connected(&mut self) -> Result<Session> {
        let session = self.connecting()?;
        self.activate(&session)?;
        Ok(session)
    }

    /// Preserve early account data without declaring subscription health.
    pub fn connecting(&mut self) -> Result<Session> {
        self.generation = self.generation.checked_add(1).ok_or(Error::Arithmetic)?;
        self.mark_gap();
        self.connected = false;
        Ok(Session {
            instance: self.instance,
            scope: self.scope.clone(),
            generation: self.generation,
        })
    }

    pub fn activate(&mut self, session: &Session) -> Result<()> {
        self.check_session(session)?;
        if session.generation != self.generation {
            return Err(Error::WrongSession);
        }
        self.connected = true;
        Ok(())
    }

    pub fn disconnected(&mut self, session: &Session) -> Result<()> {
        self.check_session(session)?;
        if session.generation == self.generation {
            self.generation = self.generation.checked_add(1).ok_or(Error::Arithmetic)?;
            self.connected = false;
            self.mark_gap();
        }
        Ok(())
    }

    pub fn fault(&mut self, error: Error) {
        self.fault.get_or_insert(error);
        self.mark_gap();
    }

    pub fn close(&mut self) {
        self.connected = false;
        self.mark_gap();
    }

    pub(crate) fn check_session(&self, session: &Session) -> Result<()> {
        if session.instance != self.instance
            || session.scope != self.scope
            || session.generation == 0
            || session.generation > self.generation
        {
            Err(Error::WrongSession)
        } else {
            Ok(())
        }
    }

    /// ACK only associates an order. It never increments fills or rolls back a
    /// fill cached before the HTTP response. No LRU eviction of dedup state.
    pub fn register(
        &mut self,
        key: OrderKey,
        original: Decimal,
        now: Instant,
    ) -> Result<OrderSnapshot> {
        let update = Update {
            key: key.clone(),
            original,
            cumulative: Decimal::ZERO,
            status: Status::Pending,
            event_ms: None,
        };
        if let Err(e) = update.validate() {
            self.fault(e);
            return Err(e);
        }
        if let Some(old) = self.orders.get(&key) {
            if old.original == original {
                return Ok(old.clone());
            }
            self.fault(Error::IdentityConflict);
            return Err(Error::IdentityConflict);
        }
        self.reserve()?;
        let snapshot = OrderSnapshot {
            scope: self.scope.clone(),
            key: key.clone(),
            original,
            cumulative: Decimal::ZERO,
            status: Status::Pending,
            event_ms: None,
            revision: 0,
            requires_reconciliation: true,
            observed_at: now,
            applied_at: Instant::now(),
        };
        self.orders.insert(key, snapshot.clone());
        self.reconcile_orders += 1;
        Ok(snapshot)
    }

    fn reserve(&mut self) -> Result<()> {
        if self.orders.len() >= self.capacity {
            self.fault(Error::Capacity);
            Err(Error::Capacity)
        } else {
            Ok(())
        }
    }

    pub fn begin_query(&self, key: &OrderKey) -> Result<QueryTicket> {
        let order = self.orders.get(key).ok_or(Error::UnknownOrder)?;
        Ok(QueryTicket {
            session: Session {
                instance: self.instance,
                scope: self.scope.clone(),
                generation: self.generation,
            },
            key: key.clone(),
            revision: order.revision,
        })
    }

    pub(crate) fn begin_discovery(&self, key: &OrderKey) -> Result<QueryTicket> {
        if !valid_id(&key.symbol) || !valid_id(&key.order_id) {
            return Err(Error::InvalidInput);
        }
        Ok(QueryTicket {
            session: Session {
                instance: self.instance,
                scope: self.scope.clone(),
                generation: self.generation,
            },
            key: key.clone(),
            revision: self.orders.get(key).map_or(0, |o| o.revision),
        })
    }

    pub fn observe(
        &mut self,
        update: Update,
        evidence: Evidence,
        now: Instant,
    ) -> Result<OrderSnapshot> {
        let result = self.apply(update, evidence, now);
        if let Err(e) = result {
            self.fault(e);
        }
        result
    }

    fn apply(&mut self, update: Update, evidence: Evidence, now: Instant) -> Result<OrderSnapshot> {
        update.validate()?;
        let session = match &evidence {
            Evidence::Ws(s) => s,
            Evidence::Rest(t) => &t.session,
        };
        // Generation zero REST is allowed before the first WS subscription.
        if session.instance != self.instance
            || session.scope != self.scope
            || session.generation > self.generation
            || (matches!(&evidence, Evidence::Ws(_)) && session.generation == 0)
        {
            return Err(Error::WrongSession);
        }
        if let Evidence::Rest(ticket) = &evidence {
            if ticket.key != update.key {
                return Err(Error::IdentityConflict);
            }
        }
        let current_session = session.generation == self.generation && self.connected;
        let previous = self.orders.get(&update.key).cloned();
        if let Some(old) = &previous {
            if old.original != update.original {
                return Err(Error::IdentityConflict);
            }
        } else {
            self.reserve()?;
        }
        let mut next = previous.clone().unwrap_or_else(|| OrderSnapshot {
            scope: self.scope.clone(),
            key: update.key.clone(),
            original: update.original,
            cumulative: Decimal::ZERO,
            status: Status::Pending,
            event_ms: None,
            revision: 0,
            requires_reconciliation: !current_session,
            observed_at: now,
            applied_at: Instant::now(),
        });
        // REST updateTime and WS E describe different stages. Do not compare
        // those timestamps as if they were a common version. REST is fenced by
        // query ticket, cumulative quantity and non-regressing terminal state.
        let newer_time = matches!(&evidence, Evidence::Rest(_))
            || match (update.event_ms, next.event_ms) {
                (Some(new), Some(old)) => new >= old,
                (None, Some(_)) => matches!(&evidence, Evidence::Rest(_)),
                _ => true,
            };
        let regressed_quantity = update.cumulative < next.cumulative;
        let terminal_conflict = next.status.terminal()
            && update.status != next.status
            && update.status != Status::Filled;
        // Never discard *increased* cumulative fills solely because timestamps,
        // REST replies or old-socket callbacks arrive out of order.
        if update.cumulative > next.cumulative {
            next.cumulative = update.cumulative;
        }
        if !regressed_quantity
            && !terminal_conflict
            && (newer_time || update.status == Status::Filled)
            && next.status != Status::Filled
        {
            next.status = update.status;
        }
        if let Some(stamp) = update.event_ms {
            next.event_ms = Some(next.event_ms.map_or(stamp, |old| old.max(stamp)));
        }
        // Quantity may move forward despite an older terminal event. Retain the
        // terminal status and demand reconciliation of its final quantity.
        if terminal_conflict
            && (update.cumulative > previous.as_ref().map_or(Decimal::ZERO, |o| o.cumulative)
                || (update.status.terminal() && newer_time))
        {
            next.requires_reconciliation = true;
        }
        if regressed_quantity && newer_time {
            next.requires_reconciliation = true;
        }
        if next.cumulative > Decimal::ZERO && matches!(next.status, Status::Pending | Status::New) {
            next.status = Status::PartiallyFilled;
            next.requires_reconciliation = true;
        }
        if !current_session {
            next.requires_reconciliation = true;
        }
        if let Evidence::Rest(ticket) = &evidence {
            if session.generation == self.generation
                && ticket.revision == next.revision
                && !regressed_quantity
                && !terminal_conflict
                && newer_time
            {
                // REST can reconcile while disconnected; connection health stays false.
                next.requires_reconciliation = false;
            }
        }
        // All accepted observations, including duplicate data, invalidate older
        // in-flight REST recovery tickets. This is conservative under busy WS.
        next.revision = next.revision.checked_add(1).ok_or(Error::Arithmetic)?;
        next.observed_at = now;
        next.applied_at = Instant::now();
        if previous.as_ref().is_some_and(|o| o.requires_reconciliation) {
            self.reconcile_orders -= 1;
        }
        if next.requires_reconciliation {
            self.reconcile_orders += 1;
        }
        self.orders.insert(update.key, next.clone());
        Ok(next)
    }
}

/// A cursor belongs to one consumer and one order. Coalescing watch snapshots
/// is safe for cumulative fills; independent readers never consume each other.
/// Advance only after the caller has durably accepted responsibility for delta.
#[derive(Debug, Clone)]
pub struct FillCursor {
    scope: Scope,
    key: OrderKey,
    accounted: Decimal,
}
impl FillCursor {
    pub fn new(scope: Scope, key: OrderKey) -> Self {
        Self {
            scope,
            key,
            accounted: Decimal::ZERO,
        }
    }
    pub fn pending(&self, snapshot: &OrderSnapshot) -> Result<Decimal> {
        if self.scope != snapshot.scope || self.key != snapshot.key {
            return Err(Error::IdentityConflict);
        }
        if snapshot.cumulative < self.accounted {
            return Err(Error::InvalidQuantity);
        }
        // Decimal checked_sub may round an unrepresentable scale alignment.
        // Build the exact coefficient and fail rather than lose a tiny fill.
        let cumulative = snapshot.cumulative.normalize();
        let accounted = self.accounted.normalize();
        let scale = cumulative.scale().max(accounted.scale());
        let left = cumulative
            .mantissa()
            .checked_mul(10i128.pow(scale - cumulative.scale()))
            .ok_or(Error::Arithmetic)?;
        let right = accounted
            .mantissa()
            .checked_mul(10i128.pow(scale - accounted.scale()))
            .ok_or(Error::Arithmetic)?;
        let mut coefficient = left.checked_sub(right).ok_or(Error::Arithmetic)?;
        let mut scale = scale;
        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        Decimal::try_from_i128_with_scale(coefficient, scale).map_err(|_| Error::Arithmetic)
    }
    pub fn acknowledge(&mut self, snapshot: &OrderSnapshot) -> Result<Decimal> {
        let delta = self.pending(snapshot)?;
        self.accounted = snapshot.cumulative;
        Ok(delta)
    }
}
