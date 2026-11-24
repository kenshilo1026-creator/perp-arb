//! Browser maker lifecycle using the existing locked, hashed, sync_all journal.
//! This module does NOT authenticate page evidence or send commands. The caller
//! must independently verify account/order association and cumulative semantics.
//! UI acknowledgements and row absence cannot create an order binding or terminal.
//! Synchronous disk I/O belongs on a dedicated blocking worker, not a WS reader.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    /// Local correlation ID, NOT a venue-supported idempotency key.
    pub request_id: String,
    pub account: String,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub limit_price: String,
    pub reduce_only: bool,
    pub max_notional: String,
}
fn identifier(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control)
}
impl Command {
    fn validate(&self) -> JResult<()> {
        if !identifier(&self.request_id)
            || !identifier(&self.account)
            || !identifier(&self.symbol)
            || !matches!(self.side.as_str(), "BUY" | "SELL")
        {
            return Err(JournalError::Identity);
        }
        let q = qty(&self.quantity)?;
        let p = qty(&self.limit_price)?;
        let cap = qty(&self.max_notional)?;
        if q <= Decimal::ZERO
            || p <= Decimal::ZERO
            || cap <= Decimal::ZERO
            || perp_arb_core::instruments::ExactAmount::product(q, p)
                .and_then(|amount| amount.compare(cap))
                .map_err(|_| JournalError::InvalidEvidence)?
                .is_gt()
        {
            return Err(JournalError::InvalidEvidence);
        }
        Ok(())
    }
    fn scope(&self) -> Scope {
        Scope {
            venue: "variational".into(),
            account: self.account.clone(),
        }
    }
}

/// Supplied ONLY after independent verification. Local request ID association
/// must come from the actual submission, never symbol/side/position matching.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub request_id: String,
    pub account: String,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub reduce_only: bool,
    pub order_id: String,
}

/// Independently verified confirmed trade, scoped by the accompanying order.
/// Quantities are incremental per immutable trade ID, never cumulative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfirmedTrade {
    pub id: String,
    pub quantity: String,
    pub price: String,
}
fn merge_trades(
    known: &mut BTreeMap<String, ConfirmedTrade>,
    trades: &[ConfirmedTrade],
) -> JResult<Decimal> {
    if trades.len() > 8192 {
        return Err(JournalError::Capacity);
    }
    for trade in trades {
        if !identifier(&trade.id)
            || qty(&trade.quantity)? <= Decimal::ZERO
            || qty(&trade.price)? <= Decimal::ZERO
        {
            return Err(JournalError::InvalidEvidence);
        }
        if let Some(old) = known.get(&trade.id) {
            if qty(&old.quantity)? != qty(&trade.quantity)?
                || qty(&old.price)? != qty(&trade.price)?
            {
                return Err(JournalError::InvalidEvidence);
            }
        } else {
            if known.len() >= 8192 {
                return Err(JournalError::Capacity);
            }
            known.insert(trade.id.clone(), trade.clone());
        }
    }
    known.values().try_fold(Decimal::ZERO, |sum, trade| {
        let q = qty(&trade.quantity)?;
        let next = sum.checked_add(q).ok_or(JournalError::InvalidEvidence)?;
        if next.checked_sub(sum) != Some(q) {
            return Err(JournalError::InvalidEvidence);
        }
        Ok(next)
    })
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "event", deny_unknown_fields)]
enum Entry {
    Prepared {
        command: Command,
    },
    Started,
    SubmitAck,
    Bound {
        identity: Identity,
    },
    Observed {
        order: OrderEvidence,
        lookup_confirmed: bool,
    },
    TradesObserved {
        trades: Vec<ConfirmedTrade>,
        order: OrderEvidence,
        lookup_confirmed: bool,
    },
    TradeConflict,
    CancelStarted,
    Unresolved,
}
#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub command: Command,
    pub dispatched: bool,
    pub submit_acknowledged: bool,
    pub order_id: Option<String>,
    pub cumulative: String,
    pub cancel_pending: bool,
    pub cancel_attempts: u32,
    pub terminal_confirmed: bool,
    pub reconciliation_required: bool,
    pub poisoned: bool,
    pub confirmed_trade_count: usize,
    pub trade_conflict: bool,
}
#[derive(Clone)]
struct State {
    view: View,
    last: Option<OrderEvidence>,
    watermark: Option<u64>,
    trades: BTreeMap<String, ConfirmedTrade>,
}
impl State {
    fn new(command: Command) -> Self {
        Self {
            view: View {
                command,
                dispatched: false,
                submit_acknowledged: false,
                order_id: None,
                cumulative: "0".into(),
                cancel_pending: false,
                cancel_attempts: 0,
                terminal_confirmed: false,
                reconciliation_required: true,
                poisoned: false,
                confirmed_trade_count: 0,
                trade_conflict: false,
            },
            last: None,
            watermark: None,
            trades: BTreeMap::new(),
        }
    }
    fn apply(&mut self, entry: &Entry) -> JResult<()> {
        if let Entry::TradesObserved {
            trades,
            order,
            lookup_confirmed,
        } = entry
        {
            if self.view.trade_conflict {
                return Err(JournalError::RecoveryRequired);
            }
            let mut known = self.trades.clone();
            let sum = merge_trades(&mut known, trades)?;
            if sum > qty(&self.view.command.quantity)?
                || qty(&order.cumulative)? != sum.max(qty(&self.view.cumulative)?)
            {
                return Err(JournalError::InvalidEvidence);
            }
            self.apply(&Entry::Observed {
                order: order.clone(),
                lookup_confirmed: *lookup_confirmed,
            })?;
            self.trades = known;
            self.view.confirmed_trade_count = self.trades.len();
            return Ok(());
        }
        let v = &mut self.view;
        match entry {
            Entry::Prepared { .. } => return Err(JournalError::Corrupt),
            Entry::Started => {
                if v.dispatched {
                    return Err(JournalError::AlreadyDispatched);
                }
                v.dispatched = true;
            }
            Entry::SubmitAck => {
                if !v.dispatched {
                    return Err(JournalError::Identity);
                }
                v.submit_acknowledged = true;
            }
            Entry::Bound { identity: i } => {
                let c = &v.command;
                if !v.dispatched
                    || i.request_id != c.request_id
                    || i.account != c.account
                    || i.symbol != c.symbol
                    || i.side != c.side
                    || qty(&i.quantity)? != qty(&c.quantity)?
                    || i.reduce_only != c.reduce_only
                    || !identifier(&i.order_id)
                    || v.order_id.as_ref().is_some_and(|old| old != &i.order_id)
                {
                    return Err(JournalError::Identity);
                }
                v.order_id = Some(i.order_id.clone());
            }
            Entry::Observed {
                order,
                lookup_confirmed,
            } => {
                let s = order.snapshot(0)?;
                if !v.dispatched
                    || s.scope != v.command.scope()
                    || Some(&s.key.order_id) != v.order_id.as_ref()
                    || s.key.symbol != v.command.symbol
                    || s.original != qty(&v.command.quantity)?
                {
                    return Err(JournalError::Identity);
                }
                let old = qty(&v.cumulative)?;
                if s.cumulative < old {
                    return Err(JournalError::InvalidEvidence);
                }
                let source_older = self
                    .watermark
                    .is_some_and(|ms| s.event_ms.is_none_or(|next| next < ms));
                if *lookup_confirmed && v.trade_conflict {
                    return Err(JournalError::RecoveryRequired);
                }
                if *lookup_confirmed
                    && (s.requires_reconciliation || s.event_ms.is_none() || source_older)
                {
                    return Err(JournalError::InvalidEvidence);
                }
                let changed = s.cumulative != old;
                // A later confirmed fill survives even after a claimed terminal.
                // A fresh, version-fenced lookup is then needed before replacement.
                if changed
                    || !s.status.terminal()
                    || s.requires_reconciliation
                    || source_older
                    || (s.status.terminal() && !v.terminal_confirmed)
                {
                    v.terminal_confirmed = false;
                    v.reconciliation_required = true;
                }
                v.cumulative = s.cumulative.to_string();
                self.last = Some(order.clone());
                if let Some(ms) = s.event_ms {
                    self.watermark = Some(self.watermark.unwrap_or(0).max(ms));
                }
                if *lookup_confirmed {
                    v.reconciliation_required = false;
                    v.terminal_confirmed = s.status.terminal();
                    v.cancel_pending = false;
                }
            }
            Entry::CancelStarted => {
                if v.order_id.is_none() || !v.dispatched || v.cancel_pending || v.terminal_confirmed
                {
                    return Err(JournalError::RecoveryRequired);
                }
                v.cancel_attempts = v
                    .cancel_attempts
                    .checked_add(1)
                    .ok_or(JournalError::Capacity)?;
                v.cancel_pending = true;
                v.terminal_confirmed = false;
                v.reconciliation_required = true;
            }
            Entry::Unresolved => {
                v.reconciliation_required = true;
                v.terminal_confirmed = false;
            }
            Entry::TradeConflict => {
                v.trade_conflict = true;
                v.reconciliation_required = true;
                v.terminal_confirmed = false;
            }
            Entry::TradesObserved { .. } => unreachable!(),
        }
        Ok(())
    }
}
pub struct DispatchPermit {
    command: Command,
}
impl DispatchPermit {
    pub fn command(&self) -> &Command {
        &self.command
    }
}
pub struct CancelPermit {
    order_id: String,
    command: Command,
}
impl CancelPermit {
    pub fn order_id(&self) -> &str {
        &self.order_id
    }
    pub fn command(&self) -> &Command {
        &self.command
    }
}
pub struct LookupTicket {
    instance: [u8; 16],
    sequence: u64,
    started: Instant,
    order_id: String,
}
impl LookupTicket {
    pub fn order_id(&self) -> &str {
        &self.order_id
    }
}
pub struct Ledger {
    journal: Journal,
    state: State,
    instance: [u8; 16],
    reconciled_at: Option<Instant>,
    lookup_started_at: Option<Instant>,
}
fn wrap(entry: Entry) -> JResult<Event> {
    Ok(Event::BrowserOrder {
        payload: serde_json::to_value(entry).map_err(|_| JournalError::Corrupt)?,
    })
}
fn unwrap(event: Event) -> JResult<Entry> {
    match event {
        Event::BrowserOrder { payload } => {
            serde_json::from_value(payload).map_err(|_| JournalError::Corrupt)
        }
        _ => Err(JournalError::Corrupt),
    }
}
impl Ledger {
    pub fn create(path: &Path, command: Command) -> JResult<Self> {
        command.validate()?;
        let instance = random()?;
        let mut journal = Journal::create(path)?;
        journal.append(wrap(Entry::Prepared {
            command: command.clone(),
        })?)?;
        Ok(Self {
            journal,
            state: State::new(command),
            instance,
            reconciled_at: None,
            lookup_started_at: None,
        })
    }
    pub fn open(path: &Path) -> JResult<Self> {
        let (journal, events) = Journal::open(path)?;
        let mut entries = events.into_iter().map(unwrap);
        let command = match entries.next().ok_or(JournalError::Corrupt)?? {
            Entry::Prepared { command } => command,
            _ => return Err(JournalError::Corrupt),
        };
        command.validate()?;
        let mut state = State::new(command);
        for entry in entries {
            state.apply(&entry?)?;
        }
        // Disk evidence preserves known fills but never restores live freshness.
        state.view.reconciliation_required = true;
        state.view.terminal_confirmed = false;
        Ok(Self {
            journal,
            state,
            instance: random()?,
            reconciled_at: None,
            lookup_started_at: None,
        })
    }
    pub fn view(&self) -> View {
        self.state.view.clone()
    }
    fn commit(&mut self, entry: Entry) -> JResult<()> {
        if self.state.view.poisoned {
            return Err(JournalError::Poisoned);
        }
        let mut next = self.state.clone();
        next.apply(&entry)?;
        let confirmed = matches!(
            &entry,
            Entry::Observed {
                lookup_confirmed: true,
                ..
            } | Entry::TradesObserved {
                lookup_confirmed: true,
                ..
            }
        );
        if let Err(error) = self.journal.append(wrap(entry)?) {
            // Keep newly known exposure visible even when durability failed.
            // No further dispatch permission may escape this poisoned owner.
            self.state = next;
            self.state.view.poisoned = true;
            self.reconciled_at = None;
            self.lookup_started_at = None;
            return Err(error);
        }
        self.state = next;
        self.reconciled_at = confirmed.then(Instant::now);
        self.lookup_started_at = None;
        Ok(())
    }
    pub fn dispatch(&mut self) -> JResult<DispatchPermit> {
        self.commit(Entry::Started)?;
        Ok(DispatchPermit {
            command: self.state.view.command.clone(),
        })
    }
    pub fn submit_ack(&mut self, request_id: &str) -> JResult<()> {
        if request_id != self.state.view.command.request_id {
            return Err(JournalError::Identity);
        }
        self.commit(Entry::SubmitAck)
    }
    pub fn bind_verified(&mut self, identity: Identity) -> JResult<()> {
        self.commit(Entry::Bound { identity })
    }
    /// Use for timeout, disconnect, UI row absence, or a cancel ACK. None proves
    /// zero fill, non-submission, or that the original maker may be replaced.
    pub fn unresolved(&mut self) -> JResult<()> {
        self.commit(Entry::Unresolved)
    }
    pub fn cancel_started(&mut self) -> JResult<CancelPermit> {
        self.commit(Entry::CancelStarted)?;
        Ok(CancelPermit {
            command: self.state.view.command.clone(),
            order_id: self
                .state
                .view
                .order_id
                .clone()
                .ok_or(JournalError::Identity)?,
        })
    }
    fn check_snapshot(&self, s: &OrderSnapshot) -> JResult<()> {
        validate(s)?;
        let v = &self.state.view;
        if s.scope != v.command.scope()
            || Some(&s.key.order_id) != v.order_id.as_ref()
            || s.key.symbol != v.command.symbol
            || s.original != qty(&v.command.quantity)?
        {
            return Err(JournalError::Identity);
        }
        Ok(())
    }
    /// Caller has already established cumulative order semantics and ownership.
    /// Returns new known quantity only; duplicates or old cumulative frames add 0.
    pub fn observe_verified(&mut self, s: &OrderSnapshot) -> JResult<Decimal> {
        self.check_snapshot(s)?;
        let old = qty(&self.state.view.cumulative)?;
        if s.cumulative < old {
            self.unresolved()?;
            return Ok(Decimal::ZERO);
        }
        self.commit(Entry::Observed {
            order: OrderEvidence::from_snapshot(s),
            lookup_confirmed: false,
        })?;
        let delta = s
            .cumulative
            .checked_sub(old)
            .ok_or(JournalError::InvalidEvidence)?;
        if delta.checked_add(old) != Some(s.cumulative)
            || s.cumulative.checked_sub(delta) != Some(old)
        {
            return Err(JournalError::InvalidEvidence);
        }
        Ok(delta)
    }
    pub fn lookup_ticket(&self) -> JResult<LookupTicket> {
        if self.state.view.poisoned {
            return Err(JournalError::Poisoned);
        }
        Ok(LookupTicket {
            instance: self.instance,
            sequence: self.journal.sequence,
            started: Instant::now(),
            order_id: self
                .state
                .view
                .order_id
                .clone()
                .ok_or(JournalError::RecoveryRequired)?,
        })
    }
    /// Exact order lookup, not the extension's fuzzy Open Orders row check.
    /// Intervening fills/cancel/ACK/gaps invalidate terminal confirmation, but
    /// a larger verified cumulative is still retained for hedging.
    pub fn apply_verified_lookup(&mut self, t: LookupTicket, s: &OrderSnapshot) -> JResult<()> {
        if t.instance != self.instance || t.order_id != s.key.order_id {
            return Err(JournalError::Identity);
        }
        self.check_snapshot(s)?;
        if s.cumulative < qty(&self.state.view.cumulative)? {
            return self.unresolved();
        }
        let fresh = self.lookup_fresh(&t, s);
        let mut order = OrderEvidence::from_snapshot(s);
        order.gap |= !fresh;
        self.commit(Entry::Observed {
            order,
            lookup_confirmed: fresh,
        })?;
        if fresh {
            self.lookup_started_at = Some(t.started);
        }
        Ok(())
    }
    fn lookup_fresh(&self, t: &LookupTicket, s: &OrderSnapshot) -> bool {
        t.sequence == self.journal.sequence
            && t.started.elapsed() <= Duration::from_secs(5)
            && s.observed_at >= t.started
            && s.applied_at >= t.started
            && s.observed_at <= Instant::now()
            && s.applied_at <= Instant::now()
            && !s.requires_reconciliation
            && s.event_ms.is_some()
            && self
                .state
                .watermark
                .is_none_or(|last| s.event_ms.is_some_and(|now| now >= last))
    }
    /// Atomically persist exact trade identities and the cumulative lower bound.
    /// A bounded REST page is a set of trades, NOT a new incremental fill. Union
    /// across queries survives restarts; pre-existing anonymous cumulative is a
    /// floor, never added to the union (which could double count the same fills).
    pub fn apply_verified_trades(
        &mut self,
        t: LookupTicket,
        s: &OrderSnapshot,
        trades: Vec<ConfirmedTrade>,
    ) -> JResult<()> {
        if t.instance != self.instance || t.order_id != s.key.order_id {
            return Err(JournalError::Identity);
        }
        self.check_snapshot(s)?;
        if self.state.view.trade_conflict {
            return Err(JournalError::RecoveryRequired);
        }
        let mut page = BTreeMap::new();
        let mut known = self.state.trades.clone();
        let merged = (|| {
            if merge_trades(&mut page, &trades)? != s.cumulative {
                return Err(JournalError::InvalidEvidence);
            }
            let sum = merge_trades(&mut known, &trades)?;
            if sum > s.original {
                return Err(JournalError::InvalidEvidence);
            }
            Ok(sum)
        })();
        let sum = match merged {
            Ok(sum) => sum,
            Err(error) => {
                self.commit(Entry::TradeConflict)?;
                return Err(error);
            }
        };
        let cumulative = sum.max(qty(&self.state.view.cumulative)?);
        // Only the query's own complete evidence can confirm terminal; a union
        // of incomplete pages cannot manufacture a fresh terminal proof.
        let fresh = cumulative == s.cumulative && self.lookup_fresh(&t, s);
        let mut snapshot = s.clone();
        snapshot.cumulative = cumulative;
        if !fresh {
            snapshot.requires_reconciliation = true;
            snapshot.status = if cumulative == snapshot.original {
                Status::Filled
            } else if cumulative > Decimal::ZERO {
                Status::PartiallyFilled
            } else {
                Status::New
            };
        }
        self.commit(Entry::TradesObserved {
            trades,
            order: OrderEvidence::from_snapshot(&snapshot),
            lookup_confirmed: fresh,
        })?;
        if fresh {
            self.lookup_started_at = Some(t.started);
        }
        Ok(())
    }
    /// Compatible with the existing execution coordinator. No venue submission.
    pub fn maker_snapshot(&self) -> JResult<OrderSnapshot> {
        if self.state.view.poisoned {
            return Err(JournalError::Poisoned);
        }
        let mut s = self
            .state
            .last
            .as_ref()
            .ok_or(JournalError::RecoveryRequired)?
            .snapshot(self.journal.sequence)?;
        s.requires_reconciliation = self.state.view.reconciliation_required;
        Ok(s)
    }
    /// A new process must make a new exact lookup before clearing recovery in
    /// the paired coordinator. Historical disk freshness cannot satisfy this.
    pub fn reconciliation_snapshot(&self) -> JResult<OrderSnapshot> {
        if self.state.view.reconciliation_required
            || self
                .reconciled_at
                .is_none_or(|at| at.elapsed() > Duration::from_secs(5))
        {
            return Err(JournalError::RecoveryRequired);
        }
        self.maker_snapshot()
    }
    pub(super) fn reconciliation_snapshot_after(
        &self,
        started_at: Instant,
    ) -> JResult<OrderSnapshot> {
        if self.lookup_started_at.is_none_or(|at| at < started_at) {
            return Err(JournalError::RecoveryRequired);
        }
        self.reconciliation_snapshot()
    }
}

#[cfg(test)]
mod tests;
