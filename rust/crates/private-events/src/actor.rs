//! Single mutable owner. Producers await bounded mpsc capacity, never try_send
//! and discard. Once admitted, dropping the caller does not cancel an event.
//! Watch carries cumulative state, not lossy per-trade deltas.
use crate::{
    account, aster, hyperliquid, Error, Evidence, Health, Ledger, OrderKey, OrderSnapshot,
    QueryTicket, Result, Scope, Session, Update,
};
use perp_arb_core::Decimal;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

enum Command {
    Connected,
    Connecting,
    Activate(Session),
    Disconnected(Session),
    Fault(Error),
    Register(OrderKey, Decimal),
    Observe(Update, Evidence),
    Query(OrderKey),
    Subscribe(OrderKey),
    List,
    RequireReconciliation(OrderKey),
    TransportLost,
    HlEvent(Session, hyperliquid::Event),
    HlSummary(OrderKey),
    HlPending,
    HlQuery(OrderKey),
    HlRest(hyperliquid::QueryTicket, Update),
    AccountChanged(Session),
    AccountBegin,
    AccountApply(
        account::Ticket,
        account::Snapshot,
        Option<account::Inventory>,
    ),
    AccountView,
    PositionApply(account::Ticket, account::Snapshot, String),
    ExternalPending,
    Shutdown,
}
enum Reply {
    Unit,
    Session(Session),
    Order(OrderSnapshot),
    Query(QueryTicket),
    Subscription(watch::Receiver<OrderSnapshot>),
    Orders(Vec<OrderSnapshot>),
    HlReport(hyperliquid::IngestReport),
    HlSummary(Option<hyperliquid::FillSummary>),
    Keys(Vec<OrderKey>),
    HlQuery(hyperliquid::QueryTicket),
    AccountTicket(account::Ticket),
    AccountReport(Option<account::Report>),
}
struct Envelope {
    command: Command,
    received_at: Instant,
    reply: oneshot::Sender<Result<Reply>>,
}

#[derive(Clone)]
pub struct Coordinator {
    position_symbol: Option<String>,
    transport: Arc<AtomicBool>,
    scope: Scope,
    tx: mpsc::Sender<Envelope>,
    health: watch::Receiver<Health>,
}

pub struct TransportLease(Arc<AtomicBool>);
impl Drop for TransportLease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub struct Worker {
    position_symbol: Option<String>,
    ledger: Ledger,
    rx: mpsc::Receiver<Envelope>,
    health: watch::Sender<Health>,
    watches: BTreeMap<OrderKey, watch::Sender<OrderSnapshot>>,
    fills: hyperliquid::FillBook,
    pending_fills: BTreeSet<OrderKey>,
    external_pending: BTreeSet<OrderKey>,
    external_accounted: BTreeSet<OrderKey>,
    account_revision: u64,
    account_report: Option<account::Report>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Cancellation/panic must not leave readers holding a healthy view.
        self.ledger.close();
        self.publish();
    }
}

/// Separating creation from spawn also permits deterministic queue/backpressure
/// tests. A single worker owns all state for this account.
pub fn channel(scope: Scope, orders: usize, ingress: usize) -> Result<(Coordinator, Worker)> {
    if ingress == 0 || ingress > 65_536 || orders > 65_536 {
        return Err(Error::InvalidInput);
    }
    let ledger = Ledger::new(scope.clone(), orders)?;
    let (tx, rx) = mpsc::channel(ingress);
    let (health, health_rx) = watch::channel(ledger.health());
    Ok((
        Coordinator {
            position_symbol: None,
            transport: Arc::new(AtomicBool::new(false)),
            scope,
            tx,
            health: health_rx,
        },
        Worker {
            position_symbol: None,
            ledger,
            rx,
            health,
            watches: BTreeMap::new(),
            fills: hyperliquid::FillBook::new(orders.saturating_mul(16).min(65_536)),
            pending_fills: BTreeSet::new(),
            external_pending: BTreeSet::new(),
            external_accounted: BTreeSet::new(),
            account_revision: 0,
            account_report: None,
        },
    ))
}

/// Dedicated per-trade coordinator; unrelated account orders stay out of its ledger.
pub fn position_channel(
    scope: Scope,
    symbol: &str,
    orders: usize,
    ingress: usize,
) -> Result<(Coordinator, Worker)> {
    if symbol.is_empty() || symbol.len() > 128 || symbol.chars().any(char::is_control) {
        return Err(Error::InvalidInput);
    }
    let (mut c, mut w) = channel(scope, orders, ingress)?;
    c.position_symbol = Some(symbol.into());
    w.position_symbol = Some(symbol.into());
    Ok((c, w))
}

impl Worker {
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    fn publish(&mut self) {
        self.publish_health();
        // Normal event hot path updates just one watched order below. This
        // account-wide traversal is reserved for gaps, shutdown and errors.
        for (key, sender) in &self.watches {
            if let Ok(snapshot) = self.ledger.snapshot(key) {
                sender.send_replace(snapshot);
            }
        }
    }

    fn publish_health(&self) {
        let mut health = self.ledger.health();
        health.unresolved_fill_orders = self.pending_fills.len();
        health.unresolved_external_orders = self.external_pending.len();
        health.account_revision = self.account_revision;
        self.health.send_replace(health);
    }

    fn hl_event(
        &mut self,
        session: Session,
        event: hyperliquid::Event,
        at: Instant,
    ) -> Result<hyperliquid::IngestReport> {
        self.ledger.check_session(&session)?;
        let mut report = hyperliquid::IngestReport::default();
        match event {
            hyperliquid::Event::Orders(orders) => {
                for update in orders {
                    if self
                        .position_symbol
                        .as_ref()
                        .is_some_and(|s| s != &update.key.symbol)
                    {
                        continue;
                    }
                    let key = update.key.clone();
                    let snapshot =
                        self.ledger
                            .observe(update, Evidence::Ws(session.clone()), at)?;
                    if self
                        .fills
                        .summary(&key)
                        .is_some_and(|f| f.observed_quantity > snapshot.cumulative)
                    {
                        if self
                            .fills
                            .summary(&key)
                            .is_some_and(|f| f.external_kind.is_some())
                        {
                            self.external_pending.insert(key.clone());
                        } else {
                            self.pending_fills.insert(key.clone());
                        }
                        self.ledger.require_reconciliation(&key)?;
                    }
                    report.order_updates += 1;
                }
            }
            hyperliquid::Event::Fills { snapshot, fills } => {
                report.snapshot = snapshot;
                for fill in fills {
                    if self
                        .position_symbol
                        .as_ref()
                        .is_some_and(|s| s != &fill.key.symbol)
                    {
                        continue;
                    }
                    let key = fill.key.clone();
                    if self.fills.insert(fill)? {
                        report.new_fills += 1;
                        if self
                            .fills
                            .summary(&key)
                            .is_some_and(|f| f.external_kind.is_some())
                        {
                            self.external_pending.insert(key.clone());
                            self.external_accounted.remove(&key);
                        } else {
                            self.pending_fills.insert(key.clone());
                        }
                        if self.ledger.snapshot(&key).is_ok() {
                            self.ledger.require_reconciliation(&key)?;
                        }
                    } else {
                        report.duplicate_fills += 1;
                    }
                }
            }
            hyperliquid::Event::AccountChange | hyperliquid::Event::Other => (),
        }
        Ok(report)
    }

    fn hl_rest(
        &mut self,
        ticket: hyperliquid::QueryTicket,
        update: Update,
        at: Instant,
    ) -> Result<OrderSnapshot> {
        let key = ticket.key.clone();
        let reply_time = update.event_ms;
        let result = self
            .ledger
            .observe(update, Evidence::Rest(ticket.inner), at)?;
        let fills = self.fills.summary(&key).unwrap_or_default();
        if fills.unique_fills != ticket.fill_count
            || fills.observed_quantity > result.cumulative
            || reply_time.is_none_or(|ms| ms < fills.last_time_ms)
        {
            self.ledger.require_reconciliation(&key)?;
        } else if !result.requires_reconciliation {
            self.pending_fills.remove(&key);
        }
        self.ledger.snapshot(&key)
    }

    pub async fn run(mut self) {
        let mut shutting_down = false;
        while let Some(envelope) = self.rx.recv().await {
            if matches!(
                &envelope.command,
                Command::Connected
                    | Command::Connecting
                    | Command::Disconnected(_)
                    | Command::TransportLost
            ) {
                self.external_pending
                    .extend(self.external_accounted.iter().cloned());
                self.external_accounted.clear();
            }
            if matches!(
                &envelope.command,
                Command::Connected
                    | Command::Connecting
                    | Command::Disconnected(_)
                    | Command::TransportLost
                    | Command::AccountBegin
                    | Command::AccountChanged(_)
                    | Command::RequireReconciliation(_)
                    | Command::Fault(_)
                    | Command::Register(..)
                    | Command::Observe(..)
                    | Command::HlEvent(..)
                    | Command::HlRest(..)
            ) {
                match self.account_revision.checked_add(1) {
                    Some(n) => self.account_revision = n,
                    None => self.ledger.fault(Error::Arithmetic),
                }
            }
            let mut account_wide = true;
            let result = match envelope.command {
                Command::Connected | Command::Connecting | Command::Activate(_)
                    if shutting_down =>
                {
                    Err(Error::Closed)
                }
                Command::Connected => self.ledger.connected().map(Reply::Session),
                Command::Connecting => self.ledger.connecting().map(Reply::Session),
                Command::Activate(s) => self.ledger.activate(&s).map(|_| Reply::Unit),
                Command::Disconnected(session) => {
                    self.ledger.disconnected(&session).map(|_| Reply::Unit)
                }
                Command::Fault(error) => {
                    self.ledger.fault(error);
                    Ok(Reply::Unit)
                }
                Command::Register(key, original) => self
                    .ledger
                    .register(key, original, envelope.received_at)
                    .map(Reply::Order),
                Command::Observe(update, evidence) => {
                    let result = self.ledger.observe(update, evidence, envelope.received_at);
                    if let Ok(snapshot) = &result {
                        account_wide = false;
                        if let Some(sender) = self.watches.get(&snapshot.key) {
                            sender.send_replace(snapshot.clone());
                        }
                        self.publish_health();
                    }
                    result.map(Reply::Order)
                }
                Command::Query(key) => {
                    account_wide = false;
                    self.ledger.begin_query(&key).map(Reply::Query)
                }
                Command::List => {
                    account_wide = false;
                    Ok(Reply::Orders(self.ledger.snapshots()))
                }
                Command::RequireReconciliation(key) => self
                    .ledger
                    .require_reconciliation(&key)
                    .map(|_| Reply::Unit),
                Command::TransportLost => self.ledger.transport_lost().map(|_| Reply::Unit),
                Command::HlEvent(session, event) => {
                    let result = self.hl_event(session, event, envelope.received_at);
                    if let Err(e) = result {
                        self.ledger.fault(e);
                    }
                    result.map(Reply::HlReport)
                }
                Command::HlSummary(key) => {
                    account_wide = false;
                    Ok(Reply::HlSummary(self.fills.summary(&key)))
                }
                Command::HlPending => {
                    account_wide = false;
                    Ok(Reply::Keys(self.pending_fills.iter().cloned().collect()))
                }
                Command::HlQuery(key) => {
                    account_wide = false;
                    self.ledger.begin_discovery(&key).map(|inner| {
                        Reply::HlQuery(hyperliquid::QueryTicket {
                            fill_count: self.fills.summary(&key).map_or(0, |f| f.unique_fills),
                            key,
                            inner,
                        })
                    })
                }
                Command::HlRest(ticket, update) => {
                    let result = self.hl_rest(ticket, update, envelope.received_at);
                    if let Err(e) = result {
                        self.ledger.fault(e);
                    }
                    result.map(Reply::Order)
                }
                Command::AccountChanged(session) => {
                    self.ledger.check_session(&session).map(|_| Reply::Unit)
                }
                Command::AccountBegin => {
                    account_wide = false;
                    // The ticket advances account_revision; publish immediately so
                    // old proofs cannot authorize risk while this query is pending.
                    self.publish_health();
                    let health = self.ledger.health();
                    Ok(Reply::AccountTicket(account::Ticket {
                        scope: self.ledger.scope().clone(),
                        instance: health.instance,
                        generation: health.generation,
                        revision: self.account_revision,
                        started_at: Instant::now(),
                    }))
                }
                Command::AccountApply(ticket, snapshot, expected) => {
                    let health = self.ledger.health();
                    let result = account::reconcile(
                        ticket,
                        snapshot,
                        expected,
                        self.ledger.scope(),
                        health.instance,
                        health.generation,
                        self.account_revision,
                    );
                    result.map(|mut report| {
                        if report.matches_expected()
                            && report.inventory.includes_wallets
                            && health.connected
                            && health.fault.is_none()
                        {
                            let covered: Vec<_> = self
                                .external_pending
                                .iter()
                                .filter(|key| {
                                    !key.symbol.starts_with('@')
                                        && !key.symbol.contains('/')
                                        && self.ledger.snapshot(key).is_err()
                                        && report.inventory.covers(&key.symbol)
                                        && !report.inventory.open_orders.contains_key(*key)
                                        && self.fills.summary(key).is_some_and(|f| {
                                            report
                                                .exchange_time_ms
                                                .is_some_and(|t| f.last_time_ms < t)
                                        })
                                })
                                .cloned()
                                .collect();
                            for key in covered {
                                self.external_pending.remove(&key);
                                self.external_accounted.insert(key);
                                report.external_orders_accounted += 1;
                            }
                        }
                        self.account_report = Some(report.clone());
                        Reply::AccountReport(Some(report))
                    })
                }
                Command::AccountView => {
                    account_wide = false;
                    Ok(Reply::AccountReport(self.account_report.clone()))
                }
                Command::PositionApply(ticket, snapshot, symbol) => {
                    let health = self.ledger.health();
                    account::position_report(
                        ticket,
                        snapshot,
                        symbol,
                        self.ledger.scope(),
                        health.instance,
                        health.generation,
                        self.account_revision,
                    )
                    .map(|report| {
                        self.account_report = Some(report.clone());
                        Reply::AccountReport(Some(report))
                    })
                }
                Command::ExternalPending => {
                    account_wide = false;
                    Ok(Reply::Keys(self.external_pending.iter().cloned().collect()))
                }
                Command::Subscribe(key) => {
                    account_wide = false;
                    self.ledger.snapshot(&key).map(|snapshot| {
                        let sender = self
                            .watches
                            .entry(key)
                            .or_insert_with(|| watch::channel(snapshot).0);
                        Reply::Subscription(sender.subscribe())
                    })
                }
                Command::Shutdown => {
                    // Reject further sends, drain all admitted events before exit.
                    self.rx.close();
                    self.ledger.close();
                    shutting_down = true;
                    Ok(Reply::Unit)
                }
            };
            if shutting_down {
                self.ledger.close();
                account_wide = true;
            }
            if account_wide {
                self.publish();
            }
            let _ = envelope.reply.send(result);
        }
        self.ledger.close();
        self.publish();
    }
}

impl Coordinator {
    /// Verified transport notification (funding/balance/position/config change)
    /// invalidates outstanding account queries, including non-order events.
    pub async fn account_changed(&self, session: Session) -> Result<()> {
        self.call(Command::AccountChanged(session))
            .await
            .map(|_| ())
    }
    pub async fn begin_account_query(&self) -> Result<account::Ticket> {
        match self.call(Command::AccountBegin).await? {
            Reply::AccountTicket(t) => Ok(t),
            _ => unreachable!(),
        }
    }
    pub async fn apply_account_snapshot(
        &self,
        ticket: account::Ticket,
        snapshot: account::Snapshot,
        expected: Option<account::Inventory>,
    ) -> Result<account::Report> {
        if snapshot.scope != self.scope {
            return Err(Error::WrongSession);
        }
        match self
            .call(Command::AccountApply(ticket, snapshot, expected))
            .await?
        {
            Reply::AccountReport(Some(r)) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn account_report(&self) -> Result<Option<account::Report>> {
        match self.call(Command::AccountView).await? {
            Reply::AccountReport(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn apply_position_snapshot(
        &self,
        ticket: account::Ticket,
        snapshot: account::Snapshot,
        symbol: &str,
    ) -> Result<account::Report> {
        if self.position_symbol.as_deref() != Some(symbol) || snapshot.scope != self.scope {
            return Err(Error::WrongSession);
        }
        match self
            .call(Command::PositionApply(ticket, snapshot, symbol.into()))
            .await?
        {
            Reply::AccountReport(Some(r)) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn external_pending(&self) -> Result<Vec<OrderKey>> {
        match self.call(Command::ExternalPending).await? {
            Reply::Keys(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn ingest_hyperliquid_ws_at(
        &self,
        session: Session,
        raw: &str,
        received_at: Instant,
    ) -> Result<hyperliquid::IngestReport> {
        if self.scope.venue != "hyperliquid" {
            return Err(Error::WrongSession);
        }
        let event = match hyperliquid::decode_ws(raw, &self.scope.account) {
            Ok(v) => v,
            Err(e) => {
                self.call(Command::Fault(e)).await?;
                return Err(e);
            }
        };
        match self
            .call_at(Command::HlEvent(session, event), received_at)
            .await?
        {
            Reply::HlReport(v) => Ok(v),
            _ => unreachable!(),
        }
    }
    pub async fn hyperliquid_fill_summary(
        &self,
        key: OrderKey,
    ) -> Result<Option<hyperliquid::FillSummary>> {
        match self.call(Command::HlSummary(key)).await? {
            Reply::HlSummary(v) => Ok(v),
            _ => unreachable!(),
        }
    }
    pub async fn hyperliquid_pending(&self) -> Result<Vec<OrderKey>> {
        match self.call(Command::HlPending).await? {
            Reply::Keys(v) => Ok(v),
            _ => unreachable!(),
        }
    }
    pub async fn begin_hyperliquid_query(&self, key: OrderKey) -> Result<hyperliquid::QueryTicket> {
        if self.scope.venue != "hyperliquid" {
            return Err(Error::WrongSession);
        }
        match self.call(Command::HlQuery(key)).await? {
            Reply::HlQuery(v) => Ok(v),
            _ => unreachable!(),
        }
    }
    pub async fn ingest_hyperliquid_rest(
        &self,
        ticket: hyperliquid::QueryTicket,
        raw: &str,
    ) -> Result<OrderSnapshot> {
        if self.scope.venue != "hyperliquid" {
            return Err(Error::WrongSession);
        }
        let update = match hyperliquid::decode_rest(raw) {
            Ok(v) => v,
            // Not found is uncertainty, not an empty or rejected order.
            Err(Error::UnknownOrder) => return Err(Error::UnknownOrder),
            Err(e) => {
                self.call(Command::Fault(e)).await?;
                return Err(e);
            }
        };
        match self.call(Command::HlRest(ticket, update)).await? {
            Reply::Order(v) => Ok(v),
            _ => unreachable!(),
        }
    }
    pub fn acquire_transport(&self) -> Result<TransportLease> {
        self.transport
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::TransportInUse)?;
        Ok(TransportLease(self.transport.clone()))
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub async fn snapshots(&self) -> Result<Vec<OrderSnapshot>> {
        match self.call(Command::List).await? {
            Reply::Orders(v) => Ok(v),
            _ => unreachable!(),
        }
    }
    pub async fn require_reconciliation(&self, key: OrderKey) -> Result<()> {
        self.call(Command::RequireReconciliation(key))
            .await
            .map(|_| ())
    }
    pub async fn transport_lost(&self) -> Result<()> {
        self.call(Command::TransportLost).await.map(|_| ())
    }
    async fn call_at(&self, command: Command, received_at: Instant) -> Result<Reply> {
        let (reply, receiver) = oneshot::channel();
        self.tx
            .send(Envelope {
                command,
                received_at,
                reply,
            })
            .await
            .map_err(|_| Error::Closed)?;
        receiver.await.map_err(|_| Error::Closed)?
    }
    async fn call(&self, command: Command) -> Result<Reply> {
        self.call_at(command, Instant::now()).await
    }

    pub fn health(&self) -> Health {
        self.health.borrow().clone()
    }
    pub fn watch_health(&self) -> watch::Receiver<Health> {
        self.health.clone()
    }
    pub async fn connected(&self) -> Result<Session> {
        match self.call(Command::Connected).await? {
            Reply::Session(s) => Ok(s),
            _ => unreachable!(),
        }
    }
    pub async fn connecting(&self) -> Result<Session> {
        match self.call(Command::Connecting).await? {
            Reply::Session(s) => Ok(s),
            _ => unreachable!(),
        }
    }
    pub async fn activate(&self, session: Session) -> Result<()> {
        self.call(Command::Activate(session)).await.map(|_| ())
    }
    pub async fn disconnected(&self, session: Session) -> Result<()> {
        self.call(Command::Disconnected(session)).await.map(|_| ())
    }
    pub async fn register(&self, key: OrderKey, original: Decimal) -> Result<OrderSnapshot> {
        match self.call(Command::Register(key, original)).await? {
            Reply::Order(s) => Ok(s),
            _ => unreachable!(),
        }
    }
    pub async fn observe(
        &self,
        update: Update,
        evidence: Evidence,
        received_at: Instant,
    ) -> Result<OrderSnapshot> {
        match self
            .call_at(Command::Observe(update, evidence), received_at)
            .await?
        {
            Reply::Order(s) => Ok(s),
            _ => unreachable!(),
        }
    }
    pub async fn begin_query(&self, key: OrderKey) -> Result<QueryTicket> {
        match self.call(Command::Query(key)).await? {
            Reply::Query(t) => Ok(t),
            _ => unreachable!(),
        }
    }
    /// Subscribe after register/ACK, or after the first pre-ACK event arrived.
    /// Read borrow_and_update() immediately before waiting on changed().
    pub async fn subscribe(&self, key: OrderKey) -> Result<watch::Receiver<OrderSnapshot>> {
        match self.call(Command::Subscribe(key)).await? {
            Reply::Subscription(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.call(Command::Shutdown).await.map(|_| ())
    }

    pub async fn ingest_aster_ws(
        &self,
        session: Session,
        raw: &str,
    ) -> Result<Option<OrderSnapshot>> {
        self.ingest_aster_ws_at(session, raw, Instant::now()).await
    }
    pub async fn ingest_aster_ws_at(
        &self,
        session: Session,
        raw: &str,
        received_at: Instant,
    ) -> Result<Option<OrderSnapshot>> {
        if self.scope.venue != "aster" {
            return Err(Error::WrongSession);
        }
        match aster::decode_ws(raw) {
            Ok(Some(update))
                if self
                    .position_symbol
                    .as_ref()
                    .is_some_and(|s| s != &update.key.symbol) =>
            {
                Ok(None)
            }
            Ok(Some(update)) => self
                .observe(update, Evidence::Ws(session), received_at)
                .await
                .map(Some),
            Ok(None) => {
                // Balance, funding, margin and configuration notifications invalidate
                // inventory proof even when they carry no regular order update.
                self.call_at(Command::AccountChanged(session), received_at)
                    .await?;
                Ok(None)
            }
            Err(Error::StreamExpired) => {
                self.disconnected(session).await?;
                Err(Error::StreamExpired)
            }
            Err(e) => {
                self.call(Command::Fault(e)).await?;
                Err(e)
            }
        }
    }
    pub async fn ingest_aster_rest(&self, ticket: QueryTicket, raw: &str) -> Result<OrderSnapshot> {
        if self.scope.venue != "aster" {
            return Err(Error::WrongSession);
        }
        let received_at = Instant::now();
        match aster::decode_rest(raw) {
            Ok(update) => {
                self.observe(update, Evidence::Rest(ticket), received_at)
                    .await
            }
            Err(e) => {
                self.call(Command::Fault(e)).await?;
                Err(e)
            }
        }
    }
}
