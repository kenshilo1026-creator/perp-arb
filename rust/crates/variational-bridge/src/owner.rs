//! Single blocking owner for the browser and paired execution WALs. Network I/O
//! stays outside this worker. No trading transport or synthetic account gate.
use crate::{native::Query, Result};
use perp_arb_execution::{
    durable::{self, browser, BrowserLookupTicket, DurableEngine, JournalError, Target},
    Intent,
};
use perp_arb_private_events::{account::Report, lookup::Outcome, Health, OrderKey, OrderSnapshot};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{mpsc, oneshot};

pub struct MakerLookup {
    browser: browser::LookupTicket,
    paired: BrowserLookupTicket,
    account: String,
}
impl MakerLookup {
    pub fn request(&self) -> crate::ReadRequest {
        crate::ReadRequest::NativeLookup {
            account: self.account.clone(),
            order_id: self.browser.order_id().into(),
        }
    }
}
pub struct Progress {
    pub maker: browser::View,
    pub exposure: String,
    pub in_flight: String,
    pub intents: Vec<durable::IntentState>,
    pub recovery_pending: Vec<Target>,
    pub faulted: bool,
}
type Reply<T> = oneshot::Sender<Result<T>>;
pub struct MakerRiskCheck {
    pub maker: Report,
    pub maker_health: Health,
    pub hedge: Report,
    pub hedge_health: Health,
    pub price_ok: bool,
    pub max_age: std::time::Duration,
}
enum Work {
    WsTrade(crate::wire::FillCandidate, Reply<Progress>),
    WireDispatch(
        String,
        perp_arb_private_events::orders::Order,
        perp_arb_core::Decimal,
        Reply<durable::DispatchPermit>,
    ),
    MakerDispatch(Box<MakerRiskCheck>, Reply<browser::DispatchPermit>),
    MakerSubmitted(Box<crate::native::Submission>, Reply<Progress>),
    MakerRecovered(Box<crate::native::Recovery>, Reply<Progress>),
    #[cfg(test)]
    Barrier(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>),
    Begin(Reply<MakerLookup>),
    Apply(MakerLookup, Box<Query>, Reply<Progress>),
    Progress(Reply<Progress>),
    Gap(Target, Reply<()>),
    Reserve(Reply<Option<Intent>>),
    ReserveConfirmed(Reply<Option<Intent>>),
    Dispatch(String, Reply<durable::DispatchPermit>),
    Acknowledge(String, OrderKey, Reply<()>),
    Hedge(String, OrderSnapshot, Reply<Progress>),
    HedgeLookup(String, Reply<durable::LookupTicket>),
    ApplyHedge(durable::LookupTicket, Outcome, Reply<Progress>),
    Cancel(Reply<browser::CancelPermit>),
}
struct State {
    maker: browser::Ledger,
    paired: DurableEngine,
}
impl State {
    fn progress(&self) -> Result<Progress> {
        Ok(Progress {
            maker: self.maker.view(),
            exposure: self.paired.exposure().map_err(err)?.to_string(),
            in_flight: self.paired.in_flight().map_err(err)?.to_string(),
            intents: self.paired.intents(),
            recovery_pending: self.paired.recovery_pending(),
            faulted: self.paired.faulted()
                || self.maker.view().poisoned
                || self.maker.view().trade_conflict,
        })
    }
    fn gap(&mut self, target: Target) -> Result<()> {
        // Fence paired dispatch first, even if the browser WAL subsequently fails.
        self.paired
            .require_reconciliation(target.clone())
            .map_err(err)?;
        if target == Target::Maker {
            self.maker.unresolved().map_err(err)?;
        }
        Ok(())
    }
    fn overflow(&mut self) -> Result<()> {
        self.gap(Target::Maker)?;
        for intent in self.paired.intents() {
            if intent.client_id.is_some() {
                self.gap(Target::Hedge(intent.intent.id))?;
            }
        }
        Ok(())
    }
    fn apply(&mut self, ticket: MakerLookup, query: Query) -> Result<Progress> {
        if let Err(error) = query.record(&mut self.maker, ticket.browser) {
            self.gap(Target::Maker)?;
            return Err(error);
        }
        // Reconcile before observe: observe changes the paired WAL revision and
        // would invalidate our own lookup ticket. Stale queries may still add
        // confirmed quantity but may not clear a recovery fence.
        match self
            .paired
            .reconcile_browser_maker(ticket.paired, &self.maker)
        {
            Ok(()) => (),
            Err(JournalError::RecoveryRequired) => self
                .paired
                .observe_browser_maker(&self.maker)
                .map_err(err)?,
            Err(e) => return Err(err(e)),
        }
        self.progress()
    }
    fn permits_allowed(&self) -> Result<()> {
        let v = self.maker.view();
        if v.poisoned || v.trade_conflict {
            return Err("maker ledger fault requires investigation".into());
        }
        Ok(())
    }
    fn run(&mut self, work: Work) {
        match work {
            Work::WsTrade(c, reply) => {
                let _ = reply.send((|| {
                    self.permits_allowed()?;
                    let v = self.maker.view();
                    if c.order_id != v.order_id
                        || c.symbol.as_ref() != Some(&v.command.symbol)
                        || c.side.as_ref() != Some(&v.command.side)
                        || c.status.as_deref() != Some("confirmed")
                        || c.trade_type.as_deref() != Some("trade")
                    {
                        return Err("WS maker identity/status mismatch".into());
                    }
                    let quantity = c.quantity.ok_or("WS quantity missing")?;
                    let price = c.price.ok_or("WS price missing")?;
                    let cumulative =
                        perp_arb_core::decimal(&quantity).map_err(|e| e.to_string())?;
                    let original =
                        perp_arb_core::decimal(&v.command.quantity).map_err(|e| e.to_string())?;
                    let now = std::time::Instant::now();
                    let snapshot = OrderSnapshot {
                        scope: perp_arb_private_events::Scope {
                            venue: "variational".into(),
                            account: v.command.account,
                        },
                        key: OrderKey {
                            symbol: v.command.symbol,
                            order_id: v.order_id.ok_or("RFQ not bound")?,
                        },
                        original,
                        cumulative,
                        status: if cumulative == original {
                            perp_arb_private_events::Status::Filled
                        } else {
                            perp_arb_private_events::Status::PartiallyFilled
                        },
                        event_ms: None,
                        revision: 0,
                        requires_reconciliation: true,
                        observed_at: now,
                        applied_at: now,
                    };
                    let ticket = self.maker.lookup_ticket().map_err(err)?;
                    self.maker
                        .apply_verified_trades(
                            ticket,
                            &snapshot,
                            vec![browser::ConfirmedTrade {
                                id: c.trade_id.ok_or("trade ID missing")?,
                                quantity,
                                price,
                            }],
                        )
                        .map_err(err)?;
                    self.paired
                        .observe_browser_maker(&self.maker)
                        .map_err(err)?;
                    self.progress()
                })());
            }
            Work::WireDispatch(id, order, cap, reply) => {
                let _ = reply.send((|| {
                    self.permits_allowed()?;
                    if order.reduce_only != self.maker.view().command.reduce_only {
                        return Err("both legs must preserve open/close reduce-only intent".into());
                    }
                    self.paired
                        .dispatch_browser_obligation(&self.maker, &id, order, cap)
                        .map_err(err)
                })());
            }
            Work::MakerDispatch(check, reply) => {
                let _ = reply.send((|| {
                    self.permits_allowed()?;
                    self.paired
                        .validate_new_browser_risk(
                            (&check.maker, &check.maker_health),
                            (&check.hedge, &check.hedge_health),
                            check.price_ok,
                            check.max_age,
                        )
                        .map_err(err)?;
                    self.maker.dispatch().map_err(err)
                })());
            }
            Work::MakerSubmitted(submission, reply) => {
                let result = submission
                    .record(&mut self.maker)
                    .and_then(|()| self.paired.bind_browser_maker(&self.maker).map_err(err))
                    .and_then(|()| self.progress());
                let _ = reply.send(result);
            }
            Work::MakerRecovered(recovery, reply) => {
                let result = recovery
                    .record(&mut self.maker)
                    .and_then(|()| self.paired.bind_browser_maker(&self.maker).map_err(err))
                    .and_then(|()| self.progress());
                let _ = reply.send(result);
            }
            #[cfg(test)]
            Work::Barrier(ready, release) => {
                let _ = ready.send(());
                release
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap();
            }
            Work::Begin(reply) => {
                let _ = reply.send((|| {
                    // Paired ticket must precede browser query start.
                    let paired = self.paired.begin_browser_reconciliation().map_err(err)?;
                    let browser = self.maker.lookup_ticket().map_err(err)?;
                    Ok(MakerLookup {
                        paired,
                        browser,
                        account: self.maker.view().command.account,
                    })
                })());
            }
            Work::Apply(ticket, query, reply) => {
                let _ = reply.send(self.apply(ticket, *query));
            }
            Work::Progress(reply) => {
                let _ = reply.send(self.progress());
            }
            Work::Gap(target, reply) => {
                let _ = reply.send(self.gap(target));
            }
            Work::Reserve(reply) => {
                let _ = reply.send(
                    self.permits_allowed()
                        .and_then(|()| self.paired.reserve_hedge().map_err(err)),
                );
            }
            Work::ReserveConfirmed(reply) => {
                let _ = reply.send(self.permits_allowed().and_then(|()| {
                    self.paired
                        .reserve_browser_obligation(&self.maker)
                        .map_err(err)
                }));
            }
            Work::Dispatch(id, reply) => {
                let _ = reply.send(
                    self.permits_allowed()
                        .and_then(|()| self.paired.prepare_dispatch(&id).map_err(err)),
                );
            }
            Work::Acknowledge(id, key, reply) => {
                let _ = reply.send(self.paired.acknowledge(&id, key).map_err(err));
            }
            Work::Hedge(id, snapshot, reply) => {
                let result = self
                    .paired
                    .observe(Target::Hedge(id), &snapshot)
                    .map_err(err)
                    .and_then(|()| self.progress());
                let _ = reply.send(result);
            }
            Work::HedgeLookup(id, reply) => {
                let _ = reply.send(self.paired.lookup_ticket(Target::Hedge(id)).map_err(err));
            }
            Work::ApplyHedge(ticket, outcome, reply) => {
                let result = self
                    .paired
                    .apply_lookup(ticket, outcome)
                    .map_err(err)
                    .and_then(|()| self.progress());
                let _ = reply.send(result);
            }
            Work::Cancel(reply) => {
                let _ = reply.send(self.maker.cancel_started().map_err(err));
            }
        }
    }
}
fn err(e: JournalError) -> String {
    e.to_string()
}

/// The factory runs on the blocking worker, including create/open/fsync. New
/// engines still need the existing independent account reports on both legs.
/// The caller owns transport, forwards every gap, and consumes linear permits.
/// This component does not submit orders or enable the live CLI.
pub struct Owner {
    tx: mpsc::Sender<Work>,
    overflow: Arc<AtomicBool>,
    worker: tokio::task::JoinHandle<Result<()>>,
}
impl Owner {
    // Called only after Validator's bound-RFQ, instrument and epoch checks.
    pub(crate) async fn apply_ws_trade(
        &self,
        trade: crate::wire::FillCandidate,
    ) -> Result<Progress> {
        self.call(|r| Work::WsTrade(trade, r)).await
    }
    pub async fn start<F>(capacity: usize, factory: F) -> Result<Self>
    where
        F: FnOnce() -> Result<(browser::Ledger, DurableEngine)> + Send + 'static,
    {
        if capacity == 0 || capacity > 1024 {
            return Err("owner capacity must be 1..=1024".into());
        }
        let (tx, mut rx) = mpsc::channel(capacity);
        let (ready, initialized) = oneshot::channel();
        let overflow = Arc::new(AtomicBool::new(false));
        let flag = overflow.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let (maker, mut paired) = match factory() {
                Ok(value) => value,
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return Ok(());
                }
            };
            if let Err(error) = paired.validate_browser_binding(&maker) {
                let _ = ready.send(Err(err(error)));
                return Ok(());
            }
            if maker.view().order_id.is_some() {
                if let Err(error) = paired.bind_browser_maker(&maker) {
                    let _ = ready.send(Err(err(error)));
                    return Ok(());
                }
            }
            // Crash between the two WAL writes: replay the maker's known fill
            // floor into the paired WAL before publishing any progress. Existing
            // restart/unknown-submission fences remain in place.
            let sync = match maker.maker_snapshot() {
                Ok(_) => paired.observe_browser_maker(&maker),
                Err(JournalError::RecoveryRequired) => Ok(()),
                Err(error) => Err(error),
            };
            if let Err(error) = sync {
                let _ = ready.send(Err(err(error)));
                return Ok(());
            }
            let mut state = State { maker, paired };
            if ready.send(Ok(())).is_err() {
                return Ok(());
            }
            while let Some(work) = rx.blocking_recv() {
                if flag.swap(false, Ordering::AcqRel) {
                    state.overflow()?;
                }
                state.run(work);
            }
            Ok(())
        });
        let initialized = initialized
            .await
            .unwrap_or_else(|_| Err("owner initialization stopped".into()));
        if let Err(error) = initialized {
            drop(tx);
            let _ = worker.await; // release both WAL locks before reporting failure
            return Err(error);
        }
        Ok(Self {
            tx,
            overflow,
            worker,
        })
    }
    async fn call<T>(&self, make: impl FnOnce(Reply<T>) -> Work) -> Result<T> {
        let (reply, receive) = oneshot::channel();
        match self.tx.try_send(make(reply)) {
            Ok(()) => (),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.overflow.store(true, Ordering::Release);
                return Err("owner queue full; reconciliation required".into());
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err("execution owner stopped".into())
            }
        }
        // Accepted evidence is processed even if the waiter is canceled. A lost
        // permit reply stays durably unknown; it can never be issued twice.
        receive.await.map_err(|_| "execution owner stopped")?
    }
    pub async fn begin_lookup(&self) -> Result<MakerLookup> {
        self.call(Work::Begin).await
    }
    pub async fn prepare_maker(&self, check: MakerRiskCheck) -> Result<browser::DispatchPermit> {
        self.call(|r| Work::MakerDispatch(Box::new(check), r)).await
    }
    pub async fn record_maker_submission(
        &self,
        submission: crate::native::Submission,
    ) -> Result<Progress> {
        self.call(|r| Work::MakerSubmitted(Box::new(submission), r))
            .await
    }
    pub async fn recover_maker(&self, recovery: crate::native::Recovery) -> Result<Progress> {
        self.call(|r| Work::MakerRecovered(Box::new(recovery), r))
            .await
    }
    pub async fn apply_lookup(&self, ticket: MakerLookup, query: Query) -> Result<Progress> {
        self.call(|r| Work::Apply(ticket, Box::new(query), r)).await
    }
    pub async fn progress(&self) -> Result<Progress> {
        self.call(Work::Progress).await
    }
    pub async fn gap(&self, target: Target) -> Result<()> {
        self.call(|r| Work::Gap(target, r)).await
    }
    pub async fn reserve_hedge(&self) -> Result<Option<Intent>> {
        self.call(Work::Reserve).await
    }
    pub async fn reserve_confirmed_hedge(&self) -> Result<Option<Intent>> {
        self.call(Work::ReserveConfirmed).await
    }
    pub async fn prepare_hedge(&self, id: String) -> Result<durable::DispatchPermit> {
        self.call(|r| Work::Dispatch(id, r)).await
    }
    pub async fn prepare_wire_hedge(
        &self,
        id: String,
        order: perp_arb_private_events::orders::Order,
        cap: perp_arb_core::Decimal,
    ) -> Result<durable::DispatchPermit> {
        self.call(|r| Work::WireDispatch(id, order, cap, r)).await
    }
    pub async fn acknowledge_hedge(&self, id: String, key: OrderKey) -> Result<()> {
        self.call(|r| Work::Acknowledge(id, key, r)).await
    }
    pub async fn observe_hedge(&self, id: String, snapshot: OrderSnapshot) -> Result<Progress> {
        self.call(|r| Work::Hedge(id, snapshot, r)).await
    }
    pub async fn begin_hedge_lookup(&self, id: String) -> Result<durable::LookupTicket> {
        self.call(|r| Work::HedgeLookup(id, r)).await
    }
    pub async fn apply_hedge_lookup(
        &self,
        ticket: durable::LookupTicket,
        outcome: Outcome,
    ) -> Result<Progress> {
        self.call(|r| Work::ApplyHedge(ticket, outcome, r)).await
    }
    pub async fn prepare_cancel(&self) -> Result<browser::CancelPermit> {
        self.call(Work::Cancel).await
    }
    /// Drain accepted work and release WAL locks. Dropping also closes the queue;
    /// callers needing proof of completion must await this method.
    pub async fn shutdown(self) -> Result<()> {
        drop(self.tx);
        self.worker.await.map_err(|_| "execution owner panicked")?
    }
}

#[cfg(test)]
mod tests;
