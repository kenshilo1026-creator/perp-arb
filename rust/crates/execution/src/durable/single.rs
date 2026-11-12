//! Full single-order submission journal for the explicit order-test CLI.
//! Shares the locked, hashed, sync_all journal with the paper hedge coordinator.
use super::*;
use perp_arb_private_events::orders::Order;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", deny_unknown_fields)]
enum Entry {
    Prepared {
        order: Order,
        max_notional: String,
    },
    Started,
    Accepted {
        order_id: String,
        cumulative: Option<String>,
    },
    Observed {
        order: OrderEvidence,
    },
    CancelStarted,
    Unresolved,
}
#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub order: Order,
    pub max_notional: String,
    pub dispatched: bool,
    pub cancel_attempts: u32,
    pub order_id: Option<String>,
    pub cumulative: String,
    pub terminal: bool,
    pub faulted: bool,
    pub reconciled: bool,
}
pub struct Permit {
    order: Order,
}
impl Permit {
    pub fn order(&self) -> &Order {
        &self.order
    }
}
pub struct Ticket {
    instance: [u8; 16],
    sequence: u64,
    started: Instant,
    request: Request,
}
impl Ticket {
    pub fn request(&self) -> &Request {
        &self.request
    }
}
pub struct Ledger {
    journal: Journal,
    view: View,
    instance: [u8; 16],
    poisoned: bool,
}
pub fn new_client_id(venue: &str) -> JResult<String> {
    let id = random()?
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    match venue {
        "aster" => Ok(format!("pa{id}")),
        "hyperliquid" => Ok(format!("0x{id}")),
        _ => Err(JournalError::Identity),
    }
}
fn check(order: &Order, max: &str) -> JResult<()> {
    order.validate().map_err(|_| JournalError::Identity)?;
    let maximum = qty(max)?;
    if maximum <= Decimal::ZERO {
        return Err(JournalError::InvalidEvidence);
    }
    let amount = perp_arb_core::instruments::ExactAmount::product(
        order
            .quantity()
            .map_err(|_| JournalError::InvalidEvidence)?,
        order.price().map_err(|_| JournalError::InvalidEvidence)?,
    )
    .map_err(|_| JournalError::InvalidEvidence)?;
    if amount
        .compare(maximum)
        .map_err(|_| JournalError::InvalidEvidence)?
        .is_gt()
    {
        return Err(JournalError::InvalidEvidence);
    }
    Ok(())
}
fn wrap(entry: Entry) -> JResult<Event> {
    Ok(Event::SingleOrder {
        payload: serde_json::to_value(entry).map_err(|_| JournalError::Corrupt)?,
    })
}
fn unwrap(event: Event) -> JResult<Entry> {
    match event {
        Event::SingleOrder { payload } => {
            serde_json::from_value(payload).map_err(|_| JournalError::Corrupt)
        }
        _ => Err(JournalError::Corrupt),
    }
}
impl Ledger {
    pub fn create(path: &Path, order: Order, max_notional: String) -> JResult<Self> {
        check(&order, &max_notional)?;
        let instance = random()?;
        let mut journal = Journal::create(path)?;
        journal.append(wrap(Entry::Prepared {
            order: order.clone(),
            max_notional: max_notional.clone(),
        })?)?;
        Ok(Self {
            journal,
            view: View {
                order,
                max_notional,
                dispatched: false,
                cancel_attempts: 0,
                order_id: None,
                cumulative: "0".into(),
                terminal: false,
                faulted: false,
                reconciled: false,
            },
            instance,
            poisoned: false,
        })
    }
    pub fn open(path: &Path) -> JResult<Self> {
        let (journal, events) = Journal::open(path)?;
        let mut entries = events.into_iter().map(unwrap);
        let (order, max_notional) = match entries.next().ok_or(JournalError::Corrupt)?? {
            Entry::Prepared {
                order,
                max_notional,
            } => (order, max_notional),
            _ => return Err(JournalError::Corrupt),
        };
        check(&order, &max_notional)?;
        let mut view = View {
            order,
            max_notional,
            dispatched: false,
            cancel_attempts: 0,
            order_id: None,
            cumulative: "0".into(),
            terminal: false,
            faulted: false,
            reconciled: false,
        };
        for entry in entries {
            Self::apply(&mut view, &entry?)?;
        }
        // Terminal is historical; every resume must make a fresh lookup, never resubmit.
        Ok(Self {
            journal,
            view,
            instance: random()?,
            poisoned: false,
        })
    }
    pub fn view(&self) -> View {
        let mut v = self.view.clone();
        v.faulted |= self.poisoned;
        v
    }
    fn apply(v: &mut View, e: &Entry) -> JResult<()> {
        match e {
            Entry::Prepared { .. } => return Err(JournalError::Corrupt),
            Entry::Started => {
                if v.dispatched {
                    return Err(JournalError::AlreadyDispatched);
                }
                v.dispatched = true;
            }
            Entry::Accepted {
                order_id,
                cumulative,
            } => {
                if !v.dispatched
                    || order_id.parse::<u64>().is_err()
                    || v.order_id.as_ref().is_some_and(|id| id != order_id)
                {
                    return Err(JournalError::Identity);
                }
                v.order_id = Some(order_id.clone());
                if let Some(cumulative) = cumulative {
                    let amount = qty(cumulative)?;
                    if amount < Decimal::ZERO
                        || amount
                            > v.order
                                .quantity()
                                .map_err(|_| JournalError::InvalidEvidence)?
                    {
                        return Err(JournalError::InvalidEvidence);
                    }
                    if amount > qty(&v.cumulative)? {
                        v.faulted |= v.terminal;
                        v.cumulative = amount.to_string();
                        v.terminal = false;
                    }
                }
            }
            Entry::CancelStarted => {
                if !v.dispatched {
                    return Err(JournalError::Identity);
                }
                v.cancel_attempts = v
                    .cancel_attempts
                    .checked_add(1)
                    .ok_or(JournalError::Capacity)?;
            }
            Entry::Unresolved => (),
            Entry::Observed { order } => {
                let s = order.snapshot(0)?;
                if !v.dispatched
                    || s.scope != v.order.request().map_err(|_| JournalError::Identity)?.scope
                    || s.key.symbol != v.order.symbol
                    || s.original != v.order.quantity().map_err(|_| JournalError::Identity)?
                    || v.order_id.as_ref().is_some_and(|id| *id != s.key.order_id)
                {
                    return Err(JournalError::Identity);
                }
                let old = qty(&v.cumulative)?;
                if s.cumulative < old {
                    return Err(JournalError::InvalidEvidence);
                }
                if v.terminal && (s.cumulative != old || !s.status.terminal()) {
                    v.faulted = true;
                }
                v.cumulative = s.cumulative.to_string();
                v.order_id = Some(s.key.order_id);
                v.terminal = s.status.terminal() && !s.requires_reconciliation;
            }
        }
        Ok(())
    }
    fn commit(&mut self, e: Entry) -> JResult<()> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        let mut v = self.view.clone();
        v.reconciled = false;
        Self::apply(&mut v, &e)?;
        if let Err(error) = self.journal.append(wrap(e)?) {
            self.view = v;
            self.poisoned = true;
            return Err(error);
        }
        self.view = v;
        Ok(())
    }
    pub fn dispatch(&mut self) -> JResult<Permit> {
        if self.view.faulted {
            return Err(JournalError::InvalidEvidence);
        }
        self.commit(Entry::Started)?;
        Ok(Permit {
            order: self.view.order.clone(),
        })
    }
    pub fn accepted(&mut self, id: String) -> JResult<()> {
        self.accepted_result(perp_arb_private_events::orders::Accepted {
            order_id: id,
            cumulative: None,
        })
    }
    pub fn accepted_result(
        &mut self,
        ack: perp_arb_private_events::orders::Accepted,
    ) -> JResult<()> {
        self.commit(Entry::Accepted {
            order_id: ack.order_id,
            cumulative: ack.cumulative,
        })
    }
    pub fn cancel_started(&mut self) -> JResult<()> {
        self.commit(Entry::CancelStarted)
    }
    pub fn unresolved(&mut self) -> JResult<()> {
        self.commit(Entry::Unresolved)
    }
    pub fn ticket(&self) -> JResult<Ticket> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        Ok(Ticket {
            instance: self.instance,
            sequence: self.journal.sequence,
            started: Instant::now(),
            request: self
                .view
                .order
                .request()
                .map_err(|_| JournalError::Identity)?,
        })
    }
    pub fn lookup(&mut self, t: Ticket, outcome: Outcome) -> JResult<()> {
        if t.instance != self.instance {
            return Err(JournalError::Identity);
        }
        match outcome {
            Outcome::NotFound => self.unresolved(),
            Outcome::Found(f) => {
                t.request.verify(&f).map_err(|_| JournalError::Identity)?;
                if f.order.cumulative < qty(&self.view.cumulative)? {
                    return self.unresolved();
                }
                let now = Instant::now();
                let s = OrderSnapshot {
                    scope: f.scope,
                    key: f.order.key,
                    original: f.order.original,
                    cumulative: f.order.cumulative,
                    status: f.order.status,
                    event_ms: f.order.event_ms,
                    revision: 0,
                    requires_reconciliation: t.sequence != self.journal.sequence
                        || t.started.elapsed() > Duration::from_secs(30)
                        || f.order.event_ms.is_none(),
                    observed_at: now,
                    applied_at: now,
                };
                let fresh = !s.requires_reconciliation;
                self.commit(Entry::Observed {
                    order: OrderEvidence::from_snapshot(&s),
                })?;
                self.view.reconciled = fresh;
                Ok(())
            }
        }
    }
    /// Only accepts a previously bound order; coordinator retains pre-ACK events.
    /// WS contributes fills, but this manual test requires REST before declaring completion.
    pub fn observe(&mut self, s: &OrderSnapshot) -> JResult<()> {
        validate(s).map_err(|_| JournalError::InvalidEvidence)?;
        if self.view.order_id.as_ref() != Some(&s.key.order_id)
            || s.scope
                != self
                    .view
                    .order
                    .request()
                    .map_err(|_| JournalError::Identity)?
                    .scope
            || s.key.symbol != self.view.order.symbol
            || s.original
                != self
                    .view
                    .order
                    .quantity()
                    .map_err(|_| JournalError::Identity)?
        {
            return Err(JournalError::Identity);
        }
        if s.cumulative < qty(&self.view.cumulative)? {
            return Ok(());
        }
        let mut e = OrderEvidence::from_snapshot(s);
        e.gap = true;
        // Preserve historical terminal confirmation on identical terminal replay.
        if self.view.terminal && s.status.terminal() && s.cumulative == qty(&self.view.cumulative)?
        {
            e.gap = false;
        }
        self.commit(Entry::Observed { order: e })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_ack_fill_survives_unknown_lookup_and_restart_without_certifying_terminal() {
        let p = path();
        let mut l = Ledger::create(&p, order(), "25".into()).unwrap();
        l.dispatch().unwrap();
        l.accepted_result(perp_arb_private_events::orders::Accepted {
            order_id: "7".into(),
            cumulative: Some("0.01".into()),
        })
        .unwrap();
        let t = l.ticket().unwrap();
        l.lookup(t, Outcome::NotFound).unwrap();
        assert_eq!(l.view().cumulative, "0.01");
        assert!(!l.view().terminal && !l.view().reconciled);
        drop(l);
        let mut l = Ledger::open(&p).unwrap();
        assert_eq!(l.view().cumulative, "0.01");
        let t = l.ticket().unwrap();
        let stale = outcome(&l, "0.005", Status::Canceled);
        l.lookup(t, stale).unwrap();
        assert_eq!(l.view().cumulative, "0.01");
        assert!(!l.view().terminal && !l.view().reconciled);
        let t = l.ticket().unwrap();
        let final_state = outcome(&l, "0.01", Status::Filled);
        l.lookup(t, final_state).unwrap();
        assert!(l.view().terminal && l.view().reconciled);
        drop(l);
        std::fs::remove_file(p).unwrap();
    }
    fn outcome(l: &Ledger, q: &str, status: Status) -> Outcome {
        let r = l.view.order.request().unwrap();
        Outcome::Found(perp_arb_private_events::lookup::Found {
            scope: r.scope,
            side: r.side,
            client_id: Some(l.view.order.client_id.clone()),
            order: Update {
                key: OrderKey {
                    symbol: r.symbol,
                    order_id: "7".into(),
                },
                original: r.original,
                cumulative: qty(q).unwrap(),
                status,
                event_ms: Some(100),
            },
        })
    }
    #[test]
    fn terminal_reopen_is_not_fresh_and_unknown_cannot_certify_it() {
        let p = path();
        let mut l = Ledger::create(&p, order(), "25".into()).unwrap();
        l.dispatch().unwrap();
        l.accepted("7".into()).unwrap();
        assert_eq!(l.view().cumulative, "0");
        let t = l.ticket().unwrap();
        let f = outcome(&l, "0.005", Status::Canceled);
        l.lookup(t, f).unwrap();
        assert!(l.view().terminal && l.view().reconciled);
        drop(l);
        let mut l = Ledger::open(&p).unwrap();
        assert!(!l.view().reconciled);
        let t = l.ticket().unwrap();
        l.lookup(t, Outcome::NotFound).unwrap();
        assert!(!l.view().reconciled);
        assert_eq!(l.view().cumulative, "0.005");
        drop(l);
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn wrong_client_id_and_stale_query_cannot_release_unknown() {
        let p = path();
        let mut l = Ledger::create(&p, order(), "25".into()).unwrap();
        l.dispatch().unwrap();
        let t = l.ticket().unwrap();
        let Outcome::Found(mut f) = outcome(&l, "0.005", Status::Canceled) else {
            panic!()
        };
        f.client_id = Some("wrong".into());
        assert!(l.lookup(t, Outcome::Found(f)).is_err());
        let t = l.ticket().unwrap();
        let f = outcome(&l, "0.005", Status::Canceled);
        l.cancel_started().unwrap();
        l.lookup(t, f).unwrap();
        assert!(!l.view().reconciled);
        assert!(!l.view().terminal);
        assert_eq!(l.view().cumulative, "0.005");
        let t = l.ticket().unwrap();
        let f = outcome(&l, "0.005", Status::Canceled);
        l.lookup(t, f).unwrap();
        assert!(l.view().terminal);
        drop(l);
        std::fs::remove_file(p).unwrap();
    }
    fn order() -> Order {
        Order {
            venue: "aster".into(),
            network: perp_arb_private_events::orders::Network::Mainnet,
            account: format!("0x{:040x}", 1),
            symbol: "ETHUSDT".into(),
            side: "BUY".into(),
            quantity: "0.01".into(),
            limit_price: "2000".into(),
            tif: perp_arb_private_events::orders::TimeInForce::PostOnly,
            reduce_only: false,
            client_id: new_client_id("aster").unwrap(),
            asset: None,
        }
    }
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("{}.wal", new_client_id("aster").unwrap()))
    }
    #[test]
    fn complete_intent_is_durable_before_dispatch_and_not_found_cannot_resubmit() {
        let p = path();
        let o = order();
        let mut l = Ledger::create(&p, o.clone(), "25".into()).unwrap();
        assert_eq!(l.dispatch().unwrap().order(), &o);
        drop(l);
        let mut l = Ledger::open(&p).unwrap();
        assert_eq!(l.view().order, o);
        assert!(matches!(l.dispatch(), Err(JournalError::AlreadyDispatched)));
        let t = l.ticket().unwrap();
        l.lookup(t, Outcome::NotFound).unwrap();
        assert!(!l.view().terminal);
        l.cancel_started().unwrap();
        assert!(!l.view().terminal);
        drop(l);
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn monetary_cap_checked_exactly_and_corrupt_tail_not_repaired() {
        let p = path();
        assert!(Ledger::create(&p, order(), "19.99999999999999999999999999".into()).is_err());
        assert!(!p.exists());
        let l = Ledger::create(&p, order(), "20".into()).unwrap();
        drop(l);
        let mut b = std::fs::read(&p).unwrap();
        b.pop();
        std::fs::write(&p, &b).unwrap();
        assert!(matches!(Ledger::open(&p), Err(JournalError::Corrupt)));
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn failed_sync_does_not_issue_permit() {
        let p = path();
        let mut l = Ledger::create(&p, order(), "20".into()).unwrap();
        l.journal.fail = Some(false);
        assert!(l.dispatch().is_err());
        assert!(l.view().faulted);
        drop(l);
        let l = Ledger::open(&p).unwrap();
        assert!(l.view().dispatched);
        drop(l);
        std::fs::remove_file(p).unwrap();
    }
}
