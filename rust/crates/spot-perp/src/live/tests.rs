use super::*;
use super::{
    journal::{Entry, Store},
    runtime::Owner,
    spot::{HedgeTransport, Timing},
};
use perp_arb_order_test::venue::Venue as Trading;
use perp_arb_private_events::{
    actor,
    lookup::{Found, Outcome, Request},
    orders::{Accepted, Order},
    Evidence, OrderKey, Scope, Status, Update,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

fn config() -> Config {
    let rules = policy::Rules {
        step: "1".into(),
        minimum: "1".into(),
        min_notional: "5".into(),
        maximum: None,
        max_notional: None,
    };
    Config {
        symbol: "ETH".into(),
        native_symbol: "ETHUSDT".into(),
        venue: "aster".into(),
        account: format!("0x{}", "a".repeat(40)),
        asset: None,
        mode: Mode::Close,
        total: "10".into(),
        clip: None,
        max_gap: "0.0025".into(),
        rules: rules.clone(),
        spot_rules: rules,
    }
}
fn books(c: &Config) -> Input {
    let mut i = c.input().unwrap();
    i.spot = policy::Book {
        bid: "100".into(),
        ask: "100.1".into(),
        bid_size: "10".into(),
        ask_size: "10".into(),
        age_ms: 0,
    };
    i.perp = policy::Book {
        bid: "100.1".into(),
        ask: "100.2".into(),
        bid_size: "10".into(),
        ask_size: "10".into(),
        age_ms: 0,
    };
    i
}
fn directory() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "spot-owner-test-{}",
        perp_arb_execution::durable::single::new_client_id("aster").unwrap()
    ));
    std::fs::create_dir(&p).unwrap();
    p
}
async fn store(c: Config) -> (PathBuf, Store) {
    let p = directory();
    let s = Store::create(p.join("run.jsonl"), c, "test-spot-account".into())
        .await
        .unwrap();
    (p, s)
}
fn intent(q: &str, id: &str) -> perp_arb_binance_spot::orders::Intent {
    perp_arb_binance_spot::orders::Intent {
        symbol: "ETHUSDT".into(),
        side: "SELL".into(),
        quantity: q.into(),
        client_id: id.into(),
        kind: perp_arb_binance_spot::orders::Kind::Market,
    }
}
async fn maker_fill(s: &Store, q: &str, terminal: bool) {
    let c = s.state().unwrap().config;
    s.record(Entry::Maker {
        order: c.order("10".into(), "100.1".into()).unwrap(),
    })
    .await
    .unwrap();
    s.record(Entry::Started { index: 0 }).await.unwrap();
    s.record(Entry::Bound {
        index: 0,
        id: "7".into(),
    })
    .await
    .unwrap();
    s.record(Entry::Fill {
        index: 0,
        quantity: q.into(),
        terminal,
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn durable_reservation_blocks_duplicate_hedge_after_restart_and_notfound() {
    let (p, s) = store(config()).await;
    maker_fill(&s, "5", false).await;
    s.record(Entry::Hedge {
        intent: intent("5", "pa-original"),
    })
    .await
    .unwrap();
    assert_eq!(s.state().unwrap().due().unwrap(), D::ZERO);
    assert!(s
        .record(Entry::Hedge {
            intent: intent("5", "pa-retry")
        })
        .await
        .is_err());
    drop(s);
    let s = Store::open(p.join("run.jsonl")).await.unwrap();
    assert_eq!(s.state().unwrap().due().unwrap(), D::ZERO);
    assert!(!s.state().unwrap().settled().unwrap());
    assert!(s.record(Entry::Finished).await.is_err());
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn cancel_fill_delta_reserves_only_unhedged_quantity_and_terminal_partial_releases_remainder()
{
    let (p, s) = store(config()).await;
    maker_fill(&s, "5", false).await;
    s.record(Entry::Hedge {
        intent: intent("5", "pa-one"),
    })
    .await
    .unwrap();
    s.record(Entry::Fill {
        index: 0,
        quantity: "8".into(),
        terminal: true,
    })
    .await
    .unwrap();
    assert_eq!(s.state().unwrap().due().unwrap(), D::from(3));
    s.record(Entry::Hedged {
        index: 0,
        quantity: "4".into(),
        terminal: true,
        base_delta: Some("-4".into()),
        fees: Default::default(),
    })
    .await
    .unwrap();
    assert_eq!(s.state().unwrap().due().unwrap(), D::from(4));
    assert!(s
        .record(Entry::Hedge {
            intent: intent("5", "pa-too-many")
        })
        .await
        .is_err());
    s.record(Entry::Hedge {
        intent: intent("4", "pa-two"),
    })
    .await
    .unwrap();
    s.record(Entry::Hedged {
        index: 1,
        quantity: "4".into(),
        terminal: true,
        base_delta: Some("-4".into()),
        fees: Default::default(),
    })
    .await
    .unwrap();
    assert!(s.state().unwrap().settled().unwrap());
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn stale_or_wide_gap_never_blocks_existing_fill_but_market_minimum_does() {
    let (p, s) = store(config()).await;
    maker_fill(&s, "5", false).await;
    let mut i = books(&config());
    i.spot.bid = "90".into();
    i.spot.age_ms = 9000;
    assert_eq!(
        runtime::hedge_quantity(&s.state().unwrap(), &i).unwrap(),
        D::from(5)
    );
    i.spot.bid = "0.1".into();
    i.spot.age_ms = 0;
    assert_eq!(
        runtime::hedge_quantity(&s.state().unwrap(), &i).unwrap(),
        D::ZERO
    );
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn journal_lock_torn_tail_identity_and_monotonic_fill_checks() {
    use std::io::Write;
    let (p, s) = store(config()).await;
    maker_fill(&s, "5", false).await;
    assert!(Store::open(p.join("run.jsonl")).await.is_err());
    assert!(s.record(Entry::Started { index: 0 }).await.is_err());
    assert!(s
        .record(Entry::Fill {
            index: 0,
            quantity: "4".into(),
            terminal: false
        })
        .await
        .is_err());
    assert!(s
        .record(Entry::Bound {
            index: 0,
            id: "8".into()
        })
        .await
        .is_err());
    drop(s);
    std::fs::OpenOptions::new()
        .append(true)
        .open(p.join("run.jsonl"))
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    assert!(Store::open(p.join("run.jsonl")).await.is_err());
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn live_role_is_explicit_even_when_binance_maker_would_be_better() {
    let mut i = books(&config());
    std::mem::swap(&mut i.spot, &mut i.perp);
    assert_eq!(
        policy::evaluate_perp_maker(&i)
            .unwrap()
            .candidate
            .unwrap()
            .maker,
        policy::Venue::Aster
    );
}

struct MockVenue {
    c: actor::Coordinator,
    session: perp_arb_private_events::Session,
    current: Arc<Mutex<Option<(Order, D, bool)>>>,
    submits: AtomicUsize,
    cancels: AtomicUsize,
    late_cancel: bool,
    lost_ack: bool,
}
impl MockVenue {
    async fn update(&self, order: &Order, q: D, terminal: bool) {
        let status = if terminal && q == d(&order.quantity).unwrap() {
            Status::Filled
        } else if terminal {
            Status::Canceled
        } else if q > D::ZERO {
            Status::PartiallyFilled
        } else {
            Status::New
        };
        self.c
            .observe(
                Update {
                    key: OrderKey {
                        symbol: order.symbol.clone(),
                        order_id: "7".into(),
                    },
                    original: d(&order.quantity).unwrap(),
                    cumulative: q,
                    status,
                    event_ms: Some(123),
                },
                Evidence::Ws(self.session.clone()),
                Instant::now(),
            )
            .await
            .unwrap();
    }
}
impl Trading for MockVenue {
    async fn submit(&self, o: &Order) -> Result<Accepted> {
        self.submits.fetch_add(1, Ordering::SeqCst);
        *self.current.lock().unwrap() = Some((o.clone(), D::from(4), false));
        self.update(o, D::from(4), false).await; // fill precedes HTTP acknowledgement
        self.update(o, D::from(4), false).await; // duplicate notification
        if !self.late_cancel {
            tokio::time::sleep(Duration::from_millis(15)).await;
            *self.current.lock().unwrap() = Some((o.clone(), D::from(10), true));
            self.update(o, D::from(10), true).await;
        }
        if self.lost_ack {
            Err("mock lost response".into())
        } else {
            Ok(Accepted {
                order_id: "7".into(),
                cumulative: None,
            })
        }
    }
    async fn cancel(&self, o: &Order) -> Result<()> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        let Some((_, old, _)) = self.current.lock().unwrap().clone() else {
            return Ok(());
        };
        let q = if self.late_cancel { D::from(6) } else { old };
        *self.current.lock().unwrap() = Some((o.clone(), q, true));
        self.update(o, q, true).await;
        Ok(())
    }
    async fn lookup(&self, r: &Request) -> Result<Outcome> {
        let Some((o, q, terminal)) = self.current.lock().unwrap().clone() else {
            return Ok(Outcome::NotFound);
        };
        let status = if terminal && q == d(&o.quantity)? {
            Status::Filled
        } else if terminal {
            Status::Canceled
        } else {
            Status::PartiallyFilled
        };
        Ok(Outcome::Found(Found {
            scope: r.scope.clone(),
            side: o.side.clone(),
            client_id: Some(o.client_id),
            order: Update {
                key: OrderKey {
                    symbol: o.symbol,
                    order_id: "7".into(),
                },
                original: d(&o.quantity)?,
                cumulative: q,
                status,
                event_ms: Some(124),
            },
        }))
    }
}
struct MockHedge {
    health: watch::Receiver<bool>,
    calls: AtomicUsize,
    quantities: Mutex<Vec<D>>,
    fail: bool,
}
impl HedgeTransport for MockHedge {
    fn health(&self) -> watch::Receiver<bool> {
        self.health.clone()
    }
    async fn sync(&self) -> Result<()> {
        Ok(())
    }
    async fn account(&self) -> Result<perp_arb_binance_spot::client::Account> {
        Ok(perp_arb_binance_spot::client::Account {
            can_trade: true,
            balances: BTreeMap::from([("ETH".into(), (D::from(100), D::ZERO))]),
        })
    }
    async fn execute(
        &self,
        s: Store,
        _: PathBuf,
        index: usize,
        received: Instant,
        source: String,
        risk: watch::Sender<bool>,
    ) -> Result<Timing> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let q = d(&s.state()?.hedges[index].intent.quantity)?;
        self.quantities.lock().unwrap().push(q);
        if self.fail {
            risk.send_replace(true);
            return Err("mock unknown hedge".into());
        }
        s.record(Entry::Hedged {
            index,
            quantity: q.to_string(),
            terminal: true,
            base_delta: Some((-q).to_string()),
            fees: Default::default(),
        })
        .await?;
        Ok(Timing {
            resumed: false,
            source,
            received_to_dispatch_ms: received.elapsed().as_secs_f64() * 1000.,
            dispatch_to_ack_ms: Some(1.),
            dispatch_to_confirmation_ms: 1.,
            confirmation_source: "mock_ws".into(),
        })
    }
}
async fn fixture(
    late_cancel: bool,
    lost_ack: bool,
    fail: bool,
) -> (
    PathBuf,
    Store,
    Owner<MockVenue, MockHedge>,
    watch::Sender<Input>,
    watch::Sender<bool>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let cfg = config();
    let (dir, store) = store(cfg.clone()).await;
    let (c, w) = actor::channel(
        Scope {
            venue: "aster".into(),
            account: cfg.account.clone(),
        },
        64,
        64,
    )
    .unwrap();
    let worker = w.spawn();
    let session = c.connected().await.unwrap();
    let v = Arc::new(MockVenue {
        c: c.clone(),
        session,
        current: Default::default(),
        submits: AtomicUsize::new(0),
        cancels: AtomicUsize::new(0),
        late_cancel,
        lost_ack,
    });
    let (b, books) = watch::channel(books(&cfg));
    let (hs, health) = watch::channel(true);
    let (ss, stop) = watch::channel(false);
    let h = Arc::new(MockHedge {
        health,
        calls: AtomicUsize::new(0),
        quantities: Mutex::new(vec![]),
        fail,
    });
    let owner = Owner {
        store: store.clone(),
        venue: v,
        coordinator: c,
        hedger: h,
        books,
        directory: dir.clone(),
        stop,
        timings: vec![],
        progress: None,
    };
    (dir, store, owner, b, hs, ss, worker)
}
#[tokio::test]
async fn real_owner_partial_pre_ack_and_duplicate_ws_never_double_hedges() {
    let (p, s, owner, _books, _health, _stop, worker) = fixture(false, false, false).await;
    let coord = owner.coordinator.clone();
    let v = owner.venue.clone();
    let h = owner.hedger.clone();
    tokio::time::timeout(Duration::from_secs(8), owner.execute(false))
        .await
        .unwrap()
        .unwrap();
    let state = s.state().unwrap();
    assert!(state.finished);
    assert_eq!(state.filled().unwrap(), D::from(10));
    assert_eq!(state.hedged().unwrap(), D::from(10));
    assert_eq!(v.submits.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.quantities.lock().unwrap().iter().copied().sum::<D>(),
        D::from(10)
    );
    coord.shutdown().await.unwrap();
    worker.await.unwrap();
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn real_owner_stop_cancels_then_hedges_late_fill_without_new_maker() {
    let (p, s, owner, _books, _health, stop, worker) = fixture(true, false, false).await;
    let coord = owner.coordinator.clone();
    let v = owner.venue.clone();
    let signal = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop.send_replace(true);
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    tokio::time::timeout(Duration::from_secs(8), owner.execute(false))
        .await
        .unwrap()
        .unwrap();
    let state = s.state().unwrap();
    assert!(state.finished);
    assert_eq!(state.filled().unwrap(), D::from(6));
    assert_eq!(state.hedged().unwrap(), D::from(6));
    assert_eq!(v.submits.load(Ordering::SeqCst), 1);
    assert!(v.cancels.load(Ordering::SeqCst) > 0);
    signal.abort();
    let _ = signal.await;
    coord.shutdown().await.unwrap();
    worker.await.unwrap();
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn lost_ack_reconciles_original_identity_and_returns_error_without_new_maker() {
    let (p, s, owner, _books, _health, _stop, worker) = fixture(false, true, false).await;
    let coord = owner.coordinator.clone();
    let v = owner.venue.clone();
    let result = tokio::time::timeout(Duration::from_secs(8), owner.execute(false))
        .await
        .unwrap();
    assert!(result.is_err());
    assert!(s.state().unwrap().settled().unwrap());
    assert_eq!(v.submits.load(Ordering::SeqCst), 1);
    coord.shutdown().await.unwrap();
    worker.await.unwrap();
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}

#[tokio::test]
async fn uncertain_hedge_stops_maker_and_preserves_original_reservation_for_recovery() {
    let (p, s, owner, _books, _health, _stop, worker) = fixture(true, false, true).await;
    let coord = owner.coordinator.clone();
    let venue = owner.venue.clone();
    assert!(
        tokio::time::timeout(Duration::from_secs(8), owner.execute(false))
            .await
            .unwrap()
            .is_err()
    );
    let state = s.state().unwrap();
    assert!(!state.finished);
    assert_eq!(state.hedges.len(), 1);
    assert!(!state.hedges[0].terminal);
    assert!(venue.cancels.load(Ordering::SeqCst) > 0);
    assert_eq!(venue.submits.load(Ordering::SeqCst), 1);
    coord.shutdown().await.unwrap();
    worker.await.unwrap();
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}

#[tokio::test]
async fn amount_changes_do_not_cancel_but_unsafe_price_change_does() {
    let (p, s, owner, quotes, _health, stop, worker) = fixture(true, false, false).await;
    let coord = owner.coordinator.clone();
    let venue = owner.venue.clone();
    let checked = venue.clone();
    let controls = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        let mut i = quotes.borrow().clone();
        i.spot.bid_size = "1".into();
        quotes.send_replace(i);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(checked.cancels.load(Ordering::SeqCst), 0);
        let mut i = quotes.borrow().clone();
        i.spot.bid = "90".into();
        quotes.send_replace(i);
        tokio::time::sleep(Duration::from_millis(80)).await;
        stop.send_replace(true);
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    tokio::time::timeout(Duration::from_secs(8), owner.execute(false))
        .await
        .unwrap()
        .unwrap();
    assert!(venue.cancels.load(Ordering::SeqCst) > 0);
    assert_eq!(s.state().unwrap().hedged().unwrap(), D::from(6));
    controls.await.unwrap();
    coord.shutdown().await.unwrap();
    worker.await.unwrap();
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}

#[tokio::test]
async fn restart_recovery_queries_saved_maker_without_new_submission_and_preserves_fees() {
    let (p, s, owner, _books, _health, _stop, worker) = fixture(true, false, false).await;
    maker_fill(&s, "4", false).await;
    let original = s.state().unwrap().makers[0].order.clone();
    *owner.venue.current.lock().unwrap() = Some((original, D::from(4), false));
    let coord = owner.coordinator.clone();
    let venue = owner.venue.clone();
    tokio::time::timeout(Duration::from_secs(8), owner.execute(true))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(venue.submits.load(Ordering::SeqCst), 0);
    assert_eq!(s.state().unwrap().hedged().unwrap(), D::from(6));
    assert!(s.state().unwrap().finished);
    assert_eq!(s.state().unwrap().net_base_delta().unwrap(), Some(D::ZERO));
    coord.shutdown().await.unwrap();
    worker.await.unwrap();
    drop(s);
    let replay = Store::open(p.join("run.jsonl")).await.unwrap();
    assert!(replay.state().unwrap().finished);
    assert_eq!(
        replay.state().unwrap().net_base_delta().unwrap(),
        Some(D::ZERO)
    );
    drop(replay);
    std::fs::remove_dir_all(p).unwrap();
}

#[tokio::test]
async fn stale_maker_keeps_queue_and_hedges_fills_but_stale_hedge_cancels() {
    let (p, s, owner, quotes, _health, stop, worker) = fixture(true, false, false).await;
    let coord = owner.coordinator.clone();
    let venue = owner.venue.clone();
    let task = tokio::spawn(owner.execute(false));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if s.state().unwrap().hedged().unwrap() == D::from(4) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    for age in [1035, 0, 1008, 0, 1014] {
        let mut i = quotes.borrow().clone();
        i.perp.age_ms = age;
        quotes.send_replace(i);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(venue.cancels.load(Ordering::SeqCst), 0);
        assert_eq!(venue.submits.load(Ordering::SeqCst), 1);
    }
    let original = s.state().unwrap().makers[0].order.clone();
    *venue.current.lock().unwrap() = Some((original.clone(), D::from(5), false));
    venue.update(&original, D::from(5), false).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if s.state().unwrap().hedged().unwrap() == D::from(5) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(venue.cancels.load(Ordering::SeqCst), 0);
    let mut i = quotes.borrow().clone();
    i.spot.age_ms = 1001;
    quotes.send_replace(i);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if s.state().unwrap().settled().unwrap() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(venue.submits.load(Ordering::SeqCst), 1);
    assert_eq!(venue.cancels.load(Ordering::SeqCst), 1);
    assert_eq!(s.state().unwrap().hedged().unwrap(), D::from(6));
    let rows: Vec<serde_json::Value> = std::fs::read_to_string(p.join("process.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(rows.iter().any(|r| r["event"] == "maker-hold"));
    assert_eq!(
        rows.iter().find(|r| r["event"] == "cancel").unwrap()["details"]["reason"],
        "stale_quote"
    );
    coord.shutdown().await.unwrap();
    worker.await.unwrap();
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}

#[tokio::test]
async fn base_commission_is_persisted_and_reported_as_net_residual_not_invented_hedge() {
    let (p, s) = store(config()).await;
    maker_fill(&s, "10", true).await;
    s.record(Entry::Hedge {
        intent: intent("10", "pa-fees"),
    })
    .await
    .unwrap();
    s.record(Entry::Hedged {
        index: 0,
        quantity: "10".into(),
        terminal: true,
        base_delta: Some("-10.01".into()),
        fees: BTreeMap::from([("ETH".into(), "0.01".into())]),
    })
    .await
    .unwrap();
    s.record(Entry::Finished).await.unwrap();
    drop(s);
    let replay = Store::open(p.join("run.jsonl")).await.unwrap();
    assert_eq!(
        replay.state().unwrap().net_base_delta().unwrap(),
        Some(d("-0.01").unwrap())
    );
    assert_eq!(replay.state().unwrap().hedges.len(), 1);
    drop(replay);
    std::fs::remove_dir_all(p).unwrap();
}

/// Deterministically delay submit and lookup independently, including a negative
/// lookup taken BEFORE the order exists but returned AFTER the successful ACK.
struct RaceVenue {
    inner: Arc<MockVenue>,
    submitting: tokio::sync::Notify,
    allow_submit: tokio::sync::Notify,
    querying: tokio::sync::Notify,
    allow_lookup: tokio::sync::Notify,
    first_lookup: AtomicUsize,
    negative_lookup: bool,
    lookup_error: bool,
}
impl Trading for RaceVenue {
    async fn submit(&self, o: &Order) -> Result<Accepted> {
        self.submitting.notify_one();
        self.allow_submit.notified().await;
        self.inner.submit(o).await
    }
    async fn cancel(&self, o: &Order) -> Result<()> {
        self.inner.cancel(o).await
    }
    async fn lookup(&self, r: &Request) -> Result<Outcome> {
        if self.negative_lookup && self.first_lookup.fetch_add(1, Ordering::SeqCst) == 0 {
            self.querying.notify_one();
            self.allow_lookup.notified().await;
            return if self.lookup_error {
                Err("old request timed out".into())
            } else {
                Ok(Outcome::NotFound)
            };
        }
        self.inner.lookup(r).await
    }
}
async fn race_case(negative_lookup: bool, lookup_error: bool, persistent_gap: bool) {
    let (p, s, owner, quotes, _health, stop, worker) = fixture(true, false, false).await;
    let coord = owner.coordinator.clone();
    let venue = Arc::new(RaceVenue {
        inner: owner.venue,
        submitting: Default::default(),
        allow_submit: Default::default(),
        querying: Default::default(),
        allow_lookup: Default::default(),
        first_lookup: AtomicUsize::new(0),
        negative_lookup,
        lookup_error,
    });
    let owner = Owner {
        store: owner.store,
        venue: venue.clone(),
        coordinator: owner.coordinator,
        hedger: owner.hedger,
        books: owner.books,
        directory: owner.directory,
        stop: owner.stop,
        timings: vec![],
        progress: None,
    };
    let task = tokio::spawn(owner.execute(false));
    tokio::time::timeout(Duration::from_secs(3), venue.submitting.notified())
        .await
        .unwrap();
    if negative_lookup {
        tokio::time::timeout(Duration::from_secs(3), venue.querying.notified())
            .await
            .unwrap();
    } else {
        let mut i = quotes.borrow().clone();
        if persistent_gap {
            i.spot.bid = "90".into();
        } else {
            i.perp.bid = "100.11".into();
        }
        quotes.send_replace(i);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(venue.inner.cancels.load(Ordering::SeqCst), 0);
        if !persistent_gap {
            quotes.send_replace(books(&config()));
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
    }
    venue.allow_submit.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if s.state().unwrap().makers[0].id.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    venue.allow_lookup.notify_one();
    tokio::time::sleep(Duration::from_millis(200)).await;
    if persistent_gap {
        assert!(venue.inner.cancels.load(Ordering::SeqCst) > 0);
    } else {
        assert_eq!(
            venue.inner.cancels.load(Ordering::SeqCst),
            0,
            "transient quote/old negative lookup must not cancel the acknowledged order"
        );
    }
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(8), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(venue.inner.submits.load(Ordering::SeqCst), 1);
    assert_eq!(
        s.state().unwrap().filled().unwrap(),
        s.state().unwrap().hedged().unwrap()
    );
    let rows: Vec<serde_json::Value> = std::fs::read_to_string(p.join("process.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(rows.iter().all(|r| r["time_ms"].as_u64().unwrap() > 0));
    for event in [
        "maker-plan",
        "maker-submit",
        "maker-ack",
        "cancel",
        "cancel-result",
        "maker-terminal",
        "hedge",
        "hedge-confirmed",
        "complete",
    ] {
        assert!(rows.iter().any(|r| r["event"] == event), "missing {event}");
    }
    let cancel = rows.iter().find(|r| r["event"] == "cancel").unwrap();
    assert_eq!(
        cancel["details"]["reason"],
        if persistent_gap {
            "adverse_gap"
        } else {
            "user_stop"
        }
    );
    assert!(cancel["details"]["books"]["spot"]["age_ms"].is_number());
    assert_eq!(
        rows.iter().find(|r| r["event"] == "hedge").unwrap()["details"]["quantity"],
        "4"
    );
    coord.shutdown().await.unwrap();
    worker.await.unwrap();
    drop(s);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn transient_price_change_during_submit_does_not_latch_cancel() {
    race_case(false, false, false).await;
}
#[tokio::test]
async fn pre_ack_notfound_arriving_after_ack_does_not_cancel() {
    race_case(true, false, false).await;
}
#[tokio::test]
async fn pre_ack_lookup_error_arriving_after_ack_does_not_cancel() {
    race_case(true, true, false).await;
}
#[tokio::test]
async fn persistent_unsafe_gap_during_submit_still_cancels_and_hedges() {
    race_case(false, false, true).await;
}
