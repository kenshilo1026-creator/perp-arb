use super::*;
use perp_arb_private_events::{account::Snapshot, actor, lookup::Found};
use std::{collections::BTreeSet, path::PathBuf};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "perp-arb-journal-{:x}.wal",
            u128::from_le_bytes(random().unwrap())
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn d(s: &str) -> Decimal {
    qty(s).unwrap()
}
fn key(symbol: &str, id: &str) -> OrderKey {
    OrderKey {
        symbol: symbol.into(),
        order_id: id.into(),
    }
}
async fn setup() -> (Temp, DurableEngine, Plan) {
    setup_venues("aster", "hyperliquid").await
}
async fn setup_venues(maker_venue: &str, hedge_venue: &str) -> (Temp, DurableEngine, Plan) {
    let temp = Temp::new();
    let scope = |venue: &str| Scope {
        venue: venue.into(),
        account: "fixture".into(),
    };
    let (m, mw) = actor::channel(scope(maker_venue), 32, 8).unwrap();
    let (h, hw) = actor::channel(scope(hedge_venue), 32, 8).unwrap();
    let mt = mw.spawn();
    let ht = hw.spawn();
    for c in [&m, &h] {
        c.connected().await.unwrap();
        let inv = Inventory {
            perp_dexes: BTreeSet::from([String::new()]),
            includes_wallets: true,
            ..Inventory::default()
        };
        let t = c.begin_account_query().await.unwrap();
        let now = Instant::now();
        c.apply_account_snapshot(
            t,
            Snapshot {
                scope: c.scope().clone(),
                inventory: inv.clone(),
                started_at: now,
                completed_at: now,
                exchange_time_ms: Some(1000),
            },
            Some(inv),
        )
        .await
        .unwrap();
    }
    let p = Plan {
        run_id: "durable1".into(),
        maker: Leg {
            scope: m.scope().clone(),
            symbol: "ETHUSDT".into(),
            side: Side::Sell,
        },
        hedge: Leg {
            scope: h.scope().clone(),
            symbol: "ETH".into(),
            side: Side::Buy,
        },
        maker_order: key("ETHUSDT", "1"),
        quantity: d("10"),
        step: d("0.1"),
        minimum: d("0.2"),
    };
    let mr = m.account_report().await.unwrap().unwrap();
    let hr = h.account_report().await.unwrap().unwrap();
    let e = DurableEngine::create(
        &temp.0,
        p.clone(),
        (&mr, &m.health()),
        (&hr, &h.health()),
        true,
        Duration::from_secs(5),
    )
    .unwrap();
    m.shutdown().await.unwrap();
    h.shutdown().await.unwrap();
    mt.await.unwrap();
    ht.await.unwrap();
    (temp, e, p)
}

#[tokio::test]
async fn browser_partial_cancel_race_reserves_only_new_hedge_on_both_native_venues() {
    // Synthetic verified account/order evidence only, no browser or exchange.
    for venue in ["aster", "hyperliquid"] {
        let (path, mut engine, p) = setup_venues("variational", venue).await;
        let maker_path = Temp::new();
        let command = browser::Command {
            request_id: "browser-request".into(),
            account: p.maker.scope.account.clone(),
            symbol: p.maker.symbol.clone(),
            side: "SELL".into(),
            quantity: "10".into(),
            limit_price: "100".into(),
            max_notional: "1000".into(),
            reduce_only: false,
        };
        let mut maker_ledger = browser::Ledger::create(&maker_path.0, command.clone()).unwrap();
        maker_ledger.dispatch().unwrap();
        maker_ledger
            .bind_verified(browser::Identity {
                request_id: command.request_id,
                account: command.account,
                symbol: command.symbol,
                side: command.side,
                quantity: command.quantity,
                reduce_only: false,
                order_id: p.maker_order.order_id.clone(),
            })
            .unwrap();
        maker_ledger.observe_verified(&maker(&p, "2", 10)).unwrap();
        engine.observe_browser_maker(&maker_ledger).unwrap();
        let first = engine.reserve_hedge().unwrap().unwrap();
        assert_eq!(first.quantity, d("2"));
        let permit = engine.prepare_dispatch(&first.id).unwrap();
        assert_eq!(permit.scope().venue, venue);
        assert!(permit
            .client_id()
            .starts_with(if venue == "aster" { "pa" } else { "0x" }));
        engine.observe_browser_maker(&maker_ledger).unwrap();
        assert!(engine.reserve_hedge().unwrap().is_none());
        maker_ledger.cancel_started().unwrap();
        maker_ledger.unresolved().unwrap();
        maker_ledger.observe_verified(&maker(&p, "3", 11)).unwrap();
        engine.observe_browser_maker(&maker_ledger).unwrap();
        let second = engine.reserve_hedge().unwrap().unwrap();
        assert_eq!(second.quantity, d("1"));
        assert!(!engine.state.engine.maker_terminal_confirmed());
        // Restart: maker lookup can only clear maker recovery. Unknown hedge
        // remains reserved and blocks another submission until independently found.
        let old_engine_ticket = engine.begin_browser_reconciliation().unwrap();
        drop(engine);
        drop(maker_ledger);
        let mut engine = DurableEngine::open(&path.0, &p).unwrap();
        let mut maker_ledger = browser::Ledger::open(&maker_path.0).unwrap();
        assert!(engine
            .reconcile_browser_maker(old_engine_ticket, &maker_ledger)
            .is_err());
        let engine_ticket = engine.begin_browser_reconciliation().unwrap();
        assert!(engine
            .reconcile_browser_maker(engine_ticket, &maker_ledger)
            .is_err());
        let engine_ticket = engine.begin_browser_reconciliation().unwrap();
        let ticket = maker_ledger.lookup_ticket().unwrap();
        let mut terminal = maker(&p, "3", 12);
        terminal.status = Status::Canceled;
        maker_ledger
            .apply_verified_lookup(ticket, &terminal)
            .unwrap();
        // A gap on the paired owner after the lookup began invalidates it too.
        engine.require_reconciliation(Target::Maker).unwrap();
        assert!(engine
            .reconcile_browser_maker(engine_ticket, &maker_ledger)
            .is_err());
        let engine_ticket = engine.begin_browser_reconciliation().unwrap();
        assert!(engine
            .reconcile_browser_maker(engine_ticket, &maker_ledger)
            .is_err());
        let engine_ticket = engine.begin_browser_reconciliation().unwrap();
        let ticket = maker_ledger.lookup_ticket().unwrap();
        let mut terminal = maker(&p, "3", 13);
        terminal.status = Status::Canceled;
        maker_ledger
            .apply_verified_lookup(ticket, &terminal)
            .unwrap();
        engine
            .reconcile_browser_maker(engine_ticket, &maker_ledger)
            .unwrap();
        assert!(!engine.recovery_pending().contains(&Target::Maker));
        assert!(engine.recovery_pending().contains(&Target::Hedge(first.id)));
        assert!(engine.reserve_hedge().is_err());
        assert_eq!(engine.in_flight().unwrap(), d("3"));
    }
}
fn maker(p: &Plan, q: &str, ms: u64) -> OrderSnapshot {
    let now = Instant::now();
    OrderSnapshot {
        scope: p.maker.scope.clone(),
        key: p.maker_order.clone(),
        original: p.quantity,
        cumulative: d(q),
        status: if q == "10" {
            Status::Filled
        } else {
            Status::PartiallyFilled
        },
        event_ms: Some(ms),
        revision: 1,
        requires_reconciliation: false,
        observed_at: now,
        applied_at: now,
    }
}
fn answer(t: &LookupTicket, q: &str, status: Status, ms: u64) -> Outcome {
    let r = t.request();
    let (id, client_id) = match &r.selector {
        Selector::OrderId(id) => (id.clone(), None),
        Selector::ClientId(id) => ("2".into(), Some(id.clone())),
    };
    Outcome::Found(Found {
        scope: r.scope.clone(),
        side: r.side.clone(),
        client_id,
        order: Update {
            key: key(&r.symbol, &id),
            original: r.original,
            cumulative: d(q),
            status,
            event_ms: Some(ms),
        },
    })
}
fn resolve(e: &mut DurableEngine, target: Target, q: &str, status: Status, ms: u64) {
    let ticket = e.lookup_ticket(target).unwrap();
    let result = answer(&ticket, q, status, ms);
    e.apply_lookup(ticket, result).unwrap();
}
fn dispatch(e: &mut DurableEngine, p: &Plan, q: &str) -> DispatchPermit {
    e.observe(Target::Maker, &maker(p, q, 100)).unwrap();
    let i = e.reserve_hedge().unwrap().unwrap();
    e.prepare_dispatch(&i.id).unwrap()
}

#[tokio::test]
async fn restart_keeps_unknown_reserved_and_not_found_never_allows_retry() {
    let (temp, mut e, p) = setup().await;
    let permit = dispatch(&mut e, &p, "5");
    let id = permit.intent.id;
    assert!(matches!(
        e.prepare_dispatch(&id),
        Err(JournalError::AlreadyDispatched)
    ));
    drop(e);
    let mut e = DurableEngine::open(&temp.0, &p).unwrap();
    assert_eq!(e.exposure().unwrap(), d("5"));
    assert_eq!(e.in_flight().unwrap(), d("5"));
    assert_eq!(e.reserve_hedge(), Err(JournalError::RecoveryRequired));
    resolve(&mut e, Target::Maker, "5", Status::PartiallyFilled, 100);
    for _ in 0..3 {
        let t = e.lookup_ticket(Target::Hedge(id.clone())).unwrap();
        assert_eq!(
            t.request().selector,
            Selector::ClientId(permit.client_id.clone())
        );
        e.apply_lookup(t, Outcome::NotFound).unwrap();
    }
    assert_eq!(e.recovery_pending(), vec![Target::Hedge(id.clone())]);
    assert_eq!(e.in_flight().unwrap(), d("5"));
    resolve(&mut e, Target::Hedge(id), "2", Status::Canceled, 101);
    assert!(e.recovery_pending().is_empty());
    let next = e.reserve_hedge().unwrap().unwrap();
    assert_eq!(next.quantity, d("3"));
    assert_eq!(
        e.expected_positions(Target::Maker).unwrap()[&PositionKey {
            symbol: "ETHUSDT".into(),
            side: "BOTH".into()
        }],
        d("-5")
    );
    assert_eq!(
        e.expected_positions(Target::Hedge(next.id)).unwrap()[&PositionKey {
            symbol: "ETH".into(),
            side: "BOTH".into()
        }],
        d("2")
    );
}

#[tokio::test]
async fn reserved_but_never_dispatched_can_resume_after_maker_lookup() {
    let (temp, mut e, p) = setup().await;
    e.observe(Target::Maker, &maker(&p, "2", 100)).unwrap();
    let i = e.reserve_hedge().unwrap().unwrap();
    drop(e);
    let mut e = DurableEngine::open(&temp.0, &p).unwrap();
    assert_eq!(e.intents()[0].intent, i);
    assert!(e.intents()[0].client_id.is_none());
    assert!(matches!(
        e.prepare_dispatch(&i.id),
        Err(JournalError::RecoveryRequired)
    ));
    resolve(&mut e, Target::Maker, "2", Status::PartiallyFilled, 100);
    assert_eq!(e.prepare_dispatch(&i.id).unwrap().intent, i);
    assert!(e.reserve_hedge().unwrap().is_none());
}

#[tokio::test]
async fn ws_dispatch_time_does_not_block_fresh_rest_order_time() {
    let (temp, mut e, p) = setup().await;
    e.observe(Target::Maker, &maker(&p, "5", 200)).unwrap();
    drop(e);
    let mut e = DurableEngine::open(&temp.0, &p).unwrap();
    resolve(&mut e, Target::Maker, "5", Status::PartiallyFilled, 100);
    assert!(e.recovery_pending().is_empty());
    assert_eq!(e.reserve_hedge().unwrap().unwrap().quantity, d("5"));
}

#[tokio::test]
async fn disconnect_fences_pending_lookup_and_keeps_known_fills() {
    let (_temp, mut e, p) = setup().await;
    let id = dispatch(&mut e, &p, "5").intent.id;
    let target = Target::Hedge(id);
    let t = e.lookup_ticket(target.clone()).unwrap();
    let reply = answer(&t, "2", Status::Canceled, 100);
    e.require_reconciliation(target.clone()).unwrap();
    e.apply_lookup(t, reply).unwrap();
    assert_eq!(e.in_flight().unwrap(), d("3"));
    assert_eq!(e.recovery_pending(), vec![target.clone()]);
    resolve(&mut e, target, "2", Status::Canceled, 100);
    assert_eq!(e.reserve_hedge().unwrap().unwrap().quantity, d("3"));
}

#[tokio::test]
async fn exclusive_owner_and_exact_plan_binding() {
    let (temp, e, p) = setup().await;
    assert!(matches!(
        DurableEngine::open(&temp.0, &p),
        Err(JournalError::Locked)
    ));
    assert!(Journal::create(&temp.0).is_err());
    drop(e);
    let mut wrong = p.clone();
    wrong.hedge.scope.account = "other".into();
    assert!(matches!(
        DurableEngine::open(&temp.0, &wrong),
        Err(JournalError::Identity)
    ));
    assert!(DurableEngine::open(&temp.0, &p).is_ok());
}

#[tokio::test]
async fn acknowledged_and_duplicate_fills_replay_exactly() {
    let (temp, mut e, p) = setup().await;
    let i = dispatch(&mut e, &p, "5").intent;
    e.acknowledge(&i.id, key("ETH", "2")).unwrap();
    assert_eq!(e.exposure().unwrap(), d("5"));
    resolve(
        &mut e,
        Target::Hedge(i.id.clone()),
        "5",
        Status::Filled,
        110,
    );
    let s = e.state.engine.hedges[&i.id].clone();
    assert!(s.terminal);
    resolve(
        &mut e,
        Target::Hedge(i.id.clone()),
        "5",
        Status::Filled,
        110,
    );
    assert_eq!(e.exposure().unwrap(), d("0"));
    drop(e);
    let mut e = DurableEngine::open(&temp.0, &p).unwrap();
    assert_eq!(e.exposure().unwrap(), d("0"));
    assert_eq!(e.in_flight().unwrap(), d("0"));
    resolve(&mut e, Target::Maker, "5", Status::PartiallyFilled, 100);
    resolve(&mut e, Target::Hedge(i.id), "5", Status::Filled, 110);
    assert!(e.reserve_hedge().unwrap().is_none());
}

#[tokio::test]
async fn concurrent_or_expired_lookup_cannot_release_remaining_quantity() {
    let (_temp, mut e, p) = setup().await;
    let id = dispatch(&mut e, &p, "5").intent.id;
    let ticket = e.lookup_ticket(Target::Hedge(id.clone())).unwrap();
    let response = answer(&ticket, "2", Status::Canceled, 101);
    e.observe(Target::Maker, &maker(&p, "6", 102)).unwrap();
    e.apply_lookup(ticket, response).unwrap();
    assert_eq!(e.in_flight().unwrap(), d("3"));
    let mut t = e.lookup_ticket(Target::Hedge(id.clone())).unwrap();
    t.started_at = Instant::now() - Duration::from_secs(31);
    let response = answer(&t, "2", Status::Canceled, 101);
    e.apply_lookup(t, response).unwrap();
    assert_eq!(e.in_flight().unwrap(), d("3"));
    resolve(&mut e, Target::Hedge(id), "2", Status::Canceled, 101);
    assert_eq!(e.reserve_hedge().unwrap().unwrap().quantity, d("4"));
}

#[tokio::test]
async fn stale_private_events_do_not_erase_rest_fills_or_bypass_identity() {
    let (_temp, mut e, p) = setup().await;
    e.observe(Target::Maker, &maker(&p, "5", 100)).unwrap();
    let mut stale = maker(&p, "2", 90);
    e.observe(Target::Maker, &stale).unwrap();
    assert_eq!(e.exposure().unwrap(), d("5"));
    stale.scope.account = "wrong".into();
    assert_eq!(
        e.observe(Target::Maker, &stale),
        Err(JournalError::Identity)
    );
    stale = maker(&p, "6", 101);
    stale.revision = 0;
    e.observe(Target::Maker, &stale).unwrap();
    assert_eq!(e.exposure().unwrap(), d("6"));
}

#[tokio::test]
async fn unbound_ws_order_requires_client_id_lookup() {
    let (_temp, mut e, p) = setup().await;
    let id = dispatch(&mut e, &p, "5").intent.id;
    let t = e.lookup_ticket(Target::Hedge(id.clone())).unwrap();
    let Outcome::Found(f) = answer(&t, "5", Status::Filled, 101) else {
        unreachable!()
    };
    let mut s = maker(&p, "5", 101);
    s.scope = f.scope.clone();
    s.key = f.order.key.clone();
    s.original = d("5");
    s.status = Status::Filled;
    assert_eq!(
        e.observe(Target::Hedge(id.clone()), &s),
        Err(JournalError::Identity)
    );
    e.apply_lookup(t, Outcome::Found(f)).unwrap();
    e.observe(Target::Hedge(id), &s).unwrap();
    assert_eq!(e.exposure().unwrap(), d("0"));
}

#[tokio::test]
async fn wrong_lookup_identity_cannot_release_or_change_reservation() {
    let (_temp, mut e, p) = setup().await;
    let id = dispatch(&mut e, &p, "5").intent.id;
    for field in 0..5 {
        let t = e.lookup_ticket(Target::Hedge(id.clone())).unwrap();
        let Outcome::Found(mut f) = answer(&t, "5", Status::Filled, 101) else {
            unreachable!()
        };
        match field {
            0 => f.client_id = Some("wrong".into()),
            1 => f.scope.account = "wrong".into(),
            2 => f.side = "SELL".into(),
            3 => f.order.key.symbol = "BTC".into(),
            _ => {
                f.order.original = d("6");
                f.order.status = Status::PartiallyFilled;
            }
        }
        assert_eq!(
            e.apply_lookup(t, Outcome::Found(f)),
            Err(JournalError::Identity)
        );
        assert_eq!(e.in_flight().unwrap(), d("5"));
    }
    e.acknowledge(&id, key("ETH", "3")).unwrap();
    let t = e.lookup_ticket(Target::Hedge(id)).unwrap();
    let f = answer(&t, "5", Status::Filled, 101);
    assert!(e.apply_lookup(t, f).is_err());
}

#[tokio::test]
async fn lookup_ticket_from_previous_process_instance_is_rejected() {
    let (temp, mut e, p) = setup().await;
    dispatch(&mut e, &p, "5");
    let t = e.lookup_ticket(Target::Maker).unwrap();
    let f = answer(&t, "5", Status::PartiallyFilled, 100);
    drop(e);
    let mut e = DurableEngine::open(&temp.0, &p).unwrap();
    assert_eq!(e.apply_lookup(t, f), Err(JournalError::Identity));
}

#[tokio::test]
async fn old_terminal_frames_do_not_reopen_already_retried_reservation() {
    let (temp, mut e, p) = setup().await;
    let id = dispatch(&mut e, &p, "5").intent.id;
    resolve(
        &mut e,
        Target::Hedge(id.clone()),
        "2",
        Status::Canceled,
        101,
    );
    let j = e.reserve_hedge().unwrap().unwrap();
    e.prepare_dispatch(&j.id).unwrap();
    e.acknowledge(&j.id, key("ETH", "3")).unwrap();
    let mut s = maker(&p, "3", 102);
    s.scope = p.hedge.scope.clone();
    s.key = key("ETH", "3");
    s.original = d("3");
    s.status = Status::Filled;
    e.observe(Target::Hedge(j.id.clone()), &s).unwrap();
    drop(e);
    let mut e = DurableEngine::open(&temp.0, &p).unwrap();
    s.key = key("ETH", "2");
    s.original = d("5");
    s.cumulative = d("2");
    s.status = Status::Canceled;
    s.event_ms = Some(101);
    e.observe(Target::Hedge(id.clone()), &s).unwrap();
    assert_eq!(e.in_flight().unwrap(), d("0"));
    assert_eq!(e.exposure().unwrap(), d("0"));
    s.cumulative = d("3");
    s.event_ms = Some(103);
    assert_eq!(
        e.observe(Target::Hedge(id), &s),
        Err(JournalError::Engine(Error::InvalidEvidence))
    );
    assert_eq!(e.exposure().unwrap(), d("-1"));
    drop(e);
    let e = DurableEngine::open(&temp.0, &p).unwrap();
    assert!(e.state.engine.fault);
    assert_eq!(e.exposure().unwrap(), d("-1"));
}

#[tokio::test]
async fn journal_truncation_and_bitflip_fail_closed_without_repair() {
    for corrupt in [false, true] {
        let (temp, mut e, p) = setup().await;
        dispatch(&mut e, &p, "5");
        drop(e);
        let mut bytes = std::fs::read(&temp.0).unwrap();
        if corrupt {
            bytes[20] ^= 1;
        } else {
            bytes.pop();
        }
        std::fs::write(&temp.0, &bytes).unwrap();
        assert!(matches!(
            DurableEngine::open(&temp.0, &p),
            Err(JournalError::Corrupt)
        ));
        assert_eq!(std::fs::read(&temp.0).unwrap(), bytes);
    }
}

#[tokio::test]
async fn dispatch_write_failure_never_returns_permission_and_poisons_owner() {
    for partial in [false, true] {
        let (temp, mut e, p) = setup().await;
        e.observe(Target::Maker, &maker(&p, "5", 100)).unwrap();
        let i = e.reserve_hedge().unwrap().unwrap();
        e.journal.fail = Some(partial);
        assert!(matches!(e.prepare_dispatch(&i.id), Err(JournalError::Io)));
        assert!(matches!(
            e.prepare_dispatch(&i.id),
            Err(JournalError::Poisoned)
        ));
        assert_eq!(e.reserve_hedge(), Err(JournalError::Poisoned));
        drop(e);
        if partial {
            assert!(matches!(
                DurableEngine::open(&temp.0, &p),
                Err(JournalError::Corrupt)
            ));
        } else {
            let e = DurableEngine::open(&temp.0, &p).unwrap();
            assert_eq!(e.in_flight().unwrap(), d("5"));
            assert!(e.recovery_pending().contains(&Target::Hedge(i.id)));
        }
    }
}

#[tokio::test]
async fn failed_fill_persistence_keeps_known_exposure_visible_but_blocks_dispatch() {
    let (_temp, mut e, p) = setup().await;
    e.journal.fail = Some(true);
    assert_eq!(
        e.observe(Target::Maker, &maker(&p, "5", 100)),
        Err(JournalError::Io)
    );
    assert_eq!(e.exposure().unwrap(), d("5"));
    assert_eq!(e.reserve_hedge(), Err(JournalError::Poisoned));
}

#[tokio::test]
async fn distinct_journals_have_distinct_client_ids_even_with_same_run_name() {
    let (_a, mut a, p) = setup().await;
    let (_b, mut b, _) = setup().await;
    let a = dispatch(&mut a, &p, "5");
    let b = dispatch(&mut b, &p, "5");
    assert_ne!(a.client_id, b.client_id);
    assert_eq!(a.client_id.len(), 34);
}

// Separate process exits without Rust destructors: crash-after-dispatch recovery, not a power-loss test.
#[test]
fn abrupt_exit_child() {
    let Ok(path) = std::env::var("PERP_ARB_TEST_CRASH_JOURNAL") else {
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (temp, mut e, p) = setup().await;
        drop(e);
        std::fs::rename(&temp.0, &path).unwrap();
        e = DurableEngine::open(Path::new(&path), &p).unwrap();
        resolve(&mut e, Target::Maker, "5", Status::PartiallyFilled, 100);
        let i = e.reserve_hedge().unwrap().unwrap();
        e.prepare_dispatch(&i.id).unwrap();
        std::process::exit(73);
    });
}
#[tokio::test]
async fn actual_process_exit_after_dispatch_replays_as_unknown() {
    let (_unused, e, p) = setup().await;
    drop(e);
    let temp = Temp::new();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable::tests::abrupt_exit_child",
            "--nocapture",
        ])
        .env("PERP_ARB_TEST_CRASH_JOURNAL", &temp.0)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(73),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let mut e = DurableEngine::open(&temp.0, &p).unwrap();
    assert_eq!(e.exposure().unwrap(), d("5"));
    assert_eq!(e.in_flight().unwrap(), d("5"));
    assert_eq!(e.reserve_hedge(), Err(JournalError::RecoveryRequired));
}
