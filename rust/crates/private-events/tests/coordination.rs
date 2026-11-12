use perp_arb_core::{decimal, Decimal};
use perp_arb_private_events::{actor, aster, *};
use std::time::{Duration, Instant};

fn d(s: &str) -> Decimal {
    decimal(s).unwrap()
}
fn scope() -> Scope {
    Scope {
        venue: "aster".into(),
        account: "test-account".into(),
    }
}
fn key(id: &str) -> OrderKey {
    OrderKey {
        symbol: "ETHUSDT".into(),
        order_id: id.into(),
    }
}
fn update(id: &str, qty: &str, status: Status, time: u64) -> Update {
    Update {
        key: key(id),
        original: d("10"),
        cumulative: d(qty),
        status,
        event_ms: Some(time),
    }
}
fn ledger() -> (Ledger, Session) {
    let mut ledger = Ledger::new(scope(), 32).unwrap();
    let session = ledger.connected().unwrap();
    (ledger, session)
}
fn ws(l: &mut Ledger, s: &Session, u: Update) -> OrderSnapshot {
    l.observe(u, Evidence::Ws(s.clone()), Instant::now())
        .unwrap()
}
fn rest(l: &mut Ledger, u: Update) -> OrderSnapshot {
    let ticket = l.begin_query(&u.key).unwrap();
    l.observe(u, Evidence::Rest(ticket), Instant::now())
        .unwrap()
}
fn frame(qty: &str, status: &str) -> String {
    serde_json::json!({"e":"ORDER_TRADE_UPDATE","E":100,"o":{
        "s":"ETHUSDT","i":1,"q":"10","z":qty,"X":status}})
    .to_string()
}

#[test]
fn pre_ack_fill_is_preserved_and_ack_is_not_a_fill() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "4", Status::PartiallyFilled, 1));
    let ack = l.register(key("1"), d("10"), Instant::now()).unwrap();
    assert_eq!(ack.cumulative, d("4"));
    assert_eq!(ack.status, Status::PartiallyFilled);
    let new = l.register(key("2"), d("10"), Instant::now()).unwrap();
    assert_eq!(new.cumulative, Decimal::ZERO);
    assert_eq!(new.status, Status::Pending);
}

#[test]
fn websocket_rest_race_deduplicates_in_both_orders() {
    for rest_first in [false, true] {
        let (mut l, s) = ledger();
        l.register(key("1"), d("10"), Instant::now()).unwrap();
        let ticket = l.begin_query(&key("1")).unwrap();
        let mut cursor = FillCursor::new(scope(), key("1"));
        let u = update("1", "4.25", Status::PartiallyFilled, 2);
        let sources = if rest_first {
            vec![Evidence::Rest(ticket), Evidence::Ws(s)]
        } else {
            vec![Evidence::Ws(s), Evidence::Rest(ticket)]
        };
        let mut total = Decimal::ZERO;
        for source in sources {
            let snapshot = l.observe(u.clone(), source, Instant::now()).unwrap();
            total += cursor.acknowledge(&snapshot).unwrap();
        }
        assert_eq!(total, d("4.25"));
    }
}

#[test]
fn permutations_of_duplicates_and_reordering_account_for_exact_total() {
    fn visit(items: &mut [Update], at: usize, count: &mut usize) {
        if at == items.len() {
            let (mut l, s) = ledger();
            let mut cursor = FillCursor::new(scope(), key("1"));
            let mut total = Decimal::ZERO;
            for u in items.iter() {
                total += cursor.acknowledge(&ws(&mut l, &s, u.clone())).unwrap();
            }
            assert_eq!(total, d("10"));
            assert_eq!(l.snapshot(&key("1")).unwrap().status, Status::Filled);
            *count += 1;
            return;
        }
        for index in at..items.len() {
            items.swap(index, at);
            visit(items, at + 1, count);
            items.swap(index, at);
        }
    }
    let mut events = [
        update("1", "0", Status::New, 1),
        update("1", "2", Status::PartiallyFilled, 2),
        update("1", "2", Status::PartiallyFilled, 2),
        update("1", "6", Status::Canceled, 4),
        update("1", "8", Status::PartiallyFilled, 3),
        update("1", "10", Status::Filled, 5),
    ];
    let mut count = 0;
    visit(&mut events, 0, &mut count);
    assert_eq!(count, 720);
}

#[test]
fn older_timestamp_with_more_fills_is_not_discarded() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "2", Status::PartiallyFilled, 50));
    let snapshot = ws(&mut l, &s, update("1", "4", Status::PartiallyFilled, 40));
    assert_eq!(snapshot.cumulative, d("4"));
    assert_eq!(snapshot.event_ms, Some(50));
}

#[test]
fn cancel_race_retains_late_fills_and_demands_reconciliation() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "2", Status::Canceled, 50));
    let snapshot = ws(&mut l, &s, update("1", "4", Status::PartiallyFilled, 40));
    assert_eq!(snapshot.cumulative, d("4"));
    assert_eq!(snapshot.status, Status::Canceled);
    assert!(snapshot.requires_reconciliation);
    let final_snapshot = rest(&mut l, update("1", "4", Status::Canceled, 60));
    assert!(!final_snapshot.requires_reconciliation);
}

#[test]
fn filled_cannot_be_reopened_or_reset_by_stale_rest() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "10", Status::Filled, 50));
    let snapshot = rest(&mut l, update("1", "0", Status::New, 40));
    assert_eq!(snapshot.cumulative, d("10"));
    assert_eq!(snapshot.status, Status::Filled);
}

#[test]
fn zero_fill_cancel_is_visible() {
    let (mut l, s) = ledger();
    let snapshot = ws(&mut l, &s, update("1", "0", Status::Canceled, 1));
    assert!(snapshot.status.terminal());
    assert_eq!(
        FillCursor::new(scope(), key("1"))
            .pending(&snapshot)
            .unwrap(),
        Decimal::ZERO
    );
}

#[test]
fn disconnect_and_reconnect_do_not_clear_known_fills_or_certify_recovery() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "4", Status::PartiallyFilled, 1));
    l.disconnected(&s).unwrap();
    assert!(!l.health().ready_for_new_risk());
    let new = l.connected().unwrap();
    assert!(new.generation() > s.generation());
    let current = ws(&mut l, &new, update("1", "4", Status::PartiallyFilled, 1));
    assert!(current.requires_reconciliation);
    rest(&mut l, update("1", "4", Status::PartiallyFilled, 2));
    assert!(l.health().ready_for_new_risk());
}

#[test]
fn old_socket_can_contribute_fills_but_not_disconnect_new_socket() {
    let (mut l, old) = ledger();
    l.disconnected(&old).unwrap();
    let new = l.connected().unwrap();
    ws(&mut l, &new, update("1", "2", Status::PartiallyFilled, 10));
    l.disconnected(&old).unwrap();
    assert!(l.health().connected);
    let late = ws(&mut l, &old, update("1", "4", Status::PartiallyFilled, 9));
    assert_eq!(late.cumulative, d("4"));
    assert!(late.requires_reconciliation);
}

#[test]
fn rest_query_started_before_disconnect_cannot_reconcile_gap() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "2", Status::PartiallyFilled, 1));
    let ticket = l.begin_query(&key("1")).unwrap();
    l.disconnected(&s).unwrap();
    let snapshot = l
        .observe(
            update("1", "4", Status::PartiallyFilled, 2),
            Evidence::Rest(ticket),
            Instant::now(),
        )
        .unwrap();
    assert_eq!(snapshot.cumulative, d("4"));
    assert!(snapshot.requires_reconciliation);
    let snapshot = rest(&mut l, update("1", "4", Status::PartiallyFilled, 3));
    assert!(!snapshot.requires_reconciliation);
    assert!(!l.health().ready_for_new_risk());
}

#[test]
fn rest_query_racing_a_new_ws_event_cannot_certify_reconciliation() {
    let (mut l, s) = ledger();
    l.register(key("1"), d("10"), Instant::now()).unwrap();
    let ticket = l.begin_query(&key("1")).unwrap();
    ws(&mut l, &s, update("1", "2", Status::PartiallyFilled, 1));
    let snapshot = l
        .observe(
            update("1", "2", Status::PartiallyFilled, 1),
            Evidence::Rest(ticket),
            Instant::now(),
        )
        .unwrap();
    assert!(snapshot.requires_reconciliation);
    assert!(!rest(&mut l, update("1", "2", Status::PartiallyFilled, 2)).requires_reconciliation);
}

#[test]
fn orders_symbols_accounts_and_venues_are_isolated() {
    let (mut l, s) = ledger();
    let first = ws(&mut l, &s, update("1", "4", Status::PartiallyFilled, 1));
    let mut another = update("1", "2", Status::PartiallyFilled, 1);
    another.key.symbol = "BTCUSDT".into();
    let btc = ws(&mut l, &s, another);
    assert_eq!(first.cumulative, d("4"));
    assert_eq!(btc.cumulative, d("2"));
    assert_eq!(
        FillCursor::new(scope(), key("1")).pending(&btc),
        Err(Error::IdentityConflict)
    );
    for alternate in [
        Scope {
            venue: "aster".into(),
            account: "other".into(),
        },
        Scope {
            venue: "hyperliquid".into(),
            account: "test-account".into(),
        },
    ] {
        let mut other = Ledger::new(alternate.clone(), 8).unwrap();
        let session = other.connected().unwrap();
        assert_eq!(
            l.observe(
                update("1", "4", Status::PartiallyFilled, 1),
                Evidence::Ws(session),
                Instant::now()
            )
            .unwrap_err(),
            Error::WrongSession
        );
        assert_eq!(
            FillCursor::new(alternate, key("1")).pending(&first),
            Err(Error::IdentityConflict)
        );
    }
}

#[test]
fn query_ticket_cannot_be_applied_to_another_order() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "4", Status::PartiallyFilled, 1));
    let ticket = l.begin_query(&key("1")).unwrap();
    assert_eq!(
        l.observe(
            update("2", "4", Status::PartiallyFilled, 1),
            Evidence::Rest(ticket),
            Instant::now()
        )
        .unwrap_err(),
        Error::IdentityConflict
    );
}

#[test]
fn malformed_quantity_never_erases_known_fills() {
    for (qty, status) in [
        ("-1", Status::PartiallyFilled),
        ("11", Status::Filled),
        ("4", Status::Filled),
        ("0", Status::PartiallyFilled),
        ("2", Status::New),
        ("2", Status::Rejected),
    ] {
        let (mut l, s) = ledger();
        ws(&mut l, &s, update("1", "2", Status::PartiallyFilled, 1));
        assert!(l
            .observe(update("1", qty, status, 2), Evidence::Ws(s), Instant::now())
            .is_err());
        assert_eq!(l.snapshot(&key("1")).unwrap().cumulative, d("2"));
        assert!(l.health().fault.is_some());
    }
}

#[test]
fn original_quantity_changes_are_identity_conflicts() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "2", Status::PartiallyFilled, 1));
    assert_eq!(
        l.register(key("1"), d("20"), Instant::now()).unwrap_err(),
        Error::IdentityConflict
    );
    assert_eq!(l.snapshot(&key("1")).unwrap().original, d("10"));
}

#[test]
fn capacity_fault_never_evicts_open_or_terminal_dedup_state() {
    let mut l = Ledger::new(scope(), 1).unwrap();
    let s = l.connected().unwrap();
    ws(&mut l, &s, update("1", "10", Status::Filled, 1));
    assert_eq!(
        l.observe(
            update("2", "2", Status::PartiallyFilled, 2),
            Evidence::Ws(s.clone()),
            Instant::now()
        )
        .unwrap_err(),
        Error::Capacity
    );
    assert_eq!(l.health().fault, Some(Error::Capacity));
    let old = ws(&mut l, &s, update("1", "10", Status::Filled, 1));
    assert_eq!(old.cumulative, d("10"));
    rest(&mut l, update("1", "10", Status::Filled, 2));
    assert!(!l.health().ready_for_new_risk()); // account-wide overflow stays sticky
}

#[test]
fn independent_cursors_and_order_replacement_do_not_share_cumulative_totals() {
    let (mut l, s) = ledger();
    let first = ws(&mut l, &s, update("1", "4", Status::Canceled, 1));
    let second = ws(&mut l, &s, update("2", "3", Status::PartiallyFilled, 2));
    for _ in 0..2 {
        let mut cursor = FillCursor::new(scope(), key("1"));
        assert_eq!(cursor.acknowledge(&first).unwrap(), d("4"));
        assert_eq!(cursor.acknowledge(&first).unwrap(), Decimal::ZERO);
        assert_eq!(cursor.pending(&second), Err(Error::IdentityConflict));
    }
    assert_eq!(
        FillCursor::new(scope(), key("2")).pending(&second).unwrap(),
        d("3")
    );
}

#[test]
fn exact_decimal_quantities_preserve_tiny_fills() {
    let (mut l, s) = ledger();
    let q = "0.0000000000000000000000000001";
    let snapshot = ws(&mut l, &s, update("1", q, Status::PartiallyFilled, 1));
    assert_eq!(snapshot.cumulative, d(q));
    assert_eq!(
        aster::decode_ws(&frame(
            "0.00000000000000000000000000001",
            "PARTIALLY_FILLED"
        ))
        .unwrap_err(),
        Error::InvalidQuantity
    );
}

#[test]
fn unrepresentable_delta_is_an_error_instead_of_silent_rounding() {
    let (mut l, s) = ledger();
    let mut u = update(
        "1",
        "0.0000000000000000000000000001",
        Status::PartiallyFilled,
        1,
    );
    u.original = d("10000000000000000000000000000");
    let mut cursor = FillCursor::new(scope(), key("1"));
    cursor.acknowledge(&ws(&mut l, &s, u.clone())).unwrap();
    u.cumulative = u.original;
    u.status = Status::Filled;
    assert_eq!(cursor.pending(&ws(&mut l, &s, u)), Err(Error::Arithmetic));
}

#[tokio::test]
async fn aborting_worker_invalidates_health_without_erasing_fills() {
    let (c, worker) = actor::channel(scope(), 8, 2).unwrap();
    let join = worker.spawn();
    let s = c.connected().await.unwrap();
    c.observe(
        update("1", "4", Status::PartiallyFilled, 1),
        Evidence::Ws(s),
        Instant::now(),
    )
    .await
    .unwrap();
    let receiver = c.subscribe(key("1")).await.unwrap();
    join.abort();
    assert!(join.await.unwrap_err().is_cancelled());
    assert!(!c.health().connected);
    assert_eq!(receiver.borrow().cumulative, d("4"));
    assert!(receiver.borrow().requires_reconciliation);
}

#[test]
fn aster_decode_requires_real_cumulative_fields_and_exact_id() {
    let raw = frame("2", "PARTIALLY_FILLED").replace("\"i\":1", "\"i\":9007199254740993");
    let decoded = aster::decode_ws(&raw).unwrap().unwrap();
    assert_eq!(decoded.key.order_id, "9007199254740993");
    assert_eq!(decoded.cumulative, d("2"));
    for raw in [
        "{}".to_string(),
        "[]".into(),
        frame("NaN", "PARTIALLY_FILLED"),
        frame("2", "MYSTERY"),
        frame("2", "FILLED"),
        frame("2", "PARTIALLY_FILLED").replace("\"z\":\"2\"", "\"z\":2.0"),
        " ".repeat(aster::MAX_FRAME_BYTES + 1),
    ] {
        assert!(
            aster::decode_ws(&raw).is_err(),
            "must reject malformed order"
        );
    }
}

#[test]
fn unrelated_events_expiry_and_rest_error_are_distinct() {
    assert!(aster::decode_ws(r#"{"e":"ACCOUNT_UPDATE"}"#)
        .unwrap()
        .is_none());
    assert_eq!(
        aster::decode_ws(r#"{"e":"listenKeyExpired"}"#).unwrap_err(),
        Error::StreamExpired
    );
    assert!(aster::decode_rest(r#"{"code":-2013,"msg":"Order does not exist"}"#).is_err());
    let decoded = aster::decode_rest(r#"{"symbol":"ETHUSDT","orderId":"1","origQty":"10","executedQty":"0","status":"CANCELED","updateTime":100}"#).unwrap();
    assert_eq!(decoded.status, Status::Canceled);
    assert_eq!(decoded.cumulative, Decimal::ZERO);
}

#[test]
fn rest_fallback_before_first_ws_does_not_claim_stream_health() {
    let mut l = Ledger::new(scope(), 8).unwrap();
    l.register(key("1"), d("10"), Instant::now()).unwrap();
    let snapshot = rest(&mut l, update("1", "10", Status::Filled, 1));
    assert!(!snapshot.requires_reconciliation);
    assert_eq!(snapshot.cumulative, d("10"));
    assert!(!l.health().ready_for_new_risk());
    l.connected().unwrap();
    assert!(l.snapshot(&key("1")).unwrap().requires_reconciliation);
}

#[test]
fn rest_update_time_is_not_compared_to_ws_dispatch_time() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "4", Status::PartiallyFilled, 200));
    l.disconnected(&s).unwrap();
    l.connected().unwrap();
    let snapshot = rest(&mut l, update("1", "4", Status::PartiallyFilled, 100));
    assert!(!snapshot.requires_reconciliation);
    assert!(l.health().ready_for_new_risk());
}

#[test]
fn tokens_cannot_cross_two_coordinators_for_the_same_account() {
    let (mut first, session) = ledger();
    let (mut second, _) = ledger();
    first.register(key("1"), d("10"), Instant::now()).unwrap();
    second.register(key("1"), d("10"), Instant::now()).unwrap();
    let ticket = first.begin_query(&key("1")).unwrap();
    for evidence in [Evidence::Ws(session), Evidence::Rest(ticket)] {
        assert_eq!(
            second
                .observe(
                    update("1", "4", Status::PartiallyFilled, 1),
                    evidence,
                    Instant::now()
                )
                .unwrap_err(),
            Error::WrongSession
        );
    }
}

#[test]
fn newer_regressed_quantity_and_conflicting_terminal_states_require_recovery() {
    let (mut l, s) = ledger();
    ws(&mut l, &s, update("1", "4", Status::Canceled, 1));
    let snapshot = ws(&mut l, &s, update("1", "2", Status::Expired, 2));
    assert!(snapshot.requires_reconciliation);
    assert_eq!(snapshot.cumulative, d("4"));
    assert_eq!(snapshot.status, Status::Canceled);
    assert_eq!(l.health().reconcile_orders, 1);
    rest(&mut l, update("1", "4", Status::Canceled, 3));
    assert_eq!(l.health().reconcile_orders, 0);
}

#[tokio::test]
async fn aster_rest_reconciliation_updates_existing_subscriber() {
    let (c, worker) = actor::channel(scope(), 8, 2).unwrap();
    let join = worker.spawn();
    let s = c.connected().await.unwrap();
    c.ingest_aster_ws(s.clone(), &frame("2", "PARTIALLY_FILLED"))
        .await
        .unwrap();
    let receiver = c.subscribe(key("1")).await.unwrap();
    c.disconnected(s).await.unwrap();
    c.connected().await.unwrap();
    let ticket = c.begin_query(key("1")).await.unwrap();
    c.ingest_aster_rest(ticket, r#"{"symbol":"ETHUSDT","orderId":1,"origQty":"10","executedQty":"10","status":"FILLED","updateTime":200}"#).await.unwrap();
    assert_eq!(receiver.borrow().cumulative, d("10"));
    assert!(!receiver.borrow().requires_reconciliation);
    assert!(c.health().ready_for_new_risk());
    c.shutdown().await.unwrap();
    join.await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_commands_already_admitted_after_stop_request() {
    let (c, worker) = actor::channel(scope(), 8, 3).unwrap();
    // No worker running yet: all three commands enter FIFO before timeouts.
    assert!(
        tokio::time::timeout(Duration::from_millis(5), c.register(key("1"), d("10")))
            .await
            .is_err()
    );
    assert!(tokio::time::timeout(Duration::from_millis(5), c.shutdown())
        .await
        .is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(5), c.register(key("2"), d("10")))
            .await
            .is_err()
    );
    let join = worker.spawn();
    join.await.unwrap();
    assert!(!c.health().connected);
    assert_eq!(c.health().reconcile_orders, 2);
    assert_eq!(c.connected().await.unwrap_err(), Error::Closed);
}

#[tokio::test]
async fn actor_coalesces_snapshots_without_losing_cumulative_fills() {
    let (c, worker) = actor::channel(scope(), 8, 2).unwrap();
    let join = worker.spawn();
    let s = c.connected().await.unwrap();
    c.register(key("1"), d("10")).await.unwrap();
    let mut receiver = c.subscribe(key("1")).await.unwrap();
    let mut second = receiver.clone();
    for qty in ["2", "4", "7", "10"] {
        let status = if qty == "10" {
            Status::Filled
        } else {
            Status::PartiallyFilled
        };
        c.observe(
            update("1", qty, status, 1),
            Evidence::Ws(s.clone()),
            Instant::now(),
        )
        .await
        .unwrap();
    }
    receiver.changed().await.unwrap();
    second.changed().await.unwrap();
    for snapshot in [
        receiver.borrow_and_update().clone(),
        second.borrow_and_update().clone(),
    ] {
        assert_eq!(
            FillCursor::new(scope(), key("1"))
                .pending(&snapshot)
                .unwrap(),
            d("10")
        );
    }
    c.shutdown().await.unwrap();
    join.await.unwrap();
    assert!(!c.health().connected);
    assert!(receiver.borrow().requires_reconciliation);
    assert!(c.register(key("2"), d("10")).await.is_err());
}

#[tokio::test]
async fn bounded_ingress_blocks_and_admitted_event_survives_caller_cancellation() {
    let (c, worker) = actor::channel(scope(), 8, 1).unwrap();
    // Worker has not started: the first command is admitted, but its caller
    // times out. The second command must wait for capacity and is not admitted.
    assert!(
        tokio::time::timeout(Duration::from_millis(5), c.register(key("1"), d("10")))
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(5), c.register(key("2"), d("10")))
            .await
            .is_err()
    );
    let join = worker.spawn();
    assert!(c.subscribe(key("1")).await.is_ok());
    assert_eq!(
        c.subscribe(key("2")).await.unwrap_err(),
        Error::UnknownOrder
    );
    c.shutdown().await.unwrap();
    join.await.unwrap();
}

#[tokio::test]
async fn concurrent_ws_rest_and_slow_consumer_keep_exact_exposure() {
    let (c, worker) = actor::channel(scope(), 8, 1).unwrap();
    let join = worker.spawn();
    let s = c.connected().await.unwrap();
    c.register(key("1"), d("10")).await.unwrap();
    let receiver = c.subscribe(key("1")).await.unwrap();
    let ticket = c.begin_query(key("1")).await.unwrap();
    let (a, b) = tokio::join!(
        c.observe(
            update("1", "10", Status::Filled, 2),
            Evidence::Ws(s),
            Instant::now()
        ),
        c.observe(
            update("1", "4", Status::PartiallyFilled, 1),
            Evidence::Rest(ticket),
            Instant::now()
        )
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(receiver.borrow().cumulative, d("10"));
    c.shutdown().await.unwrap();
    join.await.unwrap();
}

#[tokio::test]
async fn aster_ingestion_fault_wakes_health_and_retains_known_quantity() {
    let (c, worker) = actor::channel(scope(), 8, 2).unwrap();
    let join = worker.spawn();
    let s = c.connected().await.unwrap();
    c.ingest_aster_ws(s.clone(), &frame("2", "PARTIALLY_FILLED"))
        .await
        .unwrap();
    let receiver = c.subscribe(key("1")).await.unwrap();
    let mut health = c.watch_health();
    assert_eq!(
        c.ingest_aster_ws(s, "{ invalid token=secret }")
            .await
            .unwrap_err(),
        Error::InvalidInput
    );
    health.changed().await.unwrap();
    assert!(health.borrow().fault.is_some());
    assert_eq!(receiver.borrow().cumulative, d("2"));
    assert!(receiver.borrow().requires_reconciliation);
    c.shutdown().await.unwrap();
    join.await.unwrap();
}

#[tokio::test]
async fn expiry_is_a_gap_not_a_zero_fill_or_rejection() {
    let (c, worker) = actor::channel(scope(), 8, 2).unwrap();
    let join = worker.spawn();
    let s = c.connected().await.unwrap();
    c.ingest_aster_ws(s.clone(), &frame("2", "PARTIALLY_FILLED"))
        .await
        .unwrap();
    let receiver = c.subscribe(key("1")).await.unwrap();
    assert_eq!(
        c.ingest_aster_ws(s, r#"{"e":"listenKeyExpired"}"#)
            .await
            .unwrap_err(),
        Error::StreamExpired
    );
    assert!(!c.health().connected);
    assert_eq!(receiver.borrow().cumulative, d("2"));
    c.shutdown().await.unwrap();
    join.await.unwrap();
}

#[tokio::test]
async fn dropping_all_producers_finishes_worker_and_invalidates_views() {
    let (c, worker) = actor::channel(scope(), 8, 2).unwrap();
    let join = worker.spawn();
    c.connected().await.unwrap();
    c.register(key("1"), d("10")).await.unwrap();
    let receiver = c.subscribe(key("1")).await.unwrap();
    let health = c.watch_health();
    drop(c);
    join.await.unwrap();
    assert!(!health.borrow().connected);
    assert!(receiver.borrow().requires_reconciliation);
}
