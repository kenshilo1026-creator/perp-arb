use perp_arb_core::decimal;
use perp_arb_private_events::{actor, hyperliquid, Error, OrderKey, Scope, Status};
use serde_json::{json, Value};
use std::time::Instant;
const USER: &str = "0x0000000000000000000000000000000000000001";
#[tokio::test]
async fn early_session_preserves_events_without_health_until_activation() {
    let (c, w) = channel(16);
    let early = c.connecting().await.unwrap();
    ingest(&c, &early, &fills(vec![fill(1, "2")], false)).await;
    assert!(!c.health().connected);
    c.activate(early.clone()).await.unwrap();
    assert!(c.health().connected);
    c.disconnected(early.clone()).await.unwrap();
    c.connecting().await.unwrap();
    assert_eq!(c.activate(early).await.unwrap_err(), Error::WrongSession);
    assert!(!c.health().connected);
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn unrepresentable_fill_total_is_fault_instead_of_silent_rounding() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    ingest(
        &c,
        &s,
        &fills(vec![fill(1, "10000000000000000000000000000")], false),
    )
    .await;
    assert_eq!(
        c.ingest_hyperliquid_ws_at(
            s,
            &fills(vec![fill(2, "0.0000000000000000000000000001")], false),
            Instant::now()
        )
        .await
        .unwrap_err(),
        Error::Arithmetic
    );
    assert_eq!(
        c.hyperliquid_fill_summary(key())
            .await
            .unwrap()
            .unwrap()
            .unique_fills,
        1
    );
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
fn key() -> OrderKey {
    OrderKey {
        symbol: "xyz:ETH".into(),
        order_id: "9007199254740993".into(),
    }
}
fn order(q: &str, status: &str, time: u64) -> Value {
    json!({"order":{"coin":key().symbol,"oid":9007199254740993u64,"origSz":"10","sz":q},"status":status,"statusTimestamp":time})
}
fn fill(tid: u64, q: &str) -> Value {
    json!({"coin":key().symbol,"oid":9007199254740993u64,"tid":tid,"sz":q,"px":"2000","side":"B","time":100+tid,"hash":format!("0x{:064x}",tid)})
}
fn fills(items: Vec<Value>, snapshot: bool) -> String {
    json!({"channel":"userFills","data":{"user":USER,"isSnapshot":snapshot,"fills":items}})
        .to_string()
}
fn ws_order(q: &str, status: &str, time: u64) -> String {
    json!({"channel":"orderUpdates","data":[order(q,status,time)]}).to_string()
}
fn rest(q: &str, status: &str, time: u64) -> String {
    json!({"status":"order","order":order(q,status,time)}).to_string()
}
fn channel(orders: usize) -> (actor::Coordinator, tokio::task::JoinHandle<()>) {
    let (c, w) = actor::channel(
        Scope {
            venue: "hyperliquid".into(),
            account: USER.into(),
        },
        orders,
        8,
    )
    .unwrap();
    (c, w.spawn())
}
async fn ingest(
    c: &actor::Coordinator,
    s: &perp_arb_private_events::Session,
    raw: &str,
) -> hyperliquid::IngestReport {
    c.ingest_hyperliquid_ws_at(s.clone(), raw, Instant::now())
        .await
        .unwrap()
}
#[test]
fn exact_ids_native_symbols_and_all_terminal_classes() {
    for (name, expected) in [
        ("open", Status::PartiallyFilled),
        ("triggered", Status::PartiallyFilled),
        ("canceled", Status::Canceled),
        ("scheduledCancel", Status::Canceled),
        ("reduceOnlyCanceled", Status::Canceled),
    ] {
        let o = hyperliquid::decode_rest(&rest("7", name, 100)).unwrap();
        assert_eq!(o.key, key());
        assert_eq!(o.cumulative, decimal("3").unwrap());
        assert_eq!(o.status, expected);
    }
    assert_eq!(
        hyperliquid::decode_rest(&rest("0", "filled", 100))
            .unwrap()
            .cumulative,
        decimal("10").unwrap()
    );
    assert_eq!(
        hyperliquid::decode_rest(&rest("10", "oracleRejected", 100))
            .unwrap()
            .status,
        Status::Rejected
    );
    assert!(hyperliquid::decode_rest(&rest("7", "futureRejected", 100)).is_err());
}
#[test]
fn strict_sizes_timestamps_snapshot_flags_and_unknown_order() {
    for (q, status) in [
        ("-1", "open"),
        ("11", "open"),
        ("2", "filled"),
        ("2", "rejected"),
        ("NaN", "open"),
    ] {
        assert!(hyperliquid::decode_rest(&rest(q, status, 100)).is_err());
    }
    assert_eq!(
        hyperliquid::decode_rest("{\"status\":\"unknownOid\"}").unwrap_err(),
        Error::UnknownOrder
    );
    let mut raw: Value = serde_json::from_str(&fills(vec![fill(1, "2")], true)).unwrap();
    raw["data"]["isSnapshot"] = json!("true");
    assert!(hyperliquid::decode_ws(&raw.to_string(), USER).is_err());
    let mut o = order("9", "open", 100);
    o["order"]["sz"] = json!(9.0);
    assert!(hyperliquid::decode_rest(&json!({"status":"order","order":o}).to_string()).is_err());
}
#[tokio::test]
async fn snapshot_live_and_reconnect_duplicates_do_not_add_twice() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    assert_eq!(
        ingest(&c, &s, &fills(vec![fill(1, "2"), fill(2, "3")], true))
            .await
            .new_fills,
        2
    );
    assert_eq!(
        ingest(&c, &s, &fills(vec![fill(2, "3")], false))
            .await
            .duplicate_fills,
        1
    );
    c.disconnected(s).await.unwrap();
    let s = c.connected().await.unwrap();
    assert_eq!(
        ingest(&c, &s, &fills(vec![fill(1, "2"), fill(2, "3")], true))
            .await
            .duplicate_fills,
        2
    );
    let sum = c.hyperliquid_fill_summary(key()).await.unwrap().unwrap();
    assert_eq!(sum.observed_quantity, decimal("5").unwrap());
    assert_eq!(c.health().unresolved_fill_orders, 1);
    assert!(!c.health().ready_for_new_risk());
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn fills_before_metadata_remain_pending_until_order_status_confirms() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    ingest(&c, &s, &fills(vec![fill(1, "2")], false)).await;
    assert!(c.snapshots().await.unwrap().is_empty());
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    let o = c
        .ingest_hyperliquid_rest(t, &rest("8", "open", 200))
        .await
        .unwrap();
    assert_eq!(o.cumulative, decimal("2").unwrap());
    assert!(!o.requires_reconciliation);
    assert!(c.hyperliquid_pending().await.unwrap().is_empty());
    assert!(c.health().ready_for_new_risk());
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn rest_baseline_and_later_trade_are_not_added_or_treated_as_complete() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    c.register(key(), decimal("10").unwrap()).await.unwrap();
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    c.ingest_hyperliquid_rest(t, &rest("6", "open", 100))
        .await
        .unwrap();
    ingest(&c, &s, &fills(vec![fill(1, "3")], false)).await;
    assert_eq!(
        c.subscribe(key()).await.unwrap().borrow().cumulative,
        decimal("4").unwrap()
    );
    assert!(!c.health().ready_for_new_risk());
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    let o = c
        .ingest_hyperliquid_rest(t, &rest("3", "open", 200))
        .await
        .unwrap();
    assert_eq!(o.cumulative, decimal("7").unwrap());
    assert!(!o.requires_reconciliation);
    // Snapshot repeats overlapping history; never add 7+4+3.
    ingest(&c, &s, &fills(vec![fill(0, "4"), fill(1, "3")], true)).await;
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    let o = c
        .ingest_hyperliquid_rest(t, &rest("3", "open", 200))
        .await
        .unwrap();
    assert_eq!(o.cumulative, decimal("7").unwrap());
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn fill_arriving_during_discovery_invalidates_older_query() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    ingest(&c, &s, &fills(vec![fill(1, "2")], false)).await;
    let o = c
        .ingest_hyperliquid_rest(t, &rest("8", "open", 200))
        .await
        .unwrap();
    assert!(o.requires_reconciliation);
    assert_eq!(c.health().unresolved_fill_orders, 1);
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    assert!(
        !c.ingest_hyperliquid_rest(t, &rest("8", "open", 200))
            .await
            .unwrap()
            .requires_reconciliation
    );
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn stale_query_cannot_hide_more_observed_fills_or_clear_gap() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    ingest(&c, &s, &fills(vec![fill(1, "5")], false)).await;
    for (remaining, time) in [("8", 200), ("5", 90)] {
        let t = c.begin_hyperliquid_query(key()).await.unwrap();
        assert!(
            c.ingest_hyperliquid_rest(t, &rest(remaining, "open", time))
                .await
                .unwrap()
                .requires_reconciliation
        );
        assert_eq!(c.health().unresolved_fill_orders, 1);
    }
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn late_fills_after_cancel_are_preserved_until_final_quantity_reconciles() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    ingest(&c, &s, &ws_order("8", "canceled", 200)).await;
    ingest(&c, &s, &fills(vec![fill(1, "3")], false)).await;
    assert_eq!(
        c.subscribe(key()).await.unwrap().borrow().status,
        Status::Canceled
    );
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    let o = c
        .ingest_hyperliquid_rest(t, &rest("7", "canceled", 201))
        .await
        .unwrap();
    assert_eq!(o.cumulative, decimal("3").unwrap());
    assert!(!o.requires_reconciliation);
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn conflicting_duplicate_is_sticky_fault_without_overwriting_quantity() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    ingest(&c, &s, &fills(vec![fill(1, "2")], false)).await;
    assert_eq!(
        c.ingest_hyperliquid_ws_at(s, &fills(vec![fill(1, "3")], false), Instant::now())
            .await
            .unwrap_err(),
        Error::IdentityConflict
    );
    assert_eq!(
        c.hyperliquid_fill_summary(key())
            .await
            .unwrap()
            .unwrap()
            .observed_quantity,
        decimal("2").unwrap()
    );
    assert_eq!(c.health().fault, Some(Error::IdentityConflict));
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn bounded_dedup_does_not_evict_and_recount_old_trades() {
    let (c, w) = channel(1);
    let s = c.connected().await.unwrap();
    ingest(
        &c,
        &s,
        &fills((0..16).map(|i| fill(i, "0.1")).collect(), true),
    )
    .await;
    assert_eq!(
        c.ingest_hyperliquid_ws_at(
            s.clone(),
            &fills(vec![fill(16, "0.1")], false),
            Instant::now()
        )
        .await
        .unwrap_err(),
        Error::Capacity
    );
    assert_eq!(
        ingest(&c, &s, &fills(vec![fill(0, "0.1")], false))
            .await
            .duplicate_fills,
        1
    );
    assert_eq!(
        c.hyperliquid_fill_summary(key())
            .await
            .unwrap()
            .unwrap()
            .unique_fills,
        16
    );
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn same_trade_id_different_orders_and_native_symbols_are_separate() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    let mut f2 = fill(1, "3");
    f2["oid"] = json!(2);
    f2["coin"] = json!("kPEPE");
    let mut f3 = f2.clone();
    f3["coin"] = json!("KPEPE");
    assert_eq!(
        ingest(&c, &s, &fills(vec![fill(1, "2"), f2, f3], false))
            .await
            .new_fills,
        3
    );
    assert_eq!(c.hyperliquid_pending().await.unwrap().len(), 3);
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn wrong_user_venue_and_instance_are_rejected() {
    let (c, w) = channel(16);
    let (other, ow) = channel(16);
    let s = other.connected().await.unwrap();
    c.connected().await.unwrap();
    assert_eq!(
        c.ingest_hyperliquid_ws_at(s, &fills(vec![fill(1, "2")], false), Instant::now())
            .await
            .unwrap_err(),
        Error::WrongSession
    );
    let mut raw: Value = serde_json::from_str(&fills(vec![], true)).unwrap();
    raw["data"]["user"] = json!("0x0000000000000000000000000000000000000002");
    assert!(hyperliquid::decode_ws(&raw.to_string(), USER).is_err());
    c.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
    w.await.unwrap();
    ow.await.unwrap();
}
#[tokio::test]
async fn reconnect_fences_query_and_preserves_old_session_new_fills() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    ingest(&c, &s, &ws_order("8", "open", 200)).await;
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    c.disconnected(s.clone()).await.unwrap();
    c.connected().await.unwrap();
    ingest(&c, &s, &fills(vec![fill(1, "3")], false)).await;
    assert!(
        c.ingest_hyperliquid_rest(t, &rest("7", "open", 300))
            .await
            .unwrap()
            .requires_reconciliation
    );
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn unknown_oid_preserves_pending_and_known_fills() {
    let (c, w) = channel(16);
    let s = c.connected().await.unwrap();
    ingest(&c, &s, &ws_order("8", "open", 200)).await;
    ingest(&c, &s, &fills(vec![fill(1, "2")], false)).await;
    let t = c.begin_hyperliquid_query(key()).await.unwrap();
    assert_eq!(
        c.ingest_hyperliquid_rest(t, "{\"status\":\"unknownOid\"}")
            .await
            .unwrap_err(),
        Error::UnknownOrder
    );
    assert_eq!(
        c.subscribe(key()).await.unwrap().borrow().cumulative,
        decimal("2").unwrap()
    );
    assert_eq!(c.health().unresolved_fill_orders, 1);
    c.shutdown().await.unwrap();
    w.await.unwrap();
}
#[tokio::test]
async fn reordered_duplicate_fills_sum_exactly_in_all_six_orders() {
    for permutation in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let (c, w) = channel(16);
        let s = c.connected().await.unwrap();
        let fs = [fill(1, "0.1"), fill(2, "0.2"), fill(1, "0.1")];
        for i in permutation {
            ingest(&c, &s, &fills(vec![fs[i].clone()], false)).await;
        }
        assert_eq!(
            c.hyperliquid_fill_summary(key())
                .await
                .unwrap()
                .unwrap()
                .observed_quantity,
            decimal("0.3").unwrap()
        );
        c.shutdown().await.unwrap();
        w.await.unwrap();
    }
}
