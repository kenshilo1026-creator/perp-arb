use perp_arb_core::decimal;
use perp_arb_private_events::{
    account::{self, Inventory, Snapshot},
    actor, Error, OrderKey, Scope,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
const USER: &str = "0x0000000000000000000000000000000000000001";
#[tokio::test]
async fn beginning_a_new_audit_immediately_revokes_the_old_report() {
    let (c, w) = actor::channel(scope("aster"), 8, 8).unwrap();
    let task = w.spawn();
    c.connected().await.unwrap();
    let t = c.begin_account_query().await.unwrap();
    let r = c
        .apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), Some(empty()))
        .await
        .unwrap();
    assert!(r.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(5)
    ));
    let _pending = c.begin_account_query().await.unwrap();
    assert!(!r.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(5)
    ));
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn mismatched_inventory_coverage_orders_and_age_are_never_usable() {
    let (c, w) = actor::channel(scope("aster"), 8, 8).unwrap();
    let task = w.spawn();
    c.connected().await.unwrap();
    let mut actual = empty();
    actual.positions.insert(
        account::PositionKey {
            symbol: "ETHUSDT".into(),
            side: "BOTH".into(),
        },
        decimal("2").unwrap(),
    );
    actual.open_orders.insert(
        key("ETHUSDT"),
        account::OpenOrder {
            side: "BUY".into(),
            remaining: decimal("1").unwrap(),
            price: decimal("100").unwrap(),
        },
    );
    let ticket = c.begin_account_query().await.unwrap();
    let r = c
        .apply_account_snapshot(ticket, snapshot(c.scope().clone(), actual), Some(empty()))
        .await
        .unwrap();
    assert_eq!(r.position_differences, 1);
    assert_eq!(r.order_differences, 1);
    assert!(!r.matches_expected());
    let mut expected = empty();
    expected.includes_spot = true;
    let t = c.begin_account_query().await.unwrap();
    let r = c
        .apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), Some(expected))
        .await
        .unwrap();
    assert!(!r.coverage_matches);
    let t = c.begin_account_query().await.unwrap();
    let r = c
        .apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), Some(empty()))
        .await
        .unwrap();
    assert!(!r.usable(
        c.scope(),
        &c.health(),
        Instant::now() + Duration::from_secs(10),
        Duration::from_secs(5)
    ));
    assert!(!r.usable(
        c.scope(),
        &c.health(),
        r.observed_at - Duration::from_secs(1),
        Duration::from_secs(5)
    ));
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
fn scope(venue: &str) -> Scope {
    Scope {
        venue: venue.into(),
        account: USER.into(),
    }
}
fn empty() -> Inventory {
    Inventory {
        perp_dexes: BTreeSet::from([String::new()]),
        includes_wallets: true,
        ..Inventory::default()
    }
}
fn snapshot(s: Scope, inv: Inventory) -> Snapshot {
    let now = Instant::now();
    Snapshot {
        scope: s,
        inventory: inv,
        started_at: now,
        completed_at: now,
        exchange_time_ms: Some(1000),
    }
}
fn fill(dir: &str, coin: &str, tid: u64, time: u64) -> String {
    json!({"channel":"userFills","data":{"user":USER,"isSnapshot":true,"fills":[{"coin":coin,"oid":1,"tid":tid,"sz":"2","px":"10","side":"A","time":time,"hash":format!("0x{:064x}",tid),"dir":dir}]}}).to_string()
}
fn key(coin: &str) -> OrderKey {
    OrderKey {
        symbol: coin.into(),
        order_id: "1".into(),
    }
}

#[test]
fn hedge_mode_positions_not_netted_and_wallets_are_not_mark_pnl() {
    let p = json!([{"symbol":"ETHUSDT","positionSide":"LONG","positionAmt":"2"},{"symbol":"ETHUSDT","positionSide":"SHORT","positionAmt":"-2"}]);
    let b = json!([{"asset":"USDT","balance":"100","availableBalance":"150"}]);
    let inv = account::aster_with_balances(&p, &json!([]), &b).unwrap();
    assert_eq!(inv.positions.len(), 2);
    assert_eq!(inv.wallets["USDT"], decimal("100").unwrap());
    let mut bad = p.clone();
    bad[1]["positionAmt"] = json!("2");
    assert!(account::aster(&bad, &json!([])).is_err());
    let mut duplicate = p.as_array().unwrap().clone();
    duplicate.push(p[0].clone());
    assert_eq!(
        account::aster(&json!(duplicate), &json!([])).unwrap_err(),
        Error::IdentityConflict
    );
}
#[test]
fn inventory_rejects_duplicate_orders_terminal_open_order_and_missing_fields() {
    let o = json!({"symbol":"ETHUSDT","orderId":1,"side":"BUY","origQty":"3","executedQty":"1","price":"2000","status":"PARTIALLY_FILLED"});
    assert_eq!(
        account::aster(&json!([]), &json!([o.clone()]))
            .unwrap()
            .open_orders
            .values()
            .next()
            .unwrap()
            .remaining,
        decimal("2").unwrap()
    );
    assert!(account::aster(&json!([]), &json!([o.clone(), o.clone()])).is_err());
    let mut terminal = o;
    terminal["status"] = json!("CANCELED");
    assert!(account::aster(&json!([]), &json!([terminal])).is_err());
    assert!(
        account::aster_with_balances(&json!([]), &json!([]), &json!([{"asset":"USDT"}])).is_err()
    );
}
#[test]
fn hl_dex_coverage_and_numeric_token_identity() {
    let state = json!({"marginSummary":{"totalRawUsd":"100"},"assetPositions":[{"position":{"coin":"xyz:ETH","szi":"-2"}}]});
    let spot = json!({"balances":[{"coin":"SAME","token":1,"total":"3","hold":"1"},{"coin":"SAME","token":2,"total":"4","hold":"0"}]});
    let inv =
        account::hyperliquid(&[("xyz".into(), state.clone(), json!([]))], Some(&spot)).unwrap();
    assert_eq!(inv.balances.len(), 2);
    assert!(!inv.covers("ETH"));
    assert!(inv.covers("xyz:ETH"));
    assert!(inv.covers("@1"));
    assert!(account::hyperliquid(&[(String::new(), state, json!([]))], Some(&spot)).is_err());
    assert!(account::hyperliquid(
        &[(String::new(), json!({"assetPositions":[]}), json!([]))],
        None
    )
    .is_err());
}
#[tokio::test]
async fn observational_snapshot_cannot_enable_risk_and_foreign_actor_ticket_rejected() {
    let (c, w) = actor::channel(scope("aster"), 8, 8).unwrap();
    let task = w.spawn();
    c.connected().await.unwrap();
    let t = c.begin_account_query().await.unwrap();
    let r = c
        .apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), None)
        .await
        .unwrap();
    assert!(!r.matches_expected());
    assert!(!r.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(5)
    ));
    let (other, worker) = actor::channel(scope("aster"), 8, 8).unwrap();
    let t2 = worker.spawn();
    other.connected().await.unwrap();
    let ticket = c.begin_account_query().await.unwrap();
    assert_eq!(
        other
            .apply_account_snapshot(
                ticket,
                snapshot(other.scope().clone(), empty()),
                Some(empty())
            )
            .await
            .unwrap_err(),
        Error::WrongSession
    );
    c.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
    task.await.unwrap();
    t2.await.unwrap();
}
#[tokio::test]
async fn account_updates_and_reconnect_invalidate_inflight_and_completed_proofs() {
    let (c, w) = actor::channel(scope("aster"), 8, 8).unwrap();
    let task = w.spawn();
    let s = c.connected().await.unwrap();
    let t = c.begin_account_query().await.unwrap();
    let r = c
        .apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), Some(empty()))
        .await
        .unwrap();
    assert!(r.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(5)
    ));
    let t = c.begin_account_query().await.unwrap();
    c.ingest_aster_ws(
        s.clone(),
        r#"{"e":"ACCOUNT_UPDATE","a":{"m":"FUNDING_FEE"}}"#,
    )
    .await
    .unwrap();
    assert!(!r.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(5)
    ));
    assert_eq!(
        c.apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), Some(empty()))
            .await
            .unwrap_err(),
        Error::WrongSession
    );
    let t = c.begin_account_query().await.unwrap();
    c.disconnected(s).await.unwrap();
    c.connected().await.unwrap();
    assert_eq!(
        c.apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), Some(empty()))
            .await
            .unwrap_err(),
        Error::WrongSession
    );
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn external_fill_routes_to_inventory_without_creating_executable_order() {
    let (c, w) = actor::channel(scope("hyperliquid"), 16, 8).unwrap();
    let task = w.spawn();
    let s = c.connected().await.unwrap();
    c.ingest_hyperliquid_ws_at(
        s.clone(),
        &fill("Settlement", "ETH", 1, 100),
        Instant::now(),
    )
    .await
    .unwrap();
    assert!(c.hyperliquid_pending().await.unwrap().is_empty());
    assert_eq!(c.external_pending().await.unwrap(), vec![key("ETH")]);
    assert!(c.snapshots().await.unwrap().is_empty());
    // Observational equality and a mismatched expected wallet both leave exposure unresolved.
    for expected in [
        None,
        Some({
            let mut e = empty();
            e.wallets.insert("USDC".into(), decimal("1").unwrap());
            e
        }),
    ] {
        let t = c.begin_account_query().await.unwrap();
        let r = c
            .apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), expected)
            .await
            .unwrap();
        assert!(!r.matches_expected());
        assert_eq!(c.health().unresolved_external_orders, 1);
    }
    let t = c.begin_account_query().await.unwrap();
    let mut old = snapshot(c.scope().clone(), empty());
    old.exchange_time_ms = Some(100);
    c.apply_account_snapshot(t, old, Some(empty()))
        .await
        .unwrap();
    assert_eq!(c.health().unresolved_external_orders, 1);
    let t = c.begin_account_query().await.unwrap();
    let r = c
        .apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), Some(empty()))
        .await
        .unwrap();
    assert_eq!(r.external_orders_accounted, 1);
    assert_eq!(c.health().unresolved_external_orders, 0);
    assert!(c.snapshots().await.unwrap().is_empty());
    c.disconnected(s).await.unwrap();
    c.connected().await.unwrap();
    assert_eq!(c.health().unresolved_external_orders, 1);
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn dust_without_spot_server_watermark_never_cleared_by_perp_timestamp() {
    let (c, w) = actor::channel(scope("hyperliquid"), 8, 8).unwrap();
    let task = w.spawn();
    let s = c.connected().await.unwrap();
    c.ingest_hyperliquid_ws_at(
        s,
        &fill("Spot Dust Conversion", "@1", 1, 100),
        Instant::now(),
    )
    .await
    .unwrap();
    let mut inv = empty();
    inv.includes_spot = true;
    let t = c.begin_account_query().await.unwrap();
    let r = c
        .apply_account_snapshot(t, snapshot(c.scope().clone(), inv.clone()), Some(inv))
        .await
        .unwrap();
    assert_eq!(r.external_orders_accounted, 0);
    assert!(!c.health().ready_for_new_risk());
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn same_tid_at_different_block_times_is_not_discarded() {
    let (c, w) = actor::channel(scope("hyperliquid"), 8, 8).unwrap();
    let task = w.spawn();
    let s = c.connected().await.unwrap();
    for time in [100, 100, 101] {
        c.ingest_hyperliquid_ws_at(
            s.clone(),
            &fill("Close Long", "ETH", 7, time),
            Instant::now(),
        )
        .await
        .unwrap();
    }
    let f = c
        .hyperliquid_fill_summary(key("ETH"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.unique_fills, 2);
    assert_eq!(f.observed_quantity, decimal("4").unwrap());
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn funding_transfer_and_liquidation_ledger_invalidate_proof_not_order_quantity() {
    let (c, w) = actor::channel(scope("hyperliquid"), 8, 8).unwrap();
    let task = w.spawn();
    let s = c.connected().await.unwrap();
    for (channel, field) in [
        ("userFundings", "fundings"),
        ("userNonFundingLedgerUpdates", "nonFundingLedgerUpdates"),
    ] {
        let t = c.begin_account_query().await.unwrap();
        let r = c
            .apply_account_snapshot(t, snapshot(c.scope().clone(), empty()), Some(empty()))
            .await
            .unwrap();
        let mut data = json!({"user":USER});
        data[field] = json!([{"delta":{"type":"liquidation"}}]);
        c.ingest_hyperliquid_ws_at(
            s.clone(),
            &json!({"channel":channel,"data":data}).to_string(),
            Instant::now(),
        )
        .await
        .unwrap();
        assert!(!r.usable(
            c.scope(),
            &c.health(),
            Instant::now(),
            Duration::from_secs(5)
        ));
        assert!(c.snapshots().await.unwrap().is_empty());
    }
    c.shutdown().await.unwrap();
    task.await.unwrap();
}
#[test]
fn unsupported_or_malformed_account_payload_is_not_a_zero_balance() {
    for data in [
        Value::Null,
        json!({"user":USER}),
        json!({"user":"wrong","fundings":[]}),
        json!({"user":USER,"fundings":[1]}),
    ] {
        assert!(perp_arb_private_events::hyperliquid::decode_ws(
            &json!({"channel":"userFundings","data":data}).to_string(),
            USER
        )
        .is_err());
    }
}
