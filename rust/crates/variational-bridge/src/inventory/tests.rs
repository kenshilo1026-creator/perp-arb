use super::*;
use crate::wire::{Channel, Event, Observation};
use serde_json::json;
const ACCOUNT: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
fn receipt() -> Value {
    let i = json!({"underlying":"ETH","instrument_type":"perpetual_future","settlement_asset":"USDC","funding_interval_s":3600});
    let r = json!({"positions_source":"https://omni.variational.io/api/positions","portfolio_source":"https://omni.variational.io/api/portfolio?compute_margin=true",
        "positions":[{"instrument":i,"qty":"-2","avg_entry_price":"100"}],
        "portfolio":{"balance":"500","cross_balance":"500","isolated":[]},"portfolio_server_ms":11000,"observed_ms":11500,
        "orders":{"source":"https://omni.variational.io/api/orders/v2","rows":[],"window_complete":true,"atomic_snapshot":false,"observed_ms":11500}});
    json!({"protocol":crate::native::PROTOCOL,"started_ms":10000,"finished_ms":12000,"account":ACCOUNT,"session_continuity":true,"ok":true,"dispatched":false,"inventory":{"rounds":[r.clone(),r]}})
}
fn query(r: Value) -> Result<Query> {
    Query::new(Receipt::parse(&r, Some(ACCOUNT), 12000)?, Instant::now())
}

#[tokio::test]
async fn selected_symbol_ignores_other_positions_wallets_and_orders_but_keeps_session_fence() {
    let mut feed = crate::account_feed::Feed::start_symbol(ACCOUNT, "ETH").unwrap();
    feed.ingest(&event(Observation::Connected, 1))
        .await
        .unwrap();
    let c = feed.coordinator();
    let mut r = receipt();
    for (index, round) in r["inventory"]["rounds"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        let mut other = round["positions"][0].clone();
        other["instrument"]["underlying"] = json!("BTC");
        other["qty"] = json!(if index == 0 { "3" } else { "4" });
        round["positions"].as_array_mut().unwrap().push(other);
        round["portfolio"]["balance"] = json!(if index == 0 { "500" } else { "501" });
        round["orders"]["rows"] = json!([{"instrument":round["positions"][1]["instrument"].clone(),"status":"pending","rfq_id":"manual"}]);
    }
    let ticket = c.begin_account_query().await.unwrap();
    let report = query(r)
        .unwrap()
        .reconcile_symbol(&c, ticket, "ETH")
        .await
        .unwrap();
    assert_eq!(report.inventory().positions.len(), 1);
    assert!(report.inventory().wallets.is_empty());
    assert!(report.inventory().open_orders.is_empty());
    assert!(!report.matches_expected()); // It does not pretend to be a whole-account audit.
    feed.ingest(&event(
        Observation::Unclassified {
            keys: vec!["funding".into()],
        },
        1,
    ))
    .await
    .unwrap();
    assert!(report.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(30)
    ));
    assert!(!report.usable(
        c.scope(),
        &c.health(),
        Instant::now() + Duration::from_secs(31),
        Duration::from_secs(30)
    ));
    feed.ingest(&event(Observation::Disconnected, 1))
        .await
        .unwrap();
    assert!(!report.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(30)
    ));
    feed.shutdown().await.unwrap();
}
fn expected() -> Inventory {
    let mut inv = Inventory {
        perp_dexes: BTreeSet::from([String::new()]),
        includes_wallets: true,
        ..Inventory::default()
    };
    inv.wallets.insert("USDC".into(), decimal("500").unwrap());
    inv.positions.insert(
        PositionKey {
            symbol: "ETH".into(),
            side: "BOTH".into(),
        },
        decimal("-2").unwrap(),
    );
    inv
}
#[test]
fn stable_inventory_includes_signed_positions_and_wallets() {
    assert_eq!(
        query(receipt()).unwrap().snapshot().unwrap().inventory,
        expected()
    );
}
#[tokio::test]
async fn slow_hedge_audit_does_not_age_completed_browser_query_before_reconcile() {
    let mut feed = crate::account_feed::Feed::start_positions_only(ACCOUNT).unwrap();
    feed.ingest(&event(Observation::Connected, 1))
        .await
        .unwrap();
    let c = feed.coordinator();
    let ticket = c.begin_account_query().await.unwrap();
    let q = query(receipt()).unwrap();
    let hedge = async {
        // Longer than the browser observation's five-second consumption limit.
        tokio::time::sleep(Duration::from_millis(5100)).await;
        Err("hedge audit failed independently".into())
    };
    let (maker_result, hedge_result) = crate::paired::runtime::reconcile_audits(
        async { Ok(q) },
        hedge,
        &c,
        ticket,
        Some(expected()),
    )
    .await;
    assert!(maker_result.unwrap().matches_expected());
    assert_eq!(
        hedge_result.unwrap_err(),
        "hedge audit failed independently"
    );
    feed.shutdown().await.unwrap();
}

#[test]
fn genuinely_stale_inventory_is_still_rejected() {
    let mut q = query(receipt()).unwrap();
    q.completed = Instant::now() - Duration::from_secs(6);
    assert_eq!(
        q.snapshot_with_wallets(false).unwrap_err(),
        "inventory observation stale"
    );
}
#[test]
fn execution_excludes_balance_changes_but_keeps_position_and_source_checks() {
    let mut r = receipt();
    r["inventory"]["rounds"][1]["portfolio"]["balance"] = json!("500.10");
    r["inventory"]["rounds"][1]["portfolio"]["cross_balance"] = json!("500.10");
    assert!(query(r.clone()).unwrap().snapshot().is_err());
    let snapshot = query(r.clone())
        .unwrap()
        .snapshot_with_wallets(false)
        .unwrap();
    assert!(!snapshot.inventory.includes_wallets);
    assert!(snapshot.inventory.wallets.is_empty());
    assert_eq!(snapshot.inventory.positions, expected().positions);
    for change in 0..4 {
        let mut bad = r.clone();
        let round = &mut bad["inventory"]["rounds"][1];
        match change {
            0 => round["positions"][0]["qty"] = json!("-3"),
            1 => round["positions"][0]["avg_entry_price"] = json!("101"),
            2 => round["orders"]["window_complete"] = json!(false),
            _ => round["portfolio_server_ms"] = json!(0),
        }
        assert!(query(bad)
            .and_then(|q| q.snapshot_with_wallets(false))
            .is_err());
    }
}

#[tokio::test]
async fn execution_balance_ws_changes_do_not_invalidate_positions_report() {
    let mut feed = crate::account_feed::Feed::start_positions_only(ACCOUNT).unwrap();
    feed.ingest(&event(Observation::Connected, 1))
        .await
        .unwrap();
    let portfolio = |qty: &str, balance: &str| Observation::Portfolio {
        positions: vec![crate::wire::Position {
            symbol: "ETH".into(),
            signed_quantity: qty.into(),
        }],
        wallet_balance: Some(balance.into()),
    };
    feed.ingest(&event(portfolio("-2", "500"), 1))
        .await
        .unwrap();
    let c = feed.coordinator();
    let ticket = c.begin_account_query().await.unwrap();
    feed.ingest(&event(portfolio("-2", "500.1"), 1))
        .await
        .unwrap();
    let mut r = receipt();
    r["inventory"]["rounds"][1]["portfolio"]["balance"] = json!("500.1");
    r["inventory"]["rounds"][1]["portfolio"]["cross_balance"] = json!("500.1");
    // Old baselines with wallet values also work; only positions/orders compare.
    let report = query(r)
        .unwrap()
        .reconcile_positions(&c, ticket, Some(expected()))
        .await
        .unwrap();
    assert!(report.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(30)
    ));
    feed.ingest(&event(portfolio("-2", "501"), 1))
        .await
        .unwrap();
    assert!(report.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(30)
    ));
    feed.ingest(&event(portfolio("-3", "501"), 1))
        .await
        .unwrap();
    assert!(!report.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(30)
    ));
    feed.shutdown().await.unwrap();
}
#[test]
fn changing_or_incomplete_or_wrong_account_snapshots_are_not_certified() {
    for change in 0..11 {
        let mut r = receipt();
        let round = &mut r["inventory"]["rounds"][1];
        match change {
            0 => round["portfolio"]["balance"] = json!("501"),
            1 => round["positions"][0]["qty"] = json!("-3"),
            2 => round["orders"]["window_complete"] = json!(false),
            3 => round["positions"][0]["instrument"]["instrument_type"] = json!("swap"),
            4 => round["portfolio"]["isolated"] = Value::Null,
            5 => round["portfolio_server_ms"] = json!(0),
            6 => {
                let p = round["positions"][0].clone();
                round["positions"].as_array_mut().unwrap().push(p);
            }
            7 => round["positions"][0]["instrument"]["funding_interval_s"] = json!(7200),
            8 => round["positions"][0]["avg_entry_price"] = json!("101"),
            9 => round["positions"][0]["avg_entry_price"] = json!("NaN"),
            _ => r["account"] = json!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        }
        assert!(
            query(r).and_then(|q| q.snapshot()).is_err(),
            "change {change}"
        );
    }
}
#[test]
fn isolated_collateral_is_accounted_without_enabling_swap_positions() {
    let mut r = receipt();
    for round in r["inventory"]["rounds"].as_array_mut().unwrap() {
        let mut i = round["positions"][0]["instrument"].clone();
        i["instrument_type"] = json!("swap");
        round["portfolio"]["isolated"] =
            json!([{"instrument":i,"balance":"5","allocated_qty":"0"}]);
        round["portfolio"]["balance"] = json!("505");
    }
    let inv = query(r.clone()).unwrap().snapshot().unwrap().inventory;
    assert_eq!(inv.wallets.len(), 2);
    r["inventory"]["rounds"][0]["portfolio"]["isolated"][0]["allocated_qty"] = json!("1");
    assert!(query(r).unwrap().snapshot().is_err());
}
fn event(o: Observation, generation: u64) -> Event {
    Event {
        channel: Channel::Private,
        generation,
        received_ms: 12000,
        captured_ms: Some(12000),
        received_elapsed_us: 0,
        parsed_elapsed_us: 0,
        observation: o,
    }
}
#[tokio::test]
async fn no_expected_inventory_never_arms_and_independent_expected_can_match() {
    let mut feed = crate::account_feed::Feed::start(ACCOUNT).unwrap();
    feed.ingest(&event(Observation::Connected, 1))
        .await
        .unwrap();
    let c = feed.coordinator();
    let t = c.begin_account_query().await.unwrap();
    let report = query(receipt())
        .unwrap()
        .reconcile(&c, t, None)
        .await
        .unwrap();
    assert!(!report.matches_expected());
    let t = c.begin_account_query().await.unwrap();
    let report = query(receipt())
        .unwrap()
        .reconcile(&c, t, Some(expected()))
        .await
        .unwrap();
    assert!(report.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(30)
    ));
    feed.ingest(&event(Observation::Heartbeat, 1))
        .await
        .unwrap();
    assert!(report.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(30)
    ));
    feed.ingest(&event(
        Observation::Unclassified {
            keys: vec!["funding".into()],
        },
        1,
    ))
    .await
    .unwrap();
    assert!(!report.usable(
        c.scope(),
        &c.health(),
        Instant::now(),
        Duration::from_secs(30)
    ));
    feed.shutdown().await.unwrap();
}
#[tokio::test]
async fn event_during_read_and_gap_reject_old_tickets_then_allow_new_baseline() {
    let mut feed = crate::account_feed::Feed::start(ACCOUNT).unwrap();
    feed.ingest(&event(Observation::Connected, 1))
        .await
        .unwrap();
    let c = feed.coordinator();
    let t = c.begin_account_query().await.unwrap();
    let q = query(receipt()).unwrap();
    feed.ingest(&event(Observation::Unclassified { keys: vec![] }, 1))
        .await
        .unwrap();
    assert!(q.reconcile(&c, t, Some(expected())).await.is_err());
    let t = c.begin_account_query().await.unwrap();
    let q = query(receipt()).unwrap();
    feed.ingest(&event(
        Observation::Gap {
            reason: "fixture".into(),
        },
        1,
    ))
    .await
    .unwrap();
    assert!(q.reconcile(&c, t, Some(expected())).await.is_err());
    feed.ingest(&event(Observation::Heartbeat, 1))
        .await
        .unwrap();
    let t = c.begin_account_query().await.unwrap();
    assert!(query(receipt())
        .unwrap()
        .reconcile(&c, t, Some(expected()))
        .await
        .unwrap()
        .matches_expected());
    feed.shutdown().await.unwrap();
}
