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
