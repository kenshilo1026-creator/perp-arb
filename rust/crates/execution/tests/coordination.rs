use perp_arb_core::{decimal, Decimal};
use perp_arb_execution::{Engine, Error, Leg, Plan, Side};
use perp_arb_private_events::{
    account::{Inventory, Snapshot},
    actor::{self, Coordinator},
    Evidence, OrderKey, OrderSnapshot, Scope, Status, Update,
};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
fn d(q: &str) -> Decimal {
    decimal(q).unwrap()
}
fn scope(v: &str) -> Scope {
    Scope {
        venue: v.into(),
        account: "fixture".into(),
    }
}
fn key(symbol: &str, id: &str) -> OrderKey {
    OrderKey {
        symbol: symbol.into(),
        order_id: id.into(),
    }
}

#[tokio::test]
async fn position_reports_arm_only_the_selected_leg_and_expire_on_disconnect() {
    let (m, mw) = actor::position_channel(scope("aster"), "ETHUSDT", 32, 8).unwrap();
    let (h, hw) = actor::position_channel(scope("hyperliquid"), "ETH", 32, 8).unwrap();
    let tasks = [mw.spawn(), hw.spawn()];
    for (c, symbol) in [(&m, "ETHUSDT"), (&h, "ETH")] {
        c.connected().await.unwrap();
        let ticket = c.begin_account_query().await.unwrap();
        let now = Instant::now();
        c.apply_position_snapshot(
            ticket,
            Snapshot {
                scope: c.scope().clone(),
                inventory: Inventory {
                    perp_dexes: BTreeSet::from([String::new()]),
                    ..Inventory::default()
                },
                started_at: now,
                completed_at: now,
                exchange_time_ms: None,
            },
            symbol,
        )
        .await
        .unwrap();
    }
    let p = Plan {
        run_id: "scoped".into(),
        maker: Leg {
            scope: m.scope().clone(),
            symbol: "ETHUSDT".into(),
            side: Side::Buy,
        },
        hedge: Leg {
            scope: h.scope().clone(),
            symbol: "ETH".into(),
            side: Side::Sell,
        },
        maker_order: key("ETHUSDT", "1"),
        quantity: d("1"),
        step: d("0.1"),
        minimum: d("0.1"),
    };
    let mr = m.account_report().await.unwrap().unwrap();
    let hr = h.account_report().await.unwrap().unwrap();
    assert!(Engine::arm(
        p.clone(),
        (&mr, &m.health()),
        (&hr, &h.health()),
        true,
        Duration::from_secs(5)
    )
    .is_ok());
    let mut wrong = p.clone();
    wrong.hedge.symbol = "BTC".into();
    assert!(Engine::arm(
        wrong,
        (&mr, &m.health()),
        (&hr, &h.health()),
        true,
        Duration::from_secs(5)
    )
    .is_err());
    m.transport_lost().await.unwrap();
    assert!(Engine::arm(
        p,
        (&mr, &m.health()),
        (&hr, &h.health()),
        true,
        Duration::from_secs(5)
    )
    .is_err());
    m.shutdown().await.unwrap();
    h.shutdown().await.unwrap();
    for t in tasks {
        t.await.unwrap();
    }
}
async fn setup() -> (
    Engine,
    Coordinator,
    Coordinator,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let (m, mw) = actor::channel(scope("aster"), 32, 8).unwrap();
    let (h, hw) = actor::channel(scope("hyperliquid"), 32, 8).unwrap();
    let tasks = vec![mw.spawn(), hw.spawn()];
    m.connected().await.unwrap();
    h.connected().await.unwrap();
    for c in [&m, &h] {
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
        run_id: "paper1".into(),
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
    let mut undersized = p.clone();
    undersized.minimum = d("11");
    assert!(matches!(
        Engine::arm(
            undersized,
            (&mr, &m.health()),
            (&hr, &h.health()),
            true,
            Duration::from_secs(5)
        ),
        Err(Error::Configuration)
    ));
    assert!(matches!(
        Engine::arm(
            p.clone(),
            (&mr, &m.health()),
            (&hr, &h.health()),
            false,
            Duration::from_secs(5)
        ),
        Err(Error::NewRiskBlocked)
    ));
    let engine = Engine::arm(
        p,
        (&mr, &m.health()),
        (&hr, &h.health()),
        true,
        Duration::from_secs(5),
    )
    .unwrap();
    m.register(key("ETHUSDT", "1"), d("10")).await.unwrap();
    (engine, m, h, tasks)
}
fn snap(
    venue: &str,
    symbol: &str,
    id: &str,
    original: &str,
    cumulative: &str,
    status: Status,
    revision: u64,
) -> OrderSnapshot {
    let now = Instant::now();
    OrderSnapshot {
        scope: scope(venue),
        key: key(symbol, id),
        original: d(original),
        cumulative: d(cumulative),
        status,
        event_ms: Some(revision),
        revision,
        requires_reconciliation: false,
        observed_at: now,
        applied_at: now,
    }
}
async fn finish(m: Coordinator, h: Coordinator, tasks: Vec<tokio::task::JoinHandle<()>>) {
    m.shutdown().await.unwrap();
    h.shutdown().await.unwrap();
    for t in tasks {
        t.await.unwrap();
    }
}
#[tokio::test]
async fn private_coordinator_to_execution_partial_fills_while_hedge_is_unknown() {
    let (mut e, m, h, tasks) = setup().await;
    let session = m.connected().await.unwrap();
    let mut updates = e.maker_stream(&m).await.unwrap();
    for qty in ["2", "2", "5"] {
        m.observe(
            Update {
                key: key("ETHUSDT", "1"),
                original: d("10"),
                cumulative: d(qty),
                status: Status::PartiallyFilled,
                event_ms: Some(1),
            },
            Evidence::Ws(session.clone()),
            Instant::now(),
        )
        .await
        .unwrap();
        updates.changed().await.unwrap();
        e.observe_maker(&updates.borrow_and_update()).unwrap();
        if qty == "2" && e.in_flight().unwrap() == Decimal::ZERO {
            assert_eq!(e.reserve_hedge().unwrap().unwrap().quantity, d("2"));
        }
    }
    // First unknown submission is still reserved. Only the new three units are submitted.
    assert_eq!(e.reserve_hedge().unwrap().unwrap().quantity, d("3"));
    assert!(e.reserve_hedge().unwrap().is_none());
    assert_eq!(e.in_flight().unwrap(), d("5"));
    assert_eq!(e.confirmed_hedge().unwrap(), d("0"));
    assert!(!m.health().ready_for_new_risk()); // reconnect/order gap never blocks already owed hedges
    finish(m, h, tasks).await;
}
#[tokio::test]
async fn acknowledgements_duplicates_and_fill_before_ack_do_not_double_hedge() {
    let (mut e, m, h, tasks) = setup().await;
    e.observe_maker(&snap(
        "aster",
        "ETHUSDT",
        "1",
        "10",
        "4",
        Status::PartiallyFilled,
        1,
    ))
    .unwrap();
    let i = e.reserve_hedge().unwrap().unwrap();
    e.acknowledge(&i.id, key("ETH", "2")).unwrap();
    assert_eq!(e.confirmed_hedge().unwrap(), d("0"));
    assert!(e.reserve_hedge().unwrap().is_none());
    let filled = snap("hyperliquid", "ETH", "2", "4", "4", Status::Filled, 2);
    e.observe_hedge(&i.id, &filled).unwrap();
    e.observe_hedge(&i.id, &filled).unwrap();
    e.acknowledge(&i.id, key("ETH", "2")).unwrap();
    assert_eq!(e.exposure().unwrap(), d("0"));
    assert!(e.reserve_hedge().unwrap().is_none());
    e.observe_maker(&snap(
        "aster",
        "ETHUSDT",
        "1",
        "10",
        "6",
        Status::PartiallyFilled,
        3,
    ))
    .unwrap();
    let j = e.reserve_hedge().unwrap().unwrap();
    e.observe_hedge(
        &j.id,
        &snap("hyperliquid", "ETH", "3", "2", "2", Status::Filled, 1),
    )
    .unwrap();
    e.acknowledge(&j.id, key("ETH", "3")).unwrap();
    assert_eq!(e.exposure().unwrap(), d("0"));
    finish(m, h, tasks).await;
}
#[tokio::test]
async fn partial_cancel_gap_holds_reservation_until_terminal_reconciliation() {
    let (mut e, m, h, tasks) = setup().await;
    e.observe_maker(&snap(
        "aster",
        "ETHUSDT",
        "1",
        "10",
        "5",
        Status::PartiallyFilled,
        1,
    ))
    .unwrap();
    let i = e.reserve_hedge().unwrap().unwrap();
    let mut canceled = snap("hyperliquid", "ETH", "2", "5", "2", Status::Canceled, 2);
    canceled.requires_reconciliation = true;
    e.observe_hedge(&i.id, &canceled).unwrap();
    assert_eq!(e.in_flight().unwrap(), d("3"));
    assert!(e.reserve_hedge().unwrap().is_none());
    canceled.requires_reconciliation = false;
    canceled.revision = 3;
    e.observe_hedge(&i.id, &canceled).unwrap();
    assert_eq!(e.reserve_hedge().unwrap().unwrap().quantity, d("3"));
    // Late positive correction is retained and freezes automatic resubmission.
    canceled.cumulative = d("3");
    canceled.revision = 4;
    assert_eq!(
        e.observe_hedge(&i.id, &canceled),
        Err(Error::InvalidEvidence)
    );
    assert_eq!(e.exposure().unwrap(), d("2"));
    assert_eq!(e.reserve_hedge(), Err(Error::Faulted));
    finish(m, h, tasks).await;
}
#[tokio::test]
async fn cancel_race_dust_and_rejected_hedge_keep_exact_obligation() {
    let (mut e, m, h, tasks) = setup().await;
    let mut maker = snap("aster", "ETHUSDT", "1", "10", "0.15", Status::Canceled, 1);
    maker.requires_reconciliation = true;
    e.observe_maker(&maker).unwrap();
    assert!(!e.maker_terminal_confirmed());
    assert!(e.reserve_hedge().unwrap().is_none());
    assert_eq!(e.exposure().unwrap(), d("0.15"));
    maker.cumulative = d("0.35");
    maker.revision = 2;
    maker.requires_reconciliation = false;
    e.observe_maker(&maker).unwrap();
    assert!(e.maker_terminal_confirmed());
    let i = e.reserve_hedge().unwrap().unwrap();
    assert_eq!(i.quantity, d("0.3"));
    e.observe_hedge(
        &i.id,
        &snap("hyperliquid", "ETH", "2", "0.3", "0", Status::Rejected, 1),
    )
    .unwrap();
    let retry = e.reserve_hedge().unwrap().unwrap();
    assert_ne!(retry.id, i.id);
    assert_eq!(retry.quantity, d("0.3"));
    e.observe_hedge(
        &retry.id,
        &snap("hyperliquid", "ETH", "3", "0.3", "0.3", Status::Filled, 1),
    )
    .unwrap();
    assert_eq!(e.exposure().unwrap(), d("0.05"));
    assert!(e.reserve_hedge().unwrap().is_none());
    finish(m, h, tasks).await;
}
#[tokio::test]
async fn wrong_account_order_and_quantity_cannot_release_unknown_submission() {
    let (mut e, m, h, tasks) = setup().await;
    e.observe_maker(&snap(
        "aster",
        "ETHUSDT",
        "1",
        "10",
        "2",
        Status::PartiallyFilled,
        1,
    ))
    .unwrap();
    let i = e.reserve_hedge().unwrap().unwrap();
    let mut filled = snap("hyperliquid", "ETH", "2", "2", "2", Status::Filled, 1);
    filled.scope.account = "other".into();
    assert_eq!(e.observe_hedge(&i.id, &filled), Err(Error::Identity));
    filled.scope = scope("hyperliquid");
    filled.original = d("3");
    filled.status = Status::PartiallyFilled;
    assert_eq!(e.observe_hedge(&i.id, &filled), Err(Error::Identity));
    assert_eq!(e.in_flight().unwrap(), d("2"));
    assert_eq!(e.confirmed_hedge().unwrap(), d("0"));
    assert!(e.reserve_hedge().unwrap().is_none());
    finish(m, h, tasks).await;
}

#[tokio::test]
async fn both_live_coordinator_interfaces_drive_paper_hedge_to_completion() {
    let (mut e, m, h, tasks) = setup().await;
    let mt = m.begin_query(key("ETHUSDT", "1")).await.unwrap();
    m.observe(
        Update {
            key: key("ETHUSDT", "1"),
            original: d("10"),
            cumulative: d("10"),
            status: Status::Filled,
            event_ms: Some(10),
        },
        Evidence::Rest(mt),
        Instant::now(),
    )
    .await
    .unwrap();
    e.consume_maker(&m).await.unwrap();
    let intent = e.reserve_hedge().unwrap().unwrap();
    h.register(key("ETH", "20"), intent.quantity).await.unwrap();
    e.acknowledge(&intent.id, key("ETH", "20")).unwrap();
    let mut stream = e.hedge_stream(&intent.id, &h).await.unwrap();
    let ht = h.begin_query(key("ETH", "20")).await.unwrap();
    h.observe(
        Update {
            key: key("ETH", "20"),
            original: d("10"),
            cumulative: d("10"),
            status: Status::Filled,
            event_ms: Some(11),
        },
        Evidence::Rest(ht),
        Instant::now(),
    )
    .await
    .unwrap();
    stream.changed().await.unwrap();
    e.observe_hedge(&intent.id, &stream.borrow_and_update())
        .unwrap();
    assert_eq!(e.exposure().unwrap(), Decimal::ZERO);
    assert_eq!(e.in_flight().unwrap(), Decimal::ZERO);
    assert!(e.maker_terminal_confirmed());
    finish(m, h, tasks).await;
}
