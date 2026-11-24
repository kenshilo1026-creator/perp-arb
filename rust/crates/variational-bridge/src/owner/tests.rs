use super::*;
use crate::native::{Receipt, PROTOCOL};
use perp_arb_core::decimal;
use perp_arb_execution::{Leg, Plan, Side};
use perp_arb_private_events::{
    account::{Inventory, Snapshot},
    actor,
    lookup::{Found, Selector},
    Scope, Status, Update,
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    path::PathBuf,
    time::{Duration, Instant},
};
const ACCOUNT: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn fast_ws_event() -> crate::wire::Event {
    use crate::wire::*;
    Event {
        channel: Channel::Private,
        generation: 1,
        received_ms: 1000,
        captured_ms: Some(1000),
        received_elapsed_us: 0,
        parsed_elapsed_us: 1,
        observation: Observation::FillCandidate {
            candidate: Box::new(FillCandidate {
                symbol: Some("ETH".into()),
                side: Some("BUY".into()),
                order_id: Some("rfq-1".into()),
                trade_id: Some("ws-1".into()),
                quantity: Some("1".into()),
                quote_amount: None,
                price: Some("99".into()),
                status: Some("confirmed".into()),
                trade_type: Some("trade".into()),
                instrument: Some(crate::paired::tests::plan("aster").instrument),
            }),
        },
    }
}
struct FastVenue(std::sync::atomic::AtomicUsize);
impl perp_arb_order_test::venue::Venue for FastVenue {
    async fn submit(
        &self,
        _: &perp_arb_private_events::orders::Order,
    ) -> Result<perp_arb_private_events::orders::Accepted> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(perp_arb_private_events::orders::Accepted {
            order_id: "1".into(),
            cumulative: None,
        })
    }
    async fn cancel(&self, _: &perp_arb_private_events::orders::Order) -> Result<()> {
        panic!("no cancellation on WS hedge lane")
    }
    async fn lookup(&self, _: &perp_arb_private_events::lookup::Request) -> Result<Outcome> {
        panic!("fast dispatch must not wait for REST")
    }
}
#[tokio::test]
async fn ws_fast_lane_sends_without_rest_and_deduplicates_rest_and_restart() {
    for venue in ["aster", "hyperliquid"] {
        let files = Files::new();
        let owner = start(&files, venue).await;
        let mut validator = crate::ws_validation::Validator::default();
        validator.observe(&fast_ws_event()).unwrap();
        let epoch = validator.epoch();
        let validator = tokio::sync::Mutex::new(validator);
        let trace = crate::latency::Trace::new(Instant::now());
        let mock = FastVenue(std::sync::atomic::AtomicUsize::new(0));
        let lane = tokio::sync::Mutex::new(());
        let mut seen = BTreeSet::new();
        for _ in 0..2 {
            crate::paired::fast::process(
                &owner,
                &mock,
                &crate::paired::tests::resolved(venue),
                &crate::paired::tests::plan(venue),
                &validator,
                epoch,
                &lane,
                &trace,
                &mut seen,
            )
            .await
            .unwrap();
        }
        assert_eq!(mock.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            decimal(&owner.progress().await.unwrap().maker.cumulative).unwrap(),
            decimal("1").unwrap()
        );
        assert!(!owner.progress().await.unwrap().maker.terminal_confirmed);
        apply(&owner, &[("ws-1", "1")], false).await;
        assert_eq!(
            owner.progress().await.unwrap().maker.confirmed_trade_count,
            1
        );
        owner.shutdown().await.unwrap();
        let owner = reopen(&files, venue).await;
        assert_eq!(
            decimal(&owner.progress().await.unwrap().maker.cumulative).unwrap(),
            decimal("1").unwrap()
        );
        assert!(owner.reserve_confirmed_hedge().await.is_err()); // original send still unknown
        owner.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn ws_fast_lane_rejects_wrong_side_price_quantity_and_disarmed_epoch() {
    for case in 0..4 {
        let files = Files::new();
        let owner = start(&files, "aster").await;
        let mut event = fast_ws_event();
        if let crate::wire::Observation::FillCandidate { candidate: c } = &mut event.observation {
            match case {
                0 => c.side = Some("SELL".into()),
                1 => c.price = Some("101".into()),
                2 => c.quantity = Some("3".into()),
                _ => (),
            }
        }
        let mut validator = crate::ws_validation::Validator::default();
        validator.observe(&event).unwrap();
        let epoch = validator.epoch();
        if case == 3 {
            event.observation = crate::wire::Observation::Gap {
                reason: "fixture disconnect".into(),
            };
            validator.observe(&event).unwrap();
        }
        let mock = FastVenue(std::sync::atomic::AtomicUsize::new(0));
        let result = crate::paired::fast::process(
            &owner,
            &mock,
            &crate::paired::tests::resolved("aster"),
            &crate::paired::tests::plan("aster"),
            &tokio::sync::Mutex::new(validator),
            epoch,
            &tokio::sync::Mutex::new(()),
            &crate::latency::Trace::new(Instant::now()),
            &mut BTreeSet::new(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(mock.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(
            owner.progress().await.unwrap().maker.confirmed_trade_count,
            0
        );
        owner.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn confirmed_obligation_can_hedge_during_maker_gap_but_never_unknown_hedge() {
    let files = Files::new();
    let owner = start(&files, "aster").await;
    apply(&owner, &[("a", "1")], false).await;
    owner.gap(Target::Maker).await.unwrap();
    assert!(owner.reserve_hedge().await.is_err());
    let i = owner.reserve_confirmed_hedge().await.unwrap().unwrap();
    let mock = MockVenue {
        sent: Default::default(),
        unknown: true,
        filled: "0",
    };
    assert!(crate::execution_io::dispatch_hedge(
        &owner,
        &mock,
        i.id.clone(),
        hedge_order("aster"),
        &limits(),
        decimal("100").unwrap()
    )
    .await
    .is_err());
    assert!(owner
        .progress()
        .await
        .unwrap()
        .recovery_pending
        .contains(&Target::Maker));
    assert!(owner.reserve_confirmed_hedge().await.is_err());
    assert_eq!(mock.sent.lock().unwrap().len(), 1);
    owner.shutdown().await.unwrap();
}

struct PairedVenue {
    sent: std::sync::Mutex<Vec<perp_arb_private_events::orders::Order>>,
    unknown: bool,
}
impl perp_arb_order_test::venue::Venue for PairedVenue {
    async fn submit(
        &self,
        o: &perp_arb_private_events::orders::Order,
    ) -> Result<perp_arb_private_events::orders::Accepted> {
        let mut sent = self.sent.lock().unwrap();
        sent.push(o.clone());
        if self.unknown {
            return Err("fixture unknown submit".into());
        }
        Ok(perp_arb_private_events::orders::Accepted {
            order_id: sent.len().to_string(),
            cumulative: Some(if sent.len() == 1 {
                "0.4".into()
            } else {
                o.quantity.clone()
            }),
        })
    }
    async fn cancel(&self, _: &perp_arb_private_events::orders::Order) -> Result<()> {
        panic!("IOC cancel not needed")
    }
    async fn lookup(&self, r: &perp_arb_private_events::lookup::Request) -> Result<Outcome> {
        if self.unknown {
            return Ok(Outcome::NotFound);
        }
        let sent = self.sent.lock().unwrap();
        let (index, o) = sent
            .iter()
            .enumerate()
            .find(|(n, o)| match &r.selector {
                Selector::ClientId(id) => id == &o.client_id,
                Selector::OrderId(id) => id == &(n + 1).to_string(),
            })
            .unwrap();
        let cumulative = if index == 0 {
            decimal("0.4").unwrap()
        } else {
            decimal(&o.quantity).unwrap()
        };
        Ok(Outcome::Found(Found {
            scope: r.scope.clone(),
            side: o.side.clone(),
            client_id: Some(o.client_id.clone()),
            order: Update {
                key: OrderKey {
                    symbol: o.symbol.clone(),
                    order_id: (index + 1).to_string(),
                },
                original: decimal(&o.quantity).unwrap(),
                cumulative,
                status: if index == 0 {
                    Status::Canceled
                } else {
                    Status::Filled
                },
                event_ms: Some(13000),
            },
        }))
    }
}

#[tokio::test]
async fn paired_runner_hedges_partial_fills_residuals_and_stops_without_duplicate_unknown_post() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Message},
    };
    for (venue, unknown) in [("aster", false), ("hyperliquid", false), ("aster", true)] {
        let files = Files::new();
        let owner = start(&files, venue).await;
        let (bridge, _events) = crate::Bridge::bind(crate::Config {
            extension_id: "a".repeat(32),
            private_port: 0,
            reference_port: 0,
            command_port: 0,
            queue_capacity: 128,
        })
        .await
        .unwrap();
        let mut req = format!("ws://{}", bridge.command_address)
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "Origin",
            "chrome-extension://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse()
                .unwrap(),
        );
        let (mut socket, _) = connect_async(req).await.unwrap();
        socket.send(Message::Text(json!({"type":"REGISTER","role":"extension","nativeProtocol":PROTOCOL,"nativeTradingEnabled":true}).to_string().into())).await.unwrap();
        let mut health = bridge.health();
        while !health.borrow().command_connected {
            health.changed().await.unwrap();
        }
        let server = tokio::spawn(async move {
            let mut reads = 0;
            while let Some(Ok(Message::Text(raw))) = socket.next().await {
                let m: serde_json::Value = serde_json::from_str(&raw).unwrap();
                if m["type"] != "NATIVE_ORDER_REQUEST" {
                    continue;
                }
                let now = crate::wire::now_ms().unwrap();
                let receipt = if m["action"] == "cancel" {
                    json!({"protocol":PROTOCOL,"started_ms":now,"finished_ms":now,"account":ACCOUNT,"session_continuity":true,"ok":true,"dispatched":true,"source":"https://omni.variational.io/api/orders/cancel","rfq_id":"rfq-1"})
                } else {
                    assert_eq!(m["action"], "lookup");
                    reads += 1;
                    let full = reads > 1 && !unknown;
                    let q = if full {
                        query(&[("a", "1"), ("b", "1")], true)
                    } else {
                        query(&[("a", "1")], false)
                    };
                    let mut r = serde_json::to_value(q.receipt()).unwrap();
                    r["started_ms"] = json!(now);
                    r["finished_ms"] = json!(now);
                    for key in ["orders", "trades"] {
                        r[key]["observed_ms"] = json!(now);
                        for row in r[key]["rows"].as_array_mut().unwrap() {
                            row["response_server_ms"] = json!(now);
                        }
                    }
                    r
                };
                socket.send(Message::Text(json!({"type":"NATIVE_ORDER_RESULT","requestId":m["requestId"],"ok":true,"receipt":receipt}).to_string().into())).await.unwrap();
            }
        });
        let mut p = crate::paired::tests::plan(venue);
        if unknown {
            p.maker_wait_seconds = 1;
            p.recovery_seconds = 1;
        }
        let r = crate::paired::tests::resolved(venue);
        let mock = PairedVenue {
            sent: Default::default(),
            unknown,
        };
        let (_ft, f) = tokio::sync::watch::channel(None::<String>);
        let (_st, s) = tokio::sync::watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(12),
            crate::paired::runtime::trade_loop(
                &p,
                &r,
                &owner,
                &bridge,
                &mock,
                &|| Ok(true),
                &tokio::sync::Mutex::new(crate::ws_validation::Validator::default()),
                &tokio::sync::Notify::new(),
                &f,
                s,
                false,
                Instant::now(),
                &tokio::sync::Mutex::new(()),
                None,
            ),
        )
        .await
        .unwrap();
        if unknown {
            assert!(result.is_err());
            assert_eq!(mock.sent.lock().unwrap().len(), 1);
            assert_eq!(owner.progress().await.unwrap().in_flight, "1");
        } else {
            assert_eq!(result.unwrap()["completed"], true);
            let sent = mock.sent.lock().unwrap().clone();
            assert!(sent.len() >= 2);
            assert_eq!(
                decimal(&owner.progress().await.unwrap().exposure).unwrap(),
                perp_arb_core::Decimal::ZERO
            );
            assert!(sent.iter().all(|o| o.reduce_only));
        }
        bridge.shutdown().await;
        server.await.unwrap();
        owner.shutdown().await.unwrap();
    }
}
struct Files {
    maker: PathBuf,
    paired: PathBuf,
}
impl Files {
    fn new() -> Self {
        let mut b = [0; 16];
        getrandom::fill(&mut b).unwrap();
        let prefix = format!("paired-owner-{:x}", u128::from_le_bytes(b));
        Self {
            maker: std::env::temp_dir().join(format!("{prefix}-maker.wal")),
            paired: std::env::temp_dir().join(format!("{prefix}-pair.wal")),
        }
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.maker);
        let _ = std::fs::remove_file(&self.paired);
    }
}
fn command() -> browser::Command {
    browser::Command {
        request_id: "request-1".into(),
        account: ACCOUNT.into(),
        symbol: "ETH".into(),
        side: "BUY".into(),
        quantity: "2".into(),
        limit_price: "100".into(),
        max_notional: "200".into(),
        reduce_only: true,
    }
}
fn plan(venue: &str) -> Plan {
    Plan {
        run_id: "owner-test".into(),
        maker: Leg {
            scope: Scope {
                venue: "variational".into(),
                account: ACCOUNT.into(),
            },
            symbol: "ETH".into(),
            side: Side::Buy,
        },
        hedge: Leg {
            scope: Scope {
                venue: venue.into(),
                account: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            },
            symbol: if venue == "aster" { "ETHUSDT" } else { "ETH" }.into(),
            side: Side::Sell,
        },
        maker_order: OrderKey {
            symbol: "ETH".into(),
            order_id: "rfq-1".into(),
        },
        quantity: decimal("2").unwrap(),
        step: decimal("0.1").unwrap(),
        minimum: decimal("0.1").unwrap(),
    }
}
fn query(trades: &[(&str, &str)], terminal: bool) -> Query {
    let instrument = json!({"underlying":"ETH","instrument_type":"perpetual_future","settlement_asset":"USDC","funding_interval_s":3600});
    let rows: Vec<_> = trades.iter().map(|(id, qty)| json!({"id":id,"source_rfq":"rfq-1","instrument":instrument,"side":"buy","qty":qty,"price":"99","status":"confirmed","trade_type":"trade"})).collect();
    let r = json!({"protocol":PROTOCOL,"started_ms":10000,"finished_ms":12000,"account":ACCOUNT,
        "session_continuity":true,"ok":true,"dispatched":false,
        "orders":{"source":"https://omni.variational.io/api/orders/v2","observed_ms":11000,"atomic_snapshot":false,"window_complete":false,
        "rows":[{"rfq_id":"rfq-1","instrument":instrument,"side":"buy","qty":"2","order_type":"limit","limit_price":"100",
            "status":if terminal {"cleared"} else {"pending"},"clearing_status":"success_trades_booked_into_pool",
            "is_reduce_only":true,"is_auto_resize":false,"use_mark_price":false,"response_server_ms":11000}]},
        "trades":{"source":"https://omni.variational.io/api/trades","observed_ms":11500,"atomic_snapshot":false,"window_complete":false,"rows":rows}});
    Query::new(
        Receipt::parse(&r, Some(ACCOUNT), 12000).unwrap(),
        Some("rfq-1".into()),
    )
    .unwrap()
}
async fn start(files: &Files, venue: &str) -> Owner {
    setup_owner(files, venue, false).await.0
}
async fn setup_owner(files: &Files, venue: &str, pending: bool) -> (Owner, MakerRiskCheck) {
    let mut p = plan(venue);
    if pending {
        p.maker_order = DurableEngine::pending_browser_order(&command()).unwrap();
    }
    let mut reports = Vec::new();
    for leg in [&p.maker, &p.hedge] {
        let (c, w) = actor::channel(leg.scope.clone(), 32, 8).unwrap();
        let task = w.spawn();
        c.connected().await.unwrap();
        // Explicit synthetic expected and observed inventories for paper tests.
        let expected = Inventory {
            perp_dexes: BTreeSet::from([String::new()]),
            includes_wallets: true,
            ..Inventory::default()
        };
        let ticket = c.begin_account_query().await.unwrap();
        let now = Instant::now();
        c.apply_account_snapshot(
            ticket,
            Snapshot {
                scope: leg.scope.clone(),
                inventory: expected.clone(),
                started_at: now,
                completed_at: now,
                exchange_time_ms: Some(1000),
            },
            Some(expected),
        )
        .await
        .unwrap();
        reports.push((c.account_report().await.unwrap().unwrap(), c.health()));
        c.shutdown().await.unwrap();
        task.await.unwrap();
    }
    let mp = files.maker.clone();
    let pp = files.paired.clone();
    let check = MakerRiskCheck {
        maker: reports[0].0.clone(),
        maker_health: reports[0].1.clone(),
        hedge: reports[1].0.clone(),
        hedge_health: reports[1].1.clone(),
        price_ok: true,
        max_age: Duration::from_secs(30),
    };
    let owner = Owner::start(16, move || {
        let mut maker = browser::Ledger::create(&mp, command()).map_err(err)?;
        if !pending {
            drop(maker.dispatch().map_err(err)?);
            let c = command();
            maker
                .bind_verified(browser::Identity {
                    request_id: c.request_id,
                    account: c.account,
                    symbol: c.symbol,
                    side: c.side,
                    quantity: c.quantity,
                    reduce_only: c.reduce_only,
                    order_id: "rfq-1".into(),
                })
                .map_err(err)?;
        }
        let paired = DurableEngine::create(
            &pp,
            p,
            (&reports[0].0, &reports[0].1),
            (&reports[1].0, &reports[1].1),
            true,
            Duration::from_secs(5),
        )
        .map_err(err)?;
        Ok((maker, paired))
    })
    .await
    .unwrap();
    (owner, check)
}
async fn reopen(files: &Files, venue: &str) -> Owner {
    let mp = files.maker.clone();
    let pp = files.paired.clone();
    let p = plan(venue);
    Owner::start(16, move || {
        Ok((
            browser::Ledger::open(&mp).map_err(err)?,
            DurableEngine::open(&pp, &p).map_err(err)?,
        ))
    })
    .await
    .unwrap()
}
async fn apply(owner: &Owner, trades: &[(&str, &str)], terminal: bool) -> Progress {
    let ticket = owner.begin_lookup().await.unwrap();
    owner
        .apply_lookup(ticket, query(trades, terminal))
        .await
        .unwrap()
}
#[tokio::test]
async fn both_native_venues_reserve_only_new_fills_during_cancel_and_restart() {
    for venue in ["aster", "hyperliquid"] {
        let files = Files::new();
        let owner = start(&files, venue).await;
        let p = apply(&owner, &[("a", "1")], false).await;
        assert_eq!(p.exposure, "1");
        assert!(p.maker.command.reduce_only);
        let first = owner.reserve_hedge().await.unwrap().unwrap();
        let permit = owner.prepare_hedge(first.id.clone()).await.unwrap();
        assert_eq!(permit.scope().venue, venue);
        assert_eq!(permit.intent().side, Side::Sell);
        assert!(owner.prepare_hedge(first.id.clone()).await.is_err());
        let cancel = owner.prepare_cancel().await.unwrap();
        assert_eq!(cancel.order_id(), "rfq-1");
        apply(&owner, &[("a", "1"), ("b", "0.5")], false).await;
        let second = owner.reserve_hedge().await.unwrap().unwrap();
        assert_eq!(second.quantity, decimal("0.5").unwrap());
        apply(&owner, &[("b", "0.5")], false).await;
        assert!(owner.reserve_hedge().await.unwrap().is_none());
        owner.shutdown().await.unwrap();
        let owner = reopen(&files, venue).await;
        assert!(owner.reserve_hedge().await.is_err());
        let p = apply(&owner, &[("a", "1"), ("b", "0.5"), ("c", "0.5")], true).await;
        assert_eq!(decimal(&p.maker.cumulative).unwrap(), decimal("2").unwrap());
        assert_eq!(p.maker.confirmed_trade_count, 3);
        assert_eq!(p.in_flight, "1.5");
        assert!(!p.recovery_pending.contains(&Target::Maker));
        assert!(p
            .recovery_pending
            .contains(&Target::Hedge(first.id.clone())));
        assert!(owner.reserve_hedge().await.is_err()); // unknown first hedge still reserved
        let ticket = owner.begin_hedge_lookup(first.id.clone()).await.unwrap();
        owner
            .apply_hedge_lookup(ticket, Outcome::NotFound)
            .await
            .unwrap();
        assert!(owner.reserve_hedge().await.is_err());
        let ticket = owner.begin_hedge_lookup(first.id.clone()).await.unwrap();
        let req = ticket.request().clone();
        let Selector::ClientId(client_id) = req.selector else {
            panic!()
        };
        let p = owner
            .apply_hedge_lookup(
                ticket,
                Outcome::Found(Found {
                    scope: req.scope,
                    side: "SELL".into(),
                    client_id: Some(client_id),
                    order: Update {
                        key: OrderKey {
                            symbol: req.symbol,
                            order_id: "100".into(),
                        },
                        original: first.quantity,
                        cumulative: first.quantity,
                        status: Status::Filled,
                        event_ms: Some(12000),
                    },
                }),
            )
            .await
            .unwrap();
        assert_eq!(decimal(&p.exposure).unwrap(), decimal("1").unwrap());
        assert_eq!(p.in_flight, "0.5");
        assert_eq!(
            owner.reserve_hedge().await.unwrap().unwrap().quantity,
            decimal("0.5").unwrap()
        );
        owner.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn intervening_gap_preserves_fill_but_cannot_clear_recovery() {
    let files = Files::new();
    let owner = start(&files, "aster").await;
    let ticket = owner.begin_lookup().await.unwrap();
    owner.gap(Target::Maker).await.unwrap();
    let p = owner
        .apply_lookup(ticket, query(&[("a", "2")], true))
        .await
        .unwrap();
    assert_eq!(p.exposure, "2");
    assert!(!p.maker.terminal_confirmed);
    assert!(owner.reserve_hedge().await.is_err());
    apply(&owner, &[("a", "2")], true).await;
    assert_eq!(
        owner.reserve_hedge().await.unwrap().unwrap().quantity,
        decimal("2").unwrap()
    );
    owner.shutdown().await.unwrap();
}
#[tokio::test]
async fn conflicting_trade_quantity_fences_dispatch_and_retains_exposure() {
    let files = Files::new();
    let owner = start(&files, "hyperliquid").await;
    apply(&owner, &[("a", "1")], false).await;
    let ticket = owner.begin_lookup().await.unwrap();
    // Same ID, a larger quantity: never interpreted as a second incremental fill.
    assert!(owner
        .apply_lookup(ticket, query(&[("a", "2")], true))
        .await
        .is_err());
    let p = owner.progress().await.unwrap();
    assert!(p.faulted);
    assert_eq!(p.exposure, "1");
    assert!(owner.reserve_hedge().await.is_err());
    owner.shutdown().await.unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn slow_factory_does_not_block_async_reader_and_failure_is_reported() {
    let async_thread = std::thread::current().id();
    let (started, ready) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let launch = tokio::spawn(Owner::start(1, move || {
        assert_ne!(std::thread::current().id(), async_thread);
        started.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(2)).unwrap();
        Err("fixture initialization failure".into())
    }));
    tokio::time::timeout(Duration::from_secs(1), ready)
        .await
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await; // async reader runs while blocking owner waits
    release.send(()).unwrap();
    assert!(launch.await.unwrap().is_err());
    assert!(Owner::start(0, || panic!("must not initialize"))
        .await
        .is_err());
}
#[tokio::test]
async fn overflow_fences_both_wals_before_following_dispatch() {
    let files = Files::new();
    let owner = start(&files, "aster").await;
    apply(&owner, &[("a", "1")], false).await;
    let intent = owner.reserve_hedge().await.unwrap().unwrap();
    let (ready, started) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    assert!(owner.tx.try_send(Work::Barrier(ready, wait)).is_ok());
    started.await.unwrap();
    let mut replies = Vec::new();
    for _ in 0..16 {
        let (reply, receive) = oneshot::channel();
        assert!(owner.tx.try_send(Work::Progress(reply)).is_ok());
        replies.push(receive);
    }
    assert!(owner.progress().await.err().unwrap().contains("queue full"));
    release.send(()).unwrap();
    for reply in replies {
        assert!(reply.await.unwrap().is_ok());
    }
    assert!(owner.prepare_hedge(intent.id).await.is_err());
    assert!(owner
        .progress()
        .await
        .unwrap()
        .recovery_pending
        .contains(&Target::Maker));
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_waiter_does_not_discard_accepted_fill_evidence() {
    let files = Files::new();
    let owner = start(&files, "aster").await;
    let ticket = owner.begin_lookup().await.unwrap();
    let (reply, receive) = oneshot::channel();
    assert!(owner
        .tx
        .try_send(Work::Apply(
            ticket,
            Box::new(query(&[("a", "1")], false)),
            reply
        ))
        .is_ok());
    drop(receive);
    assert_eq!(owner.progress().await.unwrap().maker.cumulative, "1");
    owner.shutdown().await.unwrap();
    let owner = reopen(&files, "aster").await;
    assert_eq!(
        owner.progress().await.unwrap().maker.confirmed_trade_count,
        1
    );
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn disjoint_pages_can_prove_quantity_but_not_terminal_by_union() {
    let files = Files::new();
    let owner = start(&files, "hyperliquid").await;
    apply(&owner, &[("a", "1")], false).await;
    let p = apply(&owner, &[("b", "1")], true).await;
    assert_eq!(p.maker.cumulative, "2");
    assert!(!p.maker.terminal_confirmed);
    assert!(p.maker.reconciliation_required);
    assert_eq!(
        owner.reserve_hedge().await.unwrap().unwrap().quantity,
        decimal("2").unwrap()
    );
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn known_full_fill_without_date_is_hedgeable_but_not_reconciled() {
    let files = Files::new();
    let owner = start(&files, "aster").await;
    let ticket = owner.begin_lookup().await.unwrap();
    let original = query(&[("a", "2")], true);
    let mut r = original.receipt().clone();
    r.orders.as_mut().unwrap().rows[0]["response_server_ms"] = serde_json::Value::Null;
    let q = Query::new(r, Some("rfq-1".into())).unwrap();
    let p = owner.apply_lookup(ticket, q).await.unwrap();
    assert_eq!(p.exposure, "2");
    assert!(!p.maker.terminal_confirmed);
    assert_eq!(
        owner.reserve_hedge().await.unwrap().unwrap().quantity,
        decimal("2").unwrap()
    );
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn mismatched_wal_pair_cannot_start_and_releases_file_locks() {
    let a = Files::new();
    let b = Files::new();
    start(&a, "aster").await.shutdown().await.unwrap();
    let bp = b.maker.clone();
    tokio::task::spawn_blocking(move || {
        let mut maker = browser::Ledger::create(&bp, command()).unwrap();
        maker.dispatch().unwrap();
        let c = command();
        maker
            .bind_verified(browser::Identity {
                request_id: c.request_id,
                account: c.account,
                symbol: c.symbol,
                side: c.side,
                quantity: c.quantity,
                reduce_only: c.reduce_only,
                order_id: "different-rfq".into(),
            })
            .unwrap();
    })
    .await
    .unwrap();
    let bp = b.maker.clone();
    let ap = a.paired.clone();
    assert!(Owner::start(16, move || Ok((
        browser::Ledger::open(&bp).map_err(err)?,
        DurableEngine::open(&ap, &plan("aster")).map_err(err)?
    )))
    .await
    .is_err());
    // Initialization failure sends its result before returning the worker, so
    // start must join it before reporting failure to make lock release reliable.
    reopen(&a, "aster").await.shutdown().await.unwrap();
}

#[tokio::test]
async fn crash_between_maker_and_paired_writes_restores_known_exposure() {
    let files = Files::new();
    start(&files, "aster").await.shutdown().await.unwrap();
    let mp = files.maker.clone();
    tokio::task::spawn_blocking(move || {
        let mut maker = browser::Ledger::open(&mp).unwrap();
        let ticket = maker.lookup_ticket().unwrap();
        query(&[("a", "1")], false)
            .record(&mut maker, ticket)
            .unwrap();
        // Simulate termination before DurableEngine sees the committed trade.
    })
    .await
    .unwrap();
    let owner = reopen(&files, "aster").await;
    let p = owner.progress().await.unwrap();
    assert_eq!(p.exposure, "1");
    assert_eq!(p.maker.confirmed_trade_count, 1);
    assert!(owner.reserve_hedge().await.is_err());
    owner.shutdown().await.unwrap();
}

fn ws_event(qty: &str, price: &str) -> crate::wire::Event {
    use crate::wire::*;
    Event {
        channel: Channel::Private,
        generation: 1,
        received_ms: 12000,
        captured_ms: Some(12000),
        received_elapsed_us: 0,
        parsed_elapsed_us: 0,
        observation: Observation::FillCandidate {
            candidate: Box::new(FillCandidate {
                symbol: Some("ETH".into()),
                side: Some("BUY".into()),
                order_id: Some("rfq-1".into()),
                trade_id: Some("a".into()),
                quantity: Some(qty.into()),
                quote_amount: None,
                price: Some(price.into()),
                status: Some("confirmed".into()),
                trade_type: Some("trade".into()),
                instrument: Some(crate::native::Instrument {
                    underlying: "ETH".into(),
                    instrument_type: "perpetual_future".into(),
                    settlement_asset: "USDC".into(),
                    funding_interval_s: 3600,
                }),
            }),
        },
    }
}
#[test]
fn ws_trade_must_match_exact_rest_identity_quantity_and_price() {
    let mut v = crate::ws_validation::Validator::default();
    assert_eq!(
        v.observe(&ws_event("1", "99")).unwrap(),
        Some("rfq-1".into())
    );
    assert_eq!(v.verified_count(), 0);
    assert_eq!(
        v.verify(v.epoch(), &query(&[("a", "1")], false), &command(), "rfq-1")
            .unwrap(),
        1
    );
    assert_eq!(v.verified_count(), 1);
    assert!(v.observe(&ws_event("1", "99")).unwrap().is_none());
    for (qty, price) in [("2", "99"), ("1", "98")] {
        let mut v = crate::ws_validation::Validator::default();
        v.observe(&ws_event(qty, price)).unwrap();
        assert!(v
            .verify(v.epoch(), &query(&[("a", "1")], false), &command(), "rfq-1")
            .is_err());
    }
}

#[tokio::test]
async fn ws_mismatch_or_gap_preserves_rest_confirmed_exposure() {
    for kind in 0..3 {
        let files = Files::new();
        let owner = start(&files, "aster").await;
        let validator = tokio::sync::Mutex::new(crate::ws_validation::Validator::default());
        validator
            .lock()
            .await
            .observe(&ws_event("1", if kind == 1 { "98" } else { "99" }))
            .unwrap();
        let epoch = validator.lock().await.epoch();
        let ticket = owner.begin_lookup().await.unwrap();
        if kind == 2 {
            let mut gap = ws_event("1", "99");
            gap.observation = crate::wire::Observation::Gap {
                reason: "fixture gap during lookup".into(),
            };
            validator.lock().await.observe(&gap).unwrap();
        }
        let result = crate::execution_io::apply_maker_with_ws(
            &owner,
            ticket,
            query(&[("a", "1")], false),
            &validator,
            epoch,
            &command(),
            "rfq-1",
        )
        .await;
        if kind == 0 {
            assert_eq!(result.unwrap().1, 1);
        } else {
            assert!(result.err().unwrap().contains("REST fills retained"));
            assert!(owner
                .progress()
                .await
                .unwrap()
                .recovery_pending
                .contains(&Target::Maker));
        }
        assert_eq!(owner.progress().await.unwrap().exposure, "1");
        owner.shutdown().await.unwrap();
    }
}
#[test]
fn ws_gap_same_socket_and_unconfirmed_trade_cannot_pass_verification() {
    use crate::wire::Observation;
    let mut v = crate::ws_validation::Validator::default();
    let e = ws_event("1", "99");
    v.observe(&e).unwrap();
    let epoch = v.epoch();
    let mut gap = e.clone();
    gap.observation = Observation::Gap {
        reason: "fixture".into(),
    };
    v.observe(&gap).unwrap();
    assert!(v
        .verify(epoch, &query(&[("a", "1")], false), &command(), "rfq-1")
        .is_err());
    let mut pending = e;
    if let Observation::FillCandidate { candidate } = &mut pending.observation {
        candidate.status = Some("pending".into());
    }
    assert!(v.observe(&pending).unwrap().is_none());
    assert_eq!(v.pending_count(), 0);
}

fn hedge_order(venue: &str) -> perp_arb_private_events::orders::Order {
    use perp_arb_private_events::orders::*;
    let p = plan(venue);
    Order {
        venue: venue.into(),
        network: Network::Mainnet,
        account: p.hedge.scope.account,
        symbol: p.hedge.symbol,
        side: "SELL".into(),
        quantity: "1".into(),
        limit_price: "99".into(),
        tif: TimeInForce::Ioc,
        reduce_only: true,
        client_id: "assigned-by-wal".into(),
        asset: if venue == "hyperliquid" {
            Some(1)
        } else {
            None
        },
    }
}
struct MockVenue {
    sent: std::sync::Mutex<Vec<perp_arb_private_events::orders::Order>>,
    unknown: bool,
    filled: &'static str,
}
impl perp_arb_order_test::venue::Venue for MockVenue {
    async fn submit(
        &self,
        o: &perp_arb_private_events::orders::Order,
    ) -> Result<perp_arb_private_events::orders::Accepted> {
        self.sent.lock().unwrap().push(o.clone());
        if self.unknown {
            return Err("fixture lost ACK".into());
        }
        Ok(perp_arb_private_events::orders::Accepted {
            order_id: "100".into(),
            cumulative: Some(self.filled.into()),
        })
    }
    async fn cancel(&self, _: &perp_arb_private_events::orders::Order) -> Result<()> {
        panic!("no hedge cancel expected")
    }
    async fn lookup(&self, request: &perp_arb_private_events::lookup::Request) -> Result<Outcome> {
        if self.unknown {
            return Ok(Outcome::NotFound);
        }
        let order = self.sent.lock().unwrap()[0].clone();
        let found = Found {
            scope: Scope {
                venue: order.venue,
                account: order.account,
            },
            side: order.side,
            client_id: Some(order.client_id),
            order: Update {
                key: OrderKey {
                    symbol: order.symbol,
                    order_id: "100".into(),
                },
                original: decimal(&order.quantity).unwrap(),
                cumulative: decimal(self.filled).unwrap(),
                status: if self.filled == "1" {
                    Status::Filled
                } else {
                    Status::Canceled
                },
                event_ms: Some(13000),
            },
        };
        request.verify(&found).unwrap();
        Ok(Outcome::Found(found))
    }
}
fn limits() -> perp_arb_core::instruments::Instrument {
    use perp_arb_core::instruments::*;
    Instrument::new(
        PriceRule::Tick(decimal("0.1").unwrap()),
        decimal("0.1").unwrap(),
        decimal("0.1").unwrap(),
        None,
        None,
    )
    .unwrap()
}
#[tokio::test]
async fn native_wire_dispatch_both_venues_preserves_close_intent_and_terminal_residual() {
    for venue in ["aster", "hyperliquid"] {
        let files = Files::new();
        let owner = start(&files, venue).await;
        apply(&owner, &[("a", "1")], false).await;
        let i = owner.reserve_hedge().await.unwrap().unwrap();
        let mock = MockVenue {
            sent: Default::default(),
            unknown: false,
            filled: "0.4",
        };
        let p = crate::execution_io::dispatch_hedge(
            &owner,
            &mock,
            i.id.clone(),
            hedge_order(venue),
            &limits(),
            decimal("100").unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(p.exposure, "0.6");
        assert_eq!(p.in_flight, "0");
        assert_eq!(
            owner.reserve_hedge().await.unwrap().unwrap().quantity,
            decimal("0.6").unwrap()
        );
        let sent = mock.sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].reduce_only);
        assert_ne!(sent[0].client_id, "assigned-by-wal");
        assert!(owner
            .prepare_wire_hedge(i.id, hedge_order(venue), decimal("100").unwrap())
            .await
            .is_err());
        owner.shutdown().await.unwrap();
        let owner = reopen(&files, venue).await;
        assert_eq!(owner.progress().await.unwrap().exposure, "0.6");
        owner.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn lost_native_ack_and_not_found_never_resend() {
    let files = Files::new();
    let owner = start(&files, "aster").await;
    apply(&owner, &[("a", "1")], false).await;
    let i = owner.reserve_hedge().await.unwrap().unwrap();
    let mock = MockVenue {
        sent: Default::default(),
        unknown: true,
        filled: "0",
    };
    assert!(crate::execution_io::dispatch_hedge(
        &owner,
        &mock,
        i.id.clone(),
        hedge_order("aster"),
        &limits(),
        decimal("100").unwrap()
    )
    .await
    .is_err());
    crate::execution_io::reconcile_hedge(&owner, &mock, i.id.clone())
        .await
        .unwrap();
    assert!(owner.reserve_hedge().await.is_err());
    assert!(crate::execution_io::dispatch_hedge(
        &owner,
        &mock,
        i.id,
        hedge_order("aster"),
        &limits(),
        decimal("100").unwrap()
    )
    .await
    .is_err());
    assert_eq!(mock.sent.lock().unwrap().len(), 1);
    assert_eq!(owner.progress().await.unwrap().in_flight, "1");
    owner.shutdown().await.unwrap();
}
#[tokio::test]
async fn wrong_reduce_only_cap_or_precision_cannot_send_hedge() {
    let files = Files::new();
    let owner = start(&files, "aster").await;
    apply(&owner, &[("a", "1")], false).await;
    let i = owner.reserve_hedge().await.unwrap().unwrap();
    let mock = MockVenue {
        sent: Default::default(),
        unknown: false,
        filled: "1",
    };
    for kind in 0..3 {
        let mut order = hedge_order("aster");
        if kind == 0 {
            order.reduce_only = false;
        }
        if kind == 1 {
            order.limit_price = "99.01".into();
        }
        let cap = decimal(if kind == 2 { "1" } else { "100" }).unwrap();
        assert!(crate::execution_io::dispatch_hedge(
            &owner,
            &mock,
            i.id.clone(),
            order,
            &limits(),
            cap
        )
        .await
        .is_err());
    }
    assert!(mock.sent.lock().unwrap().is_empty());
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_maker_is_armed_before_rfq_ack_and_recovered_across_restart() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Message},
    };
    let files = Files::new();
    let (owner, check) = setup_owner(&files, "aster", true).await;
    let cfg = crate::Config {
        extension_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        private_port: 0,
        reference_port: 0,
        command_port: 0,
        queue_capacity: 128,
    };
    let (bridge, _events) = crate::Bridge::bind(cfg).await.unwrap();
    let mut sockets = Vec::new();
    for address in [
        bridge.private_address,
        bridge.reference_address,
        bridge.command_address,
    ] {
        let mut req = format!("ws://{address}").into_client_request().unwrap();
        req.headers_mut().insert(
            "Origin",
            "chrome-extension://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse()
                .unwrap(),
        );
        sockets.push(connect_async(req).await.unwrap().0);
    }
    let mut command_socket = sockets.pop().unwrap();
    command_socket.send(Message::Text(json!({"type":"REGISTER","role":"extension","nativeProtocol":PROTOCOL,"nativeTradingEnabled":true}).to_string().into())).await.unwrap();
    let mut health = bridge.health();
    while !health.borrow().native_trading_enabled {
        health.changed().await.unwrap();
    }
    let server = tokio::spawn(async move {
        while let Some(Ok(Message::Text(raw))) = command_socket.next().await {
            let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if v["type"] != "NATIVE_ORDER_REQUEST" {
                continue;
            }
            assert_eq!(v["action"], "submit");
            assert_eq!(v["order"]["is_reduce_only"], true);
            let now = crate::wire::now_ms().unwrap();
            let r = json!({"type":"NATIVE_ORDER_RESULT","requestId":v["requestId"],"ok":true,"receipt":{
                "protocol":PROTOCOL,"started_ms":now,"finished_ms":now,"account":ACCOUNT,"session_continuity":true,"ok":true,"dispatched":true,
                "source":"https://omni.variational.io/api/orders/new/limit","rfq_id":"rfq-1"}});
            command_socket
                .send(Message::Text(r.to_string().into()))
                .await
                .unwrap();
            break;
        }
    });
    let instrument = crate::native::Instrument {
        underlying: "ETH".into(),
        instrument_type: "perpetual_future".into(),
        settlement_asset: "USDC".into(),
        funding_interval_s: 3600,
    };
    let progress = crate::execution_io::submit_maker(&owner, &bridge, check, instrument)
        .await
        .unwrap();
    assert!(progress.maker.dispatched);
    assert_eq!(progress.maker.order_id.as_deref(), Some("rfq-1"));
    assert_eq!(progress.exposure, "0"); // ACK is not a fill
    apply(&owner, &[("a", "1")], false).await;
    assert_eq!(
        owner.reserve_hedge().await.unwrap().unwrap().quantity,
        decimal("1").unwrap()
    );
    server.await.unwrap();
    bridge.shutdown().await;
    owner.shutdown().await.unwrap();
    drop(sockets);
    let mp = files.maker.clone();
    let pp = files.paired.clone();
    let mut p = plan("aster");
    p.maker_order = DurableEngine::pending_browser_order(&command()).unwrap();
    let owner = Owner::start(16, move || {
        Ok((
            browser::Ledger::open(&mp).map_err(err)?,
            DurableEngine::open(&pp, &p).map_err(err)?,
        ))
    })
    .await
    .unwrap();
    assert_eq!(
        owner.progress().await.unwrap().maker.order_id.as_deref(),
        Some("rfq-1")
    );
    assert_eq!(owner.progress().await.unwrap().in_flight, "1");
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn price_or_account_gate_failure_does_not_dispatch_pending_maker() {
    let files = Files::new();
    let (owner, mut check) = setup_owner(&files, "aster", true).await;
    check.price_ok = false;
    assert!(owner.prepare_maker(check).await.is_err());
    assert!(!owner.progress().await.unwrap().maker.dispatched);
    owner.shutdown().await.unwrap();
}
