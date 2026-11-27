use super::runtime::{Books, Engine};
use super::*;
use perp_arb_core::market::{Depth, Level};
use perp_arb_order_test::venue::Venue;
use perp_arb_private_events::{
    actor::{self, Coordinator},
    lookup::{Found, Outcome, Request, Selector},
    orders::Accepted,
    Evidence, OrderKey, Scope, Session, Status, Update,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{watch, Notify};
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        let p = std::env::temp_dir()
            .join(perp_arb_execution::durable::single::new_client_id("aster").unwrap());
        std::fs::create_dir(&p).unwrap();
        std::fs::write(p.join("orders.jsonl"), "").unwrap();
        Self(p)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn fixture(maker: &str, close: bool, side: &str) -> (Plan, Resolved) {
    let hedge = if maker == "aster" {
        "hyperliquid"
    } else {
        "aster"
    };
    fixture_pair(maker, hedge, close, side)
}
fn fixture_pair(maker: &str, hedge: &str, close: bool, side: &str) -> (Plan, Resolved) {
    let leg = |v: &str, s: &str, tif: TimeInForce| perp_arb_order_test::Plan {
        venue: v.into(),
        network: Network::Mainnet,
        account: if v == "lighter" {
            "100".into()
        } else {
            format!("0x{}", "a".repeat(40))
        },
        symbol: if v == "aster" { "ETHUSDT" } else { "ETH" }.into(),
        side: s.into(),
        quantity: "1".into(),
        limit_price: "100".into(),
        tif,
        reduce_only: close,
        max_notional_usd: "200".into(),
        wait_seconds: 20,
    };
    let p = Plan {
        schema: "native_perp_pair_v1".into(),
        run_id: "test-native".into(),
        symbol: "ETH".into(),
        maker: leg(maker, side, TimeInForce::PostOnly),
        hedge: leg(
            hedge,
            if side == "BUY" { "SELL" } else { "BUY" },
            if hedge == "aster" {
                TimeInForce::Market
            } else {
                TimeInForce::Ioc
            },
        ),
        clip_quantity: "1".into(),
        max_gap: "0.0025".into(),
        hedge_price_mode: if hedge == "aster" {
            "market"
        } else {
            "dynamic_ioc"
        }
        .into(),
        hedge_slippage_bps: "50".into(),
        maker_wait_seconds: 1,
        recovery_seconds: 10,
        max_reprices: 20,
        leverage: 1,
    };
    let order = |p: &perp_arb_order_test::Plan| Order {
        venue: p.venue.clone(),
        network: p.network,
        account: p.account.clone(),
        symbol: p.symbol.clone(),
        side: p.side.clone(),
        quantity: p.quantity.clone(),
        limit_price: p.limit_price.clone(),
        tif: p.tif,
        reduce_only: p.reduce_only,
        client_id: perp_arb_execution::durable::single::new_client_id(&p.venue).unwrap(),
        asset: if matches!(p.venue.as_str(), "hyperliquid" | "lighter") {
            Some(0)
        } else {
            None
        },
    };
    let rules = Rules::from(
        &Instrument::new(
            PriceRule::Tick(d("0.01").unwrap()),
            d("0.1").unwrap(),
            d("0.1").unwrap(),
            None,
            None,
        )
        .unwrap(),
    );
    let r = Resolved {
        maker: order(&p.maker),
        hedge: order(&p.hedge),
        maker_rules: rules.clone(),
        hedge_rules: rules,
        step: "0.1".into(),
    };
    (p, r)
}
fn quote(bid: &str, ask: &str) -> Quote {
    Quote::new(
        d(bid).unwrap(),
        d(ask).unwrap(),
        perp_arb_market_data::hub::wall_ms(),
        None,
        Depth::Visible(vec![Level {
            price: d(bid).unwrap(),
            size: D::from(100),
        }]),
        Depth::Visible(vec![Level {
            price: d(ask).unwrap(),
            size: D::from(100),
        }]),
    )
    .unwrap()
}
#[derive(Clone)]
struct Record {
    order: Order,
    id: String,
    filled: D,
    status: Status,
}
struct Mock {
    c: Coordinator,
    session: Session,
    maker: bool,
    late_cancel: bool,
    unknown: bool,
    lost_ack: bool,
    records: Arc<Mutex<BTreeMap<String, Record>>>,
    posts: AtomicUsize,
    cancels: AtomicUsize,
    first_post: Notify,
    first_hedge: Mutex<Option<Instant>>,
    rest_gate: Mutex<Option<Arc<Notify>>>,
}
impl Venue for Mock {
    async fn submit(&self, o: &Order) -> Result<Accepted> {
        let n = self.posts.fetch_add(1, Ordering::SeqCst) + 1;
        let rec = Record {
            order: o.clone(),
            id: n.to_string(),
            filled: if self.maker {
                D::ZERO
            } else if self.late_cancel && n == 1 {
                o.quantity().unwrap() / D::TWO
            } else {
                o.quantity().unwrap()
            },
            status: if self.maker {
                Status::New
            } else if self.late_cancel && n == 1 {
                Status::Canceled
            } else {
                Status::Filled
            },
        };
        self.records
            .lock()
            .unwrap()
            .insert(o.client_id.clone(), rec.clone());
        self.first_post.notify_one();
        if !self.maker {
            self.first_hedge
                .lock()
                .unwrap()
                .get_or_insert(Instant::now());
            if let Some(gate) = self.rest_gate.lock().unwrap().as_ref() {
                gate.notify_one();
            }
        }
        if self.maker && !self.late_cancel && !self.unknown {
            let records = self.records.clone();
            let c = self.c.clone();
            let session = self.session.clone();
            let key = o.client_id.clone();
            tokio::spawn(async move {
                for (part, status) in [
                    (d("0.4").unwrap(), Status::PartiallyFilled),
                    (D::ONE, Status::Filled),
                ] {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    let r = {
                        let mut map = records.lock().unwrap();
                        let r = map.get_mut(&key).unwrap();
                        r.filled = r.order.quantity().unwrap() * part;
                        r.status = status;
                        r.clone()
                    };
                    let update = Update {
                        key: OrderKey {
                            symbol: r.order.symbol.clone(),
                            order_id: r.id,
                        },
                        original: r.order.quantity().unwrap(),
                        cumulative: r.filled,
                        status,
                        event_ms: Some(perp_arb_market_data::hub::wall_ms() as u64),
                    };
                    let _ = c
                        .observe(
                            update.clone(),
                            Evidence::Ws(session.clone()),
                            Instant::now(),
                        )
                        .await;
                    let _ = c
                        .observe(update, Evidence::Ws(session.clone()), Instant::now())
                        .await;
                }
            });
        }
        if self.unknown || self.lost_ack {
            return Err("lost ACK".into());
        }
        Ok(Accepted {
            order_id: rec.id,
            cumulative: if self.maker {
                None
            } else {
                Some(rec.filled.to_string())
            },
        })
    }
    async fn lookup(&self, r: &Request) -> Result<Outcome> {
        if self.unknown {
            return Ok(Outcome::NotFound);
        }
        // Slow maker REST cannot hold up the WS hedge lane.
        if self.maker {
            let gate = self.rest_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let Selector::ClientId(id) = &r.selector else {
            panic!()
        };
        let Some(x) = self.records.lock().unwrap().get(id).cloned() else {
            return Ok(Outcome::NotFound);
        };
        Ok(Outcome::Found(Found {
            scope: r.scope.clone(),
            side: r.side.clone(),
            client_id: Some(id.clone()),
            order: Update {
                key: OrderKey {
                    symbol: r.symbol.clone(),
                    order_id: x.id,
                },
                original: r.original,
                cumulative: x.filled,
                status: x.status,
                event_ms: Some(perp_arb_market_data::hub::wall_ms() as u64),
            },
        }))
    }
    async fn cancel(&self, o: &Order) -> Result<()> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        if let Some(r) = self.records.lock().unwrap().get_mut(&o.client_id) {
            if r.status != Status::Filled {
                if self.late_cancel {
                    r.filled = r.order.quantity().unwrap() * d("0.4").unwrap();
                }
                r.status = Status::Canceled;
            }
        }
        Ok(())
    }
}
async fn mock(
    o: &Order,
    maker: bool,
    late: bool,
    unknown: bool,
    lost_ack: bool,
) -> (Arc<Mock>, tokio::task::JoinHandle<()>) {
    let (c, w) = actor::channel(
        Scope {
            venue: o.venue.clone(),
            account: o.account.clone(),
        },
        128,
        512,
    )
    .unwrap();
    let w = w.spawn();
    let session = c.connected().await.unwrap();
    (
        Arc::new(Mock {
            c,
            session,
            maker,
            late_cancel: late,
            unknown,
            lost_ack,
            records: Default::default(),
            posts: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
            first_post: Notify::new(),
            first_hedge: Mutex::new(None),
            rest_gate: Mutex::new(None),
        }),
        w,
    )
}
#[tokio::test]
async fn both_roles_open_close_partial_ws_hedges_before_rest_and_no_duplicate_on_restart() {
    for (maker, hedge, close, side) in [
        ("aster", "hyperliquid", false, "SELL"),
        ("hyperliquid", "aster", false, "BUY"),
        ("aster", "hyperliquid", true, "BUY"),
        ("hyperliquid", "aster", true, "SELL"),
        ("lighter", "aster", false, "SELL"),
        ("aster", "lighter", false, "BUY"),
        ("lighter", "hyperliquid", false, "BUY"),
        ("hyperliquid", "lighter", false, "SELL"),
        ("lighter", "aster", true, "BUY"),
        ("aster", "lighter", true, "SELL"),
        ("lighter", "hyperliquid", true, "SELL"),
        ("hyperliquid", "lighter", true, "BUY"),
    ] {
        let files = Files::new();
        let (p, r) = fixture_pair(maker, hedge, close, side);
        p.validate().unwrap();
        let (m, mw) = mock(&r.maker, true, false, false, false).await;
        let (h, hw) = mock(&r.hedge, false, false, false, false).await;
        let gate = Arc::new(Notify::new());
        *m.rest_gate.lock().unwrap() = Some(gate.clone());
        *h.rest_gate.lock().unwrap() = Some(gate);
        let (bt, br) = watch::channel(Books::default());
        let pump = tokio::spawn(async move {
            loop {
                bt.send_replace(Books {
                    maker: Some(quote("100", "100.01")),
                    hedge: Some(quote("100", "100.01")),
                });
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        let (_stop, sr) = watch::channel(false);
        let result = Engine::new(
            p.clone(),
            r.clone(),
            [m.clone(), h.clone()],
            [m.c.clone(), h.c.clone()],
            br.clone(),
            sr.clone(),
            files.0.clone(),
            false,
        )
        .run()
        .await
        .unwrap();
        assert_eq!(result["target_completed"], true);
        assert_eq!(m.posts.load(Ordering::SeqCst), 1);
        // Maker REST could not return until a hedge POST released the gate.
        assert!(h.first_hedge.lock().unwrap().is_some());
        let sent = h
            .records
            .lock()
            .unwrap()
            .values()
            .map(|r| r.order.quantity().unwrap())
            .sum::<D>();
        assert_eq!(sent, D::ONE);
        for rec in h.records.lock().unwrap().values() {
            assert_eq!(rec.order.reduce_only, close);
            assert_ne!(rec.order.side, side);
        }
        let posts = h.posts.load(Ordering::SeqCst);
        // Recovery is idempotent even if the previous completion marker already exists.
        let restored = Engine::new(
            p,
            r,
            [m.clone(), h.clone()],
            [m.c.clone(), h.c.clone()],
            br,
            sr,
            files.0.clone(),
            true,
        )
        .run()
        .await
        .unwrap();
        assert_eq!(restored["settled"], true);
        assert_eq!(h.posts.load(Ordering::SeqCst), posts);
        assert_eq!(m.posts.load(Ordering::SeqCst), 1);
        pump.abort();
        let _ = pump.await;
        m.c.shutdown().await.unwrap();
        h.c.shutdown().await.unwrap();
        let _ = mw.await;
        let _ = hw.await;
    }
}
#[tokio::test]
async fn cancel_race_fill_still_hedged_after_user_stop_and_bad_entry_gap() {
    let files = Files::new();
    let (p, r) = fixture("hyperliquid", true, "BUY");
    let (m, mw) = mock(&r.maker, true, true, false, false).await;
    let (h, hw) = mock(&r.hedge, false, false, false, false).await;
    let (bt, br) = watch::channel(Books {
        maker: Some(quote("100", "100.01")),
        hedge: Some(quote("100", "100.01")),
    });
    let (stop, sr) = watch::channel(false);
    let engine = Engine::new(
        p,
        r,
        [m.clone(), h.clone()],
        [m.c.clone(), h.c.clone()],
        br,
        sr,
        files.0.clone(),
        false,
    );
    let task = tokio::spawn(engine.run());
    m.first_post.notified().await;
    bt.send_replace(Books {
        maker: Some(quote("100", "100.01")),
        hedge: Some(quote("90", "90.01")),
    });
    stop.send_replace(true);
    let result = task.await.unwrap().unwrap();
    assert_eq!(result["settled"], true);
    assert_eq!(result["maker_filled"], "0.4");
    assert_eq!(result["hedge_filled"], "0.4");
    assert_eq!(m.posts.load(Ordering::SeqCst), 1);
    m.c.shutdown().await.unwrap();
    h.c.shutdown().await.unwrap();
    let _ = mw.await;
    let _ = hw.await;
}
#[tokio::test]
async fn unknown_ack_and_not_found_remain_reserved_and_never_resend() {
    use perp_arb_execution::durable::single::Ledger;
    use perp_arb_order_test::Store;
    let files = Files::new();
    let (_, r) = fixture("aster", false, "SELL");
    let (v, w) = mock(&r.maker, true, false, true, false).await;
    let path = files.0.join("one.wal");
    let st = Arc::new(Store::start(
        Ledger::create(&path, r.maker, "200".into()).unwrap(),
    ));
    let (_tx, rx) = watch::channel(true);
    let wake = Arc::new(Notify::new());
    let trace = Arc::new(crate::latency::Trace::new(Instant::now()));
    assert!(worker::run(
        st.clone(),
        v.clone(),
        v.c.clone(),
        true,
        rx.clone(),
        wake.clone(),
        trace.clone(),
        1
    )
    .await
    .is_err());
    assert!(worker::run(
        st.clone(),
        v.clone(),
        v.c.clone(),
        false,
        rx,
        wake,
        trace,
        1
    )
    .await
    .is_err());
    assert_eq!(v.posts.load(Ordering::SeqCst), 1);
    assert!(!st.call(|l| Ok(l.view().terminal)).await.unwrap());
    Arc::try_unwrap(st).ok().unwrap().close().await.unwrap();
    v.c.shutdown().await.unwrap();
    let _ = w.await;
}
#[test]
fn policy_dynamic_price_and_pair_validation() {
    let (mut p, r) = fixture("aster", false, "SELL");
    let q = quote("100", "100.02");
    assert_eq!(hedge_price(&p, &r, Some(&q)).unwrap(), d("100.52").unwrap());
    assert!(hedge_price(&p, &r, None).is_err());
    assert!(entry(&p, d("100.1").unwrap(), &q, D::ONE).unwrap());
    r.validate_plan(&p).unwrap();
    let mut changed = r.clone();
    changed.hedge.account = format!("0x{}", "b".repeat(40));
    assert!(changed.validate_plan(&p).is_err());
    p.hedge_price_mode = "fixed_ioc".into();
    assert!(!entry(&p, d("100.1").unwrap(), &q, D::ONE).unwrap());
    p.hedge.side = p.maker.side.clone();
    assert!(p.validate().is_err());
}

#[tokio::test]
async fn lost_ack_reconciles_fill_without_resending_and_clips_finish_total() {
    let files = Files::new();
    let (mut p, r) = fixture("hyperliquid", false, "SELL");
    p.clip_quantity = "0.5".into();
    let (m, mw) = mock(&r.maker, true, false, false, true).await;
    let (h, hw) = mock(&r.hedge, false, false, false, true).await;
    let (bt, br) = watch::channel(Books::default());
    let pump = tokio::spawn(async move {
        loop {
            bt.send_replace(Books {
                maker: Some(quote("100", "100.01")),
                hedge: Some(quote("100", "100.01")),
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let (_s, sr) = watch::channel(false);
    let result = Engine::new(
        p,
        r,
        [m.clone(), h.clone()],
        [m.c.clone(), h.c.clone()],
        br,
        sr,
        files.0.clone(),
        false,
    )
    .run()
    .await
    .unwrap();
    assert_eq!(result["target_completed"], true);
    assert_eq!(m.posts.load(Ordering::SeqCst), 2);
    assert_eq!(
        h.records
            .lock()
            .unwrap()
            .values()
            .map(|r| r.order.quantity().unwrap())
            .sum::<D>(),
        D::ONE
    );
    pump.abort();
    let _ = pump.await;
    m.c.shutdown().await.unwrap();
    h.c.shutdown().await.unwrap();
    let _ = mw.await;
    let _ = hw.await;
}

#[tokio::test]
async fn corrupt_manifest_never_starts_recovery_requests() {
    let files = Files::new();
    let (p, r) = fixture("aster", false, "SELL");
    std::fs::write(
        files.0.join("orders.jsonl"),
        format!(
            "{}\n",
            serde_json::json!({"role":0,"index":0,"order":r.maker})
        ),
    )
    .unwrap();
    let (m, mw) = mock(&r.maker, true, false, false, false).await;
    let (h, hw) = mock(&r.hedge, false, false, false, false).await;
    let (_b, br) = watch::channel(Books::default());
    let (_s, sr) = watch::channel(false);
    let error = Engine::new(
        p,
        r,
        [m.clone(), h.clone()],
        [m.c.clone(), h.c.clone()],
        br,
        sr,
        files.0.clone(),
        true,
    )
    .run()
    .await
    .unwrap_err();
    assert!(error.contains("missing order ledger"));
    assert_eq!(
        m.posts.load(Ordering::SeqCst)
            + h.posts.load(Ordering::SeqCst)
            + m.cancels.load(Ordering::SeqCst),
        0
    );
    m.c.shutdown().await.unwrap();
    h.c.shutdown().await.unwrap();
    let _ = mw.await;
    let _ = hw.await;
}

#[tokio::test]
async fn valid_old_maker_stays_when_chasing_price_fails_gap_and_depth_size_changes() {
    let files = Files::new();
    let (mut p, r) = fixture("hyperliquid", false, "BUY");
    p.maker_wait_seconds = 30;
    let (m, mw) = mock(&r.maker, true, true, false, false).await;
    let (h, hw) = mock(&r.hedge, false, false, false, false).await;
    let (bt, br) = watch::channel(Books {
        maker: Some(quote("100", "100.01")),
        hedge: Some(quote("100", "100.01")),
    });
    let (stop, sr) = watch::channel(false);
    let task = tokio::spawn(
        Engine::new(
            p,
            r,
            [m.clone(), h.clone()],
            [m.c.clone(), h.c.clone()],
            br,
            sr,
            files.0.clone(),
            false,
        )
        .run(),
    );
    m.first_post.notified().await;
    for size in [2, 50, 3] {
        let hedge = Quote::new(
            d("100").unwrap(),
            d("100.01").unwrap(),
            perp_arb_market_data::hub::wall_ms(),
            None,
            Depth::Visible(vec![Level {
                price: d("100").unwrap(),
                size: D::from(size),
            }]),
            Depth::Visible(vec![Level {
                price: d("100.01").unwrap(),
                size: D::from(size),
            }]),
        )
        .unwrap();
        bt.send_replace(Books {
            maker: Some(quote("101", "101.01")),
            hedge: Some(hedge),
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(m.cancels.load(Ordering::SeqCst), 0);
        assert_eq!(m.posts.load(Ordering::SeqCst), 1);
    }
    stop.send_replace(true);
    assert_eq!(task.await.unwrap().unwrap()["settled"], true);
    m.c.shutdown().await.unwrap();
    h.c.shutdown().await.unwrap();
    let _ = mw.await;
    let _ = hw.await;
}

#[tokio::test]
async fn partial_terminal_ioc_releases_only_confirmed_remainder() {
    let files = Files::new();
    let (p, r) = fixture("aster", false, "SELL");
    let (m, mw) = mock(&r.maker, true, false, false, false).await;
    let (h, hw) = mock(&r.hedge, false, true, false, false).await;
    let (bt, br) = watch::channel(Books::default());
    let pump = tokio::spawn(async move {
        loop {
            bt.send_replace(Books {
                maker: Some(quote("100", "100.01")),
                hedge: Some(quote("100", "100.01")),
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let (_s, sr) = watch::channel(false);
    let result = Engine::new(
        p,
        r,
        [m.clone(), h.clone()],
        [m.c.clone(), h.c.clone()],
        br,
        sr,
        files.0.clone(),
        false,
    )
    .run()
    .await
    .unwrap();
    assert_eq!(result["target_completed"], true);
    assert_eq!(
        h.records
            .lock()
            .unwrap()
            .values()
            .map(|r| r.filled)
            .sum::<D>(),
        D::ONE
    );
    assert!(h.posts.load(Ordering::SeqCst) >= 2);
    pump.abort();
    let _ = pump.await;
    m.c.shutdown().await.unwrap();
    h.c.shutdown().await.unwrap();
    let _ = mw.await;
    let _ = hw.await;
}
