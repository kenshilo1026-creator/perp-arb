//! Native perpetual maker -> Binance Spot market execution. Explicit live action only.
mod journal;
mod progress;
mod runtime;
mod spot;
#[cfg(test)]
mod tests;
use crate::{
    d, market, metadata,
    policy::{self, Input, Mode, Venue},
    Result,
};
use perp_arb_core::Decimal as D;
use perp_arb_private_events::orders::{Network, Order, TimeInForce};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub symbol: String,
    pub native_symbol: String,
    pub venue: String,
    pub account: String,
    pub asset: Option<u32>,
    pub mode: Mode,
    pub total: String,
    pub clip: Option<String>,
    pub max_gap: String,
    pub rules: policy::Rules,
    pub spot_rules: policy::Rules,
}
impl Config {
    fn maker_side(&self) -> &'static str {
        if self.mode == Mode::Open {
            "SELL"
        } else {
            "BUY"
        }
    }
    fn hedge_side(&self) -> &'static str {
        if self.mode == Mode::Open {
            "BUY"
        } else {
            "SELL"
        }
    }
    fn venue(&self) -> Result<Venue> {
        Venue::parse_perp(&self.venue)
    }
    fn order(&self, quantity: String, price: String) -> Result<Order> {
        let o = Order {
            venue: self.venue.clone(),
            network: Network::Mainnet,
            account: self.account.clone(),
            symbol: self.native_symbol.clone(),
            side: self.maker_side().into(),
            quantity,
            limit_price: price,
            tif: TimeInForce::PostOnly,
            reduce_only: self.mode == Mode::Close,
            client_id: perp_arb_execution::durable::single::new_client_id(&self.venue)
                .map_err(|e| e.to_string())?,
            asset: self.asset,
        };
        o.validate().map_err(|e| e.to_string())?;
        Ok(o)
    }
    fn validate(&self) -> Result<()> {
        market::validate_base(&self.symbol)?;
        if !matches!(self.venue.as_str(), "aster" | "hyperliquid")
            || d(&self.total)? <= D::ZERO
            || d(&self.max_gap)? <= D::ZERO
            || d(&self.max_gap)? >= D::ONE
        {
            return Err("invalid live configuration".into());
        }
        self.order(self.total.clone(), "1".into())?;
        let mut i = self.input()?;
        i.spot.age_ms = 0;
        i.perp.age_ms = 0;
        policy::evaluate_perp_maker(&i)?;
        Ok(())
    }
    fn input(&self) -> Result<Input> {
        let dummy = policy::Book {
            bid: "1".into(),
            ask: "2".into(),
            bid_size: "0".into(),
            ask_size: "0".into(),
            age_ms: u64::MAX,
        };
        Ok(Input {
            mode: self.mode,
            perp_venue: self.venue()?,
            remaining: self.total.clone(),
            clip_limit: self.clip.clone(),
            max_gap: self.max_gap.clone(),
            spot: dummy.clone(),
            perp: dummy,
            rules: self.rules.clone(),
            resting: None,
        })
    }
}
fn env_account(venue: &str) -> Result<String> {
    let names = if venue == "aster" {
        vec!["ASTER_USER_ADDRESS"]
    } else {
        vec!["HYPERLIQUID_ACCOUNT_ADDRESS", "HYPERLIQUID_WALLET_ADDRESS"]
    };
    for name in names {
        if let Ok(v) = std::env::var(name) {
            if !v.trim().is_empty() {
                return Ok(v.trim().to_ascii_lowercase());
            }
        }
    }
    Err("missing ASTER_USER_ADDRESS or HYPERLIQUID_ACCOUNT_ADDRESS".into())
}
fn feeds(
    input: Input,
    spot: metadata::Instrument,
    perp: metadata::Instrument,
    mut stop: watch::Receiver<bool>,
) -> (watch::Receiver<Input>, tokio::task::JoinHandle<()>) {
    let (out, rx) = watch::channel(input.clone());
    let task = tokio::spawn(async move {
        let (tx, mut events) = tokio::sync::mpsc::channel(256);
        let a = market::subscribe(Venue::BinanceSpot, spot, tx.clone());
        let b = market::subscribe(input.perp_venue, perp, tx);
        let mut caches = [market::Cache::default(), market::Cache::default()];
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        loop {
            if *stop.borrow() {
                break;
            }
            tokio::select! {
                _=stop.changed()=>break,
                _=interval.tick()=>(),
                event=events.recv()=>{
                    let Some(e)=event else {break};let ix=if e.venue==Venue::BinanceSpot {0}else{1};
                    if let Some(q)=e.quote {let _=caches[ix].update(q);} else {caches[ix].quote=None;}
                }
            }
            let mut i = input.clone();
            if let Some(q) = &caches[0].quote {
                i.spot = q.aged();
            }
            if let Some(q) = &caches[1].quote {
                i.perp = q.aged();
            }
            out.send_replace(i);
        }
        a.abort();
        b.abort();
        let _ = tokio::join!(a, b);
    });
    (rx, task)
}
async fn fresh(books: &mut watch::Receiver<Input>) -> Result<Input> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let i = books.borrow().clone();
            if i.spot.age_ms <= 1000 && i.perp.age_ms <= 1000 {
                return Ok(i);
            }
            books.changed().await.map_err(|_| "public feeds ended")?;
        }
    })
    .await
    .map_err(|_| "60s without fresh public books")?
}
/// The permanent active pointer prevents a new run from concealing an unresolved one.
fn active_pointer() -> Result<PathBuf> {
    // Anchor to this build's project, not the caller's cwd: changing cwd must
    // not hide an unresolved run and allow another maker to be submitted.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../runs");
    std::fs::create_dir_all(&root).map_err(|_| "cannot create rust/runs; run from project root")?;
    Ok(root.join("spot-perp-active.json"))
}
fn set_pointer(pointer: &std::path::Path, dir: &std::path::Path, recover: bool) -> Result<()> {
    use std::io::Write;
    if pointer.exists() {
        let old = std::fs::read_to_string(pointer).map_err(|_| "cannot read active run")?;
        let saved: PathBuf =
            serde_json::from_str(&old).map_err(|_| "invalid active run pointer")?;
        if !recover || saved != dir {
            return Err(format!(
                "unresolved spot run exists; use recover --run-dir {}",
                saved.display()
            ));
        }
        return Ok(());
    }
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(pointer)
        .map_err(|_| "cannot reserve active run")?;
    let data = serde_json::to_vec(dir).map_err(|_| "active run encoding")?;
    f.write_all(&data)
        .and_then(|_| f.sync_all())
        .map_err(|_| "active run write")?;
    Ok(())
}

pub async fn run(options: BTreeMap<String, String>, recover: bool) -> Result<()> {
    let allowed = if recover {
        vec!["--run-dir", "--confirm"]
    } else {
        vec![
            "--run-dir",
            "--confirm",
            "--symbol",
            "--mode",
            "--total-size",
            "--clip-size",
            "--max-gap",
            "--perp-venue",
        ]
    };
    if options.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err("unsupported live/recover option".into());
    }
    if options.get("--confirm").map(String::as_str) != Some("PLACE_LIVE_SPOT_PERP_ORDER") {
        return Err("live/recover requires --confirm PLACE_LIVE_SPOT_PERP_ORDER".into());
    }
    let required = |key: &str| {
        options
            .get(key)
            .cloned()
            .ok_or_else(|| format!("missing {key}"))
    };
    let _global = perp_arb_order_test::AccountLock::global_execution()?;
    let requested = PathBuf::from(required("--run-dir")?);
    if !recover {
        std::fs::create_dir(&requested)
            .map_err(|_| "run directory must be new; create its parent first")?;
    }
    let dir = std::fs::canonicalize(&requested).map_err(|_| "missing run directory")?;
    let pointer = active_pointer()?;
    if !recover && pointer.exists() {
        return Err(
            "unresolved spot run exists; recover the path in rust/runs/spot-perp-active.json"
                .into(),
        );
    }
    let client = perp_arb_binance_spot::client::Client::new(
        perp_arb_binance_spot::auth::Credentials::from_env().map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    client.sync_time().await.map_err(|e| e.to_string())?;
    let saved = if recover {
        Some(journal::Store::open(dir.join("run.jsonl")).await?)
    } else {
        None
    };
    let public = market::client()?;
    let (base, venue) = if let Some(s) = &saved {
        let s = s.state()?;
        (s.config.symbol, s.config.venue)
    } else {
        (required("--symbol")?, required("--perp-venue")?)
    };
    if !matches!(venue.as_str(), "aster" | "hyperliquid") {
        return Err("live supports Aster/Hyperliquid maker -> Binance Spot taker only; other venues remain preview-only".into());
    }
    // Recovery must be able to cancel/query while public metadata or quotes are down.
    let (sm, pm) = if let Some(s) = &saved {
        let cfg = s.state()?.config;
        let item = |symbol: String| metadata::Instrument {
            symbol,
            tick: None,
            price_rule: "saved_recovery_only".into(),
            settlement: String::new(),
            market_id: None,
            filters: Default::default(),
        };
        (item(format!("{}USDT", cfg.symbol)), item(cfg.native_symbol))
    } else {
        market::instruments(&public, &base, Venue::parse_perp(&venue)?).await?
    };
    let config = if let Some(s) = &saved {
        s.state()?.config
    } else {
        Config {
            symbol: base,
            native_symbol: pm.symbol.clone(),
            venue: venue.clone(),
            account: env_account(&venue)?,
            asset: if venue == "hyperliquid" {
                Some(0)
            } else {
                None
            },
            mode: match required("--mode")?.as_str() {
                "open" => Mode::Open,
                "close" => Mode::Close,
                _ => return Err("mode must be open/close".into()),
            },
            total: required("--total-size")?,
            clip: options.get("--clip-size").cloned(),
            max_gap: options
                .get("--max-gap")
                .cloned()
                .unwrap_or_else(|| "0.0025".into()),
            rules: metadata::combine(&sm, &pm)?,
            spot_rules: metadata::combine(&sm, &sm)?,
        }
    };
    config.validate()?;
    let (shutdown, shutdown_rx) = watch::channel(false);
    let (mut books, book_task) = feeds(config.input()?, sm, pm, shutdown_rx.clone());
    let result=async {
        let mut config=config;
        let snapshot=if recover {books.borrow().clone()}else{fresh(&mut books).await?};
        // Resolve native asset ID/price precision using the established order transport validator.
        let price=if config.mode==Mode::Open {snapshot.perp.ask.clone()}else{snapshot.perp.bid.clone()};
        let q=config.rules.minimum.clone();
        let notional=d(&q)?.checked_mul(d(&price)?).ok_or("notional overflow")?;
        let quantity=if notional<d(&config.rules.min_notional)? {
            perp_arb_core::pricing::round_step(d(&config.rules.min_notional)?.checked_div(d(&price)?).ok_or("notional overflow")?,Some(d(&config.rules.step)?),perp_arb_core::pricing::Direction::Up).map_err(|e|e.to_string())?.to_string()
        } else {q};
        let template=if let Some(s)=&saved {
            s.state()?.makers.last().map(|m|m.order.clone()).unwrap_or(config.order(quantity.clone(),price.clone())?)
        }else{perp_arb_order_test::Plan{venue:config.venue.clone(),network:Network::Mainnet,account:config.account.clone(),symbol:config.native_symbol.clone(),side:config.maker_side().into(),
            quantity,limit_price:price,tif:TimeInForce::PostOnly,reduce_only:config.mode==Mode::Close,max_notional_usd:"1000000000000".into(),wait_seconds:30}.resolve().await?
        };
        if recover && template.asset!=config.asset {return Err("native asset identity changed; manual reconciliation required".into());}
        config.asset=template.asset;
        let _perp_lock=perp_arb_order_test::AccountLock::acquire(&template)?;
        let _spot_lock=perp_arb_order_test::AccountLock::for_scope("binance_spot",Network::Mainnet,&client.scope().account)?;
        let transport=std::sync::Arc::new(perp_arb_order_test::venue::LiveVenue::connect(&template).await?);
        let (coord,worker)=perp_arb_private_events::actor::channel(template.request().map_err(|e|e.to_string())?.scope,2048,1024).map_err(|e|e.to_string())?;
        let worker=worker.spawn();
        let monitor=match transport.monitor(coord.clone()).await {Ok(m)=>Some(m),Err(_) if recover=>None,Err(e)=>return Err(e)};
        let ready=if recover {Ok(Ok(()))}else{tokio::time::timeout(Duration::from_secs(20),async {let mut h=coord.watch_health();loop {if h.borrow().connected {return Ok::<_,String>(());} h.changed().await.map_err(|_|"private feed ended")?;}}).await};
        let connected=matches!(ready,Ok(Ok(())));
        let mut spot_task=None;
        let work=async {
            if !connected {return Err("perpetual private WS did not authenticate".into());}
            let (private,task)=spot::stream(client.clone(),shutdown_rx.clone(),!recover).await?;spot_task=Some(task);
            let state=if let Some(s)=&saved {Some(s.state()?)} else {None};
            if state.as_ref().is_some_and(|s|s.spot_account!=client.scope().account) {return Err("Binance credential identity differs from saved run".into());}
            if !recover {
                let report=transport.audit(&coord,None).await?;
                let inventory=report.inventory();
                if inventory.open_orders.keys().any(|k|k.symbol==config.native_symbol) {return Err("existing orders on selected perpetual symbol; cancel them before a new run".into());}
                if inventory.positions.keys().any(|k|k.symbol==config.native_symbol && k.side!="BOTH") {return Err("native spot owner requires one-way perpetual position mode".into());}
                if config.mode==Mode::Close {
                    let position=inventory.positions.iter().find(|(k,_)|k.symbol==config.native_symbol).map_or(D::ZERO,|(_,q)|*q);
                    if -position<d(&config.total)? {return Err("requested close exceeds current short; no automatic imbalance repair".into());}
                }
                let a=client.account().await.map_err(|e|e.to_string())?;
                if !a.can_trade {return Err("Binance account cannot trade".into());}
                let (asset,needed)=if config.mode==Mode::Close {(config.symbol.as_str(),d(&config.total)?)}else{("USDT",d(&config.total)?.checked_mul(d(&snapshot.spot.ask)?).and_then(|n|n.checked_mul(D::new(102,2))).ok_or("balance arithmetic")?)};
                if a.balances.get(asset).map_or(D::ZERO,|b|b.0)<needed {return Err("insufficient free Binance hedge balance for requested total".into());}
            }
            let store=if let Some(s)=saved {s}else{journal::Store::create(dir.join("run.jsonl"),config,client.scope().account).await?};
            set_pointer(&pointer,&dir,recover)?;
            let (stop_tx,stop)=watch::channel(false);
            let stop_file=dir.join("STOP");
            let signals=tokio::spawn(async move {loop {tokio::select! {_=tokio::signal::ctrl_c()=>{stop_tx.send_replace(true);break;},_=tokio::time::sleep(Duration::from_millis(100))=>{if stop_file.exists(){stop_tx.send_replace(true);break;}}}}});
            println!("Rust live: {} maker -> Binance Spot market hedge; run={}",venue,dir.display());
            let owner=runtime::Owner{store:store.clone(),venue:transport.clone(),coordinator:coord.clone(),hedger:std::sync::Arc::new(spot::LiveHedge{client:client.clone(),stream:private}),books:books.clone(),directory:dir.clone(),stop,timings:vec![],progress:None};
            let outcome=owner.execute(recover).await;
            signals.abort();let _=signals.await;
            // Print only final timing/accounting; no per-query timing spam.
            if let Ok(state)=store.state() {
                let summary=serde_json::json!({"state":if state.finished{"gross_hedges_settled"}else{"recovery_required"},"target":state.config.total,
                    "target_completed":state.finished && state.filled()?==d(&state.config.total)?,"net_base_delta_including_spot_fees":state.net_base_delta()?.map(|q|q.to_string()),
                    "maker_filled":state.filled()?.to_string(),"spot_gross_filled":state.hedged()?.to_string(),"makers":state.makers.len(),"hedges":state.hedges,"run_dir":dir});
                println!("{summary}");
                if std::fs::write(dir.join("summary.json"),format!("{summary}\n")).is_err() {eprintln!("cannot write summary.json; authoritative run journal retained");}
                if state.finished {std::fs::remove_file(&pointer).map_err(|_|"settled but could not clear active pointer")?;}
            }
            outcome
        }.await;
        shutdown.send_replace(true);
        if let Some(task)=spot_task {let _=task.await;}
        if let Some(m)=monitor {let _=m.shutdown().await;}
        let _=coord.shutdown().await;let _=worker.await;
        work
    }.await;
    shutdown.send_replace(true);
    let _ = book_task.await;
    result
}
