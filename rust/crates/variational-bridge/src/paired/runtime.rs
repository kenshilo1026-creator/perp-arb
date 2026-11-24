use super::{plan::InventoryFile, Baseline, Plan, Resolved};
use crate::{
    account_feed::Feed,
    execution_io,
    owner::{MakerRiskCheck, Owner},
    wire::{Channel, Observation},
    ws_validation::Validator,
    Bridge, Config, Result,
};
use perp_arb_core::{decimal, Decimal};
use perp_arb_execution::durable::{browser, DurableEngine, Target};
use perp_arb_market_data::hub::{wall_ms, MarketHub, Subscription};
use perp_arb_order_test::{venue::LiveVenue, AccountLock};
use perp_arb_private_events::{
    account::Report,
    actor::{self, Coordinator},
    orders::Network,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{watch, Mutex, Notify, RwLock};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Audit,
    Submit,
    Recover,
}
type Slot = Arc<RwLock<Option<Arc<Owner>>>>;
fn save_new(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| "cannot create new file; never overwrite a run or baseline")?;
    f.write_all(&serde_json::to_vec_pretty(value).map_err(|_| "serialization")?)
        .and_then(|()| f.sync_all())
        .map_err(|_| "cannot persist run file".into())
}
pub async fn preview(plan: &Plan) -> Result<Value> {
    let r = plan.resolve().await?;
    Ok(
        json!({"mode":"preview","maker":plan.maker,"instrument":plan.instrument,"hedge":r.order,"max_entry_gap":plan.max_entry_gap,"maker_fill_mode":plan.maker_fill_mode,"can_submit_orders":false}),
    )
}
pub async fn inspect(run: &Path) -> Result<Value> {
    let p = Plan::read(&run.join("plan.json"))?;
    let bytes = std::fs::read(run.join("execution-policy.json"))
        .map_err(|_| "execution policy missing; retain run directory")?;
    if bytes.len() > 4096 {
        return Err("execution policy too large".into());
    }
    let policy: Value = serde_json::from_slice(&bytes).map_err(|_| "invalid execution policy")?;
    let parse = |k: &str| {
        decimal(policy[k].as_str().ok_or("policy field missing")?).map_err(|e| e.to_string())
    };
    let ep = p.execution_plan_with_policy(parse("step")?, parse("minimum")?)?;
    let path = run.join("maker.wal");
    let paired_path = run.join("paired.wal");
    tokio::task::spawn_blocking(move || ->Result<Value>{
        let maker=browser::Ledger::open(&path).map_err(|e|e.to_string())?;
        let paired=DurableEngine::open(&paired_path,&ep).map_err(|e|e.to_string())?;
        paired.validate_browser_binding(&maker).map_err(|e|e.to_string())?;
        let intents:Vec<_>=paired.intents().into_iter().map(|i|json!({"id":i.intent.id,"quantity":i.intent.quantity.to_string(),"client_id":i.client_id,"order_id":i.order.map(|o|o.order_id),"filled":i.cumulative.to_string(),"terminal":i.terminal_confirmed})).collect();
        Ok(json!({"mode":"inspect","maker":maker.view(),"paired_unhedged":paired.exposure().map_err(|e|e.to_string())?.to_string(),"in_flight":paired.in_flight().map_err(|e|e.to_string())?.to_string(),"intents":intents,"note":"Read-only view of both journals. Recovery reconciles any interrupted cross-journal write."}))
    })
        .await
        .map_err(|_| "inspect worker")?
}

/// Event consumers remain alive throughout HTTP, journal writes and shutdown
/// recovery. The queues are bounded by the underlying transports and owner.
pub async fn execute(
    action: Action,
    plan: Plan,
    run: PathBuf,
    baseline: Option<Baseline>,
    parent_control: bool,
) -> Result<Value> {
    if action == Action::Submit {
        baseline
            .as_ref()
            .ok_or("reviewed baseline required")?
            .validate(&plan)?;
    }
    if action == Action::Audit && run.exists() {
        return Err("baseline output already exists".into());
    }
    let _process_lock = AccountLock::global_execution()?;
    let resolved = plan.resolve().await?;
    let _maker_lock = AccountLock::for_scope("variational", Network::Mainnet, &plan.maker.account)?;
    let _hedge_lock = AccountLock::acquire(&resolved.order)?;
    let venue = Arc::new(LiveVenue::connect(&resolved.order).await?);
    let (hedge, worker) = actor::channel(
        resolved.order.request().map_err(|e| e.to_string())?.scope,
        1024,
        4096,
    )
    .map_err(|e| e.to_string())?;
    let worker = worker.spawn();
    let (stop, stop_rx) = watch::channel(false);
    let slot: Slot = Arc::new(RwLock::new(None));
    let validation = Arc::new(Mutex::new(Validator::default()));
    let wake = Arc::new(Notify::new());
    let (failure, fault) = watch::channel(None::<String>);
    let mut monitor = None;
    let mut bridge = None;
    let mut collector = None;
    let mut hedge_collector = None;
    let mut fast_worker = None;
    let mut trace = None;
    let fast_wake = Arc::new(Notify::new());
    let hedge_lane = Arc::new(Mutex::new(()));
    let mut owner = None;
    let mut hub = MarketHub::new();
    let (interrupt, interrupt_rx) = watch::channel(false);
    if parent_control {
        let interrupt = interrupt.clone();
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = std::io::stdin().read_line(&mut line);
            if matches!(result, Ok(0) | Err(_)) || line.trim() == "STOP" {
                let _ = interrupt.send(true);
            }
        });
    }
    let signal = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = interrupt.send(true);
    });
    let outcome=async {
        monitor=Some(venue.monitor(hedge.clone()).await?);
        let mut feed=Feed::start(&plan.maker.account)?;
        let maker=feed.coordinator();
        let (b,mut events)=Bridge::bind(Config::new(plan.extension_id.clone())).await?;
        let timing=Arc::new(crate::latency::Trace::new(b.clock_start()));
        timing.mark("run_mode",&plan.run_id,json!({"maker_fill_mode":plan.maker_fill_mode,"recover":action==Action::Recover}));
        trace=Some(timing.clone());
        bridge=Some(b);
        let bridge=bridge.as_ref().ok_or("bridge missing")?;
        let s=slot.clone();let v=validation.clone();let notify=wake.clone();let mut stop=stop_rx.clone();let fail=failure.clone();
        let fw=fast_wake.clone(); let timing_events=timing.clone();
        collector=Some(tokio::spawn(async move{
            let result:Result<()>=async {
                loop {tokio::select!{
                    _=stop.changed()=>break,
                    e=events.recv()=>{
                        let e=e.ok_or("extension event stream stopped")?;
                        let gap=matches!(e.observation,Observation::Gap{..}|Observation::Disconnected);
                        if e.channel==Channel::Private && gap {
                            if let Some(o)=s.read().await.clone(){o.gap(Target::Maker).await?;}
                        }
                        feed.ingest(&e).await?;
                        let (candidate,unqualified)={
                            let mut validator=v.lock().await;
                            let before=validator.unqualified;
                            let result=validator.observe(&e)?;
                            (result,validator.unqualified>before)
                        };
                        if unqualified {
                            if let Observation::FillCandidate{candidate:c}=&e.observation {
                                timing_events.mark_at(e.received_elapsed_us,"maker_ws_unqualified",c.trade_id.as_deref().unwrap_or("missing"),json!({"candidate":c}));
                            }
                        }
                        if candidate.is_some() {
                            if let Observation::FillCandidate{candidate:c}=&e.observation {
                                timing_events.mark_at(e.received_elapsed_us,"maker_ws_candidate",c.trade_id.as_deref().unwrap_or("unknown"),json!({"rfq":c.order_id,"quantity":c.quantity,"candidate":c,"parsed_us":e.parsed_elapsed_us,"captured_wall_ms":e.captured_ms,"received_wall_ms":e.received_ms}));
                            }
                            notify.notify_one();fw.notify_one();
                        }
                    }
                }} Ok(())
            }.await;
            if let Err(e)=&result {
                if let Some(o)=s.read().await.clone(){let _=o.gap(Target::Maker).await;}
                let _=fail.send(Some(e.clone()));
            }
            let closed=feed.shutdown().await;
            result.and(closed)
        }));
        let s=slot.clone();let c=hedge.clone();let mut stop=stop_rx.clone();let fail=failure.clone();
        let hedge_timing=timing.clone();
        hedge_collector=Some(tokio::spawn(async move{
            let mut subscriptions=BTreeMap::new();
            let initial=c.health();
            let mut health_key=(initial.generation,initial.connected,initial.fault.is_some());
            let result:Result<()>=async {
                loop {tokio::select!{_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_millis(50))=>{}}
                    let Some(o)=s.read().await.clone() else {continue};
                    let p=o.progress().await?;
                    let h=c.health();
                    let next=(h.generation,h.connected,h.fault.is_some());
                    if next!=health_key {
                        if h.generation!=health_key.0||!h.connected||h.fault.is_some(){for i in &p.intents {if i.client_id.is_some(){o.gap(Target::Hedge(i.intent.id.clone())).await?;}}}
                        health_key=next;
                    }
                    for i in p.intents {
                        let Some(key)=i.order else {continue};
                        let first=!subscriptions.contains_key(&i.intent.id);
                        if first {
                            c.register(key.clone(),i.intent.quantity).await.map_err(|e|e.to_string())?;
                            subscriptions.insert(i.intent.id.clone(),c.subscribe(key).await.map_err(|e|e.to_string())?);
                        }
                        let rx=subscriptions.get_mut(&i.intent.id).ok_or("subscription missing")?;
                        if first || rx.has_changed().map_err(|_|"hedge subscription closed")? {
                            let snapshot=rx.borrow_and_update().clone();
                            if snapshot.event_ms.is_some()||snapshot.cumulative>Decimal::ZERO||snapshot.status.terminal(){
                                hedge_timing.mark_at(hedge_timing.at(snapshot.observed_at),"hedge_coordinator_observation",&i.intent.id,json!({"filled":snapshot.cumulative.to_string(),"terminal":snapshot.status.terminal(),"requires_reconciliation":snapshot.requires_reconciliation,"exchange_ms":snapshot.event_ms}));
                                o.observe_hedge(i.intent.id,snapshot).await?;
                            }
                        }
                    }
                } Ok(())
            }.await;
            if let Err(e)=&result{let _=fail.send(Some(e.clone()));} result
        }));
        wait_ready(bridge,&maker,&hedge,&fault,action!=Action::Audit).await?;
        if action==Action::Audit {
            let (m,h)=audit(bridge,&maker,&hedge,&venue,None).await?;
            let b=Baseline{reviewed:false,maker_account:plan.maker.account.clone(),hedge_account:plan.hedge.account.clone(),hedge_venue:plan.hedge.venue.clone(),maker:InventoryFile::from_inventory(m.inventory())?,hedge:InventoryFile::from_inventory(h.inventory())?};
            save_new(&run,&b)?;
            return Ok(json!({"mode":"audit","baseline_written":run,"reviewed":false,"can_submit_orders":false}));
        }
        let subscription=hub.subscribe(resolved.metadata.key.clone()).await?;
        // A bounded metadata lifetime; this runner never submits a replacement
        // maker. Every new invocation resolves current native specifications.
        let started=Instant::now();
        let (m_report,h_report)=if action==Action::Submit {
            let b=baseline.as_ref().ok_or("reviewed baseline required")?;
            let (m,h)=audit(bridge,&maker,&hedge,&venue,Some(b)).await?;
            if !m.matches_expected()||!h.matches_expected(){return Err("inventory differs from reviewed baseline; no maker submitted".into());}
            (Some(m),Some(h))
        }else{(None,None)};
        let execution_plan=plan.execution_plan(&resolved)?;
        let mp=run.join("maker.wal");let pp=run.join("paired.wal");
        let o=if action==Action::Submit {
            if !entry_ok(&plan,&subscription)?{return Err("entry gap/depth/quote unavailable; no maker submitted".into());}
            std::fs::create_dir(&run).map_err(|_|"run directory must be new")?;
            save_new(&run.join("plan.json"),&plan)?;
            save_new(&run.join("execution-policy.json"),&json!({"step":execution_plan.step.to_string(),"minimum":execution_plan.minimum.to_string()}))?;
            save_new(&run.join("baseline.json"),baseline.as_ref().ok_or("baseline missing")?)?;
            let command=plan.maker.clone();let mr=m_report.clone().ok_or("report missing")?;let hr=h_report.clone().ok_or("report missing")?;
            let mh=maker.health();let hh=hedge.health();
            Owner::start(128,move||{
                let paired=DurableEngine::create(&pp,execution_plan,(&mr,&mh),(&hr,&hh),true,Duration::from_secs(30)).map_err(|e|e.to_string())?;
                let maker=browser::Ledger::create(&mp,command).map_err(|e|e.to_string())?;
                Ok((maker,paired))
            }).await?
        }else{
            Owner::start(128,move||Ok((browser::Ledger::open(&mp).map_err(|e|e.to_string())?,DurableEngine::open(&pp,&execution_plan).map_err(|e|e.to_string())?))).await?
        };
        let o=Arc::new(o);owner=Some(o.clone());*slot.write().await=Some(o.clone());
        if action==Action::Submit && plan.maker_fill_mode==super::plan::FillMode::WsConfirmed {
            let fast=super::fast::Worker {
                owner:o.clone(),venue:venue.clone(),resolved:resolved.clone(),plan:plan.clone(),
                validation:validation.clone(),epoch:validation.lock().await.epoch(),
                wake:fast_wake.clone(),lane:hedge_lane.clone(),trace:timing.clone(),stop:stop_rx.clone(),failure:failure.clone(),
            };
            fast_worker=Some(tokio::spawn(fast.run()));
        }
        if action==Action::Submit {
            let check=MakerRiskCheck{maker:m_report.ok_or("report missing")?,hedge:h_report.ok_or("report missing")?,maker_health:maker.health(),hedge_health:hedge.health(),price_ok:entry_ok(&plan,&subscription)?,max_age:Duration::from_secs(30)};
            if *interrupt_rx.borrow(){return Err("interrupted before dispatch".into());}
            if let Err(e)=execution_io::submit_maker(&o,bridge,check,plan.instrument.clone()).await{eprintln!("{e}");}
            fast_wake.notify_one();
        }
        trade_loop(&plan,&resolved,&o,bridge,venue.as_ref(),&||entry_ok(&plan,&subscription),&validation,&wake,&fault,interrupt_rx,action==Action::Recover,started,&hedge_lane,Some(&timing)).await
    }.await;
    let _ = stop.send(true);
    signal.abort();
    let mut cleanup_error = None;
    if let Some(t) = fast_worker {
        if let Err(e) = t
            .await
            .map_err(|_| "fast hedge worker failed".to_string())
            .and_then(|r| r)
        {
            cleanup_error = Some(e);
        }
    }
    if let Some(t) = collector {
        if let Err(e) = t
            .await
            .map_err(|_| "private event task failed".to_string())
            .and_then(|r| r)
        {
            cleanup_error = Some(e);
        }
    }
    if let Some(t) = hedge_collector {
        if let Err(e) = t
            .await
            .map_err(|_| "hedge event task failed".to_string())
            .and_then(|r| r)
        {
            cleanup_error = Some(e);
        }
    }
    *slot.write().await = None;
    if let Some(o) = owner {
        if let (Some(t), Ok(p)) = (&trace, o.progress().await) {
            t.mark("final_progress",&plan.run_id,json!({"maker_filled":p.maker.cumulative,"maker_terminal":p.maker.terminal_confirmed,"unhedged":p.exposure,"in_flight":p.in_flight,"faulted":p.faulted}));
        }
        match Arc::try_unwrap(o) {
            Ok(o) => {
                if let Err(e) = o.shutdown().await {
                    cleanup_error = Some(e)
                }
            }
            Err(_) => cleanup_error = Some("owner still in use".into()),
        }
    }
    if let Some(b) = bridge {
        b.shutdown().await;
    }
    if let Some(m) = monitor {
        if let Err(e) = m.shutdown().await {
            cleanup_error = Some(e);
        }
    }
    let _ = hedge.shutdown().await;
    let _ = worker.await;
    if let Err(e) = hub.shutdown().await {
        cleanup_error = Some(e);
    }
    if action != Action::Audit && run.is_dir() {
        if let Some(t) = trace {
            match t.save(&run) {
                Ok(path) => eprintln!("[latency] {}", path.display()),
                Err(e) => cleanup_error = Some(e),
            }
        }
    }
    match outcome {
        Err(e) => Err(e),
        Ok(v) => {
            if let Some(e) = cleanup_error {
                Err(e)
            } else {
                Ok(v)
            }
        }
    }
}
async fn wait_ready(
    b: &Bridge,
    m: &Coordinator,
    h: &Coordinator,
    f: &watch::Receiver<Option<String>>,
    trading: bool,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30),async{
        loop {
            if let Some(e)=f.borrow().clone(){return Err(e);}
            let bh=b.health().borrow().clone();
            if bh.private_connected&&bh.command_connected&&bh.native_protocol&&bh.fault.is_none()&&m.health().connected&&h.health().connected {
                if trading&&!bh.native_trading_enabled{return Err("extension native trading is disabled; enable it in popup and restart forwarding".into());}
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await.map_err(|_|"private streams not ready within 30 seconds")?
}
async fn audit(
    b: &Bridge,
    m: &Coordinator,
    h: &Coordinator,
    v: &LiveVenue,
    expected: Option<&Baseline>,
) -> Result<(Report, Report)> {
    for attempt in 0..2 {
        let mt = m.begin_account_query().await.map_err(|e| e.to_string())?;
        let mi = expected.map(|e| e.maker.inventory()).transpose()?;
        let hi = expected.map(|e| e.hedge.inventory()).transpose()?;
        let (mq, hr) = tokio::join!(b.inventory(m.scope().account.clone()), v.audit(h, hi));
        let mr = mq?.reconcile(m, mt, mi).await;
        if attempt == 0
            && (mr.as_ref().is_err_and(|e| account_revision_race(e))
                || hr.as_ref().is_err_and(|e| account_revision_race(e)))
        {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        return Ok((mr?, hr?));
    }
    Err("account changed during both audit attempts".into())
}
pub(crate) fn account_revision_race(error: &str) -> bool {
    // Native adapters wrap coordinator errors; the browser feed exposes them
    // directly. Only this revision/session race permits another full audit.
    matches!(error, "WrongSession" | "Coordination(WrongSession)")
}
fn entry_ok(p: &Plan, s: &Subscription) -> Result<bool> {
    let view = s.snapshot();
    let Some(q) = view.fresh(wall_ms()) else {
        return Ok(false);
    };
    p.entry_ok(&q.quote)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn trade_loop<V: perp_arb_order_test::venue::Venue>(
    p: &Plan,
    r: &Resolved,
    o: &Owner,
    b: &Bridge,
    v: &V,
    entry: &impl Fn() -> Result<bool>,
    validator: &Mutex<Validator>,
    wake: &Notify,
    fault: &watch::Receiver<Option<String>>,
    stop: watch::Receiver<bool>,
    recover: bool,
    started: Instant,
    hedge_lane: &Mutex<()>,
    trace: Option<&crate::latency::Trace>,
) -> Result<Value> {
    let mut cancelling = recover;
    let mut last_error = None;
    let mut next_lookup = Instant::now();
    let deadline = Instant::now()
        + Duration::from_secs(if recover {
            p.recovery_seconds
        } else {
            p.maker_wait_seconds + p.recovery_seconds
        });
    loop {
        let progress = o.progress().await?;
        if !progress.maker.dispatched {
            return Err(
                "maker not dispatched; prepared journal retained; no automatic submit on recover"
                    .into(),
            );
        }
        if progress.maker.order_id.is_none() {
            if let Err(e) = execution_io::recover_maker_identity(o, b).await {
                last_error = Some(e);
            }
        }
        if *stop.borrow()
            || started.elapsed() > Duration::from_secs(p.maker_wait_seconds)
            || fault.borrow().is_some()
            || !entry().unwrap_or(false)
        {
            cancelling = true;
        }
        let current = o.progress().await?;
        if cancelling
            && current.maker.order_id.is_some()
            && !current.maker.terminal_confirmed
            && !current.maker.cancel_pending
        {
            if let Err(e) = execution_io::cancel_maker(o, b).await {
                last_error = Some(e);
            }
        }
        if current.maker.order_id.is_some() && Instant::now() >= next_lookup {
            let id = current.maker.order_id.as_deref().unwrap_or("unknown");
            if let Some(t) = trace {
                t.mark("maker_rest_begin", id, json!({}));
            }
            if let Err(e) = execution_io::reconcile_maker_with_ws(o, b, validator).await {
                last_error = Some(e);
                cancelling = true;
            }
            if let Some(t) = trace {
                t.mark("maker_rest_done", id, json!({}));
                for trade in validator.lock().await.verified_ids(id) {
                    t.mark("maker_rest_verified_trade", &trade, json!({"rfq":id}));
                }
            }
            next_lookup = Instant::now() + Duration::from_secs(2);
        }
        let progress = o.progress().await?;
        for intent in &progress.intents {
            if intent.client_id.is_some()
                && (!intent.terminal_confirmed
                    || progress
                        .recovery_pending
                        .contains(&Target::Hedge(intent.intent.id.clone())))
            {
                if let Some(t) = trace {
                    t.mark("hedge_rest_begin", &intent.intent.id, json!({}));
                }
                if let Err(e) = execution_io::reconcile_hedge(o, v, intent.intent.id.clone()).await
                {
                    last_error = Some(e);
                }
                if let Some(t) = trace {
                    t.mark("hedge_rest_done", &intent.intent.id, json!({}));
                    if let Some(i) = o
                        .progress()
                        .await?
                        .intents
                        .iter()
                        .find(|i| i.intent.id == intent.intent.id)
                    {
                        t.mark("hedge_rest_confirmation",&intent.intent.id,json!({"filled":i.cumulative.to_string(),"terminal":i.terminal_confirmed}));
                    }
                }
            }
        }
        {
            let _lane = hedge_lane.lock().await;
            // A WS worker may have reserved or sent while this loop awaited REST.
            let progress = o.progress().await?;
            if !progress.faulted {
                let pending = progress
                    .intents
                    .iter()
                    .find(|i| i.client_id.is_none())
                    .map(|i| i.intent.clone());
                let reservation = if pending.is_some() {
                    Ok(pending)
                } else {
                    o.reserve_confirmed_hedge().await
                };
                match reservation {
                    Ok(Some(intent)) => {
                        let mut order = r.order.clone();
                        order.quantity = intent.quantity.to_string();
                        if let Err(e) = execution_io::dispatch_hedge_traced(
                            o,
                            v,
                            intent.id,
                            order,
                            &r.metadata.limit,
                            decimal(&p.hedge.max_notional_usd).map_err(|e| e.to_string())?,
                            trace,
                            None,
                        )
                        .await
                        {
                            last_error = Some(e);
                            cancelling = true;
                        }
                    }
                    Ok(None) => (),
                    Err(e) => {
                        if decimal(&progress.exposure).map_err(|e| e.to_string())? > Decimal::ZERO {
                            last_error = Some(e);
                            cancelling = true;
                        }
                    }
                }
            }
        }
        let progress = o.progress().await?;
        if progress.maker.terminal_confirmed
            && decimal(&progress.exposure).map_err(|e| e.to_string())? == Decimal::ZERO
            && decimal(&progress.in_flight).map_err(|e| e.to_string())? == Decimal::ZERO
            && progress.recovery_pending.is_empty()
            && !progress.faulted
        {
            return Ok(
                json!({"completed":true,"maker_filled":progress.maker.cumulative,"unhedged":progress.exposure,"in_flight":progress.in_flight,"hedge_attempts":progress.intents.len()}),
            );
        }
        if Instant::now() >= deadline {
            return Err(format!("paired order incomplete; recover the SAME run directory: maker_filled={} unhedged={} in_flight={} maker_terminal={} last_error={}",progress.maker.cumulative,progress.exposure,progress.in_flight,progress.maker.terminal_confirmed,last_error.unwrap_or_else(||"terminal proof pending".into())));
        }
        tokio::select! {_=wake.notified()=>{next_lookup=Instant::now();},_=tokio::time::sleep(Duration::from_secs(1))=>()}
    }
}
