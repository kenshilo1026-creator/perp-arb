use super::*;
use crate::{
    latency::Trace,
    paired::{plan::InventoryFile, Action},
};
use perp_arb_execution::durable::single::{Ledger, View};
use perp_arb_market_data::{
    hub::{wall_ms, MarketHub},
    shared::MarketKey,
    Venue as PublicVenue,
};
use perp_arb_order_test::{
    venue::{LiveVenue, Venue},
    AccountLock, Store,
};
use perp_arb_private_events::{
    account::Inventory,
    actor::{self, Coordinator},
};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{watch, Notify},
    task::JoinHandle,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Baseline {
    pub reviewed: bool,
    pub plan: Plan,
    pub maker: InventoryFile,
    pub hedge: InventoryFile,
}
#[derive(Clone, Default)]
pub(super) struct Books {
    pub maker: Option<Quote>,
    pub hedge: Option<Quote>,
}
impl Books {
    fn fresh(&self) -> Self {
        let fresh = |q: &Option<Quote>| {
            q.as_ref()
                .filter(|q| {
                    q.freshness(wall_ms(), true, Default::default())
                        == perp_arb_core::market::Freshness::Fresh
                })
                .cloned()
        };
        Self {
            maker: fresh(&self.maker),
            hedge: fresh(&self.hedge),
        }
    }
}
struct Running {
    role: usize,
    store: Option<Arc<Store>>,
    final_view: Option<View>,
    cancel: watch::Sender<bool>,
    job: Option<JoinHandle<Result<()>>>,
    done: bool,
}
pub(super) struct Engine<V: Venue + Send + Sync + 'static> {
    pub plan: Plan,
    pub resolved: Resolved,
    pub venues: [Arc<V>; 2],
    pub coords: [Coordinator; 2],
    pub books: watch::Receiver<Books>,
    pub stop: watch::Receiver<bool>,
    pub dir: PathBuf,
    running: Vec<Running>,
    wake: Arc<Notify>,
    trace: Arc<Trace>,
    pub recover: bool,
}
impl<V: Venue + Send + Sync + 'static> Engine<V> {
    #[allow(clippy::too_many_arguments)] // Pair resources remain injectable for execution tests.
    pub(super) fn new(
        plan: Plan,
        resolved: Resolved,
        venues: [Arc<V>; 2],
        coords: [Coordinator; 2],
        books: watch::Receiver<Books>,
        stop: watch::Receiver<bool>,
        dir: PathBuf,
        recover: bool,
    ) -> Self {
        Self {
            plan,
            resolved,
            venues,
            coords,
            books,
            stop,
            dir,
            recover,
            running: vec![],
            wake: Arc::new(Notify::new()),
            trace: Arc::new(Trace::new(Instant::now())),
        }
    }
    async fn launch(&mut self, role: usize, ledger: Ledger, submit: bool) -> Result<()> {
        let store = Arc::new(Store::start(ledger));
        let (cancel, rx) = watch::channel(self.recover && role == 0);
        let timeout = if role == 0 {
            self.plan.maker_wait_seconds + self.plan.recovery_seconds
        } else {
            self.plan.recovery_seconds
        };
        let job = tokio::spawn(worker::run(
            store.clone(),
            self.venues[role].clone(),
            self.coords[role].clone(),
            submit,
            rx,
            self.wake.clone(),
            self.trace.clone(),
            timeout,
        ));
        self.running.push(Running {
            role,
            store: Some(store),
            final_view: None,
            cancel,
            job: Some(job),
            done: false,
        });
        Ok(())
    }
    async fn new_order(&mut self, role: usize, quantity: D, price: D) -> Result<()> {
        let mut o = if role == 0 {
            self.resolved.maker.clone()
        } else {
            self.resolved.hedge.clone()
        };
        o.quantity = quantity.to_string();
        o.limit_price = price.to_string();
        o.client_id = perp_arb_execution::durable::single::new_client_id(&o.venue)
            .map_err(|e| e.to_string())?;
        let rules = if role == 0 {
            &self.resolved.maker_rules
        } else {
            &self.resolved.hedge_rules
        }
        .instrument()?;
        if let Some(e) = rules
            .size_error(quantity, Some(price))
            .map_err(|e| e.to_string())?
        {
            return Err(e.code().into());
        }
        if o.tif != TimeInForce::Market
            && rules
                .price_error(price)
                .map_err(|e| e.to_string())?
                .is_some()
        {
            return Err("invalid price precision".into());
        }
        let cap = quantity
            .checked_mul(price)
            .ok_or("notional overflow")?
            .to_string();
        let index = self.running.len();
        let path = self.dir.join(format!(
            "{}-{index:06}.wal",
            if role == 0 { "maker" } else { "hedge" }
        ));
        eprintln!(
            "[{}-submit] {} {} {} quantity={} price={} tif={:?} notional={}",
            if role == 0 { "maker" } else { "hedge" },
            o.venue,
            o.side,
            o.symbol,
            o.quantity,
            o.limit_price,
            o.tif,
            cap
        );
        self.trace.mark(
            "order_reserved",
            &o.client_id,
            json!({"role":role,"quantity":quantity.to_string()}),
        );
        let manifest = self.dir.join("orders.jsonl");
        let ledger = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(manifest)
                .map_err(|e| e.to_string())?;
            writeln!(f, "{}", json!({"role":role,"index":index,"order":o}))
                .and_then(|_| f.sync_all())
                .map_err(|e| e.to_string())?;
            Ledger::create(&path, o, cap).map_err(|e| e.to_string())
        })
        .await
        .map_err(|_| "journal create worker")??;
        self.launch(role, ledger, true).await
    }
    async fn views(&self) -> Result<Vec<View>> {
        let mut views = vec![];
        for r in &self.running {
            views.push(if let Some(view) = &r.final_view {
                view.clone()
            } else {
                r.store
                    .as_ref()
                    .ok_or("missing active store")?
                    .call(|l| Ok(l.view()))
                    .await?
            });
        }
        Ok(views)
    }
    async fn open_saved(&mut self) -> Result<()> {
        let manifest_path = self.dir.join("orders.jsonl");
        let text = std::fs::read_to_string(&manifest_path)
            .map_err(|_| "missing native order manifest; never reconstruct missing orders")?;
        if text.len() > 4_194_304 || (!text.is_empty() && !text.ends_with('\n')) {
            return Err("truncated/oversized native order manifest".into());
        }
        let mut entries = vec![];
        for (index, line) in text.lines().enumerate() {
            let v: Value =
                serde_json::from_str(line).map_err(|_| "corrupt native order manifest")?;
            let role = v["role"]
                .as_u64()
                .filter(|v| *v < 2)
                .ok_or("invalid manifest role")? as usize;
            if v["index"].as_u64() != Some(index as u64) {
                return Err("non-contiguous native order manifest".into());
            }
            let order: Order =
                serde_json::from_value(v["order"].clone()).map_err(|_| "invalid manifest order")?;
            let path = self.dir.join(format!(
                "{}-{index:06}.wal",
                if role == 0 { "maker" } else { "hedge" }
            ));
            entries.push((path, role, order));
        }
        let mut found = 0;
        for e in std::fs::read_dir(&self.dir).map_err(|e| e.to_string())? {
            let p = e.map_err(|e| e.to_string())?.path();
            if p.extension().is_some_and(|s| s == "wal") {
                if !entries.iter().any(|(path, _, _)| *path == p) {
                    return Err("unlisted order ledger; stop for reconciliation".into());
                }
                found += 1;
            }
        }
        if found != entries.len() {
            return Err("missing order ledger; never resend after deleted/missing WAL".into());
        }
        let mut validated = vec![];
        for (path, role, expected) in entries {
            let ledger =
                tokio::task::spawn_blocking(move || Ledger::open(&path).map_err(|e| e.to_string()))
                    .await
                    .map_err(|_| "journal open worker")??;
            let view = ledger.view();
            if view.order != expected {
                return Err("manifest/ledger identity mismatch".into());
            }
            let t = if role == 0 {
                &self.resolved.maker
            } else {
                &self.resolved.hedge
            };
            if view.order.venue != t.venue
                || view.order.account != t.account
                || view.order.symbol != t.symbol
                || view.order.side != t.side
                || view.order.reduce_only != t.reduce_only
                || view.order.asset != t.asset
                || view.order.tif != t.tif
                || view.order.network != t.network
            {
                return Err("saved order does not match original paired plan".into());
            }
            validated.push((role, ledger));
        }
        // Validate every WAL before allowing any recovery worker to mutate an order.
        for (role, ledger) in validated {
            self.launch(role, ledger, false).await?;
        }
        Ok(())
    }
    pub(super) async fn run(mut self) -> Result<Value> {
        let outcome=async {
            self.plan.validate()?;
            self.resolved.validate_plan(&self.plan)?;
            if self.recover {self.open_saved().await?;}
            let started=Instant::now();let mut waiting=Instant::now();let mut last_log=None;
            let mut active_since=Instant::now();let mut active_id=None;
            let mut stopping=self.recover;let mut reason=if self.recover{Some("recovery".to_string())}else{None};
            let mut cleanup_since=None;
            loop {
                for r in &mut self.running {
                    if r.job.as_ref().is_some_and(|j|j.is_finished()) {
                        let result=r.job.take().unwrap().await.map_err(|_|"order task panicked")?;
                        let st=r.store.as_ref().ok_or("missing completed store")?;
                        r.final_view=Some(st.call(|l|Ok(l.view())).await?);
                        if let Some(st)=r.store.take() {
                            let st=Arc::try_unwrap(st).map_err(|_|"completed worker still holds journal")?;
                            st.close().await?;
                        }
                        r.done=true;
                        if let Err(e)=result {stopping=true;reason=Some(e);}
                    }
                }
                let views=self.views().await?;
                let mut filled=D::ZERO;let mut hedged=D::ZERO;let mut reserved=D::ZERO;let mut active=None;
                let mut settled=true;let mut maker_count=0;
                for (i,(r,v)) in self.running.iter().zip(&views).enumerate() {
                    if v.faulted {stopping=true;reason=Some("order evidence fault; retain run".into());}
                    let live=v.dispatched || !r.done;
                    settled &= !v.faulted && (!live || (v.terminal && v.reconciled)) && r.done;
                    if r.role==0 {
                        maker_count+=1;filled+=d(&v.cumulative)?;
                        if live && !(v.terminal && v.reconciled && r.done) {active=Some(i);}
                    }else{
                        hedged+=d(&v.cumulative)?;
                        if live {reserved+=if v.terminal && v.reconciled {d(&v.cumulative)?}else{d(&v.order.quantity)?};}
                    }
                }
                let target=d(&self.plan.maker.quantity)?;
                if filled>target || reserved>filled || hedged>filled{return Err("paired fill/reservation invariant violated".into());}
                if *self.stop.borrow() {stopping=true;reason.get_or_insert("user_stop".into());}
                if started.elapsed()>Duration::from_secs(3600) || (active.is_none() && maker_count>=self.plan.max_reprices && filled<target) {stopping=true;reason.get_or_insert("execution limit reached".into());}
                if stopping && cleanup_since.is_none(){cleanup_since=Some(Instant::now());}
                if settled && filled==hedged && (filled==target || stopping) {
                    return Ok(json!({"mode":"native_pair","target_completed":filled==target,"maker_filled":filled.to_string(),"hedge_filled":hedged.to_string(),"settled":true,"reason":reason,"run_dir":self.dir}));
                }
                let books=self.books.borrow().fresh();
                let due=filled-reserved;
                if due>D::ZERO {
                    let rules=self.resolved.hedge_rules.instrument()?;
                    let mut amount=rules.round_quantity(due).map_err(|e|e.to_string())?;
                    if let Some(max)=rules.max_size(){amount=amount.min(max);}
                    if let Ok(price)=hedge_price(&self.plan,&self.resolved,books.hedge.as_ref()) {
                        if rules.size_error(amount,Some(price)).map_err(|e|e.to_string())?.is_none() {
                            // No entry-gap or maker-health gate after an actual fill.
                            self.new_order(1,amount,price).await?;
                            self.trace.mark("hedge_scheduled",&self.plan.run_id,json!({"quantity":amount.to_string(),"maker_total":filled.to_string()}));
                            continue;
                        }
                    }
                }
                if let Some(i)=active {
                    let v=&views[i];
                    if active_id.as_ref()!=Some(&v.order.client_id){active_id=Some(v.order.client_id.clone());active_since=Instant::now();}
                    let remaining=d(&v.order.quantity)?-d(&v.cumulative)?;
                    let healthy=self.coords.iter().all(|c|c.health().connected && c.health().fault.is_none());
                    let why=if stopping {Some("stop_or_recovery")}
                        else if !healthy {Some("private_feed_unhealthy")}
                        else if active_since.elapsed()>Duration::from_secs(self.plan.maker_wait_seconds){Some("maker_timeout")}
                        else {match (&books.maker,&books.hedge) {
                            (Some(m),Some(h))=>{
                                let old=d(&v.order.limit_price)?;let new=passive(&self.plan.maker.side,m);
                                if remaining>D::ZERO && !entry(&self.plan,old,h,remaining)? {Some("adverse_gap_or_depth")}
                                else if new!=old && entry(&self.plan,new,h,remaining)? {Some("best_price_changed")}
                                else {None}
                            },_=>Some("stale_quote")
                        }};
                    if let Some(why)=why {
                        if !*self.running[i].cancel.borrow(){eprintln!("[reprice] {} price={} reason={why}",v.order.venue,v.order.limit_price);self.running[i].cancel.send_replace(true);}
                    }
                    waiting=Instant::now();
                }else if !stopping && settled && due==D::ZERO && filled<target {
                    if let (Some(m),Some(h))=(&books.maker,&books.hedge) {
                        let q=(target-filled).min(d(&self.plan.clip_quantity)?);
                        let price=passive(&self.plan.maker.side,m);
                        if self.coords.iter().all(|c|c.health().connected && c.health().fault.is_none()) && entry(&self.plan,price,h,q)? {
                            if q*price>d(&self.plan.maker.max_notional_usd)? {stopping=true;reason=Some("maker notional exceeds plan cap".into());continue;}
                            self.new_order(0,q,price).await?;waiting=Instant::now();continue;
                        }
                    }
                    if waiting.elapsed()>Duration::from_secs(60){stopping=true;reason=Some("60s without qualifying gap/depth".into());continue;}
                }
                if active.is_none() && filled!=hedged && waiting.elapsed()>Duration::from_secs(self.plan.recovery_seconds) {return Err("unhedged residual or unknown hedge; recover original run".into());}
                if cleanup_since.is_some_and(|s|s.elapsed()>Duration::from_secs(self.plan.recovery_seconds)) {return Err("cleanup incomplete; recover original run".into());}
                let progress=(filled,hedged,reserved,stopping);
                if last_log!=Some(progress){eprintln!("[progress] maker={filled}/{target} hedge={hedged} reserved={reserved} stopping={stopping}");last_log=Some(progress);}
                tokio::select! {_=self.wake.notified()=>(),_=self.books.changed()=>(),_=self.stop.changed(),if !stopping=>(),_=tokio::time::sleep(Duration::from_millis(50))=>()}
            }
        }.await;
        // Never abandon a pending POST. Every worker is drained before account locks release.
        for r in &self.running {
            r.cancel.send_replace(true);
        }
        for r in &mut self.running {
            if let Some(j) = r.job.take() {
                let _ = j.await;
            }
        }
        let final_views = self.views().await;
        match self.trace.save(&self.dir) {
            Ok(_) => (),
            Err(e) => eprintln!("[timing-error] {e}"),
        }
        for r in self.running {
            if let Some(store) = r.store {
                if let Ok(s) = Arc::try_unwrap(store) {
                    let _ = s.close().await;
                }
            }
        }
        match outcome {
            Ok(value) => {
                let marker = self.dir.join("finished.json");
                if marker.exists() {
                    let old: Value = read(&marker)?;
                    for key in [
                        "settled",
                        "maker_filled",
                        "hedge_filled",
                        "target_completed",
                    ] {
                        if old[key] != value[key] {
                            return Err("completion marker differs from reconciled ledgers".into());
                        }
                    }
                } else {
                    save(&marker, &value)?;
                }
                Ok(value)
            }
            Err(e) => {
                eprintln!("[recovery-required] {e}; run={}", self.dir.display());
                if let Ok(v) = final_views {
                    eprintln!("{}", json!({"orders":v}));
                }
                Err(e)
            }
        }
    }
}
fn inventory_ok(i: &Inventory, o: &Order, total: D) -> Result<()> {
    if i.open_orders.keys().any(|k| k.symbol == o.symbol) {
        return Err("existing orders on selected symbol; no new maker".into());
    }
    if i.positions
        .keys()
        .any(|k| k.symbol == o.symbol && k.side != "BOTH")
    {
        return Err("native pair requires one-way positions".into());
    }
    if o.reduce_only {
        let q = i
            .positions
            .iter()
            .find(|(k, _)| k.symbol == o.symbol)
            .map_or(D::ZERO, |(_, q)| *q);
        let available = if o.side == "BUY" { -q } else { q };
        if available < total {
            return Err("close quantity exceeds existing position".into());
        }
    }
    Ok(())
}
pub async fn execute(
    action: Action,
    p: Plan,
    dir: PathBuf,
    baseline: Option<PathBuf>,
    parent_control: bool,
) -> Result<Value> {
    p.validate()?;
    let _global = AccountLock::global_execution()?;
    let recover = action == Action::Recover;
    let pointer =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../runs/native-pair-active.json");
    if action == Action::Submit && (pointer.exists() || dir.exists()) {
        return Err("submit requires a new run directory and no unresolved native pair; inspect/recover active run".into());
    }
    if recover && pointer.exists() {
        let original: PathBuf = read(&pointer)?;
        if original != std::fs::canonicalize(&dir).map_err(|e| e.to_string())? {
            return Err("recover directory differs from active run".into());
        }
    }
    let resolved: Resolved = if recover {
        read(&dir.join("resolved.json"))?
    } else {
        p.resolve().await?
    };
    resolved.validate_plan(&p)?;
    let _ml = AccountLock::acquire(&resolved.maker)?;
    let _hl = AccountLock::acquire(&resolved.hedge)?;
    let m = Arc::new(LiveVenue::connect(&resolved.maker).await?);
    let h = Arc::new(LiveVenue::connect(&resolved.hedge).await?);
    let channel = |o: &Order| {
        let scope = o.request().map_err(|e| e.to_string())?.scope;
        (if action == Action::Audit {
            actor::channel(scope, 1024, 4096)
        } else {
            actor::position_channel(scope, &o.symbol, 1024, 4096)
        })
        .map_err(|e| e.to_string())
    };
    let (mc, mw) = channel(&resolved.maker)?;
    let (hc, hw) = channel(&resolved.hedge)?;
    let mw = mw.spawn();
    let hw = hw.spawn();
    let mut mm = None;
    let mut hm = None;
    let mut feed = None;
    let mut signal = None;
    let (stop_tx, stop) = watch::channel(false);
    let mut hub = MarketHub::new();
    let result=async {
        mm=m.monitor(mc.clone()).await.ok();hm=h.monitor(hc.clone()).await.ok();
        if !recover {
            tokio::time::timeout(Duration::from_secs(30),async {loop{if mc.health().connected&&hc.health().connected{return;}tokio::time::sleep(Duration::from_millis(50)).await;}}).await.map_err(|_|"private WS not ready")?;
            if action==Action::Audit {
                let (mr,hr)=tokio::try_join!(m.audit(&mc,None),h.audit(&hc,None))?;
                let b=Baseline{reviewed:false,plan:p.clone(),maker:InventoryFile::from_inventory(mr.inventory())?,hedge:InventoryFile::from_inventory(hr.inventory())?};
                save(&dir,&b)?;return Ok(json!({"mode":"audit","reviewed":false,"baseline_written":dir,"can_submit_orders":false}));
            }
            if let Some(path)=&baseline {
                let old:Baseline=read(path)?;
                if serde_json::to_value(&old.plan).unwrap()!=serde_json::to_value(&p).unwrap(){return Err("baseline plan changed".into());}
            }
            if !p.maker.reduce_only {
                tokio::try_join!(m.configure_isolated(&resolved.maker,p.leverage),h.configure_isolated(&resolved.hedge,p.leverage))?;
            }
            let (mr,hr)=tokio::try_join!(m.check_position(&mc,&resolved.maker.symbol),h.check_position(&hc,&resolved.hedge.symbol))?;
            inventory_ok(mr.inventory(),&resolved.maker,d(&p.maker.quantity)?)?;
            inventory_ok(hr.inventory(),&resolved.hedge,d(&p.hedge.quantity)?)?;
            let initial=crate::position_checks::Baseline::read_positions(mr.inventory(),&resolved.maker.symbol,hr.inventory(),&resolved.hedge.symbol)?;
            initial.before_order(d(&p.maker.quantity)?,&p.maker.side,&p.hedge.side,p.maker.reduce_only)?;
            if pointer.exists(){return Err("unresolved native pair; recover path in rust/runs/native-pair-active.json".into());}
            std::fs::create_dir(&dir).map_err(|_|"run directory must be new")?;
            save(&dir.join("plan.json"),&p)?;save(&dir.join("resolved.json"),&resolved)?;
            save(&dir.join("position-baseline.json"),&initial)?;
            std::fs::OpenOptions::new().write(true).create_new(true).open(dir.join("orders.jsonl")).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;
            std::fs::create_dir_all(pointer.parent().unwrap()).map_err(|e|e.to_string())?;
            save(&pointer,&std::fs::canonicalize(&dir).map_err(|e|e.to_string())?)?;
        }else {
            if pointer.exists(){let original:PathBuf=read(&pointer)?;if original!=std::fs::canonicalize(&dir).map_err(|e|e.to_string())?{return Err("recover directory differs from active run".into());}}
        }
        let key=|o:&Order|MarketKey::new(PublicVenue::from_name(&o.venue)?,&o.symbol);
        let mut ms=hub.subscribe(key(&resolved.maker)?).await?;let mut hs=hub.subscribe(key(&resolved.hedge)?).await?;
        let (tx,books)=watch::channel(Books::default());
        feed=Some(tokio::spawn(async move{loop{
            let now=wall_ms();tx.send_replace(Books{maker:ms.snapshot().fresh(now).map(|s|s.quote.clone()),hedge:hs.snapshot().fresh(now).map(|s|s.quote.clone())});
            tokio::select!{r=ms.changed()=>{if r.is_err(){break;}},r=hs.changed()=>{if r.is_err(){break;}},_=tokio::time::sleep(Duration::from_millis(100))=>()}
        }}));
        if parent_control {
            let tx=stop_tx.clone();std::thread::spawn(move||{let mut line=String::new();let _=std::io::stdin().read_line(&mut line);tx.send_replace(true);});
        }
        signal=Some(tokio::spawn(async move{let _=tokio::signal::ctrl_c().await;stop_tx.send_replace(true);}));
        let result=Engine::new(p.clone(),resolved.clone(),[m.clone(),h.clone()],[mc.clone(),hc.clone()],books,stop,dir.clone(),recover).run().await;
        if let Ok(value)=&result {
            let file=dir.join("position-baseline.json");
            if file.exists(){
                let initial:crate::position_checks::Baseline=read(&file)?;
                initial.verify(&p.maker.side,d(value["maker_filled"].as_str().ok_or("maker fill missing")?)?,d(value["hedge_filled"].as_str().ok_or("hedge fill missing")?)?,||async {
                    let (mr,hr)=tokio::try_join!(m.check_position(&mc,&resolved.maker.symbol),h.check_position(&hc,&resolved.hedge.symbol))?;
                    crate::position_checks::Baseline::read_positions(mr.inventory(),&resolved.maker.symbol,hr.inventory(),&resolved.hedge.symbol)
                }).await?;
                let verified=dir.join("position-verified.json");
                if !verified.exists(){save(&verified,&json!({"verified":true}))?;}
                eprintln!("[complete] {} 成交及持倉增量已核對",p.symbol);
            }
        }
        if result.is_ok() && pointer.exists(){std::fs::remove_file(&pointer).map_err(|e|e.to_string())?;}
        result
    }.await;
    if let Some(j) = feed {
        j.abort();
        let _ = j.await;
    }
    if let Some(j) = signal {
        j.abort();
        let _ = j.await;
    }
    let _ = hub.shutdown().await;
    if let Some(x) = mm {
        let _ = x.shutdown().await;
    }
    if let Some(x) = hm {
        let _ = x.shutdown().await;
    }
    let _ = mc.shutdown().await;
    let _ = hc.shutdown().await;
    let _ = mw.await;
    let _ = hw.await;
    result
}
pub async fn inspect(dir: &Path) -> Result<Value> {
    let p = Plan::read(&dir.join("plan.json"))?;
    let mut orders = vec![];
    for e in std::fs::read_dir(dir).map_err(|e| e.to_string())? {
        let path = e.map_err(|e| e.to_string())?.path();
        if path.extension().is_some_and(|e| e == "wal") {
            orders.push(Ledger::open(&path).map_err(|e| e.to_string())?.view());
        }
    }
    Ok(
        json!({"mode":"inspect","schema":p.schema,"orders":orders,"finished":dir.join("finished.json").exists() && (!dir.join("position-baseline.json").exists() || dir.join("position-verified.json").exists()),"orders_settled":dir.join("finished.json").exists(),"can_submit_orders":false}),
    )
}
