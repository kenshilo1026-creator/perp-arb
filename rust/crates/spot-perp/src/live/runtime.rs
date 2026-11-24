use super::spot::HedgeTransport;
use super::{
    journal::{self, Entry, Store},
    spot, Result,
};
use crate::{
    d,
    policy::{self, Input, Resting, RestingAction},
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use perp_arb_binance_spot::orders::{Intent, Kind};
use perp_arb_core::{
    pricing::{round_step, Direction},
    Decimal as D,
};
use perp_arb_order_test::venue::Venue;
use perp_arb_private_events::{
    actor::Coordinator,
    lookup::{Found, Outcome},
    orders::{Accepted, Order},
    OrderKey, OrderSnapshot,
};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};

pub struct Owner<V: Venue + Send + Sync + 'static, H: HedgeTransport> {
    pub store: Store,
    pub venue: Arc<V>,
    pub coordinator: Coordinator,
    pub hedger: Arc<H>,
    pub books: watch::Receiver<Input>,
    pub directory: PathBuf,
    pub stop: watch::Receiver<bool>,
    pub timings: Vec<spot::Timing>,
    pub progress: Option<super::progress::Progress>,
}
enum Job {
    Submit(Result<Accepted>),
    Query {
        result: Result<Outcome>,
        before_ack: bool,
    },
    Cancel(Result<()>),
    Hedge(Result<spot::Timing>),
}

/// This quantity gate is only the exchange's market lot/notional requirement.
/// The spread gate NEVER applies to a fill that already exists.
pub(super) fn hedge_quantity(state: &journal::State, book: &Input) -> Result<D> {
    let r = &state.config.spot_rules;
    let due = state.due()?;
    if due < D::ZERO {
        return Err("hedge reservations exceed maker fills".into());
    }
    let q = round_step(due, Some(d(&r.step)?), Direction::Down).map_err(|e| e.to_string())?;
    let q = if let Some(max) = &r.maximum {
        q.min(d(max)?)
    } else {
        q
    };
    let price = if state.config.mode == policy::Mode::Open {
        &book.spot.ask
    } else {
        &book.spot.bid
    };
    // When public data is stale, do not block the hedge: exchange filters remain authoritative.
    if q < d(&r.minimum)?
        || (book.spot.age_ms <= 1000
            && q.checked_mul(d(price)?).ok_or("notional overflow")? < d(&r.min_notional)?)
    {
        return Ok(D::ZERO);
    }
    Ok(q)
}
impl<V: Venue + Send + Sync + 'static, H: HedgeTransport> Owner<V, H> {
    fn event(&self, event: &str, message: String, details: serde_json::Value) {
        if let Some(log) = &self.progress {
            log.event(event, message, details);
        }
    }
    pub async fn execute(mut self, recover: bool) -> Result<()> {
        self.progress = Some(
            super::progress::Progress::open(&self.directory)
                .map_err(|_| "cannot open process.jsonl")?,
        );
        let began = Instant::now();
        let result=async {
            if self.store.state()?.finished {return Ok(());}
            if recover {
                self.recover_all().await?;
                if self.store.state()?.settled()? {self.store.record(Entry::Finished).await?;return Ok(());}
                return Err("recovery incomplete; inspect this run, do not start a new run".into());
            }
            let mut gap_since=Instant::now();let mut synced=Instant::now();let mut last_wait=Instant::now()-Duration::from_secs(10);
            loop {
                let state=self.store.state()?;
                let remaining=journal::add(d(&state.config.total)?,-state.filled()?)?;
                if remaining==D::ZERO || *self.stop.borrow() || began.elapsed()>Duration::from_secs(3600) {
                    if !state.settled()? {return Err("remaining exposure requires recovery".into());}
                    self.store.record(Entry::Finished).await?;return Ok(());
                }
                if synced.elapsed()>=Duration::from_secs(600) {self.hedger.sync().await?;synced=Instant::now();}
                if !self.coordinator.health().connected || self.coordinator.health().fault.is_some() || !*self.hedger.health().borrow() {
                    if gap_since.elapsed()>Duration::from_secs(60) {return Err("60s without healthy private/public feeds".into());}
                    if last_wait.elapsed()>=Duration::from_secs(5) {
                        self.event("wait", "等待私人 WS 恢復，暫不掛新單（上限 60 秒）".into(),serde_json::json!({"reason":"private_feed_unhealthy"}));last_wait=Instant::now();
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;continue;
                }
                let mut i=self.books.borrow().clone();i.remaining=remaining.to_string();
                let decision=policy::evaluate_perp_maker(&i)?;
                if let Some(plan)=decision.candidate {
                    // Account reads happen before maker placement, never on the fill -> hedge path.
                    let account=self.hedger.account().await?;
                    let (asset,needed)=if state.config.mode==policy::Mode::Open {("USDT",d(&plan.quantity)?.checked_mul(d(&plan.hedge_price)?).and_then(|v|v.checked_mul(D::new(102,2))).ok_or("balance overflow")?)}else{(state.config.symbol.as_str(),d(&plan.quantity)?)};
                    if !account.can_trade || account.balances.get(asset).map_or(D::ZERO,|b|b.0)<needed {return Err("insufficient Binance hedge balance before maker dispatch".into());}
                    // A balance request may take hundreds of ms: re-evaluate the latest book afterwards.
                    let mut latest=self.books.borrow().clone();latest.remaining=remaining.to_string();
                    let Some(plan)=policy::evaluate_perp_maker(&latest)?.candidate else {continue};
                    let needed=if state.config.mode==policy::Mode::Open {d(&plan.quantity)?.checked_mul(d(&plan.hedge_price)?).and_then(|v|v.checked_mul(D::new(102,2))).ok_or("balance overflow")?}else{d(&plan.quantity)?};
                    if account.balances.get(asset).map_or(D::ZERO,|b|b.0)<needed {return Err("latest maker size exceeds checked hedge balance".into());}
                    if d(&plan.quantity)?>d(&i.remaining)? {return Err("maker exceeds remaining".into());}
                    if let Some(previous)=state.makers.last() {
                        self.event("reprice",format!("前單已確認結束，安排第 {} 張單：價格 {} → {}，重新取得深度數量={}，總目標剩餘={}",state.makers.len()+1,previous.order.limit_price,plan.maker_price,plan.quantity,remaining),serde_json::json!({"attempt":state.makers.len()+1,"previous_price":previous.order.limit_price,"price":plan.maker_price,"quantity":plan.quantity,"remaining":remaining.to_string()}));
                    }
                    self.event("maker-plan",format!("{} {} {} 數量={} 價格={} 預估金額={} {}；Binance 對沖參考={} 不利價差={}%",state.config.venue,state.config.maker_side(),state.config.symbol,plan.quantity,plan.maker_price,d(&plan.quantity)?.checked_mul(d(&plan.maker_price)?).ok_or("notional overflow")?,if state.config.venue=="hyperliquid" {"USDC"}else{"USDT"},plan.hedge_price,(d(&plan.adverse_gap)?*D::from(100)).round_dp(4)),serde_json::json!({"attempt":state.makers.len()+1,"plan":plan,"books":latest}));
                    let order=state.config.order(plan.quantity,plan.maker_price)?;
                    let index=state.makers.len();self.store.record(Entry::Maker{order}).await?;
                    self.manage(index,true).await?;
                    gap_since=Instant::now();
                } else {
                    if last_wait.elapsed()>=Duration::from_secs(5) {
                        self.event("wait",format!("等待合格價差／深度：{}，已等 {} 秒（上限 60 秒）",decision.reason,gap_since.elapsed().as_secs()),serde_json::json!({"reason":decision.reason,"books":i}));last_wait=Instant::now();
                    }
                    if decision.reason=="remainder_below_minimum" {return Err("remaining quantity below exchange minimum; requested total was not increased".into());}
                    if gap_since.elapsed()>=Duration::from_secs(60) {return Err("60s without qualifying gap/depth; stopped without new maker".into());}
                    tokio::select! {
                        changed=self.books.changed()=>{changed.map_err(|_|"public feed stopped")?;},
                        _=self.stop.changed()=>(),
                        _=tokio::time::sleep(Duration::from_millis(100))=>(),
                    }
                }
                if self.store.state()?.makers.len()>=1000 {return Err("1000 maker attempts reached; inspect run".into());}
            }
        }.await;
        if result.is_err() {
            // Also handles disk/coordinator errors. Cancel only this run's exact identities.
            // Recovery is risk reducing: never opens a fresh maker or repairs initial inventory.
            let cleanup = self.recover_all().await;
            if let Err(error) = &cleanup {
                eprintln!(
                    "Rust cleanup incomplete: {error}; recover --run-dir {}",
                    self.directory.display()
                );
            }
            if cleanup.is_ok() && self.store.state()?.settled()? && !self.store.state()?.finished {
                self.store.record(Entry::Finished).await?;
            }
        }
        self.event(
            "complete",
            format!(
                "執行結束：{}",
                if result.is_ok() {
                    "已完成清理／對帳"
                } else {
                    "發生錯誤，請查看總結"
                }
            ),
            serde_json::json!({"ok":result.is_ok()}),
        );
        if let Some(log) = self.progress.take() {
            log.finish().await;
        }
        let timing = serde_json::json!({"timing_kind":"local_monotonic_not_exchange_fill_difference","hedges":self.timings});
        println!("{timing}");
        if std::fs::write(self.directory.join("timing.json"), format!("{timing}\n")).is_err() {
            eprintln!("cannot write timing.json");
        }
        result
    }
    async fn recover_all(&mut self) -> Result<()> {
        let state = self.store.state()?;
        if state.finished {
            return Ok(());
        }
        // Terminal entries are historical. Fresh exact lookup of every submitted maker precedes completion.
        for (index, m) in state.makers.iter().enumerate() {
            if m.started {
                self.manage(index, false).await?;
            }
        }
        if !self.store.state()?.settled()? {
            return Err(
                "unresolved hedge or below-minimum residual; manual review required".into(),
            );
        }
        Ok(())
    }
    async fn bind(&self, index: usize, id: String) -> Result<watch::Receiver<OrderSnapshot>> {
        let state = self
            .store
            .record(Entry::Bound {
                index,
                id: id.clone(),
            })
            .await?;
        let o = &state.makers[index].order;
        let key = OrderKey {
            symbol: o.symbol.clone(),
            order_id: id,
        };
        self.coordinator
            .register(key.clone(), d(&o.quantity)?)
            .await
            .map_err(|e| e.to_string())?;
        self.coordinator
            .subscribe(key)
            .await
            .map_err(|e| e.to_string())
    }
    async fn persist_found(&self, index: usize, f: Found) -> Result<bool> {
        let state = self.store.state()?;
        let m = &state.makers[index];
        m.order
            .request()
            .map_err(|e| e.to_string())?
            .verify(&f)
            .map_err(|_| "maker lookup identity conflict")?;
        if m.id.as_ref().is_some_and(|id| id != &f.order.key.order_id) {
            return Err("maker order ID changed".into());
        }
        if f.order.cumulative < d(&m.filled)? {
            return Ok(false);
        }
        if m.id.is_none() {
            self.store
                .record(Entry::Bound {
                    index,
                    id: f.order.key.order_id.clone(),
                })
                .await?;
        }
        let terminal = f.order.status.terminal();
        self.store
            .record(Entry::Fill {
                index,
                quantity: f.order.cumulative.to_string(),
                terminal,
            })
            .await?;
        Ok(terminal)
    }
    async fn ws_fill(&self, index: usize, s: &OrderSnapshot) -> Result<bool> {
        let state = self.store.state()?;
        let m = &state.makers[index];
        if s.scope != m.order.request().map_err(|e| e.to_string())?.scope
            || s.key.symbol != m.order.symbol
            || m.id.as_ref() != Some(&s.key.order_id)
            || s.original != d(&m.order.quantity)?
        {
            return Err("maker WS identity conflict".into());
        }
        if s.cumulative > d(&m.filled)? {
            self.store
                .record(Entry::Fill {
                    index,
                    quantity: s.cumulative.to_string(),
                    terminal: false,
                })
                .await?;
            return Ok(true);
        }
        Ok(false)
    }
    async fn launch_hedge(
        &self,
        tasks: &mut FuturesUnordered<JoinHandle<Job>>,
        received: Instant,
        source: &str,
        recovery: bool,
        risk: watch::Sender<bool>,
    ) -> Result<bool> {
        let state = self.store.state()?;
        let index = if let Some((index, _)) = state
            .hedges
            .iter()
            .enumerate()
            .find(|(_, h)| !h.terminal || h.base_delta.is_none())
        {
            if !recovery {
                return Ok(false);
            }
            index
        } else {
            let quantity = hedge_quantity(&state, &self.books.borrow())?;
            if quantity == D::ZERO {
                return Ok(false);
            }
            let index = state.hedges.len();
            let intent = Intent {
                symbol: format!("{}USDT", state.config.symbol),
                side: state.config.hedge_side().into(),
                quantity: quantity.to_string(),
                client_id: perp_arb_execution::durable::single::new_client_id("aster")
                    .map_err(|e| e.to_string())?,
                kind: Kind::Market,
            };
            self.store.record(Entry::Hedge { intent }).await?;
            index
        };
        let hedger = self.hedger.clone();
        let store = self.store.clone();
        let directory = self.directory.clone();
        let source = source.to_owned();
        let log_source = source.clone();
        let quantity = self.store.state()?.hedges[index].intent.quantity.clone();
        tasks.push(tokio::spawn(async move {
            Job::Hedge(
                hedger
                    .execute(store, directory, index, received, source, risk)
                    .await,
            )
        }));
        self.event(
            "hedge",
            format!(
                "Maker 已成交，啟動 Binance 市價對沖 #{}，數量={}（來源={}）",
                index + 1,
                quantity,
                log_source
            ),
            serde_json::json!({"hedge_index":index,"source":log_source,"quantity":quantity}),
        );
        Ok(true)
    }
    async fn manage(&mut self, index: usize, submit: bool) -> Result<()> {
        let order = self.store.state()?.makers[index].order.clone();
        let mut tasks = FuturesUnordered::new();
        let outcome = self.manage_inner(index, &order, submit, &mut tasks).await;
        // Never drop an in-flight mutating future. Its persisted identity remains queryable on error.
        while tasks.next().await.is_some() {}
        outcome
    }
    async fn manage_inner(
        &mut self,
        index: usize,
        order: &Order,
        submit: bool,
        tasks: &mut FuturesUnordered<JoinHandle<Job>>,
    ) -> Result<()> {
        let start = Instant::now();
        let mut source = "recovery";
        let mut received = start;
        let mut watch = None;
        let mut submit_pending = false;
        let mut query_pending = false;
        let mut cancel_pending = false;
        let mut hedge_pending = false;
        let mut cancel_at = None;
        let mut last_query = if submit {
            start
        } else {
            start - Duration::from_secs(2)
        };
        let mut terminal = false;
        let mut stop = !submit;
        let mut stop_reason = if submit { "" } else { "recovery" }.to_owned();
        let mut stopping_since: Option<Instant> = None;
        let mut failure = None;
        let mut hedge_failures = 0;
        let mut holding_stale_maker = false;
        let mut health = self.coordinator.watch_health();
        let mut spot_health = self.hedger.health();
        let (risk_tx, mut risk) = watch::channel(false);
        if submit {
            self.store.record(Entry::Started { index }).await?;
            let v = self.venue.clone();
            let o = order.clone();
            submit_pending = true;
            tasks.push(tokio::spawn(async move { Job::Submit(v.submit(&o).await) }));
            self.event(
                "maker-submit",
                format!(
                    "掛單 #{}：{} {} {} quantity={} price={}",
                    index + 1,
                    order.venue,
                    order.side,
                    order.symbol,
                    order.quantity,
                    order.limit_price
                ),
                serde_json::json!({"index":index,"client_id":order.client_id}),
            );
        }
        if let Some(id) = self.store.state()?.makers[index].id.clone() {
            watch = Some(self.bind(index, id).await?);
        }
        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        loop {
            if let Some(w) = &mut watch {
                let s = w.borrow_and_update().clone();
                if self.ws_fill(index, &s).await? {
                    received = s.observed_at;
                    source = "private_ws";
                }
                if !terminal && s.status.terminal() {
                    last_query = Instant::now() - Duration::from_secs(5);
                }
            }
            if !hedge_pending && hedge_failures == 0 {
                hedge_pending = self
                    .launch_hedge(tasks, received, source, !submit, risk_tx.clone())
                    .await?;
            }
            let state = self.store.state()?;
            let hard_reason = if *risk.borrow() {
                Some("hedge_uncertain")
            } else if *self.stop.borrow() {
                Some("user_stop")
            } else if !self.coordinator.health().connected
                || self.coordinator.health().fault.is_some()
            {
                Some("maker_private_unhealthy")
            } else if !*spot_health.borrow() {
                Some("hedge_private_unhealthy")
            } else if start.elapsed() > Duration::from_secs(900) {
                Some("maker_timeout")
            } else {
                None
            };
            if !stop {
                if let Some(reason) = hard_reason {
                    stop = true;
                    stop_reason = reason.into();
                }
            }
            let mut input = self.books.borrow().clone();
            // Evaluate only resting price safety. A tiny remaining quantity must not disable cancellation.
            input.remaining = state.config.total.clone();
            input.resting = Some(Resting {
                maker: state.config.venue()?,
                price: order.limit_price.clone(),
                quantity: order.quantity.clone(),
            });
            // A quote/chase decision is re-evaluated until cancel is actually dispatched.
            // A transient price during POST must not permanently latch a future cancellation.
            let quote_reason = match policy::evaluate_perp_maker(&input) {
                Ok(decision) => {
                    let holding = decision.reason == "maker_quote_stale_hold";
                    if holding && !holding_stale_maker && !stop && !terminal {
                        self.event("maker-hold",format!("保留掛單 #{}：Maker 行情暫舊，原單與新鮮對沖報價的價差仍合格；暫停追價。{}",index+1,quote_context(&input,order)),serde_json::json!({"index":index,"reason":decision.reason,"books":input}));
                    }
                    holding_stale_maker = holding;
                    match decision.resting_action {
                        Some(RestingAction::Keep) => None,
                        Some(RestingAction::CancelThenReprice) => Some("best_price_changed"),
                        Some(RestingAction::CancelForGap) => Some("adverse_gap"),
                        Some(RestingAction::CancelForQuote) => Some("stale_quote"),
                        None => Some("missing_resting_decision"),
                    }
                }
                Err(_) => Some("invalid_quote_or_policy"),
            };
            if stop {
                stopping_since.get_or_insert_with(Instant::now);
            }
            if !submit_pending
                && (stop || quote_reason.is_some())
                && !terminal
                && !cancel_pending
                && cancel_at.is_none_or(|t: Instant| t.elapsed() >= Duration::from_secs(5))
            {
                if !stop {
                    stop_reason = quote_reason.unwrap_or("unknown").into();
                    stop = true;
                }
                let v = self.venue.clone();
                let o = order.clone();
                cancel_pending = true;
                cancel_at = Some(Instant::now());
                tasks.push(tokio::spawn(async move { Job::Cancel(v.cancel(&o).await) }));
                self.event("cancel",format!("撤單 #{}：原因={} 原價={}；{}",index+1,stop_reason,order.limit_price,quote_context(&input,order)),serde_json::json!({"index":index,"reason":stop_reason,"books":input,"maker_price":order.limit_price}));
            }
            // No polling timer before a WS fill. REST remains periodic assurance/recovery only.
            let query_interval = if stop || watch.is_none() {
                Duration::from_secs(1)
            } else {
                Duration::from_secs(5)
            };
            if !query_pending && (!terminal || !submit) && last_query.elapsed() >= query_interval {
                let v = self.venue.clone();
                let r = order.request().map_err(|e| e.to_string())?;
                query_pending = true;
                last_query = Instant::now();
                let before_ack = submit_pending;
                tasks.push(tokio::spawn(async move {
                    Job::Query {
                        result: v.lookup(&r).await,
                        before_ack,
                    }
                }));
            }
            if terminal && !submit_pending && !hedge_pending && !cancel_pending && !query_pending {
                if let Some(error) = failure {
                    return Err(error);
                }
                if state.due()? > D::ZERO {
                    return Err(format!("unhedged residual {} below market lot/notional or unresolved; recover this run",state.due()?));
                }
                if state
                    .hedges
                    .iter()
                    .any(|h| !h.terminal || h.base_delta.is_none())
                {
                    return Err("hedge unresolved; recover this run".into());
                }
                self.event("maker-terminal",format!("掛單 #{} 已確認結束，累計成交={}；已完成對沖核對，才可安排下一張單",index+1,state.makers[index].filled),serde_json::json!({"index":index,"filled":state.makers[index].filled,"reason":stop_reason}));
                return Ok(());
            }
            if stopping_since.is_some_and(|t| t.elapsed() > Duration::from_secs(90)) {
                return Err("90s cleanup deadline; order identities retained for recovery".into());
            }
            tokio::select! {
                _=ticker.tick()=>(),
                _=self.books.changed()=>(),
                _=health.changed()=>(),
                _=spot_health.changed()=>(),
                _=risk.changed(),if !*risk.borrow()=>(),
                _=self.stop.changed(),if !*self.stop.borrow()=>(),
                _=async {if let Some(w)=&mut watch {let _=w.changed().await;}else{std::future::pending::<()>().await}}=>(),
                job=tasks.next(),if !tasks.is_empty()=>{
                    match job.ok_or("task queue ended")?.map_err(|_|"execution task failed")? {
                        Job::Submit(result)=>{
                            submit_pending=false;
                            match result {
                                Ok(ack)=>{self.event("maker-ack",format!("掛單 #{} 已確認 order_id={}",index+1,ack.order_id),serde_json::json!({"index":index,"order_id":ack.order_id}));watch=Some(self.bind(index,ack.order_id).await?);
                                    last_query=Instant::now();
                                    if let Some(q)=ack.cumulative {let old=self.store.state()?.makers[index].filled.clone();if d(&q)?>d(&old)? {
                                        received=Instant::now();source="submit_response";self.store.record(Entry::Fill{index,quantity:q,terminal:false}).await?;
                                    }}
                                },
                                Err(_)=>{stop=true;stop_reason="submit_uncertain".into();failure=Some("maker submit result uncertain; reconciled by exact client ID".into());}
                            }
                        },
                        Job::Query{result,before_ack}=>{
                            query_pending=false;
                            if let Ok(Outcome::Found(f))=result {
                                let id=f.order.key.order_id.clone();let changed=f.order.cumulative>d(&self.store.state()?.makers[index].filled)?;
                                terminal=self.persist_found(index,f).await?;
                                if watch.is_none() {watch=Some(self.bind(index,id).await?);}
                                if changed {received=Instant::now();source="rest_lookup";}
                            } else if !before_ack && !submit_pending {
                                stop=true;stop_reason="maker_lookup_unconfirmed".into();
                                self.event("lookup-warning",format!("掛單 #{} 查單未能確認，準備撤回並核對原單",index+1),serde_json::json!({"index":index,"not_found":matches!(result,Ok(Outcome::NotFound))}));
                            }
                        },
                        Job::Cancel(result)=>{cancel_pending=false;last_query=Instant::now()-Duration::from_secs(2);
                            self.event("cancel-result",format!("撤單 #{} {}；繼續查單確認最終成交量",index+1,if result.is_ok(){"回覆成功"}else{"回覆失敗／不明"}),serde_json::json!({"index":index,"ok":result.is_ok()}));
                        },
                        Job::Hedge(result)=>{
                            hedge_pending=false;
                            match result {
                                Ok(t)=>{
                                    self.event("hedge-confirmed",format!("Binance 對沖已確認，來源={}",t.confirmation_source),serde_json::json!({"confirmation_source":t.confirmation_source}));
                                    self.timings.push(t);
                                    if let Some(h)=self.store.state()?.hedges.last() {if h.terminal && d(&h.filled)?<d(&h.intent.quantity)? {hedge_failures+=1;stop=true;stop_reason="hedge_incomplete".into();failure=Some("market hedge ended partially filled; cancel maker and reconcile remainder".into());}}
                                },
                                Err(error)=>{hedge_failures+=1;stop=true;stop_reason="hedge_incomplete".into();failure=Some(error);}
                            }
                        },
                    }
                }
            }
        }
    }
}

fn quote_context(input: &Input, order: &Order) -> String {
    let (best, hedge) = if order.side == "BUY" {
        (&input.perp.bid, &input.spot.bid)
    } else {
        (&input.perp.ask, &input.spot.ask)
    };
    let gap = d(&order.limit_price).ok().and_then(|m| {
        d(hedge).ok().and_then(|h| {
            let diff = if order.side == "BUY" {
                m.checked_sub(h)
            } else {
                h.checked_sub(m)
            };
            diff.and_then(|v| v.checked_div(m))
                .and_then(|v| v.checked_mul(D::from(100)))
        })
    });
    format!(
        "最新 maker={} hedge={} gap={}% 上限={}%；行情 age spot={}ms perp={}ms",
        best,
        hedge,
        gap.map(|v| v.round_dp(4).to_string())
            .unwrap_or_else(|| "unknown".into()),
        d(&input.max_gap)
            .map(|v| (v * D::from(100)).to_string())
            .unwrap_or_else(|_| "unknown".into()),
        input.spot.age_ms,
        input.perp.age_ms
    )
}
