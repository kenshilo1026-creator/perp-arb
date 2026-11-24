//! Hedge POST and private confirmation run independently from maker/public readers.
use super::{
    journal::{Entry, Store},
    Result,
};
use crate::d;
use perp_arb_binance_spot::{
    client::Client,
    events::{Event, OrderEvidence},
    fees::Fees,
    journal::Journal,
    stream::Session,
};
use perp_arb_private_events::lookup::Selector;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{watch, Notify};

#[derive(Clone)]
pub struct Stream {
    pub healthy: watch::Receiver<bool>,
    orders: Arc<Mutex<BTreeMap<String, OrderEvidence>>>,
    notify: Arc<Notify>,
}
pub async fn stream(
    client: Client,
    mut stop: watch::Receiver<bool>,
    require_ready: bool,
) -> Result<(Stream, tokio::task::JoinHandle<()>)> {
    let session = if require_ready {
        Some(
            Session::connect(client.clone())
                .await
                .map_err(|e| e.to_string())?,
        )
    } else {
        None
    };
    let (health, healthy) = watch::channel(session.is_some());
    let view = Stream {
        healthy,
        orders: Default::default(),
        notify: Arc::new(Notify::new()),
    };
    let shared = view.clone();
    let task = tokio::spawn(async move {
        let mut current = session;
        loop {
            if *stop.borrow() {
                break;
            }
            if current.is_none() {
                tokio::select! {_=tokio::time::sleep(Duration::from_secs(1))=>(),_=stop.changed()=>break}
                if client.sync_time().await.is_ok() {
                    if let Ok(s) = Session::connect(client.clone()).await {
                        current = Some(s);
                        health.send_replace(true);
                    }
                }
                continue;
            }
            let event = tokio::select! {e=current.as_mut().expect("session").next()=>e,_=stop.changed()=>break};
            match event {
                Ok(Event::Order(e)) => {
                    if let Some(id) = e.found.client_id.clone() {
                        if let Ok(mut orders) = shared.orders.lock() {
                            // Only this engine's identities; unrelated account activity is not a fill source.
                            if id.starts_with("pa") && orders.len() < 10000 {
                                let replace = orders.get(&id).is_none_or(|old| {
                                    e.found.order.cumulative >= old.found.order.cumulative
                                });
                                if replace {
                                    orders.insert(id, *e);
                                }
                            }
                        }
                        shared.notify.notify_waiters();
                    }
                }
                Ok(Event::AccountChanged) => (),
                Err(_) => {
                    health.send_replace(false);
                    current = None;
                    shared.notify.notify_waiters();
                }
            }
        }
        health.send_replace(false);
        if let Some(s) = current {
            let _ = s.close().await;
        }
    });
    Ok((view, task))
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct Timing {
    pub resumed: bool,
    pub source: String,
    pub received_to_dispatch_ms: f64,
    pub dispatch_to_ack_ms: Option<f64>,
    pub dispatch_to_confirmation_ms: f64,
    pub confirmation_source: String,
}
pub trait HedgeTransport: Send + Sync + 'static {
    fn health(&self) -> watch::Receiver<bool>;
    fn account(
        &self,
    ) -> impl std::future::Future<Output = Result<perp_arb_binance_spot::client::Account>> + Send;
    fn sync(&self) -> impl std::future::Future<Output = Result<()>> + Send;
    fn execute(
        &self,
        store: Store,
        directory: PathBuf,
        index: usize,
        received: Instant,
        source: String,
        risk: watch::Sender<bool>,
    ) -> impl std::future::Future<Output = Result<Timing>> + Send;
}
pub struct LiveHedge {
    pub client: Client,
    pub stream: Stream,
}
impl HedgeTransport for LiveHedge {
    fn health(&self) -> watch::Receiver<bool> {
        self.stream.healthy.clone()
    }
    async fn account(&self) -> Result<perp_arb_binance_spot::client::Account> {
        self.client.account().await.map_err(|e| e.to_string())
    }
    async fn sync(&self) -> Result<()> {
        self.client.sync_time().await.map_err(|e| e.to_string())
    }
    async fn execute(
        &self,
        store: Store,
        directory: PathBuf,
        index: usize,
        received: Instant,
        source: String,
        risk: watch::Sender<bool>,
    ) -> Result<Timing> {
        execute(
            self.client.clone(),
            self.stream.clone(),
            store,
            directory,
            index,
            received,
            source,
            risk,
        )
        .await
    }
}
/// At most one POST per persisted intent. A transport error/NotFound never retries it.
/// Recovery may dispatch a reservation whose child journal proves it was never dispatched.
#[allow(clippy::too_many_arguments)] // Explicit per-intent lifecycle context; no global mutable state.
pub async fn execute(
    client: Client,
    stream: Stream,
    store: Store,
    directory: PathBuf,
    index: usize,
    received: Instant,
    source: String,
    risk: watch::Sender<bool>,
) -> Result<Timing> {
    let state = store.state()?;
    let hedge = state
        .hedges
        .get(index)
        .ok_or("missing reserved hedge")?
        .clone();
    let intent = hedge.intent.clone();
    let scope = client.scope();
    let path = directory.join(format!("hedge-{index:05}.jsonl"));
    if hedge.started && !path.is_file() {
        return Err("dispatched hedge child journal missing; never recreate/resubmit it".into());
    }
    let mut journal = tokio::task::spawn_blocking(move || {
        if path.exists() {
            Journal::open(&path, &scope, &intent)
        } else {
            Journal::create(&path, scope, intent)
        }
    })
    .await
    .map_err(|_| "hedge journal worker")?
    .map_err(|e| e.to_string())?;
    let request = journal.request().map_err(|e| e.to_string())?;
    let mut ack = None;
    let mut evidence = None;
    let dispatched;
    let resumed = journal.view().dispatched;
    if !journal.view().dispatched {
        if hedge.started {
            return Err(
                "inconsistent hedge dispatch journals; manual reconciliation required".into(),
            );
        }
        let (j, permit) = tokio::task::spawn_blocking(move || {
            let p = journal.prepare_dispatch();
            (journal, p)
        })
        .await
        .map_err(|_| "hedge journal worker")?;
        journal = j;
        let permit = permit.map_err(|e| e.to_string())?;
        store.record(Entry::HedgeStarted { index }).await?;
        dispatched = Instant::now();
        // Keep POST alive when a WS fill wins. Never cancel a mutating future on timeout/select.
        let c = client.clone();
        let mut post = tokio::spawn(async move { c.submit(permit).await });
        loop {
            if dispatched.elapsed() > Duration::from_secs(1) {
                risk.send_replace(true);
            }
            let private = stream
                .orders
                .lock()
                .map_err(|_| "spot stream poisoned")?
                .get(&hedge.intent.client_id)
                .cloned();
            if let Some(e) = private {
                if request.verify(&e.found).is_err() {
                    risk.send_replace(true);
                    let _ = post.await;
                    return Err("hedge WS identity conflict".into());
                }
                if e.found.order.status.terminal() {
                    evidence = Some((e, "private_ws"));
                }
            }
            tokio::select! {
                result=&mut post=>{
                    if let Ok(Ok(e))=result {ack=Some(dispatched.elapsed().as_secs_f64()*1000.0);
                        if evidence.as_ref().is_none_or(|(old,_)|e.found.order.cumulative>old.found.order.cumulative) {evidence=Some((e,"submit_response"));}
                    } else {risk.send_replace(true);}
                    break;
                },
                _=stream.notify.notified()=>(),
                _=tokio::time::sleep(Duration::from_millis(25))=>(),
            }
        }
    } else {
        dispatched = Instant::now();
        risk.send_replace(true);
        if !hedge.started {
            store.record(Entry::HedgeStarted { index }).await?;
        }
    }
    let receive_to_dispatch = dispatched.saturating_duration_since(received).as_secs_f64() * 1000.0;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut next_query = Instant::now();
    loop {
        let private = stream
            .orders
            .lock()
            .map_err(|_| "spot stream poisoned")?
            .get(&hedge.intent.client_id)
            .cloned();
        if let Some(e) = private {
            request
                .verify(&e.found)
                .map_err(|_| "hedge WS identity conflict")?;
            if evidence.as_ref().is_none_or(|(old, _)| {
                e.found.order.cumulative > old.found.order.cumulative
                    || (e.found.order.cumulative == old.found.order.cumulative
                        && e.found.order.status.terminal())
            }) {
                evidence = Some((e, "private_ws"));
            }
        }
        if let Some((e, confirmation)) = evidence.take() {
            let q = e.found.order.cumulative;
            if q < d(&hedge.filled)? {
                return Err("hedge query regressed persisted fill".into());
            }
            let terminal = e.found.order.status.terminal();
            let confirmed = e
                .received_at
                .saturating_duration_since(dispatched)
                .as_secs_f64()
                * 1000.0;
            let copy = e.clone();
            let (j, r) = tokio::task::spawn_blocking(move || {
                let r = journal.observe(copy);
                (journal, r)
            })
            .await
            .map_err(|_| "hedge journal worker")?;
            journal = j;
            r.map_err(|e| e.to_string())?;
            if terminal {
                // Trade history is authoritative for fees, independently of the fast fill signal.
                let mut by_id = request.clone();
                by_id.selector = Selector::OrderId(e.found.order.key.order_id.clone());
                let mut fees =
                    Fees::new(&request.symbol, &e.found.order.key.order_id, &request.side)
                        .map_err(|e| e.to_string())?;
                for trade in client.trades(&by_id).await.map_err(|e| e.to_string())? {
                    fees.ingest(trade).map_err(|e| e.to_string())?;
                }
                let totals = fees.reconcile(q, e.quote).map_err(|e| e.to_string())?;
                store
                    .record(Entry::Hedged {
                        index,
                        quantity: q.to_string(),
                        terminal: true,
                        base_delta: Some(totals.base_delta.to_string()),
                        fees: totals
                            .commissions
                            .into_iter()
                            .map(|(a, v)| (a, v.to_string()))
                            .collect(),
                    })
                    .await?;
                return Ok(Timing {
                    resumed,
                    source,
                    received_to_dispatch_ms: receive_to_dispatch,
                    dispatch_to_ack_ms: ack,
                    dispatch_to_confirmation_ms: confirmed,
                    confirmation_source: confirmation.into(),
                });
            }
            store
                .record(Entry::Hedged {
                    index,
                    quantity: q.to_string(),
                    terminal: false,
                    base_delta: None,
                    fees: Default::default(),
                })
                .await?;
            risk.send_replace(true);
        }
        if Instant::now() >= deadline {
            return Err("Binance hedge unresolved; original client ID retained; recover this run, never start another".into());
        }
        if Instant::now() >= next_query {
            // REST is fallback/recovery, never the fill trigger prerequisite.
            if let Ok(Some(e)) = client.query(&request).await {
                evidence = Some((e, "rest_lookup"));
            }
            next_query = Instant::now() + Duration::from_secs(1);
        } else {
            tokio::select! {_=stream.notify.notified()=>(),_=tokio::time::sleep(Duration::from_millis(25))=>()}
        }
    }
}
