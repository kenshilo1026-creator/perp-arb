//! WS and REST run concurrently. Slow REST must never delay a known fill.
use crate::{latency::Trace, Result};
use perp_arb_order_test::{venue::Venue, Store};
use perp_arb_private_events::{actor::Coordinator, OrderKey, OrderSnapshot, Status};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{watch, Notify},
    task::JoinHandle,
};

#[allow(clippy::too_many_arguments)] // One order's independent transport, journal and control channels.
pub async fn run<V: Venue + Send + Sync + 'static>(
    store: Arc<Store>,
    venue: Arc<V>,
    c: Coordinator,
    submit: bool,
    mut cancel: watch::Receiver<bool>,
    wake: Arc<Notify>,
    trace: Arc<Trace>,
    seconds: u64,
) -> Result<()> {
    let order = store.call(|l| Ok(l.view().order)).await?;
    let mut query: Option<JoinHandle<Result<()>>> = None;
    let mut cancel_job: Option<JoinHandle<Result<()>>> = None;
    let result=async {
        if submit {
            let permit=store.call(|l|l.dispatch()).await?;
            trace.mark("submit_call",&order.client_id,serde_json::json!({"venue":order.venue,"side":order.side,"quantity":order.quantity,"tif":order.tif}));
            match venue.submit(permit.order()).await {
                Ok(a)=>{store.call(move|l|l.accepted_result(a)).await?;},
                Err(e)=>{eprintln!("[submit-unknown] {} {}: {e}; 查單，不重送",order.venue,order.client_id);store.call(|l|l.unresolved()).await?;}
            }
            trace.mark("submit_return",&order.client_id,serde_json::json!({}));
            wake.notify_one();
        }
        if !store.call(|l|Ok(l.view().dispatched)).await? {return Ok(());}
        let start=Instant::now();
        let mut rx:Option<watch::Receiver<OrderSnapshot>>=None;
        let mut revision=None;
        let mut next_query=Instant::now();
        let mut next_cancel=Instant::now();
        loop {
            let view=store.call(|l|Ok(l.view())).await?;
            if view.faulted{return Err("order ledger faulted; exposure retained".into());}
            if rx.is_none() {
                if let Some(id)=view.order_id {
                    let key=OrderKey{symbol:order.symbol.clone(),order_id:id};
                    c.register(key.clone(),order.quantity().map_err(|e|e.to_string())?).await.map_err(|e|e.to_string())?;
                    rx=Some(c.subscribe(key).await.map_err(|e|e.to_string())?);
                }
            }
            if let Some(rx)=rx.as_mut() {
                let s=rx.borrow_and_update().clone();
                if revision!=Some(s.revision) && s.status!=Status::Pending {
                    revision=Some(s.revision);
                    trace.mark_at(trace.at(s.observed_at),"private_fill_observed",&order.client_id,serde_json::json!({"filled":s.cumulative.to_string(),"exchange_ms":s.event_ms,"requires_reconciliation":s.requires_reconciliation}));
                    store.call(move|l|l.observe(&s)).await?;
                    wake.notify_one();
                }
            }
            let current=store.call(|l|Ok(l.view())).await?;
            if current.terminal && current.reconciled && !current.faulted {return Ok(());}
            if query.as_ref().is_some_and(|j|j.is_finished()) {
                let r=query.take().unwrap().await.map_err(|_|"lookup worker failed")?;
                if let Err(e)=r {trace.mark("lookup_error",&order.client_id,serde_json::json!({"error":e}));}
                next_query=Instant::now()+Duration::from_millis(500);wake.notify_one();
            }
            if cancel_job.as_ref().is_some_and(|j|j.is_finished()) {
                let r=cancel_job.take().unwrap().await.map_err(|_|"cancel worker failed")?;
                eprintln!("[cancel-result] {} {}: {}；查證最終成交量",order.venue,order.client_id,if r.is_ok(){"ack"}else{"unknown"});
                next_cancel=Instant::now()+Duration::from_secs(2);next_query=Instant::now();
            }
            if query.is_none() && Instant::now()>=next_query {
                let t=store.call(|l|l.ticket()).await?;
                let v=venue.clone();let st=store.clone();let w=wake.clone();
                query=Some(tokio::spawn(async move {
                    let outcome=v.lookup(t.request()).await;
                    let r=match outcome {Ok(o)=>st.call(move|l|l.lookup(t,o)).await,Err(e)=>Err(e)};
                    w.notify_one();r
                }));
            }
            if (*cancel.borrow() || start.elapsed()>Duration::from_secs(seconds.saturating_sub(5)))
                && cancel_job.is_none() && Instant::now()>=next_cancel && !view.terminal {
                store.call(|l|l.cancel_started()).await?;
                let v=venue.clone();let o=order.clone();
                eprintln!("[cancel] {} {} {}",o.venue,o.side,o.client_id);
                cancel_job=Some(tokio::spawn(async move{v.cancel(&o).await}));
            }
            if start.elapsed()>Duration::from_secs(seconds) {return Err("order reconciliation timed out; recover same run, never resend".into());}
            // The periodic branch only checks completed REST tasks/deadlines. WS wakes immediately.
            tokio::select! {
                _=async {if let Some(rx)=rx.as_mut(){let _=rx.changed().await;}else{std::future::pending::<()>().await;}}=>(),
                _=cancel.changed(),if !*cancel.borrow()=>(),
                _=tokio::time::sleep(Duration::from_millis(20))=>(),
            }
        }
    }.await;
    if let Some(q) = query {
        q.abort();
        let _ = q.await;
    }
    if let Some(c) = cancel_job {
        let _ = c.await;
    }
    if result.is_err() {
        let _ = venue.cancel(&order).await;
    }
    wake.notify_one();
    result
}
