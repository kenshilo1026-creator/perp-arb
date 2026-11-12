use crate::{venue::Venue, Result, Store};
use perp_arb_core::Decimal;
use perp_arb_execution::durable::single::View;
use perp_arb_private_events::{actor::Coordinator, orders::Order, OrderKey, Status};
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[derive(Debug, Serialize)]
pub struct Report {
    pub state: View,
    pub completed: bool,
    pub submit_ack_ms: Option<f64>,
    pub first_coordinator_fill_ms: Option<f64>,
    pub terminal_lookup_ms: Option<f64>,
    pub submit_error: Option<String>,
    pub last_query_error: Option<String>,
    pub cancel_error: Option<String>,
    pub interrupted: bool,
}
async fn observe(
    store: &Store,
    c: Option<&Coordinator>,
    order: &Order,
    last_revision: &mut Option<u64>,
    start: Instant,
    fill: &mut Option<f64>,
) -> Result<()> {
    let Some(c) = c else {
        return Ok(());
    };
    let view = store.call(|l| Ok(l.view())).await?;
    let Some(id) = view.order_id else {
        return Ok(());
    };
    let key = OrderKey {
        symbol: order.symbol.clone(),
        order_id: id,
    };
    c.register(key.clone(), order.quantity().map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())?;
    let stream = c.subscribe(key).await.map_err(|e| e.to_string())?;
    let s = stream.borrow().clone();
    if *last_revision == Some(s.revision) || s.status == Status::Pending {
        return Ok(());
    }
    *last_revision = Some(s.revision);
    if s.cumulative > Decimal::ZERO && fill.is_none() {
        *fill = Some(s.observed_at.saturating_duration_since(start).as_secs_f64() * 1000.0);
    }
    store.call(move |l| l.observe(&s)).await
}
/// One submit at most, bounded status reconciliation and client-ID-only cancellation.
/// Restarts call this with submit=false. Ctrl+C requests cleanup; it never aborts an in-flight POST.
pub async fn run<V: Venue>(
    store: &Store,
    venue: &V,
    c: Option<&Coordinator>,
    submit: bool,
    wait: Duration,
    mut stop: watch::Receiver<bool>,
) -> Result<Report> {
    let order = store.call(|l| Ok(l.view().order)).await?;
    let result = run_inner(store, venue, c, submit, wait, &mut stop, order.clone()).await;
    if result.is_err()
        && store
            .call(|l| Ok(l.view().dispatched))
            .await
            .unwrap_or(true)
    {
        // Journal/coordinator failure after possible submission: best-effort risk-reducing cancel.
        // A failed journal cannot certify the result, so the original error remains an error.
        let _ = venue.cancel(&order).await;
    }
    result
}
async fn run_inner<V: Venue>(
    store: &Store,
    venue: &V,
    c: Option<&Coordinator>,
    submit: bool,
    wait: Duration,
    stop: &mut watch::Receiver<bool>,
    order: Order,
) -> Result<Report> {
    let start = Instant::now();
    let deadline = tokio::time::Instant::now() + if submit { wait } else { Duration::ZERO };
    let mut ack = None;
    let mut submit_error = None;
    if submit {
        if *stop.borrow() {
            return Err("interrupted before submission".into());
        }
        let permit = store.call(|l| l.dispatch()).await?;
        if *stop.borrow() {
            return Err(
                "interrupted after journal sync, before submission; recover this journal".into(),
            );
        }
        match venue.submit(permit.order()).await {
            Ok(a) => {
                ack = Some(start.elapsed().as_secs_f64() * 1000.0);
                store.call(move |l| l.accepted_result(a)).await?;
            }
            Err(e) => {
                submit_error = Some(e);
                store.call(|l| l.unresolved()).await?;
            }
        }
    }
    let mut view = store.call(|l| Ok(l.view())).await?;
    if !view.dispatched {
        return Err("journal has never submitted; recover cannot initiate a new order".into());
    }
    let mut canceled = false;
    let mut last_query_error;
    let mut cancel_error = None;
    let mut fill = None;
    let mut revision = None;
    let mut terminal = None;
    let mut interrupted = false;
    // At most ten reconciliation rounds after cancellation; unresolved leaves a nonzero exit.
    let mut after_cancel = 0;
    loop {
        interrupted |= *stop.borrow();
        observe(store, c, &order, &mut revision, start, &mut fill).await?;
        if !canceled
            && (interrupted || tokio::time::Instant::now() >= deadline || submit_error.is_some())
        {
            store.call(|l| l.cancel_started()).await?;
            if let Err(e) = venue.cancel(&order).await {
                cancel_error = Some(e);
            }
            canceled = true;
        }
        let t = store.call(|l| l.ticket()).await?;
        let health_before = c.map(|c| {
            let h = c.health();
            (h.instance, h.generation, h.connected, h.fault)
        });
        // Read-only lookup may be canceled at the deadline; the original POST is never aborted.
        let outcome = tokio::select! {
            r=venue.lookup(t.request())=>r,
            _=tokio::time::sleep_until(deadline),if !canceled=>continue,
            r=stop.changed(),if !interrupted=>{interrupted=r.is_err() || *stop.borrow();continue;},
        };
        observe(store, c, &order, &mut revision, start, &mut fill).await?;
        let health_after = c.map(|c| {
            let h = c.health();
            (h.instance, h.generation, h.connected, h.fault)
        });
        if health_before != health_after {
            store.call(|l| l.unresolved()).await?;
        }
        match outcome {
            Ok(outcome) => {
                store.call(move |l| l.lookup(t, outcome)).await?;
                last_query_error = None;
            }
            Err(e) => {
                last_query_error = Some(e);
                store.call(|l| l.unresolved()).await?;
            }
        }
        view = store.call(|l| Ok(l.view())).await?;
        if view.terminal && view.reconciled && !view.faulted {
            terminal = Some(start.elapsed().as_secs_f64() * 1000.0);
            break;
        }
        if canceled {
            after_cancel += 1;
            if after_cancel >= 10 {
                break;
            }
        }
        // Private changes are persisted at the next round; network readers remain independent.
        tokio::select! {_ = tokio::time::sleep(Duration::from_millis(500))=>(),r=stop.changed(),if !interrupted=>{interrupted=r.is_err() || *stop.borrow();}}
    }
    Ok(Report {
        completed: terminal.is_some(),
        state: view,
        submit_ack_ms: ack,
        first_coordinator_fill_ms: fill,
        terminal_lookup_ms: terminal,
        submit_error,
        last_query_error,
        cancel_error,
        interrupted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use perp_arb_execution::durable::single::{new_client_id, Ledger};
    use perp_arb_private_events::{
        lookup::{Found, Outcome, Request, Selector},
        orders::{Accepted, Network, TimeInForce},
        Update,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    struct Mock {
        submitted: AtomicUsize,
        canceled: AtomicUsize,
        unknown: bool,
        lost_ack: bool,
        slow_query: bool,
        remaining: Mutex<usize>,
    }
    impl Venue for Mock {
        async fn submit(&self, _: &Order) -> Result<Accepted> {
            self.submitted.fetch_add(1, Ordering::SeqCst);
            if self.lost_ack {
                Err("Timeout".into())
            } else {
                Ok(Accepted {
                    order_id: "7".into(),
                    cumulative: None,
                })
            }
        }
        async fn cancel(&self, _: &Order) -> Result<()> {
            self.canceled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn lookup(&self, r: &Request) -> Result<Outcome> {
            if self.slow_query && self.canceled.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            if self.unknown {
                return Ok(Outcome::NotFound);
            }
            let mut n = self.remaining.lock().unwrap();
            *n += 1;
            let terminal = self.canceled.load(Ordering::SeqCst) > 0;
            Ok(Outcome::Found(Found {
                scope: r.scope.clone(),
                side: r.side.clone(),
                client_id: match &r.selector {
                    Selector::ClientId(s) => Some(s.clone()),
                    _ => None,
                },
                order: Update {
                    key: OrderKey {
                        symbol: r.symbol.clone(),
                        order_id: "7".into(),
                    },
                    original: r.original,
                    cumulative: perp_arb_core::decimal("0.005").unwrap(),
                    status: if terminal {
                        Status::Canceled
                    } else {
                        Status::PartiallyFilled
                    },
                    event_ms: Some(100),
                },
            }))
        }
    }
    fn mock(unknown: bool, lost_ack: bool) -> Mock {
        Mock {
            submitted: AtomicUsize::new(0),
            canceled: AtomicUsize::new(0),
            unknown,
            lost_ack,
            slow_query: false,
            remaining: Mutex::new(0),
        }
    }
    #[tokio::test]
    async fn slow_read_only_lookup_cannot_delay_deadline_cancel() {
        let (p, l) = ledger();
        let store = Store::start(l);
        let mut v = mock(false, false);
        v.slow_query = true;
        let (_tx, rx) = watch::channel(false);
        let started = Instant::now();
        let report = run(&store, &v, None, true, Duration::from_millis(20), rx)
            .await
            .unwrap();
        assert!(report.completed);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(v.canceled.load(Ordering::SeqCst), 1);
        store.close().await.unwrap();
        std::fs::remove_file(p).unwrap();
    }
    fn ledger() -> (std::path::PathBuf, Ledger) {
        let cid = new_client_id("aster").unwrap();
        let path = std::env::temp_dir().join(format!("{cid}.wal"));
        let o = Order {
            venue: "aster".into(),
            network: Network::Mainnet,
            account: format!("0x{:040x}", 1),
            symbol: "ETHUSDT".into(),
            side: "BUY".into(),
            quantity: "0.01".into(),
            limit_price: "2000".into(),
            tif: TimeInForce::PostOnly,
            reduce_only: false,
            client_id: cid,
            asset: None,
        };
        let l = Ledger::create(&path, o, "25".into()).unwrap();
        (path, l)
    }
    #[tokio::test]
    async fn lost_ack_queries_and_cancels_exact_order_without_resubmitting() {
        let (p, l) = ledger();
        let store = Store::start(l);
        let v = mock(false, true);
        let (_tx, rx) = watch::channel(false);
        let r = run(&store, &v, None, true, Duration::ZERO, rx)
            .await
            .unwrap();
        assert!(r.completed);
        assert_eq!(r.state.cumulative, "0.005");
        assert_eq!(v.submitted.load(Ordering::SeqCst), 1);
        assert_eq!(v.canceled.load(Ordering::SeqCst), 1);
        store.close().await.unwrap();
        std::fs::remove_file(p).unwrap();
    }
    #[tokio::test]
    async fn unknown_order_and_cancel_ack_never_become_zero_fill_success() {
        let (p, l) = ledger();
        let store = Store::start(l);
        let v = mock(true, true);
        let (_tx, rx) = watch::channel(false);
        let r = run(&store, &v, None, true, Duration::ZERO, rx)
            .await
            .unwrap();
        assert!(!r.completed);
        assert!(!r.state.terminal);
        assert_eq!(v.submitted.load(Ordering::SeqCst), 1);
        store.close().await.unwrap();
        let l = Ledger::open(&p).unwrap();
        let store = Store::start(l);
        let (_tx, rx) = watch::channel(false);
        let _ = run(&store, &v, None, false, Duration::ZERO, rx)
            .await
            .unwrap();
        assert_eq!(v.submitted.load(Ordering::SeqCst), 1);
        store.close().await.unwrap();
        std::fs::remove_file(p).unwrap();
    }
    #[tokio::test]
    async fn cancel_before_dispatch_never_submits() {
        let (p, l) = ledger();
        let store = Store::start(l);
        let v = mock(false, false);
        let (_tx, rx) = watch::channel(true);
        assert!(run(&store, &v, None, true, Duration::ZERO, rx)
            .await
            .is_err());
        assert_eq!(v.submitted.load(Ordering::SeqCst), 0);
        store.close().await.unwrap();
        std::fs::remove_file(p).unwrap();
    }
}
