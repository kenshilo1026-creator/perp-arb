//! Optional WS-confirmed hedge lane. No REST or UI request precedes dispatch.
//! This remains opt-in pending real private-trade contract validation.
use super::{Plan, Resolved};
use crate::{latency::Trace, owner::Owner, ws_validation::Validator, Result};
use perp_arb_core::decimal;
use perp_arb_execution::durable::Target;
use perp_arb_order_test::venue::{LiveVenue, Venue};
use serde_json::json;
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::{watch, Mutex, Notify};

pub(super) struct Worker {
    pub owner: Arc<Owner>,
    pub venue: Arc<LiveVenue>,
    pub resolved: Resolved,
    pub plan: Plan,
    pub validation: Arc<Mutex<Validator>>,
    pub epoch: u64,
    pub wake: Arc<Notify>,
    pub lane: Arc<Mutex<()>>,
    pub trace: Arc<Trace>,
    pub stop: watch::Receiver<bool>,
    pub failure: watch::Sender<Option<String>>,
}
impl Worker {
    pub async fn run(mut self) -> Result<()> {
        let mut seen = BTreeSet::new();
        let result: Result<()> = async {
            loop {
                tokio::select! {_=self.stop.changed()=>break,_=self.wake.notified()=>()}
                if *self.stop.borrow() {
                    break;
                }
                if self.failure.borrow().is_some() {
                    break;
                }
                process(
                    &self.owner,
                    self.venue.as_ref(),
                    &self.resolved,
                    &self.plan,
                    &self.validation,
                    self.epoch,
                    &self.lane,
                    &self.trace,
                    &mut seen,
                )
                .await?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = &result {
            let _ = self.owner.gap(Target::Maker).await;
            let _ = self.failure.send(Some(e.clone()));
        }
        result
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process<V: Venue>(
    owner: &Owner,
    venue: &V,
    r: &Resolved,
    p: &Plan,
    validation: &Mutex<Validator>,
    epoch: u64,
    lane: &Mutex<()>,
    trace: &Trace,
    seen: &mut BTreeSet<String>,
) -> Result<()> {
    let progress = owner.progress().await?;
    let Some(rfq) = progress.maker.order_id else {
        return Ok(());
    };
    // Serialize epoch validation with the durable application. No network I/O
    // while this lock is held. Gap/conflict permanently disarms this run's fast lane.
    let validator = validation.lock().await;
    let trades = validator.fast_trades(epoch, &progress.maker.command, &rfq, &p.instrument)?;
    let mut applied = Vec::new();
    for trade in trades {
        let id = trade.trade_id.as_ref().ok_or("trade ID missing")?.clone();
        if seen.contains(&id) {
            continue;
        }
        trace.mark("maker_ws_commit_begin", &id, json!({"rfq":rfq}));
        owner.apply_ws_trade(trade).await?;
        trace.mark("maker_ws_commit_done", &id, json!({"rfq":rfq}));
        seen.insert(id.clone());
        applied.push(id);
    }
    drop(validator);
    if applied.is_empty() {
        return Ok(());
    }
    // Only the send lane is serialized with the REST runner. Its slow maker
    // lookups/cancellation never hold this lane and cannot delay a first WS hedge.
    let _lane = lane.lock().await;
    let progress = owner.progress().await?;
    if progress.faulted {
        return Err("fast maker ledger fault".into());
    }
    // Retain known obligations even if a gap occurred after the verified event.
    let pending = progress
        .intents
        .iter()
        .find(|i| i.client_id.is_none())
        .map(|i| i.intent.clone());
    let intent = match pending {
        Some(i) => Some(i),
        None => owner.reserve_confirmed_hedge().await?,
    };
    if let Some(intent) = intent {
        let mut order = r.order.clone();
        order.quantity = intent.quantity.to_string();
        trace.mark("hedge_sources", &intent.id, json!({"trade_ids":applied}));
        crate::execution_io::dispatch_hedge_traced(
            owner,
            venue,
            intent.id,
            order,
            &r.metadata.limit,
            decimal(&p.hedge.max_notional_usd).map_err(|e| e.to_string())?,
            Some(trace),
            applied.first().map(String::as_str),
        )
        .await?;
    }
    Ok(())
}
