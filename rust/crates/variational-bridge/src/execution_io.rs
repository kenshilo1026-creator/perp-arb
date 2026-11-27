//! Explicit library send path. Uses existing native Aster/Hyperliquid Venue
//! transports and the extension; no broker fallback, retry of unknown POST, or
//! automatic mutation opt-in. Caller owns live monitors and the account locks.
use crate::{
    native::Instrument,
    owner::{MakerRiskCheck, Owner, Progress},
    Bridge, Result,
};
use perp_arb_core::{decimal, instruments::Instrument as Limits, Decimal};
use perp_arb_execution::durable::Target;
use perp_arb_order_test::venue::Venue;
use perp_arb_private_events::{
    orders::{Order, TimeInForce},
    OrderKey, OrderSnapshot, Scope, Status,
};
use std::time::Instant;

pub async fn submit_maker(
    owner: &Owner,
    bridge: &Bridge,
    check: MakerRiskCheck,
    instrument: Instrument,
) -> Result<Progress> {
    let view = owner.progress().await?.maker;
    instrument.validate(&view.command.symbol)?;
    let health = bridge.health().borrow().clone();
    if !health.private_connected
        || !health.command_connected
        || !health.native_protocol
        || !health.native_trading_enabled
        || health.fault.is_some()
    {
        return Err("native maker transport not explicitly enabled and healthy".into());
    }
    let permit = owner.prepare_maker(check).await?;
    eprintln!(
        "[maker-submit] variational {} {} quantity={} price={}",
        view.command.side, view.command.symbol, view.command.quantity, view.command.limit_price
    );
    match bridge.submit_native(permit, instrument).await {
        Ok(submission) => {
            let p = owner.record_maker_submission(submission).await?;
            eprintln!(
                "[maker-ack] order_id={}",
                p.maker
                    .order_id
                    .as_deref()
                    .unwrap_or("pending exact lookup")
            );
            Ok(p)
        }
        Err(error) => {
            owner.gap(Target::Maker).await?;
            Err(format!(
                "maker submission not confirmed; no automatic re-click; inspect/recover persisted receipt: {error}"
            ))
        }
    }
}
pub async fn recover_maker_identity(owner: &Owner, bridge: &Bridge) -> Result<Progress> {
    let c = owner.progress().await?.maker.command;
    let receipt = bridge.recover_native(c.account, c.request_id).await?;
    owner.recover_maker(receipt).await
}
pub async fn reconcile_maker(owner: &Owner, bridge: &Bridge) -> Result<Progress> {
    let ticket = owner.begin_lookup().await?;
    let crate::ReadRequest::NativeLookup { account, order_id } = ticket.request() else {
        return Err("exact lookup required".into());
    };
    match bridge.native_query(Some(account), Some(order_id)).await {
        Ok(query) => owner.apply_lookup(ticket, query).await,
        Err(error) => {
            owner.gap(Target::Maker).await?;
            Err(error)
        }
    }
}
/// The socket consumer continues observing into `validator` during REST I/O.
/// A WS candidate triggers an exact RFQ query; it never directly authorizes a
/// hedge. Always persist authenticated REST fills, including on WS mismatch.
pub async fn reconcile_maker_with_ws(
    owner: &Owner,
    bridge: &Bridge,
    validator: &tokio::sync::Mutex<crate::ws_validation::Validator>,
) -> Result<(Progress, usize)> {
    let epoch = validator.lock().await.epoch();
    let command = owner.progress().await?.maker.command;
    let ticket = owner.begin_lookup().await?;
    let crate::ReadRequest::NativeLookup { account, order_id } = ticket.request() else {
        return Err("exact lookup required".into());
    };
    let query = match bridge
        .native_query(Some(account), Some(order_id.clone()))
        .await
    {
        Ok(query) => query,
        Err(error) => {
            owner.gap(Target::Maker).await?;
            return Err(error);
        }
    };
    apply_maker_with_ws(owner, ticket, query, validator, epoch, &command, &order_id).await
}
pub(crate) async fn apply_maker_with_ws(
    owner: &Owner,
    ticket: crate::owner::MakerLookup,
    query: crate::native::Query,
    validator: &tokio::sync::Mutex<crate::ws_validation::Validator>,
    epoch: u64,
    command: &perp_arb_execution::durable::browser::Command,
    order_id: &str,
) -> Result<(Progress, usize)> {
    let verification = validator
        .lock()
        .await
        .verify(epoch, &query, command, order_id);
    let progress = owner.apply_lookup(ticket, query).await?;
    match verification {
        Ok(count) => Ok((progress, count)),
        Err(error) => {
            owner.gap(Target::Maker).await?;
            Err(format!(
                "REST fills retained; WS verification failed: {error}"
            ))
        }
    }
}
pub async fn cancel_maker(owner: &Owner, bridge: &Bridge) -> Result<()> {
    let permit = owner.prepare_cancel().await?;
    eprintln!("[cancel] Variational 撤回未成交掛單");
    bridge.cancel_native(permit).await?;
    eprintln!("[cancel-ack] 撤單已回覆；查證最終成交量中");
    // Already persisted CancelStarted. ACK does not release remaining size and
    // does not prevent the owner hedging subsequently confirmed maker fills.
    Ok(())
}
pub async fn reconcile_hedge<V: Venue>(owner: &Owner, venue: &V, id: String) -> Result<Progress> {
    let ticket = owner.begin_hedge_lookup(id.clone()).await?;
    match venue.lookup(ticket.request()).await {
        Ok(outcome) => owner.apply_hedge_lookup(ticket, outcome).await,
        Err(error) => {
            owner.gap(Target::Hedge(id)).await?;
            Err(error)
        }
    }
}
/// One IOC or Aster MARKET hedge attempt. The exact network/asset/price/reduce-only/client ID is
/// persisted before POST. Incomplete/unknown outcomes remain reserved; a caller
/// may reserve only a proven terminal residual for a subsequent attempt.
pub async fn dispatch_hedge<V: Venue>(
    owner: &Owner,
    venue: &V,
    id: String,
    order: Order,
    limits: &Limits,
    cap: Decimal,
) -> Result<Progress> {
    dispatch_hedge_traced(owner, venue, id.clone(), order, limits, cap, None, None).await?;
    reconcile_hedge(owner, venue, id).await
}
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_hedge_traced<V: Venue>(
    owner: &Owner,
    venue: &V,
    id: String,
    order: Order,
    limits: &Limits,
    cap: Decimal,
    trace: Option<&crate::latency::Trace>,
    source_trade: Option<&str>,
) -> Result<Progress> {
    let mark = |stage: &str, data: serde_json::Value| {
        if let Some(t) = trace {
            t.mark(stage, &id, data);
        }
    };
    let market = order.venue == "aster" && order.tif == TimeInForce::Market;
    if order.tif != TimeInForce::Ioc && !market {
        return Err("hedge requires IOC or Aster MARKET".into());
    }
    let q = order.quantity().map_err(|e| e.to_string())?;
    let p = order.price().map_err(|e| e.to_string())?;
    if (!market && limits.price_error(p).map_err(|e| e.to_string())?.is_some())
        || limits
            .size_error(q, Some(p))
            .map_err(|e| e.to_string())?
            .is_some()
    {
        return Err("hedge does not satisfy resolved instrument constraints".into());
    }
    mark(
        "hedge_wal_begin",
        serde_json::json!({"source_trade":source_trade}),
    );
    let permit = owner.prepare_wire_hedge(id.clone(), order, cap).await?;
    mark("hedge_wal_done", serde_json::json!({}));
    let order = permit
        .wire_order()
        .ok_or("complete wire order missing")?
        .clone();
    mark(
        "hedge_submit_call",
        serde_json::json!({"source_trade":source_trade,"quantity":order.quantity}),
    );
    let accepted = match venue.submit(&order).await {
        Ok(ack) => ack,
        Err(error) => {
            mark("hedge_submit_unknown", serde_json::json!({}));
            owner.gap(Target::Hedge(id.clone())).await?;
            // No resend even if lookup reports NotFound. Keep the original
            // error visible; a separate recovery call may establish terminal.
            return Err(format!(
                "hedge submit UNKNOWN; exact client-ID lookup required: {error}"
            ));
        }
    };
    mark("hedge_ack", serde_json::json!({}));
    // Print only after transport returns, never ahead of the latency-critical send.
    eprintln!(
        "[hedge-ack] {} {} {} quantity={} order_id={}",
        order.venue, order.side, order.symbol, order.quantity, accepted.order_id
    );
    let key = OrderKey {
        symbol: order.symbol.clone(),
        order_id: accepted.order_id,
    };
    owner.acknowledge_hedge(id.clone(), key.clone()).await?;
    if let Some(raw) = accepted.cumulative {
        let cumulative = decimal(&raw).map_err(|_| "invalid hedge ACK cumulative")?;
        let now = Instant::now();
        owner
            .observe_hedge(
                id.clone(),
                OrderSnapshot {
                    scope: Scope {
                        venue: order.venue,
                        account: order.account,
                    },
                    key,
                    original: q,
                    cumulative,
                    status: if cumulative == q {
                        Status::Filled
                    } else if cumulative > Decimal::ZERO {
                        Status::PartiallyFilled
                    } else {
                        Status::New
                    },
                    event_ms: None,
                    revision: 0,
                    requires_reconciliation: true,
                    observed_at: now,
                    applied_at: now,
                },
            )
            .await?;
    }
    // Confirmation runs independently; never hold the fast send lane across REST.
    owner.progress().await
}
