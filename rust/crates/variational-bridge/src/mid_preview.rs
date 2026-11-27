//! Populate the existing page's Limit/Mid controls; never submit or cancel.
use crate::{wire, Bridge, Config, ReadRequest, Result};
use serde_json::{json, Value};
use std::time::Duration;

pub async fn request(bridge: &Bridge, symbol: &str, wait: Duration) -> Result<Value> {
    let symbol = wire::symbol(symbol)?;
    let mut health = bridge.health();
    tokio::time::timeout(wait, async {
        loop {
            {
                let state = health.borrow();
                if let Some(error) = &state.fault {
                    return Err(error.clone());
                }
                if state.command_connected {
                    return Ok(());
                }
            }
            health
                .changed()
                .await
                .map_err(|_| "Mid preview bridge closed")?;
        }
    })
    .await
    .map_err(|_| {
        "Mid preview timeout: open the requested Variational market and press extension Start"
    })??;
    let result = bridge
        .request(ReadRequest::PreviewPrice {
            symbol: symbol.clone(),
        })
        .await?;
    // Transport correlates request ID/type and validates the positive decimal.
    Ok(
        json!({"mode":"mid-price", "symbol":symbol, "price":result["price"],
              "source":"variational_page_mid_button", "can_submit_orders":false}),
    )
}

pub async fn run(config: Config, symbol: &str) -> Result<Value> {
    wire::symbol(symbol)?;
    // Avoid interacting with a page owned by an active Python/Rust trade.
    let _lock = perp_arb_order_test::AccountLock::global_execution()?;
    let (bridge, mut events) = Bridge::bind(config).await?;
    let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
    let result = request(&bridge, symbol, Duration::from_secs(30)).await;
    bridge.shutdown().await;
    drain.abort();
    let _ = drain.await;
    result
}

/// Read-only acceptance probe: populate the ticket, never send a dispatch permit.
pub async fn prepare_ticket(plan: &crate::paired::Plan) -> Result<Value> {
    let _lock = perp_arb_order_test::AccountLock::global_execution()?;
    let (bridge, mut events) = Bridge::bind(Config::new(plan.extension_id.clone())).await?;
    let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
    let result = async {
        request(&bridge, &plan.maker.symbol, Duration::from_secs(30)).await?;
        let price = bridge
            .prepare_maker_ui(&plan.maker, &plan.instrument)
            .await?;
        Ok(
            json!({"mode":"ui-prepare","symbol":plan.maker.symbol,"side":plan.maker.side,
            "quantity":plan.maker.quantity,"price":price,"can_submit_orders":false}),
        )
    }
    .await;
    bridge.shutdown().await;
    drain.abort();
    let _ = drain.await;
    result
}
