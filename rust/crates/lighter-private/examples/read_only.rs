//! Reads public metadata, authenticated account/orders, and private stream; no mutation path.
use perp_arb_lighter_private::{monitor, Client};
use perp_arb_private_events::{
    actor,
    orders::{Network, Order, TimeInForce},
};
use std::time::{Duration, Instant};
#[tokio::main]
async fn main() -> Result<(), String> {
    let account = std::env::var("LIGHTER_ACCOUNT_INDEX").map_err(|_| "missing account")?;
    let spec = perp_arb_market_data::lighter::resolve("ETH").await?;
    let o = Order {
        venue: "lighter".into(),
        network: Network::Mainnet,
        account,
        symbol: "ETH".into(),
        side: "BUY".into(),
        quantity: "1".into(),
        limit_price: "2000".into(),
        tif: TimeInForce::PostOnly,
        reduce_only: false,
        client_id: "1".into(),
        asset: spec.asset_id.map(|n| n as u32),
    };
    let client = Client::connect(&o).await?;
    let (c, worker) = actor::channel(client.scope(), 1024, 4096).map_err(|e| e.to_string())?;
    let task = worker.spawn();
    let mut monitor = Some(monitor::start(client.clone(), c.clone()).await?);
    let result=async{
        let mut health=c.watch_health();let start=Instant::now();
        tokio::time::timeout(Duration::from_secs(30),async{while !health.borrow().connected{health.changed().await.map_err(|_|"health closed")?;}Ok::<_,String>(())}).await.map_err(|_|"private WS startup timed out")??;
        let ws_ready_ms=start.elapsed().as_millis();
        let report=client.audit(&c,None).await?;
        let history=client.validate_recent_orders().await?;
        let initial_generation=c.health().generation;
        if std::env::args().any(|s|s=="--reconnect") {
            monitor.take().ok_or("monitor missing")?.shutdown().await?;
            monitor=Some(monitor::start(client.clone(),c.clone()).await?);
            tokio::time::timeout(Duration::from_secs(30),async{while !health.borrow().connected{health.changed().await.map_err(|_|"health closed")?;}Ok::<_,String>(())}).await.map_err(|_|"private WS recovery timed out")??;
            client.audit(&c,None).await?;
            if c.health().generation<=initial_generation{return Err("generation did not advance".into());}
        }
        if std::env::args().any(|s|s=="--watch") {tokio::time::sleep(Duration::from_secs(40)).await;}
        println!("{}",serde_json::json!({"read_only":true,"can_submit_orders":false,"private_ws_connected":c.health().connected,"ws_ready_ms":ws_ready_ms,"positions":report.inventory().positions.len(),"open_orders":report.inventory().open_orders.len(),"wallets":report.inventory().wallets.len(),"history_orders_validated":history,"account_audit":"ok","initial_generation":initial_generation,"final_generation":c.health().generation}));
        Ok(())
    }.await;
    if let Some(monitor) = monitor {
        monitor.shutdown().await?;
    }
    c.shutdown().await.map_err(|e| e.to_string())?;
    task.await.map_err(|_| "coordinator failed")?;
    result
}
