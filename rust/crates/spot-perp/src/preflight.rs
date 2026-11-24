//! Explicit Binance account/WS authentication check. Contains no dispatch API.
use crate::Result;
use perp_arb_binance_spot::{auth::Credentials, client::Client, events::Event, stream::Session};
use std::time::{Duration, Instant};
pub async fn binance(seconds: u64) -> Result<()> {
    if !(1..=300).contains(&seconds) {
        return Err("private observation must be 1..300 seconds".into());
    }
    let client = Client::new(Credentials::from_env().map_err(|_| {
        "set a matching HMAC BINANCE_SPOT_API_KEY/SECRET pair, or BINANCE_API_KEY/SECRET pair"
    })?)
    .map_err(|e| e.to_string())?;
    client
        .sync_time()
        .await
        .map_err(|e| format!("Binance clock preflight: {e}"))?;
    // Establish subscription before reading account. This probe does not certify
    // an inventory baseline; buffered account changes are counted below.
    let mut session = Session::connect(client.clone())
        .await
        .map_err(|e| format!("Binance private WS: {e}"))?;
    let account = client
        .account()
        .await
        .map_err(|e| format!("Binance account preflight: {e}"))?;
    println!(
        "{}",
        serde_json::json!({"kind":"binance_private_preflight","account_read":true,"ws_subscribed":true,"can_trade":account.can_trade,"orders_sent":0})
    );
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut orders = 0;
    let mut trades = 0;
    let mut account_changes = 0;
    let result=async {
        loop {
            tokio::select! {
                _=tokio::signal::ctrl_c()=>break Ok(()),
                _=tokio::time::sleep_until(deadline.into())=>break Ok(()),
                event=session.next()=>match event.map_err(|e|format!("Binance private stream interrupted: {e}; orders require reconciliation"))? {
                    Event::Order(e)=>{orders+=1;trades+=usize::from(e.trade.is_some());},
                    Event::AccountChanged=>account_changes+=1,
                }
            }
        }
    }.await;
    let closed = session.close().await.map_err(|e| e.to_string());
    println!(
        "{}",
        serde_json::json!({"kind":"binance_private_preflight_summary","order_events":orders,"trade_events":trades,"account_events":account_changes,
        "orders_sent":0,"trade_event_seen":trades>0,"actual_fill_validation":false,"inventory_reconciled":false})
    );
    result.and(closed)
}
