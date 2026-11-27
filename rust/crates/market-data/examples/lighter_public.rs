//! Public-only smoke check of Lighter metadata, snapshot/delta continuity and freshness.
use perp_arb_market_data::{
    hub::{wall_ms, MarketHub},
    shared::MarketKey,
    Venue,
};
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), String> {
    let mut hub = MarketHub::new();
    let mut subscription = hub
        .subscribe(MarketKey::new(Venue::Lighter, "ETH")?)
        .await?;
    let result=tokio::time::timeout(Duration::from_secs(15),async {
        let mut accepted=0;let mut max_age=0;
        while accepted<50 {
            let view=subscription.changed().await?;
            if let Some(snapshot)=view.fresh(wall_ms()) {accepted+=1;max_age=max_age.max(wall_ms()-snapshot.quote.source_ms().ok_or("source time missing")?);}
        }
        Ok::<_,String>(serde_json::json!({"venue":"lighter","symbol":"ETH","fresh_native_ws_revisions":accepted,"max_source_age_ms":max_age,"read_only":true}))
    }).await.map_err(|_|"no 50 fresh Lighter WS revisions in 15 seconds".to_string()).and_then(|r|r);
    hub.shutdown().await?;
    println!("{}", result?);
    Ok(())
}
