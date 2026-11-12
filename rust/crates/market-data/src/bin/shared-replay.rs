//! NDJSON public-cache event replay. No sockets and no credential loading.
use perp_arb_core::{decimal, pricing::Side};
use perp_arb_market_data::{
    shared::{MarketKey, MarketState},
    Venue,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::HashMap,
    io::{self, BufRead},
};

#[derive(Deserialize)]
struct Event {
    action: String,
    venue: Venue,
    symbol: String,
    now: i64,
    raw: Option<String>,
    quantity: String,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut states: HashMap<MarketKey, MarketState> = HashMap::new();
    for line in io::stdin().lock().lines() {
        let event: Event = serde_json::from_str(&line?)?;
        let key = MarketKey::new(event.venue, &event.symbol)?;
        let state = states
            .entry(key.clone())
            .or_insert_with(|| MarketState::new(key));
        let quantity = decimal(&event.quantity)?;
        let start = std::time::Instant::now();
        let update = match event.action.as_str() {
            "begin" => {
                state.begin();
                None
            }
            "disconnect" => {
                state.disconnect(state.view().generation, "replay disconnect");
                None
            }
            "frame" => Some(state.ingest(
                state.view().generation,
                event.raw.as_deref().ok_or("raw required")?,
                event.now,
            )?),
            "query" => None,
            _ => return Err("unknown replay action".into()),
        };
        let fresh = state.view().fresh(event.now);
        let elapsed = start.elapsed().as_nanos();
        let prices = if let Some(book) = fresh {
            [Side::Buy,Side::Sell].into_iter().map(|side| {
                let q=&book.quote;
                Ok(json!({"vwap":q.executable(side,Some(quantity))?.map(|d|d.normalize().to_string()),
                    "marginal":q.marginal(side,quantity)?.map(|d|d.normalize().to_string())}))
            }).collect::<Result<Vec<_>,perp_arb_core::Error>>()?
        } else {
            vec![]
        };
        println!(
            "{}",
            json!({"update":update,"fresh":fresh.is_some(),"book":fresh.map(|s|s.normalized()),
            "prices":prices,"parse_cache_select_ns":elapsed})
        );
    }
    Ok(())
}
