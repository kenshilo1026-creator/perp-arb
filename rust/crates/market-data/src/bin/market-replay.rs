use perp_arb_market_data::{FeedState, Update, Venue};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    io::{self, BufRead},
};

#[derive(Deserialize)]
struct Frame {
    venue: Venue,
    symbol: String,
    raw: String,
    received_ms: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut states = HashMap::new();
    for line in io::stdin().lock().lines() {
        let row: Value = serde_json::from_str(&line?)?;
        let kind = row["type"].as_str().unwrap_or("frame");
        if kind == "health" && row["healthy"] == false {
            let key = (row["venue"].to_string(), row["symbol"].to_string());
            states.remove(&key);
        }
        if kind != "frame" {
            continue;
        }
        let key = (row["venue"].to_string(), row["symbol"].to_string());
        let frame: Frame = serde_json::from_value(row)?;
        let state = states.entry(key).or_insert_with(FeedState::default);
        let start = std::time::Instant::now();
        let result = state.ingest(frame.venue, &frame.symbol, &frame.raw, frame.received_ms);
        let elapsed = start.elapsed().as_nanos() as u64;
        let mut output = match result {
            Ok(update) => json!({"update":update, "fresh":state.fresh(frame.received_ms).is_some(),
                "book":if update == Update::Accepted {state.latest().map(|s| s.normalized())} else {None}, "error":null}),
            Err(error) => json!({"update":null, "fresh":false, "book":null, "error":error}),
        };
        output["parse_and_cache_ns"] = json!(elapsed);
        println!("{output}");
    }
    Ok(())
}
