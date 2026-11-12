//! Offline Aster frame replay; never contacts an exchange or loads credentials.
use perp_arb_private_events::{aster, Evidence, FillCursor, Ledger, Scope};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{self, Read},
    time::Instant,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    io::stdin()
        .take(8 * 1024 * 1024 + 1)
        .read_to_string(&mut input)?;
    if input.len() > 8 * 1024 * 1024 {
        return Err("input limit".into());
    }
    let frames: Vec<Value> = serde_json::from_str(&input)?;
    let scope = Scope {
        venue: "aster".into(),
        account: "offline".into(),
    };
    let mut ledger = Ledger::new(scope.clone(), 4096)?;
    let session = ledger.connected()?;
    let mut cursors = BTreeMap::new();
    let mut rows = Vec::with_capacity(frames.len());
    for frame in frames {
        let result = aster::decode_ws(&frame.to_string());
        let row = match result {
            Ok(Some(update)) => {
                let cursor = cursors
                    .entry(update.key.clone())
                    .or_insert_with(|| FillCursor::new(scope.clone(), update.key.clone()));
                match ledger.observe(update, Evidence::Ws(session.clone()), Instant::now()) {
                    Ok(snapshot) => match cursor.acknowledge(&snapshot) {
                        Ok(delta) => {
                            json!({"symbol": snapshot.key.symbol, "order_id": snapshot.key.order_id,
                            "quantity": snapshot.cumulative.to_string(), "status":snapshot.status.as_str(), "delta":delta.to_string()})
                        }
                        Err(e) => json!({"error":e.to_string()}),
                    },
                    Err(e) => json!({"error":e.to_string()}),
                }
            }
            Ok(None) => json!({"ignored":true}),
            Err(e) => json!({"error":e.to_string()}),
        };
        rows.push(row);
    }
    println!("{}", serde_json::to_string(&rows)?);
    Ok(())
}
