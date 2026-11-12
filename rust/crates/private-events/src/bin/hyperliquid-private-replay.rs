//! Offline normalization only. No environment/account access.
use perp_arb_private_events::{hyperliquid, Error};
use serde_json::{json, Value};
use std::io::{self, Read};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    io::stdin()
        .take(8 * 1024 * 1024 + 1)
        .read_to_string(&mut input)?;
    if input.len() > 8 * 1024 * 1024 {
        return Err("input limit".into());
    }
    let frames: Vec<Value> = serde_json::from_str(&input)?;
    let rows:Vec<_>=frames.into_iter().map(|v|match hyperliquid::decode_rest(&v.to_string()){
        Ok(v)=>json!({"order_id":v.key.order_id,"quantity":v.cumulative.to_string(),"terminal":v.status.terminal()}),
        Err(Error::UnknownOrder)=>json!({"unknown":true}),
        Err(e)=>json!({"error":e.to_string()}),
    }).collect();
    println!("{}", serde_json::to_string(&rows)?);
    Ok(())
}
