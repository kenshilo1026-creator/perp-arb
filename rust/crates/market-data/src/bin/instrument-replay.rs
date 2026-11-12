//! Offline metadata and exact precision replay. No network or credentials.
use perp_arb_core::{
    decimal,
    instruments::{common_lot, Instrument, PriceRule},
    pricing::Direction,
    Decimal,
};
use perp_arb_market_data::catalog::{
    resolve_aster, resolve_hyperliquid, ResolvedInstrument, SymbolMappings,
};
use serde_json::{json, Value};
use std::io::{self, Read};

fn d(v: &Value) -> Result<Decimal, String> {
    decimal(v.as_str().ok_or("decimal must be a string")?).map_err(|e| e.to_string())
}
fn optional(v: &Value) -> Result<Option<Decimal>, String> {
    if v.is_null() {
        Ok(None)
    } else {
        d(v).map(Some)
    }
}
fn rule(row: &Value) -> Result<Instrument, String> {
    let price = if row["sz"].is_null() {
        PriceRule::Tick(d(&row["tick"])?)
    } else {
        PriceRule::Hyperliquid {
            sz_decimals: row["sz"].as_u64().filter(|n| *n <= 6).ok_or("invalid sz")? as u32,
        }
    };
    Instrument::new(
        price,
        d(&row["lot"])?,
        d(&row["min_size"])?,
        optional(&row["min_notional"])?,
        optional(&row["max_size"])?,
    )
    .map_err(|e| e.to_string())
}
fn text(d: Decimal) -> String {
    d.normalize().to_string()
}
fn summary(i: &Instrument) -> Value {
    let (tick, sz) = match i.price_rule() {
        PriceRule::Tick(t) => (Some(text(t)), None),
        PriceRule::Hyperliquid { sz_decimals } => (None, Some(sz_decimals)),
    };
    json!({"tick":tick,"sz":sz,"lot":text(i.lot()),"min_size":text(i.min_size()),"min_notional":i.min_notional().map(text),"max_size":i.max_size().map(text)})
}
fn resolved(i: ResolvedInstrument) -> Value {
    json!({"native":i.key.symbol(),"base":i.native_base,"canonical":i.canonical_base,"quote":i.quote,"settlement":i.settlement,
        "asset_id":i.asset_id,"base_per_native":i.units().map(|u|text(u.base_per_native())),"limit":summary(&i.limit),"market":i.market.as_ref().map(summary)})
}
fn execute(case: &Value, request: &Value, maps: &SymbolMappings) -> Result<Value, String> {
    match case["op"].as_str().ok_or("missing op")? {
        "resolve" => {
            let id = case["catalog"].as_str().ok_or("missing catalog")?;
            let payload = &request["catalogs"][id];
            let symbol = case["symbol"].as_str().ok_or("missing symbol")?;
            let result = match case["venue"].as_str() {
                Some("aster") => resolve_aster(payload, symbol, maps),
                Some("hyperliquid") => resolve_hyperliquid(payload, symbol, maps),
                _ => return Err("invalid venue".into()),
            };
            result.map(resolved)
        }
        "map" => Ok(
            json!({"canonical":maps.canonical(case["symbol"].as_str().ok_or("missing symbol")?,case["venue"].as_str().ok_or("missing venue")?),
            "native":maps.native(case["symbol"].as_str().ok_or("missing symbol")?,case["venue"].as_str().ok_or("missing venue")?)}),
        ),
        "common_lot" => {
            let lots = case["lots"]
                .as_array()
                .ok_or("missing lots")?
                .iter()
                .map(d)
                .collect::<Result<Vec<_>, _>>()?;
            common_lot(&lots)
                .map(|n| json!(n.map(text)))
                .map_err(|e| e.to_string())
        }
        "math" => {
            let i = rule(&case["instrument"])?;
            let price = d(&case["price"])?;
            let quantity = d(&case["quantity"])?;
            let result = || -> perp_arb_core::Result<Value> {
                Ok(
                    json!({"down":text(i.round_price(price,Direction::Down)?),"up":text(i.round_price(price,Direction::Up)?),
                "quantity":text(i.round_quantity(quantity)?),"size_error":i.size_error(quantity,Some(price))?.map(|e|e.code()),"tolerance":i.price_tolerance(price)?.to_string()}),
                )
            };
            result().map_err(|e| e.to_string())
        }
        _ => Err("unknown operation".into()),
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut raw = String::new();
    io::stdin().take(16_777_217).read_to_string(&mut raw)?;
    if raw.len() > 16_777_216 {
        return Err("input too large".into());
    }
    let request: Value = serde_json::from_str(&raw)?;
    let maps = SymbolMappings::parse(&request["mappings"])?;
    let cases = request["cases"].as_array().ok_or("missing cases")?;
    let results: Vec<_> = cases
        .iter()
        .map(|c| match execute(c, &request, &maps) {
            Ok(value) => json!({"value":value}),
            Err(e) => json!({"error":e}),
        })
        .collect();
    println!("{}", json!({"results":results}));
    Ok(())
}
