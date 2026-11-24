//! Public reference quotes only; never reads credentials or submits orders.
use perp_arb_core::{decimal, market::FreshnessPolicy};
use perp_arb_market_data::variational::{now_ms, PublicClient, Stats};
use serde_json::json;
use std::{io::Read, sync::Arc};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("variational-probe: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let mut symbol = "ETH".to_owned();
    let mut notional = "100".to_owned();
    let mut input = None;
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!("variational-probe [--symbol ETH] [--notional-usd 100] [--input stats-v2.json]\n公開參考報價；無下單。--input 離線讀檔，其餘只 GET 一次公開 API。");
            return Ok(());
        }
        let value = args.next().ok_or("missing option value")?;
        match arg.as_str() {
            "--symbol" => symbol = value,
            "--notional-usd" => notional = value,
            "--input" => input = Some(value),
            _ => return Err("unknown option".into()),
        }
    }
    let notional = decimal(&notional).map_err(|e| e.to_string())?;
    if notional <= perp_arb_core::Decimal::ZERO {
        return Err("notional must be positive".into());
    }
    let stats = if let Some(path) = input {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|e| e.to_string())?
            .take(8_000_001)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        Arc::new(Stats::parse(&bytes, now_ms()?)?)
    } else {
        PublicClient::new()?.snapshot().await?
    };
    let quote = stats.perpetual_quote(&symbol, notional)?;
    let now = now_ms()?;
    println!(
        "{}",
        json!({
            "venue": "variational", "kind": "indicative_only", "can_submit_orders": false,
            "symbol": quote.ticker, "contract_type": "perpetual", "quote_currency": "USDC",
            "tier": quote.tier, "tier_notional_usdc": quote.tier_notional_usdc.to_string(),
            "requested_notional_usdc": quote.requested_notional_usdc.to_string(),
            "bid": quote.bid.to_string(), "ask": quote.ask.to_string(),
            "source_ms": quote.source_ms, "received_ms": quote.received_ms,
            "source_age_ms": now.saturating_sub(quote.source_ms),
            "freshness": format!("{:?}", quote.freshness(now, FreshnessPolicy::default())),
            "listing_count": stats.listings.len(),
        })
    );
    Ok(())
}
