//! Public-only two-market/two-venue lifecycle smoke test; no account or orders.
use perp_arb_market_data::{
    catalog::SymbolMappings,
    hub::{wall_ms, MarketHub, Subscription},
    metadata::{CachePolicy, HttpPolicy, MetadataCache, PublicHttp, Source},
    shared::MarketKey,
    Venue,
};
use serde_json::{json, Value};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    time::Duration,
};
use tokio::time::Instant;
fn sample(sub: &Subscription) -> Value {
    let v = sub.snapshot();
    json!({"symbol":sub.key().symbol(),"generation":v.generation,"depth":v.accepted_depth,"bbo":v.accepted_bbo,"fresh":v.fresh(wall_ms()).is_some(),"reason":v.reason})
}
async fn phase(subs: &[&Subscription], seconds: u64) -> Vec<u64> {
    let mut fresh = vec![0; subs.len()];
    let until = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < until {
        for (i, sub) in subs.iter().enumerate() {
            if sub.snapshot().fresh(wall_ms()).is_some() {
                fresh[i] += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    fresh
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 || args[0] != "--output" {
        return Err(
            "Usage: multiplex-probe --output NEW.json (about 20 seconds; public HTTP + WS)".into(),
        );
    }
    let mut file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&args[1])?,
    );
    let http = PublicHttp::new(HttpPolicy::default())?;
    let mut hub = MarketHub::new();
    for source in [Source::AsterV3, Source::Hyperliquid] {
        let cache = MetadataCache::new(http.clone(), source, CachePolicy::default())?;
        cache.refresh(false).await?;
        hub.bind_metadata(cache, SymbolMappings::default())?;
    }
    let mut ae = hub
        .subscribe(MarketKey::new(Venue::Aster, "ETHUSDT")?)
        .await?;
    let mut he = hub
        .subscribe(MarketKey::new(Venue::Hyperliquid, "ETH")?)
        .await?;
    let initial = phase(&[&ae, &he], 5).await;
    let ab = hub
        .subscribe(MarketKey::new(Venue::Aster, "BTCUSDT")?)
        .await?;
    let hb = hub
        .subscribe(MarketKey::new(Venue::Hyperliquid, "BTC")?)
        .await?;
    let multiple = phase(&[&ae, &he, &ab, &hb], 8).await;
    let four = [sample(&ae), sample(&he), sample(&ab), sample(&hb)];
    hub.handoff(&mut ae, MarketKey::new(Venue::Aster, "BTCUSDT")?)
        .await?;
    hub.handoff(&mut he, MarketKey::new(Venue::Hyperliquid, "BTC")?)
        .await?;
    let final_phase = phase(&[&ae, &he, &ab, &hb], 5).await;
    let a = hub.stats(Venue::Aster);
    let h = hub.stats(Venue::Hyperliquid);
    let passed = initial
        .iter()
        .chain(&multiple)
        .chain(&final_phase)
        .all(|n| *n > 0)
        && four
            .iter()
            .all(|v| v["depth"].as_u64().unwrap_or(0) > 0 && v["bbo"].as_u64().unwrap_or(0) > 0)
        && [&a, &h].iter().all(|s| {
            s.connections_started == 1 && s.active_markets == 1 && s.faults == 0 && s.handovers == 0
        })
        && a.control_acks == 3
        && h.control_acks == 6;
    hub.shutdown().await?;
    let cleared = [&ae, &he, &ab, &hb]
        .iter()
        .all(|s| s.snapshot().selected(wall_ms()).is_none());
    let report = json!({"complete":passed && cleared,"public_only":true,"phase_seconds":[5,8,5],"initial_fresh_samples":initial,"four_market_fresh_samples":multiple,"four_market_views":four,"handoff_fresh_samples":final_phase,"aster":a,"hyperliquid":h,"shutdown_cleared":cleared,
        "scope":"Live public connection/routing/ACK/handoff smoke test. Counts are cumulative accepted messages; not latency percentiles, raw recording or P&L."});
    serde_json::to_writer_pretty(&mut file, &report)?;
    file.write_all(b"\n")?;
    file.flush()?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !passed || !cleared {
        return Err("multiplex probe failed; inspect report".into());
    }
    Ok(())
}
