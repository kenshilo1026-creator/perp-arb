//! Explicit public-network smoke test. Saves parsed snapshots for the existing
//! Python/Rust verifier; does not read credentials or connect to execution.
use perp_arb_market_data::{
    catalog::SymbolMappings,
    metadata::{CachePolicy, HttpPolicy, MetadataCache, PublicHttp, Source},
};
use serde_json::{json, Map, Value};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 || args[0] != "--output" {
        return Err("Usage: metadata-probe --output <NEW capture.json>; sends public requests to Aster v1/v3 and Hyperliquid".into());
    }
    // Reserve first, never overwrite earlier evidence. Incomplete files are not
    // accepted by verify_instruments.py.
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[1])?;
    let mut file = BufWriter::new(file);
    let http = PublicHttp::new(HttpPolicy::default())?;
    let mut records = Map::new();
    for source in [Source::AsterV1, Source::AsterV3, Source::Hyperliquid] {
        let cache = MetadataCache::new(http.clone(), source, CachePolicy::default())?;
        let start = Instant::now();
        let snapshot = cache.refresh(false).await?;
        let fetch_ms = start.elapsed().as_secs_f64() * 1000.0;
        let before = http.request_attempts(source);
        let mut read_ns = Vec::new();
        for _ in 0..1000 {
            let start = Instant::now();
            std::hint::black_box(cache.current()?);
            read_ns.push(start.elapsed().as_nanos() as u64);
        }
        for _ in 0..20 {
            cache.refresh(false).await?;
        }
        let after = http.request_attempts(source);
        if before != after {
            return Err("cache hits unexpectedly sent HTTP requests".into());
        }
        let (generation, eth) = cache.resolve("ETH", &SymbolMappings::default())?;
        if !cache.is_current_generation(generation) {
            return Err("metadata generation invalidated during probe".into());
        }
        read_ns.sort_unstable();
        let rows = snapshot.payload[if source == Source::Hyperliquid {
            "universe"
        } else {
            "symbols"
        }]
        .as_array()
        .ok_or("missing rows")?
        .len();
        records.insert(source.label().into(), json!({
            "url":source.url(), "payload":snapshot.payload.as_ref(), "captured_unix_ms":SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            "http_attempts":after,"additional_attempts_for_1020_cache_hits":after-before,"generation":generation,"rows":rows,
            "initial_load_ms":fetch_ms,"memory_snapshot_read_ns":{"p50":read_ns[499],"p95":read_ns[949],"p99":read_ns[989]},
            "resolved_eth":{"native":eth.key.symbol(),"asset_id":eth.asset_id,"lot":eth.limit.lot().to_string()}
        }));
        eprintln!(
            "{}: {} rows, {} HTTP attempts, 1020 cache hits without HTTP",
            source.label(),
            rows,
            after
        );
    }
    let capture: Value = json!({"complete":true,"public_only":true,"producer":"Rust metadata-probe","records":records,
        "scope":"Parsed public JSON snapshots, not raw response bytes. Memory-read timings exclude symbol parsing, network, execution and hedging."});
    serde_json::to_writer_pretty(&mut file, &capture)?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.get_ref().sync_all()?;
    Ok(())
}
