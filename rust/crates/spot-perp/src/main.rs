use perp_arb_spot_perp::{
    market::{self, Cache},
    metadata,
    policy::{self, Input, Mode, Venue},
    Result,
};
use std::{
    collections::BTreeMap,
    io::{self, BufRead, Write},
    time::{Duration, Instant},
};

fn args() -> Result<(String, BTreeMap<String, String>)> {
    let mut a = std::env::args().skip(1);
    let action = a.next().unwrap_or_else(|| "help".into());
    let mut result = BTreeMap::new();
    while let Some(key) = a.next() {
        if ![
            "--symbol",
            "--mode",
            "--total-size",
            "--clip-size",
            "--max-gap",
            "--watch-seconds",
            "--perp-venue",
            "--run-dir",
            "--confirm",
        ]
        .contains(&key.as_str())
        {
            return Err(format!("unsupported flag {key}"));
        }
        let value = a.next().ok_or("missing option value")?;
        if result.insert(key, value).is_some() {
            return Err("duplicate option".into());
        }
    }
    Ok((action, result))
}
fn evaluate_lines() -> Result<()> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());
    loop {
        let mut line = String::new();
        let count = std::io::Read::take(&mut reader, 65537)
            .read_line(&mut line)
            .map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        if count > 65536 {
            return Err("replay input record too large".into());
        }
        let start = Instant::now();
        let decision = serde_json::from_str::<Input>(&line)
            .map_err(|e| e.to_string())
            .and_then(|i| policy::evaluate(&i));
        let ns = start.elapsed().as_nanos();
        let row = match decision {
            Ok(d) => serde_json::json!({"ok":true,"decision":d,"parse_and_decide_ns":ns}),
            Err(e) => serde_json::json!({"ok":false,"error":e,"parse_and_decide_ns":ns}),
        };
        writeln!(out, "{row}").map_err(|e| e.to_string())?;
    }
    out.flush().map_err(|e| e.to_string())
}
async fn preview(options: BTreeMap<String, String>) -> Result<()> {
    if options.contains_key("--run-dir") || options.contains_key("--confirm") {
        return Err("preview does not accept live/recovery options".into());
    }
    let required = |name: &str| {
        options
            .get(name)
            .cloned()
            .ok_or_else(|| format!("missing {name}"))
    };
    let symbol = required("--symbol")?;
    market::validate_base(&symbol)?;
    let perp_venue =
        Venue::parse_perp(options.get("--perp-venue").map_or("aster", String::as_str))?;
    let mode = match required("--mode")?.as_str() {
        "open" => Mode::Open,
        "close" => Mode::Close,
        _ => return Err("mode must be open/close".into()),
    };
    let total = required("--total-size")?;
    let max_gap = options
        .get("--max-gap")
        .cloned()
        .unwrap_or_else(|| "0.0025".into());
    let watch: u64 = options
        .get("--watch-seconds")
        .map_or(Ok(0), |s| s.parse())
        .map_err(|_| "invalid watch seconds")?;
    if watch > 3600 {
        return Err("watch seconds must be <=3600".into());
    }
    // Validate numerical arguments before network IO, then replace the placeholder rules with metadata.
    let dummy = policy::Book {
        bid: "1".into(),
        ask: "2".into(),
        bid_size: "1".into(),
        ask_size: "1".into(),
        age_ms: 0,
    };
    let mut input = Input {
        mode,
        perp_venue,
        remaining: total,
        clip_limit: options.get("--clip-size").cloned(),
        max_gap,
        spot: dummy.clone(),
        perp: dummy,
        rules: policy::Rules {
            step: "0.0000000000000000000000000001".into(),
            minimum: "1".into(),
            maximum: None,
            min_notional: "5".into(),
            max_notional: None,
        },
        resting: None,
    };
    policy::evaluate(&input)?;
    let client = market::client()?;
    let (spot_meta, perp_meta) = market::instruments(&client, &symbol, perp_venue).await?;
    input.rules = metadata::combine(&spot_meta, &perp_meta)?;
    // Validate common lot before opening WS.
    policy::evaluate(&input)?;
    println!("Rust public preview only: Binance Spot / {perp_venue:?}; no order transport; mode={mode:?}; max adverse gap={}",input.max_gap);
    println!(
        "{}",
        serde_json::json!({"rules":input.rules,"spot_tick":spot_meta.tick,"perp_tick":perp_meta.tick,
            "perp_price_rule":perp_meta.price_rule,"spot_settlement":spot_meta.settlement,"perp_settlement":perp_meta.settlement,
            "comparison":"nominal prices; no USDT/USDC FX conversion; fees/funding excluded"})
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let spot_task = market::subscribe(Venue::BinanceSpot, spot_meta, tx.clone());
    let perp_task = market::subscribe(perp_venue, perp_meta, tx);
    let mut caches = [Cache::default(), Cache::default()];
    let started = Instant::now();
    let mut last_output = String::new();
    let mut samples = Vec::new();
    let mut accepted_quotes = [0_u64; 2];
    let mut accepted_rest_quotes = 0_u64;
    let mut saw_fresh_pair = false;
    let mut gap_since = Instant::now();
    let mut ticks = tokio::time::interval(Duration::from_millis(100));
    let result=async {
        loop {
            let arrival=tokio::select! {
                _=tokio::signal::ctrl_c()=>break Ok(()),
                _=ticks.tick()=>None,
                event=rx.recv()=>{
                    let event=event.ok_or("public feeds stopped")?;
                    let index=if event.venue==Venue::BinanceSpot {0}else{1};
                    if let Some(error)=event.error {eprintln!("public quote unavailable {:?}: {error}",event.venue);}
                    if let Some(quote)=event.quote {
                        let arrival=if quote.rest {None}else{Some(quote.received)};
                        let rest=quote.rest;
                        match caches[index].update(quote) {
                            Ok(true)=>{accepted_quotes[index]+=1;if rest {accepted_rest_quotes+=1;}},
                            Ok(false)=>(),
                            Err(error)=>eprintln!("{error}"),
                        }
                        arrival
                    }else{caches[index].quote=None;None}
                }
            };
            if watch>0&&started.elapsed()>=Duration::from_secs(watch) {
                break if saw_fresh_pair {Ok(())}else{Err("observation ended without a fresh pair of quotes".into())};
            }
            let (Some(spot),Some(perp))=(&caches[0].quote,&caches[1].quote)else{
                if gap_since.elapsed()>=Duration::from_secs(60) {break Err("60s without usable pair quotes".into());}
                continue;
            };
            input.spot=spot.aged();input.perp=perp.aged();
            saw_fresh_pair |= input.spot.age_ms<=1000 && input.perp.age_ms<=1000;
            let decision_started=Instant::now();let decision=policy::evaluate(&input)?;
            if samples.len()<100000 {samples.push((decision_started.elapsed().as_nanos() as u64,arrival.map(|t|t.elapsed().as_nanos() as u64)));}
            let row=serde_json::to_string(&decision).map_err(|e|e.to_string())?;
            if row!=last_output {
                if decision.reason=="stale_quote" {
                    eprintln!("public stale quote: spot_age_ms={} perp_age_ms={} spot_receive_age_ms={} perp_receive_age_ms={} spot_source_ms={} perp_source_ms={} wall_ms={}",input.spot.age_ms,input.perp.age_ms,spot.received.elapsed().as_millis(),perp.received.elapsed().as_millis(),spot.source_ms,perp.source_ms,market::wall_ms());
                }
                println!("{row}");last_output=row;
            }
            if decision.candidate.is_some() {gap_since=Instant::now();if watch==0 {break Ok(());}}
            else if decision.reason=="remainder_below_minimum" {break Err("requested remainder below venue minimum; never increasing user total".into());}
            else if gap_since.elapsed()>=Duration::from_secs(60) {break Err("60s without qualifying gap/depth; no order placed".into());}
        }
    }.await;
    spot_task.abort();
    perp_task.abort();
    let _ = tokio::join!(spot_task, perp_task);
    let summary = |mut values: Vec<u64>| {
        values.sort_unstable();
        let n = values.len();
        if n == 0 {
            serde_json::json!({"n":0})
        } else {
            serde_json::json!({"n":n,"p50_ns":values[(n-1)/2],"p95_ns":values[(n-1)*95/100],"max_ns":values[n-1]})
        }
    };
    println!(
        "{}",
        serde_json::json!({"timing_kind":"public_quote_decision_only_not_fill_latency",
        "perp_venue":perp_venue,"accepted_spot_quotes":accepted_quotes[0],"accepted_perp_quotes":accepted_quotes[1],"accepted_rest_quotes":accepted_rest_quotes,"saw_fresh_pair":saw_fresh_pair,
        "policy":summary(samples.iter().map(|s|s.0).collect()),
        "ws_receive_to_decision":summary(samples.iter().filter_map(|s|s.1).collect()),"orders_sent":0})
    );
    result
}
#[tokio::main]
async fn main() {
    let result=async {
        let(action,options)=args()?;
        match action.as_str() {
            "live"=>perp_arb_spot_perp::live::run(options,false).await,
            "recover"=>perp_arb_spot_perp::live::run(options,true).await,
            "preview"=>preview(options).await,
            "binance-preflight"=>{
                if options.keys().any(|k| k!="--watch-seconds") {return Err("binance-preflight only accepts --watch-seconds; no trade parameters".into());}
                let seconds=options.get("--watch-seconds").map_or(Ok(10),|s|s.parse::<u64>()).map_err(|_|"invalid watch seconds")?;
                perp_arb_spot_perp::preflight::binance(seconds).await
            },
            "evaluate" if options.is_empty()=>evaluate_lines(),
            "help"|"--help"=>{println!("spot-perp preview --symbol ETH --mode open|close --total-size Q [--perp-venue aster|hyperliquid|lighter] [--clip-size Q] [--max-gap 0.0025] [--watch-seconds 30]\nspot-perp evaluate < input.jsonl\nspot-perp binance-preflight [--watch-seconds 10]\nspot-perp live --symbol BASE --mode open|close --total-size Q --perp-venue aster|hyperliquid --run-dir NEW_DIR --confirm PLACE_LIVE_SPOT_PERP_ORDER\nspot-perp recover --run-dir ORIGINAL_DIR --confirm PLACE_LIVE_SPOT_PERP_ORDER\nOnly explicit live/recover can submit/cancel. Native live uses perpetual maker -> Binance Spot market hedge; existing exchange leverage. Run from the project root. Use Python --engine rust wrapper to load .env.");Ok(())},
            _=>Err("unknown action; supported: preview/evaluate/binance-preflight/live/recover/help".into()),
        }
    }.await;
    if let Err(e) = result {
        eprintln!("spot-perp: {e}");
        std::process::exit(1);
    }
}
