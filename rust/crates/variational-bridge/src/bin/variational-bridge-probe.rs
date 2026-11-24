//! Explicit, bounded read-only native receiver for the existing extension.
use perp_arb_variational_bridge::{evidence::Evidence, wire::now_ms, Bridge, Config, ReadRequest};
use serde_json::json;
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::PathBuf,
    time::Duration,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("variational-bridge-probe: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let mut id = None;
    let mut output: Option<PathBuf> = None;
    let mut seconds = 60u64;
    let mut seconds_set = false;
    let mut check_symbol = None;
    let mut native_preflight = false;
    let mut inventory = false;
    let mut native_account = None;
    let mut native_order_id = None;
    while let Some(key) = args.next() {
        if key == "--help" {
            println!("variational-bridge-probe --extension-id CHROME_ID --output NEW.jsonl [--seconds 60] [--check-symbol ETH | --native-preflight [--native-account 0x...] [--native-order-id RFQ | --inventory]]\n唯讀；native-preflight 透過 extension 讀取帳戶、委託、成交來源，不送單、不撤單。先停用佔用 8766/8767/8768 的其他 broker。");
            return Ok(());
        }
        if key == "--native-preflight" && !native_preflight {
            native_preflight = true;
            continue;
        }
        if key == "--inventory" && !inventory {
            inventory = true;
            continue;
        }
        let value = args.next().ok_or("missing argument value")?;
        match key.as_str() {
            "--extension-id" if id.is_none() => id = Some(value),
            "--output" if output.is_none() => output = Some(value.into()),
            "--native-account" if native_account.is_none() => {
                native_account = Some(perp_arb_variational_bridge::native::account(&value)?)
            }
            "--native-order-id" if native_order_id.is_none() => native_order_id = Some(value),
            "--check-symbol" if check_symbol.is_none() => {
                check_symbol = Some(perp_arb_variational_bridge::wire::symbol(&value)?)
            }
            "--seconds" if !seconds_set => {
                seconds = value.parse().map_err(|_| "invalid seconds")?;
                seconds_set = true;
            }
            _ => return Err("unknown or duplicate option".into()),
        }
    }
    if (native_preflight && check_symbol.is_some())
        || (!native_preflight && (native_account.is_some() || native_order_id.is_some()))
        || (native_order_id.is_some() && native_account.is_none())
    {
        return Err("native query needs --native-preflight; exact RFQ lookup also needs --native-account; cannot combine with UI check".into());
    }
    if inventory && (!native_preflight || native_account.is_none() || native_order_id.is_some()) {
        return Err(
            "--inventory requires --native-preflight --native-account and no order filter".into(),
        );
    }
    if !(1..=3600).contains(&seconds) {
        return Err("seconds must be 1..3600".into());
    }
    let config = Config::new(id.ok_or("--extension-id required")?);
    let path = output.ok_or("--output required")?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| "cannot create new output; existing recordings never overwritten")?;
    // Dedicated disk worker. Queue overflow is a failed recording, never a
    // silently dropped private event. No disk I/O on the socket reader tasks.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1024);
    let writer = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let mut writer = BufWriter::new(file);
        while let Some(line) = rx.blocking_recv() {
            writeln!(writer, "{line}").map_err(|_| "recording write failed")?;
        }
        writer.flush().map_err(|_| "recording flush failed")?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|_| "recording sync failed".into())
    });
    let native = native_preflight.then_some((native_account, native_order_id));
    let outcome = capture(config, seconds, check_symbol, native, inventory, &tx).await;
    drop(tx);
    writer.await.map_err(|_| "recording worker failed")??;
    println!("{}", outcome?);
    Ok(())
}

async fn capture(
    config: Config,
    seconds: u64,
    check_symbol: Option<String>,
    native: Option<(Option<String>, Option<String>)>,
    inventory: bool,
    writer: &tokio::sync::mpsc::Sender<String>,
) -> Result<serde_json::Value, String> {
    let mut account_feed = if inventory {
        Some(perp_arb_variational_bridge::account_feed::Feed::start(
            native
                .as_ref()
                .and_then(|x| x.0.as_deref())
                .ok_or("inventory account required")?,
        )?)
    } else {
        None
    };
    let account_coordinator = account_feed.as_ref().map(|f| f.coordinator());
    let (bridge, mut events) = Bridge::bind(config).await?;
    let mut health = bridge.health();
    println!(
        "{}",
        json!({"mode":"read_only", "private":bridge.private_address.to_string(),
        "reference":bridge.reference_address.to_string(), "command":bridge.command_address.to_string(), "can_submit_orders":false})
    );
    let mut evidence = Evidence::default();
    let mut ws_validation = perp_arb_variational_bridge::ws_validation::Validator::default();
    let mut count = 0u64;
    let mut portfolios = 0u64;
    let mut gaps = 0u64;
    let mut connections = [0u64; 3];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    let result = async {
        let native_requested = native.is_some();
        let check_requested = check_symbol.is_some() || native.is_some();
        let read_check = async {
            if check_symbol.is_none() && native.is_none() {
                return std::future::pending::<Result<serde_json::Value, String>>().await;
            }
            let mut ready = bridge.health();
            loop {
                if ready.borrow().command_connected { break; }
                ready.changed().await.map_err(|_| "command health closed")?;
            }
            let start = std::time::Instant::now();
            if let Some((account, order_id)) = native {
                if inventory {
                    let coordinator=account_coordinator.as_ref().ok_or("account coordinator missing")?;
                    let account=account.ok_or("inventory account required")?;
                    for attempt in 0..2 {
                    let mut account_health=coordinator.watch_health();
                    while !account_health.borrow().connected { account_health.changed().await.map_err(|_|"account feed stopped")?; }
                    let ticket=coordinator.begin_account_query().await.map_err(|e|e.to_string())?;
                    let query = bridge.inventory(account.clone()).await?;
                    writer.try_send(json!({"event":"inventory_sources","receipt":query.receipt()}).to_string()).map_err(|_| "recording queue full")?;
                    let mut summary=query.summary();
                    match query.reconcile(coordinator,ticket,None).await {
                        Ok(report)=>{summary["account_report_created"]=json!(true); summary["expected_supplied"]=json!(report.expected_supplied);},
                        Err(error)=>{
                            if error=="WrongSession" && attempt==0 {
                                writer.try_send(json!({"event":"inventory_retry","reason":"private_gap_or_account_change_during_query","attempt":1}).to_string()).map_err(|_|"recording queue full")?;
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                            summary["account_report_created"]=json!(false);summary["report_error"]=json!(error);
                        },
                    }
                    return Ok(json!({"event":"inventory_query","response":summary,"round_trip_ms":start.elapsed().as_millis()}));
                    }
                    return Err("inventory retries exhausted".into());
                }
                let query = bridge.native_query(account, order_id).await?;
                // Sanitized fixed-schema receipt stays in the private recording.
                writer.try_send(json!({"event":"native_sources","receipt":query.receipt()}).to_string()).map_err(|_| "recording queue full")?;
                return Ok(json!({"event":"native_query","response":query.summary(),"round_trip_ms":start.elapsed().as_millis()}));
            }
            let response = bridge.request(ReadRequest::CheckOpenOrder {symbol:check_symbol.ok_or("symbol missing")?, order_id:None}).await?;
            Ok(json!({"event":"read_check", "response":response, "round_trip_ms":start.elapsed().as_millis()}))
        };
        tokio::pin!(read_check);
        let mut check_done = false;
        loop {
            tokio::select! {
                result=&mut read_check, if !check_done=>{
                    let check = result?;
                    check_done = true;
                    writer.try_send(check.to_string()).map_err(|_| "recording queue full or closed")?;
                    println!("{check}");
                },
                event=events.recv()=>{
                    let event=event.ok_or("bridge stopped")?;
                    if let Some(feed)=account_feed.as_mut() { feed.ingest(&event).await?; }
                    ws_validation.observe(&event)?;
                    count+=1;
                    use perp_arb_variational_bridge::wire::Observation;
                    if matches!(event.observation, Observation::Portfolio { .. }) { portfolios+=1; }
                    if matches!(event.observation, Observation::Gap { .. }) { gaps+=1; }
                    if matches!(event.observation, Observation::Connected) {
                        use perp_arb_variational_bridge::wire::Channel;
                        let index = match event.channel { Channel::Private=>0, Channel::Reference=>1, Channel::Command=>2 };
                        connections[index]+=1;
                        println!("{}",json!({"event":"connected","channel":event.channel,"generation":event.generation}));
                    }
                    evidence.apply(&event)?;
                    writer.try_send(serde_json::to_string(&event).map_err(|_| "event serialization failed")?)
                        .map_err(|_| "recording queue full or closed")?;
                },
                changed=health.changed()=>{
                    changed.map_err(|_| "health channel closed")?;
                    if let Some(error)=health.borrow().fault.clone() { return Err(error); }
                },
                _=tokio::time::sleep_until(deadline)=>break,
                _=tokio::signal::ctrl_c()=>break,
            }
        }
        if let Some(error) = health.borrow().fault.clone() { return Err(error); }
        if check_requested && !check_done { return Err("requested read check did not finish before capture ended".into()); }
        let summary=json!({"event":"summary", "events":count, "portfolios":portfolios, "gaps":gaps,
            "fill_candidates":evidence.fill_candidates,"unclassified":evidence.unclassified,
            "ws_trades_pending_validation":ws_validation.pending_count(),
            "ws_trades_verified":ws_validation.verified_count(),
            "ws_trades_unqualified":ws_validation.unqualified,
            "fresh_portfolio":evidence.portfolio(now_ms()?).is_some(),
            "private_connections":connections[0],"reference_connections":connections[1],"command_connections":connections[2],
            "rejected_handshakes":health.borrow().rejected_handshakes,
            "read_check_completed":check_done,
            "account_identity_verified":native_requested && check_done,
            "account_inventory_reconciled":false,"can_submit_orders":false,
            "recording_finished":true,"live_execution_validated":false});
        writer.try_send(summary.to_string()).map_err(|_| "recording queue full or closed")?;
        Ok(summary)
    }.await;
    bridge.shutdown().await;
    if let Some(feed) = account_feed {
        feed.shutdown().await?;
    }
    result
}
