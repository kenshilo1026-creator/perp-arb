//! Explicitly opt-in account monitoring. No order submission/cancel endpoint.
use perp_arb_aster_private::{
    auth::Credentials,
    client::{HttpPolicy, PrivateHttp},
    monitor::{self, Phase, Policy},
};
use perp_arb_core::{decimal, Decimal};
use perp_arb_private_events::{actor, OrderKey, Status, Update};
use serde_json::json;
use std::{collections::BTreeSet, time::Duration};
use zeroize::Zeroizing;

struct Options {
    seconds: u64,
    verify_recovery: bool,
    audit: bool,
    tracked: Vec<(OrderKey, Decimal)>,
}
fn parse(args: &[String]) -> Result<Option<Options>, &'static str> {
    if args == ["--help"] {
        return Ok(None);
    }
    let mut live = false;
    let mut verify_recovery = false;
    let mut audit = false;
    let mut seconds = None;
    let mut tracked = Vec::new();
    let mut seen = BTreeSet::new();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--live-read-only" if !live => live = true,
            "--audit-account" if !audit => audit = true,
            "--verify-recovery" if !verify_recovery => verify_recovery = true,
            "--seconds" if seconds.is_none() => {
                let value = args
                    .next()
                    .ok_or("--seconds requires 1..3600")?
                    .parse::<u64>()
                    .map_err(|_| "--seconds requires 1..3600")?;
                if !(1..=3600).contains(&value) {
                    return Err("--seconds requires 1..3600");
                }
                seconds = Some(value);
            }
            "--track" => {
                let parts: Vec<_> = args
                    .next()
                    .ok_or("--track requires SYMBOL:ORDER_ID:ORIGINAL_QUANTITY")?
                    .split(':')
                    .collect();
                if parts.len() != 3
                    || tracked.len() >= 64
                    || parts[1].is_empty()
                    || !parts[1].bytes().all(|b| b.is_ascii_digit())
                {
                    return Err("invalid --track; maximum 64 unique existing orders");
                }
                let key = OrderKey {
                    symbol: parts[0].into(),
                    order_id: parts[1].into(),
                };
                let original = decimal(parts[2]).map_err(|_| "invalid original quantity")?;
                Update {
                    key: key.clone(),
                    original,
                    cumulative: Decimal::ZERO,
                    status: Status::Pending,
                    event_ms: None,
                }
                .validate()
                .map_err(|_| "invalid --track")?;
                if !seen.insert(key.clone()) {
                    return Err("duplicate --track");
                }
                tracked.push((key, original));
            }
            _ => return Err("unknown or duplicate option; use --help"),
        }
    }
    if !live {
        return Err("requires explicit --live-read-only; use --help");
    }
    let seconds = seconds.unwrap_or(if verify_recovery { 90 } else { 30 });
    if verify_recovery && seconds < 70 {
        return Err("--verify-recovery requires at least 70 seconds");
    }
    Ok(Some(Options {
        seconds,
        verify_recovery,
        audit,
        tracked,
    }))
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), String> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let Some(options) = parse(&args).map_err(str::to_string)? else {
        println!("aster-private-probe --live-read-only [--seconds 1..3600] [--verify-recovery] [--audit-account] [--track SYMBOL:ORDER_ID:ORIGINAL_QUANTITY]\nReads ASTER_API_WALLET_PRIVATE_KEY and ASTER_USER_ADDRESS from the existing process environment only. Does not load .env. Optional ASTER_API_WALLET_ADDRESS must match the derived signer. Only listenKey POST/PUT and read-only order/account GETs are available. Optional audit compares two position, wallet and open-order samples; no independent expected inventory. No orders are submitted or canceled. Prints counters only; Ctrl+C drains outstanding order-status reads. Recovery verification renews every 10 seconds and rotates the socket every 25 seconds; defaults to 90 seconds (minimum 70). No shared listenKey invalidation.");
        return Ok(());
    };
    // No credential lookup before explicit CLI opt-in and argument validation.
    let secret = Zeroizing::new(
        std::env::var("ASTER_API_WALLET_PRIVATE_KEY")
            .map_err(|_| "ASTER_API_WALLET_PRIVATE_KEY is missing")?,
    );
    let user = std::env::var("ASTER_USER_ADDRESS").map_err(|_| "ASTER_USER_ADDRESS is missing")?;
    let credentials = Credentials::new(&secret, &user).map_err(|e| e.to_string())?;
    if let Ok(signer) = std::env::var("ASTER_API_WALLET_ADDRESS") {
        if !signer.is_empty() && !signer.eq_ignore_ascii_case(credentials.signer()) {
            return Err("configured signer does not match private key".into());
        }
    }
    drop(secret);
    let client = PrivateHttp::new(credentials, HttpPolicy::default()).map_err(|e| e.to_string())?;
    let (coordinator, worker) =
        actor::channel(client.scope(), 4096, 256).map_err(|e| e.to_string())?;
    let worker = worker.spawn();
    for (key, quantity) in options.tracked {
        coordinator
            .register(key, quantity)
            .await
            .map_err(|e| e.to_string())?;
    }
    let mut policy = Policy::default();
    if options.verify_recovery {
        policy.renew_every = Duration::from_secs(10);
        policy.session_max_age = Duration::from_secs(25);
    }
    let monitor = monitor::start(client.clone(), coordinator.clone(), policy)
        .await
        .map_err(|e| e.to_string())?;
    let mut stats = monitor.watch_stats();
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(options.seconds)) => (),
        _ = tokio::signal::ctrl_c() => (),
        _ = async {
            loop {
                if stats.borrow().phase == Phase::Stopped { break; }
                if stats.changed().await.is_err() { break; }
            }
        } => (),
    }
    let audit_result = if options.audit {
        Some(client.audit_account(&coordinator, None).await)
    } else {
        None
    };
    let account_audit = match &audit_result {
        None => json!(null),
        Some(Err(e)) => json!({"error":e.to_string()}),
        Some(Ok(r)) => {
            json!({"positions":r.inventory().positions.len(),"open_orders":r.inventory().open_orders.len(),"wallets":r.inventory().wallets.len(),"expected_supplied":r.expected_supplied,"matched":r.matches_expected()})
        }
    };
    let health_before_stop = coordinator.health();
    let result = monitor.shutdown().await;
    let orders = coordinator.snapshots().await.map_err(|e| e.to_string())?;
    coordinator.shutdown().await.map_err(|e| e.to_string())?;
    worker
        .await
        .map_err(|_| "coordinator stopped unexpectedly")?;
    let stats = stats.borrow().clone();
    println!(
        "{}",
        json!({"account_audit":account_audit,"mode":"read_only","recovery_verification":options.verify_recovery,"connections":stats.connections,"connection_attempts":stats.connection_attempts,
        "renewals":stats.renewals,"ws_events":stats.ws_events,"query_successes":stats.query_successes,"query_failures":stats.query_failures,
        "tracked_orders":orders.len(),"connected_before_stop":health_before_stop.connected,
        "reconciliation_needed_before_stop":health_before_stop.reconcile_orders,"fault":health_before_stop.fault.map(|e|e.to_string()),
        "last_error":stats.last_error.map(|e|e.to_string()),"shutdown_completed":true})
    );
    result.map_err(|e| e.to_string())?;
    if let Some(result) = audit_result {
        result.map_err(|e| e.to_string())?;
    }
    if stats.connections == 0 {
        return Err(
            "private stream never became available; inspect the diagnostic counters".into(),
        );
    }
    if options.verify_recovery
        && (stats.connections < 3
            || stats.renewals < 3
            || !health_before_stop.connected
            || health_before_stop.fault.is_some()
            || health_before_stop.reconcile_orders != 0
            || stats.query_failures != 0
            || stats.last_error.is_some())
    {
        return Err(
            "recovery verification incomplete or unhealthy; inspect diagnostic counters".into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_string).collect()
    }
    #[test]
    fn requires_opt_in_and_does_not_accept_unknown_options() {
        for s in [
            "",
            "--seconds 30",
            "--help --live-read-only",
            "--live-read-only --submit",
            "--live-read-only --live-read-only",
        ] {
            assert!(parse(&args(s)).is_err());
        }
        assert!(parse(&args("--help")).unwrap().is_none());
    }
    #[test]
    fn validates_duration_and_existing_order_arguments() {
        for s in [
            "--live-read-only --seconds 0",
            "--live-read-only --seconds 3601",
            "--live-read-only --track ETHUSDT:x:10",
            "--live-read-only --track ETHUSDT:1:0",
            "--live-read-only --track ETHUSDT:1:10 --track ETHUSDT:1:10",
        ] {
            assert!(parse(&args(s)).is_err());
        }
        let o = parse(&args("--live-read-only --seconds 60 --track ETHUSDT:1:10"))
            .unwrap()
            .unwrap();
        assert_eq!(o.seconds, 60);
        assert_eq!(o.tracked.len(), 1);
    }
    #[test]
    fn recovery_verification_is_explicit_and_requires_observation_time() {
        assert!(parse(&args("--verify-recovery")).is_err());
        assert!(parse(&args("--live-read-only --verify-recovery --seconds 60")).is_err());
        assert!(parse(&args(
            "--live-read-only --verify-recovery --verify-recovery"
        ))
        .is_err());
        let o = parse(&args("--live-read-only --verify-recovery"))
            .unwrap()
            .unwrap();
        assert!(o.verify_recovery);
        assert_eq!(o.seconds, 90);
    }
}
