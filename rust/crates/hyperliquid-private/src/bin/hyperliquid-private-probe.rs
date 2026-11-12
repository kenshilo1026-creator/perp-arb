//! Explicit opt-in, address-only account monitoring. Does not read private keys.
use perp_arb_hyperliquid_private::{
    client::Client,
    monitor::{self, Policy},
};
use perp_arb_private_events::actor;
use serde_json::json;
use std::time::Duration;
fn options(args: &[String]) -> Result<Option<(u64, bool, bool)>, &'static str> {
    if args == ["--help"] {
        return Ok(None);
    }
    let mut live = false;
    let mut seconds = None;
    let mut recovery = false;
    let mut audit = false;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--live-read-only" if !live => live = true,
            "--audit-account" if !audit => audit = true,
            "--verify-recovery" if !recovery => recovery = true,
            "--seconds" if seconds.is_none() => {
                let value = args
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|v| (1..=3600).contains(v))
                    .ok_or("invalid --seconds")?;
                seconds = Some(value);
            }
            _ => return Err("unknown/duplicate argument; use --help"),
        }
    }
    if !live {
        return Err("requires explicit --live-read-only");
    }
    let seconds = seconds.unwrap_or(if recovery { 90 } else { 30 });
    if recovery && seconds < 70 {
        return Err("recovery verification requires at least 70 seconds");
    }
    Ok(Some((seconds, recovery, audit)))
}
#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), String> {
    let Some((seconds, recovery, audit)) =
        options(&std::env::args().skip(1).collect::<Vec<_>>()).map_err(str::to_string)?
    else {
        println!("hyperliquid-private-probe --live-read-only [--seconds 1..3600] [--verify-recovery] [--audit-account]\nReads HYPERLIQUID_ACCOUNT_ADDRESS only from process environment; no .env or private key reading. Subscribes orders, fills, funding and account ledger changes. Optional audit queries main perp positions, spot/wallet balances and open orders twice, observational only; no independent expected inventory. No exchange/trading endpoint. Recovery verification rotates the local socket every 25 seconds; default 90 seconds, minimum 70. Counters only; Ctrl+C drains in-flight queries.");
        return Ok(());
    };
    let user = std::env::var("HYPERLIQUID_ACCOUNT_ADDRESS")
        .map_err(|_| "HYPERLIQUID_ACCOUNT_ADDRESS is missing")?;
    let client = Client::new(&user).map_err(|e| e.to_string())?;
    let (c, w) = actor::channel(client.scope(), 4096, 256).map_err(|e| e.to_string())?;
    let worker = w.spawn();
    let mut p = Policy::default();
    if recovery {
        p.session_max_age = Duration::from_secs(25);
    }
    let monitor = monitor::start(client.clone(), c.clone(), p)
        .await
        .map_err(|e| e.to_string())?;
    let mut stats = monitor.watch_stats();
    tokio::select! {
        _=tokio::time::sleep(Duration::from_secs(seconds)) => (),
        _=tokio::signal::ctrl_c()=>(),
        _=async { loop {
            if stats.borrow().stopped {break;}
            if stats.changed().await.is_err(){break;}
        } }=>(),
    }
    let audit_result = if audit {
        Some(client.audit_account(&c, &[String::new()], true, None).await)
    } else {
        None
    };
    let account_audit = match &audit_result {
        None => json!(null),
        Some(Err(e)) => json!({"error":e.to_string()}),
        Some(Ok(r)) => {
            json!({"positions":r.inventory().positions.len(),"open_orders":r.inventory().open_orders.len(),"spot_balances":r.inventory().balances.len(),"wallets":r.inventory().wallets.len(),"expected_supplied":r.expected_supplied,"matched":r.matches_expected(),"coverage":"main_perp_and_spot"})
        }
    };
    let health = c.health();
    let result = monitor.shutdown().await;
    let s = stats.borrow().clone();
    let count = c.snapshots().await.map_err(|e| e.to_string())?.len();
    c.shutdown().await.map_err(|e| e.to_string())?;
    worker.await.map_err(|_| "coordinator task failed")?;
    let transport_recovery_passed = recovery
        && s.connections >= 3
        && s.snapshots >= 3
        && s.acknowledgements >= 12
        && s.pongs >= 2
        && s.transport_failures == 0
        && health.connected
        && health.fault.is_none();
    let order_reconciliation_complete = health.reconcile_orders == 0
        && health.unresolved_fill_orders == 0
        && health.unresolved_external_orders == 0
        && health.fault.is_none();
    println!(
        "{}",
        json!({"account_audit":account_audit,"unresolved_external_orders":health.unresolved_external_orders,"mode":"read_only","recovery_verification":recovery,"connections":s.connections,"attempts":s.attempts,
        "transport_recovery_passed":transport_recovery_passed,"order_reconciliation_complete":order_reconciliation_complete,
        "transport_failures":s.transport_failures,"unknown_order_queries":s.unknown_order_queries,
        "acknowledgements":s.acknowledgements,"snapshots":s.snapshots,"new_fills":s.new_fills,"duplicate_fills":s.duplicate_fills,
        "order_updates":s.order_updates,"queries":s.queries,"query_failures":s.query_failures,"pongs":s.pongs,"tracked_orders":count,
        "connected_before_stop":health.connected,"reconciliation_needed_before_stop":health.reconcile_orders,
        "unresolved_fill_orders_before_stop":health.unresolved_fill_orders,"fault":health.fault.map(|e|e.to_string()),
        "last_error":s.last_error.map(|e|e.to_string()),"shutdown_completed":true})
    );
    result.map_err(|e| e.to_string())?;
    if let Some(result) = audit_result {
        result.map_err(|e| e.to_string())?;
    }
    if s.connections == 0 || health.fault.is_some() {
        return Err("private stream acceptance incomplete; inspect counters".into());
    }
    if recovery
        && (!transport_recovery_passed
            || !order_reconciliation_complete
            || s.query_failures != 0
            || s.last_error.is_some())
    {
        return Err("recovery verification incomplete or unhealthy; inspect counters".into());
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
    fn explicit_read_only_arguments_before_any_environment_access() {
        for s in [
            "",
            "--seconds 30",
            "--live-read-only --submit",
            "--live-read-only --live-read-only",
            "--help --live-read-only",
        ] {
            assert!(options(&args(s)).is_err());
        }
        assert!(options(&args("--help")).unwrap().is_none());
        assert_eq!(
            options(&args("--live-read-only")).unwrap(),
            Some((30, false, false))
        );
    }
    #[test]
    fn duration_and_recovery_bounds() {
        for s in [
            "--live-read-only --seconds 0",
            "--live-read-only --seconds 3601",
            "--live-read-only --verify-recovery --seconds 60",
        ] {
            assert!(options(&args(s)).is_err());
        }
        assert_eq!(
            options(&args("--live-read-only --verify-recovery")).unwrap(),
            Some((90, true, false))
        );
    }
}
