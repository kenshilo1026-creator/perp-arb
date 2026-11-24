use perp_arb_execution::durable::single::Ledger;
use perp_arb_order_test::{
    parse, read_plan, runner, venue::LiveVenue, AccountLock, Command, Result, Store,
};
use perp_arb_private_events::actor;
use std::time::Duration;
const HELP:&str="Rust 單筆永續委託測試（非跨所自動策略）\n\norder-test preview --plan PLAN.json\norder-test submit --plan PLAN.json --journal NEW.wal --live\norder-test inspect --journal EXISTING.wal\norder-test recover --journal EXISTING.wal --live\n\npreview 只讀公開商品資料；inspect 只讀本機帳本。\nsubmit 會送出一筆真實委託，到期撤回剩餘量；成交部分不自動反向平倉。\nrecover 只查證及撤回該 client ID，絕不重新提交委託。\nHyperliquid 支援 testnet/mainnet；Aster 此批只支援 v3 mainnet。\n金鑰只讀程序環境，不載入 .env：ASTER_API_WALLET_PRIVATE_KEY / HYPERLIQUID_PRIVATE_KEY。\n詳細欄位及限制見 rust/crates/order-test/README.md。";
#[tokio::main]
async fn main() {
    match run().await {
        Ok(true) => (),
        Ok(false) => std::process::exit(2),
        Err(e) => {
            eprintln!("order-test: {e}");
            std::process::exit(1);
        }
    }
}
async fn run() -> Result<bool> {
    let Some(command) = parse(&std::env::args().skip(1).collect::<Vec<_>>())? else {
        println!("{HELP}");
        return Ok(true);
    };
    let _process_lock = if matches!(&command, Command::Submit { .. } | Command::Recover { .. }) {
        Some(AccountLock::global_execution()?)
    } else {
        None
    };
    match command {
        Command::Preview { plan } => {
            let p = read_plan(&plan)?;
            let o = p.resolve().await?;
            println!("{}",serde_json::to_string_pretty(&serde_json::json!({"mode":"preview","order":o,"max_notional_usd":p.max_notional_usd,"wait_seconds":p.wait_seconds,"credentials_loaded":false})).map_err(|_|"output serialization")?);
            Ok(true)
        }
        Command::Inspect { journal } => {
            let view =
                tokio::task::spawn_blocking(move || Ledger::open(&journal).map(|l| l.view()))
                    .await
                    .map_err(|_| "journal worker failed")?
                    .map_err(|e| e.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&view).map_err(|_| "output serialization")?
            );
            Ok(true)
        }
        Command::Submit { plan, journal } => {
            if journal.exists() {
                return Err("journal already exists; use inspect/recover, never overwrite".into());
            }
            let p = read_plan(&plan)?;
            let order = p.resolve().await?;
            let _owner = AccountLock::acquire(&order)?;
            let venue = LiveVenue::connect(&order).await?;
            let max = p.max_notional_usd;
            let order_copy = order.clone();
            let ledger =
                tokio::task::spawn_blocking(move || Ledger::create(&journal, order_copy, max))
                    .await
                    .map_err(|_| "journal worker failed")?
                    .map_err(|e| e.to_string())?;
            live(venue, ledger, true, Duration::from_secs(p.wait_seconds)).await
        }
        Command::Recover { journal } => {
            let ledger = tokio::task::spawn_blocking(move || Ledger::open(&journal))
                .await
                .map_err(|_| "journal worker failed")?
                .map_err(|e| e.to_string())?;
            let order = ledger.view().order;
            let _owner = AccountLock::acquire(&order)?;
            let venue = LiveVenue::connect(&order).await?;
            live(venue, ledger, false, Duration::ZERO).await
        }
    }
}
async fn live(venue: LiveVenue, ledger: Ledger, submit: bool, wait: Duration) -> Result<bool> {
    let scope = ledger
        .view()
        .order
        .request()
        .map_err(|e| e.to_string())?
        .scope;
    let (c, worker) = actor::channel(scope, 1024, 4096).map_err(|e| e.to_string())?;
    let worker = worker.spawn();
    let (stop, rx) = tokio::sync::watch::channel(false);
    let signal = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = stop.send(true);
    });
    let monitor = venue.monitor(c.clone()).await;
    let ready = if monitor.is_ok() {
        let mut health = c.watch_health();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let h = health.borrow().clone();
                if h.connected && h.fault.is_none() {
                    return true;
                }
                if health.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
    } else {
        false
    };
    let store = Store::start(ledger);
    let result = if submit && !ready {
        Err(
            "private stream not ready; order was not submitted; inspect the prepared journal"
                .into(),
        )
    } else {
        runner::run(
            &store,
            &venue,
            if ready { Some(&c) } else { None },
            submit,
            wait,
            rx,
        )
        .await
    };
    // Cleanup is explicit even when a journal or transport operation failed.
    let monitor_stop = match monitor {
        Ok(m) => m.shutdown().await,
        Err(e) => {
            eprintln!("private monitor: {e}");
            Ok(())
        }
    };
    let _ = c.shutdown().await;
    let _ = worker.await;
    let store_stop = store.close().await;
    signal.abort();
    match result {
        Ok(report) => {
            let complete = report.completed;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|_| "output serialization")?
            );
            monitor_stop?;
            store_stop?;
            Ok(complete)
        }
        Err(error) => Err(error),
    }
}
