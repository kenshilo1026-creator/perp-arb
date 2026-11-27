//! Bounded, explicit single-order testing. No strategy, position closing or automatic resubmission.
#![forbid(unsafe_code)]
pub mod runner;
pub mod venue;
use perp_arb_core::{decimal, instruments::ExactAmount, Decimal};
use perp_arb_execution::durable::single::{self, Ledger};
use perp_arb_private_events::orders::{Network, Order, TimeInForce};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};
pub type Result<T> = std::result::Result<T, String>;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub venue: String,
    pub network: Network,
    pub account: String,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub limit_price: String,
    pub tif: TimeInForce,
    pub reduce_only: bool,
    pub max_notional_usd: String,
    pub wait_seconds: u64,
}
impl Plan {
    pub fn validate(&self) -> Result<()> {
        let q = decimal(&self.quantity).map_err(|e| e.to_string())?;
        let p = decimal(&self.limit_price).map_err(|e| e.to_string())?;
        let cap = decimal(&self.max_notional_usd).map_err(|e| e.to_string())?;
        if q <= Decimal::ZERO
            || p <= Decimal::ZERO
            || cap <= Decimal::ZERO
            || self.wait_seconds > 30
            || self.wait_seconds == 0
        {
            return Err("invalid quantity, price, notional cap or wait_seconds (1..30)".into());
        }
        if ExactAmount::product(q, p)
            .and_then(|a| a.compare(cap))
            .map_err(|e| e.to_string())?
            .is_gt()
        {
            return Err("order exceeds max_notional_usd".into());
        }
        self.order(
            if matches!(self.venue.as_str(), "hyperliquid" | "lighter") {
                Some(0)
            } else {
                None
            },
        )?
        .validate()
        .map_err(|e| e.to_string())
    }
    fn order(&self, asset: Option<u32>) -> Result<Order> {
        Ok(Order {
            venue: self.venue.clone(),
            network: self.network,
            account: self.account.to_ascii_lowercase(),
            symbol: self.symbol.clone(),
            side: self.side.clone(),
            quantity: self.quantity.clone(),
            limit_price: self.limit_price.clone(),
            tif: self.tif,
            reduce_only: self.reduce_only,
            client_id: single::new_client_id(&self.venue).map_err(|e| e.to_string())?,
            asset,
        })
    }
    /// Public metadata only; no credentials, orders or account reads.
    pub async fn resolve(&self) -> Result<Order> {
        self.resolve_with_metadata().await.map(|(order, _)| order)
    }
    pub async fn resolve_with_metadata(
        &self,
    ) -> Result<(Order, perp_arb_market_data::catalog::ResolvedInstrument)> {
        use perp_arb_market_data::{
            catalog::{self, SymbolMappings},
            metadata,
        };
        self.validate()?;
        let maps = SymbolMappings::default();
        let resolved = match self.venue.as_str() {
            "lighter" => perp_arb_market_data::lighter::resolve(&self.symbol).await,
            "aster" => {
                let http = metadata::PublicHttp::new(metadata::HttpPolicy::default())
                    .map_err(|e| e.to_string())?;
                let info = http
                    .fetch(metadata::Source::AsterV3)
                    .await
                    .map_err(|e| e.to_string())?;
                catalog::resolve_aster(&info, &self.symbol, &maps)
            }
            "hyperliquid" => {
                let client = venue::hl_client(&self.account, self.network)?;
                let info = client.metadata().await.map_err(|e| e.to_string())?;
                catalog::resolve_hyperliquid(&info, &self.symbol, &maps)
            }
            _ => return Err("unsupported venue".into()),
        }?;
        if resolved.key.symbol() != self.symbol {
            return Err("use the exact native perpetual symbol".into());
        }
        let order = self.order(
            resolved
                .asset_id
                .map(u32::try_from)
                .transpose()
                .map_err(|_| "asset ID overflow")?,
        )?;
        let limits = if order.tif == TimeInForce::Market {
            resolved
                .market
                .as_ref()
                .ok_or("market instrument constraints unavailable")?
        } else {
            &resolved.limit
        };
        if order.tif != TimeInForce::Market
            && limits
                .price_error(order.price().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
                .is_some()
        {
            return Err("price does not match instrument tick/precision".into());
        }
        if let Some(error) = limits
            .size_error(
                order.quantity().map_err(|e| e.to_string())?,
                Some(order.price().map_err(|e| e.to_string())?),
            )
            .map_err(|e| e.to_string())?
        {
            return Err(error.code().into());
        }
        order.validate().map_err(|e| e.to_string())?;
        Ok((order, resolved))
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Preview { plan: PathBuf },
    Submit { plan: PathBuf, journal: PathBuf },
    Inspect { journal: PathBuf },
    Recover { journal: PathBuf },
}
pub fn parse(args: &[String]) -> Result<Option<Command>> {
    if args == ["--help"] {
        return Ok(None);
    }
    let Some(action) = args.first() else {
        return Err("use --help".into());
    };
    let mut plan = None;
    let mut journal = None;
    let mut live = false;
    let mut iter = args[1..].iter();
    while let Some(k) = iter.next() {
        match k.as_str() {
            "--plan" if plan.is_none() => {
                plan = Some(PathBuf::from(iter.next().ok_or("missing plan path")?))
            }
            "--journal" if journal.is_none() => {
                journal = Some(PathBuf::from(iter.next().ok_or("missing journal path")?))
            }
            "--live" if !live => live = true,
            _ => return Err("unknown or duplicate argument".into()),
        }
    }
    match action.as_str() {
        "preview" if !live && journal.is_none() => Ok(Some(Command::Preview {
            plan: plan.ok_or("--plan required")?,
        })),
        "submit" if live => Ok(Some(Command::Submit {
            plan: plan.ok_or("--plan required")?,
            journal: journal.ok_or("--journal required")?,
        })),
        "inspect" if !live && plan.is_none() => Ok(Some(Command::Inspect {
            journal: journal.ok_or("--journal required")?,
        })),
        "recover" if live && plan.is_none() => Ok(Some(Command::Recover {
            journal: journal.ok_or("--journal required")?,
        })),
        _ => Err("submit/recover require --live; inspect/preview do not accept it".into()),
    }
}
pub fn read_plan(path: &Path) -> Result<Plan> {
    let bytes = std::fs::read(path).map_err(|_| "cannot read plan")?;
    if bytes.len() > 16_384 {
        return Err("plan too large".into());
    }
    let p: Plan = serde_json::from_slice(&bytes).map_err(|_| "invalid plan JSON; see example")?;
    p.validate()?;
    Ok(p)
}
/// Account locks plus a global lock shared by the manual Python/Rust CLIs.
pub struct AccountLock {
    _file: File,
}
impl AccountLock {
    pub fn global_execution() -> Result<Self> {
        let path = std::env::temp_dir().join("perp-arb-live-execution.lock");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
        }
        let file = options
            .open(path)
            .map_err(|_| "another Python/Rust execution CLI is running")?;
        file.try_lock()
            .map_err(|_| "another Python/Rust execution CLI is running")?;
        Ok(Self { _file: file })
    }
    pub fn acquire(o: &Order) -> Result<Self> {
        Self::for_scope(&o.venue, o.network, &o.account)
    }
    pub fn for_scope(venue: &str, network: Network, account: &str) -> Result<Self> {
        let bytes = serde_json::to_vec(&(venue, network, account.to_ascii_lowercase()))
            .map_err(|_| "lock identity")?;
        let id = Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let path = std::env::temp_dir().join(format!("perp-arb-order-test-{id}.lock"));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
        }
        let file = options
            .open(path)
            .map_err(|_| "another order-test instance owns this account")?;
        file.try_lock()
            .map_err(|_| "another order-test instance owns this account")?;
        Ok(Self { _file: file })
    }
}
type Job = Box<dyn FnOnce(&mut Ledger) + Send>;
/// Serial journal owner on a blocking thread; socket readers never execute sync_all.
pub struct Store {
    tx: tokio::sync::mpsc::Sender<Job>,
    worker: tokio::task::JoinHandle<()>,
}
impl Store {
    pub fn start(mut ledger: Ledger) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Job>(32);
        let worker = tokio::task::spawn_blocking(move || {
            while let Some(job) = rx.blocking_recv() {
                job(&mut ledger);
            }
        });
        Self { tx, worker }
    }
    pub async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Ledger) -> std::result::Result<T, perp_arb_execution::durable::JournalError>
            + Send
            + 'static,
    ) -> Result<T> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Box::new(move |l| {
                let _ = tx.send(f(l));
            }))
            .await
            .map_err(|_| "journal worker closed")?;
        rx.await
            .map_err(|_| "journal worker closed")?
            .map_err(|e| e.to_string())
    }
    pub async fn close(self) -> Result<()> {
        drop(self.tx);
        self.worker
            .await
            .map_err(|_| "journal worker failed".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_owned).collect()
    }
    #[test]
    fn live_commands_require_explicit_flag_and_no_ambiguous_defaults() {
        for s in [
            "submit --plan x --journal y",
            "recover --journal y",
            "preview --plan x --live",
            "submit --plan x --plan z --journal y --live",
        ] {
            assert!(parse(&args(s)).is_err());
        }
        assert!(parse(&args("submit --plan x --journal y --live")).is_ok());
        assert!(parse(&args("inspect --journal y")).is_ok());
    }
}
