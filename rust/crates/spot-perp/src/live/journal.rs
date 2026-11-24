//! One run owner; every new order identity/reservation is durable before transport.
use super::{Config, Result};
use crate::d;
use perp_arb_binance_spot::orders::Intent;
use perp_arb_core::Decimal as D;
use perp_arb_private_events::orders::Order;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Maker {
    pub order: Order,
    pub started: bool,
    pub id: Option<String>,
    pub filled: String,
    pub terminal: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hedge {
    pub intent: Intent,
    pub started: bool,
    pub filled: String,
    pub terminal: bool,
    pub base_delta: Option<String>,
    pub fees: std::collections::BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct State {
    pub config: Config,
    pub spot_account: String,
    pub makers: Vec<Maker>,
    pub hedges: Vec<Hedge>,
    pub finished: bool,
}
pub fn add(a: D, b: D) -> Result<D> {
    let n = a.checked_add(b).ok_or("quantity overflow")?;
    if n.checked_sub(a) != Some(b) {
        return Err("quantity precision loss".into());
    }
    Ok(n)
}
impl State {
    pub fn filled(&self) -> Result<D> {
        self.makers
            .iter()
            .try_fold(D::ZERO, |a, m| add(a, d(&m.filled)?))
    }
    pub fn hedged(&self) -> Result<D> {
        self.hedges
            .iter()
            .try_fold(D::ZERO, |a, h| add(a, d(&h.filled)?))
    }
    pub fn reserved(&self) -> Result<D> {
        self.hedges.iter().try_fold(D::ZERO, |a, h| {
            add(
                a,
                d(if h.terminal {
                    &h.filled
                } else {
                    &h.intent.quantity
                })?,
            )
        })
    }
    pub fn due(&self) -> Result<D> {
        add(self.filled()?, -self.reserved()?)
    }
    /// Only this run's deltas; never compares or repairs pre-existing inventory.
    pub fn net_base_delta(&self) -> Result<Option<D>> {
        let mut spot = D::ZERO;
        for h in &self.hedges {
            let Some(delta) = &h.base_delta else {
                return Ok(None);
            };
            spot = add(spot, d(delta)?)?;
        }
        let perp = if self.config.mode == crate::policy::Mode::Open {
            -self.filled()?
        } else {
            self.filled()?
        };
        Ok(Some(add(spot, perp)?))
    }
    pub fn settled(&self) -> Result<bool> {
        Ok(self.makers.iter().all(|m| !m.started || m.terminal)
            && self
                .hedges
                .iter()
                .all(|h| h.terminal && h.base_delta.is_some())
            && self.filled()? == self.hedged()?)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", deny_unknown_fields)]
pub enum Entry {
    Prepared {
        config: Box<Config>,
        spot_account: String,
    },
    Maker {
        order: Order,
    },
    Started {
        index: usize,
    },
    Bound {
        index: usize,
        id: String,
    },
    Fill {
        index: usize,
        quantity: String,
        terminal: bool,
    },
    Hedge {
        intent: Intent,
    },
    HedgeStarted {
        index: usize,
    },
    Hedged {
        index: usize,
        quantity: String,
        terminal: bool,
        base_delta: Option<String>,
        fees: std::collections::BTreeMap<String, String>,
    },
    Finished,
}
fn apply(s: &mut State, e: &Entry) -> Result<()> {
    if s.finished {
        return Err("run already finished".into());
    }
    match e {
        Entry::Prepared { .. } => return Err("duplicate header".into()),
        Entry::Maker { order } => {
            order.validate().map_err(|e| e.to_string())?;
            if s.makers.len() >= 1000
                || !s.settled()?
                || order.venue != s.config.venue
                || order.account != s.config.account
                || order.symbol != s.config.native_symbol
                || order.side != s.config.maker_side()
                || order.reduce_only != (s.config.mode == crate::policy::Mode::Close)
                || order.asset != s.config.asset
                || order.network != perp_arb_private_events::orders::Network::Mainnet
                || order.tif != perp_arb_private_events::orders::TimeInForce::PostOnly
                || add(s.filled()?, d(&order.quantity)?)? > d(&s.config.total)?
                || s.makers
                    .iter()
                    .any(|m| m.order.client_id == order.client_id)
            {
                return Err("invalid maker reservation".into());
            }
            s.makers.push(Maker {
                order: order.clone(),
                started: false,
                id: None,
                filled: "0".into(),
                terminal: false,
            });
        }
        Entry::Started { index } => {
            let m = s.makers.get_mut(*index).ok_or("missing maker")?;
            if m.started {
                return Err("maker already dispatched".into());
            }
            m.started = true;
        }
        Entry::Bound { index, id } => {
            let m = s.makers.get_mut(*index).ok_or("missing maker")?;
            if !m.started
                || id.parse::<u64>().is_err()
                || m.id.as_ref().is_some_and(|old| old != id)
            {
                return Err("maker identity conflict".into());
            }
            m.id = Some(id.clone());
        }
        Entry::Fill {
            index,
            quantity,
            terminal,
        } => {
            let m = s.makers.get_mut(*index).ok_or("missing maker")?;
            let q = d(quantity)?;
            if !m.started
                || m.id.is_none()
                || q < d(&m.filled)?
                || q > d(&m.order.quantity)?
                || (m.terminal && (q != d(&m.filled)? || !terminal))
            {
                return Err("maker fill conflict".into());
            }
            m.filled = quantity.clone();
            m.terminal = *terminal;
        }
        Entry::Hedge { intent } => {
            intent.validate().map_err(|e| e.to_string())?;
            if s.hedges.len() >= 10000
                || s.hedges
                    .iter()
                    .any(|h| !h.terminal || h.intent.client_id == intent.client_id)
                || intent.symbol != format!("{}USDT", s.config.symbol)
                || intent.side != s.config.hedge_side()
                || intent.kind != perp_arb_binance_spot::orders::Kind::Market
                || d(&intent.quantity)? > s.due()?
            {
                return Err("hedge would exceed confirmed unhedged fills".into());
            }
            s.hedges.push(Hedge {
                intent: intent.clone(),
                started: false,
                filled: "0".into(),
                terminal: false,
                base_delta: None,
                fees: Default::default(),
            });
        }
        Entry::HedgeStarted { index } => {
            let h = s.hedges.get_mut(*index).ok_or("missing hedge")?;
            if h.started {
                return Err("hedge already dispatched".into());
            }
            h.started = true;
        }
        Entry::Hedged {
            index,
            quantity,
            terminal,
            base_delta,
            fees,
        } => {
            let h = s.hedges.get_mut(*index).ok_or("missing hedge")?;
            let q = d(quantity)?;
            if q < d(&h.filled)?
                || q > d(&h.intent.quantity)?
                || (h.terminal && (q != d(&h.filled)? || !terminal))
                || (base_delta.is_some() && !terminal)
            {
                return Err("hedge fill conflict".into());
            }
            if let Some(b) = base_delta {
                d(b)?;
            }
            for fee in fees.values() {
                if d(fee)? < D::ZERO {
                    return Err("invalid commission".into());
                }
            }
            h.filled = quantity.clone();
            h.terminal = *terminal;
            h.base_delta = base_delta.clone();
            h.fees = fees.clone();
        }
        Entry::Finished => {
            if !s.settled()? {
                return Err("unsettled run".into());
            }
            s.finished = true;
        }
    }
    Ok(())
}
struct Journal {
    file: File,
    state: State,
    hash: String,
    sequence: usize,
    poisoned: bool,
}
#[derive(Clone)]
pub struct Store(Arc<Mutex<Journal>>);
impl Store {
    pub async fn create(path: std::path::PathBuf, config: Config, account: String) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .create_new(true)
                .open(path)
                .map_err(|_| "cannot create run journal")?;
            file.try_lock().map_err(|_| "run locked")?;
            let state = State {
                config: config.clone(),
                spot_account: account.clone(),
                makers: vec![],
                hedges: vec![],
                finished: false,
            };
            let mut j = Journal {
                file,
                state,
                hash: String::new(),
                sequence: 0,
                poisoned: false,
            };
            j.write(&Entry::Prepared {
                config: Box::new(config),
                spot_account: account,
            })?;
            Ok(Self(Arc::new(Mutex::new(j))))
        })
        .await
        .map_err(|_| "journal worker failed")?
    }
    pub async fn open(path: std::path::PathBuf) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::open_sync(&path))
            .await
            .map_err(|_| "journal worker failed")?
    }
    fn open_sync(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map_err(|_| "cannot open run journal")?;
        file.try_lock().map_err(|_| "run locked")?;
        if file.metadata().map_err(|_| "journal metadata")?.len() > 64_000_000 {
            return Err("journal too large".into());
        }
        let mut raw = String::new();
        file.read_to_string(&mut raw).map_err(|_| "journal read")?;
        if !raw.ends_with('\n') {
            return Err("torn journal: do not start a new run; reconcile manually".into());
        }
        let mut hash = String::new();
        let mut state = None;
        let mut sequence = 0;
        for line in raw.lines() {
            if line.len() > 65536 {
                return Err("journal record too large".into());
            }
            let v: serde_json::Value = serde_json::from_str(line).map_err(|_| "invalid journal")?;
            let next = checksum(&hash, &v["entry"]);
            if v["sequence"].as_u64() != Some(sequence) || v["hash"] != next {
                return Err("journal checksum/sequence failure".into());
            }
            let e: Entry =
                serde_json::from_value(v["entry"].clone()).map_err(|_| "invalid journal entry")?;
            if sequence == 0 {
                let Entry::Prepared {
                    config,
                    spot_account,
                } = e
                else {
                    return Err("missing journal header".into());
                };
                config.validate()?;
                state = Some(State {
                    config: *config,
                    spot_account,
                    makers: vec![],
                    hedges: vec![],
                    finished: false,
                });
            } else {
                apply(state.as_mut().ok_or("missing state")?, &e)?;
            }
            hash = next;
            sequence += 1;
        }
        Ok(Self(Arc::new(Mutex::new(Journal {
            file,
            state: state.ok_or("empty journal")?,
            hash,
            sequence: sequence as usize,
            poisoned: false,
        }))))
    }
    pub fn state(&self) -> Result<State> {
        Ok(self.0.lock().map_err(|_| "journal poisoned")?.state.clone())
    }
    pub async fn record(&self, entry: Entry) -> Result<State> {
        let j = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut j = j.lock().map_err(|_| "journal poisoned")?;
            let mut next = j.state.clone();
            apply(&mut next, &entry)?;
            j.write(&entry)?;
            j.state = next;
            Ok(j.state.clone())
        })
        .await
        .map_err(|_| "journal worker failed")?
    }
}
impl Journal {
    fn write(&mut self, e: &Entry) -> Result<()> {
        if self.poisoned || self.sequence >= 100000 {
            return Err("journal poisoned/capacity reached".into());
        }
        let entry = serde_json::to_value(e).map_err(|_| "journal encode")?;
        let hash = checksum(&self.hash, &entry);
        let row = format!(
            "{}\n",
            serde_json::json!({"entry":entry,"sequence":self.sequence,"hash":hash})
        );
        if row.len() > 65536
            || self.file.metadata().map_err(|_| "journal metadata")?.len() + row.len() as u64
                > 64_000_000
        {
            return Err("journal capacity".into());
        }
        self.poisoned = true;
        self.file
            .write_all(row.as_bytes())
            .and_then(|_| self.file.sync_all())
            .map_err(|_| "journal write failed; no further dispatch")?;
        self.hash = hash;
        self.sequence += 1;
        self.poisoned = false;
        Ok(())
    }
}
fn checksum(previous: &str, value: &serde_json::Value) -> String {
    let mut h = Sha256::new();
    h.update(previous.as_bytes());
    h.update(value.to_string().as_bytes());
    format!("{:x}", h.finalize())
}
