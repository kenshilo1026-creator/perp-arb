//! Per-order dispatch fence. Synchronous disk operations belong on a dedicated
//! blocking worker, not a private WS reader. This is not a two-leg cash ledger.
use crate::{
    events::{self, OrderEvidence},
    orders::Intent,
    Error, Result,
};
use perp_arb_private_events::{lookup::Request, Scope};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    time::Instant,
};

pub struct Permit {
    pub(crate) intent: Intent,
    pub(crate) scope: Scope,
}
#[derive(Debug, Clone)]
pub struct View {
    pub intent: Intent,
    pub scope: Scope,
    pub dispatched: bool,
    pub order: Option<OrderEvidence>,
}
pub struct Journal {
    file: File,
    view: View,
    hash: String,
    poisoned: bool,
    records: usize,
}
impl Journal {
    pub fn create(path: &Path, scope: Scope, intent: Intent) -> Result<Self> {
        intent
            .request(scope.clone())?
            .validate()
            .map_err(|_| Error::Configuration)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(path)
            .map_err(|_| Error::Io)?;
        file.try_lock().map_err(|_| Error::Locked)?;
        let header = json!({"kind":"prepared","schema":1,"account":scope.account,"venue":scope.venue,"intent":intent});
        let mut j = Self {
            file,
            view: View {
                intent,
                scope,
                dispatched: false,
                order: None,
            },
            hash: String::new(),
            poisoned: false,
            records: 0,
        };
        j.append(header)?;
        Ok(j)
    }
    pub fn open(path: &Path, scope: &Scope, expected: &Intent) -> Result<Self> {
        expected.validate()?;
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map_err(|_| Error::Io)?;
        file.try_lock().map_err(|_| Error::Locked)?;
        if file.metadata().map_err(|_| Error::Io)?.len() > 8_000_000 {
            return Err(Error::Capacity);
        }
        let mut raw = String::new();
        file.read_to_string(&mut raw).map_err(|_| Error::Journal)?;
        if !raw.ends_with('\n') {
            return Err(Error::Journal);
        }
        let mut j = Self {
            file,
            view: View {
                intent: expected.clone(),
                scope: scope.clone(),
                dispatched: false,
                order: None,
            },
            hash: String::new(),
            poisoned: false,
            records: 0,
        };
        for (index, line) in raw.lines().enumerate() {
            if line.len() > 16384 {
                return Err(Error::Capacity);
            }
            let record: Value = serde_json::from_str(line).map_err(|_| Error::Journal)?;
            let entry = &record["entry"];
            let hash = checksum(&j.hash, entry);
            if record["hash"] != hash || record["sequence"].as_u64() != Some(index as u64) {
                return Err(Error::Journal);
            }
            if index == 0 {
                if entry
                    != &json!({"kind":"prepared","schema":1,"account":scope.account,"venue":scope.venue,"intent":expected})
                {
                    return Err(Error::Identity);
                }
            } else {
                match entry["kind"].as_str() {
                    Some("dispatched") if !j.view.dispatched => j.view.dispatched = true,
                    Some("observed") if j.view.dispatched => {
                        let e = events::rest(&j.request()?, &entry["order"], Instant::now())?;
                        j.check_observation(&e)?;
                        j.view.order = Some(e);
                    }
                    _ => return Err(Error::Journal),
                }
            }
            j.hash = hash;
            j.records += 1;
        }
        if j.records == 0 {
            return Err(Error::Journal);
        }
        Ok(j)
    }
    fn append(&mut self, entry: Value) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.records >= 20000 {
            return Err(Error::Capacity);
        }
        let hash = checksum(&self.hash, &entry);
        let record = json!({"sequence":self.records,"hash":hash,"entry":entry});
        let text = format!("{record}\n");
        if text.len() > 16384
            || self.file.metadata().map_err(|_| Error::Io)?.len() + text.len() as u64 > 8_000_000
        {
            return Err(Error::Capacity);
        }
        // Poison before any write. A partial write/fsync failure cannot yield a permit.
        self.poisoned = true;
        self.file
            .write_all(text.as_bytes())
            .and_then(|_| self.file.sync_all())
            .map_err(|_| Error::Io)?;
        self.hash = hash;
        self.records += 1;
        self.poisoned = false;
        Ok(())
    }
    pub fn view(&self) -> &View {
        &self.view
    }
    pub fn request(&self) -> Result<Request> {
        self.view.intent.request(self.view.scope.clone())
    }
    pub fn prepare_dispatch(&mut self) -> Result<Permit> {
        if self.view.dispatched {
            return Err(Error::AlreadySubmitted);
        }
        self.append(json!({"kind":"dispatched"}))?;
        self.view.dispatched = true;
        Ok(Permit {
            intent: self.view.intent.clone(),
            scope: self.view.scope.clone(),
        })
    }
    fn check_observation(&self, e: &OrderEvidence) -> Result<()> {
        if !self.view.dispatched {
            return Err(Error::Journal);
        }
        self.request()?
            .verify(&e.found)
            .map_err(|_| Error::Identity)?;
        if let Some(old) = &self.view.order {
            if e.found.order.key != old.found.order.key
                || e.found.order.cumulative < old.found.order.cumulative
                || e.quote < old.quote
                || e.found.order.cumulative == old.found.order.cumulative && e.quote != old.quote
                || old.found.order.status.terminal()
                    && e.found.order.status != old.found.order.status
                    && e.found.order.status != perp_arb_private_events::Status::Filled
                || old.found.order.status == perp_arb_private_events::Status::Filled
                    && e.found.order.status != old.found.order.status
            {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
    /// Persist verified gross execution evidence. Does not imply complete fees,
    /// account reconciliation, or permission to dispatch again after NotFound.
    pub fn observe(&mut self, e: OrderEvidence) -> Result<()> {
        self.check_observation(&e)?;
        let f = &e.found;
        let o = &f.order;
        let record = json!({"kind":"observed","order":{"symbol":o.key.symbol,"orderId":o.key.order_id.parse::<u64>().map_err(|_|Error::Identity)?,
            "clientOrderId":f.client_id,"side":f.side,"origQty":o.original.to_string(),"executedQty":o.cumulative.to_string(),
            "cummulativeQuoteQty":e.quote.to_string(),"status":if o.status==perp_arb_private_events::Status::Pending {"PENDING_NEW"} else {o.status.as_str()},"updateTime":o.event_ms.ok_or(Error::Protocol)?}});
        // Validate the same canonical representation that recovery will read.
        events::rest(&self.request()?, &record["order"], Instant::now())?;
        self.append(record)?;
        self.view.order = Some(e);
        Ok(())
    }
}
fn checksum(previous: &str, entry: &Value) -> String {
    let mut hash = Sha256::new();
    hash.update(previous.as_bytes());
    hash.update(entry.to_string().as_bytes());
    hex::encode(hash.finalize())
}
