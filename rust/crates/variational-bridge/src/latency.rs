//! Bounded in-memory timing trace. Never performs filesystem I/O on the hot path.
//! All *_us offsets share the bridge's monotonic clock; capture wall time is
//! diagnostic only and must not be subtracted from these offsets.
use crate::Result;
use serde_json::{json, Value};
use std::{io::Write, path::Path, sync::Mutex, time::Instant};

pub struct Trace {
    start: Instant,
    rows: Mutex<(Vec<Value>, usize)>,
}
impl Trace {
    pub fn new(start: Instant) -> Self {
        Self {
            start,
            rows: Mutex::new((Vec::new(), 0)),
        }
    }
    pub fn at(&self, at: Instant) -> u64 {
        at.checked_duration_since(self.start)
            .unwrap_or_default()
            .as_micros()
            .min(u64::MAX as u128) as u64
    }
    pub fn mark(&self, stage: &str, id: &str, data: Value) {
        self.mark_at(self.at(Instant::now()), stage, id, data);
    }
    pub fn mark_at(&self, at_us: u64, stage: &str, id: &str, data: Value) {
        if let Ok(mut rows) = self.rows.lock() {
            if rows.0.len() < 32768 {
                rows.0
                    .push(json!({"stage":stage,"id":id,"at_us":at_us,"data":data}));
            } else {
                rows.1 += 1;
            }
        }
    }
    pub fn save(&self, run: &Path) -> Result<std::path::PathBuf> {
        let path = run.join(format!(
            "latency-{}-{}.jsonl",
            crate::wire::now_ms()?,
            std::process::id()
        ));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| format!("latency output: {e}"))?;
        let rows = self.rows.lock().map_err(|_| "latency lock poisoned")?;
        writeln!(file,"{}",json!({"schema":1,"clock":"bridge_monotonic_microseconds","dropped":rows.1,"note":"submit_call precedes native signing/HTTP; ACK is not fill. No cross-venue fill-time guarantee."})).map_err(|e|e.to_string())?;
        for row in &rows.0 {
            writeln!(file, "{row}").map_err(|e| e.to_string())?;
        }
        file.sync_all().map_err(|e| e.to_string())?;
        Ok(path)
    }
}
