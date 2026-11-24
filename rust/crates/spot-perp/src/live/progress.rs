//! Best-effort diagnostics on a separate writer thread, never blocking fill -> hedge.
use serde_json::{json, Value};
use std::{
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
};

pub struct Progress {
    tx: mpsc::SyncSender<Value>,
    dropped: Arc<AtomicUsize>,
    worker: std::thread::JoinHandle<()>,
}
impl Progress {
    pub fn open(directory: &Path) -> std::io::Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join("process.jsonl"))?;
        let (tx, rx) = mpsc::sync_channel::<Value>(1024);
        let dropped = Arc::new(AtomicUsize::new(0));
        let worker = std::thread::Builder::new()
            .name("spot-progress".into())
            .spawn(move || {
                let mut file_ok = true;
                for row in rx {
                    // A closed terminal must not panic or interrupt execution/hedging.
                    let _ = writeln!(
                        std::io::stdout(),
                        "[{}] {}",
                        row["event"].as_str().unwrap_or("process"),
                        row["message"].as_str().unwrap_or("")
                    );
                    if file_ok && writeln!(file, "{row}").and_then(|_| file.flush()).is_err() {
                        file_ok = false;
                        let _ = writeln!(
                            std::io::stderr(),
                            "cannot write process.jsonl; execution journal remains authoritative"
                        );
                    }
                }
            })?;
        Ok(Self {
            tx,
            dropped,
            worker,
        })
    }
    pub fn event(&self, event: &str, message: String, details: Value) {
        let row = json!({"time_ms":crate::market::wall_ms(), "event":event,"message":message,"details":details});
        if self.tx.try_send(row).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub async fn finish(self) {
        drop(self.tx);
        let _ = tokio::task::spawn_blocking(move || self.worker.join()).await;
        let dropped = self.dropped.load(Ordering::Relaxed);
        if dropped > 0 {
            eprintln!("process diagnostics dropped {dropped} events; execution journal unaffected");
        }
    }
}
