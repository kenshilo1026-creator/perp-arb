//! Bounded public recorder. Disk serialization runs on a separate thread.
use crate::{FeedState, Update, Venue};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    sync::mpsc::{sync_channel, SyncSender},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::{sleep_until, timeout, timeout_at, Instant as Deadline};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};

fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as i64
}
fn emit(tx: &SyncSender<Value>, event: Value) -> Result<(), String> {
    tx.try_send(event)
        .map_err(|e| format!("recording incomplete: {e}"))
}

#[derive(Default)]
struct Stats {
    frames: u64,
    accepted: u64,
    errors: u64,
    reconnects: u64,
    parse_ns: Vec<u64>,
}
impl Stats {
    fn summary(&mut self, venue: Venue) -> Value {
        self.parse_ns.sort_unstable();
        let percentile = |p: usize| {
            self.parse_ns
                .get(self.parse_ns.len().saturating_sub(1) * p / 100)
                .copied()
        };
        json!({"venue":venue, "frames":self.frames, "accepted":self.accepted,
            "errors":self.errors, "reconnects":self.reconnects,
            "parse_and_cache_ns_p50":percentile(50), "parse_and_cache_ns_p95":percentile(95),
            "parse_and_cache_ns_p99":percentile(99)})
    }
}

async fn feed(
    venue: Venue,
    symbol: String,
    tx: SyncSender<Value>,
    deadline: Deadline,
    origin: Instant,
) -> Result<Value, String> {
    let url = venue.endpoint(&symbol)?;
    let mut state = FeedState::default();
    let mut stats = Stats::default();
    let mut backoff = 1;
    let mut attempts = 0;
    while Deadline::now() < deadline {
        if attempts > 0 {
            stats.reconnects += 1;
        }
        attempts += 1;
        emit(
            &tx,
            json!({"type":"health", "venue":venue, "symbol":symbol, "healthy":false, "received_ms":wall_ms(), "reason":"connecting"}),
        )?;
        let config = WebSocketConfig::default()
            .max_message_size(Some(1_048_576))
            .max_frame_size(Some(1_048_576));
        let connection = timeout_at(
            deadline.min(Deadline::now() + Duration::from_secs(10)),
            connect_async_with_config(&url, Some(config), true),
        )
        .await;
        let mut had_book = false;
        let failure = match connection {
            Ok(Ok((mut socket, _))) => {
                let mut failure = None;
                if let Some(subscription) = venue.subscription(&symbol) {
                    match timeout(
                        Duration::from_secs(5),
                        socket.send(Message::Text(subscription.to_string().into())),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        _ => failure = Some("subscription send failed".to_string()),
                    }
                }
                let mut heartbeat = tokio::time::interval_at(
                    Deadline::now() + Duration::from_secs(20),
                    Duration::from_secs(20),
                );
                while failure.is_none() && Deadline::now() < deadline {
                    tokio::select! {
                        _ = sleep_until(deadline) => break,
                        _ = heartbeat.tick() => {
                            let ping = if venue == Venue::Hyperliquid { Message::Text("{\"method\":\"ping\"}".into()) } else { Message::Ping(Vec::new().into()) };
                            if !matches!(timeout(Duration::from_secs(5), socket.send(ping)).await, Ok(Ok(()))) {
                                failure = Some("heartbeat send failed".into());
                            }
                        },
                        received = timeout(Duration::from_secs(15), socket.next()) => {
                            match received {
                                Ok(Some(Ok(Message::Text(raw)))) => {
                                    let received_ms = wall_ms();
                                    let received_ns = origin.elapsed().as_nanos() as u64;
                                    let start = Instant::now();
                                    let result = state.ingest(venue, &symbol, &raw, received_ms);
                                    let parse_ns = start.elapsed().as_nanos() as u64;
                                    stats.frames += 1;
                                    stats.parse_ns.push(parse_ns);
                                    let (update, error) = match result {
                                        Ok(update) => (Some(update), None),
                                        Err(error) => (None, Some(error)),
                                    };
                                    if update == Some(Update::Accepted) { stats.accepted += 1; had_book = true; }
                                    let book = if update == Some(Update::Accepted) { state.latest().map(|s| s.normalized()) } else { None };
                                    emit(&tx, json!({"type":"frame", "venue":venue, "symbol":symbol,
                                        "received_ms":received_ms, "received_monotonic_ns":received_ns,
                                        "parse_and_cache_ns":parse_ns, "raw":raw.as_str(), "update":update,
                                        "fresh":state.fresh(received_ms).is_some(), "book":book, "error":error}))?;
                                    if error.is_some() { failure = error; }
                                },
                                Ok(Some(Ok(Message::Ping(payload)))) => {
                                    if !matches!(timeout(Duration::from_secs(5), socket.send(Message::Pong(payload))).await, Ok(Ok(()))) {
                                        failure = Some("pong send failed".into());
                                    }
                                },
                                Ok(Some(Ok(Message::Pong(_)))) => {},
                                Ok(Some(Ok(Message::Close(_)))) | Ok(None) => failure = Some("socket closed".into()),
                                Ok(Some(Ok(_))) => failure = Some("unexpected binary market frame".into()),
                                Ok(Some(Err(error))) => failure = Some(error.to_string()),
                                Err(_) => failure = Some("market socket idle timeout".into()),
                            }
                        }
                    }
                }
                let _ = timeout(Duration::from_secs(1), socket.close(None)).await;
                failure
            }
            Ok(Err(error)) => Some(error.to_string()),
            Err(_) => Some("connection timeout".into()),
        };
        state.disconnect();
        if failure.is_some() {
            stats.errors += 1;
        }
        emit(
            &tx,
            json!({"type":"health", "venue":venue, "symbol":symbol, "healthy":false,
            "received_ms":wall_ms(), "reason":failure.as_deref().unwrap_or("stopped")}),
        )?;
        if Deadline::now() >= deadline {
            break;
        }
        if had_book {
            backoff = 1;
        }
        sleep_until(deadline.min(Deadline::now() + Duration::from_secs(backoff))).await;
        backoff = (backoff * 2).min(30);
    }
    Ok(stats.summary(venue))
}

pub async fn record(
    output: &Path,
    seconds: u64,
    aster_symbol: String,
    hyperliquid_coin: String,
) -> Result<Value, String> {
    // Select a concrete TLS provider before attempting any public connection.
    // Another caller may initialize it concurrently; keep that installed provider.
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
    if !(1..=3600).contains(&seconds) {
        return Err("seconds must be 1..3600".into());
    }
    Venue::Aster.endpoint(&aster_symbol)?;
    Venue::Hyperliquid.endpoint(&hyperliquid_coin)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|e| e.to_string())?;
    let (tx, rx) = sync_channel::<Value>(4096);
    let writer = std::thread::spawn(move || -> std::io::Result<()> {
        let mut output = BufWriter::new(file);
        while let Ok(event) = rx.recv() {
            writeln!(output, "{event}")?;
        }
        output.flush()
    });
    let origin = Instant::now();
    let deadline = Deadline::now() + Duration::from_secs(seconds);
    emit(
        &tx,
        json!({"type":"header", "schema":1, "public_only":true, "started_ms":wall_ms(),
        "aster_symbol":aster_symbol, "hyperliquid_coin":hyperliquid_coin, "seconds":seconds}),
    )?;
    let (aster, hyperliquid) = tokio::join!(
        feed(Venue::Aster, aster_symbol, tx.clone(), deadline, origin),
        feed(
            Venue::Hyperliquid,
            hyperliquid_coin,
            tx.clone(),
            deadline,
            origin
        )
    );
    let complete = [&aster, &hyperliquid].iter().all(|r| {
        r.as_ref()
            .is_ok_and(|v| v["accepted"].as_u64().unwrap_or(0) > 0)
    });
    let summary = json!({"type":"footer", "complete":complete, "finished_ms":wall_ms(),
        "venues":[aster.as_ref().ok(), hyperliquid.as_ref().ok()], "errors":[aster.as_ref().err(), hyperliquid.as_ref().err()]});
    let footer_result = emit(&tx, summary.clone());
    drop(tx);
    writer
        .join()
        .map_err(|_| "writer thread panicked")?
        .map_err(|e| e.to_string())?;
    footer_result?;
    aster?;
    hyperliquid?;
    if !complete {
        return Err(format!("both venues must provide snapshots: {summary}"));
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_recording_queue_is_a_failure_instead_of_silent_loss() {
        let (tx, _rx) = sync_channel(1);
        emit(&tx, json!({"first":1})).unwrap();
        assert!(emit(&tx, json!({"second":2})).is_err());
    }

    #[tokio::test]
    async fn existing_recording_is_preserved_and_tls_is_initialized_before_io() {
        let path = std::env::temp_dir().join(format!(
            "perp-arb-recorder-test-{}-{}.jsonl",
            std::process::id(),
            wall_ms()
        ));
        std::fs::write(&path, "preserve existing recording").unwrap();
        let result = record(&path, 1, "ETHUSDT".into(), "ETH".into()).await;
        assert!(result.is_err());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "preserve existing recording"
        );
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        std::fs::remove_file(path).unwrap();
    }
}
