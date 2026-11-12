use crate::{client::Client, Error, Result};
use futures_util::{SinkExt, StreamExt};
use perp_arb_private_events::{actor::Coordinator, hyperliquid, OrderKey, Session};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Duration,
};
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
    time::{sleep, sleep_until, timeout, Instant},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};

#[derive(Debug, Clone)]
pub struct Policy {
    pub ack_timeout: Duration,
    pub heartbeat_every: Duration,
    pub heartbeat_timeout: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    pub session_max_age: Duration,
    pub scan_every: Duration,
    pub poll_every: Duration,
    pub recovery_every: Duration,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            ack_timeout: Duration::from_secs(10),
            heartbeat_every: Duration::from_secs(20),
            heartbeat_timeout: Duration::from_secs(10),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            session_max_age: Duration::from_secs(86_100),
            scan_every: Duration::from_millis(100),
            poll_every: Duration::from_secs(5),
            recovery_every: Duration::from_millis(500),
        }
    }
}
impl Policy {
    fn validate(&self) -> Result<()> {
        if [
            self.ack_timeout,
            self.heartbeat_every,
            self.heartbeat_timeout,
            self.reconnect_min,
            self.reconnect_max,
            self.session_max_age,
            self.scan_every,
            self.poll_every,
            self.recovery_every,
        ]
        .iter()
        .any(|v| v.is_zero() || *v > Duration::from_secs(86_400))
            || self.reconnect_min > self.reconnect_max
            || self.reconnect_max > Duration::from_secs(60)
            || self.heartbeat_every + self.heartbeat_timeout >= Duration::from_secs(60)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub transport_failures: u64,
    pub unknown_order_queries: u64,
    pub connections: u64,
    pub attempts: u64,
    pub acknowledgements: u64,
    pub snapshots: u64,
    pub new_fills: u64,
    pub duplicate_fills: u64,
    pub order_updates: u64,
    pub queries: u64,
    pub query_failures: u64,
    pub pongs: u64,
    pub last_error: Option<Error>,
    pub stopped: bool,
}
pub struct Monitor {
    stop: watch::Sender<bool>,
    stats: watch::Receiver<Stats>,
    task: Option<JoinHandle<Result<()>>>,
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
impl Monitor {
    pub fn watch_stats(&self) -> watch::Receiver<Stats> {
        self.stats.clone()
    }
    pub async fn shutdown(mut self) -> Result<()> {
        self.stop.send_replace(true);
        self.task
            .take()
            .ok_or(Error::Closed)?
            .await
            .map_err(|_| Error::Closed)?
    }
}
pub async fn start(client: Client, c: Coordinator, p: Policy) -> Result<Monitor> {
    p.validate()?;
    if c.scope() != &client.scope() {
        return Err(Error::Configuration);
    }
    let lease = c.acquire_transport()?;
    c.transport_lost().await?;
    let (stop, stop_rx) = watch::channel(false);
    let (stats, stats_rx) = watch::channel(Stats::default());
    let stop_task = stop.clone();
    let task = tokio::spawn(async move {
        let _lease = lease;
        let mut tasks = JoinSet::new();
        tasks.spawn(ws_loop(
            client.clone(),
            c.clone(),
            p.clone(),
            stop_rx.clone(),
            stats.clone(),
        ));
        tasks.spawn(query_loop(client, c.clone(), p, stop_rx, stats.clone()));
        let mut outcome = tasks
            .join_next()
            .await
            .unwrap_or(Ok(Err(Error::Closed)))
            .unwrap_or(Err(Error::Closed));
        stop_task.send_replace(true);
        while let Some(result) = tasks.join_next().await {
            if outcome.is_ok() {
                outcome = result.unwrap_or(Err(Error::Closed));
            }
        }
        let lost = c.transport_lost().await.map_err(Error::from);
        if outcome.is_ok() {
            outcome = lost;
        }
        stats.send_modify(|s| {
            s.stopped = true;
            if let Err(e) = outcome {
                s.last_error = Some(e);
            }
        });
        outcome
    });
    Ok(Monitor {
        stop,
        stats: stats_rx,
        task: Some(task),
    })
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
async fn send(socket: &mut Socket, writes: &mut VecDeque<Instant>, message: Message) -> Result<()> {
    let now = Instant::now();
    while writes
        .front()
        .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
    {
        writes.pop_front();
    }
    // All subscribe/ping/pong controls share this budget across reconnects.
    if writes.len() >= 120 {
        return Err(Error::Capacity);
    }
    writes.push_back(now);
    timeout(Duration::from_secs(2), socket.send(message))
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Network)
}
async fn ws_loop(
    client: Client,
    c: Coordinator,
    p: Policy,
    mut stop: watch::Receiver<bool>,
    stats: watch::Sender<Stats>,
) -> Result<()> {
    let mut attempts = VecDeque::<Instant>::new();
    let mut writes = VecDeque::new();
    let mut backoff = p.reconnect_min;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let now = Instant::now();
        while attempts
            .front()
            .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
        {
            attempts.pop_front();
        }
        if attempts.len() >= 5 {
            tokio::select! { _ = stopped(&mut stop) => return Ok(()), _ = sleep_until(attempts[0] + Duration::from_secs(60)) => () }
            continue;
        }
        attempts.push_back(now);
        stats.send_modify(|s| s.attempts += 1);
        let config = WebSocketConfig::default()
            .max_message_size(Some(hyperliquid::MAX_FRAME_BYTES))
            .max_frame_size(Some(hyperliquid::MAX_FRAME_BYTES));
        let connect = timeout(
            Duration::from_secs(10),
            connect_async_with_config(client.ws(), Some(config), true),
        );
        let connection =
            tokio::select! { _ = stopped(&mut stop) => return Ok(()), result = connect => result };
        let connected_at = Instant::now();
        let outcome = match connection {
            Ok(Ok((socket, _))) => {
                session(socket, &client, &c, &p, &mut stop, &stats, &mut writes).await
            }
            Ok(Err(_)) => Err(Error::Network),
            Err(_) => Err(Error::Timeout),
        };
        // Marks both pre-ACK and established connection failures as a gap.
        c.transport_lost().await?;
        if *stop.borrow() {
            return Ok(());
        }
        if let Err(error) = outcome {
            stats.send_modify(|s| {
                s.transport_failures += 1;
                s.last_error = Some(error);
            });
            if matches!(error, Error::Coordination(_) | Error::Configuration) {
                return Err(error);
            }
        }
        if connected_at.elapsed() >= Duration::from_secs(60) {
            backoff = p.reconnect_min;
        }
        tokio::select! { _ = stopped(&mut stop) => return Ok(()), _ = sleep(backoff) => () }
        backoff = backoff.saturating_mul(2).min(p.reconnect_max);
    }
}
async fn admit(
    c: &Coordinator,
    token: &Session,
    raw: &str,
    at: std::time::Instant,
    stats: &watch::Sender<Stats>,
) -> Result<()> {
    let report = c.ingest_hyperliquid_ws_at(token.clone(), raw, at).await?;
    stats.send_modify(|s| {
        s.new_fills += report.new_fills as u64;
        s.duplicate_fills += report.duplicate_fills as u64;
        s.order_updates += report.order_updates as u64;
        s.snapshots += u64::from(report.snapshot);
    });
    Ok(())
}
async fn session(
    mut socket: Socket,
    client: &Client,
    c: &Coordinator,
    p: &Policy,
    stop: &mut watch::Receiver<bool>,
    stats: &watch::Sender<Stats>,
    writes: &mut VecDeque<Instant>,
) -> Result<()> {
    let token = c.connecting().await?;
    for kind in [
        "orderUpdates",
        "userFills",
        "userFundings",
        "userNonFundingLedgerUpdates",
    ] {
        let mut subscription = json!({"type":kind,"user":client.user()});
        if kind == "userFills" {
            subscription["aggregateByTime"] = json!(false);
        }
        send(
            &mut socket,
            writes,
            Message::Text(
                json!({"method":"subscribe","subscription":subscription})
                    .to_string()
                    .into(),
            ),
        )
        .await?;
    }
    let started = Instant::now();
    let mut acks = BTreeSet::<String>::new();
    let mut activated = false;
    let mut snapshot_seen = false;
    let mut next_ping = started + p.heartbeat_every;
    let mut pong_deadline = None;
    let mut ticker = tokio::time::interval(Duration::from_millis(20));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = stopped(stop) => return Ok(()),
            _ = ticker.tick() => {
                let now = Instant::now();
                if !activated && now.duration_since(started) >= p.ack_timeout { return Err(Error::Timeout); }
                if now.duration_since(started) >= p.session_max_age { return Ok(()); }
                if pong_deadline.is_some_and(|t| now >= t) { return Err(Error::Timeout); }
                if now >= next_ping && pong_deadline.is_none() {
                    send(&mut socket, writes, Message::Text("{\"method\":\"ping\"}".into())).await?;
                    pong_deadline = Some(Instant::now() + p.heartbeat_timeout);
                    next_ping = Instant::now() + p.heartbeat_every;
                }
            },
            frame = socket.next() => {
                let at = std::time::Instant::now();
                match frame {
                    Some(Ok(Message::Text(raw))) => {
                        let v = hyperliquid::parse(&raw)?;
                        match v.get("channel").and_then(Value::as_str) {
                            Some("subscriptionResponse") => {
                                let data = &v["data"]; let sub = &data["subscription"];
                                let kind = sub["type"].as_str().ok_or(Error::Protocol)?;
                                if data["method"] != "subscribe" || !matches!(kind,"orderUpdates"|"userFills"|"userFundings"|"userNonFundingLedgerUpdates")
                                    || sub["user"].as_str().is_none_or(|s| !s.eq_ignore_ascii_case(client.user()))
                                    || (kind == "userFills" && sub.get("aggregateByTime").is_some_and(|v| v != false))
                                    || !acks.insert(kind.into()) { return Err(Error::Protocol); }
                                stats.send_modify(|s| s.acknowledgements += 1);
                            },
                            Some("pong") => { if pong_deadline.take().is_some() { stats.send_modify(|s| s.pongs += 1); } },
                            Some("orderUpdates" | "userFills" | "userFundings" | "userNonFundingLedgerUpdates") => {
                                admit(c, &token, &raw, at, stats).await?;
                                if v["channel"] == "userFills" && v["data"]["isSnapshot"] == true { snapshot_seen = true; }
                            },
                            _ => return Err(Error::Protocol),
                        }
                        if !activated && acks.len() == 4 && snapshot_seen {
                            c.activate(token.clone()).await?;
                            activated = true;
                            stats.send_modify(|s| s.connections += 1);
                        }
                    },
                    Some(Ok(Message::Ping(data))) => send(&mut socket, writes, Message::Pong(data)).await?,
                    Some(Ok(Message::Pong(_))) => (), // protocol pong does not acknowledge JSON ping
                    Some(Ok(Message::Close(_))) | None => return Err(Error::Network),
                    Some(Err(_)) => return Err(Error::Network),
                    _ => return Err(Error::Protocol),
                }
            }
        }
    }
}
async fn query_loop(
    client: Client,
    c: Coordinator,
    p: Policy,
    mut stop: watch::Receiver<bool>,
    stats: watch::Sender<Stats>,
) -> Result<()> {
    let mut next = BTreeMap::<OrderKey, (Instant, u32)>::new();
    let mut health = c.watch_health();
    let mut generation = health.borrow().generation;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let current = health.borrow_and_update().generation;
        if current != generation {
            next.clear();
            generation = current;
        }
        let mut candidates: BTreeMap<OrderKey, bool> = c
            .snapshots()
            .await?
            .into_iter()
            .filter(|s| !s.status.terminal() || s.requires_reconciliation)
            .map(|s| (s.key, s.status.terminal()))
            .collect();
        for key in c.hyperliquid_pending().await? {
            if let Some((due, 0)) = next.get_mut(&key) {
                *due = (*due).min(Instant::now() + p.recovery_every);
            }
            candidates.insert(key, false);
        }
        let mut candidates: Vec<_> = candidates.into_iter().collect();
        candidates.sort_by_key(|(key, terminal)| (*terminal, next.get(key).map(|(t, _)| *t)));
        if let Some((key, _)) = candidates
            .into_iter()
            .find(|(k, _)| next.get(k).is_none_or(|(t, _)| *t <= Instant::now()))
        {
            if *stop.borrow() {
                return Ok(());
            }
            // Dedicated task: never blocks socket reads. Finish this lookup on stop.
            let result = client.reconcile(&c, key.clone()).await;
            if let Err(Error::Coordination(e)) = &result {
                if *e != perp_arb_private_events::Error::UnknownOrder {
                    return Err(Error::Coordination(*e));
                }
            }
            let failures = if result.is_ok() {
                0
            } else {
                next.get(&key).map_or(1, |(_, n)| n.saturating_add(1))
            };
            let delay = if failures > 0 {
                p.recovery_every
                    .saturating_mul(1 << failures.saturating_sub(1).min(7))
                    .min(Duration::from_secs(60))
            } else if !c.health().connected
                || result.as_ref().is_ok_and(|s| s.requires_reconciliation)
            {
                p.recovery_every
            } else {
                p.poll_every
            };
            next.insert(key, (Instant::now() + delay, failures));
            stats.send_modify(|s| match result {
                Ok(_) => s.queries += 1,
                Err(e) => {
                    s.query_failures += 1;
                    if e == Error::Coordination(perp_arb_private_events::Error::UnknownOrder) {
                        s.unknown_order_queries += 1;
                    }
                    s.last_error = Some(e);
                }
            });
        } else {
            tokio::select! { _ = stopped(&mut stop) => return Ok(()), _ = sleep(p.scan_every) => (), changed = health.changed() => { if changed.is_err() { return Ok(()); } } }
        }
    }
}
