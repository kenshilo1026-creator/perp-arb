use crate::{client::PrivateHttp, Error, Result};
use futures_util::{stream::FuturesUnordered, SinkExt, StreamExt};
use perp_arb_private_events::{actor::Coordinator, OrderKey, Session};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::atomic::Ordering,
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

#[derive(Clone, Debug)]
pub struct Policy {
    pub renew_every: Duration,
    pub lease_valid_for: Duration,
    pub session_max_age: Duration,
    pub heartbeat_every: Duration,
    pub heartbeat_timeout: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    pub poll_every: Duration,
    pub recovery_poll_every: Duration,
    pub retry_every: Duration,
    pub scan_every: Duration,
    pub query_concurrency: usize,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            renew_every: Duration::from_secs(1800),
            lease_valid_for: Duration::from_secs(3300),
            session_max_age: Duration::from_secs(86_100),
            heartbeat_every: Duration::from_secs(20),
            heartbeat_timeout: Duration::from_secs(10),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            poll_every: Duration::from_secs(5),
            recovery_poll_every: Duration::from_millis(500),
            retry_every: Duration::from_millis(500),
            scan_every: Duration::from_millis(250),
            query_concurrency: 2,
        }
    }
}
impl Policy {
    fn validate(&self) -> Result<()> {
        if [
            self.renew_every,
            self.lease_valid_for,
            self.session_max_age,
            self.heartbeat_every,
            self.heartbeat_timeout,
            self.reconnect_min,
            self.poll_every,
            self.recovery_poll_every,
            self.retry_every,
            self.scan_every,
        ]
        .iter()
        .any(|v| v.is_zero() || *v > Duration::from_secs(86_400))
            || self.renew_every >= self.lease_valid_for
            || self.lease_valid_for > Duration::from_secs(3300)
            || self.session_max_age > Duration::from_secs(86_100)
            || self.reconnect_max < self.reconnect_min
            || self.reconnect_max > Duration::from_secs(60)
            || self.query_concurrency == 0
            || self.query_concurrency > 4
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Connecting,
    Streaming,
    Backoff,
    Stopped,
}
#[derive(Debug, Clone)]
pub struct Stats {
    pub phase: Phase,
    pub connection_attempts: u64,
    pub connections: u64,
    pub renewals: u64,
    pub ws_events: u64,
    pub query_successes: u64,
    pub query_failures: u64,
    pub last_error: Option<Error>,
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
    pub fn stats(&self) -> Stats {
        self.stats.borrow().clone()
    }
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

struct Lease(PrivateHttp);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0 .0.active.store(false, Ordering::Release);
    }
}

pub async fn start(
    client: PrivateHttp,
    coordinator: Coordinator,
    policy: Policy,
) -> Result<Monitor> {
    policy.validate()?;
    if coordinator.scope() != &client.scope() {
        return Err(Error::Configuration);
    }
    let transport_lease = coordinator.acquire_transport()?;
    client
        .0
        .active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| Error::AlreadyRunning)?;
    let lease = Lease(client.clone());
    coordinator.transport_lost().await?;
    let (stop, stop_rx) = watch::channel(false);
    let (stats, stats_rx) = watch::channel(Stats {
        phase: Phase::Starting,
        connection_attempts: 0,
        connections: 0,
        renewals: 0,
        ws_events: 0,
        query_successes: 0,
        query_failures: 0,
        last_error: None,
    });
    let stop_task = stop.clone();
    let task = tokio::spawn(async move {
        let _lease = lease;
        let _transport_lease = transport_lease;
        let mut tasks = JoinSet::new();
        tasks.spawn(ws_loop(
            client.clone(),
            coordinator.clone(),
            policy.clone(),
            stop_rx.clone(),
            stats.clone(),
        ));
        tasks.spawn(query_loop(
            client,
            coordinator.clone(),
            policy,
            stop_rx,
            stats.clone(),
        ));
        let mut outcome = Ok(());
        if let Some(result) = tasks.join_next().await {
            outcome = result.unwrap_or(Err(Error::Closed));
        }
        stop_task.send_replace(true);
        while let Some(result) = tasks.join_next().await {
            if outcome.is_ok() {
                outcome = result.unwrap_or(Err(Error::Closed));
            }
        }
        let lost = coordinator.transport_lost().await.map_err(Error::from);
        if outcome.is_ok() {
            outcome = lost;
        }
        stats.send_modify(|s| {
            s.phase = Phase::Stopped;
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

fn fatal(error: Error) -> bool {
    matches!(
        error,
        Error::Credentials | Error::Configuration | Error::Http(401 | 403) | Error::Coordination(_)
    )
}

async fn ws_loop(
    client: PrivateHttp,
    coordinator: Coordinator,
    policy: Policy,
    mut stop: watch::Receiver<bool>,
    stats: watch::Sender<Stats>,
) -> Result<()> {
    let mut attempts = VecDeque::<Instant>::new();
    let mut writes = VecDeque::<Instant>::new();
    let mut backoff = policy.reconnect_min;
    loop {
        let now = Instant::now();
        while attempts
            .front()
            .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
        {
            attempts.pop_front();
        }
        if attempts.len() >= 5 {
            tokio::select! { _ = stopped(&mut stop) => return Ok(()), _ = sleep_until(attempts[0] + Duration::from_secs(60)) => continue }
        }
        if *stop.borrow() {
            return Ok(());
        }
        attempts.push_back(Instant::now());
        stats.send_modify(|s| {
            s.phase = Phase::Connecting;
            s.connection_attempts += 1;
        });
        let connect = async {
            let key = client.listen_key().await?;
            let lease_start = Instant::now();
            let config = WebSocketConfig::default()
                .max_message_size(Some(65_536))
                .max_frame_size(Some(65_536));
            let (socket, _) = timeout(
                Duration::from_secs(10),
                connect_async_with_config(key.socket_url(&client.0.ws_base), Some(config), true),
            )
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Network)?;
            Ok::<_, Error>((socket, lease_start))
        };
        let connection =
            tokio::select! { _ = stopped(&mut stop) => return Ok(()), r = connect => r };
        let outcome = match connection {
            Ok((socket, lease_start)) => {
                let session = coordinator.connected().await?;
                stats.send_modify(|s| {
                    s.phase = Phase::Streaming;
                    s.connections += 1;
                });
                let connected_at = Instant::now();
                let result = read_session(
                    socket,
                    &client,
                    &coordinator,
                    &session,
                    &policy,
                    &mut stop,
                    &stats,
                    &mut writes,
                    lease_start,
                )
                .await;
                coordinator.disconnected(session).await?;
                if connected_at.elapsed() >= Duration::from_secs(60) {
                    backoff = policy.reconnect_min;
                }
                result
            }
            Err(e) => Err(e),
        };
        if *stop.borrow() {
            return Ok(());
        }
        if let Err(error) = outcome {
            stats.send_modify(|s| {
                s.last_error = Some(error);
                s.phase = Phase::Backoff;
            });
            if fatal(error) {
                return Err(error);
            }
        }
        stats.send_modify(|s| s.phase = Phase::Backoff);
        tokio::select! { _ = stopped(&mut stop) => return Ok(()), _ = sleep(backoff) => () }
        backoff = backoff.saturating_mul(2).min(policy.reconnect_max);
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Renewal = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

async fn control(
    socket: &mut Socket,
    writes: &mut VecDeque<Instant>,
    message: Message,
) -> Result<()> {
    let now = Instant::now();
    while writes
        .front()
        .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(1))
    {
        writes.pop_front();
    }
    if writes.len() >= 8 {
        return Err(Error::WsLimit);
    }
    writes.push_back(now);
    timeout(Duration::from_secs(2), socket.send(message))
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Network)
}

#[allow(clippy::too_many_arguments)]
async fn read_session(
    mut socket: Socket,
    client: &PrivateHttp,
    coordinator: &Coordinator,
    session: &Session,
    policy: &Policy,
    stop: &mut watch::Receiver<bool>,
    stats: &watch::Sender<Stats>,
    writes: &mut VecDeque<Instant>,
    lease_start: Instant,
) -> Result<()> {
    let mut renew_at = lease_start + policy.renew_every;
    let mut lease_until = lease_start + policy.lease_valid_for;
    let rotate_at = Instant::now() + policy.session_max_age;
    let mut ping_at = Instant::now() + policy.heartbeat_every;
    let mut awaiting_pong: Option<(Vec<u8>, Instant)> = None;
    let mut heartbeat_id = 0u64;
    let mut renewal: Option<Renewal> = None;
    loop {
        let pong_deadline = awaiting_pong.as_ref().map_or(rotate_at, |(_, time)| *time);
        tokio::select! {
            _ = stopped(stop) => return Ok(()),
            _ = sleep_until(rotate_at) => return Ok(()),
            _ = sleep_until(lease_until.min(pong_deadline)), if awaiting_pong.is_some() || lease_until < rotate_at => return Err(Error::Timeout),
            _ = sleep_until(renew_at), if renewal.is_none() => {
                let client = client.clone();
                renewal = Some(Box::pin(async move { client.renew().await }));
            }
            result = async { match &mut renewal { Some(f) => f.await, None => std::future::pending().await } } => {
                result?;
                renewal = None;
                renew_at = Instant::now() + policy.renew_every;
                lease_until = Instant::now() + policy.lease_valid_for;
                stats.send_modify(|s| s.renewals += 1);
            }
            _ = sleep_until(ping_at), if awaiting_pong.is_none() => {
                heartbeat_id = heartbeat_id.wrapping_add(1);
                let payload = heartbeat_id.to_be_bytes().to_vec();
                control(&mut socket, writes, Message::Ping(payload.clone().into())).await?;
                awaiting_pong = Some((payload, Instant::now() + policy.heartbeat_timeout));
                ping_at = Instant::now() + policy.heartbeat_every;
            }
            message = socket.next() => {
                let received_at = std::time::Instant::now();
                match message {
                    Some(Ok(Message::Text(raw))) => {
                        match coordinator.ingest_aster_ws_at(session.clone(), raw.as_str(), received_at).await {
                            Ok(Some(_)) => stats.send_modify(|s| s.ws_events += 1),
                            Ok(None) => (),
                            Err(perp_arb_private_events::Error::StreamExpired) => return Err(Error::ListenKeyExpired),
                            Err(e) => return Err(e.into()),
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => control(&mut socket, writes, Message::Pong(payload)).await?,
                    Some(Ok(Message::Pong(payload))) => {
                        if awaiting_pong.as_ref().is_some_and(|(expected, _)| expected.as_slice() == payload.as_ref()) { awaiting_pong = None; }
                    }
                    Some(Ok(Message::Binary(_))) => return Err(Error::InvalidPayload),
                    Some(Ok(Message::Frame(_))) => (),
                    _ => return Err(Error::Network),
                }
            }
        }
    }
}

async fn query_loop(
    client: PrivateHttp,
    coordinator: Coordinator,
    policy: Policy,
    mut stop: watch::Receiver<bool>,
    stats: watch::Sender<Stats>,
) -> Result<()> {
    let mut next = BTreeMap::<OrderKey, (Instant, u32)>::new();
    let mut in_flight = BTreeSet::new();
    type QueryJob = Pin<
        Box<dyn Future<Output = (OrderKey, Result<perp_arb_private_events::OrderSnapshot>)> + Send>,
    >;
    let mut jobs = FuturesUnordered::<QueryJob>::new();
    let mut health = coordinator.watch_health();
    let mut generation = health.borrow().generation;
    let mut tick = tokio::time::interval(policy.scan_every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stopping = false;
    loop {
        if *stop.borrow() {
            stopping = true;
        }
        if stopping && jobs.is_empty() {
            return Ok(());
        }
        tokio::select! {
            _ = stopped(&mut stop), if !stopping => { stopping = true; }
            Some((key, result)) = jobs.next(), if !jobs.is_empty() => {
                in_flight.remove(&key);
                let failures = if result.is_ok() { 0 } else { next.get(&key).map_or(1, |(_, n)| n.saturating_add(1)) };
                let delay = if failures == 0 {
                    if result.as_ref().is_ok_and(|s| s.requires_reconciliation) || !coordinator.health().connected {
                        policy.poll_every.min(policy.recovery_poll_every)
                    } else { policy.poll_every }
                } else { policy.retry_every.saturating_mul(1u32 << failures.saturating_sub(1).min(7)).min(Duration::from_secs(60)) };
                next.insert(key, (Instant::now() + delay, failures));
                stats.send_modify(|s| match result { Ok(_) => s.query_successes += 1, Err(e) => { s.query_failures += 1; s.last_error = Some(e); } });
            }
            result = health.changed(), if !stopping => {
                if result.is_err() { stopping = true; }
                let now = health.borrow_and_update().generation;
                if now != generation { next.clear(); generation = now; }
            }
            _ = tick.tick(), if !stopping => {
                let mut candidates = coordinator.snapshots().await?;
                candidates.retain(|o| (!o.status.terminal() || o.requires_reconciliation) && !in_flight.contains(&o.key));
                // Recover active exposure before retained historical terminal records.
                candidates.sort_by_key(|o| (o.status.terminal(), next.get(&o.key).map(|(when,_)| *when)));
                for order in candidates {
                    if *stop.borrow() { stopping = true; break; }
                    if jobs.len() >= policy.query_concurrency { break; }
                    if next.get(&order.key).is_some_and(|(when,_)| *when > Instant::now()) { continue; }
                    let c = coordinator.clone(); let http = client.clone(); let key = order.key;
                    in_flight.insert(key.clone());
                    jobs.push(Box::pin(async move { let result = http.reconcile_order(&c, key.clone()).await; (key, result) }));
                }
            }
        }
        if stopping && jobs.is_empty() {
            return Ok(());
        }
    }
}
