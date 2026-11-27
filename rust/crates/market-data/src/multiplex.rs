use super::{wall_ms, Entry};
use crate::{
    catalog::{resolve_aster, resolve_hyperliquid, SymbolMappings},
    metadata::MetadataCache,
    parse_payload,
    shared::{MarketKey, MarketState},
    Update, Venue,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{sleep_until, timeout, Instant},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};

#[derive(Clone, Default)]
pub(super) struct Desired(pub HashMap<MarketKey, Entry>);
#[derive(Debug, Clone, Default, Serialize)]
pub struct HubStats {
    pub connections_started: u64,
    pub controls_sent: u64,
    pub control_acks: u64,
    pub heartbeat_frames_sent: u64,
    pub routed_frames: u64,
    pub ignored_frames: u64,
    pub active_markets: usize,
    pub faults: u64,
    pub handovers: u64,
    pub last_error: Option<String>,
}
#[derive(Clone, Copy)]
pub(super) struct Timing {
    pub connect: Duration,
    pub send: Duration,
    pub idle: Duration,
    pub backoff: Duration,
    pub max_backoff: Duration,
    pub heartbeat: Duration,
    pub ack: Duration,
    pub maintenance: Duration,
    pub control_spacing: Option<Duration>,
    pub connection_window: Duration,
}
impl Default for Timing {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            send: Duration::from_secs(2),
            idle: Duration::from_secs(15),
            backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            heartbeat: Duration::from_secs(20),
            ack: Duration::from_secs(5),
            maintenance: Duration::from_millis(50),
            control_spacing: None,
            connection_window: Duration::from_secs(60),
        }
    }
}

/// Owner-local rolling window survives reconnects. Normal commands leave two
/// slots for protocol pong; overload drops the socket without further writes.
pub(super) struct Pacer {
    history: VecDeque<Instant>,
    cap: usize,
    window: Duration,
    spacing: Duration,
    next: Instant,
}
impl Pacer {
    pub fn new(cap: usize, window: Duration, spacing: Duration) -> Self {
        Self {
            history: VecDeque::new(),
            cap,
            window,
            spacing,
            next: Instant::now(),
        }
    }
    pub fn ready(&mut self, normal: bool) -> Instant {
        let now = Instant::now();
        while self
            .history
            .front()
            .is_some_and(|t| now.duration_since(*t) >= self.window)
        {
            self.history.pop_front();
        }
        let limit = if normal {
            self.cap.saturating_sub(2).max(1)
        } else {
            self.cap
        };
        let quota = if self.history.len() >= limit {
            self.history[self.history.len() - limit] + self.window
        } else {
            now
        };
        if normal {
            quota.max(self.next)
        } else {
            quota
        }
    }
    pub fn take(&mut self, normal: bool) -> bool {
        if self.ready(normal) > Instant::now() {
            return false;
        }
        let now = Instant::now();
        self.history.push_back(now);
        if normal {
            self.next = now + self.spacing;
        }
        true
    }
}
struct Market {
    entry: Entry,
    state: MarketState,
    wanted: bool,
    eligible: bool,
    wire: u8,
    deadline: Option<Instant>,
}
impl Market {
    fn publish(&self) {
        self.entry
            .tx
            .send_replace(Arc::new(self.state.view().clone()));
    }
    fn clear(&mut self, why: &str) {
        self.state.disconnect(self.state.view().generation, why);
        self.deadline = None;
        self.publish();
    }
}
#[derive(Clone)]
struct Control {
    key: MarketKey,
    add: bool,
    mask: u8,
    id: u64,
    deadline: Instant,
}
impl Control {
    fn value(&self) -> Value {
        if self.key.venue() == Venue::Aster {
            let symbol = self.key.symbol().to_ascii_lowercase();
            json!({"method":if self.add {"SUBSCRIBE"} else {"UNSUBSCRIBE"},"params":[format!("{symbol}@depth10@100ms"),format!("{symbol}@bookTicker")],"id":self.id})
        } else {
            let mut subscription =
                json!({"type":if self.mask==1 {"l2Book"} else {"bbo"},"coin":self.key.symbol()});
            if self.mask == 1 {
                subscription["fast"] = json!(true);
            }
            json!({"method":if self.add {"subscribe"} else {"unsubscribe"},"subscription":subscription})
        }
    }
    fn matches(&self, v: &Value) -> bool {
        if self.key.venue() == Venue::Aster {
            v["id"].as_u64() == Some(self.id) && v.get("result").is_some_and(Value::is_null)
        } else {
            let d = &v["data"];
            let s = &d["subscription"];
            v["channel"] == "subscriptionResponse"
                && d["method"] == if self.add { "subscribe" } else { "unsubscribe" }
                && s["coin"] == self.key.symbol()
                && s["type"] == if self.mask == 1 { "l2Book" } else { "bbo" }
                && (self.mask != 1 || s["fast"] == true)
        }
    }
}
pub(super) struct Engine {
    venue: Venue,
    binding: Option<(MetadataCache, SymbolMappings)>,
    metadata: Option<Arc<Value>>,
    timing: Timing,
    markets: HashMap<MarketKey, Market>,
    seen: HashMap<MarketKey, u64>,
    outgoing: Pacer,
    connects: Pacer,
    pending: Option<Control>,
    next_id: u64,
    stats: HubStats,
    stats_tx: watch::Sender<HubStats>,
}
impl Engine {
    pub fn new(
        venue: Venue,
        binding: Option<(MetadataCache, SymbolMappings)>,
        timing: Timing,
        stats_tx: watch::Sender<HubStats>,
    ) -> Self {
        let (cap, window, spacing) = if venue == Venue::Aster {
            (8, Duration::from_secs(1), Duration::from_millis(250))
        } else {
            (600, Duration::from_secs(60), Duration::from_millis(100))
        };
        Self {
            venue,
            binding,
            metadata: None,
            timing,
            markets: HashMap::new(),
            seen: HashMap::new(),
            outgoing: Pacer::new(cap, window, timing.control_spacing.unwrap_or(spacing)),
            connects: Pacer::new(5, timing.connection_window, Duration::ZERO),
            pending: None,
            next_id: 0,
            stats: HubStats::default(),
            stats_tx,
        }
    }
    fn report(&self) {
        self.stats_tx.send_replace(self.stats.clone());
    }
    fn clear_all(&mut self, reason: &str) {
        for market in self.markets.values_mut() {
            market.clear(reason);
        }
    }
    fn wanted(&self) -> bool {
        self.markets.values().any(|m| m.wanted)
    }
    fn sync(&mut self, desired: &Desired, connected: bool) -> Result<(), String> {
        let snapshot = self.binding.as_ref().and_then(|(c, _)| c.current().ok());
        let current = snapshot.as_ref().map(|s| s.payload.clone());
        let metadata_changed = match (&self.metadata, &current) {
            (Some(a), Some(b)) => !Arc::ptr_eq(a, b),
            (None, None) => false,
            _ => true,
        };
        self.metadata = current;
        let mut fence = false;
        for (key, market) in &mut self.markets {
            if metadata_changed {
                market.eligible = eligible(key, &self.binding, &self.metadata);
            }
            let allowed = desired
                .0
                .get(key)
                .is_some_and(|e| e.id == market.entry.id && e.tx.receiver_count() > 0)
                && market.eligible;
            if market.wanted && !allowed {
                market.clear("subscription removed or metadata unavailable");
            }
            if !market.wanted && allowed && self.seen.contains_key(key) {
                fence = true;
            }
            market.wanted = allowed;
        }
        for (key, entry) in &desired.0 {
            if entry.tx.receiver_count() == 0 {
                continue;
            }
            if self
                .markets
                .get(key)
                .is_some_and(|m| m.entry.id == entry.id)
            {
                continue;
            }
            if let Some(old) = self.markets.get_mut(key) {
                old.clear("subscription replaced");
            }
            if self.seen.contains_key(key) {
                fence = true;
            }
            let allowed = eligible(key, &self.binding, &self.metadata);
            let mut state = MarketState::new(key.clone());
            state.begin();
            let mut market = Market {
                entry: entry.clone(),
                state,
                wanted: allowed,
                eligible: allowed,
                wire: 0,
                deadline: None,
            };
            if !allowed {
                market.clear("metadata unavailable or instrument rejected");
            } else {
                market.publish();
            }
            self.markets.insert(key.clone(), market);
        }
        self.markets.retain(|key, m| {
            desired
                .0
                .get(key)
                .is_some_and(|e| e.id == m.entry.id && e.tx.receiver_count() > 0)
                || (connected
                    && (m.wire != 0 || self.pending.as_ref().is_some_and(|p| p.key == *key)))
        });
        self.stats.active_markets = self.markets.values().filter(|m| m.wanted).count();
        self.report();
        if connected && (fence || metadata_changed || self.seen.len() > 256) {
            return Err("subscription/metadata handover requires new socket".into());
        }
        Ok(())
    }
    fn next_control(&self) -> Option<Control> {
        if self.pending.is_some() {
            return None;
        }
        for add in [false, true] {
            if let Some((key, m)) = self
                .markets
                .iter()
                .filter(|(_, m)| {
                    (add && m.wanted && m.wire != 3) || (!add && !m.wanted && m.wire != 0)
                })
                .min_by_key(|(k, _)| k.symbol())
            {
                let remaining = if add { 3 ^ m.wire } else { m.wire };
                let mask = if self.venue == Venue::Aster {
                    3
                } else if remaining & 1 != 0 {
                    1
                } else {
                    2
                };
                return Some(Control {
                    key: key.clone(),
                    add,
                    mask,
                    id: 0,
                    deadline: Instant::now(),
                });
            }
        }
        None
    }
    fn receive(&mut self, raw: &str) -> Result<(), String> {
        let value = parse_payload(raw)?;
        let ack = if self.venue == Venue::Aster {
            value.get("id").is_some()
        } else {
            value["channel"] == "subscriptionResponse"
        };
        if ack {
            let control = self.pending.as_ref().ok_or("unexpected subscription ACK")?;
            if !control.matches(&value) {
                return Err("mismatched subscription ACK".into());
            }
            let control = self.pending.take().expect("checked pending");
            if let Some(m) = self.markets.get_mut(&control.key) {
                if control.add {
                    m.wire |= control.mask;
                } else {
                    m.wire &= !control.mask;
                }
                if control.add && m.wanted && m.wire == 3 {
                    m.deadline = Some(Instant::now() + self.timing.idle);
                }
            }
            self.stats.control_acks += 1;
            self.report();
            return Ok(());
        }
        if value["channel"] == "pong" {
            return Ok(());
        }
        let data = if (self.venue == Venue::Aster && value.get("stream").is_some())
            || self.venue == Venue::Hyperliquid
        {
            &value["data"]
        } else {
            &value
        };
        let symbol = if self.venue == Venue::Aster {
            if data["e"] != "depthUpdate" && data["e"] != "bookTicker" {
                return Err("unexpected Aster channel".into());
            }
            data["s"].as_str().ok_or("missing market symbol")?
        } else {
            if value["channel"] != "l2Book" && value["channel"] != "bbo" {
                return Err("unexpected HL channel".into());
            }
            data["coin"].as_str().ok_or("missing market coin")?
        };
        let key = MarketKey::new(self.venue, symbol)?;
        let Some(m) = self
            .markets
            .get_mut(&key)
            .filter(|m| m.wanted && m.wire == 3)
        else {
            self.stats.ignored_frames += 1;
            return Ok(());
        };
        if let Some((cache, _)) = &self.binding {
            let fresh = cache.current().map_err(|e| e.to_string())?;
            if self
                .metadata
                .as_ref()
                .is_none_or(|old| !Arc::ptr_eq(old, &fresh.payload))
            {
                return Err("metadata changed before frame dispatch".into());
            }
        }
        let now = wall_ms();
        let result = m.state.ingest_value(m.state.view().generation, &value, now);
        if matches!(&result, Ok(Update::Accepted) | Err(_)) {
            m.publish();
        }
        if result? == Update::Accepted {
            self.stats.routed_frames += 1;
            if m.state.view().fresh(now).is_some() {
                m.deadline = Some(Instant::now() + self.timing.idle);
            }
        }
        self.report();
        Ok(())
    }
    pub async fn run(
        mut self,
        url: String,
        mut desired: watch::Receiver<Desired>,
        mut stop: watch::Receiver<bool>,
    ) {
        let mut backoff = self.timing.backoff;
        loop {
            if *stop.borrow() || stop.has_changed().is_err() || desired.has_changed().is_err() {
                break;
            }
            let _ = self.sync(&desired.borrow_and_update().clone(), false);
            if !self.wanted() {
                tokio::select! {_=stop.changed()=>{},_=desired.changed()=>{},_=tokio::time::sleep(self.timing.maintenance)=>{}}
                continue;
            }
            let ready = self.connects.ready(false);
            tokio::select! {biased;_=stop.changed()=>continue,_=desired.changed()=>continue,_=sleep_until(ready)=>{}}
            if !self.connects.take(false) {
                continue;
            }
            self.stats.connections_started += 1;
            self.report();
            let config = WebSocketConfig::default()
                .max_message_size(Some(1_048_576))
                .max_frame_size(Some(1_048_576));
            let connected = tokio::select! {biased;_=stop.changed()=>break,
            _=no_consumers(&desired,self.timing.maintenance)=>{self.clear_all("no subscribers");continue;},
            result=timeout(self.timing.connect,connect_async_with_config(&url,Some(config),true))=>result};
            self.pending = None;
            self.seen.clear();
            for m in self.markets.values_mut() {
                m.wire = 0;
                m.deadline = None;
                if m.wanted {
                    m.state.begin();
                    m.publish();
                }
            }
            let mut had_fresh = false;
            let result = match connected {
                Ok(Ok((mut socket, _))) => {
                    let session = async {
                        let mut maintenance = tokio::time::interval(self.timing.maintenance);
                        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        let mut heartbeat = Instant::now() + self.timing.heartbeat;
                        loop {
                            let next = self.next_control();
                            let send_at = self.outgoing.ready(true).max(if next.is_some() {
                                Instant::now()
                            } else {
                                heartbeat
                            });
                            tokio::select! {
                                biased;
                                _=desired.changed()=>{
                                    if desired.has_changed().is_err(){return Ok(());}
                                    self.sync(&desired.borrow_and_update().clone(),true)?;
                                    if !self.wanted(){return Ok(());}
                                },
                                _=maintenance.tick()=>{
                                    self.sync(&desired.borrow().clone(),true)?;
                                    if !self.wanted(){return Ok(());}
                                    let now=Instant::now();
                                    if self.pending.as_ref().is_some_and(|p|now>=p.deadline){return Err("subscription ACK timeout".into());}
                                    if self.markets.values().any(|m|m.wanted && m.deadline.is_some_and(|d|now>=d)){return Err("individual market data timeout".into());}
                                    self.markets.retain(|k,m|m.wanted || m.wire!=0 || self.pending.as_ref().is_some_and(|p|p.key==*k));
                                },
                                _=sleep_until(send_at)=>{
                                    if !self.outgoing.take(true){continue;}
                                    let message=if let Some(mut control)=next {
                                        self.next_id=self.next_id.checked_add(1).ok_or("control id overflow")?;
                                        control.id=self.next_id;control.deadline=Instant::now()+self.timing.ack;
                                        if control.add {self.seen.insert(control.key.clone(),self.markets[&control.key].entry.id);}
                                        let msg=Message::Text(control.value().to_string().into());
                                        self.pending=Some(control);self.stats.controls_sent+=1;msg
                                    }else{
                                        heartbeat=Instant::now()+self.timing.heartbeat;self.stats.heartbeat_frames_sent+=1;
                                        if self.venue==Venue::Hyperliquid{Message::Text("{\"method\":\"ping\"}".into())}else{Message::Ping(Vec::new().into())}
                                    };
                                    timeout(self.timing.send,socket.send(message)).await.map_err(|_|"WS send timeout")?.map_err(|e|e.to_string())?;self.report();
                                },
                                frame=socket.next()=>match frame {
                                    Some(Ok(Message::Text(raw)))=>{self.receive(&raw)?;had_fresh|=self.markets.values().any(|m|m.state.view().fresh(wall_ms()).is_some());},
                                    Some(Ok(Message::Ping(payload)))=>{
                                        if !self.outgoing.take(false){return Err("pong budget exhausted".into());}
                                        // Replaces queued automatic pong before another read can flush it.
                                        timeout(self.timing.send,socket.send(Message::Pong(payload))).await.map_err(|_|"pong timeout")?.map_err(|e|e.to_string())?;
                                        self.stats.heartbeat_frames_sent+=1;self.report();
                                    },
                                    Some(Ok(Message::Pong(_)))=>{},
                                    Some(Ok(Message::Close(_)))|None=>return Err("socket closed".into()),
                                    Some(Err(e))=>return Err(e.to_string()),
                                    _=>return Err("unexpected binary market frame".into()),
                                }
                            }
                        }
                    };
                    tokio::select! {biased;_=stop.changed()=>Ok(()),result=session=>result}
                }
                Ok(Err(e)) => Err(e.to_string()),
                Err(_) => Err("connection timeout".into()),
            };
            self.clear_all(
                result
                    .as_ref()
                    .err()
                    .map(String::as_str)
                    .unwrap_or("stopped/no subscribers"),
            );
            if let Err(error) = result {
                if error == "subscription/metadata handover requires new socket"
                    || error == "metadata changed before frame dispatch"
                {
                    self.stats.handovers += 1;
                } else {
                    self.stats.faults += 1;
                }
                self.stats.last_error = Some(error);
            }
            self.report();
            if had_fresh {
                backoff = self.timing.backoff;
            }
            if *stop.borrow() || stop.has_changed().is_err() {
                break;
            }
            tokio::select! {biased;_=stop.changed()=>break,_=tokio::time::sleep(backoff)=>{}}
            backoff = (backoff * 2).min(self.timing.max_backoff);
        }
        self.clear_all("stopped");
        self.stats.active_markets = 0;
        self.report();
    }
}

async fn no_consumers(desired: &watch::Receiver<Desired>, poll: Duration) {
    loop {
        if desired
            .borrow()
            .0
            .values()
            .all(|e| e.tx.receiver_count() == 0)
        {
            return;
        }
        tokio::time::sleep(poll).await;
    }
}

fn eligible(
    key: &MarketKey,
    binding: &Option<(MetadataCache, SymbolMappings)>,
    metadata: &Option<Arc<Value>>,
) -> bool {
    let Some((_, maps)) = binding else {
        return true;
    };
    let Some(payload) = metadata else {
        return false;
    };
    let resolved = match key.venue() {
        Venue::Lighter => crate::lighter::parse_metadata(payload, key.symbol()),
        Venue::Aster => resolve_aster(payload, key.symbol(), maps),
        Venue::Hyperliquid => resolve_hyperliquid(payload, key.symbol(), maps),
    };
    resolved.is_ok_and(|instrument| instrument.key == *key)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn offline_subscription_churn_does_not_retain_removed_markets() {
        let (stats, _) = watch::channel(HubStats::default());
        let mut engine = Engine::new(Venue::Aster, None, Timing::default(), stats);
        for id in 1..1000 {
            let (tx, _rx) = watch::channel(Arc::new(crate::shared::View::default()));
            let key = MarketKey::new(Venue::Aster, &format!("T{id}USDT")).unwrap();
            engine
                .sync(&Desired(HashMap::from([(key, Entry { id, tx })])), false)
                .unwrap();
            assert_eq!(engine.markets.len(), 1);
        }
        engine.sync(&Desired::default(), false).unwrap();
        assert!(engine.markets.is_empty());
    }
}
