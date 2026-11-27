//! One public socket per venue. No private events, credentials or orders.
use crate::{
    catalog::SymbolMappings,
    metadata::MetadataCache,
    shared::{MarketKey, View},
    Venue,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{sync::watch, task::JoinHandle};

#[path = "multiplex.rs"]
mod multiplex;
pub use multiplex::HubStats;
use multiplex::{Desired, Engine, Timing};

pub fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as i64
}

#[derive(Clone)]
pub struct Subscription {
    key: MarketKey,
    rx: watch::Receiver<Arc<View>>,
}
impl Subscription {
    pub fn key(&self) -> &MarketKey {
        &self.key
    }
    pub fn snapshot(&self) -> Arc<View> {
        self.rx.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Arc<View>, String> {
        self.rx
            .changed()
            .await
            .map_err(|_| "market producer stopped")?;
        Ok(self.rx.borrow_and_update().clone())
    }
}
#[derive(Clone)]
struct Entry {
    id: u64,
    tx: watch::Sender<Arc<View>>,
}
struct Worker {
    desired: watch::Sender<Desired>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
    stats: watch::Receiver<HubStats>,
}

/// Clone subscriptions, not hubs. Budgets are shared within this hub, not across
/// processes or independently constructed hubs. Max 32 native markets/venue.
#[derive(Default)]
pub struct MarketHub {
    lighter_workers: Vec<JoinHandle<()>>,
    entries: HashMap<MarketKey, Entry>,
    workers: HashMap<Venue, Worker>,
    next_id: u64,
    bindings: HashMap<Venue, (MetadataCache, SymbolMappings)>,
}
impl MarketHub {
    pub fn new() -> Self {
        Self::default()
    }
    /// Bind before starting this venue. Caller separately owns metadata refresh.
    pub fn bind_metadata(
        &mut self,
        cache: MetadataCache,
        maps: SymbolMappings,
    ) -> Result<(), String> {
        let venue = cache.source().venue();
        if self.workers.contains_key(&venue) {
            return Err("bind metadata before subscribing this venue".into());
        }
        self.bindings.insert(venue, (cache, maps));
        Ok(())
    }
    pub async fn subscribe(&mut self, key: MarketKey) -> Result<Subscription, String> {
        if key.venue() == Venue::Lighter {
            self.entries.retain(|_, e| e.tx.receiver_count() > 0);
            if let Some(entry) = self.entries.get(&key) {
                return Ok(Subscription {
                    key,
                    rx: entry.tx.subscribe(),
                });
            }
            if self
                .entries
                .keys()
                .filter(|k| k.venue() == Venue::Lighter)
                .count()
                >= 32
            {
                return Err("32 markets per venue limit reached".into());
            }
            let spec = crate::lighter::resolve(key.symbol()).await?;
            let (tx, rx) = watch::channel(Arc::new(View::default()));
            self.lighter_workers.retain(|t| !t.is_finished());
            self.lighter_workers
                .push(tokio::spawn(crate::lighter::stream(spec, tx.clone())));
            self.next_id += 1;
            self.entries.insert(
                key.clone(),
                Entry {
                    id: self.next_id,
                    tx,
                },
            );
            return Ok(Subscription { key, rx });
        }
        let url = match key.venue() {
            Venue::Aster => "wss://fstream.asterdex.com/ws",
            Venue::Hyperliquid => "wss://api.hyperliquid.xyz/ws",
            _ => unreachable!(),
        };
        self.subscribe_at(key, url.into(), Timing::default()).await
    }
    async fn subscribe_at(
        &mut self,
        key: MarketKey,
        url: String,
        timing: Timing,
    ) -> Result<Subscription, String> {
        self.entries.retain(|_, e| e.tx.receiver_count() > 0);
        let venue = key.venue();
        if self
            .workers
            .get(&venue)
            .is_some_and(|w| w.task.is_finished())
        {
            return Err("venue worker stopped; create a new hub after shutdown".into());
        }
        if let Some(entry) = self.entries.get(&key) {
            return Ok(Subscription {
                key,
                rx: entry.tx.subscribe(),
            });
        }
        if self.entries.keys().filter(|k| k.venue() == venue).count() >= 32 {
            return Err("32 markets per venue limit reached".into());
        }
        if !self.workers.contains_key(&venue) {
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                let _ = rustls::crypto::ring::default_provider().install_default();
            }
            let (desired, rx) = watch::channel(Desired::default());
            let (stop, stopping) = watch::channel(false);
            let (stats_tx, stats) = watch::channel(HubStats::default());
            let engine = Engine::new(venue, self.bindings.get(&venue).cloned(), timing, stats_tx);
            let task = tokio::spawn(engine.run(url, rx, stopping));
            self.workers.insert(
                venue,
                Worker {
                    desired,
                    stop,
                    task,
                    stats,
                },
            );
        }
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or("subscription id overflow")?;
        let (tx, rx) = watch::channel(Arc::new(View::default()));
        self.entries.insert(
            key.clone(),
            Entry {
                id: self.next_id,
                tx,
            },
        );
        self.publish_desired();
        Ok(Subscription { key, rx })
    }
    fn publish_desired(&self) {
        for (venue, worker) in &self.workers {
            worker.desired.send_replace(Desired(
                self.entries
                    .iter()
                    .filter(|(k, _)| k.venue() == *venue)
                    .map(|(k, e)| (k.clone(), e.clone()))
                    .collect(),
            ));
        }
    }
    /// Changes only this consumer. New view starts unavailable; other consumers
    /// of the old market remain subscribed. Does not wait for fresh quotes.
    pub async fn handoff(
        &mut self,
        consumer: &mut Subscription,
        next: MarketKey,
    ) -> Result<(), String> {
        if *consumer.key() == next {
            return Ok(());
        }
        *consumer = self.subscribe(next).await?;
        Ok(())
    }
    pub fn stats(&self, venue: Venue) -> HubStats {
        self.workers
            .get(&venue)
            .map(|w| w.stats.borrow().clone())
            .unwrap_or_default()
    }
    pub async fn shutdown(mut self) -> Result<(), String> {
        for task in self.lighter_workers.drain(..) {
            task.abort();
            let _ = task.await;
        }
        for worker in self.workers.values() {
            let _ = worker.stop.send(true);
        }
        let mut error = None;
        for (_, worker) in self.workers.drain() {
            if let Err(e) = worker.task.await {
                error = Some(e.to_string());
            }
        }
        error.map_or(Ok(()), Err)
    }
}

impl Drop for MarketHub {
    fn drop(&mut self) {
        for task in &self.lighter_workers {
            task.abort();
        }
        for worker in self.workers.values() {
            let _ = worker.stop.send(true);
        }
    }
}

#[cfg(test)]
#[path = "hub_tests.rs"]
mod tests;
