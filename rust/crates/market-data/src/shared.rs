//! One owner per public market. Consumers share immutable revisions, never fills.
use crate::{levels, parse_depth_value, parse_payload, Snapshot, Update, Venue};
use perp_arb_core::{
    market::{Depth, Freshness, FreshnessPolicy, Quote},
    pricing::Side,
};
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketKey {
    venue: Venue,
    symbol: String,
}
impl MarketKey {
    pub fn new(venue: Venue, symbol: &str) -> Result<Self, String> {
        venue.endpoint(symbol)?;
        Ok(Self {
            venue,
            symbol: if venue == Venue::Aster {
                symbol.to_ascii_uppercase()
            } else {
                symbol.into()
            },
        })
    }
    pub fn venue(&self) -> Venue {
        self.venue
    }
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    pub fn endpoint(&self) -> String {
        match self.venue {
            Venue::Aster => {
                let symbol = crate::aster_stream_symbol(&self.symbol);
                format!("wss://fstream.asterdex.com/stream?streams={symbol}@depth10@100ms/{symbol}@bookTicker")
            }
            Venue::Hyperliquid => "wss://api.hyperliquid.xyz/ws".into(),
        }
    }
    pub fn subscriptions(&self) -> Vec<serde_json::Value> {
        match self.venue {
            Venue::Aster => vec![],
            Venue::Hyperliquid => vec![
                self.venue
                    .subscription(&self.symbol)
                    .expect("HL subscription"),
                json!({"method":"subscribe", "subscription":{"type":"bbo", "coin":self.symbol}}),
            ],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Depth,
    Bbo,
}

/// Strict BBO parsing: published quantity is required on both CLOB sides.
/// A missing side invalidates the cache, rather than implying unlimited liquidity.
pub fn parse_market(
    key: &MarketKey,
    raw: &str,
    received: i64,
) -> Result<Option<(Channel, Snapshot)>, String> {
    let payload = parse_payload(raw)?;
    parse_market_value(key, &payload, received)
}
fn parse_market_value(
    key: &MarketKey,
    payload: &serde_json::Value,
    received: i64,
) -> Result<Option<(Channel, Snapshot)>, String> {
    let data = if key.venue == Venue::Aster && payload.get("stream").is_some() {
        &payload["data"]
    } else {
        payload
    };
    let bbo = match key.venue {
        Venue::Aster => data["e"] == "bookTicker",
        Venue::Hyperliquid => payload["channel"] == "bbo",
    };
    if !bbo {
        return parse_depth_value(key.venue, &key.symbol, payload, received)
            .map(|s| s.map(|s| (Channel::Depth, s)));
    }
    let (source, sequence, bids, asks) = match key.venue {
        Venue::Aster => {
            if let Some(stream) = payload.get("stream") {
                let expected = format!("{}@bookTicker", key.symbol.to_ascii_lowercase());
                if stream.as_str() != Some(expected.as_str()) {
                    return Err("wrong Aster BBO stream".into());
                }
            }
            if !data["s"]
                .as_str()
                .is_some_and(|s| s.eq_ignore_ascii_case(&key.symbol))
            {
                return Err("wrong BBO symbol".into());
            }
            let source = data["T"]
                .as_i64()
                .filter(|t| *t > 0)
                .or_else(|| data["E"].as_i64())
                .ok_or("missing BBO source time")?;
            (
                source,
                data["u"].as_u64(),
                levels(&json!([[data["b"], data["B"]]]), false)?,
                levels(&json!([[data["a"], data["A"]]]), false)?,
            )
        }
        Venue::Hyperliquid => {
            let data = &payload["data"];
            if data["coin"].as_str() != Some(key.symbol.as_str()) {
                return Err("wrong BBO coin or spelling".into());
            }
            let sides = data["bbo"].as_array().ok_or("missing BBO sides")?;
            if sides.len() != 2 {
                return Err("expected two BBO sides".into());
            }
            (
                data["time"].as_i64().ok_or("missing BBO source time")?,
                None,
                levels(&json!([sides[0]]), true)?,
                levels(&json!([sides[1]]), true)?,
            )
        }
    };
    if source < 0 {
        return Err("negative BBO source time".into());
    }
    let quote = Quote::new(
        bids[0].price,
        asks[0].price,
        received,
        Some(source),
        Depth::Visible(bids),
        Depth::Visible(asks),
    )
    .map_err(|e| e.to_string())?;
    Ok(Some((
        Channel::Bbo,
        Snapshot {
            symbol: key.symbol.clone(),
            sequence,
            quote,
        },
    )))
}

#[derive(Debug, Clone, Default)]
pub struct View {
    pub generation: u64,
    pub revision: u64,
    pub connected: bool,
    pub reason: Option<String>,
    pub accepted_depth: u64,
    pub accepted_bbo: u64,
    depth: Option<Arc<Snapshot>>,
    bbo: Option<Arc<Snapshot>>,
    depth_invalidated: bool,
}
impl View {
    /// Recompute age at use time. A watch value does not stay fresh just because
    /// no new revision arrived. Unchanged BBO never retimestamps deeper levels.
    pub fn selected(&self, now: i64) -> Option<&Snapshot> {
        if !self.connected {
            return None;
        }
        let depth = self.depth.as_deref();
        let bbo = self.bbo.as_deref();
        match (depth, bbo) {
            (Some(d), Some(b)) => {
                let dt = d.quote.source_ms();
                let bt = b.quote.source_ms();
                let changed = [Side::Sell, Side::Buy].iter().any(|s| {
                    d.quote.levels(*s).and_then(|l| l.first())
                        != b.quote.levels(*s).and_then(|l| l.first())
                });
                // Equal source time but conflicting top: conservatively keep BBO
                // only, even if the depth packet reached us later.
                if self.depth_invalidated
                    || (bt >= dt && changed)
                    || i128::from(now) - i128::from(d.quote.received_ms()) > 5000
                {
                    Some(b)
                } else {
                    Some(d)
                }
            }
            (Some(d), None) => Some(d),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }
    pub fn fresh(&self, now: i64) -> Option<&Snapshot> {
        self.selected(now).filter(|s| {
            s.quote.freshness(now, true, FreshnessPolicy::default()) == Freshness::Fresh
        })
    }
}

pub struct MarketState {
    key: MarketKey,
    view: View,
    accepting: bool,
}
impl MarketState {
    pub fn new(key: MarketKey) -> Self {
        Self {
            key,
            view: View::default(),
            accepting: false,
        }
    }
    pub fn view(&self) -> &View {
        &self.view
    }
    pub fn begin(&mut self) -> u64 {
        self.view.generation = self
            .view
            .generation
            .checked_add(1)
            .expect("generation overflow");
        self.clear("connecting");
        self.accepting = true;
        self.view.generation
    }
    fn clear(&mut self, reason: &str) {
        self.accepting = false;
        self.view.connected = false;
        self.view.depth = None;
        self.view.bbo = None;
        self.view.depth_invalidated = false;
        self.view.reason = Some(reason.into());
        self.view.revision = self
            .view
            .revision
            .checked_add(1)
            .expect("revision overflow");
    }
    pub fn disconnect(&mut self, generation: u64, reason: &str) {
        if generation == self.view.generation {
            self.clear(reason);
        }
    }
    pub fn ingest(&mut self, generation: u64, raw: &str, received: i64) -> Result<Update, String> {
        let parsed = parse_market(&self.key, raw, received);
        self.apply(generation, parsed, received)
    }
    /// Internal caller has already validated/parses the JSON envelope for routing.
    pub(crate) fn ingest_value(
        &mut self,
        generation: u64,
        payload: &serde_json::Value,
        received: i64,
    ) -> Result<Update, String> {
        let parsed = parse_market_value(&self.key, payload, received);
        self.apply(generation, parsed, received)
    }
    fn apply(
        &mut self,
        generation: u64,
        parsed: Result<Option<(Channel, Snapshot)>, String>,
        received: i64,
    ) -> Result<Update, String> {
        if !self.accepting || generation == 0 || generation != self.view.generation {
            return Ok(Update::Ignored);
        }
        let (channel, snapshot) = match parsed {
            Ok(Some(update)) => update,
            Ok(None) => return Ok(Update::Ignored),
            Err(e) => {
                self.clear(&e);
                return Err(e);
            }
        };
        if snapshot
            .quote
            .freshness(received, true, FreshnessPolicy::default())
            == Freshness::Future
        {
            self.clear("source time exceeds future tolerance");
            return Err("source time exceeds future tolerance".into());
        }
        let slot = match channel {
            Channel::Depth => &self.view.depth,
            Channel::Bbo => &self.view.bbo,
        };
        if let Some(previous) = slot {
            if snapshot.quote.source_ms() < previous.quote.source_ms()
                || matches!((snapshot.sequence, previous.sequence), (Some(new), Some(old)) if new <= old)
            {
                return Ok(Update::OutOfOrder);
            }
        }
        match channel {
            Channel::Bbo => {
                self.view.accepted_bbo += 1;
                if let Some(depth) = &self.view.depth {
                    if snapshot.quote.source_ms() >= depth.quote.source_ms()
                        && [Side::Sell, Side::Buy].iter().any(|s| {
                            snapshot.quote.levels(*s).and_then(|l| l.first())
                                != depth.quote.levels(*s).and_then(|l| l.first())
                        })
                    {
                        self.view.depth_invalidated = true;
                    }
                }
                self.view.bbo = Some(Arc::new(snapshot));
            }
            Channel::Depth => {
                self.view.accepted_depth += 1;
                // A price round-trip cannot resurrect an invalidated old depth.
                // Only a subsequent depth source timestamp proves a new snapshot.
                if self
                    .view
                    .bbo
                    .as_ref()
                    .is_none_or(|b| snapshot.quote.source_ms() > b.quote.source_ms())
                {
                    self.view.depth_invalidated = false;
                } else if self.view.bbo.as_ref().is_some_and(|b| {
                    [Side::Sell, Side::Buy].iter().any(|side| {
                        snapshot.quote.levels(*side).and_then(|l| l.first())
                            != b.quote.levels(*side).and_then(|l| l.first())
                    })
                }) {
                    // BBO can arrive before an older/equal-time depth packet.
                    // Latch that conflict too, so a later price round-trip cannot
                    // make this depth usable without a subsequent depth update.
                    self.view.depth_invalidated = true;
                }
                self.view.depth = Some(Arc::new(snapshot));
            }
        }
        self.view.connected = true;
        self.view.reason = None;
        self.view.revision = self
            .view
            .revision
            .checked_add(1)
            .expect("revision overflow");
        Ok(Update::Accepted)
    }
}
