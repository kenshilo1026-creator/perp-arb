//! Lighter public metadata and nonce-contiguous L2 books. Never treat a delta as a snapshot.
use crate::{
    catalog::ResolvedInstrument,
    shared::{MarketKey, View},
    Snapshot, Venue,
};
use futures_util::{SinkExt, StreamExt};
use perp_arb_core::{
    decimal,
    instruments::{Instrument, PriceRule},
    market::{Depth, Level, Quote},
    Decimal as D,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
pub const BASE: &str = "https://mainnet.zklighter.elliot.ai";
pub const WS: &str = "wss://mainnet.zklighter.elliot.ai/stream";
type Result<T> = std::result::Result<T, String>;
#[cfg(test)]
#[path = "lighter_tests.rs"]
mod tests;
pub fn dec(v: &Value) -> Result<D> {
    decimal(v.as_str().ok_or("missing decimal string")?).map_err(|e| e.to_string())
}
pub fn parse_metadata(v: &Value, symbol: &str) -> Result<ResolvedInstrument> {
    if v["code"] != 200 {
        return Err("Lighter metadata rejected".into());
    }
    let rows = v["order_book_details"]
        .as_array()
        .ok_or("missing Lighter markets")?;
    let rows: Vec<_> = rows
        .iter()
        .filter(|r| r["symbol"] == symbol && r["market_type"] == "perp")
        .collect();
    if rows.len() != 1 {
        return Err("Lighter symbol missing or ambiguous".into());
    }
    let r = rows[0];
    if r["status"] != "active"
        || r["is_frozen"] == true
        || r["market_config"]["force_reduce_only"] == true
    {
        return Err("Lighter market not active".into());
    }
    if r.get("multiplier").is_some() && dec(&r["multiplier"])? != D::ONE {
        return Err("Lighter multiplier unsupported".into());
    }
    let scale = |k: &str| -> Result<u32> {
        r[k].as_u64()
            .filter(|n| *n <= 12)
            .map(|n| n as u32)
            .ok_or("invalid Lighter precision".into())
    };
    let id = r["market_id"]
        .as_u64()
        .filter(|n| *n < 65536)
        .ok_or("invalid Lighter market ID")?;
    let rules = Instrument::new(
        PriceRule::Tick(D::new(1, scale("supported_price_decimals")?)),
        D::new(1, scale("supported_size_decimals")?),
        dec(&r["min_base_amount"])?,
        Some(dec(&r["min_quote_amount"])?),
        None,
    )
    .map_err(|e| e.to_string())?;
    Ok(ResolvedInstrument::external(
        MarketKey::new(Venue::Lighter, symbol)?,
        symbol.into(),
        "USDC".into(),
        Some(id as usize),
        rules,
    ))
}
pub async fn resolve(symbol: &str) -> Result<ResolvedInstrument> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| "Lighter HTTP client")?;
    let response = http
        .get(format!("{BASE}/api/v1/orderBookDetails"))
        .send()
        .await
        .map_err(|_| "Lighter metadata transport")?;
    if !response.status().is_success() {
        return Err(format!(
            "Lighter metadata HTTP {}",
            response.status().as_u16()
        ));
    }
    let body = response
        .bytes()
        .await
        .map_err(|_| "Lighter metadata body")?;
    if body.len() > 4_000_000 {
        return Err("Lighter metadata too large".into());
    }
    parse_metadata(
        &serde_json::from_slice(&body).map_err(|_| "Lighter metadata JSON")?,
        symbol,
    )
}
#[derive(Default)]
pub struct Book {
    nonce: Option<u64>,
    source_ms: Option<i64>,
    bids: BTreeMap<D, D>,
    asks: BTreeMap<D, D>,
}
impl Book {
    pub fn update(
        &mut self,
        v: &Value,
        id: usize,
        symbol: &str,
        received: i64,
    ) -> Result<Snapshot> {
        let result = self.apply(v, id, symbol, received);
        if result.is_err() {
            *self = Self::default();
        }
        result
    }
    fn apply(&mut self, v: &Value, id: usize, symbol: &str, received: i64) -> Result<Snapshot> {
        if v["channel"] != format!("order_book:{id}") {
            return Err("wrong Lighter book channel".into());
        }
        let b = &v["order_book"];
        let next = b["nonce"].as_u64().ok_or("missing book nonce")?;
        match v["type"].as_str() {
            Some("subscribed/order_book") if self.nonce.is_none() => {
                self.bids.clear();
                self.asks.clear();
            }
            Some("update/order_book")
                if self.nonce.is_some()
                    && b["begin_nonce"].as_u64() == self.nonce
                    && Some(next) > self.nonce => {}
            _ => return Err("Lighter book nonce gap; new snapshot required".into()),
        }
        for (name, side) in [("bids", &mut self.bids), ("asks", &mut self.asks)] {
            let rows = b[name].as_array().ok_or("missing book side")?;
            if rows.len() > 10000 {
                return Err("Lighter depth capacity".into());
            }
            for row in rows {
                let p = dec(&row["price"])?;
                let q = dec(&row["size"])?;
                if p <= D::ZERO || q < D::ZERO {
                    return Err("invalid Lighter level".into());
                }
                if q == D::ZERO {
                    side.remove(&p);
                } else {
                    side.insert(p, q);
                }
            }
            if side.is_empty() || side.len() > 20000 {
                return Err("Lighter depth empty/capacity".into());
            }
        }
        let bids: Vec<_> = self
            .bids
            .iter()
            .rev()
            .take(20)
            .map(|(p, q)| Level {
                price: *p,
                size: *q,
            })
            .collect();
        let asks: Vec<_> = self
            .asks
            .iter()
            .take(20)
            .map(|(p, q)| Level {
                price: *p,
                size: *q,
            })
            .collect();
        // last_updated_at is exchange microseconds; timestamp is exchange milliseconds.
        let source = v["last_updated_at"]
            .as_i64()
            .map(|n| n / 1000)
            .or_else(|| v["timestamp"].as_i64())
            .filter(|n| *n > 0)
            .ok_or("missing Lighter source time")?;
        if self.source_ms.is_some_and(|old| source < old) {
            return Err("Lighter source time regressed".into());
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
        self.nonce = Some(next);
        self.source_ms = Some(source);
        Ok(Snapshot {
            symbol: symbol.into(),
            sequence: Some(next),
            quote,
        })
    }
}
pub async fn stream(spec: ResolvedInstrument, tx: watch::Sender<Arc<View>>) {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
    let mut revision = 0;
    let mut retry = 1;
    while tx.receiver_count() > 0 {
        let result: Result<()> = async {
            let (mut ws, _) = tokio::time::timeout(
                Duration::from_secs(10),
                tokio_tungstenite::connect_async(WS),
            )
            .await
            .map_err(|_| "Lighter WS connect timeout")?
            .map_err(|_| "Lighter WS connect")?;
            let id = spec.asset_id.ok_or("missing market ID")?;
            ws.send(Message::Text(
                json!({"type":"subscribe","channel":format!("order_book/{id}")})
                    .to_string()
                    .into(),
            ))
            .await
            .map_err(|_| "Lighter subscribe")?;
            let mut book = Book::default();
            while tx.receiver_count() > 0 {
                let msg = tokio::time::timeout(Duration::from_secs(15), ws.next())
                    .await
                    .map_err(|_| "Lighter WS idle")?
                    .ok_or("Lighter WS closed")?
                    .map_err(|_| "Lighter WS read")?;
                let Message::Text(raw) = msg else {
                    if msg.is_close() {
                        return Err("Lighter WS closed".into());
                    }
                    continue;
                };
                if raw.len() > 4_000_000 {
                    return Err("Lighter WS capacity".into());
                }
                let v: Value = serde_json::from_str(&raw).map_err(|_| "Lighter WS JSON")?;
                match v["type"].as_str() {
                    Some("ping") => {
                        ws.send(Message::Text("{\"type\":\"pong\"}".into()))
                            .await
                            .map_err(|_| "Lighter pong")?;
                    }
                    Some("connected") => {}
                    Some("subscribed/order_book" | "update/order_book") => {
                        let snapshot =
                            book.update(&v, id, spec.key.symbol(), crate::hub::wall_ms())?;
                        revision += 1;
                        retry = 1;
                        tx.send_replace(Arc::new(View::from_snapshot(snapshot, revision)));
                    }
                    _ => return Err("Lighter WS protocol/error".into()),
                }
            }
            Ok(())
        }
        .await;
        revision += 1;
        let mut view = View::default();
        view.revision = revision;
        view.reason = result.err();
        tx.send_replace(Arc::new(view));
        if tx.receiver_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(retry)).await;
        retry = (retry * 2).min(30);
    }
}
