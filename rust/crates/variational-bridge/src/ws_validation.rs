//! Compare actual private trade messages with independently authenticated REST
//! records. A matching sample is evidence of that trade, not a global switch
//! enabling unverified WS-only execution.
use crate::{
    native::Query,
    wire::{Channel, Event, FillCandidate, Observation},
    Result,
};
use perp_arb_core::decimal;
use perp_arb_execution::durable::browser::Command;
use std::collections::BTreeMap;

#[derive(Default)]
pub struct Validator {
    generation: u64,
    epoch: u64,
    pending: BTreeMap<String, FillCandidate>,
    verified: BTreeMap<String, FillCandidate>,
    faulted: bool,
    pub unqualified: u64,
}
impl Validator {
    /// Explicit experimental fast mode only. Authenticated submit has already
    /// bound the RFQ to this account; a new private-stream epoch disarms it.
    pub(crate) fn fast_trades(
        &self,
        epoch: u64,
        c: &Command,
        rfq: &str,
        instrument: &crate::native::Instrument,
    ) -> Result<Vec<FillCandidate>> {
        if epoch != self.epoch || self.faulted {
            return Err("WS fast path disarmed by private stream gap/conflict".into());
        }
        let mut result = Vec::new();
        for ws in self.pending.values().chain(self.verified.values()) {
            if ws.order_id.as_deref() != Some(rfq) {
                continue;
            }
            let qty = decimal(ws.quantity.as_deref().ok_or("WS quantity missing")?)
                .map_err(|e| e.to_string())?;
            let price = decimal(ws.price.as_deref().ok_or("WS price missing")?)
                .map_err(|e| e.to_string())?;
            let limit = decimal(&c.limit_price).map_err(|e| e.to_string())?;
            if ws.symbol.as_deref() != Some(&c.symbol)
                || ws.side.as_deref() != Some(&c.side)
                || ws.instrument.as_ref() != Some(instrument)
                || qty <= perp_arb_core::Decimal::ZERO
                || qty > decimal(&c.quantity).map_err(|e| e.to_string())?
                || price <= perp_arb_core::Decimal::ZERO
                || (c.side == "BUY" && price > limit)
                || (c.side == "SELL" && price < limit)
            {
                return Err("WS fast trade disagrees with bound maker contract".into());
            }
            result.push(ws.clone());
        }
        Ok(result)
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn verified_count(&self) -> usize {
        self.verified.len()
    }
    pub(crate) fn verified_ids(&self, rfq: &str) -> Vec<String> {
        self.verified
            .iter()
            .filter(|(_, t)| t.order_id.as_deref() == Some(rfq))
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
    pub fn observe(&mut self, e: &Event) -> Result<Option<String>> {
        if e.channel != Channel::Private {
            return Ok(None);
        }
        if e.generation < self.generation {
            return Err("stale WS validation generation".into());
        }
        if e.generation > self.generation
            || matches!(
                e.observation,
                Observation::Disconnected | Observation::Gap { .. }
            )
        {
            self.epoch = self
                .epoch
                .checked_add(1)
                .ok_or("WS validation epoch exhausted")?;
            self.pending.clear();
            self.verified.clear();
            self.generation = e.generation;
            self.faulted = false;
        }
        if self.faulted {
            return Err("WS validation conflict requires fresh session".into());
        }
        let Observation::FillCandidate { candidate: c } = &e.observation else {
            return Ok(None);
        };
        let c = c.as_ref();
        if c.status.as_deref() != Some("confirmed")
            || c.trade_type.as_deref() != Some("trade")
            || c.price.is_none()
            || c.quantity.is_none()
            || c.side.is_none()
            || c.order_id.is_none()
            || c.trade_id.is_none()
            || c.instrument
                .as_ref()
                .is_none_or(|i| i.validate(c.symbol.as_deref().unwrap_or("")).is_err())
        {
            self.unqualified += 1;
            return Ok(None);
        }
        let id = c.trade_id.as_ref().ok_or("trade ID missing")?;
        if let Some(old) = self.pending.get(id).or_else(|| self.verified.get(id)) {
            if old != c {
                self.faulted = true;
                return Err("conflicting private trade ID".into());
            }
            return Ok(None);
        }
        if self.pending.len() + self.verified.len() >= 8192 {
            self.faulted = true;
            return Err("WS validation capacity exhausted".into());
        }
        self.pending.insert(id.clone(), c.clone());
        Ok(c.order_id.clone())
    }
    /// Caller keeps the returned generation from before its REST request. A gap
    /// during the request must invalidate validation, even for the same RFQ.
    pub fn verify(&mut self, generation: u64, q: &Query, c: &Command, rfq: &str) -> Result<usize> {
        if generation != self.epoch || self.faulted {
            return Err("WS verification crossed a gap".into());
        }
        let Some(_) = q.snapshot(c, rfq)? else {
            return Ok(0);
        };
        let mut matches = Vec::new();
        for (id, ws) in &self.pending {
            if ws.order_id.as_deref() != Some(rfq) {
                continue;
            }
            let rows = &q
                .receipt()
                .trades
                .as_ref()
                .ok_or("trade source missing")?
                .rows;
            let Some(row) = rows.iter().find(|r| r["id"].as_str() == Some(id)) else {
                continue;
            };
            if row["status"] != "confirmed" {
                continue;
            }
            let same = row["source_rfq"] == rfq
                && row["side"].as_str().map(str::to_uppercase) == ws.side
                && row["instrument"]
                    == serde_json::to_value(&ws.instrument)
                        .map_err(|_| "instrument serialization")?
                && [(&ws.quantity, "qty"), (&ws.price, "price")]
                    .iter()
                    .all(|(v, k)| {
                        v.as_deref().and_then(|s| decimal(s).ok())
                            == row[*k].as_str().and_then(|s| decimal(s).ok())
                    });
            if !same {
                self.faulted = true;
                return Err("WS trade differs from exact REST record".into());
            }
            matches.push(id.clone());
        }
        for id in &matches {
            if let Some(trade) = self.pending.remove(id) {
                self.verified.insert(id.clone(), trade);
            }
        }
        Ok(matches.len())
    }
}
