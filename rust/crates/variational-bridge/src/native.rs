//! Fixed browser-session sources. rfq_id is the order key; trade.id is never
//! an order key. A missing page row is unknown, and no position delta is used.
use crate::{
    orders::{Command, Identity, Ledger},
    Result,
};
use perp_arb_core::{decimal, Decimal};
use perp_arb_private_events::{OrderKey, OrderSnapshot, Scope, Status};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Instant};

pub const PROTOCOL: &str = "variational-native-v1";
const ROOT: &str = "https://omni.variational.io/api";
#[cfg(test)]
mod tests;
pub(crate) fn id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
pub fn account(s: &str) -> Result<String> {
    if s.len() != 42 || !s.starts_with("0x") || !s[2..].bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("expected Variational wallet address required".into());
    }
    Ok(s.to_ascii_lowercase())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instrument {
    pub underlying: String,
    pub instrument_type: String,
    pub settlement_asset: String,
    pub funding_interval_s: u64,
}
impl Instrument {
    pub fn validate(&self, symbol: &str) -> Result<()> {
        if self.underlying != symbol
            || crate::wire::symbol(symbol)? != symbol
            || self.instrument_type != "perpetual_future"
            || self.settlement_asset != "USDC"
            || self.funding_interval_s == 0
            || self.funding_interval_s > 86400
        {
            return Err(
                "unsupported or mismatching Variational instrument; do not guess funding interval"
                    .into(),
            );
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
    pub source: String,
    pub rows: Vec<Value>,
    pub window_complete: bool,
    pub atomic_snapshot: bool,
    pub observed_ms: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepare_id: Option<String>,
    pub protocol: String,
    pub started_ms: i64,
    pub finished_ms: i64,
    pub account: String,
    pub session_continuity: bool,
    pub ok: bool,
    pub dispatched: bool,
    pub orders: Option<Page>,
    pub trades: Option<Page>,
    pub rfq_id: Option<String>,
    pub source: Option<String>,
    pub inventory: Option<crate::inventory::Capture>,
}
impl Receipt {
    pub(crate) fn parse(value: &Value, expected: Option<&str>, now: i64) -> Result<Self> {
        let mut r: Self =
            serde_json::from_value(value.clone()).map_err(|_| "native receipt schema mismatch")?;
        if let Some(capture) = &mut r.inventory {
            capture.validate_bounds(r.started_ms, r.finished_ms)?;
        }
        if r.protocol != PROTOCOL
            || !r.ok
            || !r.session_continuity
            || account(&r.account)? != r.account
            || expected.is_some_and(|e| e != r.account)
            || r.started_ms < 1000
            || r.finished_ms < r.started_ms
            || i128::from(r.finished_ms) - i128::from(r.started_ms)
                > if value["transport"] == "ui" {
                    65000
                } else {
                    20000
                }
            || !(-1000..=5000).contains(&(i128::from(now) - i128::from(r.finished_ms)))
        {
            return Err("native receipt stale, failed or account changed".into());
        }
        if r.rfq_id.as_deref().is_some_and(|s| !id(s))
            || r.source.as_deref().is_some_and(|s| {
                ![
                    "https://omni.variational.io/api/me",
                    "https://omni.variational.io/api/orders/new/limit",
                    "https://omni.variational.io/api/orders/cancel",
                ]
                .contains(&s)
            })
        {
            return Err("unexpected native identity/source".into());
        }
        for (page, path) in [(&r.orders, "/orders/v2"), (&r.trades, "/trades")] {
            if let Some(p) = page {
                if p.source != format!("{ROOT}{path}")
                    || p.rows.len() > 500
                    || p.observed_ms < r.started_ms
                    || p.observed_ms > r.finished_ms
                    || p.atomic_snapshot
                {
                    return Err("unexpected native source or page bounds".into());
                }
            }
        }
        // Defense in depth: independently project extension rows before any
        // caller can record them. Headers/tokens/extra diagnostics are dropped.
        for page in [&mut r.orders, &mut r.trades].into_iter().flatten() {
            for row in &mut page.rows {
                let o = row.as_object_mut().ok_or("native row must be an object")?;
                o.retain(|k, v| {
                    if k == "instrument" {
                        return true;
                    }
                    [
                        "rfq_id",
                        "id",
                        "source_rfq",
                        "side",
                        "qty",
                        "order_type",
                        "limit_price",
                        "price",
                        "status",
                        "clearing_status",
                        "is_reduce_only",
                        "is_auto_resize",
                        "use_mark_price",
                        "created_at",
                        "trade_type",
                        "response_server_ms",
                    ]
                    .contains(&k.as_str())
                        && (v.is_null()
                            || v.is_boolean()
                            || v.is_number()
                            || v.as_str().is_some_and(|s| s.len() <= 128))
                });
                if let Some(instrument) = o.get_mut("instrument") {
                    let i = instrument
                        .as_object_mut()
                        .ok_or("native instrument shape")?;
                    i.retain(|k, v| {
                        [
                            "underlying",
                            "instrument_type",
                            "settlement_asset",
                            "funding_interval_s",
                        ]
                        .contains(&k.as_str())
                            && (v.is_number() || v.as_str().is_some_and(|s| s.len() <= 64))
                    });
                }
            }
        }
        Ok(r)
    }
}

/// Constructed only after a correlated response on this bridge connection.
/// Complete window != complete account history or an atomic snapshot.
pub struct Query {
    receipt: Receipt,
    order_id: Option<String>,
    received: Instant,
}
impl Query {
    pub(crate) fn new(receipt: Receipt, order_id: Option<String>) -> Result<Self> {
        if receipt.dispatched || receipt.orders.is_none() || receipt.trades.is_none() {
            return Err("native read sources missing or mutation in read response".into());
        }
        Ok(Self {
            receipt,
            order_id,
            received: Instant::now(),
        })
    }
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
    /// Record verified REST trades on the blocking ledger owner. A missing RFQ
    /// only fences the ledger; it never creates a zero-fill terminal.
    pub fn record(
        self,
        ledger: &mut Ledger,
        ticket: perp_arb_execution::durable::browser::LookupTicket,
    ) -> Result<()> {
        let view = ledger.view();
        let order_id = view.order_id.as_deref().ok_or("RFQ binding missing")?;
        let snapshot = match self.snapshot(&view.command, order_id) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                ledger.unresolved().map_err(|e| e.to_string())?;
                return Err("exact RFQ absent; remains unknown".into());
            }
            Err(error) => {
                ledger.unresolved().map_err(|e| e.to_string())?;
                return Err(error);
            }
        };
        let mut trades = BTreeMap::new();
        for row in &self.receipt.trades.as_ref().ok_or("trades missing")?.rows {
            if row["status"] == "confirmed" {
                let id = row["id"].as_str().ok_or("trade ID missing")?.to_string();
                trades.insert(
                    id.clone(),
                    perp_arb_execution::durable::browser::ConfirmedTrade {
                        id,
                        quantity: amount(row, "qty")?.to_string(),
                        price: amount(row, "price")?.to_string(),
                    },
                );
            }
        }
        ledger
            .apply_verified_trades(ticket, &snapshot, trades.into_values().collect())
            .map_err(|e| e.to_string())
    }
    pub fn summary(&self) -> Value {
        let r = &self.receipt;
        json!({"account":r.account,"source":"browser_session","order_rows":r.orders.as_ref().map(|p|p.rows.len()),
            "trade_rows":r.trades.as_ref().map(|p|p.rows.len()),"exact_order_id":self.order_id,
            "orders_window_complete":r.orders.as_ref().map(|p|p.window_complete),
            "trades_window_complete":r.trades.as_ref().map(|p|p.window_complete),
            "terminal_proven":false,"can_submit_paired_orders":false})
    }
    /// Requires an already-bound intent (usually a persisted submission receipt).
    /// RFQ absence never binds a new intent or declares a terminal state.
    pub fn snapshot(&self, c: &Command, order_id: &str) -> Result<Option<OrderSnapshot>> {
        if self.order_id.as_deref() != Some(order_id)
            || c.account != self.receipt.account
            || self.received.elapsed().as_secs() >= 5
            || !id(order_id)
        {
            return Err("native exact query scope mismatch or stale".into());
        }
        let orders = self.receipt.orders.as_ref().ok_or("orders missing")?;
        let matches: Vec<_> = orders
            .rows
            .iter()
            .filter(|o| o["rfq_id"] == order_id)
            .collect();
        if matches.is_empty() {
            return Ok(None);
        }
        if matches.len() != 1 {
            return Err("duplicate RFQ rows; query again".into());
        }
        let order = matches[0];
        let instrument: Instrument = serde_json::from_value(order["instrument"].clone())
            .map_err(|_| "order instrument missing")?;
        instrument.validate(&c.symbol)?;
        let original = positive(&c.quantity)?;
        let side = c.side.to_ascii_lowercase();
        if order["side"] != side
            || amount(order, "qty")? != original
            || order["order_type"] != "limit"
            || amount(order, "limit_price")? != positive(&c.limit_price)?
            || order["is_reduce_only"] != c.reduce_only
            || order["is_auto_resize"] != false
            || order["use_mark_price"] != false
        {
            return Err("exact RFQ does not match persisted intent".into());
        }
        let trades = self.receipt.trades.as_ref().ok_or("trades missing")?;
        let mut seen = BTreeMap::<&str, &Value>::new();
        let mut cumulative = Decimal::ZERO;
        let mut pending = false;
        for trade in &trades.rows {
            if trade["source_rfq"] != order_id {
                return Err("trade belongs to another RFQ".into());
            }
            let trade_id = trade["id"]
                .as_str()
                .filter(|s| id(s))
                .ok_or("trade ID missing")?;
            if let Some(old) = seen.insert(trade_id, trade) {
                // Server-time metadata may differ across pages, semantic rows may not.
                let mut a = old.clone();
                let mut b = trade.clone();
                a.as_object_mut()
                    .ok_or("trade shape")?
                    .remove("response_server_ms");
                b.as_object_mut()
                    .ok_or("trade shape")?
                    .remove("response_server_ms");
                if a != b {
                    return Err("conflicting duplicate trade".into());
                }
                continue;
            }
            if trade["side"] != side
                || trade["instrument"] != order["instrument"]
                || trade["trade_type"] != "trade"
            {
                return Err("trade scope mismatch".into());
            }
            let quantity = amount(trade, "qty")?;
            let _price = amount(trade, "price")?;
            match trade["status"].as_str() {
                Some("confirmed") => {
                    let next = cumulative
                        .checked_add(quantity)
                        .ok_or("fill sum overflow")?;
                    if next.checked_sub(cumulative) != Some(quantity) {
                        return Err("inexact fill sum".into());
                    }
                    cumulative = next;
                }
                Some("pending") => pending = true,
                Some("failed" | "rejected" | "canceled") => (),
                _ => return Err("unverified trade status".into()),
            }
        }
        if cumulative > original {
            return Err("RFQ overfill".into());
        }
        let server = order["response_server_ms"].as_i64().filter(|ms| {
            (i128::from(self.receipt.started_ms) - 1000
                ..=i128::from(self.receipt.finished_ms) + 1000)
                .contains(&i128::from(*ms))
        });
        let raw_status = order["status"].as_str().ok_or("RFQ status missing")?;
        // Conservative: only a cleared RFQ plus the complete confirmed quantity
        // proves a fill terminal. Canceled/rejected zero still needs a verified
        // venue cancellation/fill-race contract; offset-page absence isn't proof.
        let terminal = raw_status == "cleared"
            && order["clearing_status"] == "success_trades_booked_into_pool"
            && cumulative == original
            && !pending
            && server.is_some();
        if !matches!(raw_status, "pending" | "cleared" | "canceled" | "rejected") {
            return Err("unverified RFQ status".into());
        }
        // Full known quantity must survive even without a trustworthy source
        // watermark. The reconciliation flag, not this status alone, controls
        // whether terminal evidence is usable.
        let status = if cumulative == original {
            Status::Filled
        } else if cumulative > Decimal::ZERO {
            Status::PartiallyFilled
        } else {
            Status::New
        };
        let now = Instant::now();
        Ok(Some(OrderSnapshot {
            scope: Scope {
                venue: "variational".into(),
                account: c.account.clone(),
            },
            key: OrderKey {
                symbol: c.symbol.clone(),
                order_id: order_id.into(),
            },
            original,
            cumulative,
            status,
            // HTTP response Date is a query observation watermark, NOT fill time.
            event_ms: server.and_then(|ms| ms.try_into().ok()),
            revision: 0,
            requires_reconciliation: !terminal,
            observed_at: self.received,
            applied_at: now,
        }))
    }
}
fn positive(s: &str) -> Result<Decimal> {
    let n = decimal(s).map_err(|_| "invalid exact amount")?;
    if n <= Decimal::ZERO {
        return Err("non-positive amount".into());
    }
    Ok(n)
}
fn amount(v: &Value, field: &str) -> Result<Decimal> {
    positive(v[field].as_str().ok_or("decimal string required")?)
}

/// A correlated POST receipt. No externally constructible `verified=true` flag.
pub struct Submission {
    pub(crate) receipt: Receipt,
    pub(crate) command: Command,
}
impl Submission {
    /// Call from the ledger's blocking owner, never the WS reader task.
    pub fn record(self, ledger: &mut Ledger) -> Result<()> {
        if ledger.view().command != self.command {
            return Err("submission belongs to another ledger".into());
        }
        let r = self.receipt;
        if !r.dispatched
            || r.source.as_deref() != Some("https://omni.variational.io/api/orders/new/limit")
        {
            return Err("wrong submission source".into());
        }
        let order_id = r
            .rfq_id
            .filter(|s| id(s))
            .ok_or("RFQ acknowledgement missing")?;
        // Bind before the optional ACK record so a disk failure cannot leave a
        // successful local ACK concealing an otherwise known order identity.
        ledger
            .bind_verified(Identity {
                request_id: self.command.request_id.clone(),
                account: r.account,
                symbol: self.command.symbol,
                side: self.command.side,
                quantity: self.command.quantity,
                reduce_only: self.command.reduce_only,
                order_id,
            })
            .map_err(|e| e.to_string())?;
        ledger
            .submit_ack(&self.command.request_id)
            .map_err(|e| e.to_string())
    }
}

/// Extension-persisted submit receipt, recovered under a newly checked session.
/// Restores identity only; it never confirms fills or grants another dispatch.
pub struct Recovery {
    current: Receipt,
    request_id: String,
    receipt: Option<Receipt>,
    order: Option<Value>,
}
impl Recovery {
    pub(crate) fn parse(
        value: &Value,
        expected_account: &str,
        request_id: &str,
        now: i64,
    ) -> Result<Self> {
        let current = Receipt::parse(&value["current"], Some(expected_account), now)?;
        if current.dispatched
            || current.source.as_deref() != Some("https://omni.variational.io/api/me")
            || value["dispatchRequestId"] != request_id
        {
            return Err("recovery identity mismatch".into());
        }
        let stored = &value["stored"];
        if stored.is_null() || stored.get("receipt").is_none() {
            return Ok(Self {
                current,
                request_id: request_id.into(),
                receipt: None,
                order: None,
            });
        }
        if stored["account"] != expected_account || stored["action"] != "submit" {
            return Err("stored request scope mismatch".into());
        }
        let old_time = stored["receipt"]["finished_ms"]
            .as_i64()
            .ok_or("stored receipt time missing")?;
        if i128::from(old_time) > i128::from(current.started_ms) + 1000 {
            return Err("future stored receipt".into());
        }
        // Old ACK freshness is intentionally not restored: only current /me is
        // fresh. Restored order must be queried separately before replacement.
        let receipt = Receipt::parse(&stored["receipt"], Some(expected_account), old_time)?;
        let order = stored["order"].as_object().ok_or("stored intent missing")?;
        let mut projected = serde_json::Map::new();
        for key in [
            "instrument",
            "qty",
            "side",
            "order_type",
            "limit_price",
            "is_reduce_only",
            "is_auto_resize",
            "use_mark_price",
            "slippage_limit",
        ] {
            projected.insert(
                key.into(),
                order
                    .get(key)
                    .cloned()
                    .ok_or("stored intent field missing")?,
            );
        }
        let instrument: Instrument = serde_json::from_value(projected["instrument"].clone())
            .map_err(|_| "stored instrument invalid")?;
        instrument.validate(&instrument.underlying)?;
        let o = Value::Object(projected);
        amount(&o, "qty")?;
        amount(&o, "limit_price")?;
        if !matches!(o["side"].as_str(), Some("buy" | "sell"))
            || !o["is_reduce_only"].is_boolean()
            || o["is_auto_resize"] != false
            || o["use_mark_price"] != false
            || o["order_type"] != "limit"
            || o["slippage_limit"] != "0"
        {
            return Err("stored intent schema invalid".into());
        }
        Ok(Self {
            current,
            request_id: request_id.into(),
            receipt: Some(receipt),
            order: Some(o),
        })
    }
    pub(crate) fn wire_view(&self) -> Value {
        let stored = self.receipt.as_ref().map(|receipt| {
            json!({"action":"submit","account":receipt.account,
            "order":self.order,"receipt":receipt})
        });
        json!({"current":self.current,"dispatchRequestId":self.request_id,"stored":stored})
    }
    pub fn record(self, ledger: &mut Ledger) -> Result<()> {
        let c = ledger.view().command;
        if !ledger.view().dispatched || c.request_id != self.request_id {
            return Err("recovery belongs to another intent".into());
        }
        let receipt = self
            .receipt
            .ok_or("no persisted submit receipt; submission remains UNKNOWN")?;
        let o = self.order.ok_or("stored intent missing")?;
        let instrument: Instrument = serde_json::from_value(o["instrument"].clone())
            .map_err(|_| "stored instrument invalid")?;
        instrument.validate(&c.symbol)?;
        if receipt.account != c.account
            || amount(&o, "qty")? != positive(&c.quantity)?
            || amount(&o, "limit_price")? != positive(&c.limit_price)?
            || o["side"] != c.side.to_ascii_lowercase()
            || o["is_reduce_only"] != c.reduce_only
            || o["is_auto_resize"] != false
            || o["use_mark_price"] != false
            || o["order_type"] != "limit"
            || o["slippage_limit"] != "0"
        {
            return Err("recovered intent differs from WAL".into());
        }
        Submission {
            receipt,
            command: c,
        }
        .record(ledger)
    }
}
