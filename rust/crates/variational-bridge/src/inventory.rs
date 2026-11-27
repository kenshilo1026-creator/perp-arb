//! Account-wide, bounded browser-session observations. No guessed empty fields,
//! no self-generated expected baseline, and no atomic snapshot claim.
use crate::{
    native::{Instrument, Page, Receipt},
    Result,
};
use perp_arb_core::{decimal, Decimal};
use perp_arb_private_events::{
    account::{Inventory, OpenOrder, PositionKey, Snapshot},
    OrderKey, Scope,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    pub instrument: Instrument,
    pub qty: String,
    pub avg_entry_price: String,
    pub updated_at: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Isolated {
    pub instrument: Instrument,
    pub balance: String,
    pub allocated_qty: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Portfolio {
    pub balance: Option<String>,
    pub cross_balance: Option<String>,
    pub isolated: Option<Vec<Isolated>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Round {
    pub positions: Vec<Position>,
    pub portfolio: Portfolio,
    pub orders: Page,
    pub positions_source: String,
    pub portfolio_source: String,
    pub portfolio_server_ms: Option<i64>,
    pub observed_ms: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub rounds: [Round; 2],
}
impl Capture {
    pub(crate) fn validate_bounds(&mut self, start: i64, end: i64) -> Result<()> {
        for r in &mut self.rounds {
            if r.positions.len() > 8192
                || r.orders.rows.len() > 500
                || r.positions_source != "https://omni.variational.io/api/positions"
                || r.portfolio_source
                    != "https://omni.variational.io/api/portfolio?compute_margin=true"
                || r.orders.source != "https://omni.variational.io/api/orders/v2"
                || !r.orders.window_complete
                || r.orders.atomic_snapshot
                || !(start..=end).contains(&r.observed_ms)
                || !(start..=r.observed_ms).contains(&r.orders.observed_ms)
                || r.portfolio
                    .isolated
                    .as_ref()
                    .is_some_and(|p| p.len() > 8192)
            {
                return Err("inventory sources incomplete or out of bounds".into());
            }
            // Fixed schema projection before diagnostic recording. Keep ordinary
            // order evidence only; never arbitrary account payloads/headers.
            for row in &mut r.orders.rows {
                let o = row.as_object_mut().ok_or("inventory order shape")?;
                o.retain(|k, _| {
                    [
                        "rfq_id",
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
                        "instrument",
                        "response_server_ms",
                    ]
                    .contains(&k.as_str())
                });
                let instrument: Instrument = serde_json::from_value(
                    o.get("instrument")
                        .cloned()
                        .ok_or("inventory instrument missing")?,
                )
                .map_err(|_| "unsupported inventory instrument")?;
                instrument.validate(&instrument.underlying)?;
                for (k, v) in o.iter() {
                    if k != "instrument"
                        && !(v.is_null()
                            || v.is_boolean()
                            || v.is_number()
                            || v.as_str().is_some_and(|s| s.len() <= 128))
                    {
                        return Err("inventory order field shape".into());
                    }
                }
            }
        }
        if self.rounds[0].observed_ms > self.rounds[1].observed_ms {
            return Err("inventory rounds reversed".into());
        }
        Ok(())
    }
}
fn amount(s: &str) -> Result<Decimal> {
    decimal(s).map_err(|_| "invalid exact inventory amount".into())
}
impl Round {
    fn inventory_with_wallets(&self, check_wallets: bool) -> Result<Inventory> {
        let balance = if check_wallets {
            Some(amount(
                self.portfolio
                    .balance
                    .as_deref()
                    .ok_or("portfolio balance missing")?,
            )?)
        } else {
            None
        };
        let cross = if check_wallets {
            amount(
                self.portfolio
                    .cross_balance
                    .as_deref()
                    .ok_or("cross balance missing")?,
            )?
        } else {
            Decimal::ZERO
        };
        let mut inv = Inventory {
            perp_dexes: BTreeSet::from([String::new()]),
            includes_wallets: check_wallets,
            ..Inventory::default()
        };
        if check_wallets {
            inv.wallets.insert("USDC".into(), cross);
        }
        let mut total = cross;
        for iso in self
            .portfolio
            .isolated
            .as_ref()
            .ok_or("isolated coverage missing")?
        {
            let i = &iso.instrument;
            if !matches!(i.instrument_type.as_str(), "swap" | "perpetual_future")
                || i.settlement_asset != "USDC"
                || crate::wire::symbol(&i.underlying)? != i.underlying
                || amount(
                    iso.allocated_qty
                        .as_deref()
                        .ok_or("isolated allocation coverage missing")?,
                )? != Decimal::ZERO
            {
                return Err("unsupported isolated position allocation".into());
            }
            if !check_wallets {
                continue;
            }
            let b = amount(&iso.balance)?;
            let next = total
                .checked_add(b)
                .filter(|n| n.checked_sub(total) == Some(b))
                .ok_or("collateral sum overflow")?;
            total = next;
            let key = format!(
                "USDC:isolated:{}:{}:{}",
                i.instrument_type, i.underlying, i.funding_interval_s
            );
            if inv.wallets.insert(key, b).is_some() {
                return Err("duplicate isolated collateral".into());
            }
        }
        if balance.is_some_and(|balance| balance != total) {
            return Err("total collateral differs from cross plus isolated".into());
        }
        let mut positions = BTreeSet::new();
        for p in &self.positions {
            p.instrument.validate(&p.instrument.underlying)?;
            if !positions.insert(&p.instrument.underlying) {
                return Err("duplicate position instrument".into());
            }
            let q = amount(&p.qty)?;
            let entry = amount(&p.avg_entry_price)?;
            if entry < Decimal::ZERO || (q != Decimal::ZERO && entry == Decimal::ZERO) {
                return Err("invalid position entry price".into());
            }
            if q != Decimal::ZERO {
                inv.positions.insert(
                    PositionKey {
                        symbol: p.instrument.underlying.clone(),
                        side: "BOTH".into(),
                    },
                    q,
                );
            }
        }
        for row in &self.orders.rows {
            let instrument: Instrument = serde_json::from_value(row["instrument"].clone())
                .map_err(|_| "order instrument missing")?;
            instrument.validate(&instrument.underlying)?;
            let id = row["rfq_id"]
                .as_str()
                .filter(|s| crate::native::id(s))
                .ok_or("order ID missing")?;
            let side = match row["side"].as_str() {
                Some("buy") => "BUY",
                Some("sell") => "SELL",
                _ => return Err("unknown order side".into()),
            };
            if row["status"] != "pending"
                || row["order_type"] != "limit"
                || row["is_auto_resize"] != false
                || row["use_mark_price"] != false
                || !row["is_reduce_only"].is_boolean()
            {
                return Err("unsupported pending order; account not ready".into());
            }
            let q = amount(row["qty"].as_str().ok_or("order quantity missing")?)?;
            let p = amount(row["limit_price"].as_str().ok_or("order price missing")?)?;
            if inv
                .open_orders
                .insert(
                    OrderKey {
                        symbol: instrument.underlying,
                        order_id: id.into(),
                    },
                    OpenOrder {
                        side: side.into(),
                        remaining: q,
                        price: p,
                    },
                )
                .is_some()
            {
                return Err("duplicate pending RFQ".into());
            }
        }
        inv.validate().map_err(|e| e.to_string())?;
        // A pending RFQ doesn't expose a verified remaining-quantity contract.
        // Until that is verified, preserve rows in diagnostics but never certify
        // an account containing active orders as reconciled.
        if !inv.open_orders.is_empty() {
            return Err("active RFQs require exact order reconciliation".into());
        }
        Ok(inv)
    }
}
pub struct Query {
    receipt: Receipt,
    started: Instant,
    completed: Instant,
}
impl Query {
    pub(crate) fn new(receipt: Receipt, started: Instant) -> Result<Self> {
        if receipt.dispatched || receipt.inventory.is_none() {
            return Err("inventory read missing".into());
        }
        Ok(Self {
            receipt,
            started,
            completed: Instant::now(),
        })
    }
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
    pub async fn reconcile(
        self,
        coordinator: &perp_arb_private_events::actor::Coordinator,
        ticket: perp_arb_private_events::account::Ticket,
        expected: Option<Inventory>,
    ) -> Result<perp_arb_private_events::account::Report> {
        coordinator
            .apply_account_snapshot(ticket, self.snapshot()?, expected)
            .await
            .map_err(|e| e.to_string())
    }
    /// Execution policy: reconcile positions/orders, excluding wallet valuation.
    /// Keep the full-wallet snapshot path available for standalone diagnostics.
    pub async fn reconcile_positions(
        self,
        coordinator: &perp_arb_private_events::actor::Coordinator,
        ticket: perp_arb_private_events::account::Ticket,
        expected: Option<Inventory>,
    ) -> Result<perp_arb_private_events::account::Report> {
        let expected = expected.map(|mut inv| {
            inv.includes_wallets = false;
            inv.wallets.clear();
            inv
        });
        coordinator
            .apply_account_snapshot(ticket, self.snapshot_with_wallets(false)?, expected)
            .await
            .map_err(|e| e.to_string())
    }
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.snapshot_with_wallets(true)
    }
    pub async fn reconcile_symbol(
        mut self,
        coordinator: &perp_arb_private_events::actor::Coordinator,
        ticket: perp_arb_private_events::account::Ticket,
        symbol: &str,
    ) -> Result<perp_arb_private_events::account::Report> {
        let capture = self.receipt.inventory.as_mut().ok_or("inventory missing")?;
        for round in &mut capture.rounds {
            round
                .positions
                .retain(|p| p.instrument.underlying == symbol);
            // Neither other positions, wallet valuations nor account-wide orders
            // are part of the selected-position check. Own orders use exact IDs.
            round.portfolio.isolated = Some(vec![]);
            round.orders.rows.clear();
        }
        coordinator
            .apply_position_snapshot(ticket, self.snapshot_with_wallets(false)?, symbol)
            .await
            .map_err(|e| e.to_string())
    }
    fn snapshot_with_wallets(&self, check_wallets: bool) -> Result<Snapshot> {
        if self.completed.elapsed() > Duration::from_secs(5) {
            return Err("inventory observation stale".into());
        }
        let c = self.receipt.inventory.as_ref().ok_or("inventory missing")?;
        let a = c.rounds[0].inventory_with_wallets(check_wallets)?;
        let b = c.rounds[1].inventory_with_wallets(check_wallets)?;
        if a != b {
            return Err("inventory changed during bounded reads".into());
        }
        // Inventory keys intentionally aggregate by symbol. Still reject a
        // contract/entry-price change that preserves the net quantity.
        let details = |round: &Round| -> Result<BTreeMap<String, (Instrument, Decimal)>> {
            round
                .positions
                .iter()
                .map(|p| {
                    Ok((
                        p.instrument.underlying.clone(),
                        (p.instrument.clone(), amount(&p.avg_entry_price)?),
                    ))
                })
                .collect()
        };
        if details(&c.rounds[0])? != details(&c.rounds[1])? {
            return Err("position contract or entry changed during bounded reads".into());
        }
        let ms = c.rounds[1]
            .portfolio_server_ms
            .filter(|ms| {
                (i128::from(self.receipt.started_ms) - 1000
                    ..=i128::from(self.receipt.finished_ms) + 1000)
                    .contains(&i128::from(*ms))
            })
            .ok_or("inventory source time missing or stale")?;
        Ok(Snapshot {
            scope: Scope {
                venue: "variational".into(),
                account: self.receipt.account.clone(),
            },
            inventory: b,
            started_at: self.started,
            completed_at: self.completed,
            exchange_time_ms: Some(ms.try_into().map_err(|_| "negative source time")?),
        })
    }
    pub fn summary(&self) -> Value {
        match self.snapshot() {
            Ok(s) => {
                serde_json::json!({"stable_inventory":true,"positions":s.inventory.positions.len(),"open_orders":s.inventory.open_orders.len(),"wallet_assets":s.inventory.wallets.len(),"expected_supplied":false,"account_reconciled":false})
            }
            Err(e) => {
                serde_json::json!({"stable_inventory":false,"reason":e,"account_reconciled":false})
            }
        }
    }
}

#[cfg(test)]
mod tests;
