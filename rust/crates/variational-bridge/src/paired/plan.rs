use crate::{native, Result};
use perp_arb_core::{decimal, pricing::Side, Decimal};
use perp_arb_execution::{
    durable::{browser, DurableEngine},
    Leg, Plan as ExecutionPlan, Side as ExecutionSide,
};
use perp_arb_market_data::catalog::ResolvedInstrument;
use perp_arb_private_events::{
    account::{Inventory, PositionKey},
    orders::{Network, Order, TimeInForce},
    Scope,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub run_id: String,
    pub extension_id: String,
    pub maker: browser::Command,
    pub instrument: native::Instrument,
    pub hedge: perp_arb_order_test::Plan,
    /// Fraction: 0.0025 = 0.25%; only guards unfilled maker risk.
    pub max_entry_gap: String,
    pub maker_wait_seconds: u64,
    pub recovery_seconds: u64,
    /// Shadow remains the default until real WS trade semantics are accepted.
    #[serde(default)]
    pub maker_fill_mode: FillMode,
    /// Native hedge only. Legacy plans remain readable for recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hedge_leverage: Option<u32>,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FillMode {
    #[default]
    RestVerified,
    WsConfirmed,
}
#[derive(Clone)]
pub struct Resolved {
    pub order: Order,
    pub metadata: ResolvedInstrument,
}
impl Resolved {
    pub fn hedge_limits(&self) -> Result<&perp_arb_core::instruments::Instrument> {
        if self.order.tif == TimeInForce::Market {
            self.metadata
                .market
                .as_ref()
                .ok_or_else(|| "market instrument constraints unavailable".into())
        } else {
            Ok(&self.metadata.limit)
        }
    }
}
fn d(v: &str) -> Result<Decimal> {
    decimal(v).map_err(|e| e.to_string())
}
impl Plan {
    pub fn leverage_for_submit(&self) -> Result<Option<u32>> {
        self.validate()?;
        if self.maker.reduce_only {
            return Ok(None);
        }
        self.hedge_leverage.map(Some).ok_or_else(||
            "opening plan has no hedge_leverage; prepare a new plan with leverage_x before submit".into())
    }
    pub fn read(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|_| "cannot read paired plan")?;
        if bytes.len() > 65536 {
            return Err("paired plan too large".into());
        }
        let p: Self = serde_json::from_slice(&bytes).map_err(|_| "invalid paired plan JSON")?;
        p.validate()?;
        Ok(p)
    }
    pub fn validate(&self) -> Result<()> {
        if self.hedge_leverage.is_some_and(|x| !(1..=125).contains(&x)) {
            return Err("hedge_leverage must be in 1..125".into());
        }
        if self.run_id.is_empty()
            || self.run_id.len() > 64
            || !self
                .run_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
            || self.maker.request_id != self.run_id
            || self.extension_id.len() != 32
            || !self
                .extension_id
                .bytes()
                .all(|c| (b'a'..=b'p').contains(&c))
        {
            return Err("invalid run/request/extension identity".into());
        }
        self.instrument.validate(&self.maker.symbol)?;
        if native::account(&self.maker.account)? != self.maker.account
            || (self.hedge.venue != "lighter"
                && native::account(&self.hedge.account)? != self.hedge.account)
        {
            return Err("use canonical lowercase account addresses".into());
        }
        self.hedge.validate()?;
        let q = d(&self.maker.quantity)?;
        let p = d(&self.maker.limit_price)?;
        let cap = d(&self.maker.max_notional)?;
        if q <= Decimal::ZERO
            || p <= Decimal::ZERO
            || cap <= Decimal::ZERO
            || perp_arb_core::instruments::ExactAmount::product(q, p)
                .and_then(|a| a.compare(cap))
                .map_err(|e| e.to_string())?
                .is_gt()
            || q != d(&self.hedge.quantity)?
            || self.maker.reduce_only != self.hedge.reduce_only
            || !matches!(
                (self.maker.side.as_str(), self.hedge.side.as_str()),
                ("BUY", "SELL") | ("SELL", "BUY")
            )
            || self.hedge.network != Network::Mainnet
            || !(self.hedge.tif == TimeInForce::Ioc
                || (self.hedge.venue == "aster" && self.hedge.tif == TimeInForce::Market))
            || d(&self.max_entry_gap)? < Decimal::ZERO
            || d(&self.max_entry_gap)? > d("0.1")?
            || !(1..=60).contains(&self.maker_wait_seconds)
            || !(15..=120).contains(&self.recovery_seconds)
        {
            return Err("invalid paired size/side/cap/reduce-only/mainnet/timeout policy".into());
        }
        Ok(())
    }
    pub async fn resolve(&self) -> Result<Resolved> {
        self.validate()?;
        let (order, metadata) = self.hedge.resolve_with_metadata().await?;
        if metadata.native_base != self.maker.symbol
            || metadata.canonical_base != self.maker.symbol
            || metadata.units().is_none()
        {
            return Err(
                "paired legs require exact same base units; aliases/multipliers unsupported".into(),
            );
        }
        Ok(Resolved { order, metadata })
    }
    pub fn execution_plan(&self, r: &Resolved) -> Result<ExecutionPlan> {
        let limits = r.hedge_limits()?;
        let mut minimum = limits.min_size().max(limits.lot());
        if let Some(notional) = limits.min_notional() {
            let price = d(&self.hedge.limit_price)?;
            let raw = notional
                .checked_div(price)
                .ok_or("minimum notional overflow")?;
            let rounded = perp_arb_core::pricing::round_step(
                raw,
                Some(limits.lot()),
                perp_arb_core::pricing::Direction::Up,
            )
            .map_err(|e| e.to_string())?;
            minimum = minimum.max(rounded);
            if perp_arb_core::instruments::ExactAmount::product(minimum, price)
                .and_then(|n| n.compare(notional))
                .map_err(|e| e.to_string())?
                .is_lt()
            {
                minimum = minimum
                    .checked_add(limits.lot())
                    .ok_or("minimum size overflow")?;
            }
        }
        self.execution_plan_with_policy(limits.lot(), minimum)
    }
    pub(crate) fn execution_plan_with_policy(
        &self,
        step: Decimal,
        minimum: Decimal,
    ) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan {
            run_id: self.run_id.clone(),
            maker: Leg {
                scope: Scope {
                    venue: "variational".into(),
                    account: self.maker.account.clone(),
                },
                symbol: self.maker.symbol.clone(),
                side: if self.maker.side == "BUY" {
                    ExecutionSide::Buy
                } else {
                    ExecutionSide::Sell
                },
            },
            hedge: Leg {
                scope: Scope {
                    venue: self.hedge.venue.clone(),
                    account: self.hedge.account.clone(),
                },
                symbol: self.hedge.symbol.clone(),
                side: if self.hedge.side == "BUY" {
                    ExecutionSide::Buy
                } else {
                    ExecutionSide::Sell
                },
            },
            maker_order: DurableEngine::pending_browser_order(&self.maker)
                .map_err(|e| e.to_string())?,
            quantity: d(&self.maker.quantity)?,
            step,
            minimum,
        })
    }
    pub fn hedge_side(&self) -> Side {
        if self.hedge.side == "BUY" {
            Side::Buy
        } else {
            Side::Sell
        }
    }
    pub fn entry_ok(&self, quote: &perp_arb_core::market::Quote) -> Result<bool> {
        let Some(worst) = quote
            .marginal(self.hedge_side(), d(&self.maker.quantity)?)
            .map_err(|e| e.to_string())?
        else {
            return Ok(false);
        };
        let Some(price) = quote
            .executable(self.hedge_side(), Some(d(&self.maker.quantity)?))
            .map_err(|e| e.to_string())?
        else {
            return Ok(false);
        };
        let maker = d(&self.maker.limit_price)?;
        let adverse = if self.maker.side == "BUY" {
            maker.checked_sub(price)
        } else {
            price.checked_sub(maker)
        }
        .ok_or("gap overflow")?;
        let allowed =
            perp_arb_core::instruments::ExactAmount::product(maker, d(&self.max_entry_gap)?)
                .map_err(|e| e.to_string())?;
        Ok(
            !allowed.compare(adverse).map_err(|e| e.to_string())?.is_lt()
                && if self.hedge.tif == TimeInForce::Market {
                    true
                } else if self.hedge.side == "BUY" {
                    worst <= d(&self.hedge.limit_price)?
                } else {
                    worst >= d(&self.hedge.limit_price)?
                },
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryFile {
    pub wallets: BTreeMap<String, String>,
    pub positions: BTreeMap<String, String>,
    #[serde(default)]
    pub position_sides: Vec<PositionFile>,
    #[serde(default)]
    pub open_orders: Vec<OrderFile>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionFile {
    pub symbol: String,
    pub side: String,
    pub quantity: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderFile {
    pub symbol: String,
    pub order_id: String,
    pub side: String,
    pub remaining: String,
    pub price: String,
}
impl InventoryFile {
    pub fn from_inventory(i: &Inventory) -> Result<Self> {
        if i.includes_spot {
            return Err("unexpected spot inventory coverage".into());
        }
        Ok(Self {
            wallets: i
                .wallets
                .iter()
                .map(|(a, v)| (a.clone(), v.to_string()))
                .collect(),
            positions: i
                .positions
                .iter()
                .filter(|(p, _)| p.side == "BOTH")
                .map(|(p, q)| (p.symbol.clone(), q.to_string()))
                .collect(),
            position_sides: i
                .positions
                .iter()
                .filter(|(p, _)| p.side != "BOTH")
                .map(|(p, q)| PositionFile {
                    symbol: p.symbol.clone(),
                    side: p.side.clone(),
                    quantity: q.to_string(),
                })
                .collect(),
            open_orders: i
                .open_orders
                .iter()
                .map(|(k, v)| OrderFile {
                    symbol: k.symbol.clone(),
                    order_id: k.order_id.clone(),
                    side: v.side.clone(),
                    remaining: v.remaining.to_string(),
                    price: v.price.to_string(),
                })
                .collect(),
        })
    }
    pub fn inventory(&self) -> Result<Inventory> {
        let mut inv = Inventory {
            perp_dexes: BTreeSet::from([String::new()]),
            includes_wallets: true,
            wallets: self
                .wallets
                .iter()
                .map(|(a, q)| Ok((a.clone(), d(q)?)))
                .collect::<Result<_>>()?,
            positions: self
                .positions
                .iter()
                .map(|(s, q)| {
                    Ok((
                        PositionKey {
                            symbol: s.clone(),
                            side: "BOTH".into(),
                        },
                        d(q)?,
                    ))
                })
                .collect::<Result<_>>()?,
            ..Inventory::default()
        };
        for p in &self.position_sides {
            if inv
                .positions
                .insert(
                    PositionKey {
                        symbol: p.symbol.clone(),
                        side: p.side.clone(),
                    },
                    d(&p.quantity)?,
                )
                .is_some()
            {
                return Err("duplicate position key".into());
            }
        }
        for o in &self.open_orders {
            if inv
                .open_orders
                .insert(
                    perp_arb_private_events::OrderKey {
                        symbol: o.symbol.clone(),
                        order_id: o.order_id.clone(),
                    },
                    perp_arb_private_events::account::OpenOrder {
                        side: o.side.clone(),
                        remaining: d(&o.remaining)?,
                        price: d(&o.price)?,
                    },
                )
                .is_some()
            {
                return Err("duplicate order key".into());
            }
        }
        inv.validate().map_err(|e| e.to_string())?;
        Ok(inv)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Baseline {
    pub reviewed: bool,
    pub maker_account: String,
    pub hedge_account: String,
    pub hedge_venue: String,
    pub maker: InventoryFile,
    pub hedge: InventoryFile,
}
impl Baseline {
    pub fn read(path: &Path, p: &Plan) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|_| "cannot read baseline")?;
        if bytes.len() > 1_048_576 {
            return Err("baseline too large".into());
        }
        let b: Self = serde_json::from_slice(&bytes).map_err(|_| "invalid baseline JSON")?;
        b.validate(p)?;
        Ok(b)
    }
    pub fn validate(&self, p: &Plan) -> Result<()> {
        if !self.maker.open_orders.is_empty() || !self.hedge.open_orders.is_empty() {
            return Err("existing active orders must be reconciled before a new paired run; no orders canceled automatically".into());
        }
        if !self.maker.position_sides.is_empty() || !self.hedge.position_sides.is_empty() {
            return Err("this paired runner supports one-way accounts; hedge-mode positions are preserved but cannot be traded by this runner".into());
        }
        if !self.reviewed
            || self.maker_account != p.maker.account
            || self.hedge_account != p.hedge.account
            || self.hedge_venue != p.hedge.venue
        {
            return Err("baseline must be independently reviewed and match both accounts".into());
        }
        self.maker.inventory()?;
        self.hedge.inventory()?;
        if p.maker.reduce_only {
            for (i, s, side) in [
                (&self.maker, &p.maker.symbol, &p.maker.side),
                (&self.hedge, &p.hedge.symbol, &p.hedge.side),
            ] {
                let q = i
                    .positions
                    .get(s)
                    .map(|x| d(x))
                    .transpose()?
                    .unwrap_or(Decimal::ZERO);
                if (side == "BUY" && q >= Decimal::ZERO)
                    || (side == "SELL" && q <= Decimal::ZERO)
                    || q.abs() < d(&p.maker.quantity)?
                {
                    return Err("close requires sufficient opposing position on both legs".into());
                }
            }
        }
        Ok(())
    }
}
