//! Native perpetual pairs. No browser and no Python execution fallback.
mod runtime;
#[cfg(test)]
mod tests;
mod worker;
use crate::Result;
use perp_arb_core::{
    decimal,
    instruments::{common_lot, Instrument, PriceRule},
    market::Quote,
    pricing::{Direction, Side},
    Decimal as D,
};
use perp_arb_private_events::orders::{Network, Order, TimeInForce};
pub use runtime::{execute, inspect};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub(crate) fn d(s: &str) -> Result<D> {
    decimal(s).map_err(|e| e.to_string())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub schema: String,
    pub run_id: String,
    pub symbol: String,
    pub maker: perp_arb_order_test::Plan,
    pub hedge: perp_arb_order_test::Plan,
    pub clip_quantity: String,
    pub max_gap: String,
    pub hedge_price_mode: String,
    pub hedge_slippage_bps: String,
    pub maker_wait_seconds: u64,
    pub recovery_seconds: u64,
    pub max_reprices: usize,
    pub leverage: u32,
}
impl Plan {
    pub fn read(path: &Path) -> Result<Self> {
        let p: Self = read(path)?;
        p.validate()?;
        Ok(p)
    }
    pub fn validate(&self) -> Result<()> {
        self.maker.validate()?;
        self.hedge.validate()?;
        if self.schema != "native_perp_pair_v1"
            || self.run_id.is_empty()
            || self.run_id.len() > 64
            || !self
                .run_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
            || self.maker.venue == self.hedge.venue
            || ![&self.maker.venue, &self.hedge.venue]
                .iter()
                .all(|v| matches!(v.as_str(), "aster" | "hyperliquid" | "lighter"))
            || self.maker.network != Network::Mainnet
            || self.hedge.network != Network::Mainnet
            || self.maker.tif != TimeInForce::PostOnly
            || self.maker.reduce_only != self.hedge.reduce_only
            || !matches!(
                (self.maker.side.as_str(), self.hedge.side.as_str()),
                ("BUY", "SELL") | ("SELL", "BUY")
            )
            || d(&self.maker.quantity)? != d(&self.hedge.quantity)?
            || d(&self.clip_quantity)? <= D::ZERO
            || d(&self.clip_quantity)? > d(&self.maker.quantity)?
            || d(&self.max_gap)? < D::ZERO
            || d(&self.max_gap)? > d("0.1")?
            || d(&self.hedge_slippage_bps)? < D::ZERO
            || d(&self.hedge_slippage_bps)? > d("1000")?
            || !(1..=120).contains(&self.maker_wait_seconds)
            || !(10..=120).contains(&self.recovery_seconds)
            || !(1..=1000).contains(&self.max_reprices)
            || !(1..=125).contains(&self.leverage)
        {
            return Err("invalid native pair identity/side/quantity/policy".into());
        }
        match self.hedge_price_mode.as_str() {
            "market" if self.hedge.venue == "aster" && self.hedge.tif == TimeInForce::Market => (),
            "dynamic_ioc"
                if matches!(self.hedge.venue.as_str(), "hyperliquid" | "lighter")
                    && self.hedge.tif == TimeInForce::Ioc => {}
            "fixed_ioc" if self.hedge.tif == TimeInForce::Ioc => (),
            _ => return Err("invalid hedge price mode".into()),
        }
        Ok(())
    }
    pub async fn resolve(&self) -> Result<Resolved> {
        self.validate()?;
        let mut ml = self.maker.clone();
        let mut hl = self.hedge.clone();
        ml.quantity = self.clip_quantity.clone();
        hl.quantity = self.clip_quantity.clone();
        let (mut m, mut h) =
            tokio::try_join!(ml.resolve_with_metadata(), hl.resolve_with_metadata())?;
        for metadata in [&m.1, &h.1] {
            if metadata.native_base != self.symbol
                || metadata.canonical_base != self.symbol
                || metadata.units().is_none()
            {
                return Err(
                    "native pair requires identical base units; aliases/multipliers unsupported"
                        .into(),
                );
            }
        }
        let hr = if self.hedge.tif == TimeInForce::Market {
            h.1.market.as_ref().ok_or("market lot rules unavailable")?
        } else {
            &h.1.limit
        };
        let step = common_lot(&[m.1.limit.lot(), hr.lot()])
            .map_err(|e| e.to_string())?
            .ok_or("missing common lot")?;
        if d(&self.maker.quantity)? % step != D::ZERO || d(&self.clip_quantity)? % step != D::ZERO {
            return Err(
                "quantity/clip must match both venues' common lot; never rounded up".into(),
            );
        }
        m.0.quantity = self.maker.quantity.clone();
        h.0.quantity = self.hedge.quantity.clone();
        Ok(Resolved {
            maker: m.0,
            hedge: h.0,
            maker_rules: Rules::from(&m.1.limit),
            hedge_rules: Rules::from(hr),
            step: step.to_string(),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    tick: Option<String>,
    sz_decimals: Option<u32>,
    lot: String,
    min: String,
    notional: Option<String>,
    max: Option<String>,
    min_price: Option<String>,
    max_price: Option<String>,
}
impl Rules {
    fn from(i: &Instrument) -> Self {
        let (tick, sz_decimals) = match i.price_rule() {
            PriceRule::Tick(t) => (Some(t.to_string()), None),
            PriceRule::Hyperliquid { sz_decimals } => (None, Some(sz_decimals)),
        };
        Self {
            tick,
            sz_decimals,
            lot: i.lot().to_string(),
            min: i.min_size().to_string(),
            notional: i.min_notional().map(|v| v.to_string()),
            max: i.max_size().map(|v| v.to_string()),
            min_price: i.price_bounds().0.map(|v| v.to_string()),
            max_price: i.price_bounds().1.map(|v| v.to_string()),
        }
    }
    fn instrument(&self) -> Result<Instrument> {
        let price = match (&self.tick, self.sz_decimals) {
            (Some(t), None) => PriceRule::Tick(d(t)?),
            (None, Some(sz_decimals)) => PriceRule::Hyperliquid { sz_decimals },
            _ => return Err("invalid saved price rules".into()),
        };
        Instrument::new(
            price,
            d(&self.lot)?,
            d(&self.min)?,
            self.notional.as_deref().map(d).transpose()?,
            self.max.as_deref().map(d).transpose()?,
        )
        .and_then(|i| {
            i.with_price_bounds(
                self.min_price
                    .as_deref()
                    .map(decimal)
                    .transpose()?
                    .unwrap_or(D::ZERO),
                self.max_price
                    .as_deref()
                    .map(decimal)
                    .transpose()?
                    .unwrap_or(D::ZERO),
            )
        })
        .map_err(|e| e.to_string())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resolved {
    pub maker: Order,
    pub hedge: Order,
    maker_rules: Rules,
    hedge_rules: Rules,
    step: String,
}
impl Resolved {
    fn validate_plan(&self, p: &Plan) -> Result<()> {
        for (o, leg, rules) in [
            (&self.maker, &p.maker, &self.maker_rules),
            (&self.hedge, &p.hedge, &self.hedge_rules),
        ] {
            o.validate().map_err(|e| e.to_string())?;
            if o.venue != leg.venue
                || o.network != leg.network
                || o.account != leg.account
                || o.symbol != leg.symbol
                || o.side != leg.side
                || o.tif != leg.tif
                || o.reduce_only != leg.reduce_only
                || d(&o.quantity)? != d(&leg.quantity)?
                || d(&o.limit_price)? != d(&leg.limit_price)?
            {
                return Err("resolved identity differs from native plan".into());
            }
            let i = rules.instrument()?;
            if d(&o.quantity)? % i.lot() != D::ZERO {
                return Err("saved quantity violates lot".into());
            }
        }
        let step = common_lot(&[
            self.maker_rules.instrument()?.lot(),
            self.hedge_rules.instrument()?.lot(),
        ])
        .map_err(|e| e.to_string())?
        .ok_or("missing lot")?;
        if step != d(&self.step)? || d(&p.clip_quantity)? % step != D::ZERO {
            return Err("saved common lot mismatch".into());
        }
        Ok(())
    }
}
pub(crate) fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    if bytes.len() > 1_048_576 {
        return Err("native pair file too large".into());
    }
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}
pub(crate) fn save<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    f.write_all(&serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?)
        .and_then(|_| f.sync_all())
        .map_err(|e| e.to_string())
}
pub async fn preview(p: &Plan) -> Result<serde_json::Value> {
    let r = p.resolve().await?;
    Ok(
        serde_json::json!({"mode":"preview","schema":p.schema,"maker":r.maker,"hedge":r.hedge,"common_lot":r.step,"hedge_price_mode":p.hedge_price_mode,"can_submit_orders":false}),
    )
}
pub(crate) fn passive(side: &str, q: &Quote) -> D {
    if side == "BUY" {
        q.bid()
    } else {
        q.ask()
    }
}
pub(crate) fn entry(p: &Plan, price: D, hedge: &Quote, size: D) -> Result<bool> {
    let hs = if p.hedge.side == "BUY" {
        Side::Buy
    } else {
        Side::Sell
    };
    let Some(levels) = hedge.levels(hs) else {
        return Ok(false);
    };
    let Some(vwap) = perp_arb_core::market::vwap(levels, size).map_err(|e| e.to_string())? else {
        return Ok(false);
    };
    if p.hedge_price_mode == "fixed_ioc" {
        let limit = d(&p.hedge.limit_price)?;
        let mut left = size;
        for level in levels {
            if (hs == Side::Buy && level.price > limit) || (hs == Side::Sell && level.price < limit)
            {
                return Ok(false);
            }
            left -= left.min(level.size);
            if left == D::ZERO {
                break;
            }
        }
    }
    let adverse = if p.maker.side == "BUY" {
        price - vwap
    } else {
        vwap - price
    };
    Ok(adverse / price <= d(&p.max_gap)?)
}
pub(crate) fn hedge_price(p: &Plan, r: &Resolved, q: Option<&Quote>) -> Result<D> {
    if p.hedge_price_mode != "dynamic_ioc" {
        return d(&p.hedge.limit_price);
    }
    let q =
        q.ok_or("Hyperliquid hedge needs a current quote to form IOC price; exposure retained")?;
    let mid = if p.hedge.venue == "lighter" {
        if p.hedge.side == "BUY" {
            q.ask()
        } else {
            q.bid()
        }
    } else {
        (q.bid() + q.ask()) / D::TWO
    };
    let slip = d(&p.hedge_slippage_bps)? / D::from(10000);
    let buy = p.hedge.side == "BUY";
    r.hedge_rules
        .instrument()?
        .round_price(
            mid * (if buy { D::ONE + slip } else { D::ONE - slip }),
            if buy { Direction::Up } else { Direction::Down },
        )
        .map_err(|e| e.to_string())
}
