use crate::{positive, pricing::Side, Decimal, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

/// An empty visible book means no liquidity, not an unlimited top-of-book quote.
/// Unsized is ONLY for a venue quote that intentionally publishes no sizes.
#[derive(Debug, Clone)]
pub enum Depth {
    Visible(Vec<Level>),
    Unsized,
}

#[derive(Debug, Clone)]
pub struct Quote {
    bid: Decimal,
    ask: Decimal,
    received_ms: i64,
    source_ms: Option<i64>,
    bids: Depth,
    asks: Depth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    Disconnected,
    Stale,
    Future,
    TransportLag,
}

#[derive(Debug, Clone, Copy)]
pub struct FreshnessPolicy {
    max_age_ms: i64,
    max_transport_lag_ms: i64,
    future_tolerance_ms: i64,
}

impl Default for FreshnessPolicy {
    fn default() -> Self {
        Self {
            max_age_ms: 1_000,
            max_transport_lag_ms: 500,
            future_tolerance_ms: 250,
        }
    }
}

impl FreshnessPolicy {
    pub fn new(
        max_age_ms: i64,
        max_transport_lag_ms: i64,
        future_tolerance_ms: i64,
    ) -> Result<Self> {
        if max_age_ms <= 0 || max_transport_lag_ms <= 0 || future_tolerance_ms <= 0 {
            return Err(Error::InvalidPolicy);
        }
        Ok(Self {
            max_age_ms,
            max_transport_lag_ms,
            future_tolerance_ms,
        })
    }
}

fn validate_depth(depth: &Depth, top: Decimal, side: Side) -> Result<()> {
    let Depth::Visible(levels) = depth else {
        return Ok(());
    };
    if levels.first().is_some_and(|l| l.price != top) {
        return Err(Error::InvalidBook);
    }
    for level in levels {
        positive(level.price)?;
        positive(level.size)?;
    }
    if levels.windows(2).any(|pair| match side {
        Side::Buy => pair[0].price >= pair[1].price,
        Side::Sell => pair[0].price <= pair[1].price,
    }) {
        return Err(Error::InvalidBook);
    }
    Ok(())
}

impl Quote {
    pub fn bid(&self) -> Decimal {
        self.bid
    }
    pub fn ask(&self) -> Decimal {
        self.ask
    }
    pub fn source_ms(&self) -> Option<i64> {
        self.source_ms
    }
    pub fn received_ms(&self) -> i64 {
        self.received_ms
    }
    pub fn levels(&self, side: Side) -> Option<&[Level]> {
        match self.side(side).1 {
            Depth::Visible(rows) => Some(rows),
            Depth::Unsized => None,
        }
    }

    pub fn new(
        bid: Decimal,
        ask: Decimal,
        received_ms: i64,
        source_ms: Option<i64>,
        bids: Depth,
        asks: Depth,
    ) -> Result<Self> {
        positive(bid)?;
        positive(ask)?;
        if bid > ask {
            return Err(Error::InvalidBook);
        }
        validate_depth(&bids, bid, Side::Sell)?;
        validate_depth(&asks, ask, Side::Buy)?;
        Ok(Self {
            bid,
            ask,
            received_ms,
            source_ms,
            bids,
            asks,
        })
    }

    /// Exchange wall-clock timestamps assess age. Use Instant for local latency elsewhere.
    pub fn freshness(&self, now_ms: i64, healthy: bool, policy: FreshnessPolicy) -> Freshness {
        if !healthy {
            return Freshness::Disconnected;
        }
        let source = i128::from(self.source_ms.unwrap_or(self.received_ms));
        let age = i128::from(now_ms) - source;
        let lag = i128::from(self.received_ms) - source;
        if age > i128::from(policy.max_age_ms) {
            return Freshness::Stale;
        }
        if age < -i128::from(policy.future_tolerance_ms) {
            return Freshness::Future;
        }
        if lag > i128::from(policy.max_transport_lag_ms) {
            return Freshness::TransportLag;
        }
        Freshness::Fresh
    }

    fn side(&self, side: Side) -> (Decimal, &Depth) {
        match side {
            Side::Buy => (self.ask, &self.asks),
            Side::Sell => (self.bid, &self.bids),
        }
    }

    pub fn executable(&self, side: Side, quantity: Option<Decimal>) -> Result<Option<Decimal>> {
        let (top, depth) = self.side(side);
        let Some(quantity) = quantity else {
            return Ok(Some(top));
        };
        positive(quantity)?;
        match depth {
            Depth::Unsized => Ok(Some(top)),
            Depth::Visible(levels) => vwap(levels, quantity),
        }
    }

    pub fn marginal(&self, side: Side, quantity: Decimal) -> Result<Option<Decimal>> {
        positive(quantity)?;
        let (top, depth) = self.side(side);
        let Depth::Visible(levels) = depth else {
            return Ok(Some(top));
        };
        let mut remaining = quantity;
        for level in levels {
            if remaining <= level.size {
                return Ok(Some(level.price));
            }
            remaining = remaining.checked_sub(level.size).ok_or(Error::Arithmetic)?;
        }
        Ok(None)
    }
}

/// Full requested quantity only. Thin depth yields None, never partial VWAP.
pub fn vwap(levels: &[Level], quantity: Decimal) -> Result<Option<Decimal>> {
    positive(quantity)?;
    let mut remaining = quantity;
    let mut cost = Decimal::ZERO;
    for level in levels {
        positive(level.price)?;
        positive(level.size)?;
        let take = level.size.min(remaining);
        let part = take
            .checked_mul(level.price)
            .filter(|v| *v > Decimal::ZERO)
            .ok_or(Error::Arithmetic)?;
        cost = cost.checked_add(part).ok_or(Error::Arithmetic)?;
        remaining = remaining.checked_sub(take).ok_or(Error::Arithmetic)?;
        if remaining <= Decimal::ZERO {
            return cost
                .checked_div(quantity)
                .filter(|v| *v > Decimal::ZERO)
                .map(Some)
                .ok_or(Error::Arithmetic);
        }
    }
    Ok(None)
}
