use crate::{d, Result};
use perp_arb_core::{
    instruments::ExactAmount,
    pricing::{round_step, Direction},
    Decimal as D,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    BinanceSpot,
    Aster,
    Hyperliquid,
    Lighter,
}
fn default_perp() -> Venue {
    Venue::Aster
}
impl Venue {
    pub fn parse_perp(value: &str) -> Result<Self> {
        match value {
            "aster" => Ok(Self::Aster),
            "hyperliquid" => Ok(Self::Hyperliquid),
            "lighter" => Ok(Self::Lighter),
            "variational" => Err("Variational requires the extension bridge and verified order constraints; indicative public quotes cannot size a spot hedge".into()),
            _ => Err("unsupported perp venue; expected aster/hyperliquid/lighter".into()),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Open,
    Close,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}
impl Side {
    pub fn opposite(self) -> Self {
        if self == Self::Buy {
            Self::Sell
        } else {
            Self::Buy
        }
    }
}
pub fn side(mode: Mode, venue: Venue) -> Side {
    if (mode == Mode::Open) == (venue == Venue::BinanceSpot) {
        Side::Buy
    } else {
        Side::Sell
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Book {
    pub bid: String,
    pub ask: String,
    pub bid_size: String,
    pub ask_size: String,
    pub age_ms: u64,
}
#[derive(Clone, Copy)]
struct Prices {
    bid: D,
    ask: D,
    bid_size: D,
    ask_size: D,
}
impl Book {
    fn prices(&self) -> Result<Prices> {
        let p = Prices {
            bid: d(&self.bid)?,
            ask: d(&self.ask)?,
            bid_size: d(&self.bid_size)?,
            ask_size: d(&self.ask_size)?,
        };
        if p.bid <= D::ZERO || p.ask <= p.bid || p.bid_size < D::ZERO || p.ask_size < D::ZERO {
            return Err("invalid/crossed best-level quote".into());
        }
        Ok(p)
    }
}
impl Prices {
    fn maker(self, side: Side) -> D {
        if side == Side::Buy {
            self.bid
        } else {
            self.ask
        }
    }
    fn taker(self, side: Side) -> (D, D) {
        if side == Side::Buy {
            (self.ask, self.ask_size)
        } else {
            (self.bid, self.bid_size)
        }
    }
    fn spread(self) -> Result<D> {
        div(
            mul(sub(self.ask, self.bid)?, D::TWO)?,
            add(self.bid, self.ask)?,
        )
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    pub step: String,
    pub minimum: String,
    pub maximum: Option<String>,
    pub min_notional: String,
    pub max_notional: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Resting {
    pub maker: Venue,
    pub price: String,
    pub quantity: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub mode: Mode,
    #[serde(default = "default_perp")]
    pub perp_venue: Venue,
    pub remaining: String,
    pub clip_limit: Option<String>,
    pub max_gap: String,
    pub spot: Book,
    pub perp: Book,
    pub rules: Rules,
    pub resting: Option<Resting>,
}
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Plan {
    pub maker: Venue,
    pub maker_side: Side,
    pub taker: Venue,
    pub taker_side: Side,
    pub quantity: String,
    pub maker_price: String,
    pub hedge_price: String,
    pub adverse_gap: String,
}
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestingAction {
    Keep,
    CancelForGap,
    CancelForQuote,
    CancelThenReprice,
}
#[derive(Debug, Clone, Serialize)]
pub struct Decision {
    pub candidate: Option<Plan>,
    pub resting_action: Option<RestingAction>,
    pub reason: String,
}
fn arithmetic(value: Option<D>) -> Result<D> {
    value.ok_or_else(|| "decimal arithmetic overflow".into())
}
fn add(a: D, b: D) -> Result<D> {
    arithmetic(a.checked_add(b))
}
fn sub(a: D, b: D) -> Result<D> {
    arithmetic(a.checked_sub(b))
}
fn mul(a: D, b: D) -> Result<D> {
    perp_arb_core::instruments::exact_product(a, b).map_err(|e| e.to_string())
}
fn div(a: D, b: D) -> Result<D> {
    arithmetic(a.checked_div(b))
}
fn floor(a: D, step: D) -> Result<D> {
    round_step(a, Some(step), Direction::Down).map_err(|e| e.to_string())
}
fn ceil(a: D, step: D) -> Result<D> {
    round_step(a, Some(step), Direction::Up).map_err(|e| e.to_string())
}
fn text(a: D) -> String {
    a.normalize().to_string()
}
fn difference(m: D, h: D, side: Side) -> Result<D> {
    if side == Side::Buy {
        sub(m, h)
    } else {
        sub(h, m)
    }
}
fn gap(m: D, h: D, side: Side) -> Result<D> {
    div(difference(m, h, side)?, m)
}
// Strict '<', matching Python's >= rejection, without rounded division at the boundary.
fn acceptable(m: D, h: D, side: Side, limit: D) -> Result<bool> {
    Ok(ExactAmount::product(m, limit)
        .map_err(|e| e.to_string())?
        .compare(difference(m, h, side)?)
        .map_err(|e| e.to_string())?
        .is_gt())
}
fn positive(value: &str) -> Result<D> {
    let n = d(value)?;
    if n > D::ZERO {
        Ok(n)
    } else {
        Err("expected positive decimal".into())
    }
}

pub fn evaluate(input: &Input) -> Result<Decision> {
    evaluate_roles(input, false)
}
/// The first native live route is perpetual maker -> Binance market hedge.
/// Preview/replay still compare both roles. Never silently switch live transport.
pub fn evaluate_perp_maker(input: &Input) -> Result<Decision> {
    evaluate_roles(input, true)
}
fn evaluate_roles(input: &Input, perp_only: bool) -> Result<Decision> {
    if input.perp_venue == Venue::BinanceSpot {
        return Err("perp venue cannot be Binance Spot".into());
    }
    if input
        .resting
        .as_ref()
        .is_some_and(|o| o.maker != Venue::BinanceSpot && o.maker != input.perp_venue)
    {
        return Err("resting maker does not belong to the selected pair".into());
    }
    let remaining = positive(&input.remaining)?;
    let step = positive(&input.rules.step)?;
    if remaining.checked_rem(step) != Some(D::ZERO) {
        return Err("remaining is not a common lot multiple".into());
    }
    let gap_limit = d(&input.max_gap)?;
    if gap_limit <= D::ZERO || gap_limit >= D::ONE {
        return Err("gap must be between zero and one".into());
    }
    let min_qty = positive(&input.rules.minimum)?;
    let min_value = positive(&input.rules.min_notional)?;
    let max_qty = input
        .rules
        .maximum
        .as_deref()
        .map(positive)
        .transpose()?
        .unwrap_or(D::MAX);
    let max_value = input
        .rules
        .max_notional
        .as_deref()
        .map(positive)
        .transpose()?;
    let clip = input
        .clip_limit
        .as_deref()
        .map(positive)
        .transpose()?
        .unwrap_or(remaining);
    if min_qty > max_qty || max_value.is_some_and(|v| v < min_value) {
        return Err("inconsistent size limits".into());
    }
    let stale = input.spot.age_ms > 1000 || input.perp.age_ms > 1000;
    if stale {
        // New orders/repricing require both books. A resting limit is already
        // known: its spread needs only the opposite venue's fresh executable
        // quote, not a fresh publication of the maker's unchanged public book.
        let resting_action = if let Some(old) = &input.resting {
            let price = positive(&old.price)?;
            positive(&old.quantity)?;
            let (maker, hedge) = if old.maker == input.perp_venue {
                (&input.perp, &input.spot)
            } else {
                (&input.spot, &input.perp)
            };
            Some(if hedge.age_ms > 1000 || maker.age_ms == u64::MAX {
                // MAX is the feed owner's missing/disconnected sentinel.
                RestingAction::CancelForQuote
            } else {
                let side = side(input.mode, old.maker);
                let (hedge_price, _) = hedge.prices()?.taker(side.opposite());
                if acceptable(price, hedge_price, side, gap_limit)? {
                    RestingAction::Keep
                } else {
                    RestingAction::CancelForGap
                }
            })
        } else {
            None
        };
        return Ok(Decision {
            candidate: None,
            resting_action,
            reason: if resting_action == Some(RestingAction::Keep) {
                "maker_quote_stale_hold"
            } else {
                "stale_quote"
            }
            .into(),
        });
    }
    let spot = input.spot.prices()?;
    let perp = input.perp.prices()?;
    let books = |maker| {
        if maker == input.perp_venue {
            (perp, spot)
        } else {
            (spot, perp)
        }
    };
    let resting_action = if let Some(old) = &input.resting {
        let price = positive(&old.price)?;
        positive(&old.quantity)?; // Quantity changes alone intentionally never cancel a resting order.
        let (m, h) = books(old.maker);
        let s = side(input.mode, old.maker);
        let (hedge, _) = h.taker(s.opposite());
        Some(if !acceptable(price, hedge, s, gap_limit)? {
            RestingAction::CancelForGap
        } else if m.maker(s) != price && acceptable(m.maker(s), hedge, s, gap_limit)? {
            RestingAction::CancelThenReprice
        } else {
            RestingAction::Keep
        })
    } else {
        None
    };
    let mut best: Option<(D, D, Plan)> = None;
    let mut below_minimum = false;
    for maker in [input.perp_venue, Venue::BinanceSpot] {
        if perp_only && maker == Venue::BinanceSpot {
            continue;
        }
        let (m, h) = books(maker);
        let s = side(input.mode, maker);
        let price = m.maker(s);
        let (hedge, capacity) = h.taker(s.opposite());
        if !acceptable(price, hedge, s, gap_limit)? {
            continue;
        }
        let low = mul(price.min(m.bid).min(h.bid), sub(D::ONE, gap_limit)?)?;
        let high = mul(price.max(m.ask).max(h.ask), add(D::ONE, gap_limit)?)?;
        let minimum = ceil(min_qty.max(div(min_value, low)?), step)?;
        if remaining < minimum {
            below_minimum = true;
            continue;
        }
        let mut maximum = remaining.min(max_qty).min(clip);
        if let Some(cap) = max_value {
            maximum = maximum.min(div(cap, high)?);
        }
        let mut quantity = floor(maximum.min(capacity), step)?;
        let tail = sub(remaining, quantity)?;
        if tail > D::ZERO && tail < minimum {
            quantity = floor(sub(remaining, minimum)?, step)?;
        }
        if quantity < minimum {
            continue;
        }
        let adverse = gap(price, hedge, s)?;
        let spread = h.spread()?;
        let plan = Plan {
            maker,
            maker_side: s,
            taker: if maker == input.perp_venue {
                Venue::BinanceSpot
            } else {
                input.perp_venue
            },
            taker_side: s.opposite(),
            quantity: text(quantity),
            maker_price: text(price),
            hedge_price: text(hedge),
            adverse_gap: text(adverse),
        };
        if best
            .as_ref()
            .is_none_or(|(g, sp, _)| (adverse, spread) < (*g, *sp))
        {
            best = Some((adverse, spread, plan));
        }
    }
    let candidate = best.map(|(_, _, p)| p);
    let reason = if candidate.is_some() {
        "ready"
    } else if below_minimum {
        "remainder_below_minimum"
    } else {
        "gap_or_depth"
    }
    .into();
    Ok(Decision {
        candidate,
        resting_action,
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input() -> Input {
        serde_json::from_value(serde_json::json!({
        "mode":"close","remaining":"1000","clip_limit":null,"max_gap":"0.0025",
        "spot":{"bid":"0.11469","ask":"0.1147","bid_size":"300","ask_size":"400","age_ms":0},
        "perp":{"bid":"0.1147","ask":"0.1148","bid_size":"500","ask_size":"600","age_ms":0},
        "rules":{"step":"1","minimum":"1","maximum":null,"min_notional":"5","max_notional":null},"resting":null
    })).unwrap()
    }
    #[test]
    fn best_level_and_side_selection() {
        let p = evaluate(&input()).unwrap().candidate.unwrap();
        assert_eq!(p.maker, Venue::Aster);
        assert_eq!(p.maker_side, Side::Buy);
        assert_eq!(p.quantity, "300");
    }
    #[test]
    fn every_perp_can_be_maker_or_taker_for_both_directions() {
        for venue in [Venue::Aster, Venue::Hyperliquid, Venue::Lighter] {
            for mode in [Mode::Open, Mode::Close] {
                let mut i = input();
                i.perp_venue = venue;
                i.mode = mode;
                // Equal prices: prefer wider maker spread. Swap books to force
                // each role, checking the hedge direction and venue as well.
                i.spot.bid = "1".into();
                i.spot.ask = "1.001".into();
                i.perp.bid = "0.999".into();
                i.perp.ask = "1.002".into();
                for expected in [venue, Venue::BinanceSpot] {
                    let p = evaluate(&i).unwrap().candidate.unwrap();
                    assert_eq!(p.maker, expected);
                    assert_eq!(p.maker_side, side(mode, expected));
                    assert_eq!(p.taker_side, p.maker_side.opposite());
                    assert_eq!(
                        p.taker,
                        if expected == venue {
                            Venue::BinanceSpot
                        } else {
                            venue
                        }
                    );
                    std::mem::swap(&mut i.spot, &mut i.perp);
                }
            }
        }
    }
    #[test]
    fn selected_pair_must_match_resting_order_even_when_stale() {
        let mut i = input();
        i.perp_venue = Venue::Hyperliquid;
        i.spot.age_ms = 2000;
        i.resting = Some(Resting {
            maker: Venue::Aster,
            price: "0.1147".into(),
            quantity: "10".into(),
        });
        assert!(evaluate(&i).is_err());
        i.resting = None;
        i.perp_venue = Venue::BinanceSpot;
        assert!(evaluate(&i).is_err());
    }
    #[test]
    fn quantity_change_keeps_queue_priority() {
        let mut i = input();
        i.resting = Some(Resting {
            maker: Venue::Aster,
            price: "0.1147".into(),
            quantity: "300".into(),
        });
        i.spot.bid_size = "1".into();
        assert_eq!(
            evaluate(&i).unwrap().resting_action,
            Some(RestingAction::Keep)
        );
    }
    #[test]
    fn hold_safe_old_price_and_cancel_unsafe_original() {
        let mut i = input();
        i.perp.bid = "0.1151".into();
        i.perp.ask = "0.1152".into();
        i.resting = Some(Resting {
            maker: Venue::Aster,
            price: "0.1147".into(),
            quantity: "300".into(),
        });
        assert_eq!(
            evaluate(&i).unwrap().resting_action,
            Some(RestingAction::Keep)
        );
        i.spot.bid = "0.114".into();
        assert_eq!(
            evaluate(&i).unwrap().resting_action,
            Some(RestingAction::CancelForGap)
        );
    }
    #[test]
    fn strict_boundary() {
        assert!(!acceptable(
            d("0.115").unwrap(),
            d("0.1147125").unwrap(),
            Side::Buy,
            d("0.0025").unwrap()
        )
        .unwrap());
    }
    #[test]
    fn stale_hedge_cancels_and_does_not_plan() {
        let mut i = input();
        i.spot.age_ms = 1001;
        i.resting = Some(Resting {
            maker: Venue::Aster,
            price: "0.1147".into(),
            quantity: "300".into(),
        });
        let o = evaluate(&i).unwrap();
        assert!(o.candidate.is_none());
        assert_eq!(o.resting_action, Some(RestingAction::CancelForQuote));
    }
    #[test]
    fn saga_observed_quotes_hold_original_without_new_order() {
        let mut i = input();
        i.mode = Mode::Open;
        i.perp_venue = Venue::Hyperliquid;
        i.resting = Some(Resting {
            maker: Venue::Hyperliquid,
            price: "0.01339".into(),
            quantity: "10000".into(),
        });
        i.spot.bid = "0.01329".into();
        i.spot.ask = "0.01331".into();
        for (spot_age, perp_age) in [(472, 1035), (74, 1008), (424, 1014)] {
            i.spot.age_ms = spot_age;
            i.perp.age_ms = perp_age;
            let result = evaluate_perp_maker(&i).unwrap();
            assert_eq!(result.resting_action, Some(RestingAction::Keep));
            assert_eq!(result.reason, "maker_quote_stale_hold");
            assert!(result.candidate.is_none());
        }
    }
    #[test]
    fn maker_staleness_never_bypasses_gap_hedge_freshness_or_disconnect_checks() {
        for maker in [Venue::Aster, Venue::BinanceSpot] {
            for mode in [Mode::Open, Mode::Close] {
                let mut i = input();
                i.mode = mode;
                i.resting = Some(Resting {
                    maker,
                    price: "100".into(),
                    quantity: "300".into(),
                });
                let s = side(mode, maker);
                let (m, h) = if maker == Venue::Aster {
                    (&mut i.perp, &mut i.spot)
                } else {
                    (&mut i.spot, &mut i.perp)
                };
                m.age_ms = 1500;
                // Even a different stale best price must not trigger repricing.
                m.bid = "99".into();
                m.ask = "101".into();
                h.bid = "99.99".into();
                h.ask = "100.01".into();
                assert_eq!(
                    evaluate(&i).unwrap().resting_action,
                    Some(RestingAction::Keep)
                );
                let h = if maker == Venue::Aster {
                    &mut i.spot
                } else {
                    &mut i.perp
                };
                if s == Side::Buy {
                    h.bid = "99.75".into();
                } else {
                    h.ask = "100.25".into();
                }
                assert_eq!(
                    evaluate(&i).unwrap().resting_action,
                    Some(RestingAction::CancelForGap)
                );
                let h = if maker == Venue::Aster {
                    &mut i.spot
                } else {
                    &mut i.perp
                };
                h.age_ms = 1001;
                assert_eq!(
                    evaluate(&i).unwrap().resting_action,
                    Some(RestingAction::CancelForQuote)
                );
                h_reset(&mut i, maker);
                let m = if maker == Venue::Aster {
                    &mut i.perp
                } else {
                    &mut i.spot
                };
                m.age_ms = u64::MAX;
                assert_eq!(
                    evaluate(&i).unwrap().resting_action,
                    Some(RestingAction::CancelForQuote)
                );
                i.resting = None;
                assert!(evaluate(&i).unwrap().candidate.is_none());
            }
        }
        fn h_reset(i: &mut Input, maker: Venue) {
            let h = if maker == Venue::Aster {
                &mut i.spot
            } else {
                &mut i.perp
            };
            h.age_ms = 0;
            h.bid = "99.99".into();
            h.ask = "100.01".into();
        }
    }
    #[test]
    fn invalid_lot_rejected_no_rounding_user_total() {
        let mut i = input();
        i.remaining = "1.1".into();
        assert!(evaluate(&i).is_err());
    }
}
