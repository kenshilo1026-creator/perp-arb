use crate::{positive, Decimal, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

/// Input for tick rounding may come from a Python Decimal intermediate whose
/// scale exceeds 28. Preserve its digits until AFTER rounding to the tick.
/// This does not relax the precision contract of stored prices/quantities.
#[derive(Debug, Clone, Copy)]
pub enum RoundingInput {
    Decimal(Decimal),
    Extended { coefficient: i128, scale: u32 },
}

impl RoundingInput {
    pub fn parse(text: &str) -> Result<Self> {
        if let Ok(value) = crate::decimal(text) {
            return Ok(Self::Decimal(value));
        }
        let (negative, magnitude) = if let Some(rest) = text.strip_prefix('-') {
            (true, rest)
        } else {
            (false, text.strip_prefix('+').unwrap_or(text))
        };
        let (whole, fraction) = magnitude.split_once('.').ok_or(Error::InvalidDecimal)?;
        if whole.is_empty() && fraction.is_empty()
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|c| c.is_ascii_digit())
            || fraction.len() > 38
        {
            return Err(Error::InvalidDecimal);
        }
        let digits = format!("{whole}{fraction}");
        let coefficient = digits.parse::<i128>().map_err(|_| Error::InvalidDecimal)?;
        Ok(Self::Extended {
            coefficient: if negative { -coefficient } else { coefficient },
            scale: fraction.len() as u32,
        })
    }

    pub fn round(self, step: Option<Decimal>, direction: Direction) -> Result<Decimal> {
        let Self::Extended { coefficient, scale } = self else {
            let Self::Decimal(value) = self else {
                unreachable!()
            };
            return round_step(value, step, direction);
        };
        let step = step
            .filter(|s| *s > Decimal::ZERO)
            .ok_or(Error::InvalidDecimal)?;
        let common = scale.max(step.scale());
        let align = |number: i128, difference: u32| {
            10_i128
                .checked_pow(difference)
                .and_then(|power| number.checked_mul(power))
                .ok_or(Error::Arithmetic)
        };
        let numerator = align(coefficient, common - scale)?;
        let denominator = align(step.mantissa(), common - step.scale())?;
        let mut units = numerator
            .checked_div(denominator)
            .ok_or(Error::Arithmetic)?;
        let remainder = numerator
            .checked_rem(denominator)
            .ok_or(Error::Arithmetic)?;
        if direction == Direction::Up && remainder > 0 {
            units = units.checked_add(1).ok_or(Error::Arithmetic)?;
        } else if direction == Direction::Down && remainder < 0 {
            units = units.checked_sub(1).ok_or(Error::Arithmetic)?;
        }
        let mut result = units
            .checked_mul(step.mantissa())
            .ok_or(Error::Arithmetic)?;
        let mut result_scale = step.scale();
        while result_scale > 0 && result % 10 == 0 {
            result /= 10;
            result_scale -= 1;
        }
        Decimal::try_from_i128_with_scale(result, result_scale).map_err(|_| Error::Arithmetic)
    }
}

/// Same denominator and sign as spread_strategy.core.spread_bps.
/// Decimal division is rounded; this is NOT an entry/exit decision gate.
pub fn spread_bps(short: Decimal, long: Decimal) -> Result<Decimal> {
    positive(short)?;
    positive(long)?;
    short
        .checked_sub(long)
        .and_then(|d| d.checked_div(long))
        .and_then(|r| r.checked_mul(Decimal::from(10_000)))
        .ok_or(Error::Arithmetic)
}

/// Fraction, not percent: 0.002 = 0.2%. A favourable gap stays negative.
/// Only for observing/pricing makers, never for blocking an already-needed hedge.
pub fn adverse_price_gap(maker: Decimal, taker: Decimal, side: Side) -> Result<Decimal> {
    positive(maker)?;
    positive(taker)?;
    let difference = match side {
        Side::Buy => maker.checked_sub(taker),
        Side::Sell => taker.checked_sub(maker),
    };
    difference
        .and_then(|v| v.checked_div(maker))
        .ok_or(Error::Arithmetic)
}

/// Remainder avoids rounding the quotient across a tick boundary.
/// Non-positive/missing steps preserve Python's pass-through behaviour.
pub fn round_step(value: Decimal, step: Option<Decimal>, direction: Direction) -> Result<Decimal> {
    let Some(step) = step.filter(|s| *s > Decimal::ZERO) else {
        return Ok(value);
    };
    let remainder = value.checked_rem(step).ok_or(Error::Arithmetic)?;
    let truncated = value.checked_sub(remainder).ok_or(Error::Arithmetic)?;
    match (direction, remainder.cmp(&Decimal::ZERO)) {
        (Direction::Up, std::cmp::Ordering::Greater) => {
            truncated.checked_add(step).ok_or(Error::Arithmetic)
        }
        (Direction::Down, std::cmp::Ordering::Less) => {
            truncated.checked_sub(step).ok_or(Error::Arithmetic)
        }
        _ => Ok(truncated),
    }
}
