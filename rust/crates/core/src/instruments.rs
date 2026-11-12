//! Exact instrument arithmetic, independent of HTTP, symbols and order submission.
use crate::{
    positive,
    pricing::{round_step, Direction},
    Decimal, Error, Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceRule {
    Tick(Decimal),
    Hyperliquid { sz_decimals: u32 },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeError {
    BelowMinimumSize,
    InvalidLotSize,
    BelowMinimumNotional,
    AboveMaximumSize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceError {
    NonPositive,
    BelowMinimum,
    AboveMaximum,
    InvalidTick,
}
impl SizeError {
    pub fn code(self) -> &'static str {
        match self {
            Self::BelowMinimumSize => "below_minimum_size",
            Self::InvalidLotSize => "invalid_lot_size",
            Self::BelowMinimumNotional => "below_minimum_notional",
            Self::AboveMaximumSize => "above_maximum_size",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Instrument {
    price: PriceRule,
    lot: Decimal,
    min_size: Decimal,
    min_notional: Option<Decimal>,
    max_size: Option<Decimal>,
    min_price: Option<Decimal>,
    max_price: Option<Decimal>,
}
impl Instrument {
    pub fn new(
        price: PriceRule,
        lot: Decimal,
        min_size: Decimal,
        min_notional: Option<Decimal>,
        max_size: Option<Decimal>,
    ) -> Result<Self> {
        positive(lot)?;
        if min_size.checked_rem(lot).ok_or(Error::Arithmetic)? != Decimal::ZERO {
            return Err(Error::InvalidPolicy);
        }
        if min_size < Decimal::ZERO
            || min_notional.is_some_and(|n| n < Decimal::ZERO)
            || max_size.is_some_and(|n| n <= Decimal::ZERO || n < min_size)
        {
            return Err(Error::InvalidPolicy);
        }
        match price {
            PriceRule::Tick(tick) => positive(tick)?,
            PriceRule::Hyperliquid { sz_decimals } => {
                if sz_decimals > 6 || lot != Decimal::new(1, sz_decimals) {
                    return Err(Error::InvalidPolicy);
                }
            }
        }
        Ok(Self {
            price,
            lot,
            min_size,
            min_notional,
            max_size,
            min_price: None,
            max_price: None,
        })
    }
    pub fn with_price_bounds(mut self, min: Decimal, max: Decimal) -> Result<Self> {
        if min < Decimal::ZERO || max < Decimal::ZERO || (max > Decimal::ZERO && min > max) {
            return Err(Error::InvalidPolicy);
        }
        if let PriceRule::Tick(tick) = self.price {
            // Offset grids require a different common-lot/price lattice. Refuse
            // them rather than treating an offset as a zero-origin tick.
            if min.checked_rem(tick).ok_or(Error::Arithmetic)? != Decimal::ZERO {
                return Err(Error::InvalidPolicy);
            }
        }
        self.min_price = (min > Decimal::ZERO).then_some(min);
        self.max_price = (max > Decimal::ZERO).then_some(max);
        Ok(self)
    }
    pub fn price_error(&self, price: Decimal) -> Result<Option<PriceError>> {
        if price <= Decimal::ZERO {
            return Ok(Some(PriceError::NonPositive));
        }
        if self.min_price.is_some_and(|n| price < n) {
            return Ok(Some(PriceError::BelowMinimum));
        }
        if self.max_price.is_some_and(|n| price > n) {
            return Ok(Some(PriceError::AboveMaximum));
        }
        Ok((self.round_price(price, Direction::Down)? != price).then_some(PriceError::InvalidTick))
    }
    pub fn price_rule(&self) -> PriceRule {
        self.price
    }
    pub fn lot(&self) -> Decimal {
        self.lot
    }
    pub fn min_size(&self) -> Decimal {
        self.min_size
    }
    pub fn min_notional(&self) -> Option<Decimal> {
        self.min_notional
    }
    pub fn max_size(&self) -> Option<Decimal> {
        self.max_size
    }
    /// May return zero for a sub-lot request. Never silently increase quantity.
    pub fn round_quantity(&self, quantity: Decimal) -> Result<Decimal> {
        if quantity < Decimal::ZERO {
            return Err(Error::NonPositive);
        }
        round_step(quantity, Some(self.lot), Direction::Down)
    }
    pub fn round_price(&self, price: Decimal, direction: Direction) -> Result<Decimal> {
        positive(price)?;
        match self.price {
            PriceRule::Tick(tick) => round_step(price, Some(tick), direction),
            PriceRule::Hyperliquid { sz_decimals } => {
                let price = price.normalize();
                if price.scale() == 0 {
                    return Ok(price);
                } // Integer exception, including >5 digits.
                let integer_digits =
                    price.mantissa().to_string().len() as i32 - price.scale() as i32;
                let places = (5 - integer_digits).clamp(0, (6 - sz_decimals) as i32) as u32;
                round_step(price, Some(Decimal::new(1, places)), direction)
            }
        }
    }
    pub fn price_tolerance(&self, price: Decimal) -> Result<ExactAmount> {
        positive(price)?;
        match self.price {
            PriceRule::Tick(tick) => ExactAmount::product(tick, Decimal::ONE),
            PriceRule::Hyperliquid { .. } => ExactAmount::product(price, Decimal::new(1, 4)),
        }
    }
    /// Static size checks only. Does not validate risk, reduce-only exceptions,
    /// dynamic exchange filters or prove that an order will be accepted.
    pub fn size_error(
        &self,
        quantity: Decimal,
        price: Option<Decimal>,
    ) -> Result<Option<SizeError>> {
        if quantity <= Decimal::ZERO || quantity < self.min_size {
            return Ok(Some(SizeError::BelowMinimumSize));
        }
        if quantity.checked_rem(self.lot).ok_or(Error::Arithmetic)? != Decimal::ZERO {
            return Ok(Some(SizeError::InvalidLotSize));
        }
        if self.max_size.is_some_and(|n| quantity > n) {
            return Ok(Some(SizeError::AboveMaximumSize));
        }
        if let Some(price) = price {
            positive(price)?;
            if let Some(min) = self.min_notional.filter(|n| *n > Decimal::ZERO) {
                if ExactAmount::product(quantity, price)?.compare(min)? == std::cmp::Ordering::Less
                {
                    return Ok(Some(SizeError::BelowMinimumNotional));
                }
            }
        }
        Ok(None)
    }
}

/// A nonnegative intermediate product may need more than 28 decimal places.
/// Keep its exact coefficient/scale for comparisons and text; do not force it
/// through a rounded Decimal. Coefficient overflow still fails explicitly.
#[derive(Debug, Clone, Copy)]
pub struct ExactAmount {
    coefficient: u128,
    scale: u32,
}
impl ExactAmount {
    pub fn product(a: Decimal, b: Decimal) -> Result<Self> {
        if a < Decimal::ZERO || b < Decimal::ZERO {
            return Err(Error::NonPositive);
        }
        let (a, b) = (a.normalize(), b.normalize());
        let mut coefficient = (a.mantissa() as u128)
            .checked_mul(b.mantissa() as u128)
            .ok_or(Error::Arithmetic)?;
        let mut scale = a.scale() + b.scale();
        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        Ok(Self { coefficient, scale })
    }
    pub fn compare(self, value: Decimal) -> Result<std::cmp::Ordering> {
        if value < Decimal::ZERO {
            return Ok(std::cmp::Ordering::Greater);
        }
        if self.coefficient == 0 {
            return Ok(Decimal::ZERO.cmp(&value));
        }
        if value == Decimal::ZERO {
            return Ok(std::cmp::Ordering::Greater);
        }
        let value = value.normalize();
        let mut left = self.coefficient.to_string();
        let mut right = value.mantissa().to_string();
        let magnitude = (left.len() as i32 - self.scale as i32)
            .cmp(&(right.len() as i32 - value.scale() as i32));
        if magnitude != std::cmp::Ordering::Equal {
            return Ok(magnitude);
        }
        let width = left.len().max(right.len());
        left.extend(std::iter::repeat_n('0', width - left.len()));
        right.extend(std::iter::repeat_n('0', width - right.len()));
        Ok(left.cmp(&right))
    }
}
impl std::fmt::Display for ExactAmount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let digits = self.coefficient.to_string();
        if self.scale == 0 {
            return f.write_str(&digits);
        }
        let scale = self.scale as usize;
        if digits.len() <= scale {
            write!(f, "0.{}{digits}", "0".repeat(scale - digits.len()))
        } else {
            let point = digits.len() - scale;
            write!(f, "{}.{}", &digits[..point], &digits[point..])
        }
    }
}

/// checked_mul alone may round an underflow to zero. For a threshold or unit
/// conversion, require an exactly representable product or fail explicitly.
pub fn exact_product(a: Decimal, b: Decimal) -> Result<Decimal> {
    let (a, b) = (a.normalize(), b.normalize());
    let coefficient = a
        .mantissa()
        .checked_mul(b.mantissa())
        .ok_or(Error::Arithmetic)?;
    exact_scaled(coefficient, a.scale() + b.scale())
}
fn exact_scaled(mut coefficient: i128, mut scale: u32) -> Result<Decimal> {
    while scale > 0 && coefficient % 10 == 0 {
        coefficient /= 10;
        scale -= 1;
    }
    if scale > 28 {
        return Err(Error::Arithmetic);
    }
    Decimal::try_from_i128_with_scale(coefficient, scale).map_err(|_| Error::Arithmetic)
}
fn gcd(mut a: i128, mut b: i128) -> i128 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

pub fn common_lot(lots: &[Decimal]) -> Result<Option<Decimal>> {
    if lots.is_empty() {
        return Ok(None);
    }
    let lots: Vec<_> = lots.iter().map(|n| n.normalize()).collect();
    let scale = lots.iter().map(Decimal::scale).max().unwrap_or(0);
    let mut value = 1i128;
    for lot in lots {
        positive(lot)?;
        let units = lot
            .mantissa()
            .checked_mul(
                10_i128
                    .checked_pow(scale - lot.scale())
                    .ok_or(Error::Arithmetic)?,
            )
            .ok_or(Error::Arithmetic)?;
        value = (value / gcd(value, units))
            .checked_mul(units)
            .ok_or(Error::Arithmetic)?;
    }
    exact_scaled(value, scale).map(Some)
}

/// Explicit base units per native quantity. Aliases never imply a conversion.
#[derive(Debug, Clone, Copy)]
pub struct Units {
    base_per_native: Decimal,
}
impl Units {
    pub fn new(base_per_native: Decimal) -> Result<Self> {
        positive(base_per_native)?;
        Ok(Self { base_per_native })
    }
    pub fn base_per_native(self) -> Decimal {
        self.base_per_native
    }
    pub fn to_base(self, quantity: Decimal) -> Result<Decimal> {
        if quantity < Decimal::ZERO {
            return Err(Error::NonPositive);
        }
        exact_product(quantity, self.base_per_native)
    }
    /// Refuse a repeating or rounded conversion. Caller must deliberately choose
    /// a representable common base lot before attempting a neutral hedge.
    pub fn to_native(self, quantity: Decimal) -> Result<Decimal> {
        if quantity < Decimal::ZERO {
            return Err(Error::NonPositive);
        }
        let native = quantity
            .checked_div(self.base_per_native)
            .ok_or(Error::Arithmetic)?;
        if self.to_base(native)? != quantity {
            return Err(Error::Arithmetic);
        }
        Ok(native)
    }
    pub fn price_per_base(self, native_price: Decimal) -> Result<Decimal> {
        positive(native_price)?;
        self.to_native(native_price)
    }
}
