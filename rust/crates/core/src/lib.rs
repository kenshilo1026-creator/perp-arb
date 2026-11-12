//! Batch 1: pure market calculations. No credentials, network or order submission.
#![forbid(unsafe_code)]

pub mod instruments;
pub mod market;
pub mod pricing;

pub use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidDecimal,
    NonPositive,
    InvalidBook,
    InvalidPolicy,
    Arithmetic,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Reject unrepresentable inputs instead of silently rounding their strings.
pub fn decimal(text: &str) -> Result<Decimal> {
    Decimal::from_str_exact(text).map_err(|_| Error::InvalidDecimal)
}

pub(crate) fn positive(value: Decimal) -> Result<()> {
    if value > Decimal::ZERO {
        Ok(())
    } else {
        Err(Error::NonPositive)
    }
}
