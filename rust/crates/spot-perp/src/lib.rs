//! Binance Spot / perpetual policy, public preview and explicit native live owner.
#![forbid(unsafe_code)]
pub mod live;
pub mod market;
pub mod metadata;
pub mod perp_feed;
pub mod policy;
pub mod preflight;
pub type Result<T> = std::result::Result<T, String>;
pub(crate) fn d(value: &str) -> Result<perp_arb_core::Decimal> {
    perp_arb_core::decimal(value).map_err(|e| e.to_string())
}
