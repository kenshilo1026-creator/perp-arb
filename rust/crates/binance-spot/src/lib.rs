//! Native HMAC Spot transport. No automatic retry, strategy or live CLI.
//! A caller MUST durably record an intent before submitting. Transport errors
//! after dispatch are unknown outcomes, including timeout and malformed replies.
#![forbid(unsafe_code)]
pub mod auth;
pub mod client;
pub mod coordination;
pub mod events;
pub mod fees;
pub mod journal;
pub mod orders;
pub mod stream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Clock,
    RateLimited,
    Transport,
    Timeout,
    Protocol,
    Identity,
    Capacity,
    Arithmetic,
    AlreadySubmitted,
    StreamEnded,
    Io,
    Journal,
    Locked,
    Poisoned,
    Api { status: u16, code: Option<i64> },
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn number(s: &str) -> Result<perp_arb_core::Decimal> {
    perp_arb_core::decimal(s).map_err(|_| Error::Protocol)
}
pub(crate) fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
#[cfg(test)]
mod tests;
