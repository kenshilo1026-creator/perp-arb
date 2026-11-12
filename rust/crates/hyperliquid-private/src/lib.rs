//! Address-only monitoring plus an explicitly authenticated, account-bound TradingClient.
#![forbid(unsafe_code)]
pub mod client;
pub mod monitor;
pub mod signing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Credentials,
    Clock,
    Network,
    Timeout,
    Protocol,
    Capacity,
    Closed,
    Http(u16),
    Coordination(perp_arb_private_events::Error),
}
impl From<perp_arb_private_events::Error> for Error {
    fn from(e: perp_arb_private_events::Error) -> Self {
        Self::Coordination(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
