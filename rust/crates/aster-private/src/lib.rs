//! Aster v3 events and reads, plus explicit single-limit submit/cancel methods.
//! The private probe stays read-only; trading uses the separate order-test CLI.
#![forbid(unsafe_code)]

pub mod auth;
pub mod client;
pub mod monitor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Credentials,
    Clock,
    Network,
    Timeout,
    Http(u16),
    Exchange(i64),
    InvalidPayload,
    BodyLimit,
    Closed,
    WsLimit,
    AlreadyRunning,
    ListenKeyExpired,
    Coordination(perp_arb_private_events::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never include raw bodies, tokens, request URLs or signature material.
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<perp_arb_private_events::Error> for Error {
    fn from(e: perp_arb_private_events::Error) -> Self {
        Self::Coordination(e)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
