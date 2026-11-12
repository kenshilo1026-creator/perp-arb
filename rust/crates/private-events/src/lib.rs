//! Batch 3a: in-memory cumulative private order event coordination.
//! No credentials, private socket connection, REST requests or order submission.
#![forbid(unsafe_code)]

pub mod account;
pub mod actor;
pub mod aster;
pub mod hyperliquid;
pub mod ledger;
pub mod lookup;
pub mod orders;

pub use ledger::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidInput,
    InvalidQuantity,
    IdentityConflict,
    UnknownOrder,
    WrongSession,
    Capacity,
    Arithmetic,
    Closed,
    StreamExpired,
    TransportInUse,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Only static diagnostics: never retain payloads, URLs, tokens or keys.
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
