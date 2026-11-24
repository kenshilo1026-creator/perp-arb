//! Native local transport for the Chrome extension. CLI probes are read-only;
//! native library mutations require a durable permit and explicit extension opt-in.
//! Exact RFQ receipts are separate from fills and account-wide reconciliation.
#![forbid(unsafe_code)]
pub mod account_feed;
pub mod evidence;
pub mod execution_io;
pub mod inventory;
pub mod latency;
pub mod mid_preview;
pub mod native;
pub mod orders;
pub mod owner;
pub mod paired;
mod transport;
pub mod wire;
pub mod ws_validation;
pub use transport::{Bridge, Config, Health, ReadRequest};
pub type Result<T> = std::result::Result<T, String>;
