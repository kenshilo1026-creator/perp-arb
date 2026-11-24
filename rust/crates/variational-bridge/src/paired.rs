//! Explicit bounded two-leg orders. No automatic strategy or maker re-submission.
pub(crate) mod fast;
mod plan;
pub(crate) mod runtime;
pub use plan::{Baseline, Plan, Resolved};
pub use runtime::{execute, inspect, preview, Action};

#[cfg(test)]
pub(crate) mod tests;
