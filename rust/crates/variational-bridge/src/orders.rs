//! Legacy UI wire preparation only; native receipt-backed transport lives in native.
//! Durable permissions are not transport retries or exchange idempotency keys.
use crate::Result;
use perp_arb_execution::durable::browser::DispatchPermit;
pub use perp_arb_execution::durable::browser::{Command, Identity, Ledger};
use serde_json::{json, Value};

/// Consume the single dispatch permit. Retrying this message is NOT safe: the
/// extension/venue does not guarantee idempotence for requestId.
pub fn prepare_submit(permit: DispatchPermit) -> Value {
    let c = permit.command();
    json!({"type":"PLACE_ORDER", "requestId":c.request_id, "account":c.account,
        "symbol":c.symbol, "side":c.side, "amount":c.quantity, "price":c.limit_price,
        "orderType":"LIMIT", "reduceOnly":c.reduce_only, "submitOnly":true})
}

/// A UI success is only an ACK. A UI error may occur after a successful click,
/// so neither result can prove rejection, terminal state or a zero fill.
pub fn record_submit_result(ledger: &mut Ledger, value: &Value) -> Result<()> {
    if !ledger.view().dispatched
        || value["type"] != "ORDER_RESULT"
        || value["requestId"] != ledger.view().command.request_id
    {
        return Err("submission response identity mismatch".into());
    }
    match value["ok"].as_bool() {
        Some(true) => ledger
            .submit_ack(value["requestId"].as_str().ok_or("request ID missing")?)
            .map_err(|e| e.to_string()),
        Some(false) => ledger.unresolved().map_err(|e| e.to_string()),
        None => Err("submission response missing boolean result".into()),
    }
}
