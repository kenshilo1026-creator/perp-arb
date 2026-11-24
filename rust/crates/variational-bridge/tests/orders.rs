use perp_arb_variational_bridge::orders::{prepare_submit, record_submit_result, Command, Ledger};
use serde_json::json;

#[test]
fn submit_protocol_preserves_close_reduce_only_and_ack_cannot_bind_or_fill() {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).unwrap();
    let path = std::env::temp_dir().join(format!(
        "bridge-orders-{:x}.wal",
        u128::from_le_bytes(bytes)
    ));
    let command = Command {
        request_id: "request-1".into(),
        account: "fixture".into(),
        symbol: "ETH".into(),
        side: "SELL".into(),
        quantity: "1.25".into(),
        limit_price: "100".into(),
        reduce_only: true,
        max_notional: "125".into(),
    };
    let mut ledger = Ledger::create(&path, command).unwrap();
    let payload = prepare_submit(ledger.dispatch().unwrap());
    assert_eq!(payload["reduceOnly"], true);
    assert_eq!(payload["amount"], "1.25");
    assert_eq!(payload["orderType"], "LIMIT");
    assert_eq!(payload["submitOnly"], true);
    record_submit_result(
        &mut ledger,
        &json!({"type":"ORDER_RESULT","requestId":"request-1","ok":true,
        "orderId":"unverified-id","filled":true,"details":{"quantity":"1.25"}}),
    )
    .unwrap();
    assert!(ledger.view().order_id.is_none());
    assert_eq!(ledger.view().cumulative, "0");
    assert!(!ledger.view().terminal_confirmed);
    assert!(record_submit_result(
        &mut ledger,
        &json!({"type":"ORDER_RESULT","requestId":"other","ok":true})
    )
    .is_err());
    record_submit_result(
        &mut ledger,
        &json!({"type":"ORDER_RESULT","requestId":"request-1","ok":false}),
    )
    .unwrap();
    assert!(ledger.dispatch().is_err());
    drop(ledger);
    let mut ledger = Ledger::open(&path).unwrap();
    assert!(ledger.dispatch().is_err());
    drop(ledger);
    std::fs::remove_file(path).unwrap();
}
