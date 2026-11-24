use super::*;
const ACCOUNT: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
fn command() -> Command {
    Command {
        request_id: "request-1".into(),
        account: ACCOUNT.into(),
        symbol: "ETH".into(),
        side: "BUY".into(),
        quantity: "2".into(),
        limit_price: "100".into(),
        reduce_only: false,
        max_notional: "200".into(),
    }
}
fn instrument() -> Value {
    json!({"underlying":"ETH","instrument_type":"perpetual_future","settlement_asset":"USDC","funding_interval_s":3600})
}
fn receipt() -> Value {
    json!({"protocol":PROTOCOL,"started_ms":10000,"finished_ms":12000,"account":ACCOUNT,
        "session_continuity":true,"ok":true,"dispatched":false,
        "orders":{"source":format!("{ROOT}/orders/v2"),"observed_ms":11000,"atomic_snapshot":false,"window_complete":false,
        "rows":[{"rfq_id":"rfq-1","instrument":instrument(),"side":"buy","qty":"2","order_type":"limit",
            "limit_price":"100","status":"cleared","clearing_status":"success_trades_booked_into_pool",
            "is_reduce_only":false,"is_auto_resize":false,"use_mark_price":false,"response_server_ms":11000}]},
        "trades":{"source":format!("{ROOT}/trades"),"observed_ms":11500,"atomic_snapshot":false,"window_complete":false,
        "rows":[{"id":"trade-1","source_rfq":"rfq-1","instrument":instrument(),"side":"buy","qty":"2",
            "price":"99","status":"confirmed","trade_type":"trade","response_server_ms":11000}]}})
}
fn query(value: Value) -> Query {
    Query::new(
        Receipt::parse(&value, Some(ACCOUNT), 12000).unwrap(),
        Some("rfq-1".into()),
    )
    .unwrap()
}
fn wal() -> (std::path::PathBuf, Ledger) {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).unwrap();
    let path = std::env::temp_dir().join(format!("native-{:x}.wal", u128::from_le_bytes(bytes)));
    let ledger = Ledger::create(&path, command()).unwrap();
    (path, ledger)
}
#[test]
fn exact_cleared_rfq_and_confirmed_trades_prove_quantity() {
    let s = query(receipt())
        .snapshot(&command(), "rfq-1")
        .unwrap()
        .unwrap();
    assert_eq!(s.status, Status::Filled);
    assert_eq!(s.cumulative, decimal("2").unwrap());
    assert!(!s.requires_reconciliation);
    assert_eq!(s.event_ms, Some(11000));
}
#[test]
fn missing_rfq_and_canceled_row_are_never_zero_terminal() {
    let mut r = receipt();
    r["orders"]["rows"] = json!([]);
    assert!(query(r).snapshot(&command(), "rfq-1").unwrap().is_none());
    let mut r = receipt();
    r["orders"]["rows"][0]["status"] = json!("canceled");
    r["trades"]["rows"] = json!([]);
    let s = query(r).snapshot(&command(), "rfq-1").unwrap().unwrap();
    assert!(!s.status.terminal());
    assert!(s.requires_reconciliation);
}
#[test]
fn identity_includes_price_reduce_only_and_contract_type() {
    for (key, value) in [
        ("side", json!("sell")),
        ("qty", json!("3")),
        ("limit_price", json!("101")),
        ("is_reduce_only", json!(true)),
        ("order_type", json!("market")),
        ("use_mark_price", json!(true)),
    ] {
        let mut r = receipt();
        r["orders"]["rows"][0][key] = value;
        assert!(query(r).snapshot(&command(), "rfq-1").is_err(), "{key}");
    }
    for kind in ["swap", "perpetual_rwa_future"] {
        let mut r = receipt();
        r["orders"]["rows"][0]["instrument"]["instrument_type"] = json!(kind);
        assert!(query(r).snapshot(&command(), "rfq-1").is_err());
    }
}
#[test]
fn duplicate_trade_is_not_double_hedged_and_conflict_fails() {
    let mut r = receipt();
    let trade = r["trades"]["rows"][0].clone();
    r["trades"]["rows"].as_array_mut().unwrap().push(trade);
    assert_eq!(
        query(r.clone())
            .snapshot(&command(), "rfq-1")
            .unwrap()
            .unwrap()
            .cumulative,
        decimal("2").unwrap()
    );
    r["trades"]["rows"][1]["qty"] = json!("1");
    assert!(query(r).snapshot(&command(), "rfq-1").is_err());
}
#[test]
fn trade_id_is_not_rfq_id_and_pending_is_not_filled() {
    let mut r = receipt();
    r["trades"]["rows"][0]["source_rfq"] = json!("other");
    assert!(query(r).snapshot(&command(), "rfq-1").is_err());
    let mut r = receipt();
    r["trades"]["rows"][0]["status"] = json!("pending");
    let s = query(r).snapshot(&command(), "rfq-1").unwrap().unwrap();
    assert_eq!(s.cumulative, Decimal::ZERO);
    assert!(!s.status.terminal());
}
#[test]
fn overfill_float_quantity_and_unknown_status_are_rejected() {
    for (key, value) in [
        ("qty", json!("3")),
        ("qty", json!(2.0)),
        ("status", json!("cleared")),
        ("trade_type", json!("liquidation")),
    ] {
        let mut r = receipt();
        r["trades"]["rows"][0][key] = value;
        assert!(query(r).snapshot(&command(), "rfq-1").is_err());
    }
}
#[test]
fn stale_wrong_account_and_source_cannot_produce_query() {
    assert!(Receipt::parse(&receipt(), Some(ACCOUNT), 18000).is_err());
    assert!(Receipt::parse(&receipt(), Some("other"), 12000).is_err());
    let mut r = receipt();
    r["orders"]["source"] = json!("https://example.com/orders");
    assert!(Receipt::parse(&r, Some(ACCOUNT), 12000).is_err());
    let mut r = receipt();
    r["started_ms"] = json!(i64::MIN);
    r["finished_ms"] = json!(i64::MAX);
    assert!(Receipt::parse(&r, Some(ACCOUNT), 12000).is_err());
}
#[test]
fn missing_server_date_preserves_fills_but_cannot_finish_reconciliation() {
    let mut r = receipt();
    r["orders"]["rows"][0]["response_server_ms"] = Value::Null;
    let s = query(r).snapshot(&command(), "rfq-1").unwrap().unwrap();
    assert_eq!(s.cumulative, decimal("2").unwrap());
    assert!(s.requires_reconciliation);
    assert_eq!(s.status, Status::Filled);
    perp_arb_private_events::Update {
        key: s.key,
        original: s.original,
        cumulative: s.cumulative,
        status: s.status,
        event_ms: s.event_ms,
    }
    .validate()
    .unwrap();
}
#[test]
fn recorded_sources_strip_unknown_secrets() {
    let mut r = receipt();
    r["headers"] = json!({"authorization":"NEVER_RECORD"});
    r["orders"]["rows"][0]["token"] = json!("NEVER_RECORD");
    r["orders"]["rows"][0]["instrument"]["cookie"] = json!("NEVER_RECORD");
    let r = Receipt::parse(&r, Some(ACCOUNT), 12000).unwrap();
    assert!(!serde_json::to_string(&r).unwrap().contains("NEVER_RECORD"));
}
fn recovery() -> Value {
    json!({"current":{"protocol":PROTOCOL,"started_ms":10000,"finished_ms":12000,"account":ACCOUNT,
        "session_continuity":true,"ok":true,"dispatched":false,"source":format!("{ROOT}/me")},
        "dispatchRequestId":"request-1","stored":{"account":ACCOUNT,"action":"submit",
        "order":{"instrument":instrument(),"qty":"2","side":"buy","order_type":"limit","limit_price":"100",
            "is_reduce_only":false,"is_auto_resize":false,"use_mark_price":false,"slippage_limit":"0"},
        "receipt":{"protocol":PROTOCOL,"started_ms":2000,"finished_ms":3000,"account":ACCOUNT,
            "session_continuity":true,"ok":true,"dispatched":true,"rfq_id":"real-rfq","source":format!("{ROOT}/orders/new/limit")}}})
}
#[test]
fn lost_ack_recovery_binds_persisted_receipt_after_restart_without_resubmit() {
    let (path, mut ledger) = wal();
    drop(ledger.dispatch().unwrap());
    drop(ledger);
    let mut ledger = Ledger::open(&path).unwrap();
    let recovered = Recovery::parse(&recovery(), ACCOUNT, "request-1", 12000).unwrap();
    recovered.record(&mut ledger).unwrap();
    assert_eq!(ledger.view().order_id.as_deref(), Some("real-rfq"));
    assert!(ledger.dispatch().is_err());
    assert!(ledger.view().reconciliation_required);
    assert_eq!(ledger.view().cumulative, "0");
    drop(ledger);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn missing_receipt_and_reused_request_id_with_different_intent_remain_unknown() {
    for change in [false, true] {
        let (path, mut ledger) = wal();
        drop(ledger.dispatch().unwrap());
        let mut r = recovery();
        if change {
            r["stored"]["order"]["qty"] = json!("1");
        } else {
            r["stored"] = Value::Null;
        }
        assert!(Recovery::parse(&r, ACCOUNT, "request-1", 12000)
            .unwrap()
            .record(&mut ledger)
            .is_err());
        assert!(ledger.view().order_id.is_none());
        assert!(ledger.dispatch().is_err());
        drop(ledger);
        std::fs::remove_file(path).unwrap();
    }
}
