use perp_arb_core::{decimal, Decimal};
use perp_arb_variational_bridge::{
    evidence::{BoundTrades, Evidence},
    wire::{decode, Channel, Event, FillCandidate, Observation, Position},
};
use serde_json::{json, Value};

const NOW: i64 = 1_700_000_000_000;
fn envelope(path: &str, value: Value) -> Value {
    json!({"kind":"ws_frame", "direction":"received", "opcode":1,
        "requestId":"chrome-socket-id-not-order-id", "timestamp":"2023-11-14T22:13:20Z",
        "url":format!("wss://omni-ws-server.prod.ap-northeast-1.variational.io/{path}"),
        "payloadData":value.to_string()})
}
fn decode_private(v: &Value) -> Result<Observation, String> {
    Ok(decode(Channel::Private, &v.to_string(), NOW)?.unwrap().1)
}
fn event(observation: Observation, generation: u64, captured_ms: i64) -> Event {
    Event {
        channel: Channel::Private,
        generation,
        received_ms: NOW,
        captured_ms: Some(captured_ms),
        received_elapsed_us: 0,
        parsed_elapsed_us: 0,
        observation,
    }
}
fn portfolio(quantity: &str) -> Observation {
    Observation::Portfolio {
        wallet_balance: None,
        positions: vec![Position {
            symbol: "ETH".into(),
            signed_quantity: quantity.into(),
        }],
    }
}
fn fill(id: &str, quantity: &str) -> FillCandidate {
    FillCandidate {
        symbol: Some("ETH".into()),
        side: Some("BUY".into()),
        order_id: Some("order-1".into()),
        trade_id: Some(id.into()),
        quantity: Some(quantity.into()),
        quote_amount: None,
        price: None,
        status: None,
        trade_type: None,
        instrument: None,
    }
}

#[test]
fn full_portfolio_is_exact_sorted_and_does_not_alias_units() {
    let v = envelope(
        "portfolio",
        json!({"positions":[
        {"position_info":{"instrument":{"underlying":"ETH-PERP"},"qty":"-1.2300"}},
        {"instrument":"1000PEPE", "qty":3}]}),
    );
    assert_eq!(
        decode_private(&v).unwrap(),
        Observation::Portfolio {
            wallet_balance: None,
            positions: vec![
                Position {
                    symbol: "1000PEPE".into(),
                    signed_quantity: "3".into()
                },
                Position {
                    symbol: "ETH".into(),
                    signed_quantity: "-1.23".into()
                }
            ]
        }
    );
    assert_eq!(
        perp_arb_variational_bridge::wire::symbol("ETHUSDT").unwrap(),
        "ETHUSDT"
    );
}

#[test]
fn malformed_duplicate_swap_or_float_invalidates_whole_portfolio() {
    for rows in [
        json!([{"instrument":"ETH","qty":"1"},{"instrument":"ETH-PERP","qty":"2"}]),
        json!([{"instrument":"ETH-SWAP","qty":"1"}]),
        json!([{"instrument":"ETH","qty":1.2}]),
        json!([{"instrument":"ETH","qty":"1"},{}]),
    ] {
        assert!(decode_private(&envelope("portfolio", json!({"positions":rows}))).is_err());
    }
    assert_eq!(
        decode_private(&envelope("portfolio", json!({"positions":[]}))).unwrap(),
        Observation::Portfolio {
            positions: vec![],
            wallet_balance: None
        }
    );
}

#[test]
fn captures_require_timezone_freshness_and_exact_source() {
    let good = envelope("portfolio", json!({"positions":[]}));
    for stamp in [
        "2023-11-14T22:13:20",
        "2023-11-14T22:13:14Z",
        "2023-11-14T22:13:22Z",
    ] {
        let mut bad = good.clone();
        bad["timestamp"] = json!(stamp);
        assert!(decode_private(&bad).is_err());
    }
    let mut bad = good.clone();
    bad["url"] = json!("wss://omni-ws-server.prod.ap-northeast-1.variational.io.attacker/events");
    assert!(decode_private(&bad).is_err());
    let mut sent = good;
    sent["direction"] = json!("sent");
    assert!(decode(Channel::Private, &sent.to_string(), NOW)
        .unwrap()
        .is_none());
}

#[test]
fn fill_candidate_never_guesses_order_id_or_mix_nested_fields() {
    let raw = envelope(
        "events",
        json!({"type":"trade", "id":"ambiguous-id", "symbol":"ETH", "side":"LONG",
        "qty":"0.25", "child":{"order_id":"wrong-order","trade_id":"wrong-trade"}}),
    );
    let Observation::FillCandidate { candidate } = decode_private(&raw).unwrap() else {
        panic!()
    };
    assert_eq!(candidate.order_id, None);
    assert_eq!(candidate.trade_id, None);
    assert_eq!(candidate.quantity.as_deref(), Some("0.25"));
    assert_eq!(candidate.side.as_deref(), Some("BUY"));
    let mut bound = BoundTrades::new("ETH", "BUY", "order-1", "1").unwrap();
    assert!(bound.apply_verified_incremental(*candidate).is_err());
    assert_eq!(bound.cumulative(), Decimal::ZERO);
}

#[test]
fn nested_trade_on_portfolio_matches_existing_python_fixture_without_guessing_id() {
    let raw = envelope(
        "portfolio",
        json!({"type":"trade","data":{
        "instrument":{"underlying":"NXPC"},"side":"buy","qty":"1000","status":"confirmed"}}),
    );
    let Observation::FillCandidate { candidate } = decode_private(&raw).unwrap() else {
        panic!()
    };
    assert_eq!(candidate.symbol.as_deref(), Some("NXPC"));
    assert_eq!(candidate.quantity.as_deref(), Some("1000"));
    assert_eq!(candidate.side.as_deref(), Some("BUY"));
    assert!(candidate.order_id.is_none());
    assert!(candidate.trade_id.is_none());
}

#[test]
fn nested_trade_rejects_cross_object_identity_and_batch_ambiguity() {
    for value in [
        json!({"type":"trade","order_id":"outer","data":{"qty":"2"}}),
        json!({"type":"trade","data":[{"qty":"1"},{"qty":"2"}]}),
    ] {
        assert!(decode_private(&envelope("portfolio", value)).is_err());
    }
}

#[test]
fn source_rfq_is_order_identity_and_id_remains_trade_identity() {
    let trade = json!({"type":"trade","data":{"source_rfq":"rfq-1","id":"trade-1",
        "instrument":{"underlying":"ETH"},"side":"buy","qty":"2","status":"confirmed"}});
    let Observation::FillCandidate { candidate } =
        decode_private(&envelope("events", trade.clone())).unwrap()
    else {
        panic!()
    };
    assert_eq!(candidate.order_id.as_deref(), Some("rfq-1"));
    assert_eq!(candidate.trade_id.as_deref(), Some("trade-1"));
    let mut bad = trade.clone();
    bad["data"]["order_id"] = json!("other");
    assert!(decode_private(&envelope("events", bad)).is_err());
    let mut bad = trade;
    bad["source_rfq"] = json!("outer");
    assert!(decode_private(&envelope("events", bad)).is_err());
}

#[test]
fn diagnostics_discard_account_payload_tokens_and_headers() {
    let raw = envelope(
        "events",
        json!({"type":"new_schema","authorization":"secret-123","data":{"cookie":"secret-456"}}),
    );
    let output = serde_json::to_string(&decode_private(&raw).unwrap()).unwrap();
    assert!(!output.contains("secret-"));
    let reference = json!({"kind":"rest_response","timestamp":"2023-11-14T22:13:20Z","status":200,
        "url":"https://omni.variational.io/api/quotes/indicative?token=secret-123",
        "headers":{"cookie":"secret-456"},"body":"secret-789"});
    let result = decode(Channel::Reference, &reference.to_string(), NOW)
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(result, Observation::ReferenceObserved);
}

#[test]
fn disconnect_generation_and_age_invalidate_portfolio() {
    let mut state = Evidence::default();
    state.apply(&event(portfolio("1"), 1, NOW)).unwrap();
    assert!(state.portfolio(NOW + 5000).is_some());
    assert!(state.portfolio(NOW + 5001).is_none());
    assert!(state.portfolio(NOW - 1).is_none());
    state
        .apply(&event(Observation::Disconnected, 1, NOW))
        .unwrap();
    assert!(state.portfolio(NOW).is_none());
    state.apply(&event(portfolio("2"), 2, NOW + 1)).unwrap();
    assert!(state.apply(&event(portfolio("1"), 1, NOW)).is_err());
    assert_eq!(state.portfolio(NOW + 2).unwrap()[0].signed_quantity, "2");
    state
        .apply(&event(
            Observation::Gap {
                reason: "upstream closed".into(),
            },
            2,
            NOW + 2,
        ))
        .unwrap();
    assert!(state.portfolio(NOW + 2).is_none());
}

#[test]
fn duplicate_capture_does_not_refresh_age_and_conflict_is_rejected() {
    let mut state = Evidence::default();
    state.apply(&event(portfolio("1"), 1, NOW)).unwrap();
    let mut duplicate = event(portfolio("1"), 1, NOW);
    duplicate.received_ms = NOW + 4000;
    state.apply(&duplicate).unwrap();
    assert!(state.portfolio(NOW + 5001).is_none());
    assert!(state.apply(&event(portfolio("2"), 1, NOW)).is_err());
    assert!(state.portfolio(NOW).is_none());
}

#[test]
fn backwards_portfolio_capture_is_rejected() {
    let mut state = Evidence::default();
    state.apply(&event(portfolio("1"), 1, NOW)).unwrap();
    assert!(state.apply(&event(portfolio("2"), 1, NOW - 1)).is_err());
    assert!(state.portfolio(NOW).is_none());
}

#[test]
fn partial_trades_are_exact_and_duplicate_never_double_hedges() {
    let mut state = BoundTrades::new("ETH", "BUY", "order-1", "1").unwrap();
    assert_eq!(
        state.apply_verified_incremental(fill("a", "0.25")).unwrap(),
        decimal("0.25").unwrap()
    );
    assert_eq!(
        state.apply_verified_incremental(fill("a", "0.25")).unwrap(),
        Decimal::ZERO
    );
    assert_eq!(
        state.apply_verified_incremental(fill("b", "0.75")).unwrap(),
        decimal("0.75").unwrap()
    );
    assert_eq!(state.cumulative(), Decimal::ONE);
    assert!(!state.requires_reconciliation());
}

#[test]
fn conflict_identity_and_overfill_are_sticky_not_clamped_or_retried() {
    for mutation in 0..5 {
        let mut state = BoundTrades::new("ETH", "BUY", "order-1", "1").unwrap();
        state.apply_verified_incremental(fill("a", "0.25")).unwrap();
        let mut bad = fill("b", "0.25");
        match mutation {
            0 => bad.symbol = Some("BTC".into()),
            1 => bad.side = Some("SELL".into()),
            2 => bad.order_id = Some("other".into()),
            3 => bad.quantity = Some("1".into()),
            _ => {
                bad.trade_id = Some("a".into());
                bad.quantity = Some("0.3".into());
            }
        }
        assert!(state.apply_verified_incremental(bad).is_err());
        assert!(state.requires_reconciliation());
        assert!(state.apply_verified_incremental(fill("c", "0.75")).is_err());
        assert_eq!(state.cumulative(), decimal("0.25").unwrap());
    }
}
