use super::*;
use perp_arb_private_events::orders::Network;
use serde_json::json;
#[test]
fn selected_position_does_not_require_collateral_assets_or_other_positions() {
    let mut a = json!({"positions":[{"symbol":"ETH","position":"2","sign":-1},{"symbol":"BTC","position":"unrelated"}]});
    let i = parse_position(&a, "ETH").unwrap();
    assert_eq!(
        i.positions.values().copied().collect::<Vec<_>>(),
        vec![-D::from(2)]
    );
    assert!(i.wallets.is_empty());
    a["positions"][0]["sign"] = json!(0);
    assert!(parse_position(&a, "ETH").is_err());
    assert!(parse_position(&json!({}), "ETH").is_err());
}
fn scope() -> Scope {
    Scope {
        venue: "lighter".into(),
        account: "100".into(),
    }
}
fn row() -> Value {
    json!({"owner_account_index":100,"market_index":0,"order_index":281474976710700u64,"client_order_index":123,"initial_base_amount":"1.000","filled_base_amount":"0.25","remaining_base_amount":"0.75","is_ask":false,"status":"open","updated_at":1700000000123u64})
}
fn spec() -> ResolvedInstrument {
    perp_arb_market_data::lighter::parse_metadata(&json!({"code":200,"order_book_details":[{"symbol":"ETH","market_type":"perp","status":"active","market_id":0,"supported_price_decimals":2,"supported_size_decimals":3,"min_base_amount":"0.01","min_quote_amount":"10"}]}),"ETH").unwrap()
}
fn order() -> Order {
    Order {
        venue: "lighter".into(),
        network: Network::Mainnet,
        account: "100".into(),
        symbol: "ETH".into(),
        side: "BUY".into(),
        quantity: "1".into(),
        limit_price: "2000.01".into(),
        tif: TimeInForce::PostOnly,
        reduce_only: false,
        client_id: "123".into(),
        asset: Some(0),
    }
}
#[test]
fn real_order_identity_and_cancel_partial_fill_are_preserved() {
    let mut v = row();
    v["status"] = json!("canceled-post-only");
    let f = decode_order(&v, &scope(), "ETH", 0).unwrap();
    assert_eq!(f.order.key.order_id, "281474976710700");
    assert_eq!(f.client_id.as_deref(), Some("123"));
    assert_eq!(f.order.cumulative, D::new(25, 2));
    assert_eq!(f.order.status, Status::Canceled);
    order().request().unwrap().verify(&f).unwrap();
}
#[test]
fn archive_lookup_requires_exact_client_index_and_unambiguous_identity() {
    let request = order().request().unwrap();
    let mut different = row();
    different["client_order_index"] = json!(456);
    let page = json!({"orders":[different.clone(),row()]});
    assert!(find_exact(&page, &request, 0, "123").unwrap().is_some());
    assert!(
        find_exact(&json!({"orders":[different]}), &request, 0, "123")
            .unwrap()
            .is_none()
    );
    assert!(find_exact(&json!({"orders":[row(),row()]}), &request, 0, "123").is_err());
    let mut wrong = row();
    wrong["owner_account_index"] = json!(101);
    assert!(find_exact(&json!({"orders":[wrong]}), &request, 0, "123").is_err());
}
#[test]
fn missing_fields_and_cross_account_never_become_zero_fill() {
    for field in [
        "filled_base_amount",
        "remaining_base_amount",
        "initial_base_amount",
        "is_ask",
        "order_index",
        "owner_account_index",
        "client_order_index",
        "market_index",
    ] {
        let mut v = row();
        v.as_object_mut().unwrap().remove(field);
        assert!(decode_order(&v, &scope(), "ETH", 0).is_err(), "{field}");
    }
    for (field, value) in [
        ("owner_account_index", json!(101)),
        ("market_index", json!(1)),
        ("remaining_base_amount", json!("0.8")),
        ("order_id", json!("123")),
        ("client_order_id", json!("999")),
        ("status", json!("filled")),
    ] {
        let mut v = row();
        v[field] = value;
        assert!(decode_order(&v, &scope(), "ETH", 0).is_err(), "{field}");
    }
}
#[test]
fn exact_wire_scaling_and_post_only_ioc_reduce_flags() {
    let mut o = order();
    let w = order_wire(&o, &spec()).unwrap();
    assert_eq!(
        (w.size, w.price, w.tif, w.ask, w.reduce),
        (1000, 200001, 2, 0, 0)
    );
    assert!(w.expiry > now());
    o.tif = TimeInForce::Ioc;
    o.side = "SELL".into();
    o.reduce_only = true;
    let w = order_wire(&o, &spec()).unwrap();
    assert_eq!((w.tif, w.ask, w.reduce, w.expiry), (0, 1, 1, 0));
    for (q, p) in [
        ("1.0001", "2000.01"),
        ("1", "2000.011"),
        ("0.001", "2000.01"),
        ("1", "50000000000000"),
    ] {
        o.quantity = q.into();
        o.limit_price = p.into();
        assert!(order_wire(&o, &spec()).is_err());
    }
}
#[test]
fn numeric_account_and_client_ids_are_canonical_and_bounded() {
    for id in ["0", "01", "-1", "281474976710656", "tx_hash"] {
        let mut o = order();
        o.client_id = id.into();
        assert!(o.validate().is_err());
    }
    for account in [
        "01",
        "-1",
        "281474976710656",
        "0x0000000000000000000000000000000000000001",
    ] {
        let mut o = order();
        o.account = account.into();
        assert!(o.validate().is_err());
    }
    let mut o = order();
    o.tif = TimeInForce::Market;
    assert!(o.validate().is_err());
}
#[test]
fn inventory_requires_complete_open_orders_and_preserves_short_positions() {
    let mut a = json!({"account_index":100,"collateral":"20","pending_order_count":0,"total_order_count":0,"positions":[{"symbol":"ETH","position":"2","sign":-1}],"assets":[]});
    let i = parse_inventory(&a, &json!({"orders":[]}), &scope()).unwrap();
    assert_eq!(i.positions.values().next(), Some(&-D::TWO));
    a["total_order_count"] = json!(1);
    assert!(parse_inventory(&a, &json!({"orders":[]}), &scope()).is_err());
    a["pending_order_count"] = json!(1);
    assert!(parse_inventory(&a, &json!({"orders":[]}), &scope()).is_err());
}
#[test]
#[ignore = "requires pinned official Windows signer; offline synthetic key, never sends"]
fn official_signer_offline_abi() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../vendor/lighter/lighter-signer-windows-amd64.dll");
    let signer = signer::Signer::load(&path, &format!("{}01", "00".repeat(39)), 100, 3).unwrap();
    let auth = signer.auth(now() / 1000 + 600).unwrap();
    assert!(auth.split(':').count() == 4);
    assert!(auth.contains(":100:3:"));
    let wire = signer
        .order(&order_wire(&order(), &spec()).unwrap(), 42)
        .unwrap();
    let v: Value = serde_json::from_str(&wire.info).unwrap();
    assert_eq!(wire.kind, 14);
    assert_eq!(v["AccountIndex"], 100);
    assert_eq!(v["Nonce"], 42);
    assert_eq!(v["BaseAmount"], 1000);
    assert_eq!(v["Price"], 200001);
    assert_eq!(v["TimeInForce"], 2);
    assert_eq!(v["ClientOrderIndex"], 123);
    assert!(v["Sig"].as_str().is_some_and(|s| !s.is_empty()));
    let cancel = signer.cancel(0, 281474976710700, 43).unwrap();
    assert_eq!(cancel.kind, 15);
    let leverage = signer.leverage(0, 10000, 44).unwrap();
    assert_eq!(leverage.kind, 20);
}
