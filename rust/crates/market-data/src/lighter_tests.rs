use super::*;
fn frame(kind: &str, nonce: u64, begin: u64, bids: Value, asks: Value) -> Value {
    json!({"type":kind,"channel":"order_book:0","last_updated_at":1700000000000000u64,"offset":999,"order_book":{"nonce":nonce,"begin_nonce":begin,"bids":bids,"asks":asks}})
}
#[test]
fn deltas_keep_depth_and_nonce_gap_discards_entire_book() {
    let mut book = Book::default();
    let snapshot = frame(
        "subscribed/order_book",
        10,
        0,
        json!([{"price":"100","size":"1"},{"price":"99","size":"2"}]),
        json!([{"price":"101","size":"3"}]),
    );
    assert!(book.update(&snapshot, 0, "ETH", 1700000000001).is_ok());
    // Nonces need not be adjacent: begin_nonce is the integrity link, not offset+1.
    let delta = frame(
        "update/order_book",
        25,
        10,
        json!([{"price":"100","size":"0"}]),
        json!([]),
    );
    let s = book.update(&delta, 0, "ETH", 1700000000002).unwrap();
    assert_eq!(s.quote.bid(), D::from(99));
    assert_eq!(s.quote.ask(), D::from(101));
    assert_eq!(s.quote.source_ms(), Some(1700000000000));
    let gap = frame("update/order_book", 30, 26, json!([]), json!([]));
    assert!(book.update(&gap, 0, "ETH", 1700000000003).is_err());
    assert!(book
        .update(
            &frame("update/order_book", 31, 25, json!([]), json!([])),
            0,
            "ETH",
            1700000000004
        )
        .is_err());
    assert!(book.update(&snapshot, 0, "ETH", 1700000000005).is_ok());
}
#[test]
fn crossed_missing_time_wrong_market_and_negative_depth_invalidate() {
    let snapshot = frame(
        "subscribed/order_book",
        10,
        0,
        json!([{"price":"100","size":"1"}]),
        json!([{"price":"101","size":"2"}]),
    );
    for mutation in 0..4 {
        let mut v = snapshot.clone();
        match mutation {
            0 => v["channel"] = json!("order_book:1"),
            1 => {
                v.as_object_mut().unwrap().remove("last_updated_at");
            }
            2 => v["order_book"]["asks"][0]["price"] = json!("99"),
            _ => v["order_book"]["bids"][0]["size"] = json!("-1"),
        };
        assert!(Book::default().update(&v, 0, "ETH", 1700000000001).is_err());
    }
}
#[test]
fn metadata_only_accepts_exact_active_perpetual_and_precision() {
    let v = json!({"code":200,"order_book_details":[{"symbol":"ETH","market_id":0,"market_type":"perp","status":"active","min_base_amount":"0.01","min_quote_amount":"10","supported_price_decimals":2,"supported_size_decimals":3}]});
    let r = parse_metadata(&v, "ETH").unwrap();
    assert_eq!(r.asset_id, Some(0));
    assert_eq!(r.limit.lot(), D::new(1, 3));
    for (field, bad) in [
        ("status", json!("inactive")),
        ("market_type", json!("spot")),
        ("supported_price_decimals", json!(30)),
        ("multiplier", json!("1000")),
    ] {
        let mut broken = v.clone();
        broken["order_book_details"][0][field] = bad;
        assert!(parse_metadata(&broken, "ETH").is_err());
    }
}
