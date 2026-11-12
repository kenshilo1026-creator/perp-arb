use perp_arb_core::{decimal, pricing::Side};
use perp_arb_market_data::{parse_snapshot, FeedState, Update, Venue};
use serde_json::{json, Value};

fn hl(time: i64, depth: usize) -> Value {
    json!({"channel":"l2Book", "data":{"coin":"ETH", "time":time, "levels":[
        (0..depth).map(|i|json!({"px":(100-i).to_string(),"sz":"1","n":1})).collect::<Vec<_>>(),
        (0..depth).map(|i|json!({"px":(101+i).to_string(),"sz":"1","n":1})).collect::<Vec<_>>()
    ]}})
}
fn aster(time: i64, sequence: u64) -> Value {
    json!({"e":"depthUpdate","s":"ETHUSDT","T":time,"E":time+1,"u":sequence,
        "b":[["100","1"],["99","2"]],"a":[["101","1"],["102","2"]]})
}

#[test]
fn subscriptions_are_partial_snapshots_and_preserve_coin_spelling() {
    assert_eq!(
        Venue::Aster.endpoint("ETHUSDT").unwrap(),
        "wss://fstream.asterdex.com/ws/ethusdt@depth10@100ms"
    );
    assert!(Venue::Aster.subscription("ETHUSDT").is_none());
    assert_eq!(
        Venue::Hyperliquid.subscription("kPEPE").unwrap()["subscription"],
        json!({"type":"l2Book","coin":"kPEPE","fast":true})
    );
    assert!(Venue::Aster.endpoint("ETHUSDT?bad").is_err());
    assert!(Venue::Hyperliquid.endpoint("io:OAI").is_ok());
}

#[test]
fn smaller_snapshot_removes_old_tail_and_refuses_oversized_clip() {
    let mut feed = FeedState::default();
    feed.ingest(
        Venue::Hyperliquid,
        "ETH",
        &hl(10_000, 20).to_string(),
        10_000,
    )
    .unwrap();
    feed.ingest(
        Venue::Hyperliquid,
        "ETH",
        &hl(10_100, 5).to_string(),
        10_100,
    )
    .unwrap();
    let quote = &feed.latest().unwrap().quote;
    assert_eq!(quote.levels(Side::Buy).unwrap().len(), 5);
    assert_eq!(
        quote
            .executable(Side::Buy, Some(decimal("5").unwrap()))
            .unwrap(),
        Some(decimal("103").unwrap())
    );
    assert_eq!(
        quote
            .executable(Side::Buy, Some(decimal("6").unwrap()))
            .unwrap(),
        None
    );
    assert!(feed.fresh(11_100).is_some());
    assert!(feed.fresh(11_101).is_none());
}

#[test]
fn repeated_or_older_sequence_cannot_refresh_receipt_time() {
    let mut feed = FeedState::default();
    let frame = aster(10_000, 100).to_string();
    assert_eq!(
        feed.ingest(Venue::Aster, "ETHUSDT", &frame, 10_050)
            .unwrap(),
        Update::Accepted
    );
    assert_eq!(
        feed.ingest(Venue::Aster, "ETHUSDT", &frame, 10_500)
            .unwrap(),
        Update::OutOfOrder
    );
    assert_eq!(feed.latest().unwrap().quote.received_ms(), 10_050);
    assert_eq!(
        feed.ingest(
            Venue::Aster,
            "ETHUSDT",
            &aster(9_999, 101).to_string(),
            10_500
        )
        .unwrap(),
        Update::OutOfOrder
    );
    assert!(feed.fresh(11_001).is_none());
}

#[test]
fn disconnect_clears_book_and_new_connection_can_reset_sequence() {
    let mut feed = FeedState::default();
    feed.ingest(
        Venue::Aster,
        "ETHUSDT",
        &aster(10_000, 100).to_string(),
        10_000,
    )
    .unwrap();
    feed.disconnect();
    assert!(feed.latest().is_none());
    assert!(feed.fresh(10_000).is_none());
    assert_eq!(
        feed.ingest(
            Venue::Aster,
            "ETHUSDT",
            &aster(10_001, 1).to_string(),
            10_001
        )
        .unwrap(),
        Update::Accepted
    );
}

#[test]
fn invalid_snapshot_invalidates_previous_book() {
    let mut invalid = vec![
        json!({"channel":"error","data":"bad subscription"}),
        json!("not an object"),
    ];
    let valid = hl(10_000, 5);
    let mut empty = valid.clone();
    empty["data"]["levels"][0] = json!([]);
    invalid.push(empty);
    let mut wrong = valid.clone();
    wrong["data"]["coin"] = json!("BTC");
    invalid.push(wrong);
    let mut crossed = valid.clone();
    crossed["data"]["levels"][0][0]["px"] = json!("200");
    invalid.push(crossed);
    let mut no_time = valid.clone();
    no_time["data"].as_object_mut().unwrap().remove("time");
    invalid.push(no_time);
    let mut float = valid.clone();
    float["data"]["levels"][0][0]["sz"] = json!(1.1);
    invalid.push(float);
    for frame in invalid {
        let mut feed = FeedState::default();
        feed.ingest(Venue::Hyperliquid, "ETH", &valid.to_string(), 10_000)
            .unwrap();
        assert!(feed
            .ingest(Venue::Hyperliquid, "ETH", &frame.to_string(), 10_001)
            .is_err());
        assert!(feed.latest().is_none());
    }
}

#[test]
fn aster_wrapper_zero_size_and_source_time_follow_python_contract() {
    let mut raw = aster(10_000, 1);
    raw["a"].as_array_mut().unwrap().push(json!(["103", "0"]));
    let wrapper = json!({"stream":"ethusdt@depth10@100ms","data":raw});
    let book = parse_snapshot(Venue::Aster, "ETHUSDT", &wrapper.to_string(), 10_100)
        .unwrap()
        .unwrap();
    assert_eq!(book.quote.source_ms(), Some(10_000));
    assert_eq!(book.quote.levels(Side::Buy).unwrap().len(), 2);
    let mut diff = wrapper;
    diff["stream"] = json!("ethusdt@depth@100ms");
    assert!(parse_snapshot(Venue::Aster, "ETHUSDT", &diff.to_string(), 10_100).is_err());
}

#[test]
fn acknowledgements_do_not_make_a_disconnected_feed_healthy() {
    let mut feed = FeedState::default();
    assert_eq!(
        feed.ingest(
            Venue::Hyperliquid,
            "ETH",
            r#"{"channel":"subscriptionResponse","data":{}}"#,
            10_000
        )
        .unwrap(),
        Update::Ignored
    );
    assert!(feed.fresh(10_000).is_none());
    assert!(parse_snapshot(Venue::Hyperliquid, "ETH", "{", 10_000).is_err());
}

#[test]
fn a_future_timestamp_cannot_pin_the_cache_against_valid_new_quotes() {
    let mut feed = FeedState::default();
    assert!(feed
        .ingest(
            Venue::Hyperliquid,
            "ETH",
            &hl(20_000, 5).to_string(),
            10_000
        )
        .is_err());
    assert!(feed.latest().is_none());
    assert_eq!(
        feed.ingest(
            Venue::Hyperliquid,
            "ETH",
            &hl(10_001, 5).to_string(),
            10_001
        )
        .unwrap(),
        Update::Accepted
    );
}
