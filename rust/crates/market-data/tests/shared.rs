use perp_arb_core::{decimal, pricing::Side};
use perp_arb_market_data::{
    shared::{parse_market, MarketKey, MarketState},
    Update, Venue,
};
use serde_json::{json, Value};

fn depth(time: i64) -> String {
    json!({"channel":"l2Book","data":{"coin":"ETH","time":time,"levels":[
        [{"px":"100","sz":"2"},{"px":"99","sz":"2"}],
        [{"px":"101","sz":"2"},{"px":"102","sz":"2"}]]}})
    .to_string()
}
fn bbo(time: i64, size: &str) -> String {
    json!({"channel":"bbo","data":{"coin":"ETH","time":time,"bbo":[{"px":"100","sz":size},{"px":"101","sz":"2"}]}}).to_string()
}
fn state() -> (MarketState, u64) {
    let mut s = MarketState::new(MarketKey::new(Venue::Hyperliquid, "ETH").unwrap());
    let g = s.begin();
    s.ingest(g, &depth(10000), 10010).unwrap();
    (s, g)
}
#[test]
fn subscriptions_are_native_case_correct_and_share_bbo_depth_socket() {
    assert_eq!(
        MarketKey::new(Venue::Aster, "ethusdt").unwrap(),
        MarketKey::new(Venue::Aster, "ETHUSDT").unwrap()
    );
    let a = MarketKey::new(Venue::Aster, "ethusdt").unwrap();
    assert!(a
        .endpoint()
        .ends_with("ethusdt@depth10@100ms/ethusdt@bookTicker"));
    assert!(a.subscriptions().is_empty());
    let h = MarketKey::new(Venue::Hyperliquid, "kPEPE").unwrap();
    assert_eq!(h.subscriptions()[0]["subscription"]["fast"], true);
    assert_eq!(h.subscriptions()[1]["subscription"]["type"], "bbo");
    assert_eq!(h.subscriptions()[1]["subscription"]["coin"], "kPEPE");
    assert_ne!(h, MarketKey::new(Venue::Hyperliquid, "KPEPE").unwrap());
    assert!(MarketKey::new(Venue::Aster, "ETH/USDT").is_err());
}
#[test]
fn changed_size_invalidates_full_depth_until_new_depth_arrives() {
    let (mut s, g) = state();
    assert!(s
        .view()
        .fresh(10010)
        .unwrap()
        .quote
        .executable(Side::Sell, Some(decimal("3").unwrap()))
        .unwrap()
        .is_some());
    s.ingest(g, &bbo(10020, "1"), 10030).unwrap();
    let quote = &s.view().fresh(10030).unwrap().quote;
    assert_eq!(quote.levels(Side::Sell).unwrap().len(), 1);
    assert_eq!(
        quote
            .executable(Side::Sell, Some(decimal("3").unwrap()))
            .unwrap(),
        None
    );
    s.ingest(g, &bbo(10040, "2"), 10050).unwrap();
    assert_eq!(
        s.view()
            .fresh(10050)
            .unwrap()
            .quote
            .levels(Side::Sell)
            .unwrap()
            .len(),
        1,
        "return to old BBO must not resurrect old depth"
    );
    s.ingest(g, &depth(10060), 10070).unwrap();
    assert_eq!(
        s.view()
            .fresh(10070)
            .unwrap()
            .quote
            .levels(Side::Sell)
            .unwrap()
            .len(),
        2
    );
}
#[test]
fn unchanged_bbo_does_not_refresh_depth_age_and_reads_recheck_clock() {
    let (mut s, g) = state();
    s.ingest(g, &bbo(10990, "2"), 11000).unwrap();
    assert_eq!(
        s.view().selected(11000).unwrap().quote.source_ms(),
        Some(10000)
    );
    assert!(s.view().fresh(11000).is_some());
    assert!(s.view().fresh(11001).is_none());
    s.ingest(g, &bbo(16000, "2"), 16010).unwrap();
    assert_eq!(
        s.view()
            .fresh(16010)
            .unwrap()
            .quote
            .levels(Side::Sell)
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn equal_source_time_conflict_is_conservative_in_either_arrival_order() {
    for bbo_first in [true, false] {
        let (mut s, g) = state();
        let frames = if bbo_first {
            [bbo(10020, "1"), depth(10020)]
        } else {
            [depth(10020), bbo(10020, "1")]
        };
        for raw in frames {
            s.ingest(g, &raw, 10030).unwrap();
        }
        assert_eq!(
            s.view()
                .fresh(10030)
                .unwrap()
                .quote
                .levels(Side::Sell)
                .unwrap()
                .len(),
            1
        );
        s.ingest(g, &bbo(10040, "2"), 10050).unwrap();
        assert_eq!(
            s.view()
                .fresh(10050)
                .unwrap()
                .quote
                .levels(Side::Sell)
                .unwrap()
                .len(),
            1,
            "an equal-time conflict must remain invalidated after the BBO returns"
        );
    }
}
#[test]
fn reconnect_rejects_late_old_frames_errors_and_disconnects() {
    let (mut s, g) = state();
    s.disconnect(g, "lost");
    assert!(s.view().fresh(10010).is_none());
    assert_eq!(s.ingest(g, &depth(10000), 10010).unwrap(), Update::Ignored);
    let next = s.begin();
    s.ingest(next, &depth(9000), 9010).unwrap();
    let revision = s.view().revision;
    assert_eq!(s.ingest(g, "{broken", 10010).unwrap(), Update::Ignored);
    s.disconnect(g, "late old disconnect");
    assert_eq!(s.view().revision, revision);
    assert!(s.view().fresh(9010).is_some());
}
#[test]
fn malformed_missing_size_null_side_and_future_source_clear_both_channels() {
    let mut null_side: Value = serde_json::from_str(&bbo(10020, "1")).unwrap();
    null_side["data"]["bbo"][0] = Value::Null;
    let mut missing_size: Value = serde_json::from_str(&bbo(10020, "1")).unwrap();
    missing_size["data"]["bbo"][0]
        .as_object_mut()
        .unwrap()
        .remove("sz");
    for raw in [
        null_side.to_string(),
        missing_size.to_string(),
        bbo(10261, "1"),
        "{invalid".into(),
        bbo(10020, "0"),
    ] {
        let (mut s, g) = state();
        assert!(s.ingest(g, &raw, 10010).is_err());
        assert!(s.view().selected(10010).is_none());
        assert_eq!(s.ingest(g, &depth(10030), 10040).unwrap(), Update::Ignored);
        let next = s.begin();
        s.ingest(next, &depth(10040), 10050).unwrap();
        assert!(s.view().fresh(10050).is_some());
    }
}
#[test]
fn channel_watermarks_are_independent_and_old_updates_do_not_notify() {
    let mut s = MarketState::new(MarketKey::new(Venue::Aster, "ETHUSDT").unwrap());
    let g = s.begin();
    let d = json!({"e":"depthUpdate","s":"ETHUSDT","T":10000,"u":10,"b":[["100","2"]],"a":[["101","2"]]}).to_string();
    let b = json!({"stream":"ethusdt@bookTicker","data":{"e":"bookTicker","s":"ETHUSDT","T":10001,"u":8,"b":"100","B":"1","a":"101","A":"2"}}).to_string();
    assert_eq!(s.ingest(g, &d, 10010).unwrap(), Update::Accepted);
    assert_eq!(s.ingest(g, &b, 10010).unwrap(), Update::Accepted);
    let revision = s.view().revision;
    assert_eq!(s.ingest(g, &b, 10020).unwrap(), Update::OutOfOrder);
    assert_eq!(s.ingest(g, &d, 10020).unwrap(), Update::OutOfOrder);
    assert_eq!(s.view().revision, revision);
    assert_eq!(s.view().selected(10020).unwrap().quote.received_ms(), 10010);
    let mut wrong: Value = serde_json::from_str(&b).unwrap();
    wrong["stream"] = json!("ethusdt@depth");
    assert!(parse_market(
        &MarketKey::new(Venue::Aster, "ETHUSDT").unwrap(),
        &wrong.to_string(),
        10020
    )
    .is_err());
}
#[test]
fn acknowledgements_cannot_recover_a_disconnected_market() {
    let mut s = MarketState::new(MarketKey::new(Venue::Hyperliquid, "ETH").unwrap());
    let g = s.begin();
    assert_eq!(
        s.ingest(g, r#"{"channel":"subscriptionResponse","data":{}}"#, 10000)
            .unwrap(),
        Update::Ignored
    );
    assert!(!s.view().connected);
}
