use perp_arb_core::{decimal as d, instruments::SizeError};
use perp_arb_market_data::catalog::{resolve_aster, resolve_hyperliquid, SymbolMappings};
use serde_json::{json, Value};
fn maps() -> SymbolMappings {
    SymbolMappings::parse(&json!({"global":{"1000PEPE":"PEPE","KPEPE":"PEPE"}})).unwrap()
}
fn aster(base: &str, quote: &str) -> Value {
    json!({"symbol":format!("{base}{quote}"),"baseAsset":base,"quoteAsset":quote,"marginAsset":quote,"status":"TRADING","contractType":"PERPETUAL",
        "pricePrecision":8,"quantityPrecision":8,"filters":[
            {"filterType":"PRICE_FILTER","tickSize":"0.01","minPrice":"0.01","maxPrice":"1000000"},
            {"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"100"},
            {"filterType":"MARKET_LOT_SIZE","stepSize":"0.002","minQty":"0.002","maxQty":"10"},
            {"filterType":"MIN_NOTIONAL","notional":"5"}]})
}
fn hl() -> Value {
    json!({"collateralToken":0,"universe":[{"name":"OLD","szDecimals":2,"isDelisted":true},{"name":"ETH","szDecimals":4},{"name":"kPEPE","szDecimals":0}]})
}
#[test]
fn aster_prefers_trading_suffix_and_exact_native_does_not_switch_settlement() {
    let mut usdt = aster("ETH", "USDT");
    usdt["status"] = json!("SETTLING");
    let info = json!({"symbols":[usdt,aster("ETH","USDC")]});
    assert_eq!(
        resolve_aster(&info, "eth", &maps()).unwrap().key.symbol(),
        "ETHUSDC"
    );
    assert!(resolve_aster(&info, "ETHUSDT", &maps()).is_err());
}
#[test]
fn market_and_limit_have_independent_lots_and_caps() {
    let i = resolve_aster(&json!({"symbols":[aster("ETH","USDT")]}), "ETH", &maps()).unwrap();
    assert_eq!(i.limit.lot(), d("0.001").unwrap());
    let market = i.market.unwrap();
    assert_eq!(market.lot(), d("0.002").unwrap());
    assert_eq!(
        market.size_error(d("11").unwrap(), None).unwrap(),
        Some(SizeError::AboveMaximumSize)
    );
    assert_eq!(i.limit.size_error(d("11").unwrap(), None).unwrap(), None);
}
#[test]
fn preserves_hl_native_spelling_and_original_asset_index() {
    let i = resolve_hyperliquid(&hl(), "KPEPE", &maps()).unwrap();
    assert_eq!(i.key.symbol(), "kPEPE");
    assert_eq!(i.asset_id, Some(2));
    assert_eq!(i.canonical_base, "PEPE");
    assert!(i.units().is_none());
    assert!(resolve_hyperliquid(&hl(), "OLD", &maps()).is_err());
    assert!(resolve_hyperliquid(&hl(), "io:ETH", &maps()).is_err());
}
#[test]
fn aliases_need_explicit_units_before_common_quantity_and_native_markets_work() {
    let info = json!({"symbols":[aster("ETH","USDT"),aster("1000PEPE","USDT")]});
    let a = resolve_aster(&info, "ETH", &maps()).unwrap();
    let h = resolve_hyperliquid(&hl(), "ETH", &maps()).unwrap();
    assert_eq!(a.common_limit_base_lot(&h).unwrap(), d("0.001").unwrap());
    let a = resolve_aster(&info, "1000PEPE", &maps()).unwrap();
    let h = resolve_hyperliquid(&hl(), "kPEPE", &maps()).unwrap();
    assert!(a.common_limit_base_lot(&h).is_err());
    let a = a.with_verified_units(d("1000").unwrap()).unwrap();
    let h = h.with_verified_units(d("1000").unwrap()).unwrap();
    assert_eq!(a.common_limit_base_lot(&h).unwrap(), d("1000").unwrap());
    assert!(a.common_limit_base_lot(&a).is_err());
}
#[test]
fn no_precision_fallback_for_missing_or_invalid_filters() {
    let original = aster("ETH", "USDT");
    for field in ["tickSize", "minPrice", "maxPrice"] {
        let mut raw = original.clone();
        raw["filters"][0].as_object_mut().unwrap().remove(field);
        assert!(resolve_aster(&json!({"symbols":[raw]}), "ETH", &maps()).is_err());
    }
    for field in ["stepSize", "minQty", "maxQty"] {
        let mut raw = original.clone();
        raw["filters"][1].as_object_mut().unwrap().remove(field);
        assert!(resolve_aster(&json!({"symbols":[raw]}), "ETH", &maps()).is_err());
    }
    let mut raw = original.clone();
    raw["filters"].as_array_mut().unwrap().remove(2);
    assert!(resolve_aster(&json!({"symbols":[raw]}), "ETH", &maps())
        .unwrap()
        .market
        .is_none());
    for value in [json!(0.001), json!("0"), json!("NaN")] {
        let mut raw = original.clone();
        raw["filters"][1]["stepSize"] = value;
        assert!(resolve_aster(&json!({"symbols":[raw]}), "ETH", &maps()).is_err());
    }
}
#[test]
fn rejects_duplicate_delisted_inconsistent_and_unsupported_metadata() {
    let original = aster("ETH", "USDT");
    assert!(resolve_aster(
        &json!({"symbols":[original.clone(),original.clone()]}),
        "ETH",
        &maps()
    )
    .is_err());
    for (key, value) in [
        ("status", "BREAK"),
        ("contractType", "CURRENT_QUARTER"),
        ("marginAsset", "BTC"),
        ("baseAsset", "BTC"),
    ] {
        let mut raw = original.clone();
        raw[key] = json!(value);
        assert!(resolve_aster(&json!({"symbols":[raw]}), "ETH", &maps()).is_err());
    }
    let mut raw = original.clone();
    raw["filters"]
        .as_array_mut()
        .unwrap()
        .push(original["filters"][0].clone());
    assert!(resolve_aster(&json!({"symbols":[raw]}), "ETH", &maps()).is_err());
    for value in [json!(7), json!(-1), json!(1.5), json!("4")] {
        let mut meta = hl();
        meta["universe"][1]["szDecimals"] = value;
        assert!(resolve_hliquid(&meta).is_err());
    }
    let mut meta = hl();
    meta["collateralToken"] = json!(1);
    assert!(resolve_hliquid(&meta).is_err());
    let mut meta = hl();
    meta["universe"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"eth","szDecimals":4}));
    assert!(resolve_hliquid(&meta).is_err());
}
fn resolve_hliquid(meta: &Value) -> Result<(), String> {
    resolve_hyperliquid(meta, "ETH", &maps()).map(|_| ())
}
#[test]
fn mappings_obey_venue_precedence_and_reject_case_collisions() {
    let map=SymbolMappings::parse(&json!({"global":{"X":"GLOBAL"},"venues":{"Aster":{"x":"LOCAL"}},"venue_symbols":{"aster":{"LOCAL":"X"}}})).unwrap();
    assert_eq!(map.canonical("x", "ASTER"), "LOCAL");
    assert_eq!(map.canonical("x", "hyperliquid"), "GLOBAL");
    assert_eq!(map.native("local", "aster"), "X");
    assert!(SymbolMappings::parse(&json!({"global":{"x":"A","X":"B"}})).is_err());
    assert!(SymbolMappings::parse(&json!({"venues":{"aster":[]}})).is_err());
}

#[test]
fn non_ascii_native_symbol_is_preserved_and_only_url_path_is_encoded() {
    use perp_arb_market_data::{shared::parse_market, Venue};
    let i = resolve_aster(
        &json!({"symbols":[aster("哈基米","USDT")]}),
        "哈基米",
        &maps(),
    )
    .unwrap();
    assert_eq!(i.key.symbol(), "哈基米USDT");
    assert_eq!(i.canonical_base, "哈基米");
    assert!(i
        .key
        .endpoint()
        .contains("%E5%93%88%E5%9F%BA%E7%B1%B3usdt@depth10@100ms"));
    assert!(Venue::Aster
        .endpoint("哈基米USDT")
        .unwrap()
        .contains("%E5%93%88"));
    let raw=json!({"stream":"哈基米usdt@bookTicker","data":{"e":"bookTicker","s":"哈基米USDT","T":10000,
        "b":"1.1","B":"2","a":"1.2","A":"2"}}).to_string();
    assert!(parse_market(&i.key, &raw, 10010).unwrap().is_some());
    for bad in ["ETH/USDT", "ETH&streams=BTC", "ETH%2FUSDT", "ETH\nUSDT"] {
        assert!(Venue::Aster.endpoint(bad).is_err());
    }
}

#[test]
fn explicit_new_quote_assets_keep_their_own_denomination() {
    for quote in ["USD1", "U"] {
        let info = json!({"symbols":[aster("ETH",quote)]});
        let i = resolve_aster(&info, &format!("ETH{quote}"), &maps()).unwrap();
        assert_eq!(i.quote, quote);
        assert_eq!(i.settlement, quote);
        assert!(
            resolve_aster(&info, "ETH", &maps()).is_err(),
            "do not silently change base-symbol settlement preference"
        );
    }
}
