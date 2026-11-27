use super::*;
use perp_arb_core::{
    decimal,
    market::{Depth, Level, Quote},
};
use serde_json::json;
#[test]
fn leverage_policy_requires_explicit_open_and_preserves_legacy_recovery() {
    for venue in ["aster", "hyperliquid", "lighter"] {
        let mut p = plan(venue);
        assert_eq!(p.leverage_for_submit().unwrap(), None); // Close.
        p.maker.reduce_only = false;
        p.hedge.reduce_only = false;
        p.validate().unwrap(); // Legacy remains readable for recovery/audit.
        assert!(p
            .leverage_for_submit()
            .unwrap_err()
            .contains("prepare a new plan"));
        p.hedge_leverage = Some(5);
        assert_eq!(p.leverage_for_submit().unwrap(), Some(5));
        let roundtrip: Plan = serde_json::from_value(serde_json::to_value(&p).unwrap()).unwrap();
        assert_eq!(roundtrip.hedge_leverage, Some(5));
        p.maker.reduce_only = true;
        p.hedge.reduce_only = true;
        assert_eq!(p.leverage_for_submit().unwrap(), None);
        for invalid in [0, 126, u32::MAX] {
            p.hedge_leverage = Some(invalid);
            assert!(p.validate().is_err());
        }
    }
}
#[test]
fn only_account_revision_errors_allow_a_bounded_audit_retry() {
    for e in ["WrongSession", "Coordination(WrongSession)"] {
        assert!(runtime::account_revision_race(e));
    }
    for e in ["HTTP 429", "Timeout", "Protocol", "Coordination(Conflict)"] {
        assert!(!runtime::account_revision_race(e));
    }
}
pub(crate) fn plan(venue: &str) -> Plan {
    serde_json::from_value(json!({
        "run_id":"request-1","extension_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "maker":{"request_id":"request-1","account":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","symbol":"ETH","side":"BUY","quantity":"2","limit_price":"100","reduce_only":true,"max_notional":"200"},
        "instrument":{"underlying":"ETH","instrument_type":"perpetual_future","settlement_asset":"USDC","funding_interval_s":3600},
        "hedge":{"venue":venue,"network":"mainnet","account":if venue=="lighter"{"100"}else{"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"symbol":if venue=="aster"{"ETHUSDT"}else{"ETH"},"side":"SELL","quantity":"2","limit_price":"99","tif":"ioc","reduce_only":true,"max_notional_usd":"200","wait_seconds":20},
        "max_entry_gap":"0.0025","maker_wait_seconds":60,"recovery_seconds":15
    })).unwrap()
}
pub(crate) fn resolved(venue: &str) -> Resolved {
    use perp_arb_market_data::catalog::{self, SymbolMappings};
    let maps = SymbolMappings::default();
    let metadata = if venue == "aster" {
        catalog::resolve_aster(&json!({"symbols":[{"symbol":"ETHUSDT","baseAsset":"ETH","quoteAsset":"USDT","marginAsset":"USDT","status":"TRADING","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.1","minPrice":"0","maxPrice":"100000"},{"filterType":"LOT_SIZE","minQty":"0.1","maxQty":"10000","stepSize":"0.1"},{"filterType":"MIN_NOTIONAL","notional":"10"}]}]}),"ETHUSDT",&maps).unwrap()
    } else if venue == "lighter" {
        perp_arb_market_data::lighter::parse_metadata(&json!({"code":200,"order_book_details":[{"symbol":"ETH","market_id":0,"market_type":"perp","status":"active","min_base_amount":"0.1","min_quote_amount":"10","supported_size_decimals":1,"supported_price_decimals":2}]}),"ETH").unwrap()
    } else {
        catalog::resolve_hyperliquid(
            &json!({"universe":[{"name":"ETH","szDecimals":1}]}),
            "ETH",
            &maps,
        )
        .unwrap()
    };
    Resolved {
        order: perp_arb_private_events::orders::Order {
            venue: venue.into(),
            network: perp_arb_private_events::orders::Network::Mainnet,
            account: plan(venue).hedge.account,
            symbol: plan(venue).hedge.symbol,
            side: "SELL".into(),
            quantity: "2".into(),
            limit_price: "99".into(),
            tif: perp_arb_private_events::orders::TimeInForce::Ioc,
            reduce_only: true,
            client_id: if venue == "aster" {
                "pa-fixture"
            } else if venue == "lighter" {
                "123"
            } else {
                "0x11111111111111111111111111111111"
            }
            .into(),
            asset: metadata.asset_id.map(|a| a as u32),
        },
        metadata,
    }
}
#[test]
fn plan_rejects_wrong_side_size_reduce_only_network_and_duplicate_identity() {
    for venue in ["aster", "hyperliquid", "lighter"] {
        plan(venue).validate().unwrap();
        assert_eq!(
            plan(venue).maker_fill_mode,
            super::plan::FillMode::RestVerified
        );
    }
    for i in 0..7 {
        let mut p = plan("aster");
        match i {
            0 => p.hedge.side = "BUY".into(),
            1 => p.hedge.quantity = "3".into(),
            2 => p.hedge.reduce_only = false,
            3 => p.maker.request_id = "another-run".into(),
            4 => p.max_entry_gap = "NaN".into(),
            5 => p.hedge.network = perp_arb_private_events::orders::Network::Testnet,
            _ => p.maker.max_notional = "1".into(),
        }
        assert!(p.validate().is_err());
    }
}
fn quote(bid: &str, size: &str) -> Quote {
    Quote::new(
        decimal(bid).unwrap(),
        decimal("101").unwrap(),
        1000,
        Some(1000),
        Depth::Visible(vec![Level {
            price: decimal(bid).unwrap(),
            size: decimal(size).unwrap(),
        }]),
        Depth::Visible(vec![Level {
            price: decimal("101").unwrap(),
            size: decimal(size).unwrap(),
        }]),
    )
    .unwrap()
}
#[test]
fn aster_market_still_guards_maker_gap_but_reference_is_not_a_limit() {
    use perp_arb_private_events::orders::TimeInForce;
    let mut p = plan("aster");
    p.hedge.tif = TimeInForce::Market;
    p.hedge.limit_price = "100".into();
    p.validate().unwrap();
    assert!(p.entry_ok(&quote("99.9", "2")).unwrap());
    assert!(!p.entry_ok(&quote("99.7", "2")).unwrap());
    assert!(!p.entry_ok(&quote("100", "1")).unwrap());
    let mut r = resolved("aster");
    r.order.tif = TimeInForce::Market;
    assert!(r.hedge_limits().is_err()); // No guessed market lot rule.
    r.metadata.market = Some(
        perp_arb_core::instruments::Instrument::new(
            perp_arb_core::instruments::PriceRule::Tick(decimal("0.1").unwrap()),
            decimal("0.5").unwrap(),
            decimal("0.5").unwrap(),
            None,
            None,
        )
        .unwrap(),
    );
    assert_eq!(p.execution_plan(&r).unwrap().step, decimal("0.5").unwrap());
    let mut other = plan("hyperliquid");
    other.hedge.tif = TimeInForce::Market;
    assert!(other.validate().is_err());
}
#[test]
fn exact_gap_boundary_depth_and_ioc_limit_are_checked_before_maker() {
    let p = plan("aster");
    assert!(p.entry_ok(&quote("99.75", "2")).unwrap());
    assert!(!p.entry_ok(&quote("99.749999999", "2")).unwrap());
    assert!(!p.entry_ok(&quote("100", "1")).unwrap());
    assert!(p.entry_ok(&quote("100.5", "2")).unwrap());
    let mut p = p;
    p.hedge.limit_price = "100".into();
    assert!(!p.entry_ok(&quote("99.9", "2")).unwrap());
}
#[test]
fn hedge_minimum_includes_notional_so_small_fills_accumulate_without_trapped_reservation() {
    for venue in ["aster", "hyperliquid"] {
        let p = plan(venue);
        let e = p.execution_plan(&resolved(venue)).unwrap();
        assert_eq!(e.minimum, decimal("0.2").unwrap());
        assert_eq!(e.step, decimal("0.1").unwrap());
    }
}
#[test]
fn baseline_requires_review_identity_and_sufficient_close_positions() {
    let p = plan("aster");
    let mut b:Baseline=serde_json::from_value(json!({"reviewed":false,"maker_account":p.maker.account,"hedge_account":p.hedge.account,"hedge_venue":"aster","maker":{"wallets":{"USDC":"100"},"positions":{"ETH":"-2"}},"hedge":{"wallets":{"USDT":"100"},"positions":{"ETHUSDT":"2"}}})).unwrap();
    assert!(b.validate(&p).is_err());
    b.reviewed = true;
    b.validate(&p).unwrap();
    b.hedge.positions.insert("ETHUSDT".into(), "1".into());
    assert!(b.validate(&p).is_err());
    b.hedge.positions.insert("ETHUSDT".into(), "2".into());
    b.hedge_account = p.maker.account.clone();
    assert!(b.validate(&p).is_err());
}
