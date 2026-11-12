use perp_arb_core::{
    decimal,
    market::{vwap, Depth, Freshness, FreshnessPolicy, Level, Quote},
    pricing::{adverse_price_gap, round_step, spread_bps, Direction, Side},
    Decimal, Error,
};

fn d(value: &str) -> Decimal {
    decimal(value).unwrap()
}

#[test]
fn extended_rounding_preserves_digits_on_both_sides_of_a_tick() {
    use perp_arb_core::pricing::RoundingInput;
    for (input, up, down) in [
        ("0.02339233333333333333333333333", "0.023393", "0.023392"),
        ("-0.02339233333333333333333333333", "-0.023392", "-0.023393"),
        ("0.02339200000000000000000000001", "0.023393", "0.023392"),
        ("0.02339199999999999999999999999", "0.023392", "0.023391"),
    ] {
        let value = RoundingInput::parse(input).unwrap();
        assert_eq!(
            value.round(Some(d("0.000001")), Direction::Up).unwrap(),
            d(up)
        );
        assert_eq!(
            value.round(Some(d("0.000001")), Direction::Down).unwrap(),
            d(down)
        );
    }
    for invalid in [
        "NaN",
        "Infinity",
        "--0.1",
        "1.2.3",
        ".",
        "0.0000000000000000000000000000000000000001",
    ] {
        assert!(RoundingInput::parse(invalid).is_err(), "{invalid}");
    }
}
fn rows(values: &[(&str, &str)]) -> Vec<Level> {
    values
        .iter()
        .map(|(p, q)| Level {
            price: d(p),
            size: d(q),
        })
        .collect()
}
fn book(received: i64, source: Option<i64>) -> Quote {
    Quote::new(
        d("99"),
        d("100"),
        received,
        source,
        Depth::Visible(rows(&[("99", "0.5"), ("98", "1")])),
        Depth::Visible(rows(&[("100", "1"), ("101", "1"), ("103", "2")])),
    )
    .unwrap()
}

#[test]
fn rejects_invalid_or_silently_rounded_inputs() {
    for bad in [
        "NaN",
        "Infinity",
        "",
        "0.00000000000000000000000000001",
        "79228162514264337593543950336",
    ] {
        assert_eq!(decimal(bad), Err(Error::InvalidDecimal), "{bad}");
    }
    assert_eq!(
        decimal("0.0000000000000000000000000001").unwrap().scale(),
        28
    );
}

#[test]
fn prices_use_the_original_denominator_and_sign() {
    assert_eq!(spread_bps(d("2010"), d("2000")).unwrap(), d("50"));
    assert_eq!(spread_bps(d("1990"), d("2000")).unwrap(), d("-50"));
    assert_eq!(
        adverse_price_gap(d("100"), d("100.2"), Side::Sell).unwrap(),
        d("0.002")
    );
    assert_eq!(
        adverse_price_gap(d("100"), d("99.8"), Side::Buy).unwrap(),
        d("0.002")
    );
    assert_eq!(
        adverse_price_gap(d("100"), d("100.2"), Side::Buy).unwrap(),
        d("-0.002")
    );
    assert!(spread_bps(d("1"), d("0")).is_err());
    assert!(adverse_price_gap(d("-1"), d("1"), Side::Buy).is_err());
}

#[test]
fn tick_rounding_handles_signs_exact_ticks_and_disabled_steps() {
    for (input, up, down) in [
        ("1.001", "1.01", "1"),
        ("-1.001", "-1", "-1.01"),
        ("1.01", "1.01", "1.01"),
        ("0", "0", "0"),
    ] {
        assert_eq!(
            round_step(d(input), Some(d("0.01")), Direction::Up).unwrap(),
            d(up)
        );
        assert_eq!(
            round_step(d(input), Some(d("0.01")), Direction::Down).unwrap(),
            d(down)
        );
    }
    for step in [None, Some(d("0")), Some(d("-1"))] {
        assert_eq!(
            round_step(d("1.234"), step, Direction::Up).unwrap(),
            d("1.234")
        );
    }
    assert_eq!(
        round_step(
            d("1.0000000000000000000000000001"),
            Some(d("0.01")),
            Direction::Up
        )
        .unwrap(),
        d("1.01")
    );
}

#[test]
fn whole_clip_vwap_and_marginal_use_different_prices() {
    let quote = book(10_000, Some(10_000));
    assert_eq!(
        quote.executable(Side::Buy, Some(d("2"))).unwrap(),
        Some(d("100.5"))
    );
    assert_eq!(quote.marginal(Side::Buy, d("2")).unwrap(), Some(d("101")));
    assert_eq!(
        quote.executable(Side::Sell, Some(d("1"))).unwrap(),
        Some(d("98.5"))
    );
    assert_eq!(quote.marginal(Side::Sell, d("1")).unwrap(), Some(d("98")));
    assert_eq!(quote.executable(Side::Buy, Some(d("5"))).unwrap(), None);
    assert_eq!(quote.marginal(Side::Buy, d("5")).unwrap(), None);
    assert_eq!(quote.executable(Side::Buy, None).unwrap(), Some(d("100")));
}

#[test]
fn empty_visible_depth_is_not_an_unsized_venue() {
    let empty = Quote::new(
        d("99"),
        d("100"),
        0,
        None,
        Depth::Visible(vec![]),
        Depth::Visible(vec![]),
    )
    .unwrap();
    let unpublished =
        Quote::new(d("99"), d("100"), 0, None, Depth::Unsized, Depth::Unsized).unwrap();
    for side in [Side::Buy, Side::Sell] {
        assert_eq!(empty.executable(side, Some(d("1"))).unwrap(), None);
        assert_eq!(empty.marginal(side, d("1")).unwrap(), None);
        assert!(unpublished
            .executable(side, Some(d("1")))
            .unwrap()
            .is_some());
        assert!(unpublished.marginal(side, d("1")).unwrap().is_some());
    }
}

#[test]
fn bad_sizes_and_quantities_fail_even_when_unsized() {
    let quote = book(0, None);
    let unpublished =
        Quote::new(d("99"), d("100"), 0, None, Depth::Unsized, Depth::Unsized).unwrap();
    for quantity in ["0", "-1"] {
        assert!(quote.executable(Side::Buy, Some(d(quantity))).is_err());
        assert!(unpublished
            .executable(Side::Buy, Some(d(quantity)))
            .is_err());
        assert!(quote.marginal(Side::Sell, d(quantity)).is_err());
        assert!(vwap(&[], d(quantity)).is_err());
    }
    assert!(vwap(&rows(&[("100", "-1")]), d("1")).is_err());
}

#[test]
fn malformed_depth_is_rejected_at_ingestion() {
    for (bid, ask, bids, asks) in [
        ("101", "100", vec![], vec![]),
        ("99", "100", vec![("98", "1")], vec![]),
        ("99", "100", vec![], vec![("101", "1")]),
        ("99", "100", vec![], vec![("100", "1"), ("99", "1")]),
        ("99", "100", vec![("99", "1"), ("100", "1")], vec![]),
        ("99", "100", vec![], vec![("100", "1"), ("100", "1")]),
        ("99", "100", vec![], vec![("100", "0")]),
        ("0", "100", vec![], vec![]),
    ] {
        assert!(Quote::new(
            d(bid),
            d(ask),
            0,
            None,
            Depth::Visible(rows(&bids)),
            Depth::Visible(rows(&asks))
        )
        .is_err());
    }
}

#[test]
fn freshness_boundaries_match_production_defaults() {
    let policy = FreshnessPolicy::default();
    let quote = book(10_500, Some(10_000));
    assert_eq!(quote.freshness(11_000, true, policy), Freshness::Fresh);
    assert_eq!(quote.freshness(11_001, true, policy), Freshness::Stale);
    assert_eq!(quote.freshness(9_750, true, policy), Freshness::Fresh);
    assert_eq!(quote.freshness(9_749, true, policy), Freshness::Future);
    assert_eq!(
        book(10_501, Some(10_000)).freshness(10_501, true, policy),
        Freshness::TransportLag
    );
    assert_eq!(
        quote.freshness(10_500, false, policy),
        Freshness::Disconnected
    );
    assert_eq!(
        book(10_500, None).freshness(11_500, true, policy),
        Freshness::Fresh
    );
    assert_eq!(
        book(10_500, None).freshness(11_501, true, policy),
        Freshness::Stale
    );
}

#[test]
fn zero_source_timestamp_is_preserved_and_extreme_times_do_not_wrap() {
    assert_eq!(
        book(100, Some(0)).freshness(1001, true, FreshnessPolicy::default()),
        Freshness::Stale
    );
    assert_eq!(
        book(i64::MAX, Some(i64::MIN)).freshness(i64::MAX, true, FreshnessPolicy::default()),
        Freshness::Stale
    );
    assert_eq!(
        book(i64::MIN, Some(i64::MAX)).freshness(i64::MIN, true, FreshnessPolicy::default()),
        Freshness::Future
    );
    assert!(FreshnessPolicy::new(0, 500, 250).is_err());
    assert!(FreshnessPolicy::new(1000, -1, 250).is_err());
}

#[test]
fn overflow_and_underflow_report_errors_instead_of_panicking_or_zero_prices() {
    assert!(vwap(
        &[Level {
            price: Decimal::MAX,
            size: d("2")
        }],
        d("2")
    )
    .is_err());
    let tiny = d("0.0000000000000000000000000001");
    assert!(vwap(
        &[Level {
            price: tiny,
            size: tiny
        }],
        tiny
    )
    .is_err());
    assert!(spread_bps(Decimal::MAX, tiny).is_err());
}
