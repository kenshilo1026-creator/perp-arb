use perp_arb_core::{
    decimal as d,
    instruments::{common_lot, exact_product, Instrument, PriceError, PriceRule, SizeError, Units},
    pricing::Direction,
    Decimal,
};
fn hl(sz: u32) -> Instrument {
    Instrument::new(
        PriceRule::Hyperliquid { sz_decimals: sz },
        Decimal::new(1, sz),
        Decimal::new(1, sz),
        Some(Decimal::TEN),
        None,
    )
    .unwrap()
}
fn aster() -> Instrument {
    Instrument::new(
        PriceRule::Tick(d("0.01").unwrap()),
        d("0.001").unwrap(),
        d("0.001").unwrap(),
        Some(d("5").unwrap()),
        Some(d("10").unwrap()),
    )
    .unwrap()
    .with_price_bounds(d("0.01").unwrap(), d("1000000").unwrap())
    .unwrap()
}

#[test]
fn hl_five_figures_decimal_caps_and_integer_exception() {
    for (value, sz, down, up) in [
        ("2345.678", 4, "2345.6", "2345.7"),
        ("0.00012345678", 0, "0.000123", "0.000124"),
        ("101234.56", 5, "101234", "101235"),
        ("123456", 6, "123456", "123456"),
        ("9.99999", 0, "9.9999", "10"),
        ("0.012345", 1, "0.01234", "0.01235"),
        ("99999.9", 0, "99999", "100000"),
        ("0.0000001", 0, "0", "0.000001"),
    ] {
        let i = hl(sz);
        assert_eq!(
            i.round_price(d(value).unwrap(), Direction::Down).unwrap(),
            d(down).unwrap()
        );
        assert_eq!(
            i.round_price(d(value).unwrap(), Direction::Up).unwrap(),
            d(up).unwrap()
        );
        assert!(!i
            .round_price(d(value).unwrap(), Direction::Up)
            .unwrap()
            .normalize()
            .to_string()
            .contains('E'));
    }
}
#[test]
fn exact_ticks_and_static_min_max_notional_boundaries() {
    let i = aster();
    assert_eq!(
        i.round_quantity(d("0.0109").unwrap()).unwrap(),
        d("0.010").unwrap()
    );
    assert_eq!(
        i.round_quantity(d("0.0001").unwrap()).unwrap(),
        Decimal::ZERO
    );
    for (q, p, error) in [
        ("0", "2000", Some(SizeError::BelowMinimumSize)),
        ("0.0105", "2000", Some(SizeError::InvalidLotSize)),
        (
            "0.002",
            "2499.999999999999999999",
            Some(SizeError::BelowMinimumNotional),
        ),
        ("0.002", "2500", None),
        ("10.001", "2500", Some(SizeError::AboveMaximumSize)),
    ] {
        assert_eq!(
            i.size_error(d(q).unwrap(), Some(d(p).unwrap())).unwrap(),
            error
        );
    }
    assert_eq!(
        i.price_error(d("0.009").unwrap()).unwrap(),
        Some(PriceError::BelowMinimum)
    );
    assert_eq!(
        i.price_error(d("1000000.01").unwrap()).unwrap(),
        Some(PriceError::AboveMaximum)
    );
    assert_eq!(
        i.price_error(d("1.001").unwrap()).unwrap(),
        Some(PriceError::InvalidTick)
    );
    assert_eq!(
        hl(0).price_error(Decimal::ZERO).unwrap(),
        Some(PriceError::NonPositive)
    );
}
#[test]
fn common_lot_is_lcm_not_maximum_and_handles_trailing_zeroes() {
    for (a, b, wanted) in [
        ("0.002", "0.003", "0.006"),
        ("0.0100", "0.001", "0.01"),
        ("0.25", "0.1", "0.5"),
        ("2", "3", "6"),
    ] {
        assert_eq!(
            common_lot(&[d(a).unwrap(), d(b).unwrap()]).unwrap(),
            Some(d(wanted).unwrap())
        );
    }
    assert_eq!(common_lot(&[]).unwrap(), None);
    assert!(common_lot(&[Decimal::ZERO]).is_err());
    assert!(common_lot(&[Decimal::MAX, Decimal::new(1, 28)]).is_err());
}
#[test]
fn unit_conversions_preserve_notional_and_refuse_rounded_quantities() {
    let u = Units::new(d("1000").unwrap()).unwrap();
    assert_eq!(u.to_base(d("2").unwrap()).unwrap(), d("2000").unwrap());
    assert_eq!(u.to_native(d("2000").unwrap()).unwrap(), d("2").unwrap());
    assert_eq!(
        u.price_per_base(d("0.0123").unwrap()).unwrap(),
        d("0.0000123").unwrap()
    );
    assert_eq!(
        exact_product(
            u.to_base(d("2").unwrap()).unwrap(),
            u.price_per_base(d("0.0123").unwrap()).unwrap()
        )
        .unwrap(),
        d("0.0246").unwrap()
    );
    assert!(Units::new(d("3").unwrap())
        .unwrap()
        .to_native(Decimal::ONE)
        .is_err());
    assert!(Units::new(Decimal::ZERO).is_err());
}
#[test]
fn rejects_invalid_metadata_offsets_and_arithmetic_underflow() {
    assert!(Instrument::new(
        PriceRule::Tick(Decimal::ZERO),
        Decimal::ONE,
        Decimal::ONE,
        None,
        None
    )
    .is_err());
    assert!(Instrument::new(
        PriceRule::Hyperliquid { sz_decimals: 7 },
        Decimal::new(1, 7),
        Decimal::new(1, 7),
        None,
        None
    )
    .is_err());
    assert!(Instrument::new(
        PriceRule::Tick(Decimal::ONE),
        d("0.002").unwrap(),
        d("0.003").unwrap(),
        None,
        None
    )
    .is_err());
    assert!(aster()
        .with_price_bounds(d("0.011").unwrap(), d("100").unwrap())
        .is_err());
    assert!(exact_product(Decimal::new(1, 20), Decimal::new(1, 20)).is_err());
    assert!(exact_product(Decimal::MAX, Decimal::TEN).is_err());
    assert!(aster().round_quantity(-Decimal::ONE).is_err());
}

#[test]
fn extended_products_keep_sub_decimal_precision_for_thresholds() {
    use perp_arb_core::instruments::ExactAmount;
    use std::cmp::Ordering;
    let price = d("1.000000000000000000000000001").unwrap();
    assert_eq!(
        hl(0).price_tolerance(price).unwrap().to_string(),
        "0.0001000000000000000000000000001"
    );
    let tiny = ExactAmount::product(Decimal::new(1, 20), Decimal::new(1, 20)).unwrap();
    assert_eq!(tiny.compare(Decimal::ZERO).unwrap(), Ordering::Greater);
    assert_eq!(tiny.compare(Decimal::new(1, 28)).unwrap(), Ordering::Less);
    assert_eq!(tiny.to_string(), format!("0.{}1", "0".repeat(39)));
    assert_eq!(
        hl(6).size_error(Decimal::new(1, 6), Some(price)).unwrap(),
        Some(SizeError::BelowMinimumNotional)
    );
    for (a, b, target, result) in [
        ("0.03", "2", "0.0600", Ordering::Equal),
        ("0.03", "2", "0.0601", Ordering::Less),
        ("0.03", "2", "0.0599", Ordering::Greater),
        ("1000", "1000", "999999", Ordering::Greater),
        ("0", "1000", "0", Ordering::Equal),
        ("0", "1000", "1", Ordering::Less),
    ] {
        assert_eq!(
            ExactAmount::product(d(a).unwrap(), d(b).unwrap())
                .unwrap()
                .compare(d(target).unwrap())
                .unwrap(),
            result
        );
    }
    assert!(ExactAmount::product(Decimal::MAX, Decimal::MAX).is_err());
}
