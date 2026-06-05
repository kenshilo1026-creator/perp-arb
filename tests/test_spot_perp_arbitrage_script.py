import unittest
from decimal import Decimal

from scripts.run_spot_perp_arbitrage import (
    build_spot_perp_plan,
    compute_base_quantity,
    maker_limit_price,
    normalize_mode,
    spot_perp_sides,
)


class SpotPerpArbitrageScriptTests(unittest.TestCase):
    def test_open_mode_buys_spot_and_sells_perp(self) -> None:
        self.assertEqual(
            spot_perp_sides(mode="open"),
            {"mexc_spot": "BUY", "perp": "SELL"},
        )

    def test_close_mode_sells_spot_and_buys_perp(self) -> None:
        self.assertEqual(
            spot_perp_sides(mode="close"),
            {"mexc_spot": "SELL", "perp": "BUY"},
        )

    def test_spread_lower_side_becomes_taker(self) -> None:
        plan = build_spot_perp_plan(
            symbol="ETH",
            mode="open",
            short_venue="aster",
            quantity=Decimal("0.1"),
            clip_usd=300.0,
            spot_book={"bid": 2999.0, "ask": 3001.0, "ts_ms": 1},
            perp_book={"bid": 2990.0, "ask": 3010.0, "ts_ms": 1},
        )

        self.assertEqual(plan.taker_venue, "mexc_spot")
        self.assertEqual(plan.maker_venue, "aster")
        self.assertEqual(plan.maker_side, "SELL")
        self.assertEqual(plan.taker_side, "BUY")
        self.assertEqual(plan.maker_price, "3010")

    def test_close_mode_reverses_sides_but_keeps_spread_taker_rule(self) -> None:
        plan = build_spot_perp_plan(
            symbol="ETH",
            mode="close",
            short_venue="hyperliquid",
            quantity=Decimal("0.1"),
            clip_usd=300.0,
            spot_book={"bid": 2990.0, "ask": 3010.0, "ts_ms": 1},
            perp_book={"bid": 2999.0, "ask": 3001.0, "ts_ms": 1},
        )

        self.assertEqual(plan.taker_venue, "hyperliquid")
        self.assertEqual(plan.maker_venue, "mexc_spot")
        self.assertEqual(plan.maker_side, "SELL")
        self.assertEqual(plan.taker_side, "BUY")
        self.assertEqual(plan.maker_price, "3010")

    def test_limit_price_uses_passive_side_of_book(self) -> None:
        book = {"bid": 100.0, "ask": 100.5, "ts_ms": 1}

        self.assertEqual(maker_limit_price(book, "BUY"), "100")
        self.assertEqual(maker_limit_price(book, "SELL"), "100.5")

    def test_compute_base_quantity_from_clip_usd_uses_mid(self) -> None:
        quantity = compute_base_quantity(
            quantity=None,
            clip_usd=1000.0,
            taker_book={"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        self.assertEqual(quantity, Decimal("10"))

    def test_normalize_mode_rejects_invalid_value(self) -> None:
        with self.assertRaises(RuntimeError):
            normalize_mode("hold")


if __name__ == "__main__":
    unittest.main()
