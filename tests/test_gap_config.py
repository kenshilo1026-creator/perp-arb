import os
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.maker_price_guard import configured_maker_taker_gap, MakerPriceGuard, MakerPriceRecheck
from hydra_basis.execution_engine.depth_sizing import SizeRules, depth_clip
from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, format_spot_perp_execution_summary


class GapConfigurationTests(unittest.IsolatedAsyncioTestCase):
    def test_default_and_override_are_percent_not_fraction(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(configured_maker_taker_gap(), D("0.0025"))
        with patch.dict(os.environ, {"MAKER_TAKER_MAX_GAP_PCT": "0.30"}):
            self.assertEqual(configured_maker_taker_gap(), D("0.003"))

    def test_invalid_configuration_fails_explicitly(self):
        for value in ("", "NaN", "Infinity", "0", "-1", "100", "oops"):
            with self.subTest(value=value), patch.dict(os.environ, {"MAKER_TAKER_MAX_GAP_PCT": value}):
                with self.assertRaises(ValueError):
                    configured_maker_taker_gap()

    async def test_020_percent_can_enter_but_025_percent_cannot(self):
        book = {"bid": "0.11477", "ask": "0.1148"}
        guard = MakerPriceGuard(fetch_books=AsyncMock(return_value=({"bid": ".115", "ask": ".116"}, book)),
            tick_size=AsyncMock(return_value=".0001"), maker_side="BUY", taker_side="SELL",
            max_gap=D(".0025"), wait_for_gap_seconds=1)
        self.assertEqual(await guard.prepare(".115"), "0.115")
        await guard.check(".115")
        book["bid"] = "0.1147125"  # exactly 0.25% adverse
        with self.assertRaises(MakerPriceRecheck):
            await guard.check(".115")

    def test_auto_sizing_and_final_warning_use_same_configured_limit(self):
        maker = {"bid": ".115", "ask": ".116"}
        hedge = {"bid": ".11477", "ask": ".1148", "bids": [(".11477", 1000)]}
        q, _, _ = depth_clip(remaining=D(1000), maker_book=maker, taker_book=hedge,
            maker_side="BUY", tick=D(".0001"), rules=SizeRules(D(1), D(1), D(10000), D(5), D("Infinity")),
            wait_for_gap=True, max_gap=D(".0025"))
        self.assertEqual(q, 1000)
        plan = build_spot_perp_plan(symbol="SYN", mode="close", short_venue="aster", quantity=D(1000),
            clip_usd=None, spot_book=hedge, perp_book=maker)
        with patch("scripts.run_spot_perp_arbitrage.MAKER_TAKER_MAX_GAP", D(".0025")):
            for taker, warning in ((".11477", False), (".1147", True)):
                lines = format_spot_perp_execution_summary(plan=plan, result={
                    "execution_price_summary": {"maker_avg_price": ".115", "taker_avg_price": taker}})
                warnings = [line for line in lines if "警告" in line]
                self.assertEqual(bool(warnings), warning)
                if warning:
                    self.assertIn("0.25%", warnings[0])
