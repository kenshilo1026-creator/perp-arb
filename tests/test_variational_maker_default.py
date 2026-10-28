import unittest
from decimal import Decimal
from unittest.mock import Mock

from hydra_basis.execution_engine.models import ExecutionRequest, ExecutionSignal
from hydra_basis.execution_engine.preview import build_execution_preview
from hydra_basis.execution_engine.priority import resolve_execution_legs
from hydra_basis.risk_management.models import PositionLeg
from scripts.place_order import build_close_position_plan
from scripts.run_spot_perp_arbitrage import build_spot_perp_plan


class VariationalMakerDefaultTests(unittest.TestCase):
    def test_default_overrides_priorities_and_all_spread_relationships_in_both_directions(self):
        for short, long in (("aster", "variational"), ("variational", "aster")):
            for aster_spread, var_spread in ((.01, .0001), (.0001, .01), (.0005, .0001), (.001, .001)):
                with self.subTest(short=short, aster_spread=aster_spread, var_spread=var_spread):
                    self.assertEqual(resolve_execution_legs(
                        short_venue=short, long_venue=long,
                        priorities={"aster": 100, "variational": 0},
                        spreads={"aster": aster_spread, "variational": var_spread},
                    ), ("variational", "aster"))

    def test_place_order_open_preview_uses_variational_maker(self):
        for short, long in (("aster", "variational"), ("variational", "aster")):
            with self.subTest(short=short):
                preview = build_execution_preview(
                    request=ExecutionRequest("BTC", 1000, 100),
                    signal=ExecutionSignal("BTC", short, long, .8, 1, ""),
                    priorities={"aster": 100, "variational": 0},
                    adapters={"aster": Mock(get_orderbook=Mock(return_value={"bid": 99, "ask": 101})),
                              "variational": Mock(get_orderbook=Mock(return_value={"bid": 100, "ask": 100.01}))},
                )
                self.assertEqual((preview.maker_venue, preview.taker_venue), ("variational", "aster"))
                self.assertLess(preview.maker_spread_pct, preview.taker_spread_pct)

    def test_place_order_close_plan_keeps_variational_maker_and_correct_close_side(self):
        for var_side, aster_side, expected in (("SHORT", "LONG", "BUY"), ("LONG", "SHORT", "SELL")):
            with self.subTest(var_side=var_side):
                plan = build_close_position_plan(
                    legs=[PositionLeg("s", "v", "variational", "BTC", "perp", var_side, "10"),
                          PositionLeg("s", "a", "aster", "BTC", "perp", aster_side, "10")],
                    clip_size=Decimal("3"), priorities={"aster": 100, "variational": 0},
                    orderbooks={"aster": {"bid": 99, "ask": 101}, "variational": {"bid": 100, "ask": 100.01}},
                )
                self.assertEqual((plan.maker_venue, plan.taker_venue), ("variational", "aster"))
                self.assertEqual(plan.side_by_venue["variational"], expected)
                self.assertEqual(plan.quantity, Decimal("3"))

    def test_spot_open_close_and_replanning_keep_variational_maker_when_spreads_change(self):
        for mode, maker_side, taker_side in (("open", "SELL", "BUY"), ("close", "BUY", "SELL")):
            for spot_book, perp_book in (({"bid": 99, "ask": 101}, {"bid": 100, "ask": 100.01}),
                                         ({"bid": 100, "ask": 100.01}, {"bid": 99, "ask": 101})):
                with self.subTest(mode=mode, spot_book=spot_book):
                    plan = build_spot_perp_plan(symbol="BTC", mode=mode, short_venue="VARIATIONAL",
                                               quantity=Decimal("3"), clip_usd=None,
                                               spot_book=spot_book, perp_book=perp_book)
                    self.assertEqual((plan.maker_venue, plan.taker_venue), ("variational", "mexc_spot"))
                    self.assertEqual((plan.maker_side, plan.taker_side), (maker_side, taker_side))
                    self.assertEqual(plan.maker_orderbook, perp_book)
                    self.assertEqual(plan.taker_orderbook, spot_book)
                    self.assertEqual(Decimal(plan.maker_price), Decimal(str(perp_book["ask" if maker_side == "SELL" else "bid"])))
