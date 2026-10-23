import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.quote_safety import stamp_quote

with patch("hydra_basis.env.load_environment"):
    from scripts.place_order import ClosePositionPlan, execute_close_position_plan
    from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, execute_spot_perp_plan


def book(*, bid="99", ask="100"):
    return stamp_quote({
        "bid": float(bid), "ask": float(ask),
        "bids": [(bid, "10")], "asks": [(ask, "10")],
    })


class FilledAdapter:
    def __init__(self, *, final_side=None, initial_side=None):
        self.final_side = final_side
        self.initial_side = initial_side
        self.position_calls = 0
        self.limit_calls = []
        self.market_calls = []

    async def get_open_position(self, *, symbol, market_type):
        self.position_calls += 1
        if self.position_calls == 1:
            return ({"side": self.initial_side, "quantity": "1"}
                    if self.initial_side is not None else None)
        if self.final_side is None:
            return None
        return {"side": self.final_side, "quantity": "1"}

    async def place_limit_order(self, *, symbol, side, amount, clip_usd, price,
                                post_only=False, reduce_only=False):
        self.limit_calls.append({"post_only": post_only, "price": price})
        return {"ok": True, "terminal": True, "status": "FILLED",
                "filled_quantity": "1", "avg_price": price}

    async def place_market_order(self, *, symbol, side, amount, clip_usd,
                                 limit_price=None, reduce_only=False):
        self.market_calls.append({"limit_price": limit_price, "amount": amount})
        return {"ok": True, "terminal": True, "status": "FILLED",
                "filled_quantity": amount, "avg_price": limit_price}


class EntrypointSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_place_order_close_uses_post_only_and_bounded_hedge(self):
        maker = FilledAdapter(initial_side="SHORT")
        taker = FilledAdapter(initial_side="LONG")
        plan = ClosePositionPlan(
            symbol="BTC", quantity=D("1"), maker_venue="hyperliquid",
            taker_venue="aster", maker_price="99", clip_usd=100,
            side_by_venue={"hyperliquid": "BUY", "aster": "SELL"},
            spread_by_venue={},
        )
        with patch("scripts.place_order.fetch_close_orderbooks", new=AsyncMock(return_value={
            "hyperliquid": book(), "aster": book(),
        })):
            result = await execute_close_position_plan(
                plan=plan, adapters={"hyperliquid": maker, "aster": taker},
                symbol="BTC", venues=["hyperliquid", "aster"],
                max_hedge_slippage_bps=D("25"), max_quote_age_ms=D("1000"),
                max_maker_reprices=0,
            )
        self.assertTrue(result["ok"])
        self.assertTrue(maker.limit_calls[0]["post_only"])
        self.assertEqual(taker.market_calls[0]["limit_price"], "98.802")

    async def test_spot_perp_close_uses_same_bounded_executor_contract(self):
        spot = book(bid="99", ask="101")
        perp = book(bid="99.5", ask="100")
        plan = build_spot_perp_plan(
            symbol="BTC", mode="close", short_venue="hyperliquid", quantity=D("1"),
            clip_usd=None, spot_book=spot, perp_book=perp,
        )
        self.assertEqual(plan.maker_venue, "mexc_spot")
        maker = FilledAdapter(initial_side="LONG")
        taker = FilledAdapter(initial_side="SHORT")
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                   side_effect=[maker, taker]), patch(
                   "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                   new=AsyncMock(return_value=(spot, perp))):
            result = await execute_spot_perp_plan(
                plan=plan, leverage=1, max_hedge_slippage_bps=D("25"),
                max_quote_age_ms=D("1000"), max_maker_reprices=0,
            )
        self.assertTrue(result["ok"])
        self.assertTrue(maker.limit_calls[0]["post_only"])
        self.assertEqual(taker.market_calls[0]["limit_price"], "100.25")

    async def test_missing_limits_fail_before_order_submission(self):
        plan = ClosePositionPlan(
            symbol="BTC", quantity=D("1"), maker_venue="hyperliquid",
            taker_venue="aster", maker_price="99", clip_usd=100,
            side_by_venue={"hyperliquid": "BUY", "aster": "SELL"},
            spread_by_venue={},
        )
        maker = FilledAdapter(final_side="LONG")
        taker = FilledAdapter(final_side="SHORT")
        with self.assertRaisesRegex(RuntimeError, "requires explicit"):
            await execute_close_position_plan(
                plan=plan, adapters={"hyperliquid": maker, "aster": taker},
                symbol="BTC", venues=["hyperliquid", "aster"],
            )
        self.assertEqual(maker.limit_calls, [])
        self.assertEqual(taker.market_calls, [])


if __name__ == "__main__":
    unittest.main()
