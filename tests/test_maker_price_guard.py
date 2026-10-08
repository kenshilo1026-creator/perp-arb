import asyncio
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.maker_price_guard import (
    MakerPriceGuard, MakerPriceRecheck, adverse_price_gap, bounded_maker_price,
    wait_with_price_guard,
)
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, execute_spot_perp_plan


class PriceBandTests(unittest.TestCase):
    def price(self, desired, taker, side, bid="0.22", ask="0.23", tick="0.0001"):
        return bounded_maker_price(desired=D(desired), taker=D(taker), side=side,
                                   bid=D(bid), ask=D(ask), tick=D(tick))

    def test_close_buy_clamps_to_nearest_tick_inside_adverse_limit(self):
        price = self.price("0.2276", "0.2262", "BUY")
        self.assertEqual(price, D("0.2266"))
        self.assertLessEqual(adverse_price_gap(price, D("0.2262"), "BUY"), D("0.002"))
        self.assertGreater(adverse_price_gap(price + D("0.0001"), D("0.2262"), "BUY"), D("0.002"))

    def test_sell_clamps_up_only_when_adverse(self):
        self.assertEqual(self.price("0.2262", "0.2276", "SELL"), D("0.2272"))

    def test_favourable_spreads_are_unlimited_for_both_sides(self):
        self.assertEqual(self.price("1.1", "0.1", "SELL", "1", "1.2"), D("1.1"))
        self.assertEqual(self.price("0.1", "1.1", "BUY", "0.09", "0.11"), D("0.1"))

    def test_never_crosses_maker_book(self):
        self.assertEqual(self.price("0.2276", "0.23", "BUY", ask="0.2275"), D("0.2274"))
        self.assertEqual(self.price("0.2276", "0.22", "SELL", bid="0.2277"), D("0.2278"))

    def test_exact_boundary_is_allowed(self):
        self.assertEqual(adverse_price_gap(D("1"), D("0.998"), "BUY"), D("0.002"))
        self.assertEqual(adverse_price_gap(D("1"), D("1.002"), "SELL"), D("0.002"))

    def test_invalid_quotes_and_missing_tick_fail_closed(self):
        for value in ("0", "NaN", "Infinity", "-1"):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                self.price("0.2276", value, "BUY")
        with self.assertRaises(RuntimeError):
            self.price("0.2276", "0.2262", "BUY", tick="0")


class GuardExecutionTests(unittest.IsolatedAsyncioTestCase):
    def guard(self, fetch, side="BUY"):
        return MakerPriceGuard(fetch_books=fetch, tick_size=AsyncMock(return_value="0.0001"),
                               maker_side=side, taker_side="SELL" if side == "BUY" else "BUY",
                               check_seconds=0.001)

    async def test_favourable_quote_keeps_resting_order(self):
        fetched = asyncio.Event()
        async def books():
            fetched.set()
            return {"bid": 1, "ask": 1.2}, {"bid": 0.09, "ask": 0.1}
        async def filled():
            await fetched.wait()
            return {"ok": True}
        self.assertTrue((await wait_with_price_guard(filled(), self.guard(books, "SELL"), "1.1"))["ok"])

    async def test_quote_failure_cancels_fill_wait_task(self):
        finished = asyncio.Event()
        async def fill():
            try:
                await asyncio.Event().wait()
            finally:
                finished.set()
        guard = self.guard(AsyncMock(side_effect=RuntimeError("quote offline")))
        with self.assertRaisesRegex(MakerPriceRecheck, "quote unavailable"):
            await wait_with_price_guard(fill(), guard, "0.2276")
        self.assertTrue(finished.is_set())

    async def test_slow_quote_is_bounded_and_external_cancel_stops_both_tasks(self):
        async def offline():
            await asyncio.Event().wait()
        guard = self.guard(offline)
        with patch("hydra_basis.execution_engine.maker_price_guard.MAKER_QUOTE_TIMEOUT_SECONDS", 0.01):
            with self.assertRaises(MakerPriceRecheck):
                await guard.check("1")
        ended = []
        async def fill():
            try:
                await asyncio.Event().wait()
            finally:
                ended.append("fill")
        async def watch(price):
            try:
                await asyncio.Event().wait()
            finally:
                ended.append("watch")
        guard.watch = watch
        task = asyncio.create_task(wait_with_price_guard(fill(), guard, "1"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertCountEqual(ended, ["fill", "watch"])

    async def test_tiny_partial_fill_does_not_disable_price_guard(self):
        books = AsyncMock(side_effect=[
            ({"bid": 0.2276, "ask": 0.228}, {"bid": 0.2275, "ask": 0.2276}),
            ({"bid": 0.2276, "ask": 0.228}, {"bid": 0.2262, "ask": 0.2263}),
            ({"bid": 0.2276, "ask": 0.228}, {"bid": 0.2262, "ask": 0.2263}),
        ])
        maker = SimpleNamespace(
            place_limit_order=AsyncMock(return_value={"ok": True, "order_id": "1"}),
            wait_for_order_fill=AsyncMock(return_value={"ok": True, "filled_quantity": "1", "partial": True}),
            cancel_order=AsyncMock(return_value={"ok": True, "raw": {"status": "CANCELED", "executedQty": "2"}}),
        )
        taker = SimpleNamespace(place_market_order=AsyncMock(return_value={"ok": True}))
        result = await execute_single_clip_with_sides(
            symbol="TOKEN", clip_usd=10, quantity=D("10"), maker_venue="aster", taker_venue="mexc_spot",
            maker_side="BUY", taker_side="SELL", maker_adapter=maker, taker_adapter=taker,
            max_hedge_retries=0, state_machine=ExecutionStateMachine(), maker_price="0.2276",
            require_maker_fill_confirmation=True, min_hedge_notional_usd=5,
            maker_price_guard=self.guard(books),
        )
        maker.place_limit_order.assert_awaited_once()
        maker.cancel_order.assert_awaited_once()
        self.assertEqual(taker.place_market_order.call_args.kwargs["amount"], "2")
        self.assertEqual(result["executed_quantity"], "2")

    async def run_reprice(self, cancel_qty="0", cancel_error=False, unavailable=False):
        events = []
        prices = []
        amounts = []
        initial = True
        async def books():
            nonlocal initial
            events.append("quote")
            if initial:
                initial = False
                return {"bid": 0.2276, "ask": 0.228}, {"bid": 0.2275, "ask": 0.2276}
            if unavailable:
                raise RuntimeError("quote offline")
            return {"bid": 0.2276, "ask": 0.228}, {"bid": 0.2262, "ask": 0.2263}

        class Maker:
            async def place_limit_order(self, **kwargs):
                events.append("place")
                prices.append(kwargs["price"])
                assert kwargs["post_only"]
                return {"ok": True, "order_id": str(len(prices))}

            async def wait_for_order_fill(self, **kwargs):
                if len(prices) == 1:
                    await asyncio.Event().wait()
                return {"ok": True, "raw": {"status": "FILLED", "executedQty": "10"}}

            async def cancel_order(self, **kwargs):
                events.append("cancel")
                if cancel_error:
                    raise RuntimeError("cancel failed")
                return {"ok": True, "raw": {"status": "CANCELED", "executedQty": cancel_qty}}

        class Taker:
            async def place_market_order(self, **kwargs):
                events.append("hedge")
                amounts.append(kwargs["amount"])
                return {"ok": True}

        try:
            result = await execute_single_clip_with_sides(
                symbol="TOKEN", clip_usd=100, quantity=D("10"), maker_venue="aster",
                taker_venue="mexc_spot", maker_side="BUY", taker_side="SELL",
                maker_adapter=Maker(), taker_adapter=Taker(), max_hedge_retries=0,
                state_machine=ExecutionStateMachine(), maker_price="0.2276",
                require_maker_fill_confirmation=True, maker_fill_timeout_seconds=60,
                max_maker_reprice_attempts=-1, maker_price_guard=self.guard(books),
            )
        except RuntimeError:
            self.assertEqual(prices, ["0.2276"])
            self.assertIn("cancel", events)
            self.assertEqual(amounts, [])
            raise
        return result, events, prices, amounts

    async def test_resting_order_reprices_without_waiting_for_fill_timeout(self):
        result, events, prices, amounts = await asyncio.wait_for(self.run_reprice(), 2)
        self.assertTrue(result["ok"])
        self.assertEqual(prices, ["0.2276", "0.2266"])
        self.assertLess(events.index("cancel"), events.index("place", events.index("place") + 1))
        self.assertEqual(events[events.index("cancel") + 1], "quote")
        self.assertEqual(amounts, ["10"])

    async def test_fill_during_cancel_is_hedged_without_replacement(self):
        for qty in ("4", "10"):
            with self.subTest(qty=qty):
                result, events, prices, amounts = await self.run_reprice(cancel_qty=qty)
                self.assertEqual(prices, ["0.2276"])
                self.assertEqual(amounts, [qty])
                self.assertEqual(result["executed_quantity"], qty)

    async def test_cancel_failure_never_replaces_or_hedges(self):
        with patch("hydra_basis.execution_engine.executor.asyncio.sleep", new=AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, "cancel failed"):
                await self.run_reprice(cancel_error=True)

    async def test_unavailable_quotes_cancel_before_failed_refresh(self):
        with self.assertRaisesRegex(RuntimeError, "quote offline"):
            await self.run_reprice(unavailable=True)

    async def test_tick_size_uses_exchange_filter_and_post_only_is_sent(self):
        adapter = AsterExecutionAdapter(skip_margin_setup=True)
        adapter._exchange_info_by_symbol = {"TOKENUSDT": {"status": "TRADING", "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
            {"filterType": "LOT_SIZE", "stepSize": "1"},
        ]}}
        adapter._post_order = AsyncMock(return_value={"orderId": 1})
        self.assertEqual(await adapter.get_price_tick_size("TOKEN"), "0.0001")
        await adapter.place_limit_order(symbol="TOKEN", side="BUY", amount="10", clip_usd=3,
                                       price="0.2266", post_only=True, reduce_only=True)
        self.assertEqual(adapter._post_order.call_args.args[0]["timeInForce"], "GTX")
        self.assertEqual(adapter._post_order.call_args.args[0]["reduceOnly"], "true")

    async def test_spot_perp_open_allows_favourable_gap_and_close_caps_adverse_gap(self):
        for mode, spot, perp, expected in (
            ("open", {"bid": 0.0999, "ask": 0.1}, {"bid": 1.09, "ask": 1.1}, "1.1000"),
            ("close", {"bid": 0.2262, "ask": 0.2263}, {"bid": 0.2276, "ask": 0.228}, "0.2266"),
        ):
            with self.subTest(mode=mode):
                plan = build_spot_perp_plan(symbol="TOKEN", mode=mode, short_venue="aster",
                    quantity=D("100"), clip_usd=100, spot_book=spot, perp_book=perp)
                maker = SimpleNamespace(
                    get_price_tick_size=AsyncMock(return_value="0.0001"),
                    place_limit_order=AsyncMock(return_value={"ok": True, "raw": {
                        "status": "FILLED", "executedQty": "100"}}),
                )
                taker = SimpleNamespace(place_market_order=AsyncMock(return_value={"ok": True}))
                with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                           side_effect=lambda venue, **kw: maker if venue == "aster" else taker), \
                     patch("scripts.run_spot_perp_arbitrage.fetch_plan_books", new=AsyncMock(return_value=(spot, perp))), \
                     patch("scripts.run_spot_perp_arbitrage.fetch_required_live_position", new=AsyncMock(return_value={})), \
                     patch("scripts.run_spot_perp_arbitrage.record_successful_live_legs", return_value="test"):
                    result = await execute_spot_perp_plan(plan=plan, leverage=1)
                self.assertTrue(result["ok"])
                self.assertEqual(D(maker.place_limit_order.call_args.kwargs["price"]), D(expected))
                self.assertTrue(maker.place_limit_order.call_args.kwargs["post_only"])
                self.assertEqual(taker.place_market_order.call_args.kwargs["side"], plan.taker_side)


if __name__ == "__main__":
    unittest.main()
