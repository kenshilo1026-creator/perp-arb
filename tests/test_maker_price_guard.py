import asyncio
import unittest
import tempfile
from pathlib import Path
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.depth_sizing import SizeRules
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.maker_price_guard import (
    MakerPriceGuard, MakerPriceRecheck, adverse_price_gap, bounded_maker_price,
    wait_with_price_guard,
)
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, execute_spot_perp_plan


class ImprovedMakerPriceTests(unittest.IsolatedAsyncioTestCase):
    async def test_temporary_quote_timeout_does_not_end_entire_gap_wait(self):
        fetch = AsyncMock(side_effect=[TimeoutError("quote request timed out"),
            ({"bid": 100, "ask": 101}, {"bid": 100, "ask": 101})])
        guard = MakerPriceGuard(max_gap=D("0.002"), fetch_books=fetch, tick_size=AsyncMock(return_value="0.1"),
            maker_side="BUY", taker_side="SELL", wait_for_update=AsyncMock(), wait_for_gap_seconds=1)
        self.assertEqual(await guard.prepare("100"), "100")
        self.assertEqual(fetch.await_count, 2)

    async def test_wait_mode_requires_strictly_below_threshold_in_both_directions(self):
        from hydra_basis.execution_engine.maker_price_guard import MakerGapTimeout
        for side, maker, hedge in (
            ("BUY", {"bid": 100, "ask": 101}, {"bid": 99.8, "ask": 99.9}),
            ("SELL", {"bid": 99, "ask": 100}, {"bid": 100.1, "ask": 100.2}),
        ):
            guard = MakerPriceGuard(max_gap=D("0.002"), fetch_books=AsyncMock(return_value=(maker, hedge)),
                tick_size=AsyncMock(return_value="0.1"), maker_side=side,
                taker_side="SELL" if side == "BUY" else "BUY", wait_for_gap_seconds=0.02)
            with self.assertRaises(MakerGapTimeout):
                await guard.prepare("100")

    async def test_wait_mode_rechecks_full_depth_and_then_resumes(self):
        maker = {"bid": "0.1154", "ask": "0.1158"}
        shallow = {"bid": "0.11526", "ask": "0.1153", "bids": [("0.11526", 10), ("0.115", 990)]}
        deep = {**shallow, "bids": [("0.11526", 1000)]}
        fetch = AsyncMock(side_effect=[(maker, shallow), (maker, deep)])
        guard = MakerPriceGuard(max_gap=D("0.002"), fetch_books=fetch, tick_size=AsyncMock(return_value="0.0001"),
            maker_side="BUY", taker_side="SELL", quantity=D(1000),
            wait_for_update=AsyncMock(), wait_for_gap_seconds=1)
        self.assertEqual(await guard.prepare("0.1154"), "0.1154")
        self.assertEqual(fetch.await_count, 2)

    async def test_live_syn_snapshot_best_bid_passes_but_full_clip_vwap_exceeds_limit(self):
        # Public WS observation on 2026-11-13: top bid cannot fill 1,000 SYN.
        book = {"bid": "0.11808", "ask": "0.1182", "age_ms": 0,
                "bids": [("0.11808", "356.3"), ("0.11806", "43.9"),
                         ("0.11804", "328.5"), ("0.11802", "359.3")]}
        guard = MakerPriceGuard(max_gap=D("0.002"), fetch_books=AsyncMock(return_value=({}, book)),
                               tick_size=AsyncMock(return_value="0.0001"),
                               maker_side="BUY", taker_side="SELL", quantity=D("1000"))
        self.assertLess(adverse_price_gap(D("0.1183"), D(book["bid"]), "BUY"), D("0.002"))
        self.assertEqual(guard.taker_price(book), D("0.118049704"))
        with self.assertRaisesRegex(MakerPriceRecheck, "0.2116%"):
            await guard.check("0.1183")

    async def test_one_tick_improvement_respects_passivity_and_adverse_gap(self):
        cases = [
            ("BUY", "0.1187", "0.1191", "0.1187", "0.1188"),
            ("SELL", "0.1187", "0.1191", "0.1191", "0.1190"),
            # One-tick spread: stay passive rather than cross the book.
            ("BUY", "0.1187", "0.1188", "0.1187", "0.1187"),
            ("SELL", "0.1187", "0.1188", "0.1188", "0.1188"),
            # The cross-venue 0.2% band takes precedence over improving priority.
            ("BUY", "0.1187", "0.1191", "0.1185", "0.1187"),
            ("SELL", "0.1183", "0.1187", "0.1189", "0.1187"),
        ]
        for side, bid, ask, taker_price, expected in cases:
            with self.subTest(side=side, bid=bid, ask=ask, taker=taker_price):
                guard = MakerPriceGuard(max_gap=D("0.002"),
                    fetch_books=AsyncMock(return_value=({"bid": bid, "ask": ask},
                        {"bid": taker_price, "ask": taker_price})),
                    tick_size=AsyncMock(return_value="0.0001"), maker_side=side,
                    taker_side="SELL" if side == "BUY" else "BUY", improve_maker_by_one_tick=True)
                price = D(await guard.prepare(bid if side == "BUY" else ask))
                self.assertEqual(price, D(expected))
                self.assertLess(adverse_price_gap(price, D(taker_price), side), D("0.002"))
                self.assertTrue(price < D(ask) if side == "BUY" else price > D(bid))

    async def test_replacements_use_current_book_and_exchange_tick_without_accumulation(self):
        books = AsyncMock(return_value=({"bid": "0.11870", "ask": "0.11910"},
                                       {"bid": "0.11870", "ask": "0.11910"}))
        guard = MakerPriceGuard(max_gap=D("0.002"), fetch_books=books, tick_size=AsyncMock(return_value="0.00001"),
                               maker_side="BUY", taker_side="SELL", improve_maker_by_one_tick=True)
        first = await guard.prepare("0.11870")
        self.assertEqual(D(first), D("0.11871"))
        self.assertEqual(await guard.prepare(first), first)
        books.return_value = ({"bid": "0.11880", "ask": "0.11910"},
                              {"bid": "0.11880", "ask": "0.11910"})
        self.assertEqual(D(await guard.prepare(first)), D("0.11881"))


class PriceBandTests(unittest.TestCase):
    def price(self, desired, taker, side, bid="0.22", ask="0.23", tick="0.0001"):
        return bounded_maker_price(max_gap=D("0.002"), desired=D(desired), taker=D(taker), side=side,
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
    def setUp(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.enterContext(patch("hydra_basis.execution_engine.execution_trace.TRACE_PATH",
                                Path(directory) / "test_timing.jsonl"))

    def guard(self, fetch, side="BUY"):
        return MakerPriceGuard(max_gap=D("0.002"), fetch_books=fetch, tick_size=AsyncMock(return_value="0.0001"),
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

    async def run_reprice(self, cancel_qty="0", cancel_error=False, unavailable=False,
                          wait_seconds=None, recover_after_cancel=False):
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
            if recover_after_cancel and "cancel" in events:
                return {"bid": 0.2276, "ask": 0.228}, {"bid": 0.2275, "ask": 0.2276}
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

        guard = self.guard(books)
        guard.wait_for_gap_seconds = wait_seconds
        try:
            result = await execute_single_clip_with_sides(
                symbol="TOKEN", clip_usd=100, quantity=D("10"), maker_venue="aster",
                taker_venue="mexc_spot", maker_side="BUY", taker_side="SELL",
                maker_adapter=Maker(), taker_adapter=Taker(), max_hedge_retries=0,
                state_machine=ExecutionStateMachine(), maker_price="0.2276",
                require_maker_fill_confirmation=True, maker_fill_timeout_seconds=60,
                max_maker_reprice_attempts=-1, maker_price_guard=guard,
            )
        except RuntimeError:
            self.assertEqual(prices, ["0.2276"])
            self.assertIn("cancel", events)
            self.assertEqual(amounts, [])
            raise
        return result, events, prices, amounts

    async def test_wait_mode_cancels_once_then_exits_without_replacement(self):
        from hydra_basis.execution_engine.maker_price_guard import MakerGapTimeout
        with self.assertRaises(MakerGapTimeout):
            await self.run_reprice(wait_seconds=0.02)

    async def test_wait_mode_resumes_only_after_recovery(self):
        result, events, prices, amounts = await self.run_reprice(wait_seconds=1, recover_after_cancel=True)
        self.assertTrue(result["ok"])
        self.assertEqual(prices, ["0.2276", "0.2276"])
        self.assertEqual(events.count("cancel"), 1)
        self.assertEqual(amounts, ["10"])

    async def test_wait_mode_never_blocks_hedge_for_fill_during_cancel(self):
        result, events, prices, amounts = await self.run_reprice(cancel_qty="4", wait_seconds=0.02)
        self.assertTrue(result["ok"])
        self.assertEqual(prices, ["0.2276"])
        self.assertEqual(amounts, ["4"])

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
                timing = result["execution_timing"]
                self.assertEqual(timing["maker_confirmation_source"], "reconciled")
                self.assertIsNone(timing["maker_observed_to_hedge_call_ms"])
                self.assertGreaterEqual(timing["maker_resolved_to_hedge_call_ms"], 0)
                stages = [e["stage"] for e in result["execution_trace"]["events"]]
                self.assertIn("maker.cancel_with_retries.start", stages)
                self.assertLess(stages.index("maker.cancel_with_retries.end"), stages.index("maker.fill_resolved"))

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

    async def test_spot_perp_open_allows_favourable_gap_and_close_waits_on_adverse_gap(self):
        for mode, spot, perp, expected in (
            ("open", {"bid": 0.0999, "ask": 0.1}, {"bid": 1.09, "ask": 1.1}, "1.1"),
            ("close", {"bid": 0.1187, "ask": 0.1189}, {"bid": 0.1187, "ask": 0.1191}, "0.1187"),
            ("close", {"bid": 0.2262, "ask": 0.2263}, {"bid": 0.2276, "ask": 0.228}, None),
        ):
            with self.subTest(mode=mode):
                spot.update(bid_size=1000, ask_size=1000)
                plan = build_spot_perp_plan(symbol="TOKEN", mode=mode, short_venue="aster",
                    quantity=D("100"), clip_usd=100, spot_book=spot, perp_book=perp,
                    maker_venue_override="aster")
                maker = SimpleNamespace(
                    get_price_tick_size=AsyncMock(return_value="0.0001"),
                    place_limit_order=AsyncMock(return_value={"ok": True, "raw": {
                        "status": "FILLED", "executedQty": "100"}}),
                )
                maker.get_open_position = AsyncMock(side_effect=[
                    None if mode == "open" else {"side": "SHORT", "quantity": "100"},
                    {"side": "SHORT", "quantity": "100"} if mode == "open" else None])
                taker = SimpleNamespace(place_market_order=AsyncMock(return_value={
                    "ok": True, "terminal": True, "filled_quantity": "100"}),
                    get_open_position=AsyncMock(side_effect=[
                        None if mode == "open" else {"side": "LONG", "quantity": "100"},
                        {"side": "LONG", "quantity": "100"} if mode == "open" else None]))
                with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                           side_effect=lambda venue, **kw: maker if venue == "aster" else taker), \
                     patch("scripts.run_spot_perp_arbitrage.fetch_plan_books", new=AsyncMock(return_value=(spot, perp))), \
                     patch("scripts.run_spot_perp_arbitrage.fetch_required_live_position", new=AsyncMock(return_value={})), \
                     patch("scripts.run_spot_perp_arbitrage.record_successful_live_legs", return_value="test"), \
                     patch("scripts.run_spot_perp_arbitrage.load_spot_perp_size_rules", new=AsyncMock(
                         return_value=SizeRules(D(1), D(1), D(10000), D(5), D("Infinity")))), \
                     patch("scripts.run_spot_perp_arbitrage.GAP_WAIT_TIMEOUT_SECONDS", 0.02):
                    if expected is None:
                        from hydra_basis.execution_engine.maker_price_guard import MakerGapTimeout
                        with self.assertRaises(MakerGapTimeout):
                            await execute_spot_perp_plan(plan=plan, leverage=1)
                        maker.place_limit_order.assert_not_awaited()
                        taker.place_market_order.assert_not_awaited()
                        continue
                    result = await execute_spot_perp_plan(plan=plan, leverage=1)
                self.assertTrue(result["ok"])
                self.assertEqual(D(maker.place_limit_order.call_args.kwargs["price"]), D(expected))
                self.assertTrue(maker.place_limit_order.call_args.kwargs["post_only"])
                self.assertEqual(taker.place_market_order.call_args.kwargs["side"], plan.taker_side)


if __name__ == "__main__":
    unittest.main()
