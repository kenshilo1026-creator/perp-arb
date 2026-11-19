import asyncio
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock

from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.maker_price_guard import MakerPriceGuard, MakerPriceRecheck, create_maker_guard
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine


class MakerQuoteFollowTests(unittest.IsolatedAsyncioTestCase):
    async def test_newer_bbo_drives_entry_and_reprice_without_waiting_for_full_depth(self):
        book = {"bid": 100, "ask": 101, "fast_quote": {"bid": "100.1", "ask": 101, "age_ms": 0}}
        guard = self.guard(book, {"bid": 100, "ask": 100.1})
        price = await guard.prepare("100")
        self.assertEqual(price, "100.1")
        book["fast_quote"]["bid"] = "100.2"
        with self.assertRaisesRegex(MakerPriceRecheck, "best quote changed"):
            await guard.check(price)
        book["fast_quote"]["age_ms"] = 1001
        with self.assertRaisesRegex(MakerPriceRecheck, "stale maker"):
            await guard.check(price)

    def guard(self, maker, hedge, side="BUY"):
        return MakerPriceGuard(fetch_books=AsyncMock(side_effect=lambda: (maker, hedge)),
            tick_size=AsyncMock(return_value=".1"), maker_side=side,
            taker_side="SELL" if side == "BUY" else "BUY", quantity=D(10),
            max_gap=D(".0025"),
            follow_maker_quote=True, wait_for_gap_seconds=1, check_seconds=.001)

    async def test_unsafe_chase_keeps_safe_order_then_reprices_when_hedge_recovers(self):
        for side in ("BUY", "SELL"):
            with self.subTest(side=side):
                maker = {"bid": "100", "ask": "101"}
                hedge = {"bid": "100", "ask": "100.1"}
                guard = self.guard(maker, hedge, side)
                price = await guard.prepare("100")
                maker.update(bid="100.3", ask="101") if side == "BUY" else maker.update(bid="99", ask="100")
                if side == "SELL":
                    hedge["ask"] = "100.3"
                await guard.check(price)
                hedge["bid" if side == "BUY" else "ask"] = "100.1"
                with self.assertRaisesRegex(MakerPriceRecheck, "best quote changed"):
                    await guard.check(price)

    async def test_chase_at_exact_limit_is_held_but_original_at_limit_is_cancelled(self):
        for side in ("BUY", "SELL"):
            with self.subTest(side=side):
                maker = {"bid": "100", "ask": "101"} if side == "BUY" else {"bid": "99", "ask": "100"}
                hedge = {"bid": "99.75", "ask": "100.25"}
                guard = self.guard(maker, hedge, side)
                await guard.check("99.9" if side == "BUY" else "100.1")
                with self.assertRaisesRegex(MakerPriceRecheck, "adverse"):
                    await guard.check("100")

    async def test_kept_order_still_cancels_when_original_becomes_unsafe_or_depth_stale(self):
        for side in ("BUY", "SELL"):
            for stale in (False, True):
                with self.subTest(side=side, stale=stale):
                    maker = {"bid": "100.3", "ask": "101"} if side == "BUY" else {"bid": "99", "ask": "100"}
                    hedge = {"bid": "100", "ask": "100.3"}
                    guard = self.guard(maker, hedge, side)
                    price = "100" if side == "BUY" else "101"
                    await guard.check(price)
                    if stale:
                        hedge["age_ms"] = 1001
                    else:
                        hedge["bid" if side == "BUY" else "ask"] = "99" if side == "BUY" else "102"
                    with self.assertRaisesRegex(MakerPriceRecheck, "stale" if stale else "adverse"):
                        await guard.check(price)

    async def test_chase_uses_full_depth_vwap_and_conservative_fast_quote(self):
        for side in ("BUY", "SELL"):
            for fast in (False, True):
                with self.subTest(side=side, fast=fast):
                    maker = {"bid": "100.2", "ask": "101"} if side == "BUY" else {"bid": "99", "ask": "100"}
                    hedge = {"bid": "100.1", "ask": "100.1"}
                    levels = [["100.1", "1"], ["99.9" if side == "BUY" else "100.3", "9"]]
                    hedge["bids" if side == "BUY" else "asks"] = [["100.1", "10"]] if fast else levels
                    if fast:
                        hedge["fast_quote"] = {"bid": "99.9", "ask": "100.3"}
                    guard = self.guard(maker, hedge, side)
                    await guard.check("100" if side == "BUY" else "101")

    async def test_same_side_quote_unchanged_keeps_order_despite_opposite_side_or_size_changes(self):
        for side in ("BUY", "SELL"):
            maker, hedge = {"bid": "100", "ask": "101"}, {"bid": "100", "ask": "100.1"}
            guard = self.guard(maker, hedge, side)
            price = await guard.prepare("999")
            self.assertEqual(D(price), D(100 if side == "BUY" else 101))
            maker["ask" if side == "BUY" else "bid"] = "102" if side == "BUY" else "99"
            maker["bid_size"] = 9999
            await guard.check(price)
            maker["bid" if side == "BUY" else "ask"] = "100.1" if side == "BUY" else "100.9"
            with self.assertRaisesRegex(MakerPriceRecheck, "best quote changed"):
                await guard.check(price)

    async def test_gap_protection_still_cancels_unchanged_bid(self):
        maker, hedge = {"bid": 100, "ask": 101}, {"bid": 100, "ask": 100.1}
        guard = self.guard(maker, hedge)
        price = await guard.prepare("100")
        hedge["bid"] = 99
        with self.assertRaisesRegex(MakerPriceRecheck, "adverse"):
            await guard.check(price)

    async def test_maker_ws_update_wakes_watcher_even_when_taker_is_silent(self):
        changed, waiting, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
        book = {"bid": 100, "ask": 101}
        async def maker_update():
            waiting.set()
            await changed.wait()
        async def taker_update():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        maker = SimpleNamespace(get_price_tick_size=AsyncMock(return_value=".1"),
            hedge_quote=AsyncMock(side_effect=lambda: book), wait_for_hedge_quote=maker_update)
        taker = SimpleNamespace(hedge_quote=AsyncMock(return_value={"bid": 100, "ask": 100.1}),
                                wait_for_hedge_quote=taker_update)
        guard = await create_maker_guard(maker_adapter=maker, taker_adapter=taker,
            symbol="SYN", fetch_books=None, maker_side="BUY", taker_side="SELL", quantity=D(10),
            follow_maker_quote=True, wait_for_gap_seconds=1)
        price = await guard.prepare("100")
        watch = asyncio.create_task(guard.watch(price))
        try:
            await asyncio.wait_for(waiting.wait(), 1)
            book["bid"] = 100.1
            changed.set()
            with self.assertRaisesRegex(MakerPriceRecheck, "best quote changed"):
                await asyncio.wait_for(watch, 1)
            self.assertTrue(cancelled.is_set())
        finally:
            watch.cancel()
            await asyncio.gather(watch, return_exceptions=True)

    async def run_execution(self, side, *, changed=False, cancel_fill="0", blocked_chase=False):
        maker_book, hedge_book = {"bid": "100", "ask": "101"}, {"bid": "100", "ask": "100.1"}
        guard = self.guard(maker_book, hedge_book, side)
        orders, waits, hedges = [], [], []
        async def place(**kwargs):
            orders.append(kwargs)
            return {"ok": True, "order_id": str(len(orders))}
        async def fill(**kwargs):
            waits.append(kwargs["order_result"]["order_id"])
            if len(waits) == 1:
                if blocked_chase:
                    if side == "BUY":
                        maker_book["bid"] = "100.3"
                    else:
                        maker_book.update(bid="99", ask="100")
                        hedge_book["ask"] = "100.3"
                if changed:
                    maker_book["bid" if side == "BUY" else "ask"] = "100.1" if side == "BUY" else "100.9"
                    await asyncio.Event().wait()
                raise RuntimeError("maker fill timeout")
            return {"ok": True, "terminal": True, "filled_quantity": "10"}
        async def hedge(**kwargs):
            hedges.append(kwargs["amount"])
            return {"ok": True}
        cancel = AsyncMock(return_value={"ok": True, "terminal": True, "filled_quantity": cancel_fill})
        result = await asyncio.wait_for(execute_single_clip_with_sides(
            symbol="SYN", clip_usd=1000, quantity=D(10), maker_venue="aster", taker_venue="binance_spot",
            maker_side=side, taker_side="SELL" if side == "BUY" else "BUY",
            maker_adapter=SimpleNamespace(place_limit_order=place, wait_for_order_fill=fill, cancel_order=cancel),
            taker_adapter=SimpleNamespace(place_market_order=hedge), max_hedge_retries=0,
            state_machine=ExecutionStateMachine(), maker_price="100" if side == "BUY" else "101",
            maker_price_guard=guard, require_maker_fill_confirmation=True, max_maker_reprice_attempts=-1,
        ), 2)
        return result, orders, waits, hedges, cancel

    async def test_fill_timeout_keeps_same_order_id_without_cancel_or_resubmit(self):
        for side in ("BUY", "SELL"):
            result, orders, waits, hedges, cancel = await self.run_execution(side)
            self.assertTrue(result["ok"])
            self.assertEqual(len(orders), 1)
            self.assertEqual(waits, ["1", "1"])
            self.assertEqual(hedges, ["10"])
            cancel.assert_not_awaited()

    async def test_changed_quote_cancels_once_and_replaces_at_latest_bid_or_ask(self):
        for side in ("BUY", "SELL"):
            result, orders, waits, hedges, cancel = await self.run_execution(side, changed=True)
            self.assertTrue(result["ok"])
            self.assertEqual([D(o["price"]) for o in orders], [D(100), D("100.1")] if side == "BUY" else [D(101), D("100.9")])
            cancel.assert_awaited_once()
            self.assertEqual(hedges, ["10"])

    async def test_blocked_chase_retains_order_across_timeout_and_hedges_fill(self):
        for side in ("BUY", "SELL"):
            with self.subTest(side=side):
                result, orders, waits, hedges, cancel = await self.run_execution(side, blocked_chase=True)
                self.assertTrue(result["ok"])
                self.assertEqual(len(orders), 1)
                self.assertEqual(waits, ["1", "1"])
                self.assertEqual(hedges, ["10"])
                cancel.assert_not_awaited()

    async def test_fill_during_quote_change_cancel_hedges_without_replacement(self):
        result, orders, waits, hedges, cancel = await self.run_execution("BUY", changed=True, cancel_fill="4")
        self.assertTrue(result["ok"])
        self.assertEqual(len(orders), 1)
        self.assertEqual(hedges, ["4"])
        cancel.assert_awaited_once()
