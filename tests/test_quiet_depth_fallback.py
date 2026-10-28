import asyncio
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock

from hydra_basis.execution_engine.mexc_depth_stream import (
    MexcSpotDepthStream, DepthQuoteUnavailable, decode_rest_depth,
)
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides, maker_fill_error_is_repriceable
from hydra_basis.execution_engine.maker_price_guard import MakerPriceRecheck, MakerPriceGuard
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine


def payload(version=1):
    return {"lastUpdateId": version, "bids": [["99", "10"]], "asks": [["100", "10"]]}


class QuietDepthTests(unittest.IsolatedAsyncioTestCase):
    def stream(self, fetch=None, max_age=1):
        stream = MexcSpotDepthStream(connect=AsyncMock(), symbol="SYNUSDT",
            fetch_snapshot=fetch or AsyncMock(return_value=payload()), max_age_seconds=max_age)
        stream.healthy = True
        stream.publish(decode_rest_depth(payload()))
        stream._received -= 2
        return stream

    async def test_unchanged_book_is_confirmed_via_rest_without_reconnect(self):
        stream = self.stream()
        result = await stream.fresh_snapshot(wait_seconds=0)
        self.assertEqual(result["version"], 1)
        self.assertEqual(result["quote_source"], "rest")
        self.assertEqual(result["ts_ms"], 0)
        self.assertLess(result["age_ms"], 1000)
        stream.connect.assert_not_awaited()

    async def test_late_rest_response_cannot_overwrite_newer_ws_version(self):
        stream = self.stream()
        async def get():
            newer = decode_rest_depth(payload(3))
            newer["quote_source"] = "ws"
            stream.publish(newer)
            return payload(2)
        stream.fetch_snapshot = get
        with self.assertRaisesRegex(DepthQuoteUnavailable, "REST_version_behind_WS"):
            await stream._refresh_rest()
        self.assertEqual(stream.snapshot()["version"], 3)

    async def test_malformed_or_conflicting_rest_data_does_not_refresh_cache(self):
        for data in ({"lastUpdateId": 1, "bids": [], "asks": [["100", "10"]]},
                     {"lastUpdateId": 1, "bids": [["99", "10"]], "asks": [["101", "10"]]},
                     {"bids": [["99", "10"]], "asks": [["100", "10"]]}):
            with self.subTest(data=data):
                stream = self.stream(AsyncMock(return_value=data))
                before = stream._received
                with self.assertRaises((ValueError, KeyError)):
                    await stream._refresh_rest()
                self.assertEqual(before, stream._received)
                with self.assertRaises(DepthQuoteUnavailable):
                    stream.snapshot()

    async def test_rest_timeout_does_not_extend_quote_lifetime(self):
        cancelled = asyncio.Event()
        async def get():
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        stream = self.stream(get, max_age=0.02)
        with self.assertRaises(TimeoutError):
            await stream._refresh_rest()
        self.assertTrue(cancelled.is_set())
        with self.assertRaises(DepthQuoteUnavailable):
            stream.snapshot()

    async def test_new_rest_depth_still_triggers_adverse_spread_guard(self):
        stream = self.stream()
        async def quote():
            return stream.snapshot()
        guard = MakerPriceGuard(fetch_books=None, tick_size=None, maker_side="SELL", taker_side="BUY",
                                quantity=D(10), fetch_taker_book=quote)
        worse = payload(2)
        worse["asks"] = [["101", "10"]]
        stream.fetch_snapshot = AsyncMock(return_value=worse)
        await stream._refresh_rest()
        with self.assertRaises(MakerPriceRecheck):
            await guard.check("100")

    async def test_ws_recovery_during_slow_rest_does_not_reconnect(self):
        stream = self.stream()
        async def get():
            stream.publish(decode_rest_depth(payload(3)))
            return payload(2)
        stream.fetch_snapshot = get
        result = await stream.fresh_snapshot(wait_seconds=0)
        self.assertEqual(result["version"], 3)
        stream.connect.assert_not_awaited()

    async def test_quiet_ws_start_and_background_refresh_then_cleanup(self):
        ws = SimpleNamespace(send_json=AsyncMock(), receive=AsyncMock(side_effect=self.block), close=AsyncMock())
        fetched_twice = asyncio.Event()
        calls = 0
        async def fetch():
            nonlocal calls
            calls += 1
            if calls >= 2:
                fetched_twice.set()
            return payload()
        stream = MexcSpotDepthStream(connect=AsyncMock(return_value=ws), symbol="SYNUSDT",
                                    fetch_snapshot=fetch, max_age_seconds=0.1)
        try:
            await stream.fresh_snapshot(timeout_seconds=1)
            await asyncio.wait_for(fetched_twice.wait(), 1)
            self.assertEqual(stream.snapshot()["quote_source"], "rest")
            stream.connect.assert_awaited_once()
        finally:
            await stream.close()
        self.assertFalse(stream.healthy)
        self.assertEqual(stream._tasks, [])
        ws.close.assert_awaited_once()

    async def block(self):
        await asyncio.Future()


class PreSubmissionFailureTests(unittest.IsolatedAsyncioTestCase):
    async def execute(self, maker, taker, guard):
        return await execute_single_clip_with_sides(
            symbol="SYN", quantity=D(10), clip_usd=1000, maker_venue="aster", taker_venue="mexc_spot",
            maker_side="SELL", taker_side="BUY", maker_adapter=maker, taker_adapter=taker,
            max_hedge_retries=0, state_machine=ExecutionStateMachine(), maker_price="100",
            require_maker_fill_confirmation=True, max_maker_reprice_attempts=-1, maker_price_guard=guard)

    def guard(self):
        return SimpleNamespace(maker_side="SELL", taker_side="BUY", prepare=AsyncMock(return_value="100"),
            taker_book={}, taker_price=lambda _: D(100),
            watch=AsyncMock(side_effect=MakerPriceRecheck("price changed")))

    async def test_first_attempt_preparation_failure_never_submits_or_cancels(self):
        for error in (DepthQuoteUnavailable("market-data recovery timed out after 2.5s"), TimeoutError()):
            maker = SimpleNamespace(place_limit_order=AsyncMock(), cancel_order=AsyncMock())
            taker = SimpleNamespace(place_market_order=AsyncMock())
            guard = self.guard()
            guard.prepare.side_effect = error
            with self.assertRaises(type(error)) as raised:
                await self.execute(maker, taker, guard)
            self.assertIs(raised.exception, error)
            maker.place_limit_order.assert_not_awaited()
            maker.cancel_order.assert_not_awaited()
            taker.place_market_order.assert_not_awaited()

    async def test_third_attempt_preflight_failure_does_not_cancel_phantom_order(self):
        maker = SimpleNamespace(
            place_limit_order=AsyncMock(side_effect=[{"ok": True, "order_id": "1"}, {"ok": True, "order_id": "2"}]),
            wait_for_order_fill=AsyncMock(side_effect=self.block),
            cancel_order=AsyncMock(return_value={"ok": True, "raw": {"status": "CANCELED", "executedQty": "0"}}))
        taker = SimpleNamespace(place_market_order=AsyncMock())
        guard = self.guard()
        error = DepthQuoteUnavailable("market-data recovery timed out after 2.5s")
        guard.prepare.side_effect = ["100", "100", error]
        with self.assertRaises(DepthQuoteUnavailable) as raised:
            await self.execute(maker, taker, guard)
        self.assertIs(raised.exception, error)
        self.assertEqual(maker.place_limit_order.await_count, 2)
        self.assertEqual(maker.cancel_order.await_count, 2)
        self.assertEqual([call.kwargs["order_result"]["order_id"] for call in maker.cancel_order.await_args_list], ["1", "2"])
        taker.place_market_order.assert_not_awaited()

    async def test_timeout_after_submission_still_reconciles_real_order(self):
        maker = SimpleNamespace(place_limit_order=AsyncMock(return_value={"ok": True, "order_id": "real"}),
            wait_for_order_fill=AsyncMock(side_effect=RuntimeError("fill timeout")),
            cancel_order=AsyncMock(return_value={"ok": True, "raw": {"status": "CANCELED", "executedQty": "4"}}))
        taker = SimpleNamespace(place_market_order=AsyncMock(return_value={"ok": True}))
        guard = self.guard()
        guard.watch = self.block
        result = await self.execute(maker, taker, guard)
        self.assertEqual(result["executed_quantity"], "4")
        maker.cancel_order.assert_awaited_once()
        self.assertEqual(taker.place_market_order.call_args.kwargs["amount"], "4")

    def test_data_timeout_is_not_an_order_fill_timeout(self):
        self.assertFalse(maker_fill_error_is_repriceable(DepthQuoteUnavailable("timed out")))
        self.assertTrue(maker_fill_error_is_repriceable(RuntimeError("fill timeout")))

    async def block(self, *args, **kwargs):
        await asyncio.Future()
