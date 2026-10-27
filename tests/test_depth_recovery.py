"""Recover only before dispatch; stale quotes cannot authorize live makers."""
import asyncio
import time
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.mexc_depth_stream import MexcSpotDepthStream, DepthQuoteUnavailable
from hydra_basis.execution_engine.aster_depth_stream import AsterDepthStream
from hydra_basis.execution_engine.maker_price_guard import create_maker_guard, MakerPriceRecheck


def book(version=1, source=None, ask="100"):
    return {"version": version, "source_ms": int(time.time()*1000) if source is None else source,
            "bid": D(99), "ask": D(ask), "bids": [(D(99), D(10))], "asks": [(D(ask), D(10))]}


class DepthRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def stream(self, cls=MexcSpotDepthStream):
        stream = cls(connect=AsyncMock(), symbol="TOKENUSDT")
        stream.healthy = True
        stream.publish(book())
        return stream

    def adapter(self, stream):
        async def quote():
            return stream.snapshot()
        return SimpleNamespace(prepare_hedge_quotes=AsyncMock(), hedge_quote=quote,
            refresh_hedge_quote=stream.fresh_snapshot, wait_for_hedge_quote=stream.wait_for_update,
            get_price_tick_size=AsyncMock(return_value="0.01"))

    async def guard(self, maker, taker):
        return await create_maker_guard(maker_adapter=maker, taker_adapter=taker, symbol="TOKEN",
            fetch_books=AsyncMock(side_effect=AssertionError("unexpected REST")),
            maker_side="SELL", taker_side="BUY", quantity=D(10))

    def test_diagnostics_distinguish_missing_disconnected_and_stale(self):
        stream = self.stream()
        stream._received -= 2
        with self.assertRaisesRegex(DepthQuoteUnavailable, "stale.*venue=mexc_spot.*symbol=TOKENUSDT.*age_ms=.*version=1"):
            stream.snapshot()
        stream.healthy = False
        with self.assertRaisesRegex(DepthQuoteUnavailable, "disconnected"):
            stream.snapshot()
        stream.healthy = True
        stream._book = None
        with self.assertRaisesRegex(DepthQuoteUnavailable, "missing"):
            stream.snapshot()

    def test_same_version_full_snapshot_with_fresh_timestamp_refreshes_unchanged_book(self):
        stream = self.stream()
        stream._book["source_ms"] -= 2000
        stream._received -= 2
        stream.publish(book())
        self.assertLess(stream.snapshot()["age_ms"], 1000)
        self.assertEqual(stream.snapshot()["version"], 1)

    def test_replays_older_versions_and_stale_source_do_not_refresh(self):
        for version, source in ((1, 0), (0, int(time.time()*1000)), (1, int(time.time()*1000)-2000)):
            with self.subTest(version=version, source=source):
                stream = self.stream()
                stream._received -= 2
                received = stream._received
                stream.publish(book(version, source))
                self.assertEqual(stream._received, received)
                with self.assertRaises(DepthQuoteUnavailable):
                    stream.snapshot()

    def test_conflicting_same_version_is_rejected(self):
        stream = self.stream()
        with self.assertRaisesRegex(ValueError, "conflicting"):
            stream.publish(book(ask="101"))

    async def test_short_gap_waits_for_next_snapshot_without_reconnecting(self):
        stream = self.stream()
        stream._received -= 2
        task = asyncio.create_task(stream.fresh_snapshot(timeout_seconds=1))
        await asyncio.sleep(0)
        stream.publish(book(2))
        result = await asyncio.wait_for(task, 1)
        self.assertEqual(result["version"], 2)
        stream.connect.assert_not_awaited()

    async def test_disconnect_restarts_and_clears_old_aster_ticker(self):
        stream = self.stream(AsterDepthStream)
        stream.publish_ticker({"bid": D(100), "ask": D(101), "version": 100, "source_ms": int(time.time()*1000)})
        stream.healthy = False
        async def start():
            self.assertIsNone(stream._book)
            self.assertIsNone(stream._ticker)
            stream.healthy = True
            stream.publish(book(2))
        stream.start = AsyncMock(side_effect=start)
        result = await stream.fresh_snapshot(timeout_seconds=1)
        self.assertEqual(result["version"], 2)
        self.assertNotIn("fast_quote", result)
        stream.start.assert_awaited_once()

    async def test_timeout_closes_connection_and_remains_unusable(self):
        closed = AsyncMock()
        class Socket:
            close = closed
            send_json = AsyncMock()
            async def receive(self):
                await asyncio.Future()
        stream = MexcSpotDepthStream(connect=AsyncMock(return_value=Socket()), symbol="TOKENUSDT")
        with self.assertRaisesRegex(DepthQuoteUnavailable, "recovery timed out.*symbol=TOKENUSDT"):
            await stream.fresh_snapshot(timeout_seconds=0.03)
        closed.assert_awaited_once()
        self.assertFalse(stream.healthy)
        self.assertEqual(stream._tasks, [])
        with self.assertRaises(DepthQuoteUnavailable):
            stream.snapshot()

    async def test_guard_recovers_and_reprices_before_dispatch_but_watch_never_waits(self):
        maker_stream, taker_stream = self.stream(), self.stream()
        maker, taker = self.adapter(maker_stream), self.adapter(taker_stream)
        guard = await self.guard(maker, taker)
        taker_stream._received -= 2
        async def recover():
            taker_stream.publish(book(2, ask="101"))
        taker.refresh_hedge_quote = AsyncMock(side_effect=recover)
        price = D(await guard.prepare("100"))
        self.assertGreater(price, D(100))
        taker.refresh_hedge_quote.assert_awaited_once()
        taker_stream._received -= 2
        with self.assertRaises(MakerPriceRecheck):
            await guard.check(str(price))
        self.assertEqual(taker.refresh_hedge_quote.await_count, 1)

    async def test_prepare_rechecks_both_caches_after_one_leg_recovers(self):
        maker_stream, taker_stream = self.stream(), self.stream()
        maker, taker = self.adapter(maker_stream), self.adapter(taker_stream)
        guard = await self.guard(maker, taker)
        taker_stream.healthy = False
        async def taker_recover():
            if not taker_stream.healthy:
                maker_stream._received -= 2
                taker_stream.healthy = True
                taker_stream.publish(book(2))
        async def maker_recover():
            maker_stream.publish(book(maker_stream._book["version"]+1))
        maker.refresh_hedge_quote = AsyncMock(side_effect=maker_recover)
        taker.refresh_hedge_quote = AsyncMock(side_effect=taker_recover)
        await guard.prepare("100")
        self.assertEqual(maker.refresh_hedge_quote.await_count, 2)
        maker_stream.snapshot()
        taker_stream.snapshot()

    async def test_total_prepare_timeout_preserves_diagnostic_and_cancels_recovery(self):
        maker_stream, taker_stream = self.stream(), self.stream()
        maker, taker = self.adapter(maker_stream), self.adapter(taker_stream)
        guard = await self.guard(maker, taker)
        taker_stream.healthy = False
        cancelled = asyncio.Event()
        async def stuck():
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        taker.refresh_hedge_quote = stuck
        with patch("hydra_basis.execution_engine.maker_price_guard.MAKER_QUOTE_TIMEOUT_SECONDS", 0.03):
            with self.assertRaisesRegex(DepthQuoteUnavailable, "preparation timed out.*disconnected.*TOKENUSDT"):
                await guard.prepare("100")
        self.assertTrue(cancelled.is_set())


if __name__ == "__main__":
    unittest.main()
