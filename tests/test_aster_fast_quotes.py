"""Fast BBO warnings must not fabricate depth or interfere with fill hedging."""
import asyncio
import time
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.aster_depth_stream import (
    AsterDepthStream, decode_aster_book_ticker, decode_aster_depth,
)
from hydra_basis.execution_engine.maker_price_guard import (
    MakerPriceGuard, MakerPriceRecheck, wait_with_price_guard,
)


def ticker(version=2, bid="100", ask="100.3"):
    return {"e": "bookTicker", "s": "TOKENUSDT", "u": version,
            "b": bid, "a": ask, "B": "10", "A": "10", "E": int(time.time() * 1000)}


def depth(version=1, bid="99.9", ask="100.1"):
    return {"e": "depthUpdate", "s": "TOKENUSDT", "u": version,
            "b": [[bid, "10"]], "a": [[ask, "10"]], "E": int(time.time() * 1000)}


class AsterFastQuoteTests(unittest.IsolatedAsyncioTestCase):
    def stream(self):
        stream = AsterDepthStream(connect=AsyncMock(), symbol="TOKENUSDT")
        stream.healthy = True
        stream.publish(decode_aster_depth(depth(), stream.symbol))
        return stream

    def guard(self, stream, maker_side="SELL"):
        async def quote():
            return stream.snapshot()
        return MakerPriceGuard(fetch_books=AsyncMock(return_value=(
            {"bid": D(99), "ask": D(101)}, stream.snapshot())),
            tick_size=AsyncMock(return_value="0.01"),
            maker_side=maker_side, taker_side="BUY" if maker_side == "SELL" else "SELL",
            quantity=D(10), fetch_taker_book=quote, wait_for_update=stream.wait_for_update)

    async def test_ticker_wakes_guard_without_waiting_for_next_depth(self):
        stream = self.stream()
        guard = self.guard(stream)
        checked = asyncio.Event()
        original = guard.check
        async def check(price):
            await original(price)
            checked.set()
        guard.check = check
        watch = asyncio.create_task(guard.watch("100"))
        try:
            await asyncio.wait_for(checked.wait(), 1)
            stream.publish_ticker(decode_aster_book_ticker(ticker(), stream.symbol))
            with self.assertRaises(MakerPriceRecheck):
                await asyncio.wait_for(watch, 1)
            self.assertEqual(stream.snapshot()["version"], 1)
            guard.fetch_books.assert_not_awaited()
        finally:
            watch.cancel()
            await asyncio.gather(watch, return_exceptions=True)

    async def test_both_directions_and_reprice_use_adverse_fast_quote(self):
        for side, bid, ask in (("SELL", "100", "100.3"), ("BUY", "99.7", "100")):
            with self.subTest(side=side):
                stream = self.stream()
                stream.publish_ticker(decode_aster_book_ticker(ticker(bid=bid, ask=ask), stream.symbol))
                guard = self.guard(stream, side)
                with self.assertRaises(MakerPriceRecheck):
                    await guard.check("100")
                price = D(await guard.prepare("100"))
                self.assertGreater(price, D(100)) if side == "SELL" else self.assertLess(price, D(100))
                await guard.check(str(price))

    async def test_improved_best_price_does_not_hide_bad_full_quantity_vwap(self):
        stream = self.stream()
        book = decode_aster_depth(depth(2), stream.symbol)
        book["asks"] = [(D("100.1"), D(1)), (D(101), D(9))]
        stream.publish(book)
        stream.publish_ticker(decode_aster_book_ticker(ticker(3, "99.9", "100"), stream.symbol))
        with self.assertRaises(MakerPriceRecheck):
            await self.guard(stream).check("100")
        self.assertEqual(stream.snapshot()["asks"], book["asks"])

    async def test_out_of_order_ticker_cannot_erase_warning_and_depth_catches_up(self):
        stream = self.stream()
        stream.publish_ticker(decode_aster_book_ticker(ticker(4), stream.symbol))
        stream.publish_ticker(decode_aster_book_ticker(ticker(3, "99.9", "100"), stream.symbol))
        self.assertEqual(stream.snapshot()["fast_quote"]["ask"], D("100.3"))
        stream.publish(decode_aster_depth(depth(3), stream.symbol))
        self.assertIn("fast_quote", stream.snapshot())
        stream.publish(decode_aster_depth(depth(4), stream.symbol))
        self.assertNotIn("fast_quote", stream.snapshot())
        stream.publish_ticker(decode_aster_book_ticker(ticker(4), stream.symbol))
        self.assertNotIn("fast_quote", stream.snapshot())

    async def test_ticker_does_not_refresh_stale_depth(self):
        stream = self.stream()
        stream._received -= 2
        stream.publish_ticker(decode_aster_book_ticker(ticker(), stream.symbol))
        with self.assertRaisesRegex(RuntimeError, "stale"):
            stream.snapshot()

    async def test_old_ticker_source_timestamp_invalidates_guard(self):
        stream = self.stream()
        guard = self.guard(stream)
        event = ticker()
        event["E"] -= 2000
        stream.publish_ticker(decode_aster_book_ticker(event, stream.symbol))
        with self.assertRaisesRegex(MakerPriceRecheck, "stale"):
            await guard.check("100")

    async def test_confirmed_fill_wins_simultaneous_ticker_warning(self):
        stream = self.stream()
        stream.publish_ticker(decode_aster_book_ticker(ticker(), stream.symbol))
        result = await wait_with_price_guard(AsyncMock(return_value={"filled_quantity": "10"})(),
                                            self.guard(stream), "100")
        self.assertEqual(result["filled_quantity"], "10")

    def test_decoder_accepts_combined_stream_and_rejects_bad_prices(self):
        self.assertEqual(decode_aster_book_ticker({"data": ticker()}, "TOKENUSDT")["version"], 2)
        self.assertIsNone(decode_aster_book_ticker(ticker(), "OTHERUSDT"))
        for key, value in (("a", "NaN"), ("b", "101"), ("B", "0"), ("A", "-1")):
            with self.subTest(key=key, value=value):
                event = ticker()
                event[key] = value
                with self.assertRaises(ValueError):
                    decode_aster_book_ticker(event, "TOKENUSDT")

    async def test_combined_reader_delivers_both_events_and_disconnect_invalidates(self):
        queue = asyncio.Queue()
        ws = SimpleNamespace(receive=queue.get, close=AsyncMock())
        stream = AsterDepthStream(connect=AsyncMock(return_value=ws), symbol="TOKENUSDT")
        def message(payload):
            return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, json=lambda: {"data": payload})
        await queue.put(message(depth()))
        await queue.put(message(ticker()))
        try:
            await stream.start()
            self.assertEqual(stream.snapshot()["fast_quote"]["ask"], D("100.3"))
            await queue.put(SimpleNamespace(type=aiohttp.WSMsgType.CLOSED))
            await asyncio.wait_for(stream._tasks[0], 1)
            with self.assertRaises(RuntimeError):
                stream.snapshot()
        finally:
            await stream.close()
        ws.close.assert_awaited_once()

    async def test_adapter_subscribes_both_channels_on_same_connection(self):
        adapter = AsterExecutionAdapter()
        adapter._resolve_raw_symbol = AsyncMock(return_value="TOKENUSDT")
        session = SimpleNamespace(closed=False, ws_connect=AsyncMock(), close=AsyncMock())
        adapter._session = session
        async def start(stream):
            await stream.connect()
            stream.healthy = True
            stream.publish(decode_aster_depth(depth(), stream.symbol))
        try:
            with patch.object(AsterDepthStream, "start", start):
                await adapter.prepare_hedge_quotes("TOKEN")
            url = session.ws_connect.call_args.args[0]
            self.assertIn("/stream?streams=tokenusdt@depth20@100ms/tokenusdt@bookTicker", url)
            session.ws_connect.assert_awaited_once()
        finally:
            await adapter.close()
