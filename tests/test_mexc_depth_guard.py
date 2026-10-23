import asyncio
import unittest
from decimal import Decimal as D
from hydra_basis.execution_engine.quote_safety import stamp_quote
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.mexc_depth_stream import MexcSpotDepthStream, decode_depth
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.maker_price_guard import MakerPriceGuard, MakerPriceRecheck
from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, execute_spot_perp_plan


def varint(n):
    result = bytearray()
    while n >= 128:
        result.append((n & 127) | 128)
        n >>= 7
    result.append(n)
    return bytes(result)


def field(tag, value):
    value = value.encode() if isinstance(value, str) else value
    return varint((tag << 3) | 2) + varint(len(value)) + value


def frame(version="100", bids=(("0.2265", "1"), ("0.2259", "999"))):
    body = field(1, field(1, "0.2266") + field(2, "1000"))
    body += b"".join(field(2, field(1, price) + field(2, qty)) for price, qty in bids)
    body += field(4, version)
    return (field(1, "spot@public.limit.depth.v3.api.pb@GRVTUSDT@20")
            + field(3, "GRVTUSDT") + field(303, body))


def guard(fetch, **kwargs):
    return MakerPriceGuard(fetch_books=AsyncMock(), tick_size=AsyncMock(return_value="0.0001"),
        maker_side="BUY", taker_side="SELL", quantity=D("1000"), fetch_taker_book=fetch, **kwargs)


class DepthTests(unittest.IsolatedAsyncioTestCase):
    def test_repeated_depth_levels_preserved_and_full_clip_vwap_used(self):
        book = decode_depth(frame(), "GRVTUSDT")
        self.assertEqual(len(book["bids"]), 2)
        checker = guard(AsyncMock())
        self.assertEqual(checker.taker_price(book), D("0.2259006"))
        self.assertNotEqual(checker.taker_price(book), book["bid"])

    async def test_best_price_passes_but_full_clip_breaches_band(self):
        book = decode_depth(frame(), "GRVTUSDT")
        checker = guard(AsyncMock(return_value=book))
        with self.assertRaisesRegex(MakerPriceRecheck, "adverse maker/taker gap"):
            await checker.check("0.2268")

    async def test_insufficient_depth_requires_cancellation(self):
        book = decode_depth(frame(bids=(("0.2265", "1"),)), "GRVTUSDT")
        with self.assertRaisesRegex(MakerPriceRecheck, "insufficient hedge depth"):
            await guard(AsyncMock(return_value=book)).check("0.2268")

    async def test_stale_or_disconnected_depth_requires_cancellation(self):
        stream = MexcSpotDepthStream(connect=AsyncMock(), symbol="GRVTUSDT")
        stream.healthy = True
        with patch("hydra_basis.execution_engine.mexc_depth_stream.time.monotonic", return_value=10):
            stream.publish(decode_depth(frame(), "GRVTUSDT"))
        async def quote():
            return stream.snapshot()
        with patch("hydra_basis.execution_engine.mexc_depth_stream.time.monotonic", return_value=12):
            with self.assertRaisesRegex(MakerPriceRecheck, "stale"):
                await guard(quote).check("0.2268")
        stream.healthy = False
        with self.assertRaises(MakerPriceRecheck):
            await guard(quote).check("0.2268")

    async def test_update_wakes_guard_without_one_second_poll(self):
        stream = MexcSpotDepthStream(connect=AsyncMock(), symbol="GRVTUSDT")
        stream.healthy = True
        stream.publish(decode_depth(frame(bids=(("0.2265", "1000"),)), "GRVTUSDT"))
        checked = asyncio.Event()
        async def quote():
            checked.set()
            return stream.snapshot()
        watcher = asyncio.create_task(guard(quote, wait_for_update=stream.wait_for_update).watch("0.2268"))
        await checked.wait()
        stream.publish(decode_depth(frame(version="101"), "GRVTUSDT"))
        with self.assertRaises(MakerPriceRecheck):
            await asyncio.wait_for(watcher, 0.2)

    def test_old_version_cannot_restore_bad_depth_or_reset_age(self):
        stream = MexcSpotDepthStream(connect=AsyncMock(), symbol="GRVTUSDT")
        stream.healthy = True
        with patch("hydra_basis.execution_engine.mexc_depth_stream.time.monotonic", return_value=10):
            stream.publish(decode_depth(frame(version="101"), "GRVTUSDT"))
        with patch("hydra_basis.execution_engine.mexc_depth_stream.time.monotonic", return_value=20):
            stream.publish(decode_depth(frame(version="100"), "GRVTUSDT"))
        self.assertEqual(stream._received, 10)
        self.assertEqual(stream._book["version"], 101)

    async def test_prepare_maker_price_uses_depth_average(self):
        book = decode_depth(frame(), "GRVTUSDT")
        checker = guard(AsyncMock(return_value=book))
        checker.fetch_books = AsyncMock(return_value=({"bid": "0.2268", "ask": "0.2270"}, book))
        self.assertEqual(await checker.prepare("0.2268"), "0.2263")

    async def test_mexc_market_full_response_skips_unnecessary_query(self):
        adapter = MexcSpotExecutionAdapter(api_key="unused", api_secret="unused")
        adapter._load_symbol_rules = AsyncMock(return_value={"tick": D("0.0001"), "step": D("1"), "min_qty": D("1")})
        adapter._post_order = AsyncMock(return_value={"orderId": 1, "status": "FILLED",
            "executedQty": "1000", "cummulativeQuoteQty": "225.9"})
        adapter._get_order = AsyncMock()
        result = await adapter.place_market_order(symbol="GRVT", side="SELL", amount="1000", clip_usd=226, limit_price="0.225")
        adapter._get_order.assert_not_awaited()
        self.assertFalse(result["request_timing"]["status_query_performed"])
        self.assertEqual(result["request_timing"]["post_order_status_ms"], 0)

    async def test_ack_and_malformed_metadata_query_status_without_second_order(self):
        for payload in ({"status": "NEW", "price": "0.2265"},
                        {"status": "FILLED", "avgPrice": "NaN", "executedQty": "1000"}):
            with self.subTest(payload=payload):
                adapter = MexcSpotExecutionAdapter(api_key="unused", api_secret="unused")
                adapter._load_symbol_rules = AsyncMock(return_value={"tick": D("0.0001"), "step": D("1"), "min_qty": D("1")})
                adapter._post_order = AsyncMock(return_value={"orderId": 1, **payload})
                adapter._get_order = AsyncMock(return_value={"status": "FILLED", "avgPrice": "0.2259"})
                result = await adapter.place_market_order(symbol="GRVT", side="SELL", amount="1000", clip_usd=226, limit_price="0.225")
                adapter._post_order.assert_awaited_once()
                adapter._get_order.assert_awaited_once()
                self.assertTrue(result["request_timing"]["status_query_performed"])

    async def test_live_depth_is_ready_before_maker_and_required_if_adapter_supports_it(self):
        plan = build_spot_perp_plan(symbol="GRVT", mode="close", short_venue="aster",
            quantity=D("1000"), clip_usd=226, spot_book={"bid": .2265, "ask": .2266},
            perp_book={"bid": .2268, "ask": .227})
        taker = SimpleNamespace(prepare_hedge_quotes=AsyncMock(),
            hedge_quote=AsyncMock(return_value=decode_depth(frame(), "GRVTUSDT")),
            wait_for_hedge_quote=AsyncMock())
        maker = SimpleNamespace(place_limit_order=AsyncMock(), get_price_tick_size=AsyncMock(return_value="0.0001"))
        async def execute(**kwargs):
            taker.prepare_hedge_quotes.assert_awaited_once_with("GRVT")
            self.assertEqual(kwargs["maker_price_guard"].quantity, D("1000"))
            return {"ok": True}
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[maker, taker]), \
             patch("scripts.run_spot_perp_arbitrage.fetch_plan_books", new=AsyncMock(return_value=(stamp_quote({"bid": .2265, "ask": .2266, "bids": [(.2265, 2000)], "asks": [(.2266, 2000)]}), stamp_quote({"bid": .2268, "ask": .227, "bids": [(.2268, 2000)], "asks": [(.227, 2000)]})))), \
             patch("scripts.run_spot_perp_arbitrage.execute_single_clip", new=execute):
            await execute_spot_perp_plan(plan=plan, leverage=1, max_hedge_slippage_bps=D(20), max_quote_age_ms=D(1000), max_maker_reprices=0)
        taker.prepare_hedge_quotes = AsyncMock(side_effect=RuntimeError("depth unavailable"))
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[maker, taker]), \
             patch("scripts.run_spot_perp_arbitrage.fetch_plan_books", new=AsyncMock(return_value=(stamp_quote({"bid": .2265, "ask": .2266, "bids": [(.2265, 2000)], "asks": [(.2266, 2000)]}), stamp_quote({"bid": .2268, "ask": .227, "bids": [(.2268, 2000)], "asks": [(.227, 2000)]})))), \
             patch("scripts.run_spot_perp_arbitrage.execute_single_clip", new=AsyncMock()) as execute:
            with self.assertRaisesRegex(RuntimeError, "depth unavailable"):
                await execute_spot_perp_plan(plan=plan, leverage=1, max_hedge_slippage_bps=D(20), max_quote_age_ms=D(1000), max_maker_reprices=0)
            execute.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
