import asyncio
import json
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.order_stream import (
    OrderEventStream, MEXC_ORDER_CHANNEL, decode_aster_order, decode_mexc_order,
)


def status(*, order="123", symbol="GRVTUSDT", qty="47", state="FILLED", stamp=100):
    return {"symbol": symbol, "orderId": order, "status": state,
            "executedQty": qty, "origQty": "47", "avgPrice": "0.2101", "eventTime": stamp}


def aster_event(qty="47", state="FILLED"):
    return {"e": "ORDER_TRADE_UPDATE", "E": 100, "T": 99,
            "o": {"s": "GRVTUSDT", "i": 123, "X": state, "z": qty,
                  "q": "47", "ap": "0.2101"}}


def text_message(payload):
    return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(payload))


class FakeWebSocket:
    def __init__(self):
        self.messages = asyncio.Queue()
        self.send_json = AsyncMock()
        self.close = AsyncMock()

    async def receive(self):
        return await self.messages.get()


class DecoderTests(unittest.TestCase):
    def test_aster_cumulative_fill_and_average(self):
        decoded = decode_aster_order(aster_event())
        self.assertEqual(decoded["orderId"], "123")
        self.assertEqual(decoded["executedQty"], "47")
        self.assertEqual(decoded["avgPrice"], "0.2101")
        self.assertEqual(decoded["tradeTime"], 99)

    def test_aster_unrelated_event_and_expired_key(self):
        self.assertIsNone(decode_aster_order({"e": "ACCOUNT_UPDATE"}))
        with self.assertRaises(RuntimeError):
            decode_aster_order({"e": "listenKeyExpired"})

    def test_mexc_official_field_numbers_and_partial_status(self):
        # A binary fixture using the published wrapper/privateOrders tags.
        # Body: id, quantity, avgPrice, cumulativeQuantity, cumulativeAmount, status.
        body = bytes.fromhex("0a03313233220234373206302e323130316a0231377206332e353731377803")
        channel = MEXC_ORDER_CHANNEL.encode()
        frame = b"\x0a" + bytes([len(channel)]) + channel + b"\x1a\x08GRVTUSDT\x30\x64\x82\x13" + bytes([len(body)]) + body
        decoded = decode_mexc_order(frame)
        self.assertEqual(decoded["status"], "PARTIALLY_FILLED")
        self.assertEqual(decoded["executedQty"], "17")
        self.assertEqual(decoded["avgPrice"], "0.2101")
        self.assertEqual(decoded["cummulativeQuoteQty"], "3.5717")
        self.assertEqual(decoded["eventTime"], 100)
        self.assertEqual(decoded["symbol"], "GRVTUSDT")
        self.assertEqual(decode_mexc_order(frame[:-1] + b"\x02")["status"], "FILLED")

    def test_mexc_malformed_frames_rejected(self):
        for data in (b"\x80", b"\x0a\xff", b"\x0a\x04x", b"\x00", b"\x0f"):
            with self.subTest(data=data), self.assertRaises(ValueError):
                decode_mexc_order(data)
        self.assertIsNone(decode_mexc_order(b"\x0a\x03foo"))


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ws = FakeWebSocket()
        self.stream = OrderEventStream(venue="aster", connect=AsyncMock(return_value=self.ws),
            renew=AsyncMock(), decode=decode_aster_order)
        await self.stream.start()
        self.addAsyncCleanup(self.stream.close)

    def wait(self, fetch, **kwargs):
        return self.stream.wait_for_fill(symbol="GRVTUSDT", order_id=123,
            fetch_status=fetch, timeout_seconds=1, poll_interval_seconds=0.2,
            timeout_message="fill timeout", **kwargs)

    async def test_fill_before_http_ack_is_cached_without_rest(self):
        self.stream.publish(status())
        fetch = AsyncMock()
        result = await self.wait(fetch)
        self.assertEqual(result["filled_quantity"], "47")
        self.assertEqual(result["source"], "user_stream")
        fetch.assert_not_awaited()

    async def test_push_wakes_while_rest_request_is_blocked(self):
        querying = asyncio.Event()
        cancelled = asyncio.Event()
        async def blocked_rest():
            querying.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        waiter = asyncio.create_task(self.wait(blocked_rest))
        await querying.wait()
        self.ws.messages.put_nowait(text_message(aster_event()))
        result = await asyncio.wait_for(waiter, 0.1)
        self.assertEqual(result["source"], "user_stream")
        self.assertTrue(cancelled.is_set())

    async def test_wrong_order_or_symbol_does_not_wake_hedge(self):
        self.stream.publish(status(order="other"))
        self.stream.publish(status(symbol="BTCUSDT"))
        queried = asyncio.Event()
        async def rest():
            queried.set()
            await asyncio.Future()
        waiter = asyncio.create_task(self.wait(rest))
        await queried.wait()
        self.assertFalse(waiter.done())
        self.stream.publish(status())
        self.assertEqual((await waiter)["filled_quantity"], "47")

    async def test_out_of_order_and_duplicate_events_do_not_regress_quantity(self):
        self.stream.publish(status(qty="30", state="PARTIALLY_FILLED", stamp=200))
        self.stream.publish(status(qty="17", state="PARTIALLY_FILLED", stamp=300))
        self.stream.publish(status(qty="40", state="PARTIALLY_FILLED", stamp=100))
        self.stream.publish(status(qty="30", state="PARTIALLY_FILLED", stamp=200))
        result = await self.wait(AsyncMock(), return_on_partial_fill=True)
        self.assertEqual(result["filled_quantity"], "30")
        self.assertEqual(self.stream._sequence, 1)

    async def test_same_partial_fill_not_redelivered_as_progress(self):
        self.stream.publish(status(qty="17", state="PARTIALLY_FILLED"))
        await self.wait(AsyncMock(), return_on_partial_fill=True)
        self.assertIsNone(self.stream._cached_fill(("GRVTUSDT", "123"), True))
        self.stream.publish(status(qty="30", state="PARTIALLY_FILLED", stamp=101))
        self.assertEqual((await self.wait(AsyncMock(), return_on_partial_fill=True))["filled_quantity"], "30")

    async def test_bad_quantities_cannot_authorize_hedge(self):
        for qty, state in (("NaN", "FILLED"), ("-1", "PARTIALLY_FILLED"),
                           ("48", "FILLED"), ("17", "FILLED")):
            with self.subTest(qty=qty), self.assertRaises(ValueError):
                self.stream.publish(status(qty=qty, state=state))

    async def test_disconnect_falls_back_to_rest(self):
        self.ws.messages.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.CLOSED))
        result = await self.wait(AsyncMock(return_value=status()))
        self.assertEqual(result["source"], "rest_poll")
        self.assertEqual(result["filled_quantity"], "47")

    async def test_rest_error_does_not_suppress_later_stream_fill(self):
        called = asyncio.Event()
        async def failed():
            called.set()
            raise RuntimeError("REST unavailable")
        waiter = asyncio.create_task(self.wait(failed))
        await called.wait()
        self.stream.publish(status())
        self.assertEqual((await waiter)["source"], "user_stream")

    async def test_cancellation_cleans_up_pending_rest(self):
        querying, cancelled = asyncio.Event(), asyncio.Event()
        async def rest():
            querying.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        waiter = asyncio.create_task(self.wait(rest))
        await querying.wait()
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertTrue(cancelled.is_set())

    async def test_mexc_requires_subscription_ack_before_ready(self):
        ws = FakeWebSocket()
        stream = OrderEventStream(venue="mexc_spot", connect=AsyncMock(return_value=ws),
            renew=AsyncMock(), decode=decode_mexc_order, channel=MEXC_ORDER_CHANNEL)
        self.addAsyncCleanup(stream.close)
        starting = asyncio.create_task(stream.start())
        await asyncio.sleep(0)
        self.assertFalse(stream.healthy)
        ws.messages.put_nowait(text_message({"code": 0, "msg": MEXC_ORDER_CHANNEL}))
        await starting
        self.assertTrue(stream.healthy)
        ws.send_json.assert_awaited_once()

    async def test_rejected_subscription_closes_connection(self):
        ws = FakeWebSocket()
        ws.messages.put_nowait(text_message({"code": 401, "msg": "rejected"}))
        stream = OrderEventStream(venue="mexc_spot", connect=AsyncMock(return_value=ws),
            renew=AsyncMock(), decode=decode_mexc_order, channel=MEXC_ORDER_CHANNEL)
        with self.assertRaises(RuntimeError):
            await stream.start()
        ws.close.assert_awaited_once()

    async def test_adapter_uses_stream_with_canonical_symbol(self):
        for adapter in (AsterExecutionAdapter(), MexcSpotExecutionAdapter()):
            adapter._fill_stream = self.stream
            if isinstance(adapter, AsterExecutionAdapter):
                adapter._resolve_raw_symbol = AsyncMock(return_value="GRVTUSDT")
                adapter._get_order_status = AsyncMock(side_effect=AssertionError("REST should not run"))
            else:
                adapter._get_order = AsyncMock(side_effect=AssertionError("REST should not run"))
            self.stream.publish(status())
            result = await adapter.wait_for_order_fill(order_result={"order_id": 123}, symbol="GRVT",
                side="BUY", amount="47", timeout_seconds=1)
            self.assertEqual(result["source"], "user_stream")

    async def test_full_fill_immediately_reaches_hedge_and_records_local_latency(self):
        async def place(**kwargs):
            self.stream.publish(status())  # Push arrives before the HTTP ACK.
            return {"ok": True, "order_id": 123}
        maker = AsterExecutionAdapter()
        maker._fill_stream = self.stream
        maker._resolve_raw_symbol = AsyncMock(return_value="GRVTUSDT")
        maker.place_limit_order = place
        maker.cancel_order = AsyncMock(side_effect=AssertionError("full fill needs no cancellation"))
        maker._get_order_status = AsyncMock(side_effect=AssertionError("cached fill needs no REST"))
        taker = SimpleNamespace(place_market_order=AsyncMock(return_value={"ok": True, "avgPrice": "0.2092"}))
        result = await execute_single_clip_with_sides(symbol="GRVT", clip_usd=10, quantity=Decimal("47"),
            maker_venue="aster", taker_venue="mexc_spot", maker_side="BUY", taker_side="SELL",
            maker_adapter=maker, taker_adapter=taker, max_hedge_retries=0,
            state_machine=ExecutionStateMachine(), maker_price="0.2101", require_maker_fill_confirmation=True)
        taker.place_market_order.assert_awaited_once()
        self.assertEqual(taker.place_market_order.call_args.kwargs["amount"], "47")
        self.assertEqual(result["execution_timing"]["maker_confirmation_source"], "user_stream")
        self.assertGreaterEqual(result["execution_timing"]["maker_observed_to_hedge_call_ms"], 0)
        maker.cancel_order.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
