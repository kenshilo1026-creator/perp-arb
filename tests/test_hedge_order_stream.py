"""Order-specific terminal pushes may confirm hedges, never duplicate them."""
import asyncio
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.order_stream import OrderEventStream, decode_aster_order
from hydra_basis.execution_engine.hedge_safety import (
    HedgeFailure, execute_confirmed_market_order, merge_hedge_fills,
)


def status(order="1", qty="10", state="FILLED", original="10", symbol="TOKENUSDT", stamp=1):
    return {"symbol": symbol, "orderId": order, "status": state, "executedQty": qty,
            "origQty": original, "avgPrice": "100", "eventTime": stamp}


class HedgeOrderStreamTests(unittest.IsolatedAsyncioTestCase):
    def adapter(self, cls=AsterExecutionAdapter):
        adapter = cls()
        stream = OrderEventStream(venue="test", connect=AsyncMock(), renew=AsyncMock(), decode=decode_aster_order)
        stream.healthy = True
        adapter._fill_stream = stream
        if cls is AsterExecutionAdapter:
            adapter._resolve_raw_symbol = AsyncMock(return_value="TOKENUSDT")
        adapter.get_open_position = AsyncMock(return_value=None)
        adapter.get_order_execution = AsyncMock(return_value={"status": "NEW", "executedQty": "0"})
        return adapter, stream

    async def execute(self, adapter, **kwargs):
        return await execute_confirmed_market_order(adapter, symbol="TOKEN", side="BUY", quantity=D(10),
            clip_usd=1000, baseline=D(0), **kwargs)

    async def test_fill_before_http_ack_is_confirmed_with_average(self):
        adapter, stream = self.adapter()
        async def place(**kwargs):
            stream.publish(status())
            return {"ok": True, "order_id": "1"}
        adapter.place_market_order = AsyncMock(side_effect=place)
        result = await self.execute(adapter)
        self.assertEqual(result["source"], "user_stream")
        self.assertEqual(D(result["avg_price"]), D(100))
        self.assertEqual(result["filled_quantity"], "10")
        adapter.place_market_order.assert_awaited_once()
        adapter.get_order_execution.assert_not_awaited()
        self.assertEqual(merge_hedge_fills([result])["source"], "user_stream")

    async def test_push_interrupts_slow_rest_and_cleans_up_query(self):
        adapter, stream = self.adapter()
        querying, cancelled = asyncio.Event(), asyncio.Event()
        async def query(**kwargs):
            querying.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        adapter.get_order_execution = query
        adapter.place_market_order = AsyncMock(return_value={"ok": True, "order_id": "1"})
        task = asyncio.create_task(self.execute(adapter))
        await asyncio.wait_for(querying.wait(), 1)
        stream.publish(status())
        result = await asyncio.wait_for(task, 1)
        self.assertEqual(result["source"], "user_stream")
        self.assertTrue(cancelled.is_set())
        adapter.place_market_order.assert_awaited_once()

    async def test_push_interrupts_poll_sleep_and_partial_event_does_not_retry(self):
        adapter, stream = self.adapter()
        positioned = asyncio.Event()
        async def position(**kwargs):
            positioned.set()
            return None
        adapter.get_open_position = position
        adapter.place_market_order = AsyncMock(return_value={"ok": True, "order_id": "1"})
        stream.publish(status(qty="4", state="PARTIALLY_FILLED"))
        task = asyncio.create_task(self.execute(adapter, poll_delay=10))
        await asyncio.wait_for(positioned.wait(), 1)
        self.assertFalse(task.done())
        adapter.place_market_order.assert_awaited_once()
        stream.publish(status(stamp=2))
        result = await asyncio.wait_for(task, 1)
        self.assertEqual(result["source"], "user_stream")
        adapter.place_market_order.assert_awaited_once()

    async def test_terminal_partial_and_zero_fill_only_retry_proven_remainder(self):
        for first in ("0", "4"):
            with self.subTest(first=first):
                adapter, stream = self.adapter()
                calls = []
                async def place(**kwargs):
                    calls.append(kwargs)
                    order = str(len(calls))
                    if len(calls) == 1:
                        stream.publish(status(order=order, qty=first, state="CANCELED"))
                    else:
                        stream.publish(status(order=order, qty=kwargs["amount"], original=kwargs["amount"]))
                    return {"ok": True, "order_id": order}
                adapter.place_market_order = place
                result = await self.execute(adapter)
                self.assertEqual([D(c["amount"]) for c in calls], [D(10), D(10) - D(first)])
                self.assertEqual(result["filled_quantity"], "10")
                self.assertTrue(all("limit_price" not in c for c in calls))

    async def test_wrong_order_symbol_and_nonterminal_regression_are_ignored(self):
        adapter, stream = self.adapter()
        stream.publish(status(order="other"))
        stream.publish(status(symbol="OTHERUSDT"))
        waiter = asyncio.create_task(adapter.wait_for_hedge_terminal(order_result={"order_id": "1"}, symbol="TOKEN"))
        await asyncio.sleep(0)
        self.assertFalse(waiter.done())
        stream.publish(status(qty="0", state="CANCELED"))
        stream.publish(status(qty="0", state="NEW", stamp=2))
        result = await asyncio.wait_for(waiter, 1)
        self.assertEqual(result["filled_quantity"], "0")
        self.assertEqual(result["raw"]["status"], "CANCELED")

    async def test_disconnect_retains_rest_confirmation(self):
        adapter, stream = self.adapter()
        querying = asyncio.Event()
        release = asyncio.Event()
        async def query(**kwargs):
            querying.set()
            await release.wait()
            return status()
        adapter.get_order_execution = query
        adapter.place_market_order = AsyncMock(return_value={"ok": True, "order_id": "1"})
        task = asyncio.create_task(self.execute(adapter))
        await asyncio.wait_for(querying.wait(), 1)
        stream._failed(RuntimeError("disconnected"))
        release.set()
        result = await asyncio.wait_for(task, 1)
        self.assertEqual(result["source"], "rest")
        self.assertEqual(result["filled_quantity"], "10")

    async def test_rest_failure_does_not_suppress_later_push(self):
        adapter, stream = self.adapter()
        queried = asyncio.Event()
        async def query(**kwargs):
            queried.set()
            raise RuntimeError("REST offline")
        adapter.get_order_execution = query
        adapter.place_market_order = AsyncMock(return_value={"ok": True, "order_id": "1"})
        task = asyncio.create_task(self.execute(adapter, poll_delay=10))
        await asyncio.wait_for(queried.wait(), 1)
        stream.publish(status())
        self.assertEqual((await asyncio.wait_for(task, 1))["source"], "user_stream")

    async def test_missing_events_or_unknown_submission_never_duplicate_order(self):
        for lost_ack in (False, True):
            with self.subTest(lost_ack=lost_ack):
                adapter, stream = self.adapter()
                if lost_ack:
                    # An unrelated account fill cannot identify a lost submission.
                    stream.publish(status())
                    adapter.place_market_order = AsyncMock(side_effect=TimeoutError("lost ACK"))
                else:
                    adapter.place_market_order = AsyncMock(return_value={"ok": True, "order_id": "1"})
                with self.assertRaises(HedgeFailure) as raised:
                    await self.execute(adapter, confirmation_attempts=1)
                self.assertTrue(raised.exception.uncertain)
                adapter.place_market_order.assert_awaited_once()

    async def test_cancellation_cleans_up_both_confirmation_tasks(self):
        adapter, stream = self.adapter()
        querying, cancelled = asyncio.Event(), asyncio.Event()
        async def query(**kwargs):
            querying.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        adapter.get_order_execution = query
        adapter.place_market_order = AsyncMock(return_value={"ok": True, "order_id": "1"})
        task = asyncio.create_task(self.execute(adapter))
        await asyncio.wait_for(querying.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(cancelled.is_set())
        adapter.place_market_order.assert_awaited_once()

    async def test_mexc_push_before_ack_skips_status_enrichment(self):
        adapter, stream = self.adapter(MexcSpotExecutionAdapter)
        async def post(params):
            stream.publish(status())
            return {"orderId": "1", "status": "NEW"}
        adapter._post_order = AsyncMock(side_effect=post)
        adapter._get_order = AsyncMock(side_effect=AssertionError("unnecessary status query"))
        result = await self.execute(adapter)
        self.assertEqual(result["source"], "user_stream")
        self.assertFalse(result["request_timing"]["status_query_performed"])
        adapter._get_order.assert_not_awaited()
        adapter._post_order.assert_awaited_once()

    async def test_mexc_push_interrupts_post_order_enrichment(self):
        adapter, stream = self.adapter(MexcSpotExecutionAdapter)
        adapter._post_order = AsyncMock(return_value={"orderId": "1", "status": "NEW"})
        querying, cancelled = asyncio.Event(), asyncio.Event()
        async def get(params):
            querying.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        adapter._get_order = get
        task = asyncio.create_task(self.execute(adapter))
        await asyncio.wait_for(querying.wait(), 1)
        stream.publish(status())
        result = await asyncio.wait_for(task, 1)
        self.assertEqual(result["source"], "user_stream")
        self.assertTrue(cancelled.is_set())
        adapter._post_order.assert_awaited_once()

    async def test_mexc_stream_disconnect_still_enriches_from_rest(self):
        adapter, stream = self.adapter(MexcSpotExecutionAdapter)
        stream.healthy = False
        adapter._post_order = AsyncMock(return_value={"orderId": "1"})
        adapter._get_order = AsyncMock(return_value=status())
        result = await self.execute(adapter)
        self.assertEqual(result["source"], "rest")
        self.assertEqual(result["filled_quantity"], "10")


if __name__ == "__main__":
    unittest.main()
