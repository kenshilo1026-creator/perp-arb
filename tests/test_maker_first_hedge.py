"""The spread threshold controls resting makers, never confirmed-fill hedges."""
import asyncio
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.hedge_safety import execute_confirmed_market_order, HedgeFailure
from hydra_basis.execution_engine.maker_price_guard import (
    MakerPriceGuard, MakerPriceRecheck, adverse_price_gap, bounded_maker_price, create_maker_guard,
)
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine


class MakerFirstHedgeTests(unittest.IsolatedAsyncioTestCase):
    async def execute(self, maker, taker, **kw):
        return await execute_single_clip_with_sides(
            symbol="TOKEN", quantity=D(10), clip_usd=1000,
            maker_venue="mexc_spot", taker_venue="aster", maker_side="SELL", taker_side="BUY",
            maker_adapter=maker, taker_adapter=taker, maker_price="100",
            state_machine=ExecutionStateMachine(), max_hedge_retries=0,
            require_maker_fill_confirmation=True, verify_hedge_fill=True,
            maker_market_type="spot", **kw,
        )

    async def test_fill_hedges_at_market_even_if_gap_jumps_above_threshold(self):
        maker = SimpleNamespace(
            place_limit_order=AsyncMock(return_value={"ok": True, "order_id": 1}),
            wait_for_order_fill=AsyncMock(return_value={
                "ok": True, "status": "FILLED", "filled_quantity": "10", "avg_price": "100"}),
            get_open_position=AsyncMock(side_effect=[{"side": "LONG", "quantity": "10"}, None]),
        )
        taker = SimpleNamespace(
            place_market_order=AsyncMock(return_value={"ok": True, "terminal": True,
                "filled_quantity": "10", "avg_price": "101"}),
            get_open_position=AsyncMock(side_effect=[{"side": "SHORT", "quantity": "10"}, None]),
        )
        guard = SimpleNamespace(maker_side="SELL", taker_side="BUY", taker_book={"ask": 100},
            taker_price=lambda b: D(100), prepare=AsyncMock(return_value="100"),
            watch=AsyncMock(side_effect=MakerPriceRecheck("price jumped")))
        result = await self.execute(maker, taker, maker_price_guard=guard, taker_reduce_only=True)
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(result["execution_price_summary"]["taker_avg_price"], "101")
        kwargs = taker.place_market_order.call_args.kwargs
        self.assertNotIn("limit_price", kwargs)
        self.assertTrue(kwargs["reduce_only"])
        self.assertEqual(kwargs["amount"], "10")

    async def test_partial_hedge_starts_before_cancel_returns_and_late_fill_only_once(self):
        submitted = asyncio.Event()
        amounts = []
        async def hedge(**kw):
            amounts.append(D(kw["amount"]))
            submitted.set()
            self.assertNotIn("limit_price", kw)
            return {"ok": True, "terminal": True, "filled_quantity": kw["amount"], "avg_price": "101"}
        async def cancel(**kw):
            # A sequential cancel-then-hedge implementation times out here.
            await asyncio.wait_for(submitted.wait(), 1)
            return {"ok": True, "terminal": True, "filled_quantity": "10", "avg_price": "100.2"}
        maker = SimpleNamespace(
            place_limit_order=AsyncMock(return_value={"ok": True, "order_id": 1}),
            wait_for_order_fill=AsyncMock(return_value={"ok": True, "partial": True,
                "filled_quantity": "4", "avg_price": "102"}),
            cancel_order=cancel,
            get_open_position=AsyncMock(side_effect=[{"side": "LONG", "quantity": "10"}, None]),
        )
        taker = SimpleNamespace(place_market_order=hedge,
            get_open_position=AsyncMock(side_effect=[{"side": "SHORT", "quantity": "10"}, None]))
        result = await self.execute(maker, taker)
        self.assertEqual(amounts, [D(4), D(6)])
        self.assertEqual(result["execution_price_summary"]["maker_avg_price"], "100.2")
        self.assertEqual(result["execution_price_summary"]["taker_avg_price"], "101")
        maker.place_limit_order.assert_awaited_once()

    async def test_unknown_hedge_is_not_duplicated_or_reversed(self):
        adapter = SimpleNamespace(place_market_order=AsyncMock(side_effect=TimeoutError("lost response")),
            get_open_position=AsyncMock(return_value=None))
        with self.assertRaises(HedgeFailure) as raised:
            await execute_confirmed_market_order(adapter, symbol="TOKEN", side="BUY", quantity=D(10),
                clip_usd=1000, baseline=D(0), confirmation_attempts=1)
        self.assertTrue(raised.exception.uncertain)
        adapter.place_market_order.assert_awaited_once()

    async def test_known_partial_retries_immediately_without_price_limit(self):
        adapter = SimpleNamespace(place_market_order=AsyncMock(side_effect=[
            {"terminal": True, "filled_quantity": "4", "avg_price": "100"},
            {"terminal": True, "filled_quantity": "6", "avg_price": "102"},
        ]))
        with patch("hydra_basis.execution_engine.hedge_safety.asyncio.sleep", new=AsyncMock()) as sleep:
            result = await execute_confirmed_market_order(adapter, symbol="TOKEN", side="BUY",
                quantity=D(10), clip_usd=1000, baseline=D(0))
        self.assertEqual(D(result["avg_price"]), D("101.2"))
        self.assertTrue(all(c.args[0] == 0 for c in sleep.await_args_list))
        self.assertTrue(all("limit_price" not in c.kwargs for c in adapter.place_market_order.await_args_list))

    def test_reprice_both_sides_strictly_inside_threshold(self):
        for side, taker in (("SELL", D("100.2")), ("BUY", D("99.8"))):
            with self.subTest(side=side):
                price = bounded_maker_price(desired=D(100), taker=taker,
                    bid=D(99), ask=D(101), side=side, tick=D("0.01"))
                self.assertLess(adverse_price_gap(price, taker, side), D("0.002"))

    async def test_depth_vwap_not_just_best_quote_triggers_reprice(self):
        book = {"bid": 99, "ask": 100, "asks": [(100, 1), (101, 9)]}
        guard = MakerPriceGuard(fetch_books=AsyncMock(return_value=({}, book)), tick_size=None,
            maker_side="SELL", taker_side="BUY", quantity=D(10))
        with self.assertRaises(MakerPriceRecheck):
            await guard.check("100")

    async def test_stream_guard_uses_cache_without_rest_and_disconnect_cancels(self):
        book = {"bid": 99, "ask": 100, "asks": [(100, 10)], "bids": [(99, 10)]}
        maker = SimpleNamespace(get_price_tick_size=AsyncMock(return_value="0.01"),
            prepare_hedge_quotes=AsyncMock(), hedge_quote=AsyncMock(return_value=book))
        taker = SimpleNamespace(prepare_hedge_quotes=AsyncMock(), hedge_quote=AsyncMock(return_value=book),
            wait_for_hedge_quote=AsyncMock())
        rest = AsyncMock(side_effect=AssertionError("REST on streaming path"))
        guard = await create_maker_guard(maker_adapter=maker, taker_adapter=taker, symbol="TOKEN",
            fetch_books=rest, maker_side="SELL", taker_side="BUY", quantity=D(10))
        await guard.prepare("100")
        await guard.check("100")
        rest.assert_not_awaited()
        taker.hedge_quote.side_effect = RuntimeError("disconnected")
        with self.assertRaises(MakerPriceRecheck):
            await guard.check("100")

    async def test_mexc_market_and_post_only_are_distinct_order_types(self):
        adapter = MexcSpotExecutionAdapter(api_key="unused", api_secret="unused")
        adapter._post_order = AsyncMock(return_value={"orderId": 1, "status": "FILLED",
            "executedQty": "10", "cummulativeQuoteQty": "1000"})
        adapter._get_order = AsyncMock(side_effect=AssertionError("unnecessary query"))
        await adapter.place_market_order(symbol="TOKEN", side="BUY", amount="10", clip_usd=1000)
        self.assertEqual(adapter._post_order.call_args.args[0]["type"], "MARKET")
        self.assertNotIn("price", adapter._post_order.call_args.args[0])
        adapter._get_order.assert_not_awaited()
        await adapter.place_limit_order(symbol="TOKEN", side="SELL", amount="10", clip_usd=1000,
            price="100", post_only=True)
        self.assertEqual(adapter._post_order.call_args.args[0]["type"], "LIMIT_MAKER")
        self.assertNotIn("timeInForce", adapter._post_order.call_args.args[0])
