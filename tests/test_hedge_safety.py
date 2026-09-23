from __future__ import annotations

import unittest
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.hedge_safety import (
    HedgeFailure, execute_confirmed_market_order, terminal_fill_quantity,
)
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from scripts.place_order import (
    ClosePositionPlan, execute_close_position_plan, record_open_execution_from_live_positions,
)


class PositionAdapter:
    def __init__(self, quantity="0"):
        self.quantity = Decimal(quantity)
        self.orders = []

    async def get_open_position(self, **kwargs):
        if self.quantity == 0:
            return None
        return {"side": "LONG" if self.quantity > 0 else "SHORT",
                "quantity": str(abs(self.quantity))}

    async def get_open_position_snapshot(self, **kwargs):
        return {"position": await self.get_open_position(), "portfolio_version": 2}

    async def place_market_order(self, **kwargs):
        self.orders.append(kwargs)
        amount = Decimal(kwargs["amount"])
        self.quantity += amount if kwargs["side"] == "BUY" else -amount
        return {"ok": True, "terminal": True, "filled_quantity": str(amount)}


class HedgeSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sleep_patch = patch("asyncio.sleep", new=AsyncMock())
        self.sleep_patch.start()
        self.addCleanup(self.sleep_patch.stop)

    async def hedge(self, adapter, **kwargs):
        return await execute_confirmed_market_order(
            adapter, symbol="TEST", side="BUY", quantity=Decimal("10000"),
            clip_usd=1000, baseline=Decimal("0"), poll_delay=0,
            confirmation_attempts=3, **kwargs,
        )

    async def test_terminal_partial_fill_retries_only_remaining_quantity(self):
        class Taker(PositionAdapter):
            async def place_market_order(self, **kwargs):
                self.orders.append(kwargs)
                fill = Decimal("5000")
                self.quantity += fill
                return {"ok": True, "terminal": True, "filled_quantity": str(fill)}
        adapter = Taker()
        result = await self.hedge(adapter)
        self.assertEqual([o["amount"] for o in adapter.orders], ["10000", "5000"])
        self.assertEqual(result["filled_quantity"], "10000")

    async def test_timeout_after_full_fill_reconciles_without_duplicate(self):
        class Taker(PositionAdapter):
            async def place_market_order(self, **kwargs):
                await super().place_market_order(**kwargs)
                raise TimeoutError("response lost")
        adapter = Taker()
        self.assertTrue((await self.hedge(adapter))["ok"])
        self.assertEqual(len(adapter.orders), 1)

    async def test_unknown_partial_fill_does_not_retry_or_claim_success(self):
        class Taker(PositionAdapter):
            async def place_market_order(self, **kwargs):
                self.orders.append(kwargs)
                self.quantity = Decimal("5000")
                raise TimeoutError("response lost")
        adapter = Taker()
        with self.assertRaises(HedgeFailure) as raised:
            await self.hedge(adapter)
        self.assertTrue(raised.exception.uncertain)
        self.assertEqual(len(adapter.orders), 1)

    async def test_ack_or_false_ok_cannot_mark_hedge_complete(self):
        for result in ({"ok": True, "order_id": "1"}, {"ok": False}):
            adapter = PositionAdapter()
            adapter.place_market_order = AsyncMock(return_value=result)
            with self.assertRaises(HedgeFailure):
                await self.hedge(adapter)
            self.assertEqual(adapter.place_market_order.await_count, 1)

    async def test_rejection_exhaustion_preserves_confirmed_quantity(self):
        adapter = PositionAdapter()
        adapter.place_market_order = AsyncMock(return_value={
            "ok": False, "terminal": True, "filled_quantity": "0",
        })
        with self.assertRaises(HedgeFailure) as raised:
            await self.hedge(adapter)
        self.assertFalse(raised.exception.uncertain)
        self.assertEqual(raised.exception.confirmed_quantity, 0)
        self.assertEqual(adapter.place_market_order.await_count, 3)

    def test_nonterminal_variational_fill_cannot_authorize_remainder(self):
        self.assertIsNone(terminal_fill_quantity({
            "ok": True, "terminal": False, "status": "FILLED", "filled_quantity": "5",
        }))

    def test_invalid_or_historical_quantity_cannot_authorize_remainder(self):
        for value in ("-5", "NaN", "Infinity", "invalid"):
            self.assertIsNone(terminal_fill_quantity({"terminal": True, "filled_quantity": value}))
        self.assertIsNone(terminal_fill_quantity({
            "status": "FILLED", "raw": {"other_order": {"filled_quantity": "10000"}},
        }))

    async def test_stale_position_after_partial_fill_is_polled_until_updated(self):
        adapter = PositionAdapter()
        adapter.place_market_order = AsyncMock(side_effect=[
            {"ok": True, "terminal": True, "filled_quantity": "5000"},
            TimeoutError("lost response"),
        ])
        adapter.get_open_position = AsyncMock(side_effect=[
            None, {"side": "LONG", "quantity": "10000"},
        ])
        self.assertTrue((await self.hedge(adapter))["ok"])
        self.assertEqual(adapter.place_market_order.await_count, 2)

    async def execute_pair(self, maker, taker, **kwargs):
        return await execute_single_clip_with_sides(
            symbol="TEST", clip_usd=1000, quantity=Decimal("10000"),
            maker_venue="variational", taker_venue="hyperliquid",
            maker_side="SELL", taker_side="BUY", maker_adapter=maker,
            taker_adapter=taker, max_hedge_retries=0,
            state_machine=ExecutionStateMachine(), require_maker_fill_confirmation=True,
            verify_hedge_fill=True, **kwargs,
        )

    def maker(self, *, late_fill=True):
        class Maker(PositionAdapter):
            def __init__(self):
                super().__init__("-25000")
                self.cancelled = False
                self.snapshot_checks = 0

            async def place_limit_order(self, **kwargs):
                self.quantity -= Decimal("5000")
                return {"ok": True, "filled": True, "filled_quantity": "5000"}

            async def cancel_order(self, **kwargs):
                self.cancelled = True
                return {"ok": True, "raw": {"status": "already_absent"}}

            async def get_open_position_snapshot(self, **kwargs):
                self.snapshot_checks += 1
                if late_fill and self.cancelled and self.snapshot_checks == 6:
                    self.quantity -= Decimal("5000")
                return await super().get_open_position_snapshot(**kwargs)
        return Maker()

    async def test_35000_30000_cancel_race_hedges_final_10000(self):
        maker, taker = self.maker(), PositionAdapter("25000")
        result = await self.execute_pair(maker, taker)
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(result["executed_quantity"], "10000")
        self.assertEqual(maker.quantity, -35000)
        self.assertEqual(taker.quantity, 35000)
        self.assertEqual([o["amount"] for o in taker.orders], ["10000"])

    async def test_existing_mismatch_blocks_before_maker_order(self):
        maker = self.maker()
        maker.place_limit_order = AsyncMock()
        with self.assertRaisesRegex(RuntimeError, "imbalance before maker"):
            await self.execute_pair(maker, PositionAdapter("20000"))
        maker.place_limit_order.assert_not_called()

    async def test_lost_maker_response_reconciles_fill_created_during_cleanup(self):
        maker, taker = self.maker(late_fill=False), PositionAdapter("25000")
        maker.place_limit_order = AsyncMock(side_effect=ConnectionError("socket closed after click"))

        async def cancel(**kwargs):
            maker.quantity = Decimal("-30000")
            return {"ok": True}

        maker.cancel_order = AsyncMock(side_effect=cancel)
        result = await self.execute_pair(maker, taker)
        self.assertTrue(result["hedge_verified"])
        self.assertTrue(result["maker_result"]["ok"])
        self.assertEqual(taker.quantity, 30000)
        self.assertEqual(maker.cancel_order.await_count, 1)

    async def test_failed_close_response_still_hedges_fill_during_cleanup(self):
        maker, taker = PositionAdapter("-25000"), PositionAdapter("25000")
        maker.get_limit_price_preview = AsyncMock(return_value="0.1")
        maker.place_limit_order = AsyncMock(return_value={
            "ok": False, "error": "submit verification failed", "requestId": "2z-close",
            "orderId": "maker-2z", "details": {"usedLimitPrice": "0.1"},
        })

        async def cancel(**kwargs):
            self.assertEqual(kwargs["order_result"]["orderId"], "maker-2z")
            self.assertEqual(kwargs["order_result"]["details"]["baselinePositionQty"], "-25000")
            maker.quantity = Decimal("-20000")
            return {"ok": True, "status": "cancelled"}

        maker.cancel_order = AsyncMock(side_effect=cancel)
        result = await execute_close_position_plan(
            plan=ClosePositionPlan(
                symbol="2Z", quantity=Decimal("10000"), maker_venue="variational",
                taker_venue="hyperliquid", maker_price="0.1", clip_usd=1000,
                side_by_venue={"variational": "BUY", "hyperliquid": "SELL"}, spread_by_venue={},
            ),
            adapters={"variational": maker, "hyperliquid": taker},
            symbol="2Z", venues=["variational", "hyperliquid"],
        )
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(maker.quantity, -20000)
        self.assertEqual(taker.quantity, 20000)
        self.assertTrue(maker.place_limit_order.await_args.kwargs["reduce_only"])
        self.assertEqual(taker.orders[0]["side"], "SELL")
        self.assertEqual(taker.orders[0]["amount"], "5000")
        self.assertEqual(maker.orders, [])
        self.assertEqual(maker.cancel_order.await_count, 1)

    async def test_failed_response_preserves_original_baseline_and_rejection(self):
        maker, taker = self.maker(late_fill=False), PositionAdapter("25000")
        response = {
            "ok": False, "error": "Reduce-only order rejected", "requestId": "reject-2z",
            "orderId": "maker-2z", "details": {"baselinePositionQty": "-25000",
                                                     "baselinePortfolioVersion": 17, "usedLimitPrice": "0.1"},
        }
        maker.place_limit_order = AsyncMock(return_value=response)
        maker.cancel_order = AsyncMock(return_value={"ok": True})
        with self.assertRaisesRegex(RuntimeError, "Reduce-only order rejected.*requestId=reject-2z") as raised:
            await self.execute_pair(maker, taker)
        self.assertIs(raised.exception.order_result, response)
        self.assertIs(maker.cancel_order.await_args.kwargs["order_result"], response)
        maker.place_limit_order.assert_awaited_once()
        self.assertEqual(taker.orders, [])

    async def test_returned_timeout_is_repriced_only_after_cancel_and_reconciliation(self):
        maker, taker = self.maker(late_fill=False), PositionAdapter("25000")
        maker.cancel_order = AsyncMock(return_value={"ok": True})
        calls = []

        async def submit(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return {"ok": False, "error": "limit order fill timeout", "orderId": "old-order",
                        "details": {"usedLimitPrice": "0.1"}}
            self.assertEqual(maker.cancel_order.await_count, 1)
            maker.quantity = Decimal("-35000")
            return {"ok": True, "filled": True, "filled_quantity": "10000"}

        maker.place_limit_order = submit
        result = await self.execute_pair(
            maker, taker, max_maker_reprice_attempts=1,
            maker_price_refresher=AsyncMock(return_value="0.11"),
        )
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(maker.cancel_order.await_args.kwargs["order_result"]["orderId"], "old-order")
        self.assertEqual(calls[-1]["price"], "0.11")
        self.assertEqual(taker.quantity, 35000)

    async def test_waiter_failure_preserves_accepted_order_and_cancels_it_once(self):
        maker, taker = self.maker(late_fill=False), PositionAdapter("25000")
        maker.place_limit_order = AsyncMock(return_value={"ok": True, "orderId": "accepted-order"})
        maker.wait_for_order_fill = AsyncMock(side_effect=RuntimeError("fill feed disconnected"))
        maker.cancel_order = AsyncMock(return_value={"ok": True})
        with self.assertRaisesRegex(RuntimeError, "fill feed disconnected") as raised:
            await self.execute_pair(maker, taker)
        self.assertEqual(raised.exception.order_result["orderId"], "accepted-order")
        self.assertEqual(maker.cancel_order.await_count, 1)
        self.assertEqual(taker.orders, [])

    async def test_exhausted_rejection_never_closes_filled_maker(self):
        maker = self.maker(late_fill=False)
        maker.close_position = AsyncMock()
        taker = PositionAdapter("25000")
        taker.place_market_order = AsyncMock(return_value={
            "ok": False, "terminal": True, "filled_quantity": "0",
        })
        with self.assertRaisesRegex(RuntimeError, "confirmed=0 remaining=5000"):
            await self.execute_pair(maker, taker)
        self.assertEqual(maker.quantity, -30000)
        self.assertEqual(maker.orders, [])
        maker.close_position.assert_not_called()
        self.assertEqual(taker.place_market_order.await_count, 3)
        for call in taker.place_market_order.await_args_list:
            self.assertEqual(call.kwargs["side"], "BUY")
            self.assertEqual(call.kwargs["amount"], "5000")
            self.assertNotIn("reduce_only", call.kwargs)

    async def test_ambiguous_hedge_never_closes_maker(self):
        maker = self.maker(late_fill=False)
        maker.close_position = AsyncMock()
        taker = PositionAdapter("25000")
        taker.place_market_order = AsyncMock(side_effect=TimeoutError("lost response"))
        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await self.execute_pair(maker, taker)
        self.assertEqual(maker.orders, [])
        maker.close_position.assert_not_called()
        self.assertEqual(taker.place_market_order.await_count, 1)

    async def test_exhausted_partial_hedge_preserves_all_filled_positions(self):
        maker, taker = self.maker(late_fill=False), PositionAdapter("25000")
        maker.close_position = AsyncMock()

        async def submit(**kwargs):
            taker.orders.append(kwargs)
            fill = Decimal("2000") if len(taker.orders) == 1 else Decimal("0")
            taker.quantity += fill
            return {"ok": fill > 0, "terminal": True, "filled_quantity": str(fill)}

        taker.place_market_order = submit
        with self.assertRaisesRegex(RuntimeError, "confirmed=2000 remaining=3000"):
            await self.execute_pair(maker, taker)
        self.assertEqual(maker.quantity, -30000)
        self.assertEqual(taker.quantity, 27000)
        self.assertEqual(maker.orders, [])
        maker.close_position.assert_not_called()
        self.assertEqual([order["amount"] for order in taker.orders], ["5000", "3000", "3000"])

    async def test_partial_fill_then_rejection_completes_on_original_hedge_side(self):
        maker, taker = self.maker(late_fill=False), PositionAdapter("25000")
        maker.close_position = AsyncMock()
        fills = iter([Decimal("2000"), Decimal("0"), Decimal("3000")])

        async def submit(**kwargs):
            taker.orders.append(kwargs)
            fill = next(fills)
            taker.quantity += fill
            return {"ok": fill > 0, "terminal": True, "filled_quantity": str(fill)}

        taker.place_market_order = submit
        result = await self.execute_pair(maker, taker)
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(maker.quantity, -30000)
        self.assertEqual(taker.quantity, 30000)
        self.assertEqual([order["amount"] for order in taker.orders], ["5000", "3000", "3000"])
        self.assertTrue(all(order["side"] == "BUY" and not order.get("reduce_only") for order in taker.orders))
        self.assertEqual(maker.orders, [])
        maker.close_position.assert_not_called()

    async def test_preflight_failure_prevents_maker_submission(self):
        maker, taker = self.maker(), PositionAdapter("25000")
        maker.place_limit_order = AsyncMock()
        taker.ensure_isolated_margin = AsyncMock(side_effect=RuntimeError("margin unavailable"))
        with self.assertRaisesRegex(RuntimeError, "margin unavailable"):
            await self.execute_pair(maker, taker)
        maker.place_limit_order.assert_not_called()

    async def test_live_position_confirmation_catches_false_full_fill(self):
        maker = self.maker(late_fill=False)
        taker = PositionAdapter("25000")
        taker.place_market_order = AsyncMock(return_value={
            "ok": True, "terminal": True, "filled_quantity": "5000",
        })
        with self.assertRaisesRegex(RuntimeError, "live position confirmation failed"):
            await self.execute_pair(maker, taker)
        self.assertEqual(taker.place_market_order.await_count, 1)

    async def test_variational_taker_requires_actual_position_change(self):
        maker = PositionAdapter("-25000")

        async def place_limit(**kwargs):
            maker.quantity -= Decimal("10000")
            return {"ok": True, "terminal": True, "filled": True, "filled_quantity": "10000"}

        maker.place_limit_order = place_limit
        taker = PositionAdapter("25000")

        async def place_market(**kwargs):
            await PositionAdapter.place_market_order(taker, **kwargs)
            return {"ok": True, "terminal": False, "filled": True, "status": "FILLED"}

        taker.place_market_order = place_market
        result = await execute_single_clip_with_sides(
            symbol="TEST", clip_usd=1000, quantity=Decimal("10000"),
            maker_venue="hyperliquid", taker_venue="variational",
            maker_side="SELL", taker_side="BUY", maker_adapter=maker,
            taker_adapter=taker, max_hedge_retries=0,
            state_machine=ExecutionStateMachine(), require_maker_fill_confirmation=True,
            verify_hedge_fill=True,
        )
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(taker.quantity, 35000)

    async def test_verified_registry_retries_stale_nonzero_position(self):
        maker, taker = PositionAdapter("-35000"), PositionAdapter("35000")
        taker.get_open_position = AsyncMock(side_effect=[
            {"side": "LONG", "quantity": "30000"},
            {"side": "LONG", "quantity": "35000"},
        ])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            await record_open_execution_from_live_positions(
                execution_result={"ok": True, "executed_quantity": "5000", "hedge_verified": True,
                                  "maker_result": {"ok": True}, "hedge_result": {"ok": True}},
                adapters_by_venue={"variational": maker, "hyperliquid": taker},
                symbol="TEST", short_venue="variational", long_venue="hyperliquid", registry_path=path,
            )
            self.assertTrue(path.exists())
        self.assertEqual(taker.get_open_position.await_count, 2)

    async def test_verified_registry_never_synthesizes_missing_variational_leg(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            with self.assertRaisesRegex(RuntimeError, "no live perp position"):
                await record_open_execution_from_live_positions(
                    execution_result={"ok": True, "executed_quantity": "5000", "hedge_verified": True},
                    adapters_by_venue={"variational": PositionAdapter(), "hyperliquid": PositionAdapter("5000")},
                    symbol="TEST", short_venue="variational", long_venue="hyperliquid", registry_path=path,
                )
            self.assertFalse(path.exists())


class HyperliquidFillTests(unittest.IsolatedAsyncioTestCase):
    async def test_ioc_exposes_actual_fill_size(self):
        adapter = object.__new__(HyperliquidExecutionAdapter)
        adapter.slippage_bps = 50
        adapter.ensure_isolated_margin = AsyncMock(return_value=0)
        adapter._get_mid_price = AsyncMock(return_value=0.1)
        adapter._post_order = AsyncMock(return_value={
            "response": {"data": {"statuses": [{"filled": {
                "oid": 1, "totalSz": "5000", "avgPx": "0.1",
            }}]}},
        })
        result = await adapter.place_market_order(symbol="TEST", side="BUY", amount="10000", clip_usd=1000)
        self.assertTrue(result["terminal"])
        self.assertEqual(result["filled_quantity"], "5000")
        adapter._post_order.return_value = {"response": {"data": {"statuses": [{"error": "rejected"}]}}}
        with self.assertRaises(RuntimeError) as raised:
            await adapter.place_market_order(symbol="TEST", side="BUY", amount="10000", clip_usd=1000)
        self.assertEqual(terminal_fill_quantity(raised.exception.order_result), 0)


if __name__ == "__main__":
    unittest.main()
