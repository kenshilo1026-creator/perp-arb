"""Regression coverage for partial close fills, cancel races and hedge remainders."""
import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.lighter_adapter import LighterExecutionAdapter
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.hedge_safety import terminal_fill_quantity
from hydra_basis.execution_engine.order_fill import extract_filled_quantity, poll_until_filled
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from scripts.place_order import ClosePositionPlan, execute_close_position_plan


class Signer:
    ORDER_TYPE_LIMIT = 0
    ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
    ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
    DEFAULT_IOC_EXPIRY = 0

    def __init__(self):
        self.create_order = AsyncMock(return_value=(None, "tx", None))
        self.cancel_order = AsyncMock(return_value=(None, "cancel-tx", None))

    def create_auth_token_with_expiry(self, expiry):
        return "test-token", None


def lighter():
    return LighterExecutionAdapter(
        signer_client_factory=Signer, market_config_loader=lambda symbol: (219, 10, 1000000),
        orderbook_loader=lambda symbol: {"bid": 0.169, "ask": 0.17},
    )


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        patcher = patch("asyncio.sleep", new=AsyncMock())
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_rest_fill_is_already_in_token_units(self):
        adapter = lighter()
        for fill in ("210.0", "2000.0", "0.0"):
            order = {"client_order_index": 123, "market_index": 219,
                     "status": "canceled", "filled_base_amount": fill}
            with patch("hydra_basis.execution_engine.lighter_adapter.fetch_json",
                       new=AsyncMock(return_value={"orders": [order]})) as fetch:
                result = await adapter._get_order_status(123, 219, 20000, 10)
            self.assertEqual(terminal_fill_quantity(result), Decimal(fill))
            self.assertTrue(fetch.call_args.args[2].endswith("/accountOrders"))
            self.assertEqual(fetch.call_args.kwargs["params"]["client_order_indexes"], "123")

    async def test_rest_remaining_is_subtracted_without_scaling_twice(self):
        order = {"client_order_index": 123, "market_index": 219, "status": "open",
                 "initial_base_amount": "2000.0", "remaining_base_amount": "1790.0"}
        with patch("hydra_basis.execution_engine.lighter_adapter.fetch_json",
                   new=AsyncMock(return_value={"orders": [order]})):
            result = await lighter()._get_order_status(123, 219, 20000, 10)
        self.assertEqual(Decimal(result["filled_quantity"]), 210)
        self.assertIsNone(terminal_fill_quantity(result))
        self.assertIsNone(extract_filled_quantity({"remainingBaseAmount": "1790"}))

    async def test_absent_or_wrong_order_never_becomes_filled(self):
        for orders in ([], [{"client_order_index": 124, "market_index": 219}],
                       [{"client_order_index": 123, "market_index": 220}]):
            with patch("hydra_basis.execution_engine.lighter_adapter.fetch_json",
                       new=AsyncMock(return_value={"orders": orders})):
                result = await lighter()._get_order_status(123, 219, 20000, 10)
            self.assertEqual(result["status"], "UNKNOWN")
            with self.assertRaisesRegex(RuntimeError, "timeout"):
                await poll_until_filled(fetch_status=AsyncMock(return_value=result),
                                        timeout_seconds=0, poll_interval_seconds=0,
                                        timeout_message="timeout", return_on_partial_fill=True)

    async def test_lighter_cancel_waits_for_final_fill_including_race(self):
        adapter = lighter()
        adapter.get_order_execution = AsyncMock(side_effect=[
            {"status": "OPEN", "filled_quantity": "210"},
            {"status": "OPEN", "filled_quantity": "300"},
            {"status": "CANCELED", "filled_quantity": "350"},
        ])
        result = await adapter.cancel_order(order_result={"client_order_index": 123, "market_index": 219},
                                            symbol="ANSEM", side="BUY", amount="2000")
        self.assertEqual(terminal_fill_quantity(result), 350)
        self.assertEqual(adapter.get_order_execution.await_count, 3)
        adapter.client.cancel_order.assert_awaited_once_with(market_index=219, order_index=123)

    async def test_unconfirmed_cancel_cannot_authorize_replacement(self):
        adapter = lighter()
        adapter.get_order_execution = AsyncMock(return_value={"status": "OPEN", "filled_quantity": "210"})
        with self.assertRaisesRegex(RuntimeError, "refusing replacement"):
            await adapter.cancel_order(order_result={"client_order_index": 123, "market_index": 219},
                                       symbol="ANSEM", side="BUY", amount="2000")

    async def test_lighter_hedge_is_ioc_and_close_is_reduce_only(self):
        adapter = lighter()
        await adapter.place_market_order(symbol="ANSEM", side="BUY", amount="2000", clip_usd=340,
                                         reduce_only=True)
        request = adapter.client.create_order.call_args.kwargs
        self.assertEqual(request["time_in_force"], Signer.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL)
        self.assertEqual(request["order_expiry"], 0)
        self.assertTrue(request["reduce_only"])
        self.assertEqual(request["base_amount"], 20000)
        await adapter.place_limit_order(symbol="ANSEM", side="BUY", amount="2000", clip_usd=340,
                                        price="0.169", reduce_only=True)
        self.assertEqual(adapter.client.create_order.call_args.kwargs["time_in_force"], 1)
        self.assertTrue(adapter.client.create_order.call_args.kwargs["reduce_only"])

    async def test_lighter_lost_response_keeps_identity_for_lookup(self):
        adapter = lighter()
        adapter._get_client().create_order.side_effect = TimeoutError("lost response")
        with self.assertRaises(TimeoutError) as raised:
            await adapter.place_market_order(symbol="ANSEM", side="BUY", amount="2000", clip_usd=340)
        self.assertEqual(raised.exception.order_result["market_index"], 219)
        self.assertIn("client_order_index", raised.exception.order_result)

    async def test_aster_close_passes_reduce_only_for_both_order_types(self):
        adapter = AsterExecutionAdapter(signer_address="unused", private_key="unused", user_address="unused")
        adapter._resolve_raw_symbol = AsyncMock(return_value="ANSEMUSDT")
        adapter._format_quantity = AsyncMock(return_value="2000")
        adapter.ensure_isolated_margin = AsyncMock()
        adapter.ensure_leverage = AsyncMock()
        adapter._post_order = AsyncMock(return_value={"orderId": 123})
        await adapter.place_limit_order(symbol="ANSEM", side="SELL", amount="2000", clip_usd=340,
                                        price="0.17", reduce_only=True)
        await adapter.place_market_order(symbol="ANSEM", side="SELL", amount="2000", clip_usd=340,
                                         reduce_only=True)
        for call in adapter._post_order.await_args_list:
            self.assertEqual(call.args[0]["reduceOnly"], "true")

    async def test_aster_unknown_order_must_still_confirm_terminal(self):
        adapter = AsterExecutionAdapter(signer_address="unused", private_key="unused", user_address="unused")
        adapter._resolve_raw_symbol = AsyncMock(return_value="ANSEMUSDT")
        adapter.build_signed_params = lambda params: params
        adapter._delete_signed_query = AsyncMock(side_effect=RuntimeError("-2011 Unknown order"))
        adapter._get_order_status = AsyncMock(side_effect=[
            {"status": "PARTIALLY_FILLED", "executedQty": "210"},
            {"status": "FILLED", "executedQty": "2000"},
        ])
        result = await adapter.cancel_order(order_result={"order_id": 123}, symbol="ANSEM",
                                            side="SELL", amount="2000")
        self.assertEqual(terminal_fill_quantity(result), 2000)
        self.assertEqual(adapter._get_order_status.await_count, 2)


class Leg:
    def __init__(self, position):
        self.position = Decimal(position)
        self.orders = []
        self.statuses = {}

    async def get_open_position(self, **kwargs):
        if self.position == 0:
            return None
        return {"side": "LONG" if self.position > 0 else "SHORT", "quantity": str(abs(self.position))}

    async def place_market_order(self, **kwargs):
        self.orders.append(kwargs)
        amount = Decimal(kwargs["amount"])
        # First IOC partially fills; the second fills exactly the remainder.
        fill = min(amount, Decimal("210")) if len(self.orders) == 1 else amount
        self.position += fill if kwargs["side"] == "BUY" else -fill
        self.statuses[len(self.orders)] = {"status": "CANCELED", "filled_quantity": str(fill)}
        return {"ok": True, "order_id": len(self.orders)}

    async def get_order_execution(self, *, order_result, **kwargs):
        return self.statuses[order_result["order_id"]]


class CloseFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        patcher = patch("asyncio.sleep", new=AsyncMock())
        patcher.start()
        self.addCleanup(patcher.stop)

    async def run_pair(self, maker, taker, **overrides):
        kwargs = dict(symbol="ANSEM", clip_usd=340, quantity=Decimal("2000"), maker_venue="aster",
                      taker_venue="lighter", maker_side="SELL", taker_side="BUY", maker_adapter=maker,
                      taker_adapter=taker, max_hedge_retries=0, state_machine=ExecutionStateMachine(),
                      require_maker_fill_confirmation=True, maker_fill_timeout_seconds=0,
                      max_maker_reprice_attempts=3, maker_reduce_only=True, taker_reduce_only=True,
                      verify_hedge_fill=True)
        kwargs.update(overrides)
        return await execute_single_clip_with_sides(**kwargs)

    def maker(self, fill="2000", *, timeouts=0, cancel_fill=None):
        maker = Leg("10000")
        maker.place_limit_order = AsyncMock(side_effect=lambda **kw: {"ok": True, "order_id": maker.place_limit_order.call_count})
        waits = 0

        async def wait(**kwargs):
            nonlocal waits
            waits += 1
            if waits <= timeouts:
                raise RuntimeError("maker fill timeout")
            maker.position -= Decimal(fill)
            return {"ok": True, "raw": {"status": "FILLED" if fill == "2000" else "PARTIALLY_FILLED",
                                        "executedQty": fill}}

        async def cancel(**kwargs):
            if waits <= timeouts:
                final = Decimal(cancel_fill or "0")
                maker.position -= final
            else:
                final = Decimal(fill)
            return {"ok": True, "raw": {"status": "CANCELED", "executedQty": str(final)}}

        maker.wait_for_order_fill = wait
        maker.cancel_order = AsyncMock(side_effect=cancel)
        return maker

    async def test_partial_hedge_queries_final_order_and_only_submits_remainder(self):
        maker, taker = self.maker(), Leg("-10000")
        result = await self.run_pair(maker, taker)
        self.assertTrue(result["hedge_verified"])
        self.assertEqual([o["amount"] for o in taker.orders], ["2000", "1790"])
        self.assertTrue(all(o["reduce_only"] and o["side"] == "BUY" for o in taker.orders))
        self.assertEqual(maker.position, -taker.position)

    async def test_timeout_cancel_with_fill_hedges_instead_of_replacing(self):
        maker, taker = self.maker(timeouts=1, cancel_fill="350"), Leg("-10000")
        result = await self.run_pair(maker, taker)
        self.assertEqual(result["executed_quantity"], "350")
        self.assertEqual(maker.place_limit_order.await_count, 1)
        self.assertEqual([o["amount"] for o in taker.orders], ["350", "140"])
        self.assertEqual(maker.position, -taker.position)

    async def test_replacement_partial_fill_cancels_the_replacement_too(self):
        maker, taker = self.maker(fill="210", timeouts=1), Leg("-10000")
        result = await self.run_pair(maker, taker)
        self.assertEqual(result["executed_quantity"], "210")
        self.assertEqual([c.kwargs["order_result"]["order_id"] for c in maker.cancel_order.await_args_list], [1, 2])
        self.assertEqual(maker.position, -taker.position)

    async def test_exhausted_maker_wait_still_hedges_cancel_fill(self):
        maker, taker = self.maker(timeouts=1, cancel_fill="350"), Leg("-10000")
        result = await self.run_pair(maker, taker, max_maker_reprice_attempts=0)
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(result["executed_quantity"], "350")
        self.assertEqual(maker.position, -taker.position)

    async def test_interrupt_cancels_then_hedges_confirmed_fill_before_exit(self):
        maker, taker = self.maker(), Leg("-10000")
        maker.wait_for_order_fill = AsyncMock(side_effect=asyncio.CancelledError())

        async def cancel(**kwargs):
            maker.position -= Decimal("350")
            return {"ok": True, "raw": {"status": "CANCELED", "executedQty": "350"}}

        maker.cancel_order = AsyncMock(side_effect=cancel)
        with self.assertRaises(asyncio.CancelledError):
            await self.run_pair(maker, taker)
        self.assertEqual(maker.place_limit_order.await_count, 1)
        self.assertEqual([o["amount"] for o in taker.orders], ["350", "140"])
        self.assertEqual(maker.position, -taker.position)

    async def test_lighter_maker_aster_taker_also_verifies_partial_hedges(self):
        maker, taker = Leg("-10000"), Leg("10000")
        maker.place_limit_order = AsyncMock(return_value={"ok": True, "client_order_index": 123})

        async def wait(**kwargs):
            maker.position += 2000
            return {"ok": True, "raw": {"status": "FILLED", "filled_quantity": "2000"}}

        maker.wait_for_order_fill = wait
        result = await self.run_pair(maker, taker, maker_venue="lighter", taker_venue="aster",
                                     maker_side="BUY", taker_side="SELL")
        self.assertTrue(result["hedge_verified"])
        self.assertEqual([o["amount"] for o in taker.orders], ["2000", "1790"])
        self.assertTrue(all(o["reduce_only"] and o["side"] == "SELL" for o in taker.orders))
        self.assertEqual(maker.position, -taker.position)

    async def test_unknown_hedge_never_claims_batch_complete_or_resubmits(self):
        maker, taker = self.maker(), Leg("-10000")
        taker.get_order_execution = AsyncMock(return_value={"status": "UNKNOWN"})
        state = ExecutionStateMachine()
        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await self.run_pair(maker, taker, state_machine=state)
        self.assertNotEqual(state.state, "completed")
        self.assertEqual(len(taker.orders), 1)

    async def test_terminal_hedge_failure_retries_only_missing_side_without_reversing_maker(self):
        maker, taker = self.maker(), Leg("-10000")
        maker.close_position = AsyncMock()

        async def query(*, order_result, **kwargs):
            return {"status": "CANCELED", "filled_quantity": "210" if order_result["order_id"] == 1 else "0"}

        async def submit(**kwargs):
            taker.orders.append(kwargs)
            if len(taker.orders) == 1:
                taker.position += 210
            return {"ok": True, "order_id": len(taker.orders)}

        taker.get_order_execution, taker.place_market_order = query, submit
        with self.assertRaisesRegex(RuntimeError, "confirmed=210 remaining=1790"):
            await self.run_pair(maker, taker)
        self.assertEqual([o["amount"] for o in taker.orders], ["2000", "1790", "1790"])
        self.assertEqual(maker.place_limit_order.await_count, 1)
        maker.close_position.assert_not_awaited()

    async def test_close_direction_is_checked_before_maker_dispatch(self):
        maker, taker = self.maker(), Leg("-10000")
        with self.assertRaisesRegex(RuntimeError, "close quantity/direction"):
            await self.run_pair(maker, taker, maker_side="BUY", taker_side="SELL")
        maker.place_limit_order.assert_not_awaited()

    async def test_existing_imbalance_blocks_new_maker(self):
        maker, taker = self.maker(), Leg("-8000")
        with self.assertRaisesRegex(RuntimeError, "existing position imbalance"):
            await self.run_pair(maker, taker)
        maker.place_limit_order.assert_not_awaited()

    async def test_close_plan_enables_verification_and_reduce_only_in_either_direction(self):
        for maker_venue, taker_venue in (("aster", "lighter"), ("lighter", "aster")):
            plan = ClosePositionPlan(symbol="ANSEM", quantity=Decimal("2000"), maker_venue=maker_venue,
                                     taker_venue=taker_venue, maker_price="0.17", clip_usd=340,
                                     side_by_venue={"aster": "SELL", "lighter": "BUY"}, spread_by_venue={})
            with patch("scripts.place_order.execute_single_clip_with_sides", new=AsyncMock()) as execute:
                await execute_close_position_plan(plan=plan, adapters={"aster": object(), "lighter": object()},
                                                  symbol="ANSEM", venues=["aster", "lighter"])
            for key in ("maker_reduce_only", "taker_reduce_only", "verify_hedge_fill"):
                self.assertTrue(execute.call_args.kwargs[key])


if __name__ == "__main__":
    unittest.main()
