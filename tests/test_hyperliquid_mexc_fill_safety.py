"""Order-terminal evidence must precede replacement and residual hedging."""
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.hedge_safety import execute_confirmed_market_order, HedgeFailure, terminal_fill_quantity
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine


def hl_status(status, remaining, original="10"):
    return {"status": "order", "order": {"status": status, "order": {
        "oid": 123, "coin": "TEST", "origSz": original, "sz": remaining,
    }}}


class FillSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        sleep = patch("asyncio.sleep", new=AsyncMock())
        sleep.start()
        self.addCleanup(sleep.stop)

    def hyperliquid(self, statuses):
        adapter = object.__new__(HyperliquidExecutionAdapter)
        adapter._get_order_status = AsyncMock(side_effect=statuses)
        adapter._get_asset_index = AsyncMock(return_value=1)
        adapter._post_order = AsyncMock(return_value={"status": "ok", "response": {"data": {"statuses": ["success"]}}})
        return adapter

    def mexc(self, statuses):
        adapter = MexcExecutionAdapter(api_key="test", api_secret="test")
        adapter._get_order_status = AsyncMock(side_effect=statuses)
        adapter._post_cancel = AsyncMock(return_value={"success": True, "data": [{"errorCode": 0}]})
        return adapter

    async def test_hyperliquid_cancel_waits_and_includes_late_fill(self):
        adapter = self.hyperliquid([hl_status("open", "8"), hl_status("canceled", "6")])
        result = await adapter.cancel_order(order_result={"order_id": 123}, symbol="TEST", side="BUY", amount="10")
        self.assertEqual(terminal_fill_quantity(result), 4)
        self.assertEqual(adapter._get_order_status.await_count, 2)

    async def test_hyperliquid_cancel_error_can_race_full_fill(self):
        adapter = self.hyperliquid([hl_status("filled", "0")])
        adapter._post_order.side_effect = RuntimeError("already filled")
        result = await adapter.cancel_order(order_result={"order_id": 123}, symbol="TEST", side="BUY", amount="10")
        self.assertEqual(terminal_fill_quantity(result), 10)

    async def test_hyperliquid_terminal_zero_is_not_original_quantity(self):
        for status in ("canceled", "marginCanceled", "scheduledCancel", "iocCancelRejected"):
            adapter = self.hyperliquid([hl_status(status, "10")])
            result = await adapter.get_order_execution(order_result={"order_id": 123}, symbol="TEST")
            self.assertEqual(terminal_fill_quantity(result), 0)

    async def test_hyperliquid_unknown_or_incomplete_status_cannot_authorize_replacement(self):
        for data in ({"status": "unknownOid"}, {"status": "order", "order": {"status": "filled"}}):
            adapter = self.hyperliquid([data])
            result = await adapter.get_order_execution(order_result={"order_id": 123}, symbol="TEST")
            self.assertIsNone(terminal_fill_quantity(result))

    async def test_hyperliquid_partial_fill_wait_exposes_actual_quantity(self):
        adapter = self.hyperliquid([hl_status("open", "6")])
        result = await adapter.wait_for_order_fill(order_result={"order_id": 123}, symbol="TEST",
                                                  side="BUY", amount="10", timeout_seconds=0,
                                                  allow_partial_fill=True)
        self.assertEqual(Decimal(result["filled_quantity"]), 4)
        self.assertIsNone(terminal_fill_quantity(result))

    async def test_mexc_terminal_state_mapping_requires_actual_deal_volume(self):
        for state in (3, 4, 5):
            adapter = self.mexc([{"state": state, "dealVol": "4", "vol": "10"}])
            result = await adapter.get_order_execution(order_result={"order_id": 123}, symbol="TEST")
            self.assertEqual(terminal_fill_quantity(result), 4)
        adapter = self.mexc([{"state": 4, "vol": "10"}])
        with self.assertRaisesRegex(RuntimeError, "cumulative fill unavailable"):
            await adapter.get_order_execution(order_result={"order_id": 123}, symbol="TEST")

    async def test_mexc_cancel_waits_for_terminal_cumulative_fill(self):
        adapter = self.mexc([{"state": 2, "dealVol": "2"}, {"state": 4, "dealVol": "4"}])
        result = await adapter.cancel_order(order_result={"order_id": 123}, symbol="TEST", side="BUY", amount="10")
        self.assertEqual(terminal_fill_quantity(result), 4)
        self.assertEqual(adapter._get_order_status.await_count, 2)

    async def test_mexc_partial_hedge_queries_status_and_sends_only_remaining_six(self):
        adapter = self.mexc([{"state": 2, "dealVol": "4"}, {"state": 4, "dealVol": "4"},
                             {"state": 3, "dealVol": "6"}])
        adapter._post_order = AsyncMock(side_effect=[{"data": 123}, {"data": 124}])
        adapter.get_open_position = AsyncMock(return_value={"side": "LONG", "quantity": "4"})
        result = await execute_confirmed_market_order(adapter, symbol="TEST", side="BUY", quantity=Decimal("10"),
                                                      baseline=Decimal("0"), clip_usd=100, poll_delay=0)
        self.assertEqual([c.args[0]["vol"] for c in adapter._post_order.await_args_list], [10, 6])
        self.assertEqual(adapter._get_order_status.await_count, 3)
        self.assertEqual(result["filled_quantity"], "10")

    async def test_mexc_live_partial_must_not_be_resubmitted(self):
        adapter = self.mexc([])
        adapter._get_order_status = AsyncMock(return_value={"state": 2, "dealVol": "4"})
        adapter._post_order = AsyncMock(return_value={"data": 123})
        adapter.get_open_position = AsyncMock(return_value={"side": "LONG", "quantity": "4"})
        with self.assertRaises(HedgeFailure):
            await execute_confirmed_market_order(adapter, symbol="TEST", side="BUY", quantity=Decimal("10"),
                                                 baseline=Decimal("0"), clip_usd=100, confirmation_attempts=2)
        adapter._post_order.assert_awaited_once()

    async def execute_cancel_race(self, venue, *, final_fill="4", exhausted=False):
        maker = (self.hyperliquid([hl_status("canceled", str(10 - Decimal(final_fill)))]) if venue == "hyperliquid"
                 else self.mexc([{"state": 4, "dealVol": final_fill}]))
        maker.place_limit_order = AsyncMock(return_value={"ok": True, "order_id": 123})
        maker.wait_for_order_fill = AsyncMock(side_effect=RuntimeError("maker fill timeout"))
        maker.get_open_position = AsyncMock(side_effect=[None, {"side": "LONG", "quantity": final_fill}])
        taker = self.mexc([{"state": 3, "dealVol": final_fill}])
        taker._post_order = AsyncMock(return_value={"data": 456})
        taker.get_open_position = AsyncMock(side_effect=[None, {"side": "SHORT", "quantity": final_fill}])
        result = await execute_single_clip_with_sides(
            symbol="TEST", quantity=Decimal("10"), clip_usd=100, maker_venue=venue, taker_venue="mexc",
            maker_side="BUY", taker_side="SELL", maker_adapter=maker, taker_adapter=taker,
            max_hedge_retries=0, state_machine=ExecutionStateMachine(), require_maker_fill_confirmation=True,
            max_maker_reprice_attempts=0 if exhausted else 1, verify_hedge_fill=True,
        )
        maker.place_limit_order.assert_awaited_once()
        self.assertEqual(result["executed_quantity"], final_fill)
        self.assertEqual(taker._post_order.call_args.args[0]["vol"], float(final_fill))

    async def test_hyperliquid_cancel_race_hedges_four_without_replacing_ten(self):
        await self.execute_cancel_race("hyperliquid")

    async def test_mexc_cancel_race_hedges_four_without_replacing_ten(self):
        await self.execute_cancel_race("mexc")

    async def test_exhausted_maker_wait_still_hedges_final_fill(self):
        for venue in ("hyperliquid", "mexc"):
            await self.execute_cancel_race(venue, exhausted=True)

    async def test_nonterminal_cancel_ack_never_permits_replacement(self):
        for venue in ("hyperliquid", "mexc"):
            maker = self.hyperliquid([]) if venue == "hyperliquid" else self.mexc([])
            maker._get_order_status = AsyncMock(return_value=(
                hl_status("open", "6") if venue == "hyperliquid" else {"state": 2, "dealVol": "4"}
            ))
            with self.assertRaisesRegex(RuntimeError, "refusing replacement"):
                await maker.cancel_order(order_result={"order_id": 123}, symbol="TEST", side="BUY", amount="10")

    async def test_zero_fill_cancel_allows_one_replacement(self):
        maker = self.hyperliquid([hl_status("canceled", "10")])
        maker.place_limit_order = AsyncMock(return_value={"ok": True, "order_id": 123})
        maker.wait_for_order_fill = AsyncMock(side_effect=[RuntimeError("maker fill timeout"),
            {"ok": True, "status": "FILLED", "filled_quantity": "10"}])
        maker.get_open_position = AsyncMock(side_effect=[None, {"side": "LONG", "quantity": "10"}])
        taker = self.mexc([{"state": 3, "dealVol": "10"}])
        taker._post_order = AsyncMock(return_value={"data": 456})
        taker.get_open_position = AsyncMock(side_effect=[None, {"side": "SHORT", "quantity": "10"}])
        result = await execute_single_clip_with_sides(
            symbol="TEST", quantity=Decimal("10"), clip_usd=100, maker_venue="hyperliquid", taker_venue="mexc",
            maker_side="BUY", taker_side="SELL", maker_adapter=maker, taker_adapter=taker,
            max_hedge_retries=0, state_machine=ExecutionStateMachine(), require_maker_fill_confirmation=True,
            max_maker_reprice_attempts=1, verify_hedge_fill=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(maker.place_limit_order.await_count, 2)
        taker._post_order.assert_awaited_once()

    async def test_actual_overfill_is_hedged_but_never_reported_as_success(self):
        maker = self.hyperliquid([])
        maker.place_limit_order = AsyncMock(return_value={
            "ok": True, "status": "FILLED", "filled_quantity": "14", "order_id": 123,
        })
        maker.get_open_position = AsyncMock(side_effect=[None, {"side": "LONG", "quantity": "14"}])
        maker.close_position = AsyncMock()
        taker = self.mexc([{"state": 3, "dealVol": "14"}])
        taker._post_order = AsyncMock(return_value={"data": 456})
        taker.get_open_position = AsyncMock(side_effect=[None, {"side": "SHORT", "quantity": "14"}])
        state = ExecutionStateMachine()
        with self.assertRaisesRegex(RuntimeError, "maker exceeded requested quantity"):
            await execute_single_clip_with_sides(
                symbol="TEST", quantity=Decimal("10"), clip_usd=100, maker_venue="hyperliquid", taker_venue="mexc",
                maker_side="BUY", taker_side="SELL", maker_adapter=maker, taker_adapter=taker,
                max_hedge_retries=0, state_machine=state, require_maker_fill_confirmation=True,
                verify_hedge_fill=True,
            )
        self.assertNotEqual(state.state, "completed")
        taker._post_order.assert_awaited_once()
        self.assertEqual(taker._post_order.call_args.args[0]["vol"], 14)
        maker.close_position.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
