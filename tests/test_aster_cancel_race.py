"""Tests for the Aster cancel race: -2011 "Unknown order sent." during maker cleanup.

Kept separate from test_execution_engine.py because that module still imports the
removed scripts.run_execution_once (renamed to place_order.py in 148d7ab) and
cannot be collected until it is repaired.
"""
from __future__ import annotations

import unittest
from decimal import Decimal

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine


class AsterCancelOrderTests(unittest.IsolatedAsyncioTestCase):
    def _build_adapter(self, *, delete_error: str | None = None, status_payload: dict | None = None):
        class Adapter(AsterExecutionAdapter):
            def __init__(self) -> None:
                super().__init__(signer_address="0x1", private_key="unused", user_address="0x2")
                self.delete_calls = 0
                self.status_calls = 0

            def build_signed_params(self, params: dict) -> dict:
                return params

            async def _resolve_raw_symbol(self, symbol: str) -> str:
                return "BTWUSDT"

            async def _delete_signed_query(self, url: str, params: dict) -> dict:
                self.delete_calls += 1
                if delete_error is not None:
                    raise RuntimeError(delete_error)
                return {"orderId": params["orderId"], "status": "CANCELED", "executedQty": "0"}

            async def _get_order_status(self, *, symbol: str, order_id: object) -> dict:
                self.status_calls += 1
                if status_payload is None:
                    raise AssertionError("order status should not be queried")
                return status_payload

        return Adapter()

    async def test_cancel_order_treats_unknown_order_as_already_gone(self) -> None:
        adapter = self._build_adapter(
            delete_error="aster cancel 400: {'code': -2011, 'msg': 'Unknown order sent.'}",
            status_payload={"orderId": "9", "status": "FILLED", "executedQty": "3623"},
        )

        result = await adapter.cancel_order(
            order_result={"ok": True, "order_id": "9"},
            symbol="BTW",
            side="SELL",
            amount="3623",
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["already_gone"])
        self.assertEqual(result["raw"]["status"], "FILLED")
        self.assertEqual(result["raw"]["executedQty"], "3623")
        self.assertEqual(adapter.status_calls, 1)

    async def test_cancel_order_reraises_other_cancel_errors(self) -> None:
        adapter = self._build_adapter(
            delete_error=(
                "aster cancel 400: {'code': -1021, "
                "'msg': 'Timestamp for this request is outside of the recvWindow.'}"
            ),
        )

        with self.assertRaisesRegex(RuntimeError, "-1021"):
            await adapter.cancel_order(
                order_result={"ok": True, "order_id": "9"},
                symbol="BTW",
                side="SELL",
                amount="3623",
            )

        self.assertEqual(adapter.status_calls, 0)

    async def test_cancel_order_returns_ok_when_cancel_succeeds(self) -> None:
        adapter = self._build_adapter()

        result = await adapter.cancel_order(
            order_result={"ok": True, "order_id": "9"},
            symbol="BTW",
            side="SELL",
            amount="3623",
        )

        self.assertTrue(result["ok"])
        self.assertNotIn("already_gone", result)
        self.assertEqual(adapter.status_calls, 0)


class ExecutorCancelRaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_execute_single_clip_hedges_full_quantity_when_cancel_finds_order_already_filled(self) -> None:
        taker_amounts: list[str] = []

        class MakerAdapter:
            def __init__(self) -> None:
                self.cancel_calls = 0

            async def place_limit_order(self, **kwargs):
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                return {
                    "ok": True,
                    "order_id": "maker-1",
                    "raw": {"status": "PARTIALLY_FILLED", "executedQty": "4"},
                }

            async def cancel_order(self, **kwargs):
                # Remainder filled between the fill check and the cancel; the
                # adapter resolves the -2011 race and reports the terminal fill.
                self.cancel_calls += 1
                return {
                    "ok": True,
                    "already_gone": True,
                    "raw": {"status": "FILLED", "executedQty": "10"},
                }

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                taker_amounts.append(kwargs["amount"])
                return {"ok": True, "order_id": "taker-1"}

        maker_adapter = MakerAdapter()
        result = await execute_single_clip_with_sides(
            symbol="BTW",
            clip_usd=1000.0,
            quantity=Decimal("10"),
            maker_venue="aster",
            taker_venue="mexc",
            maker_side="SELL",
            taker_side="BUY",
            maker_adapter=maker_adapter,
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=5.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(maker_adapter.cancel_calls, 1)
        self.assertEqual(taker_amounts, ["10"])


if __name__ == "__main__":
    unittest.main()
