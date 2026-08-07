from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_broker import VariationalCommandBroker
from scripts.place_order import (
    MAKER_REPRICE_ATTEMPTS,
    maker_reprice_attempts_for_venue,
    record_open_execution_from_live_positions,
)


class FakeRequester:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def send(self, raw: str) -> None:
        self.messages.append(json.loads(raw))


def pending_order(requester: FakeRequester, *, submitted: bool = True) -> dict:
    return {
        "requester": requester,
        "requestId": "req-safety",
        "order": {
            "symbol": "NXPC",
            "side": "BUY",
            "amount": "1000",
            "orderType": "LIMIT",
        },
        "submitted": submitted,
        "submittedResult": {"ok": True},
        "orderId": None,
        "timeoutTask": None,
        "baselinePositionQty": Decimal("1000"),
    }


class VariationalFillSafetyTests(unittest.IsolatedAsyncioTestCase):
    def test_variational_reprices_only_through_guarded_executor_loop(self) -> None:
        self.assertEqual(
            maker_reprice_attempts_for_venue("variational"),
            MAKER_REPRICE_ATTEMPTS,
        )

    async def test_submit_ack_preserves_price_for_timeout_reprice_decision(self) -> None:
        broker = VariationalCommandBroker(quiet=True)
        requester = FakeRequester()
        broker._pending_requests["req-safety"] = pending_order(requester, submitted=False)

        await broker._handle_order_result(
            {
                "type": "ORDER_RESULT",
                "requestId": "req-safety",
                "ok": True,
                "orderId": None,
                "details": {"usedLimitPrice": "100.25"},
            }
        )

        self.assertEqual(len(requester.messages), 1)
        accepted = requester.messages[0]
        self.assertEqual(accepted["type"], "ORDER_ACCEPTED")
        self.assertEqual(accepted["details"]["usedLimitPrice"], "100.25")
        self.assertEqual(
            maker_reprice_attempts_for_venue("hyperliquid"),
            MAKER_REPRICE_ATTEMPTS,
        )

    async def test_portfolio_position_delta_confirms_fill_quantity(self) -> None:
        broker = VariationalCommandBroker(quiet=True)
        requester = FakeRequester()
        broker._positions["NXPC"] = {
            "symbol": "NXPC",
            "side": "LONG",
            "quantity": "1000",
            "market_type": "perp",
        }
        broker._pending_requests["req-safety"] = pending_order(requester)

        await broker.handle_fill_event(
            {
                "kind": "ws_frame",
                "direction": "received",
                "url": "wss://example.test/portfolio",
                "payloadData": json.dumps(
                    {
                        "type": "portfolio",
                        "positions": [
                            {
                                "instrument": {"underlying": "NXPC"},
                                "qty": "2000",
                            }
                        ],
                    }
                ),
            }
        )

        self.assertEqual(len(requester.messages), 1)
        result = requester.messages[0]
        self.assertTrue(result["filled"])
        self.assertEqual(
            result["details"]["fill"]["filledBaseAmount"], "1000"
        )
        self.assertEqual(
            result["details"]["fill"]["source"], "portfolio_position_delta"
        )

    async def test_trade_event_on_portfolio_socket_is_not_discarded(self) -> None:
        broker = VariationalCommandBroker(quiet=True)
        requester = FakeRequester()
        broker._pending_requests["req-safety"] = pending_order(requester)

        await broker.handle_fill_event(
            {
                "kind": "ws_frame",
                "direction": "received",
                "url": "wss://example.test/portfolio",
                "payloadData": json.dumps(
                    {
                        "type": "trade",
                        "data": {
                            "instrument": {"underlying": "NXPC"},
                            "side": "buy",
                            "qty": "1000",
                            "status": "confirmed",
                        },
                    }
                ),
            }
        )

        self.assertEqual(len(requester.messages), 1)
        self.assertTrue(requester.messages[0]["filled"])
        self.assertEqual(
            requester.messages[0]["details"]["fill"]["symbol"], "NXPC"
        )

    async def test_timeout_without_order_id_is_cancelled_before_exit(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("submit")
                error = RuntimeError("variational limit order fill timeout after 60s")
                setattr(
                    error,
                    "order_result",
                    {
                        "type": "ORDER_ACCEPTED",
                        "ok": True,
                        "orderId": None,
                        "details": {"usedLimitPrice": "100"},
                    },
                )
                raise error

            async def cancel_order(self, **kwargs):
                calls.append("cancel_by_symbol_side_amount")
                self.assert_no_order_id(kwargs["order_result"])
                return {"ok": True, "status": "cancelled"}

            @staticmethod
            def assert_no_order_id(order_result):
                if order_result.get("orderId") is not None:
                    raise AssertionError("expected symbol-side-amount cancellation")

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker")
                return {"ok": True}

        with self.assertRaisesRegex(RuntimeError, "fill timeout"):
            await execute_single_clip_with_sides(
                symbol="NXPC",
                clip_usd=1000.0,
                quantity=Decimal("1000"),
                maker_venue="variational",
                taker_venue="hyperliquid",
                maker_side="SELL",
                taker_side="BUY",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=0,
                state_machine=ExecutionStateMachine(),
                require_maker_fill_confirmation=True,
                max_maker_reprice_attempts=0,
            )

        self.assertEqual(calls, ["submit", "cancel_by_symbol_side_amount"])

    async def test_small_price_move_keeps_existing_order_instead_of_replacing(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("submit")
                error = RuntimeError("variational limit order fill timeout after 60s")
                setattr(
                    error,
                    "order_result",
                    {
                        "type": "ORDER_ACCEPTED",
                        "ok": True,
                        "orderId": None,
                        "details": {"usedLimitPrice": "100"},
                    },
                )
                raise error

            async def has_open_order(self, **kwargs):
                calls.append("check_open")
                return True

            async def wait_for_order_fill(self, **kwargs):
                calls.append("wait_existing")
                return {
                    "ok": True,
                    "filled": True,
                    "filled_quantity": "1000",
                    "status": "FILLED",
                }

            async def cancel_order(self, **kwargs):
                calls.append("cancel")
                return {"ok": True}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker")
                return {"ok": True}

        async def refresh_price() -> str:
            calls.append("refresh_price")
            return "100.01"

        result = await execute_single_clip_with_sides(
            symbol="NXPC",
            clip_usd=1000.0,
            quantity=Decimal("1000"),
            maker_venue="variational",
            taker_venue="hyperliquid",
            maker_side="SELL",
            taker_side="BUY",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=0,
            state_machine=ExecutionStateMachine(),
            require_maker_fill_confirmation=True,
            max_maker_reprice_attempts=-1,
            maker_reprice_min_change_pct=0.0005,
            maker_price_refresher=refresh_price,
            maker_keep_existing_check_delay_seconds=0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(
            calls,
            ["submit", "refresh_price", "check_open", "wait_existing", "taker"],
        )

    async def test_large_price_move_cancels_before_replacement(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            def __init__(self) -> None:
                self.submits = 0

            async def place_limit_order(self, **kwargs):
                self.submits += 1
                calls.append(f"submit_{self.submits}")
                if self.submits == 1:
                    error = RuntimeError("variational limit order fill timeout after 60s")
                    setattr(
                        error,
                        "order_result",
                        {
                            "type": "ORDER_ACCEPTED",
                            "ok": True,
                            "orderId": None,
                            "details": {"usedLimitPrice": "100"},
                        },
                    )
                    raise error
                return {
                    "ok": True,
                    "filled": True,
                    "filled_quantity": "1000",
                    "status": "FILLED",
                }

            async def cancel_order(self, **kwargs):
                calls.append("cancel_old")
                return {"ok": True, "status": "cancelled"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker")
                return {"ok": True}

        async def refresh_price() -> str:
            calls.append("refresh_price")
            return "101"

        result = await execute_single_clip_with_sides(
            symbol="NXPC",
            clip_usd=1000.0,
            quantity=Decimal("1000"),
            maker_venue="variational",
            taker_venue="hyperliquid",
            maker_side="SELL",
            taker_side="BUY",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=0,
            state_machine=ExecutionStateMachine(),
            require_maker_fill_confirmation=True,
            max_maker_reprice_attempts=-1,
            maker_reprice_min_change_pct=0.0005,
            maker_price_refresher=refresh_price,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(
            calls,
            ["submit_1", "refresh_price", "cancel_old", "submit_2", "taker"],
        )

    async def test_missing_order_after_timeout_is_not_replaced(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("submit")
                error = RuntimeError("variational limit order fill timeout after 60s")
                setattr(
                    error,
                    "order_result",
                    {
                        "type": "ORDER_ACCEPTED",
                        "ok": True,
                        "orderId": None,
                        "details": {"usedLimitPrice": "100"},
                    },
                )
                raise error

            async def has_open_order(self, **kwargs):
                calls.append("check_absent")
                return False

            async def cancel_order(self, **kwargs):
                calls.append("confirm_absent")
                return {"ok": True, "status": "already_absent"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker")
                return {"ok": True}

        async def refresh_price() -> str:
            calls.append("refresh_price")
            return "100.01"

        with self.assertRaisesRegex(RuntimeError, "disappeared without fill confirmation"):
            await execute_single_clip_with_sides(
                symbol="NXPC",
                clip_usd=1000.0,
                quantity=Decimal("1000"),
                maker_venue="variational",
                taker_venue="hyperliquid",
                maker_side="SELL",
                taker_side="BUY",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=0,
                state_machine=ExecutionStateMachine(),
                require_maker_fill_confirmation=True,
                max_maker_reprice_attempts=-1,
                maker_reprice_min_change_pct=0.0005,
                maker_price_refresher=refresh_price,
                maker_keep_existing_check_delay_seconds=0,
            )

        self.assertEqual(
            calls,
            ["submit", "refresh_price", "check_absent", "confirm_absent"],
        )

    async def test_live_position_mismatch_stops_before_registry_recording(self) -> None:
        class Adapter:
            def __init__(self, side: str, quantity: str) -> None:
                self.side = side
                self.quantity = quantity

            async def get_open_position(self, *, symbol: str, market_type: str):
                return {
                    "symbol": symbol,
                    "market_type": market_type,
                    "side": self.side,
                    "quantity": self.quantity,
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            with self.assertRaisesRegex(RuntimeError, "FAIL-CLOSED live position mismatch"):
                await record_open_execution_from_live_positions(
                    execution_result={"ok": True, "executed_quantity": "1000"},
                    adapters_by_venue={
                        "hyperliquid": Adapter("SHORT", "2000"),
                        "variational": Adapter("LONG", "6000"),
                    },
                    symbol="NXPC",
                    short_venue="hyperliquid",
                    long_venue="variational",
                    registry_path=registry_path,
                )
            self.assertFalse(registry_path.exists())


if __name__ == "__main__":
    unittest.main()
