from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_broker import VariationalCommandBroker
from scripts.place_order import (
    ClosePositionPlan,
    MAKER_REPRICE_ATTEMPTS,
    execute_close_position_plan,
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
        "baselinePortfolioVersion": 7,
    }


class VariationalFillSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_resolves_variational_price_before_dispatch(self) -> None:
        submitted: dict[str, object] = {}

        class MakerAdapter:
            quantity = "1000"

            async def get_open_position(self, **kwargs):
                return {"side": "LONG", "quantity": self.quantity} if self.quantity != "0" else None

            async def get_limit_price_preview(self, **kwargs):
                return "0.1828"

            async def place_limit_order(self, **kwargs):
                submitted.update(kwargs)
                self.quantity = "0"
                return {
                    "ok": True,
                    "filled": True,
                    "filled_quantity": "1000",
                    "status": "FILLED",
                }

        class TakerAdapter:
            quantity = "1000"

            async def get_open_position(self, **kwargs):
                return {"side": "SHORT", "quantity": self.quantity} if self.quantity != "0" else None

            async def place_market_order(self, **kwargs):
                self.quantity = "0"
                return {"ok": True, "status": "FILLED", "filled_quantity": kwargs["amount"]}

        result = await execute_close_position_plan(
            plan=ClosePositionPlan(
                symbol="NXPC",
                quantity=Decimal("1000"),
                maker_venue="variational",
                taker_venue="hyperliquid",
                maker_price="0.1827",
                clip_usd=182.8,
                side_by_venue={"variational": "SELL", "hyperliquid": "BUY"},
                spread_by_venue={"variational": 0.0, "hyperliquid": 0.0},
            ),
            adapters={
                "variational": MakerAdapter(),
                "hyperliquid": TakerAdapter(),
            },
            symbol="NXPC",
            venues=["variational", "hyperliquid"],
        )

        self.assertTrue(result["ok"])
        self.assertEqual(submitted["price"], "0.1828")
        self.assertTrue(submitted["reduce_only"])

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
        self.assertEqual(accepted["details"]["baselinePositionQty"], "1000")
        self.assertEqual(accepted["details"]["baselinePortfolioVersion"], 7)
        self.assertEqual(
            maker_reprice_attempts_for_venue("hyperliquid"),
            MAKER_REPRICE_ATTEMPTS,
        )

    async def test_post_submit_absent_error_preserves_position_baseline(self) -> None:
        broker = VariationalCommandBroker(quiet=True)
        requester = FakeRequester()
        broker._pending_requests["req-safety"] = pending_order(requester, submitted=False)

        await broker._handle_order_result(
            {
                "type": "ORDER_RESULT",
                "requestId": "req-safety",
                "ok": False,
                "error": "Variational limit submit click did not create a matching open order.",
                "details": {
                    "clickedViaPageGesture": True,
                    "submitVerifiedOpenOrder": False,
                    "usedLimitPrice": "0.1828",
                },
            }
        )

        self.assertEqual(len(requester.messages), 1)
        result = requester.messages[0]
        self.assertFalse(result["ok"])
        self.assertTrue(result["details"]["postSubmitAmbiguous"])
        self.assertEqual(result["details"]["baselinePositionQty"], "1000")
        self.assertEqual(result["details"]["baselinePortfolioVersion"], 7)

    async def test_post_submit_absent_error_uses_current_position_delta_as_fill(self) -> None:
        broker = VariationalCommandBroker(quiet=True)
        requester = FakeRequester()
        pending = pending_order(requester, submitted=False)
        pending["order"]["side"] = "SELL"
        broker._pending_requests["req-safety"] = pending

        await broker._handle_order_result(
            {
                "type": "ORDER_RESULT",
                "requestId": "req-safety",
                "ok": False,
                "error": "Variational limit submit click did not create a matching open order.",
                "details": {
                    "clickedViaPageGesture": True,
                    "submitVerifiedOpenOrder": False,
                },
            }
        )

        self.assertEqual(len(requester.messages), 1)
        result = requester.messages[0]
        self.assertTrue(result["ok"])
        self.assertTrue(result["filled"])
        self.assertEqual(
            result["details"]["fill"]["source"], "portfolio_position_delta"
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
                error = RuntimeError("variational order acceptance timeout after 30s")
                setattr(
                    error,
                    "order_result",
                    {
                        "type": "ORDER_DISPATCHED",
                        "ok": True,
                        "orderId": None,
                        "details": {
                            "potentiallySubmitted": True,
                            "symbol": "NXPC",
                            "side": "SELL",
                            "amount": "1000",
                            "baselinePositionQty": "1000",
                            "baselinePortfolioVersion": 1,
                        },
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

        with self.assertRaisesRegex(RuntimeError, "acceptance timeout"):
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

    async def test_cancel_already_absent_reconciles_partial_fill_before_replacement(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            def __init__(self) -> None:
                self.position_checks = 0

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
                        "details": {
                            "usedLimitPrice": "100",
                            "baselinePositionQty": "1000",
                            "baselinePortfolioVersion": 1,
                        },
                    },
                )
                raise error

            async def cancel_order(self, **kwargs):
                calls.append("cancel_already_absent")
                return {"ok": True, "raw": {"status": "already_absent"}}

            async def get_open_position_snapshot(self, **kwargs):
                self.position_checks += 1
                calls.append(f"position_{self.position_checks}")
                if self.position_checks <= 5:
                    return {
                        "position": {"side": "LONG", "quantity": "1000"},
                        "portfolio_version": 2,
                    }
                return {
                    "position": {"side": "LONG", "quantity": "400"},
                    "portfolio_version": 3,
                }

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append(f"taker_{kwargs['amount']}")
                return {"ok": True}

        async def refresh_price() -> str:
            calls.append("refresh_price")
            return "101"

        with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
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
        self.assertEqual(result["executed_quantity"], "600")
        self.assertEqual(calls.count("cancel_already_absent"), 1)
        self.assertEqual(calls[-2:], ["position_6", "taker_600"])

    async def test_missing_order_reconciles_position_fill_and_hedges(self) -> None:
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
                        "details": {
                            "usedLimitPrice": "100",
                            "baselinePositionQty": "1000",
                            "baselinePortfolioVersion": 1,
                        },
                    },
                )
                raise error

            async def has_open_order(self, **kwargs):
                calls.append("check_absent")
                return False

            async def get_open_position_snapshot(self, **kwargs):
                calls.append("position_now_flat")
                return {"position": None, "portfolio_version": 2}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append(f"taker_{kwargs['amount']}")
                return {"ok": True}

        async def refresh_price() -> str:
            raise AssertionError("price refresh must not run before fill reconciliation")

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
            ["submit", "position_now_flat", "taker_1000"],
        )

    async def test_post_submit_absent_error_reconciles_before_cleanup(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("submit")
                error = RuntimeError(
                    "variational browser order failed for NXPC: Variational limit "
                    "submit click did not create a matching open order."
                )
                setattr(
                    error,
                    "order_result",
                    {
                        "type": "ORDER_RESULT",
                        "ok": False,
                        "details": {
                            "postSubmitAmbiguous": True,
                            "baselinePositionQty": "1000",
                            "baselinePortfolioVersion": 1,
                            "usedLimitPrice": "0.1828",
                        },
                    },
                )
                raise error

            async def get_open_position_snapshot(self, **kwargs):
                calls.append("position_now_flat")
                return {"position": None, "portfolio_version": 2}

            async def cancel_order(self, **kwargs):
                calls.append("cancel")
                return {"ok": True, "raw": {"status": "already_absent"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append(f"taker_{kwargs['amount']}")
                return {"ok": True}

        result = await execute_single_clip_with_sides(
            symbol="NXPC",
            clip_usd=182.8,
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
            maker_price_refresher=lambda: None,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls, ["submit", "position_now_flat", "taker_1000"])

    async def test_post_submit_absent_unfilled_error_cancels_before_exit(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("submit")
                error = RuntimeError(
                    "variational browser order failed for NXPC: Variational limit "
                    "submit click did not create a matching open order."
                )
                setattr(
                    error,
                    "order_result",
                    {
                        "type": "ORDER_RESULT",
                        "ok": False,
                        "details": {
                            "postSubmitAmbiguous": True,
                            "baselinePositionQty": "1000",
                            "baselinePortfolioVersion": 1,
                            "usedLimitPrice": "0.1828",
                        },
                    },
                )
                raise error

            async def get_open_position_snapshot(self, **kwargs):
                calls.append("position_unchanged")
                return {
                    "position": {"side": "LONG", "quantity": "1000"},
                    "portfolio_version": 2,
                }

            async def cancel_order(self, **kwargs):
                calls.append("cancel_before_exit")
                return {"ok": True, "raw": {"status": "already_absent"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker")
                return {"ok": True}

        with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, "did not create a matching open order"):
                await execute_single_clip_with_sides(
                    symbol="NXPC",
                    clip_usd=182.8,
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

        self.assertEqual(calls.count("position_unchanged"), 5)
        self.assertEqual(calls[-1], "cancel_before_exit")
        self.assertNotIn("taker", calls)

    async def test_missing_unfilled_order_with_stable_position_is_replaced(self) -> None:
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
                            "details": {
                                "usedLimitPrice": "100",
                                "baselinePositionQty": "1000",
                                "baselinePortfolioVersion": 1,
                            },
                        },
                    )
                    raise error
                return {
                    "ok": True,
                    "filled": True,
                    "filled_quantity": "1000",
                    "status": "FILLED",
                }

            async def has_open_order(self, **kwargs):
                calls.append("check_absent")
                return False

            async def get_open_position_snapshot(self, **kwargs):
                calls.append("position_unchanged")
                return {
                    "position": {"side": "LONG", "quantity": "1000"},
                    "portfolio_version": 2,
                }

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker")
                return {"ok": True}

        async def refresh_price() -> str:
            calls.append("refresh_price")
            return "100.01"

        with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
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
        self.assertEqual(calls.count("position_unchanged"), 10)
        self.assertEqual(calls[-2:], ["submit_2", "taker"])

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
