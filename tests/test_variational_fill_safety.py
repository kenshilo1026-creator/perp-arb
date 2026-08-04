from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

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
    def test_variational_never_reprices_after_ambiguous_fill(self) -> None:
        self.assertEqual(maker_reprice_attempts_for_venue("variational"), 0)
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
