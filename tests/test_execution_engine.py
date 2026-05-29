import unittest
import json
import tempfile
from pathlib import Path
from aiohttp import web

from hydra_basis.config import EXECUTION_VENUES_PATH, MONITOR_SIGNALS_PATH
from hydra_basis.execution_engine.interfaces import FakeExecutionAdapter
from hydra_basis.execution_engine.market_data import select_variational_quote_fields
from hydra_basis.execution_engine.models import ExecutionPreview, ExecutionRequest, ExecutionSignal
from hydra_basis.execution_engine.orderbook_spread_store import OrderbookSpreadStore
from hydra_basis.execution_engine.preview import build_execution_preview
from hydra_basis.execution_engine.priority import resolve_execution_legs
from hydra_basis.execution_engine.risk import compute_spread_pct, orderbook_is_anomalous, spread_requires_confirm
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol, save_monitor_signals
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter, build_place_order_payload
from scripts.run_execution_preview import compute_batch_count


class ExecutionConfigTests(unittest.TestCase):
    def test_execution_paths_point_to_expected_files(self) -> None:
        self.assertEqual(MONITOR_SIGNALS_PATH, Path("data/monitor_signals.json"))
        self.assertEqual(EXECUTION_VENUES_PATH, Path("configs/execution_venues.json"))

    def test_execution_models_hold_core_fields(self) -> None:
        signal = ExecutionSignal(
            symbol="BTC",
            short_venue="hyperliquid",
            long_venue="lighter",
            annualized_avg=0.55,
            score=0.91,
            updated_at="2026-05-29T00:00:00+00:00",
        )
        request = ExecutionRequest(symbol="BTC", total_usd=10000.0, clip_usd=500.0)
        self.assertEqual(signal.symbol, "BTC")
        self.assertEqual(request.total_usd, 10000.0)


class MonitorSignalStoreTests(unittest.TestCase):
    def test_save_monitor_signals_writes_ranked_cross_exchange_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "monitor_signals.json"
            save_monitor_signals(
                path=path,
                cross_exchange_signals=[
                    {
                        "symbol": "BTC",
                        "short_venue": "hyperliquid",
                        "long_venue": "lighter",
                        "stats": {"annualized_avg": 0.55, "score": 0.9, "signal": True},
                    }
                ],
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["cross_exchange_signals"][0]["symbol"], "BTC")
            self.assertIn("updated_at", payload)


class OrderbookSpreadStoreTests(unittest.TestCase):
    def test_save_and_load_latest_orderbook_spreads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "orderbook_spreads.json"
            store = OrderbookSpreadStore(path)
            store.save(
                {
                    ("lighter", "BTC"): {
                        "bid": 100.0,
                        "ask": 100.05,
                        "spread_pct": 0.0004998750312421895,
                        "ts_ms": 123456,
                    }
                }
            )
            loaded = store.load()
            self.assertEqual(loaded[("lighter", "BTC")]["bid"], 100.0)
            self.assertEqual(loaded[("lighter", "BTC")]["ask"], 100.05)


class LoadBestSignalTests(unittest.TestCase):
    def test_rejects_monitor_signal_file_older_than_24h(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "monitor_signals.json"
            path.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-05-27T00:00:00+00:00",
                        "cross_exchange_signals": [],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(RuntimeError):
                load_best_signal_for_symbol(path=path, symbol="BTC")

    def test_loads_best_signal_for_requested_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "monitor_signals.json"
            path.write_text(
                json.dumps(
                    {
                        "updated_at": "2099-05-29T00:00:00+00:00",
                        "cross_exchange_signals": [
                            {
                                "symbol": "BTC",
                                "short_venue": "mexc",
                                "long_venue": "lighter",
                                "annualized_avg": 0.6,
                                "score": 0.9,
                                "signal": True,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            signal = load_best_signal_for_symbol(path=path, symbol="BTC")
            self.assertEqual(signal.short_venue, "mexc")


class PriorityTests(unittest.TestCase):
    def test_lower_priority_number_becomes_taker(self) -> None:
        maker, taker = resolve_execution_legs(
            short_venue="hyperliquid",
            long_venue="lighter",
            priorities={"hyperliquid": 2, "lighter": 0},
            spreads={"hyperliquid": 0.0004, "lighter": 0.0002},
        )
        self.assertEqual(maker, "hyperliquid")
        self.assertEqual(taker, "lighter")

    def test_equal_priority_uses_smaller_spread_as_taker(self) -> None:
        maker, taker = resolve_execution_legs(
            short_venue="mexc",
            long_venue="aster",
            priorities={"mexc": 2, "aster": 2},
            spreads={"mexc": 0.0008, "aster": 0.0003},
        )
        self.assertEqual(maker, "mexc")
        self.assertEqual(taker, "aster")

    def test_when_only_one_side_exceeds_point_one_percent_lower_spread_side_becomes_taker(self) -> None:
        maker, taker = resolve_execution_legs(
            short_venue="hyperliquid",
            long_venue="lighter",
            priorities={"hyperliquid": 0, "lighter": 2},
            spreads={"hyperliquid": 0.0012, "lighter": 0.0004},
        )
        self.assertEqual(maker, "hyperliquid")
        self.assertEqual(taker, "lighter")

    def test_when_both_sides_below_point_one_percent_priority_still_decides_taker(self) -> None:
        maker, taker = resolve_execution_legs(
            short_venue="hyperliquid",
            long_venue="lighter",
            priorities={"hyperliquid": 2, "lighter": 0},
            spreads={"hyperliquid": 0.0002, "lighter": 0.0003},
        )
        self.assertEqual(maker, "hyperliquid")
        self.assertEqual(taker, "lighter")


class ExecutionAdapterTests(unittest.TestCase):
    def test_fake_adapter_returns_orderbook_snapshot(self) -> None:
        adapter = FakeExecutionAdapter(
            venue="lighter",
            orderbook={"bid": 100.0, "ask": 100.05, "ts_ms": 123456},
        )
        snapshot = adapter.get_orderbook("BTC")
        self.assertEqual(snapshot["bid"], 100.0)
        self.assertEqual(snapshot["ask"], 100.05)

    def test_select_variational_quote_fields_uses_quotes_base_and_size_tiers(self) -> None:
        bid, ask = select_variational_quote_fields(
            {
                "ticker": "IN",
                "quotes": {
                    "base": {"bid": "0.0957", "ask": "0.09592"},
                    "size_1k": {"bid": "0.09558", "ask": "0.09599"},
                    "size_100k": {"bid": "0.09089", "ask": "0.1005"},
                },
            },
            500.0,
        )
        self.assertEqual(bid, 0.09558)
        self.assertEqual(ask, 0.09599)


class ExecutionRiskTests(unittest.TestCase):
    def test_spread_requires_confirm_above_point_one_percent(self) -> None:
        self.assertTrue(spread_requires_confirm(0.0011))
        self.assertFalse(spread_requires_confirm(0.0010))

    def test_orderbook_is_anomalous_for_crossed_book(self) -> None:
        self.assertTrue(orderbook_is_anomalous({"bid": 101.0, "ask": 100.0, "ts_ms": 1}))

    def test_compute_spread_pct(self) -> None:
        spread = compute_spread_pct({"bid": 100.0, "ask": 100.1, "ts_ms": 1})
        self.assertGreater(spread, 0.0009)


class PreviewTests(unittest.TestCase):
    def test_build_execution_preview_computes_batches_and_confirm_gate(self) -> None:
        signal = ExecutionSignal(
            symbol="BTC",
            short_venue="hyperliquid",
            long_venue="lighter",
            annualized_avg=0.5,
            score=0.9,
            updated_at="2099-05-29T00:00:00+00:00",
        )
        request = ExecutionRequest(symbol="BTC", total_usd=10000.0, clip_usd=500.0)
        preview = build_execution_preview(
            request=request,
            signal=signal,
            priorities={"hyperliquid": 2, "lighter": 0},
            adapters={
                "hyperliquid": FakeExecutionAdapter("hyperliquid", {"bid": 100.0, "ask": 100.08, "ts_ms": 1}),
                "lighter": FakeExecutionAdapter("lighter", {"bid": 100.0, "ask": 100.05, "ts_ms": 1}),
            },
        )
        self.assertIsInstance(preview, ExecutionPreview)
        self.assertEqual(preview.batch_count, 20)
        self.assertEqual(preview.maker_venue, "hyperliquid")
        self.assertEqual(preview.taker_venue, "lighter")
        self.assertFalse(preview.requires_confirm)


class StateMachineTests(unittest.TestCase):
    def test_state_machine_moves_from_idle_to_preview_and_confirm(self) -> None:
        sm = ExecutionStateMachine()
        sm.to_preview_ready()
        sm.to_awaiting_confirm()
        self.assertEqual(sm.state, "awaiting_confirm")

    def test_state_machine_enters_emergency_exit_after_hedge_failure(self) -> None:
        sm = ExecutionStateMachine()
        sm.to_preview_ready()
        sm.to_awaiting_confirm()
        sm.to_placing_maker_leg()
        sm.to_hedging_taker_leg()
        sm.to_retrying_hedge()
        sm.to_emergency_exit()
        self.assertEqual(sm.state, "emergency_exit")


class VariationalBrowserAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._messages: list[dict] = []
        self._app = web.Application()
        self._app.router.add_get("/", self._ws_handler)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self._port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://127.0.0.1:{self._port}/"

    async def asyncTearDown(self) -> None:
        await self._runner.cleanup()

    async def _ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue
            payload = json.loads(msg.data)
            self._messages.append(payload)
            if payload["type"] == "REGISTER":
                await ws.send_json({"type": "REGISTER_ACK", "ok": True, "role": payload["role"]})
            elif payload["type"] == "PLACE_ORDER":
                await ws.send_json({"type": "ORDER_DISPATCHED", "requestId": payload["requestId"], "ok": True})
                await ws.send_json(
                    {
                        "type": "ORDER_RESULT",
                        "requestId": payload["requestId"],
                        "ok": True,
                        "orderId": "demo-order",
                    }
                )
        return ws

    async def test_place_limit_order_sends_broker_payload_and_returns_result(self) -> None:
        adapter = VariationalBrowserExecutionAdapter(broker_url=self._url, client_role="strategy")

        result = await adapter.place_limit_order(
            symbol="BTC",
            side="SELL",
            amount="100",
            market="BTC-PERP",
            account="main",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["orderId"], "demo-order")
        self.assertEqual(self._messages[0]["type"], "REGISTER")
        self.assertEqual(self._messages[1]["type"], "PLACE_ORDER")
        self.assertEqual(self._messages[1]["market"], "BTC-PERP")

    async def test_place_limit_order_raises_when_broker_returns_failure(self) -> None:
        async def failing_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                if msg.type != web.WSMsgType.TEXT:
                    continue
                payload = json.loads(msg.data)
                if payload["type"] == "REGISTER":
                    await ws.send_json({"type": "REGISTER_ACK", "ok": True, "role": payload["role"]})
                elif payload["type"] == "PLACE_ORDER":
                    await ws.send_json(
                        {
                            "type": "ORDER_RESULT",
                            "requestId": payload["requestId"],
                            "ok": False,
                            "error": "browser reject",
                        }
                    )
            return ws

        await self._runner.cleanup()
        self._app = web.Application()
        self._app.router.add_get("/", failing_handler)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self._port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://127.0.0.1:{self._port}/"

        adapter = VariationalBrowserExecutionAdapter(broker_url=self._url, client_role="strategy")

        with self.assertRaises(RuntimeError):
            await adapter.place_limit_order(symbol="BTC", side="BUY", amount="50")

    def test_build_place_order_payload_uses_expected_command_shape(self) -> None:
        payload = build_place_order_payload(
            request_id="req-1",
            side="BUY",
            amount="50",
            market="BTC-PERP",
            account="main",
            timeout_ms=5000,
        )

        self.assertEqual(payload["type"], "PLACE_ORDER")
        self.assertEqual(payload["requestId"], "req-1")
        self.assertEqual(payload["market"], "BTC-PERP")


class ExecutionPreviewCliTests(unittest.TestCase):
    def test_compute_batch_count_rounds_up(self) -> None:
        self.assertEqual(compute_batch_count(10000.0, 500.0), 20)
        self.assertEqual(compute_batch_count(10250.0, 500.0), 21)


if __name__ == "__main__":
    unittest.main()
