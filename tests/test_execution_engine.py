import unittest
import json
import tempfile
import os
from pathlib import Path
from aiohttp import web
from decimal import Decimal
from unittest import mock

from hydra_basis.config import EXECUTION_VENUES_PATH, MONITOR_SIGNALS_PATH
from hydra_basis.execution_engine.interfaces import FakeExecutionAdapter
from hydra_basis.execution_engine.market_data import select_variational_quote_fields
from hydra_basis.execution_engine.models import ExecutionPreview, ExecutionRequest, ExecutionSignal
from hydra_basis.execution_engine.orderbook_spread_store import OrderbookSpreadStore
from hydra_basis.execution_engine.runtime import estimate_clip_usd_from_size
from hydra_basis.execution_engine.preview import build_execution_preview
from hydra_basis.execution_engine.priority import resolve_execution_legs
from hydra_basis.execution_engine.risk import compute_spread_pct, orderbook_is_anomalous, spread_requires_confirm
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol, save_monitor_signals
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter, build_place_order_payload
from hydra_basis.execution_engine.lighter_adapter import (
    build_lighter_limit_order_request,
    build_lighter_market_order_request,
    compute_base_quantity_from_clip_usd,
    LighterExecutionAdapter,
)
from hydra_basis.execution_engine.lighter_live import build_lighter_client_factory_from_env
from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter
from hydra_basis.alerts import build_ranked_alert_digest
from hydra_basis.execution_engine.executor import execute_single_clip, execute_single_clip_with_sides
from hydra_basis.execution_engine.executor import execution_sides_for_signal
from hydra_basis.execution_engine import mexc_spot_adapter
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.risk_management.models import PositionLeg
from hydra_basis.risk_management.registry import PositionRegistry
from scripts.run_execution_preview import compute_batch_count
from scripts.run_execution_once import (
    MAKER_FILL_TIMEOUT_SECONDS,
    MAX_MAKER_REPRICE_ATTEMPTS,
    ClosePositionPlan,
    build_adapter_for_venue,
    build_close_position_plan,
    compute_batch_count as compute_single_clip_batch_count,
    compute_token_batch_count,
    discover_live_close_candidates_for_symbol,
    execute_close_position_plan,
    format_execution_price_summary_zh,
    format_execution_result_summary_zh,
    format_open_positions_zh,
    load_live_close_candidates,
    load_live_close_legs,
    run_close_execution_once,
    validate_maker_fill_supported,
)


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

    def test_format_execution_price_summary_zh_shows_avg_prices_and_gap(self) -> None:
        text = format_execution_price_summary_zh(
            {
                "execution_price_summary": {
                    "maker_avg_price": "100",
                    "taker_avg_price": "100.5",
                    "price_gap_pct": "0.004987531172069825436408977556",
                    "pre_trade": {
                        "maker_price": "100",
                        "taker_price": "100.5",
                        "price_gap_pct": "0.004987531172069825436408977556",
                    },
                }
            }
        )

        self.assertIn("成交均價", text)
        self.assertIn("maker=100", text)
        self.assertIn("taker=100.5", text)
        self.assertIn("成交價差=0.4988%", text)
        self.assertIn("下單前價差=0.4988%", text)

    def test_format_execution_result_summary_zh_omits_full_nested_payload(self) -> None:
        text = format_execution_result_summary_zh(
            {
                "ok": True,
                "maker_result": {
                    "type": "ORDER_RESULT",
                    "orderId": "maker-1",
                    "status": "FILLED",
                    "details": {"fill": {"raw": {"very": "large"}}},
                },
                "hedge_result": {
                    "ok": True,
                    "order_id": "taker-1",
                    "raw": {"status": "FILLED", "avgPrice": "100.5"},
                },
                "execution_price_summary": {
                    "maker_avg_price": "100",
                    "taker_avg_price": "100.5",
                    "price_gap_pct": "0.004987531172069825436408977556",
                },
            }
        )

        self.assertIn("執行結果: ok=True", text)
        self.assertIn("maker_order_id=maker-1", text)
        self.assertIn("taker_order_id=taker-1", text)
        self.assertIn("成交均價", text)
        self.assertNotIn("details", text)
        self.assertNotIn("very", text)


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

    def test_save_and_load_no_orderbook_sentinel(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "orderbook_spreads.json"
            store = OrderbookSpreadStore(path)
            store.save({("lighter", "BOT"): {"status": "no_orderbook"}})
            loaded = store.load()
            self.assertEqual(loaded, {("lighter", "BOT"): {"status": "no_orderbook"}})


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

    def test_when_only_one_side_exceeds_point_one_percent_priority_still_decides_taker(self) -> None:
        maker, taker = resolve_execution_legs(
            short_venue="hyperliquid",
            long_venue="lighter",
            priorities={"hyperliquid": 2, "lighter": 0},
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

    def test_when_both_sides_above_point_one_percent_higher_spread_becomes_maker(self) -> None:
        maker, taker = resolve_execution_legs(
            short_venue="hyperliquid",
            long_venue="lighter",
            priorities={"hyperliquid": 0, "lighter": 2},
            spreads={"hyperliquid": 0.0012, "lighter": 0.0020},
        )
        self.assertEqual(maker, "lighter")
        self.assertEqual(taker, "hyperliquid")


class ExecutionClosePlanTests(unittest.TestCase):
    def test_format_open_positions_zh_lists_open_strategies(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("manual-BEAT-1", "beat-short", "aster", "BEAT", "perp", "SHORT", "10"),
                PositionLeg("manual-BEAT-1", "beat-long", "variational", "BEAT", "perp", "LONG", "10"),
                PositionLeg("manual-OLD-1", "old", "mexc", "OLD", "perp", "LONG", "1", "closed"),
            ]
        )

        lines = format_open_positions_zh(registry)

        self.assertIn("1. strategy_id=manual-BEAT-1", lines)
        self.assertIn("BEAT", lines)
        self.assertIn("aster SHORT 10", lines)
        self.assertNotIn("manual-OLD-1", lines)

    def test_build_close_position_plan_reverses_sides_and_uses_execution_priority_when_spreads_not_both_high(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("manual-BEAT-1", "beat-short", "aster", "BEAT", "perp", "SHORT", "10"),
                PositionLeg("manual-BEAT-1", "beat-long", "lighter", "BEAT", "perp", "LONG", "10"),
            ]
        )

        plan = build_close_position_plan(
            registry=registry,
            strategy_id="manual-BEAT-1",
            clip_size=Decimal("3"),
            priorities={"aster": 2, "lighter": 0},
            orderbooks={
                "aster": {"bid": 99.0, "ask": 99.2, "ts_ms": 1},
                "lighter": {"bid": 100.0, "ask": 100.05, "ts_ms": 1},
            },
        )

        self.assertEqual(plan.symbol, "BEAT")
        self.assertEqual(plan.maker_venue, "aster")
        self.assertEqual(plan.taker_venue, "lighter")
        self.assertEqual(plan.side_by_venue["aster"], "BUY")
        self.assertEqual(plan.side_by_venue["lighter"], "SELL")
        self.assertEqual(plan.quantity, Decimal("3"))

    def test_build_close_position_plan_uses_higher_spread_as_maker_when_both_spreads_high(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("manual-BEAT-1", "beat-short", "aster", "BEAT", "perp", "SHORT", "10"),
                PositionLeg("manual-BEAT-1", "beat-long", "lighter", "BEAT", "perp", "LONG", "10"),
            ]
        )

        plan = build_close_position_plan(
            registry=registry,
            strategy_id="manual-BEAT-1",
            clip_size=Decimal("3"),
            priorities={"aster": 0, "lighter": 2},
            orderbooks={
                "aster": {"bid": 99.0, "ask": 99.2, "ts_ms": 1},
                "lighter": {"bid": 100.0, "ask": 100.3, "ts_ms": 1},
            },
        )

        self.assertEqual(plan.maker_venue, "lighter")
        self.assertEqual(plan.taker_venue, "aster")


class ExecutionCloseFlowTests(unittest.IsolatedAsyncioTestCase):
    def test_run_execution_once_uses_longer_maker_wait_and_reprice_attempts(self) -> None:
        self.assertEqual(MAKER_FILL_TIMEOUT_SECONDS, 180.0)
        self.assertEqual(MAX_MAKER_REPRICE_ATTEMPTS, 3)

    async def test_execute_close_position_plan_passes_maker_reprice_attempts(self) -> None:
        leg_a = PositionLeg(
            strategy_id="manual-close",
            leg_id="leg-a",
            venue="variational",
            symbol="BEAT",
            side="LONG",
            quantity="10",
            market_type="perp",
        )
        leg_b = PositionLeg(
            strategy_id="manual-close",
            leg_id="leg-b",
            venue="aster",
            symbol="BEAT",
            side="SHORT",
            quantity="10",
            market_type="perp",
        )
        plan = ClosePositionPlan(
            strategy_id="manual-close",
            symbol="BEAT",
            quantity=Decimal("10"),
            maker_venue="variational",
            taker_venue="aster",
            maker_price="1.23",
            clip_usd=1000.0,
            side_by_venue={"variational": "SELL", "aster": "BUY"},
            spread_by_venue={"variational": 0.001, "aster": 0.001},
            orderbook_by_venue={
                "variational": {"bid": 1.22, "ask": 1.23, "ts_ms": 1},
                "aster": {"bid": 1.21, "ask": 1.24, "ts_ms": 1},
            },
            legs_by_venue={"variational": leg_a, "aster": leg_b},
        )

        async def fake_execute_single_clip_with_sides(**kwargs):
            self.assertEqual(kwargs["maker_fill_timeout_seconds"], 180.0)
            self.assertEqual(kwargs["max_maker_reprice_attempts"], 3)
            self.assertEqual(kwargs["maker_orderbook"]["ask"], 1.23)
            self.assertEqual(kwargs["taker_orderbook"]["ask"], 1.24)
            self.assertFalse(kwargs["use_maker_orderbook_price"])
            return {"ok": True}

        registry = mock.Mock(spec=PositionRegistry)
        with mock.patch(
            "scripts.run_execution_once.execute_single_clip_with_sides",
            new=fake_execute_single_clip_with_sides,
        ):
            result = await execute_close_position_plan(
                plan=plan,
                registry=registry,
                adapters={"variational": object(), "aster": object()},
            )

        self.assertTrue(result["ok"])

    async def test_discover_live_close_candidates_for_symbol_uses_live_adapters_without_registry(self) -> None:
        calls: list[tuple[str, str, str]] = []

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def get_open_position(self, *, symbol: str, market_type: str):
                calls.append((self.venue, symbol, market_type))
                if self.venue == "aster" and market_type == "perp":
                    return {"symbol": symbol, "market_type": market_type, "side": "SHORT", "quantity": "10"}
                if self.venue == "mexc_spot" and market_type == "spot":
                    return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "10"}
                return None

        def fake_build_adapter(venue: str, *, leverage: int = 1, broker_url=None):
            return Adapter(venue)

        with mock.patch("scripts.run_execution_once.build_adapter_for_venue", new=fake_build_adapter):
            candidates, errors = await discover_live_close_candidates_for_symbol(
                symbol="BEAT",
                venues=["aster", "mexc_spot"],
            )

        self.assertFalse(errors)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0][0].venue, "aster")
        self.assertEqual(candidates[0][1].venue, "mexc_spot")
        self.assertIn(("aster", "BEAT", "perp"), calls)
        self.assertIn(("mexc_spot", "BEAT", "spot"), calls)

    async def test_run_close_execution_once_does_not_load_position_registry(self) -> None:
        async def fake_run_with_broker(*, symbol: str, broker_url=None) -> None:
            self.assertEqual(symbol, "BEAT")

        with mock.patch("scripts.run_execution_once.prompt_text", return_value="BEAT"), \
            mock.patch("scripts.run_execution_once.run_close_execution_once_with_broker", new=fake_run_with_broker), \
            mock.patch.object(PositionRegistry, "load", side_effect=AssertionError("registry should not be loaded")):
            await run_close_execution_once()

    async def test_close_mode_does_not_prompt_for_leverage(self) -> None:
        prompts: list[str] = []

        async def fake_discover(*, symbol: str, broker_url=None, **kwargs):
            return ([
                (
                    PositionLeg("live-BEAT", "a", "aster", "BEAT", "perp", "SHORT", "10"),
                    PositionLeg("live-BEAT", "b", "lighter", "BEAT", "perp", "LONG", "10"),
                )
            ], {})

        async def fake_fetch_orderbooks(*, symbol: str, venues: list[str], clip_usd: float):
            return {
                "aster": {"bid": 99.0, "ask": 99.2, "ts_ms": 1},
                "lighter": {"bid": 100.0, "ask": 100.05, "ts_ms": 1},
            }

        def fake_prompt_text(label: str) -> str:
            prompts.append(label)
            if label == "ticker":
                return "BEAT"
            raise AssertionError(f"unexpected prompt_text {label}")

        def fake_prompt_int(label: str) -> int:
            prompts.append(label)
            if label == "要平倉的編號":
                return 1
            raise AssertionError(f"unexpected prompt_int {label}")

        def fake_prompt_decimal(label: str) -> Decimal:
            prompts.append(label)
            if label == "clip_size_token":
                return Decimal("1")
            raise AssertionError(f"unexpected prompt_decimal {label}")

        with mock.patch("scripts.run_execution_once.discover_live_close_candidates_for_symbol", new=fake_discover), \
            mock.patch("scripts.run_execution_once.fetch_close_orderbooks", new=fake_fetch_orderbooks), \
            mock.patch("scripts.run_execution_once.prompt_text", new=fake_prompt_text), \
            mock.patch("scripts.run_execution_once.prompt_int", new=fake_prompt_int), \
            mock.patch("scripts.run_execution_once.prompt_decimal", new=fake_prompt_decimal), \
            mock.patch("scripts.run_execution_once.LIVE_CLOSE_SCAN_VENUES", ["aster", "lighter"]), \
            mock.patch("scripts.run_execution_once.input", return_value="n"):
            await run_close_execution_once()

        self.assertNotIn("leverage_x", prompts)

    async def test_load_live_close_legs_uses_adapter_positions_not_registry_quantities(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("manual-BEAT-1", "beat-short", "aster", "BEAT", "perp", "SHORT", "999"),
                PositionLeg("manual-BEAT-1", "beat-long", "lighter", "BEAT", "perp", "LONG", "999"),
            ]
        )

        class Adapter:
            def __init__(self, side: str, quantity: str) -> None:
                self.side = side
                self.quantity = quantity

            async def get_open_position(self, *, symbol: str, market_type: str):
                return {"symbol": symbol, "market_type": market_type, "side": self.side, "quantity": self.quantity}

        live_legs = await load_live_close_legs(
            registry=registry,
            strategy_id="manual-BEAT-1",
            adapters={
                "aster": Adapter("SHORT", "10"),
                "lighter": Adapter("LONG", "8"),
            },
        )

        self.assertEqual([leg.quantity for leg in live_legs], ["10", "8"])
        self.assertEqual([leg.side for leg in live_legs], ["SHORT", "LONG"])

    async def test_load_live_close_legs_rejects_missing_live_position_query(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("manual-BEAT-1", "beat-short", "aster", "BEAT", "perp", "SHORT", "10"),
                PositionLeg("manual-BEAT-1", "beat-long", "variational", "BEAT", "perp", "LONG", "10"),
            ]
        )

        class Adapter:
            pass

        with self.assertRaisesRegex(RuntimeError, "live position query unavailable"):
            await load_live_close_legs(
                registry=registry,
                strategy_id="manual-BEAT-1",
                adapters={"aster": Adapter(), "variational": Adapter()},
            )

    async def test_load_live_close_candidates_passes_broker_url_to_variational_adapter(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("manual-BEAT-1", "beat-short", "aster", "BEAT", "perp", "SHORT", "10"),
                PositionLeg("manual-BEAT-1", "beat-long", "variational", "BEAT", "perp", "LONG", "10"),
            ]
        )
        calls: list[tuple[str, object]] = []

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def get_open_position(self, *, symbol: str, market_type: str):
                side = "SHORT" if self.venue == "aster" else "LONG"
                return {"symbol": symbol, "market_type": market_type, "side": side, "quantity": "10"}

        def fake_build_adapter(venue: str, *, leverage: int = 1, broker_url=None):
            calls.append((venue, broker_url))
            return Adapter(venue)

        with mock.patch("scripts.run_execution_once.build_adapter_for_venue", new=fake_build_adapter):
            live, errors = await load_live_close_candidates(
                registry=registry,
                broker_url="ws://127.0.0.1:8768",
            )

        self.assertFalse(errors)
        self.assertIn("manual-BEAT-1", live)
        self.assertIn(("variational", "ws://127.0.0.1:8768"), calls)

    async def test_execute_close_position_plan_marks_legs_closed_after_successful_two_leg_close(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("manual-BEAT-1", "beat-short", "aster", "BEAT", "perp", "SHORT", "10"),
                PositionLeg("manual-BEAT-1", "beat-long", "lighter", "BEAT", "perp", "LONG", "10"),
            ]
        )
        plan = build_close_position_plan(
            registry=registry,
            strategy_id="manual-BEAT-1",
            clip_size=Decimal("10"),
            priorities={"aster": 2, "lighter": 0},
            orderbooks={
                "aster": {"bid": 99.0, "ask": 99.2, "ts_ms": 1},
                "lighter": {"bid": 100.0, "ask": 100.05, "ts_ms": 1},
            },
        )
        calls: list[tuple[str, str]] = []

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def place_limit_order(self, **kwargs):
                calls.append((self.venue, kwargs["side"]))
                return {"ok": True, "order_id": f"{self.venue}-maker", "raw": {"status": "FILLED"}}

            async def place_market_order(self, **kwargs):
                calls.append((self.venue, kwargs["side"]))
                return {"ok": True, "order_id": f"{self.venue}-taker"}

        result = await execute_close_position_plan(
            plan=plan,
            registry=registry,
            adapters={"aster": Adapter("aster"), "lighter": Adapter("lighter")},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls, [("aster", "BUY"), ("lighter", "SELL")])
        self.assertEqual(registry.get_leg("beat-short").status, "closed")
        self.assertEqual(registry.get_leg("beat-long").status, "closed")

    async def test_execute_close_position_plan_omits_price_for_variational_maker_mid_button(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("manual-BEAT-1", "beat-short", "variational", "BEAT", "perp", "SHORT", "10"),
                PositionLeg("manual-BEAT-1", "beat-long", "aster", "BEAT", "perp", "LONG", "10"),
            ]
        )
        plan = build_close_position_plan(
            registry=registry,
            strategy_id="manual-BEAT-1",
            clip_size=Decimal("10"),
            priorities={"variational": 2, "aster": 0},
            orderbooks={
                "variational": {"bid": 99.0, "ask": 99.2, "ts_ms": 1},
                "aster": {"bid": 100.0, "ask": 100.05, "ts_ms": 1},
            },
        )
        maker_kwargs: dict = {}

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def place_limit_order(self, **kwargs):
                maker_kwargs.update(kwargs)
                return {"ok": True, "order_id": "maker", "raw": {"status": "FILLED"}}

            async def place_market_order(self, **kwargs):
                return {"ok": True, "order_id": "taker"}

        result = await execute_close_position_plan(
            plan=plan,
            registry=registry,
            adapters={"variational": Adapter("variational"), "aster": Adapter("aster")},
        )

        self.assertTrue(result["ok"])
        self.assertNotIn("price", maker_kwargs)


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

    async def test_get_open_position_sends_broker_position_query(self) -> None:
        await self._runner.cleanup()

        async def position_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                if msg.type != web.WSMsgType.TEXT:
                    continue
                payload = json.loads(msg.data)
                self._messages.append(payload)
                if payload["type"] == "REGISTER":
                    await ws.send_json({"type": "REGISTER_ACK", "ok": True, "role": payload["role"]})
                elif payload["type"] == "GET_OPEN_POSITION":
                    await ws.send_json(
                        {
                            "type": "POSITION_RESULT",
                            "requestId": payload["requestId"],
                            "ok": True,
                            "position": {
                                "symbol": "BEAT",
                                "market_type": "perp",
                                "side": "LONG",
                                "quantity": "10",
                            },
                        }
                    )
            return ws

        self._app = web.Application()
        self._app.router.add_get("/", position_handler)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self._port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://127.0.0.1:{self._port}/"

        adapter = VariationalBrowserExecutionAdapter(broker_url=self._url, client_role="strategy")
        position = await adapter.get_open_position(symbol="BEAT", market_type="perp")

        self.assertEqual(position["side"], "LONG")
        self.assertEqual(position["quantity"], "10")
        self.assertEqual(self._messages[1]["type"], "GET_OPEN_POSITION")

    async def test_cancel_order_sends_broker_cancel_query(self) -> None:
        await self._runner.cleanup()

        async def cancel_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                if msg.type != web.WSMsgType.TEXT:
                    continue
                payload = json.loads(msg.data)
                self._messages.append(payload)
                if payload["type"] == "REGISTER":
                    await ws.send_json({"type": "REGISTER_ACK", "ok": True, "role": payload["role"]})
                elif payload["type"] == "CANCEL_ORDER":
                    await ws.send_json(
                        {
                            "type": "CANCEL_RESULT",
                            "requestId": payload["requestId"],
                            "ok": True,
                            "orderId": payload["orderId"],
                        }
                    )
            return ws

        self._app = web.Application()
        self._app.router.add_get("/", cancel_handler)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self._port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://127.0.0.1:{self._port}/"

        adapter = VariationalBrowserExecutionAdapter(broker_url=self._url, client_role="strategy")
        result = await adapter.cancel_order(
            order_result={"ok": True, "order_id": "var-1"},
            symbol="BEAT",
            side="BUY",
            amount="10",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["orderId"], "var-1")
        self.assertEqual(self._messages[1]["type"], "CANCEL_ORDER")

    async def test_preview_limit_order_price_sends_broker_preview_query(self) -> None:
        await self._runner.cleanup()

        async def preview_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                if msg.type != web.WSMsgType.TEXT:
                    continue
                payload = json.loads(msg.data)
                self._messages.append(payload)
                if payload["type"] == "REGISTER":
                    await ws.send_json({"type": "REGISTER_ACK", "ok": True, "role": payload["role"]})
                elif payload["type"] == "PREVIEW_LIMIT_ORDER_PRICE":
                    await ws.send_json(
                        {
                            "type": "PRICE_PREVIEW_RESULT",
                            "requestId": payload["requestId"],
                            "ok": True,
                            "price": "3.88806",
                        }
                    )
            return ws

        self._app = web.Application()
        self._app.router.add_get("/", preview_handler)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self._port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://127.0.0.1:{self._port}/"

        adapter = VariationalBrowserExecutionAdapter(broker_url=self._url, client_role="strategy")
        result = await adapter.preview_limit_order_price(symbol="BEAT", side="BUY", amount="100")

        self.assertEqual(result["price"], "3.88806")
        self.assertEqual(self._messages[1]["type"], "PREVIEW_LIMIT_ORDER_PRICE")
        self.assertEqual(self._messages[1]["symbol"], "BEAT")

    async def test_preview_limit_order_price_timeout_has_actionable_message(self) -> None:
        await self._runner.cleanup()

        async def silent_preview_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                if msg.type != web.WSMsgType.TEXT:
                    continue
                payload = json.loads(msg.data)
                self._messages.append(payload)
                if payload["type"] == "REGISTER":
                    await ws.send_json({"type": "REGISTER_ACK", "ok": True, "role": payload["role"]})
                elif payload["type"] == "PREVIEW_LIMIT_ORDER_PRICE":
                    continue
            return ws

        self._app = web.Application()
        self._app.router.add_get("/", silent_preview_handler)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self._port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://127.0.0.1:{self._port}/"

        adapter = VariationalBrowserExecutionAdapter(
            broker_url=self._url,
            client_role="strategy",
            timeout_seconds=0.2,
        )
        with self.assertRaisesRegex(RuntimeError, "reload the Variational Chrome extension"):
            await adapter.preview_limit_order_price(symbol="BEAT", side="BUY", amount="100")

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

    async def test_place_limit_order_timeout_keeps_submitted_order_id_on_exception(self) -> None:
        async def delayed_fill_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                if msg.type != web.WSMsgType.TEXT:
                    continue
                payload = json.loads(msg.data)
                if payload["type"] == "REGISTER":
                    await ws.send_json({"type": "REGISTER_ACK", "ok": True, "role": payload["role"]})
                elif payload["type"] == "PLACE_ORDER":
                    await ws.send_json({"type": "ORDER_DISPATCHED", "requestId": payload["requestId"], "ok": True})
                    await ws.send_json(
                        {
                            "type": "ORDER_RESULT",
                            "requestId": payload["requestId"],
                            "ok": True,
                            "orderId": "var-timeout",
                        }
                    )
                    await asyncio.sleep(1.0)
            return ws

        await self._runner.cleanup()
        self._app = web.Application()
        self._app.router.add_get("/", delayed_fill_handler)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self._port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://127.0.0.1:{self._port}/"

        adapter = VariationalBrowserExecutionAdapter(
            broker_url=self._url,
            client_role="strategy",
            fill_timeout_seconds=0.05,
        )

        with self.assertRaisesRegex(RuntimeError, "variational limit order fill timeout after 0s") as ctx:
            await adapter.place_limit_order(symbol="BTC", side="BUY", amount="50")

        self.assertEqual(getattr(ctx.exception, "order_result", {})["orderId"], "var-timeout")

    async def test_cancel_order_uses_nested_submitted_order_id(self) -> None:
        await self._runner.cleanup()

        async def cancel_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                if msg.type != web.WSMsgType.TEXT:
                    continue
                payload = json.loads(msg.data)
                self._messages.append(payload)
                if payload["type"] == "REGISTER":
                    await ws.send_json({"type": "REGISTER_ACK", "ok": True, "role": payload["role"]})
                elif payload["type"] == "CANCEL_ORDER":
                    await ws.send_json(
                        {
                            "type": "CANCEL_RESULT",
                            "requestId": payload["requestId"],
                            "ok": True,
                            "orderId": payload["orderId"],
                        }
                    )
            return ws

        self._app = web.Application()
        self._app.router.add_get("/", cancel_handler)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self._port = self._site._server.sockets[0].getsockname()[1]
        self._url = f"http://127.0.0.1:{self._port}/"

        adapter = VariationalBrowserExecutionAdapter(broker_url=self._url, client_role="strategy")
        result = await adapter.cancel_order(
            order_result={
                "ok": False,
                "details": {
                    "submitted": {
                        "orderId": "var-nested",
                    }
                },
            },
            symbol="BEAT",
            side="BUY",
            amount="10",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(self._messages[1]["orderId"], "var-nested")

    async def test_failed_order_writes_debug_payload_when_path_is_configured(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            debug_path = Path(temp_dir) / "variational_order_debug.json"

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
                                "details": {"buttons": [{"text": "Order History"}]},
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

            adapter = VariationalBrowserExecutionAdapter(
                broker_url=self._url,
                client_role="strategy",
                debug_payload_path=debug_path,
            )

            with self.assertRaises(RuntimeError):
                await adapter.place_limit_order(symbol="BTC", side="BUY", amount="50")

            payload = json.loads(debug_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["error"], "browser reject")
            self.assertEqual(payload["details"]["buttons"][0]["text"], "Order History")

    def test_build_place_order_payload_uses_expected_command_shape(self) -> None:
        payload = build_place_order_payload(
            request_id="req-1",
            symbol="BTC",
            side="BUY",
            amount="50",
            order_type="limit",
            price="61000",
            market="BTC-PERP",
            account="main",
            timeout_ms=5000,
        )

        self.assertEqual(payload["type"], "PLACE_ORDER")
        self.assertEqual(payload["requestId"], "req-1")
        self.assertEqual(payload["symbol"], "BTC")
        self.assertEqual(payload["orderType"], "LIMIT")
        self.assertEqual(payload["price"], "61000")
        self.assertEqual(payload["market"], "BTC-PERP")


class LighterExecutionAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_compute_base_quantity_from_clip_usd_uses_mid_price(self) -> None:
        quantity = compute_base_quantity_from_clip_usd(
            clip_usd=1000.0,
            orderbook={"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        self.assertEqual(quantity, Decimal("10"))

    async def test_build_lighter_market_order_request_uses_aggressive_limit_price(self) -> None:
        request = build_lighter_market_order_request(
            side="buy",
            quantity=Decimal("10"),
            orderbook={"bid": 99.0, "ask": 101.0, "ts_ms": 1},
            base_amount_multiplier=1000,
            price_multiplier=100,
            slippage_bps=100.0,
            market_index=7,
            client_order_index=123,
        )

        self.assertEqual(request["market_index"], 7)
        self.assertEqual(request["client_order_index"], 123)
        self.assertEqual(request["base_amount"], 10000)
        self.assertEqual(request["price"], 10201)
        self.assertFalse(request["is_ask"])

    async def test_build_lighter_limit_order_request_uses_explicit_passive_price(self) -> None:
        request = build_lighter_limit_order_request(
            side="sell",
            quantity=Decimal("10"),
            price=Decimal("100.25"),
            base_amount_multiplier=1000,
            price_multiplier=100,
            market_index=7,
            client_order_index=123,
        )

        self.assertEqual(request["market_index"], 7)
        self.assertEqual(request["client_order_index"], 123)
        self.assertEqual(request["base_amount"], 10000)
        self.assertEqual(request["price"], 10025)
        self.assertTrue(request["is_ask"])

    async def test_build_lighter_market_order_request_rejects_below_min_base_or_quote(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "min_base_amount=0.005"):
            build_lighter_market_order_request(
                side="buy",
                quantity=Decimal("0.003"),
                orderbook={"bid": 1766.5, "ask": 1766.54, "ts_ms": 1},
                base_amount_multiplier=10000,
                price_multiplier=100,
                slippage_bps=100.0,
                market_index=0,
                min_base_amount=Decimal("0.005"),
                min_quote_amount=Decimal("10"),
            )

    async def test_place_market_order_calls_create_order_with_expected_fields(self) -> None:
        class FakeSignerClient:
            ORDER_TYPE_LIMIT = "limit"
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = "gtt"

            def __init__(self) -> None:
                self.calls = []

            async def create_order(self, **kwargs):
                self.calls.append(kwargs)
                return None, "tx-hash", None

        adapter = LighterExecutionAdapter(
            signer_client_factory=lambda: FakeSignerClient(),
            market_config_loader=lambda symbol: (7, 1000, 100),
            orderbook_loader=lambda symbol: {"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        result = await adapter.place_market_order(symbol="BTC", side="BUY", amount="10", clip_usd=1000.0)

        self.assertTrue(result["ok"])
        self.assertEqual(result["tx_hash"], "tx-hash")
        call = adapter.client.calls[0]
        self.assertEqual(call["market_index"], 7)
        self.assertEqual(call["base_amount"], 10000)
        self.assertEqual(call["price"], 10201)
        self.assertFalse(call["is_ask"])

    async def test_place_limit_order_calls_create_order_with_explicit_price(self) -> None:
        class FakeSignerClient:
            ORDER_TYPE_LIMIT = "limit"
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = "gtt"

            def __init__(self) -> None:
                self.calls = []

            async def create_order(self, **kwargs):
                self.calls.append(kwargs)
                return None, "tx-hash", None

        adapter = LighterExecutionAdapter(
            signer_client_factory=lambda: FakeSignerClient(),
            market_config_loader=lambda symbol: (7, 1000, 100),
            orderbook_loader=lambda symbol: {"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        result = await adapter.place_limit_order(
            symbol="BTC",
            side="SELL",
            amount="10",
            clip_usd=1000.0,
            price="100.25",
        )

        self.assertTrue(result["ok"])
        call = adapter.client.calls[0]
        self.assertEqual(call["market_index"], 7)
        self.assertEqual(call["base_amount"], 10000)
        self.assertEqual(call["price"], 10025)
        self.assertTrue(call["is_ask"])
        self.assertFalse(call["reduce_only"])

    async def test_place_market_order_accepts_async_market_and_orderbook_loaders(self) -> None:
        class FakeSignerClient:
            ORDER_TYPE_LIMIT = "limit"
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = "gtt"

            def __init__(self) -> None:
                self.calls = []

            async def create_order(self, **kwargs):
                self.calls.append(kwargs)
                return None, "tx-hash", None

        async def load_market(symbol):
            return (9, 100, 10)

        async def load_orderbook(symbol):
            return {"bid": 10.0, "ask": 10.1, "ts_ms": 1}

        adapter = LighterExecutionAdapter(
            signer_client_factory=lambda: FakeSignerClient(),
            market_config_loader=load_market,
            orderbook_loader=load_orderbook,
        )

        result = await adapter.place_market_order(symbol="ETH", side="SELL", amount="2", clip_usd=20.0)

        self.assertTrue(result["ok"])
        call = adapter.client.calls[0]
        self.assertEqual(call["market_index"], 9)
        self.assertTrue(call["is_ask"])

    async def test_wait_for_order_fill_polls_client_order_index_until_filled(self) -> None:
        class FakeSignerClient:
            def __init__(self) -> None:
                self.calls = 0

            async def get_order(self, client_order_index):
                self.calls += 1
                if self.calls == 1:
                    return {"status": "open", "client_order_index": client_order_index}
                return {"status": "filled", "client_order_index": client_order_index}

        adapter = LighterExecutionAdapter(
            signer_client_factory=lambda: FakeSignerClient(),
            market_config_loader=lambda symbol: (7, 1000, 100),
            orderbook_loader=lambda symbol: {"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        result = await adapter.wait_for_order_fill(
            order_result={"ok": True, "client_order_index": 123},
            symbol="BTC",
            side="SELL",
            amount="1",
            timeout_seconds=1.0,
            poll_interval_seconds=0.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["raw"]["status"], "filled")

    async def test_wait_for_order_fill_times_out_when_lighter_order_stays_open(self) -> None:
        class FakeSignerClient:
            async def get_order(self, client_order_index):
                return {"status": "open", "client_order_index": client_order_index}

        adapter = LighterExecutionAdapter(
            signer_client_factory=lambda: FakeSignerClient(),
            market_config_loader=lambda symbol: (7, 1000, 100),
            orderbook_loader=lambda symbol: {"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        with self.assertRaisesRegex(RuntimeError, "lighter limit order fill timeout"):
            await adapter.wait_for_order_fill(
                order_result={"ok": True, "client_order_index": 123},
                symbol="BTC",
                side="SELL",
                amount="1",
                timeout_seconds=0.01,
                poll_interval_seconds=0.0,
            )


class LighterLiveFactoryTests(unittest.TestCase):
    def test_build_lighter_client_factory_reads_env_and_checks_client(self) -> None:
        class FakeSignerClient:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

            def check_client(self):
                return None

        with mock.patch.dict(
            os.environ,
            {
                "LIGHTER_ACCOUNT_INDEX": "11",
                "LIGHTER_API_KEY_INDEX": "7",
                "LIGHTER_PRIVATE_KEY": "demo-private-key",
            },
            clear=False,
        ):
            with mock.patch(
                "hydra_basis.execution_engine.lighter_live.import_lighter_signer_client",
                return_value=FakeSignerClient,
            ):
                factory = build_lighter_client_factory_from_env(base_url="https://lighter.test")
                client = factory()

        self.assertEqual(client.kwargs["url"], "https://lighter.test")
        self.assertEqual(client.kwargs["account_index"], 11)
        self.assertEqual(client.kwargs["api_private_keys"], {7: "demo-private-key"})


class SingleClipExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_execute_single_clip_with_sides_waits_for_maker_fill_before_hedging(self) -> None:
        calls: list[tuple[str, str]] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append(("maker_submit", kwargs["side"]))
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append(("maker_filled", kwargs["side"]))
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "FILLED"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append(("taker_market", kwargs["side"]))
                return {"ok": True, "order_id": "taker-1"}

        result = await execute_single_clip_with_sides(
            symbol="BEAT",
            clip_usd=1000.0,
            quantity=Decimal("10"),
            maker_venue="variational",
            taker_venue="aster",
            maker_side="SELL",
            taker_side="BUY",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=5.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls, [("maker_submit", "SELL"), ("maker_filled", "SELL"), ("taker_market", "BUY")])

    async def test_execute_single_clip_passes_maker_price_when_provided(self) -> None:
        maker_kwargs: dict[str, object] = {}

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                maker_kwargs.update(kwargs)
                return {"ok": True, "orderId": "maker-1"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                return {"ok": True, "orderId": "taker-1"}

        await execute_single_clip(
            symbol="BTC",
            clip_usd=1000.0,
            quantity=Decimal("0.01"),
            maker_venue="mexc_spot",
            taker_venue="aster",
            short_venue="mexc_spot",
            long_venue="aster",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            maker_price="60000",
        )

        self.assertEqual(maker_kwargs["price"], "60000")

    async def test_execute_single_clip_defaults_maker_price_from_orderbook_when_missing(self) -> None:
        maker_kwargs: dict[str, object] = {}

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                maker_kwargs.update(kwargs)
                return {"ok": True, "orderId": "maker-1"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                return {"ok": True, "orderId": "taker-1"}

        await execute_single_clip(
            symbol="BTC",
            clip_usd=1000.0,
            quantity=Decimal("0.01"),
            maker_venue="aster",
            taker_venue="variational",
            short_venue="aster",
            long_venue="variational",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            maker_orderbook={"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        self.assertEqual(maker_kwargs["price"], "101")

    async def test_execute_single_clip_retries_hedge_once_then_completes(self) -> None:
        calls: list[tuple[str, str]] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append(("maker", kwargs["side"]))
                return {"ok": True, "orderId": "var-1"}

        class TakerAdapter:
            def __init__(self) -> None:
                self.attempt = 0

            async def place_market_order(self, **kwargs):
                self.attempt += 1
                calls.append(("taker", kwargs["side"]))
                if self.attempt == 1:
                    raise RuntimeError("temporary failure")
                return {"ok": True, "orderId": "lighter-1"}

        state_machine = ExecutionStateMachine()
        result = await execute_single_clip(
            symbol="BTC",
            clip_usd=1000.0,
            quantity=Decimal("10"),
            maker_venue="variational",
            taker_venue="lighter",
            short_venue="variational",
            long_venue="lighter",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=2,
            state_machine=state_machine,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(state_machine.state, "completed")
        self.assertEqual(calls, [("maker", "SELL"), ("taker", "BUY"), ("taker", "BUY")])

    async def test_execute_single_clip_waits_for_maker_fill_before_hedging(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append("maker_filled")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "FILLED"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True, "order_id": "taker-1"}

        result = await execute_single_clip(
            symbol="BTC",
            clip_usd=1000.0,
            quantity=Decimal("10"),
            maker_venue="variational",
            taker_venue="lighter",
            short_venue="variational",
            long_venue="lighter",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=5.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls, ["maker_submit", "maker_filled", "taker_market"])

    async def test_execute_single_clip_cancels_and_replaces_maker_after_fill_timeout(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            def __init__(self) -> None:
                self.submit_count = 0

            async def place_limit_order(self, **kwargs):
                self.submit_count += 1
                calls.append(f"maker_submit_{self.submit_count}")
                return {"ok": True, "order_id": f"maker-{self.submit_count}", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append(f"wait_{kwargs['order_result']['order_id']}")
                if kwargs["order_result"]["order_id"] == "maker-1":
                    raise RuntimeError("maker fill timeout")
                return {"ok": True, "order_id": "maker-2", "raw": {"status": "FILLED"}}

            async def cancel_order(self, **kwargs):
                calls.append(f"cancel_{kwargs['order_result']['order_id']}")
                return {"ok": True, "order_id": kwargs["order_result"]["order_id"]}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True, "order_id": "taker-1"}

        result = await execute_single_clip(
            symbol="BTC",
            clip_usd=1000.0,
            quantity=Decimal("10"),
            maker_venue="variational",
            taker_venue="lighter",
            short_venue="variational",
            long_venue="lighter",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=5.0,
            max_maker_reprice_attempts=1,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(
            calls,
            ["maker_submit_1", "wait_maker-1", "cancel_maker-1", "maker_submit_2", "wait_maker-2", "taker_market"],
        )

    async def test_execute_single_clip_cancels_variational_order_when_submit_phase_timeout_carries_order_id(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            def __init__(self) -> None:
                self.submit_count = 0

            async def place_limit_order(self, **kwargs):
                self.submit_count += 1
                calls.append(f"maker_submit_{self.submit_count}")
                if self.submit_count == 1:
                    error = RuntimeError("variational limit order fill timeout after 60s")
                    setattr(error, "order_result", {"ok": True, "orderId": "maker-timeout-1"})
                    raise error
                return {"ok": True, "order_id": "maker-2", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append(f"wait_{kwargs['order_result']['order_id']}")
                return {"ok": True, "order_id": "maker-2", "raw": {"status": "FILLED"}}

            async def cancel_order(self, **kwargs):
                order_id = kwargs["order_result"].get("order_id") or kwargs["order_result"].get("orderId")
                calls.append(f"cancel_{order_id}")
                return {"ok": True, "order_id": order_id}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True, "order_id": "taker-1"}

        result = await execute_single_clip(
            symbol="BTC",
            clip_usd=1000.0,
            quantity=Decimal("10"),
            maker_venue="variational",
            taker_venue="lighter",
            short_venue="variational",
            long_venue="lighter",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=5.0,
            max_maker_reprice_attempts=1,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(
            calls,
            ["maker_submit_1", "cancel_maker-timeout-1", "maker_submit_2", "wait_maker-2", "taker_market"],
        )

    async def test_execute_single_clip_does_not_reprice_non_timeout_fill_errors(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append("maker_wait")
                raise RuntimeError("maker fill websocket disconnected")

            async def cancel_order(self, **kwargs):
                calls.append("maker_cancel")
                return {"ok": True}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True}

        with self.assertRaisesRegex(RuntimeError, "websocket disconnected"):
            await execute_single_clip(
                symbol="BTC",
                clip_usd=1000.0,
                quantity=Decimal("10"),
                maker_venue="variational",
                taker_venue="lighter",
                short_venue="variational",
                long_venue="lighter",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=1,
                state_machine=ExecutionStateMachine(),
                require_maker_fill_confirmation=True,
                maker_fill_timeout_seconds=5.0,
                max_maker_reprice_attempts=1,
            )

        self.assertEqual(calls, ["maker_submit", "maker_wait"])

    async def test_execute_single_clip_stops_before_any_order_when_price_gap_exceeds_limit(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append("maker_filled")
                return {"ok": True, "raw": {"status": "FILLED", "avgPrice": "100"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True, "raw": {"avgPrice": "102"}}

        with self.assertRaisesRegex(RuntimeError, "pre-trade maker/taker price gap"):
            await execute_single_clip(
                symbol="BTC",
                clip_usd=1000.0,
                quantity=Decimal("10"),
                maker_venue="variational",
                taker_venue="aster",
                short_venue="variational",
                long_venue="aster",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=1,
                state_machine=ExecutionStateMachine(),
                require_maker_fill_confirmation=True,
                maker_fill_timeout_seconds=5.0,
                maker_orderbook={"bid": 99.0, "ask": 100.0, "ts_ms": 1},
                taker_orderbook={"bid": 99.0, "ask": 102.0, "ts_ms": 1},
                max_execution_price_gap_pct=0.01,
            )

        self.assertEqual(calls, [])

    async def test_execute_single_clip_uses_adapter_price_preview_before_variational_maker_order(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def preview_limit_order_price(self, **kwargs):
                calls.append("preview_price")
                return {"ok": True, "price": "3.88806"}

            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append("maker_filled")
                return {"ok": True, "raw": {"status": "FILLED", "avgPrice": "3.88806"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True, "raw": {"avgPrice": "3.89"}}

        result = await execute_single_clip(
            symbol="BEAT",
            clip_usd=388.8,
            quantity=Decimal("100"),
            maker_venue="variational",
            taker_venue="aster",
            short_venue="aster",
            long_venue="variational",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            maker_orderbook={"bid": 3.82, "ask": 3.83, "ts_ms": 1},
            use_maker_orderbook_price=False,
            taker_orderbook={"bid": 3.88, "ask": 3.89, "ts_ms": 1},
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=5.0,
            max_execution_price_gap_pct=0.01,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls, ["preview_price", "maker_submit", "maker_filled", "taker_market"])
        self.assertEqual(result["execution_price_summary"]["pre_trade"]["maker_price"], "3.88806")

    async def test_execute_single_clip_returns_average_prices_and_price_gap(self) -> None:
        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                return {"ok": True, "raw": {"status": "FILLED", "avgPrice": "100"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                return {"ok": True, "raw": {"avgPrice": "100.5"}}

        result = await execute_single_clip(
            symbol="BTC",
            clip_usd=1000.0,
            quantity=Decimal("10"),
            maker_venue="variational",
            taker_venue="aster",
            short_venue="variational",
            long_venue="aster",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=1,
            state_machine=ExecutionStateMachine(),
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=5.0,
            taker_orderbook={"bid": 99.0, "ask": 100.5, "ts_ms": 1},
            max_execution_price_gap_pct=0.01,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["execution_price_summary"]["maker_avg_price"], "100")
        self.assertEqual(result["execution_price_summary"]["taker_avg_price"], "100.5")
        self.assertEqual(result["execution_price_summary"]["price_gap_pct"], "0.004987531172069825436408977556")

    async def test_execute_single_clip_does_not_hedge_without_maker_fill_confirmation(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True, "order_id": "taker-1"}

        with self.assertRaisesRegex(RuntimeError, "maker fill confirmation unavailable"):
            await execute_single_clip(
                symbol="BTC",
                clip_usd=1000.0,
                quantity=Decimal("10"),
                maker_venue="variational",
                taker_venue="lighter",
                short_venue="variational",
                long_venue="lighter",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=1,
                state_machine=ExecutionStateMachine(),
                require_maker_fill_confirmation=True,
                maker_fill_timeout_seconds=5.0,
            )

        self.assertEqual(calls, ["maker_submit"])

    async def test_execute_single_clip_enters_emergency_exit_after_hedge_failures(self) -> None:
        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                return {"ok": True, "orderId": "var-1"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                raise RuntimeError("permanent failure")

        state_machine = ExecutionStateMachine()
        with self.assertRaises(RuntimeError):
            await execute_single_clip(
                symbol="BTC",
                clip_usd=1000.0,
                quantity=Decimal("10"),
                maker_venue="variational",
                taker_venue="lighter",
                short_venue="variational",
                long_venue="lighter",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=1,
                state_machine=state_machine,
            )

        self.assertEqual(state_machine.state, "emergency_exit")

    def test_execution_sides_for_signal_match_short_maker_case(self) -> None:
        maker_side, taker_side = execution_sides_for_signal(
            maker_venue="variational",
            short_venue="variational",
            long_venue="lighter",
        )
        self.assertEqual(maker_side, "SELL")
        self.assertEqual(taker_side, "BUY")


class MexcSpotExecutionAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_signed_spot_request_includes_extended_recv_window(self) -> None:
        class Adapter(MexcSpotExecutionAdapter):
            def _timestamp_ms(self) -> int:
                return 1234567890

        adapter = Adapter(api_key="k", api_secret="s")
        params = adapter._signed_order_params({"symbol": "ETHUSDT"})

        self.assertEqual(params["timestamp"], 1234567890)
        self.assertEqual(params["recvWindow"], 60000)
        self.assertIn("signature", params)

    async def test_signed_spot_order_request_uses_query_params_with_json_content_type(self) -> None:
        post_calls: list[dict] = []

        class Response:
            status = 200

            async def __aenter__(self) -> "Response":
                return self

            async def __aexit__(self, exc_type, exc, tb) -> None:
                return None

            async def json(self) -> dict:
                return {"orderId": "spot-1"}

        class Session:
            async def __aenter__(self) -> "Session":
                return self

            async def __aexit__(self, exc_type, exc, tb) -> None:
                return None

            def post(self, url: str, **kwargs) -> Response:
                post_calls.append({"url": url, **kwargs})
                return Response()

        class Adapter(MexcSpotExecutionAdapter):
            def _timestamp_ms(self) -> int:
                return 1234567890

        original_session = mexc_spot_adapter.aiohttp.ClientSession
        mexc_spot_adapter.aiohttp.ClientSession = Session
        self.addCleanup(setattr, mexc_spot_adapter.aiohttp, "ClientSession", original_session)

        adapter = Adapter(api_key="k", api_secret="s")
        await adapter._post_order(
            {
                "symbol": "ETHUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "quantity": "0.1",
                "price": "3000",
            }
        )

        self.assertEqual(
            post_calls[0]["headers"],
            {
                "X-MEXC-APIKEY": "k",
                "Content-Type": "application/json",
            },
        )
        self.assertNotIn("data", post_calls[0])
        self.assertEqual(post_calls[0]["params"]["timestamp"], 1234567890)
        self.assertIn("signature", post_calls[0]["params"])

    async def test_place_order_rejects_missing_spot_credentials_before_http(self) -> None:
        adapter = MexcSpotExecutionAdapter(api_key="", api_secret="")

        with self.assertRaisesRegex(RuntimeError, "MEXC spot API credentials missing"):
            await adapter.place_market_order(
                symbol="ETH",
                side="BUY",
                amount="0.1",
                clip_usd=0.0,
            )

    async def test_place_market_order_posts_spot_market_quantity(self) -> None:
        calls: list[dict] = []

        class Adapter(MexcSpotExecutionAdapter):
            async def _post_order(self, params: dict) -> dict:
                calls.append(dict(params))
                return {"orderId": "spot-1"}

        result = await Adapter(api_key="k", api_secret="s").place_market_order(
            symbol="eth",
            side="BUY",
            amount="0.1",
            clip_usd=0.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["symbol"], "ETHUSDT")
        self.assertEqual(calls[0]["side"], "BUY")
        self.assertEqual(calls[0]["type"], "MARKET")
        self.assertEqual(calls[0]["quantity"], "0.1")

    async def test_place_limit_order_posts_spot_limit_price(self) -> None:
        calls: list[dict] = []

        class Adapter(MexcSpotExecutionAdapter):
            async def _post_order(self, params: dict) -> dict:
                calls.append(dict(params))
                return {"orderId": "spot-2"}

        result = await Adapter(api_key="k", api_secret="s").place_limit_order(
            symbol="eth",
            side="SELL",
            amount="0.1",
            clip_usd=0.0,
            price="3000.5",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["symbol"], "ETHUSDT")
        self.assertEqual(calls[0]["side"], "SELL")
        self.assertEqual(calls[0]["type"], "LIMIT")
        self.assertEqual(calls[0]["price"], "3000.5")
        self.assertEqual(calls[0]["timeInForce"], "GTC")

    async def test_wait_for_order_fill_polls_spot_order_until_filled(self) -> None:
        class Adapter(MexcSpotExecutionAdapter):
            def __init__(self) -> None:
                super().__init__(api_key="k", api_secret="s")
                self.calls = 0

            async def _get_order(self, params: dict) -> dict:
                self.calls += 1
                if self.calls == 1:
                    return {"status": "NEW", "orderId": params["orderId"]}
                return {"status": "FILLED", "orderId": params["orderId"]}

        result = await Adapter().wait_for_order_fill(
            order_result={"ok": True, "order_id": 42},
            symbol="ETH",
            side="BUY",
            amount="0.1",
            timeout_seconds=1.0,
            poll_interval_seconds=0.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["raw"]["status"], "FILLED")

    async def test_wait_for_order_fill_times_out_when_spot_order_stays_new(self) -> None:
        class Adapter(MexcSpotExecutionAdapter):
            async def _get_order(self, params: dict) -> dict:
                return {"status": "NEW", "orderId": params["orderId"]}

        with self.assertRaisesRegex(RuntimeError, "mexc spot limit order fill timeout"):
            await Adapter(api_key="k", api_secret="s").wait_for_order_fill(
                order_result={"ok": True, "order_id": 42},
                symbol="ETH",
                side="BUY",
                amount="0.1",
                timeout_seconds=0.01,
                poll_interval_seconds=0.0,
            )

    async def test_cancel_order_posts_signed_spot_cancel(self) -> None:
        calls: list[dict] = []

        class Adapter(MexcSpotExecutionAdapter):
            async def _delete_order(self, params: dict) -> dict:
                calls.append(dict(params))
                return {"orderId": "spot-2", "status": "CANCELED"}

        result = await Adapter(api_key="k", api_secret="s").cancel_order(
            order_result={"ok": True, "order_id": "spot-2"},
            symbol="eth",
            side="BUY",
            amount="0.1",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["symbol"], "ETHUSDT")
        self.assertEqual(calls[0]["orderId"], "spot-2")
        self.assertEqual(result["raw"]["status"], "CANCELED")

    async def test_mexc_spot_get_open_position_returns_free_and_locked_balance(self) -> None:
        class Adapter(MexcSpotExecutionAdapter):
            async def _get_account(self) -> dict:
                return {
                    "balances": [
                        {"asset": "ETH", "free": "0.2", "locked": "0.1"},
                    ]
                }

        position = await Adapter(api_key="k", api_secret="s").get_open_position(symbol="ETH", market_type="spot")

        self.assertEqual(position["side"], "LONG")
        self.assertEqual(position["quantity"], "0.3")


class AsterOrderFillWatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_for_order_fill_polls_aster_order_until_filled(self) -> None:
        class Adapter(AsterExecutionAdapter):
            def __init__(self) -> None:
                super().__init__(signer_address="0x1", private_key="unused", user_address="0x2")
                self.calls = 0

            async def _resolve_raw_symbol(self, symbol: str) -> str:
                return "ETHUSDT"

            async def _get_order_status(self, *, symbol: str, order_id: object) -> dict:
                self.calls += 1
                if self.calls == 1:
                    return {"status": "NEW", "orderId": order_id}
                return {"status": "FILLED", "orderId": order_id}

        result = await Adapter().wait_for_order_fill(
            order_result={"ok": True, "order_id": 123},
            symbol="ETH",
            side="BUY",
            amount="0.1",
            timeout_seconds=1.0,
            poll_interval_seconds=0.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["raw"]["status"], "FILLED")

    async def test_aster_get_open_position_returns_live_side_and_quantity(self) -> None:
        class Adapter(AsterExecutionAdapter):
            def __init__(self) -> None:
                super().__init__(signer_address="0x1", private_key="unused", user_address="0x2")

            async def _fetch_position_risk(self) -> list[dict]:
                return [
                    {"symbol": "ETHUSDT", "positionAmt": "-0.25", "positionSide": "BOTH"},
                    {"symbol": "BTCUSDT", "positionAmt": "0", "positionSide": "BOTH"},
                ]

        position = await Adapter().get_open_position(symbol="ETH", market_type="perp")

        self.assertEqual(position["symbol"], "ETH")
        self.assertEqual(position["side"], "SHORT")
        self.assertEqual(position["quantity"], "0.25")


class MexcFuturesOrderFillWatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_for_order_fill_polls_contract_order_until_filled(self) -> None:
        class Adapter(MexcExecutionAdapter):
            def __init__(self) -> None:
                super().__init__(api_key="k", api_secret="s")
                self.calls = 0

            async def _get_order_status(self, order_id: object) -> dict:
                self.calls += 1
                if self.calls == 1:
                    return {"state": 2, "orderId": order_id}
                return {"state": 3, "orderId": order_id}

        result = await Adapter().wait_for_order_fill(
            order_result={"ok": True, "order_id": "abc"},
            symbol="ETH",
            side="BUY",
            amount="0.1",
            timeout_seconds=1.0,
            poll_interval_seconds=0.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["raw"]["state"], 3)

    async def test_mexc_futures_get_open_position_returns_live_side_and_quantity(self) -> None:
        class Adapter(MexcExecutionAdapter):
            async def _get_open_positions(self, symbol: str) -> list[dict]:
                return [
                    {"symbol": "ETH_USDT", "positionType": 2, "holdVol": "12"},
                ]

        position = await Adapter(api_key="k", api_secret="s").get_open_position(symbol="ETH", market_type="perp")

        self.assertEqual(position["side"], "SHORT")
        self.assertEqual(position["quantity"], "12")


class HyperliquidOrderFillWatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_for_order_fill_polls_order_status_until_filled(self) -> None:
        class Adapter(HyperliquidExecutionAdapter):
            def __init__(self) -> None:
                self.private_key = "unused"
                self.account_address = "0xabc"
                self.slippage_bps = 50.0
                self.default_leverage = 1
                self._universe = ["ETH"]
                self._isolated_asset_indices = set()
                self.calls = 0

            async def _get_order_status(self, order_id: object) -> dict:
                self.calls += 1
                if self.calls == 1:
                    return {"status": "open", "order": {"status": "open", "oid": order_id}}
                return {"status": "order", "order": {"status": "filled", "oid": order_id}}

        result = await Adapter().wait_for_order_fill(
            order_result={"ok": True, "order_id": 456},
            symbol="ETH",
            side="BUY",
            amount="0.1",
            timeout_seconds=1.0,
            poll_interval_seconds=0.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["raw"]["order"]["status"], "filled")

    async def test_hyperliquid_get_open_position_returns_live_side_and_quantity(self) -> None:
        class Adapter(HyperliquidExecutionAdapter):
            def __init__(self) -> None:
                self.account_address = "0xabc"

            async def _fetch_clearinghouse_state(self) -> dict:
                return {
                    "assetPositions": [
                        {"position": {"coin": "ETH", "szi": "-0.5"}},
                    ]
                }

        position = await Adapter().get_open_position(symbol="ETH", market_type="perp")

        self.assertEqual(position["side"], "SHORT")
        self.assertEqual(position["quantity"], "0.5")


class LighterLivePositionTests(unittest.IsolatedAsyncioTestCase):
    async def test_lighter_get_open_position_uses_supported_client_position_method(self) -> None:
        class Client:
            async def get_positions(self):
                return {
                    "positions": [
                        {"symbol": "ETH", "position": "-1.5"},
                    ]
                }

        adapter = LighterExecutionAdapter(
            signer_client_factory=lambda: Client(),
            market_config_loader=lambda symbol: {},
            orderbook_loader=lambda symbol: {},
        )

        position = await adapter.get_open_position(symbol="ETH", market_type="perp")

        self.assertEqual(position["side"], "SHORT")
        self.assertEqual(position["quantity"], "1.5")

    async def test_lighter_get_open_position_rejects_when_no_client_method_and_snapshot_has_no_positions(self) -> None:
        class Adapter(LighterExecutionAdapter):
            async def _fetch_account_snapshot(self) -> dict[str, object]:
                return {"account_index": 1}

        adapter = Adapter(
            signer_client_factory=lambda: object(),
            market_config_loader=lambda symbol: {},
            orderbook_loader=lambda symbol: {},
        )

        with self.assertRaisesRegex(RuntimeError, "account snapshot had no positions"):
            await adapter.get_open_position(symbol="ETH", market_type="perp")

    async def test_lighter_get_open_position_falls_back_to_account_snapshot(self) -> None:
        class Adapter(LighterExecutionAdapter):
            async def _fetch_account_snapshot(self) -> dict[str, object]:
                return {
                    "positions": [
                        {"symbol": "BTC", "sign": 1, "position": "0"},
                        {"symbol": "ETH", "sign": -1, "position": "-2.5"},
                    ]
                }

        adapter = Adapter(
            signer_client_factory=lambda: object(),
            market_config_loader=lambda symbol: {},
            orderbook_loader=lambda symbol: {},
        )

        position = await adapter.get_open_position(symbol="ETH", market_type="perp")

        self.assertEqual(position["side"], "SHORT")
        self.assertEqual(position["quantity"], "2.5")

    async def test_lighter_get_open_position_matches_snapshot_by_market_id_when_symbol_missing(self) -> None:
        class Adapter(LighterExecutionAdapter):
            async def _fetch_account_snapshot(self) -> dict[str, object]:
                return {
                    "positions": [
                        {"market_id": 88, "sign": 1, "position": "0"},
                        {"market_id": 99, "sign": 1, "position": "123.45"},
                    ]
                }

        adapter = Adapter(
            signer_client_factory=lambda: object(),
            market_config_loader=lambda symbol: {"market_index": 99},
            orderbook_loader=lambda symbol: {},
        )

        position = await adapter.get_open_position(symbol="PROVE", market_type="perp")

        self.assertEqual(position["symbol"], "PROVE")
        self.assertEqual(position["side"], "LONG")
        self.assertEqual(position["quantity"], "123.45")

    async def test_lighter_get_open_position_falls_back_to_snapshot_when_client_positions_are_empty(self) -> None:
        class Client:
            async def get_positions(self):
                return {"positions": []}

        class Adapter(LighterExecutionAdapter):
            async def _fetch_account_snapshot(self) -> dict[str, object]:
                return {
                    "accounts": [
                        {
                            "positions": [
                                {"market_index": 99, "sign": 1, "position": "100"},
                            ],
                        }
                    ]
                }

        adapter = Adapter(
            signer_client_factory=lambda: Client(),
            market_config_loader=lambda symbol: {"market_index": 99},
            orderbook_loader=lambda symbol: {},
        )

        position = await adapter.get_open_position(symbol="PROVE", market_type="perp")

        self.assertEqual(position["symbol"], "PROVE")
        self.assertEqual(position["side"], "LONG")
        self.assertEqual(position["quantity"], "100")


class ExecutionPreviewCliTests(unittest.TestCase):
    def test_compute_batch_count_rounds_up(self) -> None:
        self.assertEqual(compute_batch_count(10000.0, 500.0), 20)
        self.assertEqual(compute_batch_count(10250.0, 500.0), 21)
        self.assertEqual(compute_single_clip_batch_count(10250.0, 500.0), 21)
        self.assertEqual(compute_token_batch_count(Decimal("10"), Decimal("3")), 4)

    def test_estimate_clip_usd_from_token_size_uses_average_mid(self) -> None:
        clip_usd = estimate_clip_usd_from_size(
            clip_size=Decimal("10"),
            short_book={"bid": 99.0, "ask": 101.0},
            long_book={"bid": 109.0, "ask": 111.0},
        )

        self.assertEqual(clip_usd, 1050.0)

    def test_ranked_alert_digest_includes_capital_return(self) -> None:
        digest = build_ranked_alert_digest(
            cross_exchange_alerts=[
                {
                    "symbol": "BTC",
                    "short_venue": "lighter",
                    "long_venue": "mexc",
                    "stats": {"annualized_avg": 0.40, "positive_ratio": 1.0},
                }
            ],
            spot_perp_alerts=[],
        )
        self.assertIn("資本回報", digest)

    def test_run_execution_once_passes_leverage_to_supported_venues(self) -> None:
        with mock.patch("scripts.run_execution_once.AsterExecutionAdapter") as aster_cls:
            build_adapter_for_venue("aster", leverage=3)
        with mock.patch("scripts.run_execution_once.HyperliquidExecutionAdapter") as hyper_cls:
            build_adapter_for_venue("hyperliquid", leverage=4)
        with mock.patch("scripts.run_execution_once.MexcExecutionAdapter") as mexc_cls:
            build_adapter_for_venue("mexc", leverage=5)

        aster_cls.assert_called_once_with(leverage=3)
        hyper_cls.assert_called_once_with(leverage=4)
        mexc_cls.assert_called_once_with(leverage=5)

    def test_run_execution_once_passes_broker_url_to_variational_adapter(self) -> None:
        with mock.patch("scripts.run_execution_once.VariationalBrowserExecutionAdapter") as adapter_cls:
            build_adapter_for_venue("variational", broker_url="ws://127.0.0.1:9999")

        adapter_cls.assert_called_once_with(
            broker_url="ws://127.0.0.1:9999",
            timeout_seconds=30.0,
        )

    def test_run_execution_once_allows_variational_as_maker_after_fill_watcher_exists(self) -> None:
        validate_maker_fill_supported("variational")
        validate_maker_fill_supported("aster")


if __name__ == "__main__":
    unittest.main()
