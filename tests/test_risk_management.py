import tempfile
import unittest
from pathlib import Path

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter, format_aster_step_quantity
from hydra_basis.execution_engine.lighter_adapter import LighterExecutionAdapter
from hydra_basis.execution_engine.mexc_adapter import mexc_close_side
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.hyperliquid_adapter import extract_hyperliquid_order_id
from hydra_basis.risk_management.closers import MarketTypeRouterCloser
from hydra_basis.risk_management.manager import EmergencyRiskManager
from hydra_basis.risk_management.models import PositionLeg, RiskEvent
from hydra_basis.risk_management.reconciliation import reconcile_registry_positions
from hydra_basis.risk_management.funding_risk import (
    FundingRiskConfig,
    FundingRiskManager,
    FundingRiskState,
    FundingSettlement,
    ProjectedFundingRate,
    funding_cashflow_pct,
    load_funding_risk_config,
)
from hydra_basis.risk_management.funding_runtime import process_funding_risk_once
from hydra_basis.risk_management.recording import record_successful_execution, record_successful_live_legs
from hydra_basis.risk_management.registry import PositionRegistry
from hydra_basis.risk_management.runtime import process_watcher_once
from hydra_basis.risk_management.watchers import (
    build_registry_events_for_venue_symbol,
    parse_aster_risk_signal,
    parse_hyperliquid_risk_signal,
)
from hydra_basis.config import POSITION_REGISTRY_PATH


class GlobalRiskManagementTests(unittest.IsolatedAsyncioTestCase):
    async def test_liquidation_event_closes_other_open_legs_in_same_strategy(self) -> None:
        calls: list[dict] = []

        class FakeCloser:
            async def get_open_position(self, *, symbol: str, market_type: str):
                return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "100"}

            async def close_position(self, **kwargs):
                calls.append(kwargs)
                return {"ok": True, "order_id": f"{kwargs['venue']}-{kwargs['symbol']}"}

        registry = PositionRegistry(
            legs=[
                PositionLeg(
                    strategy_id="arb-1",
                    leg_id="aster-short",
                    venue="aster",
                    symbol="LAB",
                    market_type="perp",
                    side="SHORT",
                    quantity="100",
                    status="open",
                ),
                PositionLeg(
                    strategy_id="arb-1",
                    leg_id="mexc-spot",
                    venue="mexc",
                    symbol="LAB",
                    market_type="spot",
                    side="LONG",
                    quantity="100",
                    status="open",
                ),
            ]
        )
        manager = EmergencyRiskManager(registry=registry, closers={"mexc": FakeCloser()})

        result = await manager.handle_event(
            RiskEvent(
                strategy_id="arb-1",
                leg_id="aster-short",
                venue="aster",
                symbol="LAB",
                event_type="LIQUIDATION",
            )
        )

        self.assertEqual(result["closed_leg_ids"], ["mexc-spot"])
        self.assertEqual(calls[0]["side"], "SELL")
        self.assertEqual(registry.get_leg("mexc-spot").status, "emergency_closed")

    async def test_adl_event_closes_multiple_other_open_legs_but_not_closed_legs(self) -> None:
        calls: list[tuple[str, str]] = []

        class FakeCloser:
            def __init__(self, side: str) -> None:
                self.side = side

            async def get_open_position(self, *, symbol: str, market_type: str):
                return {"symbol": symbol, "market_type": market_type, "side": self.side, "quantity": "1"}

            async def close_position(self, **kwargs):
                calls.append((kwargs["venue"], kwargs["side"]))
                return {"ok": True}

        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-2", "trigger", "lighter", "BTC", "perp", "SHORT", "1", "open"),
                PositionLeg("arb-2", "hyper-long", "hyperliquid", "BTC", "perp", "LONG", "1", "open"),
                PositionLeg("arb-2", "mexc-short", "mexc", "BTC", "perp", "SHORT", "1", "open"),
                PositionLeg("arb-2", "old-leg", "aster", "BTC", "perp", "LONG", "1", "closed"),
            ]
        )
        manager = EmergencyRiskManager(
            registry=registry,
            closers={
                "hyperliquid": FakeCloser("LONG"),
                "mexc": FakeCloser("SHORT"),
                "aster": FakeCloser("LONG"),
            },
        )

        result = await manager.handle_event(
            RiskEvent("arb-2", "trigger", "lighter", "BTC", "ADL")
        )

        self.assertEqual(result["closed_leg_ids"], ["hyper-long", "mexc-short"])
        self.assertEqual(calls, [("hyperliquid", "SELL"), ("mexc", "BUY")])
        self.assertEqual(registry.get_leg("old-leg").status, "closed")

    async def test_missing_closer_marks_leg_close_failed(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-3", "trigger", "aster", "ETH", "perp", "SHORT", "2", "open"),
                PositionLeg("arb-3", "other", "unknown", "ETH", "perp", "LONG", "2", "open"),
            ]
        )
        manager = EmergencyRiskManager(registry=registry, closers={})

        result = await manager.handle_event(
            RiskEvent("arb-3", "trigger", "aster", "ETH", "LIQUIDATION")
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["failed_leg_ids"], ["other"])
        self.assertEqual(registry.get_leg("other").status, "close_failed")

    async def test_emergency_close_uses_live_position_quantity_not_registry_quantity(self) -> None:
        calls: list[dict] = []

        class FakeCloser:
            async def get_open_position(self, *, symbol: str, market_type: str):
                return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "8"}

            async def close_position(self, **kwargs):
                calls.append(kwargs)
                return {"ok": True}

        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-live", "trigger", "aster", "ETH", "perp", "SHORT", "999", "open"),
                PositionLeg("arb-live", "other", "lighter", "ETH", "perp", "LONG", "999", "open"),
            ]
        )
        manager = EmergencyRiskManager(registry=registry, closers={"lighter": FakeCloser()})

        result = await manager.handle_event(
            RiskEvent("arb-live", "trigger", "aster", "ETH", "ADL")
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["quantity"], "8")
        self.assertEqual(calls[0]["side"], "SELL")

    async def test_emergency_close_fails_when_live_position_query_is_unavailable(self) -> None:
        class FakeCloser:
            async def close_position(self, **kwargs):
                raise AssertionError("must not close without live position query")

        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-live", "trigger", "aster", "ETH", "perp", "SHORT", "1", "open"),
                PositionLeg("arb-live", "other", "lighter", "ETH", "perp", "LONG", "1", "open"),
            ]
        )
        manager = EmergencyRiskManager(registry=registry, closers={"lighter": FakeCloser()})

        result = await manager.handle_event(
            RiskEvent("arb-live", "trigger", "aster", "ETH", "ADL")
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["failed_leg_ids"], ["other"])
        self.assertIn("live position query unavailable", result["close_results"]["other"]["error"])

    async def test_emergency_close_retries_with_fresh_live_quantity(self) -> None:
        calls: list[dict] = []

        class FakeCloser:
            def __init__(self) -> None:
                self.query_count = 0

            async def get_open_position(self, *, symbol: str, market_type: str):
                self.query_count += 1
                quantity = "8" if self.query_count == 1 else "7"
                return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": quantity}

            async def close_position(self, **kwargs):
                calls.append(kwargs)
                if len(calls) == 1:
                    return {"ok": False, "error": "temporary exchange error"}
                return {"ok": True, "order_id": "retry-ok"}

        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-retry", "trigger", "aster", "ETH", "perp", "SHORT", "1", "open"),
                PositionLeg("arb-retry", "other", "mexc", "ETH", "spot", "999", "open"),
            ]
        )
        manager = EmergencyRiskManager(
            registry=registry,
            closers={"mexc": FakeCloser()},
            max_close_retries=1,
        )

        result = await manager.handle_event(RiskEvent("arb-retry", "trigger", "aster", "ETH", "ADL"))

        self.assertTrue(result["ok"])
        self.assertEqual([call["quantity"] for call in calls], ["8", "7"])
        self.assertEqual(registry.get_leg("other").status, "emergency_closed")


class RiskReconciliationTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile_alerts_missing_live_position_and_side_mismatch(self) -> None:
        class FakeCloser:
            async def get_open_position(self, *, symbol: str, market_type: str):
                if market_type == "spot":
                    return None
                return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "1"}

        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-1", "short", "aster", "ETH", "perp", "SHORT", "1", "open"),
                PositionLeg("arb-1", "spot", "mexc", "ETH", "spot", "LONG", "1", "open"),
            ]
        )

        result = await reconcile_registry_positions(
            registry=registry,
            closers={"aster": FakeCloser(), "mexc": FakeCloser()},
        )

        self.assertEqual(result["mismatch_count"], 2)
        self.assertTrue(any("side mismatch" in item for item in result["messages"]))
        self.assertTrue(any("missing live position" in item for item in result["messages"]))

    async def test_reconcile_updates_registry_quantity_to_live_quantity(self) -> None:
        class FakeCloser:
            async def get_open_position(self, *, symbol: str, market_type: str):
                return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "2.5"}

        registry = PositionRegistry(
            legs=[PositionLeg("arb-2", "long", "lighter", "SOL", "perp", "LONG", "1", "open")]
        )

        result = await reconcile_registry_positions(registry=registry, closers={"lighter": FakeCloser()})

        self.assertEqual(result["updated_leg_ids"], ["long"])
        self.assertEqual(registry.get_leg("long").quantity, "2.5")

    async def test_reconcile_reports_unregistered_live_positions_when_supported(self) -> None:
        class FakeCloser:
            async def get_open_position(self, *, symbol: str, market_type: str):
                return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "1"}

            async def list_open_positions(self):
                return [
                    {"symbol": "ETH", "market_type": "perp", "side": "LONG", "quantity": "1"},
                    {"symbol": "BTC", "market_type": "perp", "side": "SHORT", "quantity": "0.1"},
                ]

        registry = PositionRegistry(
            legs=[PositionLeg("arb-3", "eth-long", "hyperliquid", "ETH", "perp", "LONG", "1", "open")]
        )

        result = await reconcile_registry_positions(registry=registry, closers={"hyperliquid": FakeCloser()})

        self.assertEqual(result["unregistered_count"], 1)
        self.assertTrue(any("unregistered live position" in item for item in result["messages"]))


class FundingRiskTests(unittest.TestCase):
    def test_funding_cashflow_uses_position_side(self) -> None:
        self.assertEqual(funding_cashflow_pct(side="SHORT", funding_rate=0.001), 0.001)
        self.assertEqual(funding_cashflow_pct(side="LONG", funding_rate=0.001), -0.001)
        self.assertEqual(funding_cashflow_pct(side="LONG", funding_rate=-0.001), 0.001)
        self.assertEqual(funding_cashflow_pct(side="SHORT", funding_rate=-0.001), -0.001)

    def test_longer_interval_window_counts_pair_net_funding(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-1", "a-short", "aster", "LAB", "perp", "SHORT", "100", "open"),
                PositionLeg("arb-1", "b-long", "variational", "LAB", "perp", "LONG", "100", "open"),
            ]
        )
        manager = FundingRiskManager(
            registry=registry,
            state=FundingRiskState(),
            config=FundingRiskConfig(
                enabled=True,
                check_interval_seconds=3600,
                consecutive_negative_windows=2,
                auto_close_negative_funding_pct=0.1,
            ),
        )

        first = manager.ingest_settlements(
            [
                FundingSettlement("a-short", 1_000, 0.0010, 1.0),
                FundingSettlement("a-short", 2_000, 0.0010, 1.0),
                FundingSettlement("a-short", 3_000, 0.0010, 1.0),
                FundingSettlement("a-short", 4_000, 0.0010, 1.0),
                FundingSettlement("b-long", 4_000, 0.0050, 4.0),
            ]
        )
        second = manager.ingest_settlements(
            [
                FundingSettlement("a-short", 5_000, 0.0010, 1.0),
                FundingSettlement("a-short", 6_000, 0.0010, 1.0),
                FundingSettlement("a-short", 7_000, 0.0010, 1.0),
                FundingSettlement("a-short", 8_000, 0.0010, 1.0),
                FundingSettlement("b-long", 8_000, 0.0050, 4.0),
            ]
        )

        self.assertEqual(first["action"], "negative_window_recorded")
        self.assertAlmostEqual(first["net_cashflow_pct"], -0.001)
        self.assertEqual(second["action"], "notify_consecutive_negative")
        self.assertEqual(second["consecutive_negative_windows"], 2)
        self.assertIsNone(second.get("risk_event"))

    def test_projection_auto_closes_when_unsettled_net_loss_exceeds_config(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-1", "a-short", "aster", "LAB", "perp", "SHORT", "100", "open"),
                PositionLeg("arb-1", "b-long", "variational", "LAB", "perp", "LONG", "100", "open"),
            ]
        )
        manager = FundingRiskManager(
            registry=registry,
            state=FundingRiskState(),
            config=FundingRiskConfig(
                enabled=True,
                check_interval_seconds=3600,
                consecutive_negative_windows=2,
                auto_close_negative_funding_pct=0.1,
            ),
        )

        result = manager.evaluate_projection(
            strategy_id="arb-1",
            rates=[
                ProjectedFundingRate("a-short", funding_rate=0.0001, interval_hours=1.0),
                ProjectedFundingRate("b-long", funding_rate=0.0020, interval_hours=4.0),
            ],
        )

        self.assertEqual(result["action"], "auto_close_projected_negative_funding")
        self.assertLess(result["net_cashflow_pct"], -0.001)
        event = result["risk_event"]
        self.assertEqual(event.strategy_id, "arb-1")
        self.assertEqual(event.event_type, "FUNDING_AUTO_CLOSE")

    def test_load_funding_risk_config_reads_jsonc(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "funding_risk.jsonc"
            path.write_text(
                """
                {
                  // pair-level funding risk
                  "enabled": true,
                  "check_interval_seconds": 3600,
                  "consecutive_negative_windows": 2,
                  "auto_close_negative_funding_pct": 0.1
                }
                """,
                encoding="utf-8",
            )

            config = load_funding_risk_config(path)

        self.assertTrue(config.enabled)
        self.assertEqual(config.consecutive_negative_windows, 2)
        self.assertEqual(config.auto_close_negative_funding_pct, 0.1)


class FundingRiskRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_funding_risk_once_auto_closes_counterparty_on_projection_loss(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            state_path = Path(temp_dir) / "funding_risk_state.json"
            registry = PositionRegistry(
                legs=[
                    PositionLeg("arb-1", "a-short", "aster", "LAB", "perp", "SHORT", "100", "open"),
                    PositionLeg("arb-1", "mexc-spot", "mexc", "LAB", "spot", "LONG", "100", "open"),
                ]
            )
            registry.save(registry_path)

            class Provider:
                async def fetch_settlements(self, registry, state):
                    return []

                async def fetch_projected_rates(self, registry):
                    return {
                        "arb-1": [
                            ProjectedFundingRate("a-short", funding_rate=-0.0020, interval_hours=1.0),
                        ]
                    }

            class Closer:
                def __init__(self, side: str) -> None:
                    self.side = side

                async def get_open_position(self, *, symbol: str, market_type: str):
                    return {"symbol": symbol, "market_type": market_type, "side": self.side, "quantity": "100"}

                async def close_position(self, **kwargs):
                    return {"ok": True, "kwargs": kwargs}

            result = await process_funding_risk_once(
                registry_path=registry_path,
                state_path=state_path,
                provider=Provider(),
                closers={"aster": Closer("SHORT"), "mexc": Closer("LONG")},
                config=FundingRiskConfig(
                    enabled=True,
                    check_interval_seconds=3600,
                    consecutive_negative_windows=2,
                    auto_close_negative_funding_pct=0.1,
                ),
                dry_run=False,
            )

        self.assertEqual(result["auto_close_results"][0]["closed_leg_ids"], ["a-short", "mexc-spot"])
        self.assertIn("funding", result["messages"][0].lower())


class PositionRegistryTests(unittest.TestCase):
    def test_default_position_registry_path(self) -> None:
        self.assertEqual(POSITION_REGISTRY_PATH, Path("data/position_registry.json"))

    def test_save_and_load_position_registry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "position_registry.json"
            registry = PositionRegistry(
                legs=[
                    PositionLeg("arb-1", "leg-1", "mexc", "BTC", "spot", "LONG", "0.1", "open")
                ]
            )

            registry.save(path)
            loaded = PositionRegistry.load(path)

            self.assertEqual(loaded.get_leg("leg-1").venue, "mexc")
            self.assertEqual(loaded.get_leg("leg-1").market_type, "spot")

    def test_position_registry_persists_margin_topup_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "position_registry.json"
            registry = PositionRegistry(
                legs=[
                    PositionLeg(
                        "arb-1",
                        "leg-1",
                        "aster",
                        "ETH",
                        "perp",
                        "LONG",
                        "0.1",
                        margin_topups=2,
                        last_margin_topup_ts_ms=123,
                    )
                ]
            )

            registry.save(path)
            loaded = PositionRegistry.load(path)

            self.assertEqual(loaded.get_leg("leg-1").margin_topups, 2)
            self.assertEqual(loaded.get_leg("leg-1").last_margin_topup_ts_ms, 123)


class ExecutionRecordingTests(unittest.TestCase):
    def test_record_successful_execution_writes_short_and_long_legs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "position_registry.json"

            strategy_id = record_successful_execution(
                path=path,
                symbol="LAB",
                quantity="100",
                short_venue="aster",
                long_venue="variational",
                execution_result={
                    "ok": True,
                    "maker_result": {"ok": True, "order_id": "maker-1"},
                    "hedge_result": {"ok": True, "order_id": "taker-1"},
                },
                strategy_id="manual-LAB-1",
            )

            self.assertEqual(strategy_id, "manual-LAB-1")
            registry = PositionRegistry.load(path)
            self.assertEqual(registry.get_leg("manual-LAB-1:aster:short").side, "SHORT")
            self.assertEqual(registry.get_leg("manual-LAB-1:variational:long").side, "LONG")
            self.assertEqual(registry.get_leg("manual-LAB-1:aster:short").status, "open")

    def test_record_successful_execution_rejects_partial_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "position_registry.json"

            with self.assertRaises(RuntimeError):
                record_successful_execution(
                    path=path,
                    symbol="LAB",
                    quantity="100",
                    short_venue="aster",
                    long_venue="mexc",
                    execution_result={"ok": False, "maker_result": {"ok": True}},
                    strategy_id="manual-LAB-2",
                )

            self.assertFalse(path.exists())

    def test_record_successful_live_legs_writes_spot_perp_pair_with_live_quantities(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "position_registry.json"

            strategy_id = record_successful_live_legs(
                path=path,
                symbol="BEAT",
                execution_result={
                    "ok": True,
                    "maker_result": {"ok": True, "order_id": "maker-1"},
                    "hedge_result": {"ok": True, "order_id": "taker-1"},
                },
                legs=[
                    {
                        "venue": "aster",
                        "market_type": "perp",
                        "side": "SHORT",
                        "quantity": "9.8",
                    },
                    {
                        "venue": "mexc_spot",
                        "market_type": "spot",
                        "side": "LONG",
                        "quantity": "10.1",
                    },
                ],
                strategy_id="spot-perp-BEAT-1",
            )

            self.assertEqual(strategy_id, "spot-perp-BEAT-1")
            registry = PositionRegistry.load(path)
            short_leg = registry.get_leg("spot-perp-BEAT-1:aster:perp:short")
            spot_leg = registry.get_leg("spot-perp-BEAT-1:mexc_spot:spot:long")
            self.assertEqual(short_leg.market_type, "perp")
            self.assertEqual(short_leg.quantity, "9.8")
            self.assertEqual(spot_leg.market_type, "spot")
            self.assertEqual(spot_leg.quantity, "10.1")

    def test_record_successful_live_legs_rejects_missing_live_quantity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "position_registry.json"

            with self.assertRaises(RuntimeError):
                record_successful_live_legs(
                    path=path,
                    symbol="BEAT",
                    execution_result={
                        "ok": True,
                        "maker_result": {"ok": True},
                        "hedge_result": {"ok": True},
                    },
                    legs=[
                        {"venue": "aster", "market_type": "perp", "side": "SHORT", "quantity": "0"},
                    ],
                    strategy_id="spot-perp-BEAT-2",
                )

            self.assertFalse(path.exists())


class RiskWatcherMappingTests(unittest.TestCase):
    def test_build_registry_events_for_venue_symbol_maps_liquidation_to_open_leg(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-1", "aster-short", "aster", "LAB", "perp", "SHORT", "100", "open"),
                PositionLeg("arb-1", "mexc-spot", "mexc", "LAB", "spot", "LONG", "100", "open"),
            ]
        )

        events = build_registry_events_for_venue_symbol(
            registry=registry,
            venue="aster",
            symbol="LAB",
            event_type="LIQUIDATION",
            message="force order",
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].strategy_id, "arb-1")
        self.assertEqual(events[0].leg_id, "aster-short")

    def test_build_registry_events_for_venue_symbol_ignores_closed_legs(self) -> None:
        registry = PositionRegistry(
            legs=[
                PositionLeg("arb-1", "aster-short", "aster", "LAB", "perp", "SHORT", "100", "closed"),
            ]
        )

        events = build_registry_events_for_venue_symbol(
            registry=registry,
            venue="aster",
            symbol="LAB",
            event_type="ADL",
        )

        self.assertEqual(events, [])

    def test_parse_hyperliquid_user_event_liquidation(self) -> None:
        signal = parse_hyperliquid_risk_signal(
            {
                "channel": "userEvents",
                "data": {
                    "liquidation": {
                        "coin": "BTC",
                        "liquidated_ntl_pos": "1000",
                    }
                },
            }
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.venue, "hyperliquid")
        self.assertEqual(signal.symbol, "BTC")
        self.assertEqual(signal.event_type, "LIQUIDATION")

    def test_parse_aster_force_order_and_adl_order_update(self) -> None:
        liquidation = parse_aster_risk_signal(
            {"e": "forceOrder", "o": {"s": "LABUSDT", "X": "FILLED"}}
        )
        adl = parse_aster_risk_signal(
            {
                "e": "ORDER_TRADE_UPDATE",
                "o": {"s": "BTCUSDT", "c": "adl_autoclose-123", "X": "FILLED"},
            }
        )

        self.assertIsNotNone(liquidation)
        self.assertEqual(liquidation.symbol, "LAB")
        self.assertEqual(liquidation.event_type, "LIQUIDATION")
        self.assertIsNotNone(adl)
        self.assertEqual(adl.symbol, "BTC")
        self.assertEqual(adl.event_type, "ADL")


class RiskRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_watcher_once_closes_counterparty_and_saves_registry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "position_registry.json"
            registry = PositionRegistry(
                legs=[
                    PositionLeg("arb-1", "aster-short", "aster", "LAB", "perp", "SHORT", "100", "open"),
                    PositionLeg("arb-1", "mexc-long", "mexc", "LAB", "perp", "LONG", "100", "open"),
                ]
            )
            registry.save(path)

            class FakeWatcher:
                async def watch(self):
                    yield type(
                        "Signal",
                        (),
                        {
                            "venue": "aster",
                            "symbol": "LAB",
                            "event_type": "LIQUIDATION",
                            "message": "force order",
                        },
                    )()

            class FakeCloser:
                async def get_open_position(self, *, symbol: str, market_type: str):
                    return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "100"}

                async def close_position(self, **kwargs):
                    return {"ok": True}

            result = await process_watcher_once(
                registry_path=path,
                watcher=FakeWatcher(),
                closers={"mexc": FakeCloser()},
            )

            self.assertEqual(result["closed_leg_ids"], ["mexc-long"])
            loaded = PositionRegistry.load(path)
        self.assertEqual(loaded.get_leg("mexc-long").status, "emergency_closed")


class MarginTopupTests(unittest.IsolatedAsyncioTestCase):
    def test_liquidation_distance_uses_long_and_short_direction(self) -> None:
        from hydra_basis.risk_management.margin_topup import liquidation_distance_pct

        self.assertEqual(liquidation_distance_pct(side="LONG", mark_price=100.0, liquidation_price=95.0), 5.0)
        self.assertEqual(liquidation_distance_pct(side="SHORT", mark_price=100.0, liquidation_price=105.0), 5.0)

    def test_loads_default_margin_topup_config(self) -> None:
        from hydra_basis.risk_management.margin_topup import load_margin_topup_config

        config = load_margin_topup_config(Path("configs/margin_topup.jsonc"))

        self.assertTrue(config.enabled)
        self.assertEqual(config.liq_distance_trigger_pct, 5.0)
        self.assertEqual(config.topup_amount_usd, 500.0)
        self.assertEqual(config.max_topups_per_leg, 3)
        self.assertEqual(config.cooldown_seconds, 60)

    async def test_margin_topup_manager_adds_margin_and_updates_registry(self) -> None:
        from hydra_basis.risk_management.margin_topup import (
            MarginHealthSnapshot,
            MarginTopupConfig,
            MarginTopupManager,
        )

        calls: list[dict] = []
        registry = PositionRegistry(
            legs=[PositionLeg("arb-1", "leg-1", "aster", "ETH", "perp", "LONG", "0.1")]
        )

        class FakeTopupper:
            async def add_isolated_margin(self, **kwargs):
                calls.append(kwargs)
                return {"ok": True, "order_id": "topup-1"}

        manager = MarginTopupManager(
            registry=registry,
            toppers={"aster": FakeTopupper()},
            config=MarginTopupConfig(
                enabled=True,
                liq_distance_trigger_pct=5.0,
                topup_amount_usd=500.0,
                max_topups_per_leg=3,
                cooldown_seconds=60,
            ),
            now_ms=lambda: 100_000,
        )

        result = await manager.handle_snapshot(
            MarginHealthSnapshot(
                venue="aster",
                symbol="ETH",
                leg_id="leg-1",
                mark_price=100.0,
                liquidation_price=95.0,
                side="LONG",
            )
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "topup_done")
        self.assertEqual(calls[0]["amount_usd"], 500.0)
        self.assertEqual(registry.get_leg("leg-1").margin_topups, 1)
        self.assertEqual(registry.get_leg("leg-1").last_margin_topup_ts_ms, 100_000)

    async def test_margin_topup_manager_respects_cooldown_and_max_topups(self) -> None:
        from hydra_basis.risk_management.margin_topup import (
            MarginHealthSnapshot,
            MarginTopupConfig,
            MarginTopupManager,
        )

        registry = PositionRegistry(
            legs=[
                PositionLeg(
                    "arb-1",
                    "leg-1",
                    "aster",
                    "ETH",
                    "perp",
                    "LONG",
                    "0.1",
                    margin_topups=1,
                    last_margin_topup_ts_ms=90_000,
                )
            ]
        )
        manager = MarginTopupManager(
            registry=registry,
            toppers={"aster": object()},
            config=MarginTopupConfig(True, 5.0, 500.0, 1, 60),
            now_ms=lambda: 100_000,
        )

        snapshot = MarginHealthSnapshot("aster", "ETH", "leg-1", 100.0, 95.0, "LONG")

        self.assertEqual((await manager.handle_snapshot(snapshot))["action"], "max_topups_reached")

        registry.get_leg("leg-1").margin_topups = 0
        self.assertEqual((await manager.handle_snapshot(snapshot))["action"], "cooldown")

    async def test_margin_topup_manager_returns_emergency_event_when_topup_unsupported(self) -> None:
        from hydra_basis.risk_management.margin_topup import (
            MarginHealthSnapshot,
            MarginTopupConfig,
            MarginTopupManager,
        )

        class UnsupportedTopupper:
            async def add_isolated_margin(self, **kwargs):
                raise RuntimeError("lighter isolated margin top-up is not supported")

        registry = PositionRegistry(
            legs=[PositionLeg("arb-1", "leg-1", "lighter", "ETH", "perp", "SHORT", "0.1")]
        )
        manager = MarginTopupManager(
            registry=registry,
            toppers={"lighter": UnsupportedTopupper()},
            config=MarginTopupConfig(True, 5.0, 500.0, 3, 60),
            now_ms=lambda: 100_000,
        )

        result = await manager.handle_snapshot(
            MarginHealthSnapshot("lighter", "ETH", "leg-1", 100.0, 105.0, "SHORT")
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["action"], "topup_failed")
        self.assertEqual(result["risk_event"].event_type, "MANUAL_EMERGENCY")


class EmergencyCloserAdapterTests(unittest.IsolatedAsyncioTestCase):
    def test_mexc_close_side_codes(self) -> None:
        self.assertEqual(mexc_close_side("BUY"), 2)
        self.assertEqual(mexc_close_side("SELL"), 4)

    def test_mexc_defaults_to_isolated_open_type(self) -> None:
        from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter

        adapter = MexcExecutionAdapter(api_key="k", api_secret="s")

        self.assertEqual(adapter.open_type, 1)

    def test_hyperliquid_order_status_error_raises(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Order has invalid price"):
            extract_hyperliquid_order_id(
                {
                    "status": "ok",
                    "response": {
                        "data": {
                            "statuses": [{"error": "Order has invalid price."}],
                        }
                    },
                },
                fill_type="filled",
            )

    async def test_aster_close_position_uses_reduce_only_market_order(self) -> None:
        calls: list[dict] = []

        class FakeAster(AsterExecutionAdapter):
            async def _resolve_raw_symbol(self, symbol: str) -> str:
                return f"{symbol}USDT"

            async def _post_order(self, params: dict) -> dict:
                calls.append(params)
                return {"orderId": 123}

        adapter = FakeAster(signer_address="0xsigner", private_key="0xprivate", user_address="0xuser")

        result = await adapter.close_position(
            venue="aster",
            symbol="LAB",
            side="BUY",
            quantity="100",
            market_type="perp",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["reduceOnly"], "true")
        self.assertEqual(calls[0]["type"], "MARKET")

    async def test_aster_place_order_sets_isolated_margin_before_order(self) -> None:
        calls: list[dict] = []

        class FakeAster(AsterExecutionAdapter):
            async def _resolve_raw_symbol(self, symbol: str) -> str:
                return f"{symbol}USDT"

            async def _format_quantity(self, symbol: str, quantity: str, *, market: bool) -> str:
                return quantity

            async def _post_signed_query(self, url: str, params: dict) -> dict:
                calls.append({"url": url, "params": params})
                return {"code": 200, "msg": "success"} if "marginType" in url else {"orderId": 123}

            def build_signed_params(self, params: dict) -> dict:
                return params

        adapter = FakeAster(signer_address="0xsigner", private_key="0xprivate", user_address="0xuser")

        await adapter.place_market_order(symbol="ETH", side="BUY", amount="0.003", clip_usd=0.0)

        self.assertIn("/fapi/v3/marginType", calls[0]["url"])
        self.assertEqual(calls[0]["params"]["marginType"], "ISOLATED")
        self.assertIn("/fapi/v3/leverage", calls[1]["url"])
        self.assertIn("/fapi/v3/order", calls[2]["url"])

    async def test_aster_ignores_margin_type_already_isolated_error(self) -> None:
        calls: list[dict] = []

        class FakeAster(AsterExecutionAdapter):
            async def _resolve_raw_symbol(self, symbol: str) -> str:
                return f"{symbol}USDT"

            async def _post_signed_query(self, url: str, params: dict) -> dict:
                calls.append({"url": url, "params": params})
                raise RuntimeError("aster order 400: {'code': -4046, 'msg': 'No need to change margin type.'}")

            def build_signed_params(self, params: dict) -> dict:
                return params

        adapter = FakeAster(signer_address="0xsigner", private_key="0xprivate", user_address="0xuser")

        await adapter.ensure_isolated_margin("ETH")
        await adapter.ensure_isolated_margin("ETH")

        self.assertEqual(len(calls), 1)
        self.assertIn("ETHUSDT", adapter._isolated_symbols)

    async def test_aster_place_order_sets_leverage_before_order(self) -> None:
        calls: list[dict] = []

        class FakeAster(AsterExecutionAdapter):
            async def _resolve_raw_symbol(self, symbol: str) -> str:
                return f"{symbol}USDT"

            async def _format_quantity(self, symbol: str, quantity: str, *, market: bool) -> str:
                return quantity

            async def ensure_isolated_margin(self, symbol: str) -> None:
                return None

            async def _post_signed_query(self, url: str, params: dict) -> dict:
                calls.append({"url": url, "params": params})
                return {"orderId": 123}

            def build_signed_params(self, params: dict) -> dict:
                return params

        adapter = FakeAster(
            signer_address="0xsigner",
            private_key="0xprivate",
            user_address="0xuser",
            leverage=7,
        )

        await adapter.place_market_order(symbol="ETH", side="BUY", amount="0.003", clip_usd=0.0)

        self.assertIn("/fapi/v3/leverage", calls[0]["url"])
        self.assertEqual(calls[0]["params"]["leverage"], 7)
        self.assertIn("/fapi/v3/order", calls[1]["url"])

    async def test_aster_add_isolated_margin_posts_position_margin_add(self) -> None:
        calls: list[dict] = []

        class FakeAster(AsterExecutionAdapter):
            async def _resolve_raw_symbol(self, symbol: str) -> str:
                return f"{symbol}USDT"

            async def _post_signed_query(self, url: str, params: dict) -> dict:
                calls.append({"url": url, "params": params})
                return {"amount": 500.0, "code": 200, "type": 1}

            def build_signed_params(self, params: dict) -> dict:
                return params

        adapter = FakeAster(signer_address="0xsigner", private_key="0xprivate", user_address="0xuser")

        result = await adapter.add_isolated_margin(
            venue="aster",
            symbol="ETH",
            side="LONG",
            amount_usd=500.0,
        )

        self.assertTrue(result["ok"])
        self.assertIn("/fapi/v3/positionMargin", calls[0]["url"])
        self.assertEqual(calls[0]["params"]["symbol"], "ETHUSDT")
        self.assertEqual(calls[0]["params"]["amount"], "500.0")
        self.assertEqual(calls[0]["params"]["type"], 1)
        self.assertEqual(calls[0]["params"]["positionSide"], "BOTH")

    async def test_hyperliquid_add_isolated_margin_posts_update_action(self) -> None:
        from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter

        actions: list[dict] = []

        class FakeHyperliquid(HyperliquidExecutionAdapter):
            def __init__(self) -> None:
                self.private_key = "0x" + "1" * 64
                self.account_address = "0x" + "2" * 40
                self._universe = ["ETH"]

            async def _post_order(self, action: dict) -> dict:
                actions.append(action)
                return {"status": "ok"}

        adapter = FakeHyperliquid()

        result = await adapter.add_isolated_margin(
            venue="hyperliquid",
            symbol="ETH",
            side="SHORT",
            amount_usd=500.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(actions[0]["type"], "updateIsolatedMargin")
        self.assertEqual(actions[0]["asset"], 0)
        self.assertFalse(actions[0]["isBuy"])
        self.assertEqual(actions[0]["ntli"], 500_000_000)

    async def test_lighter_add_isolated_margin_is_explicitly_unsupported(self) -> None:
        adapter = LighterExecutionAdapter(
            signer_client_factory=lambda: object(),
            market_config_loader=lambda symbol: (7, 1000, 100),
            orderbook_loader=lambda symbol: {"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        with self.assertRaisesRegex(RuntimeError, "not supported"):
            await adapter.add_isolated_margin(
                venue="lighter",
                symbol="ETH",
                side="LONG",
                amount_usd=500.0,
            )

    async def test_hyperliquid_place_order_sets_isolated_leverage_before_order(self) -> None:
        from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter

        actions: list[dict] = []

        class FakeHyperliquid(HyperliquidExecutionAdapter):
            def __init__(self) -> None:
                self.private_key = "0x" + "1" * 64
                self.account_address = "0x" + "2" * 40
                self.slippage_bps = 50.0
                self.default_leverage = 1
                self._universe = ["ETH"]
                self._isolated_asset_indices = set()

            async def _get_mid_price(self, symbol: str) -> float:
                return 1773.75

            async def _post_order(self, action: dict) -> dict:
                actions.append(action)
                if action["type"] == "updateLeverage":
                    return {"status": "ok", "response": {"type": "default"}}
                return {"status": "ok", "response": {"data": {"statuses": [{"filled": {"oid": 1}}]}}}

        adapter = FakeHyperliquid()

        await adapter.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=0.0)

        self.assertEqual(actions[0]["type"], "updateLeverage")
        self.assertFalse(actions[0]["isCross"])
        self.assertEqual(actions[1]["type"], "order")


    async def test_aster_v3_post_order_uses_api_wallet_signature_fields(self) -> None:
        calls: list[dict] = []

        class FakeAster(AsterExecutionAdapter):
            def _nonce_us(self) -> int:
                return 1748310859508867

            def _sign(self, encoded_params: str) -> str:
                calls.append({"encoded_params": encoded_params})
                return "0xsig"

            async def _post_signed_query(self, url: str, params: dict) -> dict:
                calls.append({"url": url, "params": params})
                return {"orderId": 456}

        adapter = FakeAster(signer_address="0xsigner", private_key="0xprivate", user_address="0xuser")

        result = await adapter._post_order({"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "1"})

        self.assertTrue(result["orderId"])
        self.assertIn("/fapi/v3/order", calls[1]["url"])
        self.assertTrue(calls[1]["url"].startswith("https://fapi.asterdex.com"))
        self.assertEqual(calls[1]["params"]["signer"], "0xsigner")
        self.assertEqual(calls[1]["params"]["user"], "0xuser")
        self.assertEqual(calls[1]["params"]["nonce"], "1748310859508867")
        self.assertEqual(calls[1]["params"]["signature"], "0xsig")
        self.assertIn("signer=0xsigner", calls[0]["encoded_params"])

    def test_format_aster_step_quantity_floors_to_step_and_strips_trailing_zeros(self) -> None:
        self.assertEqual(format_aster_step_quantity("0.00100000", "0.001"), "0.001")
        self.assertEqual(format_aster_step_quantity("0.0019", "0.001"), "0.001")
        self.assertEqual(format_aster_step_quantity("12.340000", "0.01"), "12.34")

    def test_format_aster_step_quantity_reports_when_quantity_is_below_step(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "quantity.*below.*stepSize=0.001"):
            format_aster_step_quantity("0.0001", "0.001")

    async def test_aster_market_order_formats_quantity_to_exchange_step_size(self) -> None:
        calls: list[dict] = []

        class FakeAster(AsterExecutionAdapter):
            async def _resolve_raw_symbol(self, symbol: str) -> str:
                return f"{symbol}USDT"

            async def _quantity_step_size(self, symbol: str, *, market: bool) -> str:
                return "0.001"

            async def ensure_isolated_margin(self, symbol: str) -> None:
                return None

            async def ensure_leverage(self, symbol: str) -> None:
                return None

            async def _post_order(self, params: dict) -> dict:
                calls.append(params)
                return {"orderId": 789}

        adapter = FakeAster(signer_address="0xsigner", private_key="0xprivate", user_address="0xuser")

        result = await adapter.place_market_order(
            symbol="BTC",
            side="BUY",
            amount="0.00100000",
            clip_usd=0.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["quantity"], "0.001")

    async def test_aster_resolve_raw_symbol_prefers_usdt_execution_symbol(self) -> None:
        class FakeAster(AsterExecutionAdapter):
            async def _load_exchange_info_by_symbol(self) -> dict[str, dict]:
                return {
                    "ETHUSD": {"symbol": "ETHUSD", "status": "TRADING"},
                    "ETHUSDT": {"symbol": "ETHUSDT", "status": "TRADING"},
                }

        adapter = FakeAster(signer_address="0xsigner", private_key="0xprivate", user_address="0xuser")

        self.assertEqual(await adapter._resolve_raw_symbol("ETH"), "ETHUSDT")

    async def test_lighter_close_position_uses_reduce_only_create_order(self) -> None:
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

        result = await adapter.close_position(
            venue="lighter",
            symbol="BTC",
            side="SELL",
            quantity="10",
            market_type="perp",
        )

        self.assertTrue(result["ok"])
        self.assertTrue(adapter.client.calls[0]["reduce_only"])

    async def test_mexc_spot_close_position_sells_long_spot_position(self) -> None:
        calls: list[dict] = []

        class FakeMexcSpot(MexcSpotExecutionAdapter):
            async def _post_order(self, params: dict) -> dict:
                calls.append(params)
                return {"orderId": "spot-1"}

        adapter = FakeMexcSpot(api_key="k", api_secret="s")

        result = await adapter.close_position(
            venue="mexc",
            symbol="LAB",
            side="SELL",
            quantity="100",
            market_type="spot",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["symbol"], "LABUSDT")
        self.assertEqual(calls[0]["side"], "SELL")
        self.assertEqual(calls[0]["type"], "MARKET")

    async def test_market_type_router_closer_delegates_by_market_type(self) -> None:
        calls: list[str] = []

        class PerpCloser:
            async def close_position(self, **kwargs):
                calls.append(f"perp:{kwargs['symbol']}")
                return {"ok": True}

        class SpotCloser:
            async def close_position(self, **kwargs):
                calls.append(f"spot:{kwargs['symbol']}")
                return {"ok": True}

        closer = MarketTypeRouterCloser(perp=PerpCloser(), spot=SpotCloser())

        await closer.close_position(venue="mexc", symbol="BTC", side="SELL", quantity="1", market_type="spot")
        await closer.close_position(venue="mexc", symbol="BTC", side="BUY", quantity="1", market_type="perp")

        self.assertEqual(calls, ["spot:BTC", "perp:BTC"])


if __name__ == "__main__":
    unittest.main()
