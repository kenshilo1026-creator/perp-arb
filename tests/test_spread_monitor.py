import json
import tempfile
import unittest
import asyncio
from pathlib import Path
from unittest import mock

from hydra_basis.spread_monitor.config import load_spread_monitor_config
from hydra_basis.spread_monitor.engine import build_symbol_snapshots
from hydra_basis.spread_monitor.formatting import (
    format_spread_digest,
    format_spread_pair_debug_console,
    format_spread_pair_debug_telegram,
    format_spread_debug_console,
    format_spread_debug_telegram,
    format_spread_filter_reason,
    format_spread_live_status_console,
    format_spread_opportunity,
    format_spread_opportunity_console,
    format_spread_progress_console,
)
from hydra_basis.spread_monitor.models import SpreadMonitorConfig, VenueSpreadSnapshot
from hydra_basis.spread_monitor.runtime import build_stream_bootstrap, parse_lighter_ticker_message
from hydra_basis.spread_monitor.runtime import pump_runner_forever
from hydra_basis.spread_monitor.selector import (
    collect_spread_filter_diagnostics,
    collect_spread_pair_filter_diagnostics,
    compute_cross_exchange_spread_pct,
    select_best_spread_opportunity,
)
from hydra_basis.notifications.telegram import send_telegram


class SpreadMonitorConfigTests(unittest.TestCase):
    def test_loads_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "spread_monitor.json"
            path.write_text(
                json.dumps(
                    {
                        "venues": ["hyperliquid", "lighter"],
                        "max_funding_rate_pct": 0.1,
                        "max_orderbook_spread_pct": 0.1,
                        "min_cross_exchange_spread_pct": 0.2,
                        "execution_notional_usd": 1000,
                        "max_stale_seconds": 15,
                        "max_price_deviation_pct": 2.0,
                        "max_short_term_move_pct": 3.0,
                        "hedge_retry_count": 2,
                    }
                ),
                encoding="utf-8",
            )
            config = load_spread_monitor_config(path)

        self.assertEqual(config.venues, ["hyperliquid", "lighter"])
        self.assertEqual(config.execution_notional_usd, 1000.0)

    def test_loads_config_file_with_json_comments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "spread_monitor.jsonc"
            path.write_text(
                """{
  // 監控哪些交易所
  "venues": ["hyperliquid", "lighter"],
  // 資金費率上限
  "max_funding_rate_pct": 0.1,
  "max_orderbook_spread_pct": 0.1,
  "min_cross_exchange_spread_pct": 0.2,
  "execution_notional_usd": 1000,
  "max_stale_seconds": 15,
  "max_price_deviation_pct": 2.0,
  "max_short_term_move_pct": 3.0,
  "hedge_retry_count": 2
}""",
                encoding="utf-8",
            )

            config = load_spread_monitor_config(path)

        self.assertEqual(config.venues, ["hyperliquid", "lighter"])


class SpreadSelectorTests(unittest.TestCase):
    def test_selects_best_opportunity_with_one_tight_orderbook_side(self) -> None:
        config = SpreadMonitorConfig(
            venues=["hyperliquid", "lighter"],
            max_funding_rate_pct=0.1,
            max_orderbook_spread_pct=0.1,
            min_cross_exchange_spread_pct=0.2,
            execution_notional_usd=1000.0,
            max_stale_seconds=15,
            max_price_deviation_pct=2.0,
            max_short_term_move_pct=3.0,
            hedge_retry_count=2,
        )
        symbol_snapshots = {
            "BTC": [
                VenueSpreadSnapshot("hyperliquid", "BTC", 101.0, 100.95, 101.0, 0.0001, 1_000_000),
                VenueSpreadSnapshot("lighter", "BTC", 100.0, 99.95, 100.0, 0.0001, 1_000_000),
            ]
        }
        opportunity = select_best_spread_opportunity(
            symbol_snapshots=symbol_snapshots,
            config=config,
            now_ms=1_005_000,
        )

        self.assertIsNotNone(opportunity)
        self.assertEqual(opportunity.symbol, "BTC")
        self.assertEqual(opportunity.short_venue, "hyperliquid")
        self.assertEqual(opportunity.long_venue, "lighter")
        self.assertGreater(opportunity.expected_profit_usd, 0)

    def test_cross_exchange_spread_pct(self) -> None:
        self.assertAlmostEqual(compute_cross_exchange_spread_pct(short_price=101.0, long_price=100.0), 0.01)

    def test_selects_using_executable_bid_and_ask_not_mark_price(self) -> None:
        config = SpreadMonitorConfig(
            venues=["hyperliquid", "lighter"],
            max_funding_rate_pct=0.1,
            max_orderbook_spread_pct=0.1,
            min_cross_exchange_spread_pct=0.2,
            execution_notional_usd=1000.0,
            max_stale_seconds=15,
            max_price_deviation_pct=2.0,
            max_short_term_move_pct=3.0,
            hedge_retry_count=2,
        )
        symbol_snapshots = {
            "BTC": [
                VenueSpreadSnapshot("hyperliquid", "BTC", 100.00, 100.05, 100.10, 0.0001, 1_000_000),
                VenueSpreadSnapshot("lighter", "BTC", 99.95, 99.80, 99.85, 0.0001, 1_000_000),
            ]
        }

        opportunity = select_best_spread_opportunity(
            symbol_snapshots=symbol_snapshots,
            config=config,
            now_ms=1_005_000,
        )

        self.assertIsNotNone(opportunity)
        self.assertAlmostEqual(opportunity.short_price, 100.05)
        self.assertAlmostEqual(opportunity.long_price, 99.85)

    def test_collects_funding_filter_reason(self) -> None:
        config = SpreadMonitorConfig(
            venues=["hyperliquid", "lighter"],
            max_funding_rate_pct=0.1,
            max_orderbook_spread_pct=0.1,
            min_cross_exchange_spread_pct=0.2,
            execution_notional_usd=1000.0,
            max_stale_seconds=15,
            max_price_deviation_pct=2.0,
            max_short_term_move_pct=3.0,
            hedge_retry_count=2,
        )
        symbol_snapshots = {
            "BTC": [
                VenueSpreadSnapshot("hyperliquid", "BTC", 101.0, 100.95, 101.0, 0.0020, 1_000_000),
                VenueSpreadSnapshot("lighter", "BTC", 100.0, 99.95, 100.0, 0.0020, 1_000_000),
            ]
        }

        diagnostics = collect_spread_filter_diagnostics(
            symbol_snapshots=symbol_snapshots,
            config=config,
            now_ms=1_005_000,
        )

        self.assertEqual(diagnostics[0].symbol, "BTC")
        self.assertEqual(diagnostics[0].reason, "funding_rate_too_high")

    def test_collects_cross_spread_below_threshold_reason(self) -> None:
        config = SpreadMonitorConfig(
            venues=["hyperliquid", "lighter"],
            max_funding_rate_pct=0.1,
            max_orderbook_spread_pct=0.1,
            min_cross_exchange_spread_pct=0.5,
            execution_notional_usd=1000.0,
            max_stale_seconds=15,
            max_price_deviation_pct=2.0,
            max_short_term_move_pct=3.0,
            hedge_retry_count=2,
        )
        symbol_snapshots = {
            "BTC": [
                VenueSpreadSnapshot("hyperliquid", "BTC", 101.0, 100.20, 100.25, 0.0001, 1_000_000),
                VenueSpreadSnapshot("lighter", "BTC", 100.0, 99.90, 99.95, 0.0001, 1_000_000),
            ]
        }

        diagnostics = collect_spread_filter_diagnostics(
            symbol_snapshots=symbol_snapshots,
            config=config,
            now_ms=1_005_000,
        )

        self.assertEqual(diagnostics[0].symbol, "BTC")
        self.assertEqual(diagnostics[0].reason, "cross_spread_below_threshold")

    def test_collects_pair_level_diagnostic_with_cross_spread_value(self) -> None:
        config = SpreadMonitorConfig(
            venues=["hyperliquid", "lighter"],
            max_funding_rate_pct=0.1,
            max_orderbook_spread_pct=0.1,
            min_cross_exchange_spread_pct=0.5,
            execution_notional_usd=1000.0,
            max_stale_seconds=15,
            max_price_deviation_pct=2.0,
            max_short_term_move_pct=3.0,
            hedge_retry_count=2,
        )
        symbol_snapshots = {
            "BTC": [
                VenueSpreadSnapshot("hyperliquid", "BTC", 101.0, 100.20, 100.25, 0.0001, 1_000_000),
                VenueSpreadSnapshot("lighter", "BTC", 100.0, 99.90, 99.95, 0.0001, 1_000_000),
            ]
        }

        diagnostics = collect_spread_pair_filter_diagnostics(
            symbol_snapshots=symbol_snapshots,
            config=config,
            now_ms=1_005_000,
        )

        self.assertEqual(len(diagnostics), 2)
        self.assertEqual(diagnostics[0].symbol, "BTC")
        self.assertEqual(diagnostics[0].left_venue, "hyperliquid")
        self.assertEqual(diagnostics[0].right_venue, "lighter")
        self.assertEqual(diagnostics[0].reason, "cross_spread_below_threshold")
        self.assertGreater(diagnostics[0].cross_exchange_spread_pct, 0)

    def test_rejects_pair_with_extreme_price_deviation(self) -> None:
        config = SpreadMonitorConfig(
            venues=["lighter", "mexc"],
            max_funding_rate_pct=0.1,
            max_orderbook_spread_pct=0.1,
            min_cross_exchange_spread_pct=0.2,
            execution_notional_usd=1000.0,
            max_stale_seconds=15,
            max_price_deviation_pct=2.0,
            max_short_term_move_pct=3.0,
            hedge_retry_count=2,
        )
        symbol_snapshots = {
            "DIA": [
                VenueSpreadSnapshot("lighter", "DIA", 511.98, 511.97, 511.98, 0.0001, 1_000_000),
                VenueSpreadSnapshot("mexc", "DIA", 0.1670, 0.1669, 0.1670, 0.0001, 1_000_000),
            ]
        }

        opportunity = select_best_spread_opportunity(
            symbol_snapshots=symbol_snapshots,
            config=config,
            now_ms=1_005_000,
        )
        diagnostics = collect_spread_pair_filter_diagnostics(
            symbol_snapshots=symbol_snapshots,
            config=config,
            now_ms=1_005_000,
        )

        self.assertIsNone(opportunity)
        self.assertTrue(any(d.reason == "price_deviation_too_high" for d in diagnostics))


class SpreadFormattingTests(unittest.TestCase):
    def test_format_spread_opportunity_uses_chinese_labels(self) -> None:
        rendered = format_spread_opportunity(
            type(
                "Opportunity",
                (),
                {
                    "symbol": "BTC",
                    "short_venue": "hyperliquid",
                    "long_venue": "lighter",
                    "short_price": 100.05,
                    "long_price": 99.85,
                    "cross_exchange_spread_pct": 0.002,
                    "expected_profit_usd": 2.0,
                },
            )()
        )

        self.assertIn("價差機會", rendered)
        self.assertIn("做空", rendered)
        self.assertIn("做多", rendered)
        self.assertIn("預估利潤", rendered)

    def test_format_spread_opportunity_console_uses_english_labels(self) -> None:
        rendered = format_spread_opportunity_console(
            type(
                "Opportunity",
                (),
                {
                    "symbol": "BTC",
                    "short_venue": "hyperliquid",
                    "long_venue": "lighter",
                    "short_price": 100.05,
                    "long_price": 99.85,
                    "cross_exchange_spread_pct": 0.002,
                    "expected_profit_usd": 2.0,
                },
            )()
        )

        self.assertIn("spread opportunity", rendered)
        self.assertIn("short hyperliquid", rendered)
        self.assertIn("long lighter", rendered)
        self.assertIn("expected profit", rendered)

    def test_format_spread_digest_sorts_by_cross_spread_desc(self) -> None:
        rendered = format_spread_digest(
            [
                type(
                    "Opportunity",
                    (),
                    {
                        "symbol": "ETH",
                        "short_venue": "mexc",
                        "long_venue": "aster",
                        "short_price": 101.0,
                        "long_price": 100.0,
                        "cross_exchange_spread_pct": 0.01,
                        "expected_profit_usd": 100.0,
                    },
                )(),
                type(
                    "Opportunity",
                    (),
                    {
                        "symbol": "BTC",
                        "short_venue": "lighter",
                        "long_venue": "mexc",
                        "short_price": 102.0,
                        "long_price": 100.0,
                        "cross_exchange_spread_pct": 0.02,
                        "expected_profit_usd": 200.0,
                    },
                )(),
            ]
        )

        self.assertTrue(rendered.startswith("價差機會彙總"))
        self.assertIn("#1 | 幣種 BTC", rendered)
        self.assertIn("#2 | 幣種 ETH", rendered)

    def test_format_spread_filter_reason_uses_chinese_labels(self) -> None:
        self.assertEqual(format_spread_filter_reason("funding_rate_too_high"), "資金費過高")

    def test_format_spread_debug_console_uses_english_labels(self) -> None:
        rendered = format_spread_debug_console(symbol="BTC", reason="cross_spread_below_threshold")

        self.assertEqual(rendered, "spread debug | symbol BTC | filtered=cross_spread_below_threshold")

    def test_format_spread_debug_telegram_uses_chinese_labels(self) -> None:
        rendered = format_spread_debug_telegram(symbol="BTC", reason="cross_spread_below_threshold")

        self.assertEqual(rendered, "價差除錯 | 幣種 BTC | 過濾原因=跨所價差低於門檻")

    def test_format_spread_pair_debug_console_uses_english_labels(self) -> None:
        diagnostic = type(
            "PairDiagnostic",
            (),
            {
                "symbol": "BTC",
                "left_venue": "hyperliquid",
                "right_venue": "lighter",
                "cross_exchange_spread_pct": 0.0019,
                "reason": "cross_spread_below_threshold",
            },
        )()

        rendered = format_spread_pair_debug_console(diagnostic)

        self.assertIn("spread debug", rendered)
        self.assertIn("venues hyperliquid/lighter", rendered)
        self.assertIn("symbol BTC", rendered)
        self.assertIn("cross spread", rendered)

    def test_format_spread_pair_debug_telegram_uses_chinese_labels(self) -> None:
        diagnostic = type(
            "PairDiagnostic",
            (),
            {
                "symbol": "BTC",
                "left_venue": "hyperliquid",
                "right_venue": "lighter",
                "cross_exchange_spread_pct": 0.0019,
                "reason": "cross_spread_below_threshold",
            },
        )()

        rendered = format_spread_pair_debug_telegram(diagnostic)

        self.assertIn("價差除錯", rendered)
        self.assertIn("交易所 hyperliquid / lighter", rendered)
        self.assertIn("幣種 BTC", rendered)
        self.assertIn("目前價差", rendered)

    def test_format_spread_progress_console_uses_counts(self) -> None:
        rendered = format_spread_progress_console(
            venues=["hyperliquid", "lighter"],
            asset_ctx_counts={"hyperliquid": 10, "lighter": 5},
            quote_counts={"hyperliquid": 8, "lighter": 4},
            shared_symbols=3,
        )

        self.assertEqual(
            rendered,
            "spread progress | asset_ctx=hyperliquid:10,lighter:5 | quotes=hyperliquid:8,lighter:4 | shared_symbols=3",
        )

    def test_format_spread_live_status_console_uses_english_labels(self) -> None:
        rendered = format_spread_live_status_console(
            venue="aster",
            asset_ctx_count=556,
            quote_count=1,
        )

        self.assertEqual(rendered, "spread live | venue=aster | asset_ctx=556 | quotes=1")


class SpreadEngineTests(unittest.TestCase):
    def test_build_symbol_snapshots_uses_asset_ctx_and_live_quotes(self) -> None:
        symbol_snapshots = build_symbol_snapshots(
            venues=["hyperliquid"],
            asset_ctx_snapshots_by_venue={
                "hyperliquid": {
                    "BTC": {"markPx": 100.0, "funding": 0.0001, "ts_ms": 1234},
                }
            },
            quote_snapshots_by_venue={
                "hyperliquid": {
                    "BTC": {"bid": 99.0, "ask": 100.0, "ts_ms": 1234}
                }
            },
        )

        self.assertEqual(symbol_snapshots["BTC"][0].mark_price, 100.0)
        self.assertEqual(symbol_snapshots["BTC"][0].bid_price, 99.0)
        self.assertEqual(symbol_snapshots["BTC"][0].ask_price, 100.0)

    def test_build_symbol_snapshots_groups_mapped_symbols_under_canonical_symbol(self) -> None:
        symbol_snapshots = build_symbol_snapshots(
            venues=["hyperliquid", "mexc"],
            asset_ctx_snapshots_by_venue={
                "hyperliquid": {
                    "KPEPE": {"markPx": 0.001, "funding": 0.0001, "ts_ms": 1234},
                },
                "mexc": {
                    "1000PEPE": {"markPx": 0.0011, "funding": 0.0002, "ts_ms": 1235},
                },
            },
            quote_snapshots_by_venue={
                "hyperliquid": {
                    "KPEPE": {"bid": 0.001, "ask": 0.00101, "ts_ms": 1234},
                },
                "mexc": {
                    "1000PEPE": {"bid": 0.00109, "ask": 0.0011, "ts_ms": 1235},
                },
            },
        )

        self.assertIn("PEPE", symbol_snapshots)
        self.assertEqual(len(symbol_snapshots["PEPE"]), 2)
        self.assertEqual({snapshot.venue for snapshot in symbol_snapshots["PEPE"]}, {"hyperliquid", "mexc"})


class SpreadRuntimeTests(unittest.TestCase):
    def test_build_stream_bootstrap_prepares_ordered_hyperliquid_and_sorted_mexc_symbols(self) -> None:
        bootstrap = build_stream_bootstrap(
            venues=["mexc", "hyperliquid", "lighter"],
            hyperliquid_ordered_symbols=["ETH", "BTC"],
            lighter_market_map={"BTC": 1},
            mexc_symbols={"SOL", "BTC"},
        )

        self.assertEqual(bootstrap.hyperliquid_ordered_symbols, ["ETH", "BTC"])
        self.assertEqual(bootstrap.lighter_market_map, {"BTC": 1})
        self.assertEqual(bootstrap.mexc_subscription_symbols, ["BTC", "SOL"])
        self.assertEqual(bootstrap.enabled_venues, ["mexc", "hyperliquid", "lighter"])

    def test_parse_lighter_ticker_message_skips_empty_bid_or_ask(self) -> None:
        parsed = parse_lighter_ticker_message(
            {
                "channel": "ticker:123",
                "ticker": {
                    "s": "DIA",
                    "b": {"price": ""},
                    "a": {"price": "511.94"},
                },
                "timestamp": 123456,
            },
            market_by_channel={"ticker:123": "DIA"},
        )

        self.assertEqual(parsed, {})


class TelegramNotificationTests(unittest.TestCase):
    def test_async_send_telegram_offloads_to_sync_sender(self) -> None:
        with mock.patch("hydra_basis.notifications.telegram.send_telegram_sync") as mocked_sync_sender:
            asyncio.run(send_telegram("hello"))

        mocked_sync_sender.assert_called_once_with("hello")


class SpreadPumpRunnerTests(unittest.TestCase):
    def test_pump_runner_forever_yields_between_iterations(self) -> None:
        class FakeRunner:
            def __init__(self) -> None:
                self.calls = 0

            async def pump_once(self) -> None:
                self.calls += 1
                if self.calls >= 2:
                    raise asyncio.CancelledError()

        async def exercise() -> None:
            runner = FakeRunner()
            with mock.patch("hydra_basis.spread_monitor.runtime.asyncio.sleep", new=mock.AsyncMock()) as mocked_sleep:
                with self.assertRaises(asyncio.CancelledError):
                    await pump_runner_forever("fake", runner)
                mocked_sleep.assert_awaited()

        asyncio.run(exercise())
