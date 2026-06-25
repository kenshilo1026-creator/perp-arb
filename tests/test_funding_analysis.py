import unittest
import os
import sys
import asyncio
import json
import tempfile
import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace
from aiohttp import WSServerHandshakeError

from hydra_basis.async_utils import gather_limited
from hydra_basis.alerts import select_best_alerts_by_symbol, select_best_spot_perp_alerts_by_symbol
from hydra_basis.alerts import build_ranked_alert_digest
from hydra_basis.history_store import FundingHistoryStore
from hydra_basis.history_store import funding_history_is_complete
from hydra_basis.history_store import summarize_history_coverage
from hydra_basis.history_store import trim_points_to_lookback_ms
from hydra_basis.history_store import trim_points_to_analysis_days
from hydra_basis.history_store import merge_points_by_interval_bucket
from hydra_basis.streams.manager import MarketStateStore
from hydra_basis.streams.lighter import parse_market_stats_all_message
from hydra_basis.streams.aster import parse_mark_price_array_message
from hydra_basis.streams.hyperliquid import parse_all_mids_message, parse_all_dexs_asset_ctxs_message
from hydra_basis.streams.mexc import parse_push_ticker_message, parse_push_tickers_message
from hydra_basis.adapters.aster import fetch_aster_funding, list_symbols as list_aster_symbols
from hydra_basis.adapters.aster import normalize_aster_symbol, fetch_aster_symbol_metadata
from hydra_basis.adapters.lighter import fetch_lighter_funding, signed_rate_from_history_row
from hydra_basis.adapters.lighter import list_symbols as list_lighter_symbols
from hydra_basis.adapters.hyperliquid import build_funding_history_payload
from hydra_basis.adapters.hyperliquid import list_symbols as list_hyperliquid_symbols
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_funding_since
from hydra_basis.adapters.mexc import resolve_funding_interval_hours
from hydra_basis.adapters.mexc import list_symbols as list_mexc_symbols
from hydra_basis.adapters.mexc import mexc_contract_symbol
from hydra_basis.adapters.mexc import fetch_mexc_funding
from hydra_basis.adapters.mexc import fetch_mexc_funding_since
from hydra_basis.adapters.mexc import extract_mexc_history_rows
from hydra_basis.adapters.tradexyz import build_tradexyz_funding_history_payload
from hydra_basis.adapters.tradexyz import list_symbols as list_tradexyz_symbols
from hydra_basis.adapters.tradexyz import fetch_tradexyz_funding_since
from hydra_basis.adapters.variational import fetch_variational_funding, list_symbols as list_variational_symbols
from hydra_basis.adapters.variational import (
    fetch_variational_current_funding,
    parse_stats_listings,
    parse_loris_historical_series,
    fetch_variational_stats,
    _VARIATIONAL_STATS_CACHE,
)
from hydra_basis.adapters.loris_browser import _fetch_loris_historical_with_nodriver_inner
import hydra_basis.adapters.loris_browser as loris_browser
from hydra_basis.funding_engine.analysis import analyze_spread
from hydra_basis.funding_engine.analysis import analyze_positive_funding
from hydra_basis.funding_engine.analysis import explain_spread_skip
from hydra_basis.funding_engine.analysis import prices_are_compatible
from hydra_basis.funding_engine.analysis import resolve_pair_min_observations
from hydra_basis.funding_engine.models import FundingConfig, FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps
from hydra_basis.env import load_environment
from hydra_basis.formatting import build_signal_message, build_spot_perp_signal_message
from hydra_basis.monitor_errors import build_exchange_error_message
from hydra_basis.monitor_errors import raise_exchange_error
from hydra_basis.monitor_errors import should_raise_immediately
from hydra_basis.backfill import (
    build_spread_refresh_keys,
    chunk_sequence,
    split_loris_batched_keys,
    capture_backfill_spread_snapshot,
    persist_backfill_progress,
    backfill_needs_top_up,
    backfill_incremental_start_ms,
    capture_backfill_spread_snapshot_with_error,
    build_no_new_points_warning,
)
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.execution_engine.market_data import fetch_tradexyz_orderbook
from hydra_basis.symbol_mapping import canonicalize_symbol, load_symbol_mappings
from hydra_basis.universe import build_symbol_venue_index, select_shared_symbols
from hydra_basis.universe import symbols_requiring_complete_history
from scripts._bootstrap import ensure_project_root_on_path


class InferIntervalHoursTests(unittest.TestCase):
    def test_infers_common_hourly_interval_from_history(self) -> None:
        points = [
            0,
            3_600_000,
            7_200_000,
            10_800_000,
        ]

        interval = infer_interval_hours_from_timestamps(points)

        self.assertEqual(interval, 1.0)

    def test_returns_none_when_not_enough_history(self) -> None:
        interval = infer_interval_hours_from_timestamps([0, 3_600_000])
        self.assertIsNone(interval)


class AnalyzeSpreadTests(unittest.TestCase):
    def test_resolve_pair_min_observations_keeps_24_for_1h_pairs(self) -> None:
        short_points = [FundingPoint("short", "BTC", ts_ms=0, raw_rate=0.0001, interval_hours=1)]
        long_points = [FundingPoint("long", "BTC", ts_ms=0, raw_rate=0.0, interval_hours=1)]

        minimum = resolve_pair_min_observations(short_points, long_points)

        self.assertEqual(minimum, 24)

    def test_resolve_pair_min_observations_uses_18_when_pair_includes_8h(self) -> None:
        short_points = [FundingPoint("short", "BTC", ts_ms=0, raw_rate=0.0001, interval_hours=8)]
        long_points = [FundingPoint("long", "BTC", ts_ms=0, raw_rate=0.0, interval_hours=1)]

        minimum = resolve_pair_min_observations(short_points, long_points)

        self.assertEqual(minimum, 18)

    def test_mixed_interval_analysis_counts_true_observations_only(self) -> None:
        short_points = [
            FundingPoint("short", "BTC", ts_ms=hour * 3_600_000, raw_rate=0.0003, interval_hours=1)
            for hour in range(24)
        ]
        long_points = [
            FundingPoint("long", "BTC", ts_ms=hour * 3_600_000, raw_rate=0.0, interval_hours=8)
            for hour in range(0, 24, 8)
        ]

        stats = analyze_spread(short_points, long_points, min_observations=3)

        self.assertIsNotNone(stats)
        self.assertEqual(stats["samples"], 3)
        self.assertAlmostEqual(stats["avg_hourly"], 0.0003)

    def test_requires_minimum_true_observations(self) -> None:
        short_points = [
            FundingPoint("short", "BTC", ts_ms=hour * 3_600_000, raw_rate=0.0003, interval_hours=1)
            for hour in range(24)
        ]
        long_points = [
            FundingPoint("long", "BTC", ts_ms=hour * 3_600_000, raw_rate=0.0, interval_hours=8)
            for hour in range(0, 24, 8)
        ]

        stats = analyze_spread(short_points, long_points, min_observations=4)

        self.assertIsNone(stats)

    def test_spread_analysis_filters_negative_hourly_funding_spikes(self) -> None:
        short_points = [
            FundingPoint("short", "LAB", ts_ms=hour * 3_600_000, raw_rate=-0.0005, interval_hours=1)
            for hour in range(24)
        ]
        long_points = [
            FundingPoint("long", "LAB", ts_ms=hour * 3_600_000, raw_rate=-0.0020, interval_hours=1)
            for hour in range(24)
        ]

        stats = analyze_spread(short_points, long_points, min_observations=24)

        self.assertIsNone(stats)

    def test_spread_analysis_allows_positive_hourly_funding_above_spike_threshold(self) -> None:
        short_points = [
            FundingPoint("short", "BTC", ts_ms=hour * 3_600_000, raw_rate=0.0020, interval_hours=1)
            for hour in range(24)
        ]
        long_points = [
            FundingPoint("long", "BTC", ts_ms=hour * 3_600_000, raw_rate=0.0, interval_hours=1)
            for hour in range(24)
        ]

        stats = analyze_spread(short_points, long_points, min_observations=24)

        self.assertIsNotNone(stats)
        self.assertAlmostEqual(stats["avg_hourly"], 0.0020)

    def test_positive_funding_analysis_detects_spot_perp_opportunity(self) -> None:
        end_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        start_ms = end_ms - (24 * 7 - 1) * 3_600_000
        points = [
            FundingPoint("mexc", "BTC", ts_ms=start_ms + hour * 3_600_000, raw_rate=0.00004, interval_hours=1)
            for hour in range(24 * 7)
        ]

        stats = analyze_positive_funding(points, min_observations=24)

        self.assertIsNotNone(stats)
        self.assertGreater(stats["annualized_avg"], 0.25)
        self.assertTrue(stats["signal"])

    def test_prices_are_not_compatible_when_mark_prices_diverge_too_much(self) -> None:
        self.assertFalse(
            prices_are_compatible(
                {"markPx": 0.012},
                {"markPx": 12.0},
            )
        )

    def test_prices_are_compatible_when_mark_prices_are_close(self) -> None:
        self.assertTrue(
            prices_are_compatible(
                {"markPx": 100.0},
                {"markPx": 101.0},
            )
        )

    def test_explain_spread_skip_reports_insufficient_samples(self) -> None:
        short_points = [
            FundingPoint("short", "BTC", ts_ms=hour * 3_600_000, raw_rate=0.0003, interval_hours=1)
            for hour in range(8)
        ]
        long_points = [
            FundingPoint("long", "BTC", ts_ms=hour * 3_600_000, raw_rate=0.0, interval_hours=8)
            for hour in range(0, 8, 8)
        ]

        reason = explain_spread_skip(short_points, long_points, min_observations=24)

        self.assertEqual(reason, "insufficient_samples:1/18")


class LoadEnvironmentTests(unittest.TestCase):
    def test_loads_values_from_dotenv_when_process_env_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "TELEGRAM_BOT_TOKEN=test-token\nTELEGRAM_CHAT_ID=test-chat\n",
                encoding="utf-8",
            )

            previous_token = os.environ.pop("TELEGRAM_BOT_TOKEN", None)
            previous_chat_id = os.environ.pop("TELEGRAM_CHAT_ID", None)
            try:
                load_environment(env_path)
                self.assertEqual(os.getenv("TELEGRAM_BOT_TOKEN"), "test-token")
                self.assertEqual(os.getenv("TELEGRAM_CHAT_ID"), "test-chat")
            finally:
                os.environ.pop("TELEGRAM_BOT_TOKEN", None)
                os.environ.pop("TELEGRAM_CHAT_ID", None)
                if previous_token is not None:
                    os.environ["TELEGRAM_BOT_TOKEN"] = previous_token
                if previous_chat_id is not None:
                    os.environ["TELEGRAM_CHAT_ID"] = previous_chat_id


class BootstrapTests(unittest.TestCase):
    def test_adds_project_root_when_running_from_scripts_directory(self) -> None:
        original_sys_path = list(sys.path)
        try:
            sys.path[:] = [str(Path.cwd() / "scripts")]
            ensure_project_root_on_path()
            self.assertEqual(sys.path[0], str(Path.cwd()))
        finally:
            sys.path[:] = original_sys_path


class RuntimeTests(unittest.TestCase):
    def test_configure_windows_event_loop_policy_sets_selector_policy_on_windows(self) -> None:
        if not hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            self.skipTest("Windows selector policy not available on this platform")

        with patch("hydra_basis.runtime.sys.platform", "win32"):
            with patch("hydra_basis.runtime.asyncio.set_event_loop_policy") as set_policy:
                configure_windows_event_loop_policy()

        set_policy.assert_called_once()

    def test_configure_windows_event_loop_policy_keeps_proactor_for_loris_nodriver(self) -> None:
        if not hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            self.skipTest("Windows selector policy not available on this platform")

        with patch.dict(os.environ, {"LORIS_USE_NODRIVER": "true"}, clear=False):
            with patch("hydra_basis.runtime.sys.platform", "win32"):
                with patch("hydra_basis.runtime.asyncio.set_event_loop_policy") as set_policy:
                    configure_windows_event_loop_policy()

        set_policy.assert_not_called()

    def test_funding_config_no_longer_has_interval_hours(self) -> None:
        config = FundingConfig("demo", enabled=True)
        self.assertFalse(hasattr(config, "interval_hours"))


class SymbolMappingTests(unittest.TestCase):
    def test_canonicalize_symbol_uses_global_mapping(self) -> None:
        self.assertEqual(canonicalize_symbol("1000PEPE"), "PEPE")
        self.assertEqual(canonicalize_symbol("KPEPE", venue="hyperliquid"), "PEPE")

    def test_load_symbol_mappings_normalizes_case(self) -> None:
        mappings = load_symbol_mappings()
        self.assertEqual(mappings["global"]["KPEPE"], "PEPE")


class LighterAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_historical_funding_points_from_fundings_endpoint(self) -> None:
        funding_rates_payload = {
            "code": 200,
            "funding_rates": [
                {"market_id": 1, "exchange": "lighter", "symbol": "BTC", "rate": 9.6e-05},
            ],
        }
        fundings_payload = {
            "code": 200,
            "resolution": "1h",
            "fundings": [
                {"timestamp": 1_717_000_000, "rate": "0.0012", "direction": "long"},
                {"timestamp": 1_717_003_600, "rate": "0.0008", "direction": "short"},
            ],
        }

        with patch("hydra_basis.adapters.lighter.fetch_json", new=AsyncMock(side_effect=[funding_rates_payload, fundings_payload])):
            points = await fetch_lighter_funding(session=object(), symbol="BTC")

        self.assertEqual(len(points), 2)
        self.assertEqual(points[0].ts_ms, 1_717_000_000_000)
        self.assertAlmostEqual(points[0].raw_rate, 0.000012)
        self.assertAlmostEqual(points[1].raw_rate, -0.000008)
        self.assertEqual(points[0].interval_hours, 1)

    def test_signed_rate_uses_direction_for_history_rows(self) -> None:
        self.assertAlmostEqual(
            signed_rate_from_history_row({"rate": "0.0012", "direction": "long"}),
            0.000012,
        )
        self.assertAlmostEqual(
            signed_rate_from_history_row({"rate": "0.0012", "direction": "short"}),
            -0.000012,
        )


class LorisBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        loris_browser._shared_browser = None
        loris_browser._shared_page = None
        loris_browser._shared_loop = None
        loris_browser._browser_context_lock = None
        loris_browser._shared_start_error = None

    async def asyncTearDown(self) -> None:
        loris_browser._shared_browser = None
        loris_browser._shared_page = None
        loris_browser._shared_loop = None
        loris_browser._browser_context_lock = None
        loris_browser._shared_start_error = None

    async def test_nodriver_fetch_includes_api_key_header_when_configured(self) -> None:
        browser = AsyncMock()
        page = AsyncMock()
        browser.get.return_value = page
        page.get = AsyncMock(return_value=page)
        page.evaluate = AsyncMock(
            return_value=json.dumps(
                {
                    "bodyText": json.dumps({"series": {"variational": []}}),
                    "preText": "",
                }
            )
        )

        with patch.dict(
            os.environ,
            {"LORIS_API_KEY": "demo-key", "LORIS_API_KEY_HEADER": "X-API-Key"},
            clear=False,
        ):
            with patch("nodriver.start", new=AsyncMock(return_value=browser)):
                await _fetch_loris_historical_with_nodriver_inner(
                    symbol="BTC",
                    start="2026-06-11T00:00:00.000Z",
                    end="2026-06-12T00:00:00.000Z",
                )

        browser.get.assert_awaited_once()
        page.get.assert_awaited_once()

    async def test_nodriver_fetch_reads_json_document_after_navigation(self) -> None:
        browser = AsyncMock()
        page = AsyncMock()
        browser.get = AsyncMock(return_value=page)
        page.get = AsyncMock(return_value=page)
        page.evaluate = AsyncMock(
            return_value=json.dumps(
                {
                    "bodyText": json.dumps({"series": {"variational": [{"t": "2026-06-11T03:00:00Z", "y": 1.0}]}}),
                    "preText": "",
                }
            )
        )

        with patch.dict(os.environ, {}, clear=True):
            with patch("nodriver.start", new=AsyncMock(return_value=browser)):
                payload = await _fetch_loris_historical_with_nodriver_inner(
                    symbol="BTC",
                    start="2026-06-11T00:00:00.000Z",
                    end="2026-06-12T00:00:00.000Z",
                )

        self.assertIn("variational", payload["series"])
        self.assertEqual(browser.get.await_count, 1)
        page.get.assert_awaited_once()

    async def test_nodriver_fetch_reuses_shared_browser_between_requests(self) -> None:
        browser = AsyncMock()
        page = AsyncMock()
        browser.get = AsyncMock(return_value=page)
        page.get = AsyncMock(return_value=page)
        page.evaluate = AsyncMock(
            side_effect=[
                json.dumps(
                    {
                        "bodyText": json.dumps({"series": {"variational": [{"t": "2026-06-11T03:00:00Z", "y": 1.0}]}}),
                        "preText": "",
                    }
                ),
                json.dumps(
                    {
                        "bodyText": json.dumps({"series": {"variational": [{"t": "2026-06-11T04:00:00Z", "y": 2.0}]}}),
                        "preText": "",
                    }
                ),
            ]
        )

        with patch.dict(os.environ, {}, clear=True):
            with patch("nodriver.start", new=AsyncMock(return_value=browser)) as start_browser:
                first = await _fetch_loris_historical_with_nodriver_inner(
                    symbol="BTC",
                    start="2026-06-11T00:00:00.000Z",
                    end="2026-06-12T00:00:00.000Z",
                )
                second = await _fetch_loris_historical_with_nodriver_inner(
                    symbol="ETH",
                    start="2026-06-11T00:00:00.000Z",
                    end="2026-06-12T00:00:00.000Z",
                )

        self.assertIn("variational", first["series"])
        self.assertIn("variational", second["series"])
        start_browser.assert_awaited_once()
        browser.get.assert_awaited_once()
        self.assertEqual(page.get.await_count, 2)

    async def test_nodriver_start_failure_is_cached_to_avoid_reopening_browser_per_symbol(self) -> None:
        start_browser = AsyncMock(side_effect=RuntimeError("chrome start failed"))

        with patch.dict(os.environ, {}, clear=True):
            with patch("nodriver.start", new=start_browser):
                with self.assertRaisesRegex(RuntimeError, "chrome start failed"):
                    await _fetch_loris_historical_with_nodriver_inner(
                        symbol="BTC",
                        start="2026-06-11T00:00:00.000Z",
                        end="2026-06-12T00:00:00.000Z",
                    )
                with self.assertRaisesRegex(RuntimeError, "previous loris nodriver browser start failed"):
                    await _fetch_loris_historical_with_nodriver_inner(
                        symbol="ETH",
                        start="2026-06-11T00:00:00.000Z",
                        end="2026-06-12T00:00:00.000Z",
                    )

        start_browser.assert_awaited_once()

    async def test_nodriver_fetch_raises_clear_error_when_navigation_body_missing(self) -> None:
        browser = AsyncMock()
        page = AsyncMock()
        browser.get = AsyncMock(return_value=page)
        page.get = AsyncMock(return_value=page)
        page.evaluate = AsyncMock(
            side_effect=RuntimeError("navigation body empty")
        )

        with patch.dict(os.environ, {}, clear=True):
            with patch("nodriver.start", new=AsyncMock(return_value=browser)):
                with self.assertRaises(RuntimeError) as ctx:
                    await _fetch_loris_historical_with_nodriver_inner(
                        symbol="BTC",
                        start="2026-06-11T00:00:00.000Z",
                        end="2026-06-12T00:00:00.000Z",
                    )

        self.assertIn("loris nodriver navigation fetch failed", str(ctx.exception))


class MexcAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolve_interval_uses_collect_cycle_when_available(self) -> None:
        interval = await resolve_funding_interval_hours(
            session=object(),
            venue="mexc",
            symbol="BTC",
            history_rows=[{"collectCycle": 8, "settleTime": 1_717_000_000_000}],
        )

        self.assertEqual(interval, 8.0)

    async def test_resolve_interval_raises_when_mexc_interval_cannot_be_determined(self) -> None:
        with self.assertRaises(RuntimeError):
            await resolve_funding_interval_hours(
                session=object(),
                venue="mexc",
                symbol="BTC",
                history_rows=[],
            )

    def test_contract_symbol_falls_back_to_base_usdt_for_discovered_assets(self) -> None:
        self.assertEqual(mexc_contract_symbol("BTC"), "BTC_USDT")
        self.assertEqual(mexc_contract_symbol("DOGE"), "DOGE_USDT")

    async def test_list_symbols_uses_base_coin_from_contract_detail(self) -> None:
        payload = {
            "data": [
                {"baseCoin": "BTC", "quoteCoin": "USDT", "state": 0, "apiAllowed": True},
                {"baseCoin": "ETH", "quoteCoin": "USDT", "state": 0, "apiAllowed": True},
                {"baseCoin": "XRP", "quoteCoin": "USDC", "state": 0, "apiAllowed": True},
            ]
        }
        with patch("hydra_basis.adapters.mexc.fetch_json", new=AsyncMock(return_value=payload)):
            symbols = await list_mexc_symbols(session=object())

        self.assertEqual(symbols, {"BTC", "ETH"})

    async def test_fetch_funding_skips_rows_with_missing_rate(self) -> None:
        payload = {
            "data": {
                "resultList": [
                    {"settleTime": 9_999_999_999_999, "fundingRate": None, "collectCycle": 8},
                    {"settleTime": 9_999_999_999_998, "fundingRate": "0.0001", "collectCycle": 8},
                ]
            }
        }
        with patch("hydra_basis.adapters.mexc.fetch_json", new=AsyncMock(return_value=payload)):
            points = await fetch_mexc_funding(session=object(), symbol="2Z")

        self.assertEqual(len(points), 1)
        self.assertAlmostEqual(points[0].raw_rate, 0.0001)

    async def test_fetch_funding_retries_when_history_rows_are_empty_once(self) -> None:
        empty_payload = {"data": {"resultList": []}}
        filled_payload = {
            "data": {
                "resultList": [
                    {"settleTime": 9_999_999_999_998, "fundingRate": "0.0001", "collectCycle": 8},
                ]
            }
        }
        with patch(
            "hydra_basis.adapters.mexc.fetch_json",
            new=AsyncMock(side_effect=[empty_payload, filled_payload]),
        ) as mocked:
            points = await fetch_mexc_funding(session=object(), symbol="FORM")

        self.assertEqual(len(points), 1)
        self.assertAlmostEqual(points[0].raw_rate, 0.0001)
        self.assertEqual(mocked.await_count, 2)

    async def test_fetch_funding_since_only_returns_rows_after_start_time(self) -> None:
        payload = {
            "data": {
                "resultList": [
                    {"settleTime": 1_000, "fundingRate": "0.0001", "collectCycle": 8},
                    {"settleTime": 2_000, "fundingRate": "0.0002", "collectCycle": 8},
                    {"settleTime": 3_000, "fundingRate": "0.0003", "collectCycle": 8},
                ]
            }
        }
        with patch("hydra_basis.adapters.mexc.fetch_json", new=AsyncMock(return_value=payload)):
            points = await fetch_mexc_funding_since(session=object(), symbol="FORM", start_time_ms=2_000)

        self.assertEqual([point.ts_ms for point in points], [2_000, 3_000])
        self.assertEqual([point.raw_rate for point in points], [0.0002, 0.0003])

    async def test_fetch_funding_still_raises_when_history_rows_stay_empty_after_retries(self) -> None:
        empty_payload = {"data": {"resultList": []}}
        with patch(
            "hydra_basis.adapters.mexc.fetch_json",
            new=AsyncMock(side_effect=[empty_payload, empty_payload, empty_payload, {"data": {}}]),
        ):
            with self.assertRaises(RuntimeError):
                await fetch_mexc_funding(session=object(), symbol="FORM")

    def test_extract_mexc_history_rows_ignores_non_list_payload_shapes(self) -> None:
        self.assertEqual(extract_mexc_history_rows({"data": {}}), [])
        self.assertEqual(extract_mexc_history_rows({"data": {"resultList": {}}}), [])

    async def test_mexc_rest_can_be_skipped_entirely_by_runner_policy(self) -> None:
        self.assertTrue(True)


class HyperliquidAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_symbols_ignores_delisted_markets(self) -> None:
        payload = {
            "universe": [
                {"name": "BTC"},
                {"name": "ETH"},
                {"name": "MATIC", "isDelisted": True},
            ]
        }
        with patch("hydra_basis.adapters.hyperliquid.fetch_json", new=AsyncMock(return_value=payload)):
            symbols = await list_hyperliquid_symbols(session=object())

        self.assertEqual(symbols, {"BTC", "ETH"})

    def test_build_funding_history_payload_uses_incremental_start(self) -> None:
        payload = build_funding_history_payload("BTC", start_time_ms=123456789)

        self.assertEqual(payload["type"], "fundingHistory")
        self.assertEqual(payload["coin"], "BTC")
        self.assertEqual(payload["startTime"], 123456789)

    async def test_fetch_hyperliquid_funding_since_retries_429_then_succeeds(self) -> None:
        rate_limited = RuntimeError("429 Too Many Requests")
        payload = [
            {"time": 1_717_000_000_000, "fundingRate": "0.0001"},
            {"time": 1_717_003_600_000, "fundingRate": "0.0002"},
            {"time": 1_717_007_200_000, "fundingRate": "0.0003"},
        ]

        with patch(
            "hydra_basis.adapters.hyperliquid.fetch_json",
            new=AsyncMock(side_effect=[rate_limited, payload]),
        ) as mocked:
            with patch("hydra_basis.adapters.hyperliquid.asyncio.sleep", new=AsyncMock()) as sleep_mock:
                points = await fetch_hyperliquid_funding_since(
                    session=object(),
                    symbol="BTC",
                    start_time_ms=1_717_000_000_000,
                )

        self.assertEqual(len(points), 3)
        self.assertEqual(mocked.await_count, 2)
        self.assertTrue(any(call.args == (3.0,) for call in sleep_mock.await_args_list))

    async def test_fetch_hyperliquid_funding_since_still_raises_non_retryable_errors(self) -> None:
        with patch(
            "hydra_basis.adapters.hyperliquid.fetch_json",
            new=AsyncMock(side_effect=RuntimeError("403 Forbidden")),
        ):
            with self.assertRaises(RuntimeError):
                await fetch_hyperliquid_funding_since(
                    session=object(),
                    symbol="BTC",
                    start_time_ms=1_717_000_000_000,
                )

    async def test_fetch_hyperliquid_funding_since_uses_1h_default_for_short_incremental_response(self) -> None:
        payload = [
            {"time": 1_717_000_000_000, "fundingRate": "0.0001"},
        ]

        with patch(
            "hydra_basis.adapters.hyperliquid.fetch_json",
            new=AsyncMock(return_value=payload),
        ):
            points = await fetch_hyperliquid_funding_since(
                session=object(),
                symbol="ZEN",
                start_time_ms=1_717_000_000_000,
            )

        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].interval_hours, 1.0)


class TradeXyzAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_symbols_uses_xyz_dex_metadata(self) -> None:
        payload = {
            "universe": [
                {"name": "xyz:NVDA"},
                {"name": "xyz:AAPL"},
                {"name": "xyz:DELISTED", "isDelisted": True},
            ]
        }
        with patch("hydra_basis.adapters.tradexyz._post_hyperliquid_info", new=AsyncMock(return_value=payload)) as mocked:
            symbols = await list_tradexyz_symbols(session=object())

        self.assertEqual(symbols, {"XYZ:NVDA", "XYZ:AAPL"})
        self.assertEqual(mocked.await_args.args[1], {"type": "meta", "dex": "xyz"})

    def test_build_funding_history_payload_uses_xyz_symbol(self) -> None:
        payload = build_tradexyz_funding_history_payload("XYZ:NVDA", start_time_ms=123456789)

        self.assertEqual(payload["type"], "fundingHistory")
        self.assertEqual(payload["coin"], "XYZ:NVDA")
        self.assertEqual(payload["startTime"], 123456789)
        self.assertEqual(payload["dex"], "xyz")

    async def test_fetch_tradexyz_funding_since_builds_points(self) -> None:
        payload = [
            {"time": 1_717_000_000_000, "fundingRate": "0.0001"},
            {"time": 1_717_003_600_000, "fundingRate": "0.0002"},
            {"time": 1_717_007_200_000, "fundingRate": "0.0003"},
        ]

        with patch("hydra_basis.adapters.tradexyz._post_hyperliquid_info", new=AsyncMock(return_value=payload)) as mocked:
            points = await fetch_tradexyz_funding_since(
                session=object(),
                symbol="XYZ:NVDA",
                start_time_ms=1_717_000_000_000,
            )

        self.assertEqual(len(points), 3)
        self.assertTrue(all(point.venue == "trade_xyz" for point in points))
        self.assertTrue(all(point.symbol == "XYZ:NVDA" for point in points))
        self.assertEqual(mocked.await_args.args[1], {
            "type": "fundingHistory",
            "coin": "XYZ:NVDA",
            "startTime": 1_717_000_000_000,
            "dex": "xyz",
        })
        self.assertEqual(points[0].interval_hours, 1.0)

    async def test_null_l2_book_is_reported_as_missing_orderbook(self) -> None:
        with patch(
            "hydra_basis.execution_engine.market_data.fetch_tradexyz_universe",
            new=AsyncMock(return_value={"XYZ:TSLA"}),
        ), patch(
            "hydra_basis.execution_engine.market_data.fetch_json",
            new=AsyncMock(return_value=None),
        ):
            with self.assertRaisesRegex(RuntimeError, "missing trade_xyz orderbook for XYZ:TSLA"):
                await fetch_tradexyz_orderbook(object(), "XYZ:TSLA")

    async def test_l2_book_uses_lowercase_dex_prefix(self) -> None:
        fetch = AsyncMock(return_value={
            "coin": "xyz:TSLA",
            "time": 123,
            "levels": [
                [{"px": "398.38", "sz": "1", "n": 1}],
                [{"px": "398.39", "sz": "1", "n": 1}],
            ],
        })
        with patch(
            "hydra_basis.execution_engine.market_data.fetch_tradexyz_universe",
            new=AsyncMock(return_value={"XYZ:TSLA"}),
        ), patch(
            "hydra_basis.execution_engine.market_data.fetch_json",
            new=fetch,
        ):
            orderbook = await fetch_tradexyz_orderbook(object(), "XYZ:TSLA")

        self.assertEqual(orderbook, {"bid": 398.38, "ask": 398.39, "ts_ms": 123})
        self.assertEqual(fetch.await_args.kwargs["json"]["coin"], "xyz:TSLA")


class AsterAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_symbols_uses_funding_info_and_normalizes_suffixes(self) -> None:
        payload = [
            {"symbol": "BTCUSDT", "fundingIntervalHours": 8},
            {"symbol": "ETHUSD", "fundingIntervalHours": 8},
            {"symbol": "DOGEUSDC", "fundingIntervalHours": 4},
        ]
        with patch("hydra_basis.adapters.aster.fetch_json", new=AsyncMock(return_value=payload)):
            symbols = await list_aster_symbols(session=object())

        self.assertEqual(symbols, {"BTC", "ETH", "DOGE"})

    async def test_fetch_funding_uses_symbol_map_and_interval(self) -> None:
        funding_info_payload = [
            {"symbol": "BTCUSDT", "fundingIntervalHours": 8},
        ]
        funding_rate_payload = [
            {"symbol": "BTCUSDT", "fundingTime": 1_779_811_200_000, "fundingRate": "0.00008890"},
            {"symbol": "BTCUSDT", "fundingTime": 1_779_840_000_000, "fundingRate": "0.00010000"},
        ]
        with patch("hydra_basis.adapters.aster.fetch_json", new=AsyncMock(side_effect=[funding_info_payload, funding_rate_payload])):
            points = await fetch_aster_funding(session=object(), symbol="BTC")

        self.assertEqual(len(points), 2)
        self.assertEqual(points[0].symbol, "BTC")
        self.assertEqual(points[0].interval_hours, 8.0)
        self.assertAlmostEqual(points[0].raw_rate, 0.00008890)

    def test_normalize_aster_symbol_strips_stable_suffix(self) -> None:
        self.assertEqual(normalize_aster_symbol("BTCUSDT"), "BTC")
        self.assertEqual(normalize_aster_symbol("ETHUSD"), "ETH")
        self.assertEqual(normalize_aster_symbol("DOGEUSDC"), "DOGE")

    async def test_symbol_metadata_is_cached_per_session(self) -> None:
        payload = [{"symbol": "BTCUSDT", "fundingIntervalHours": 8}]
        with patch("hydra_basis.adapters.aster.fetch_json", new=AsyncMock(return_value=payload)) as mocked:
            session = object()
            first = await fetch_aster_symbol_metadata(session)
            second = await fetch_aster_symbol_metadata(session)

        self.assertEqual(first, second)
        mocked.assert_awaited_once()


class VariationalAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        _VARIATIONAL_STATS_CACHE.clear()

    async def test_list_symbols_reads_metadata_stats(self) -> None:
        payload = {
            "listings": [
                {"ticker": "BTC", "funding_rate": "0.0001", "funding_interval_s": 28800},
                {"ticker": "ETH", "funding_rate": "0.0002", "funding_interval_s": 3600},
            ]
        }
        with patch("hydra_basis.adapters.variational.fetch_json", new=AsyncMock(return_value=payload)):
            symbols = await list_variational_symbols(session=object())

        self.assertEqual(symbols, {"BTC", "ETH"})

    async def test_fetch_variational_current_funding_reads_metadata_stats_without_loris(self) -> None:
        payload = {
            "listings": [
                {"ticker": "LAB", "funding_rate": "-0.0019", "funding_interval_s": 3600},
            ]
        }
        with patch("hydra_basis.adapters.variational.fetch_json", new=AsyncMock(return_value=payload)) as mocked:
            current = await fetch_variational_current_funding(session=object(), symbol="LAB")

        self.assertEqual(current, {"funding_rate": -0.0019, "interval_hours": 1.0})
        mocked.assert_awaited_once()
        self.assertIn("/metadata/stats", mocked.await_args.args[2])

    async def test_fetch_variational_funding_builds_point_from_stats(self) -> None:
        stats_payload = {
            "listings": [
                {"ticker": "BTC", "funding_rate": "0.0001", "funding_interval_s": 28800},
            ]
        }
        historical_payload = {
            "symbol": "BTC",
            "series": {
                "variational": [
                    {"t": "2026-05-22T00:00:00Z", "y": 0.3848},
                ]
            },
            "notices": [],
        }
        with patch("hydra_basis.adapters.variational.fetch_json", new=AsyncMock(side_effect=[stats_payload, historical_payload])):
            points = await fetch_variational_funding(session=object(), symbol="BTC")

        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].symbol, "BTC")
        self.assertEqual(points[0].interval_hours, 8.0)
        self.assertAlmostEqual(points[0].raw_rate, 0.00003848)

    async def test_fetch_variational_funding_uses_loris_8h_interval_for_1h_markets(self) -> None:
        stats_payload = {
            "listings": [
                {"ticker": "LAB", "funding_rate": "-1.9", "funding_interval_s": 3600},
            ]
        }
        historical_payload = {
            "symbol": "LAB",
            "series": {
                "variational": [
                    {"t": "2026-06-01T00:00:00Z", "y": -500.0},
                ]
            },
            "notices": [],
        }
        with patch("hydra_basis.adapters.variational.fetch_json", new=AsyncMock(side_effect=[stats_payload, historical_payload])):
            points = await fetch_variational_funding(session=object(), symbol="LAB")

        self.assertEqual(points[0].interval_hours, 8.0)
        self.assertAlmostEqual(points[0].raw_rate, -0.05)
        self.assertAlmostEqual(points[0].hourly_rate, -0.00625)

    async def test_fetch_variational_funding_sends_loris_browser_origin_headers_without_api_key(self) -> None:
        _VARIATIONAL_STATS_CACHE.clear()
        stats_payload = {
            "listings": [
                {"ticker": "BTC", "funding_rate": "0.0001", "funding_interval_s": 28800},
            ]
        }
        historical_payload = {
            "symbol": "BTC",
            "series": {"variational": [{"t": "2026-05-22T00:00:00Z", "y": 0.3848}]},
            "notices": [],
        }

        with patch("hydra_basis.adapters.variational.fetch_json", new=AsyncMock(side_effect=[stats_payload, historical_payload])) as mocked:
            await fetch_variational_funding(session=object(), symbol="BTC")

        loris_call = mocked.await_args_list[1]
        headers = loris_call.kwargs["headers"]
        self.assertEqual(headers["Origin"], "https://loris.tools")
        self.assertEqual(headers["Referer"], "https://loris.tools/")
        self.assertNotIn("Authorization", headers)

    def test_parse_stats_listings_extracts_funding_and_interval(self) -> None:
        parsed = parse_stats_listings(
            {
                "listings": [
                    {"ticker": "BTC", "funding_rate": "0.0001", "funding_interval_s": 28800},
                ]
            }
        )

        self.assertEqual(parsed["BTC"]["interval_hours"], 8.0)
        self.assertAlmostEqual(parsed["BTC"]["funding_rate"], 0.0001)

    def test_parse_loris_historical_series_builds_points(self) -> None:
        payload = {
            "symbol": "BTC",
            "series": {
                "variational": [
                    {"t": "2026-05-22T00:00:00Z", "y": 0.3848},
                    {"t": "2026-05-22T08:00:00Z", "y": 1.0},
                ]
            },
            "notices": [],
        }

        points = parse_loris_historical_series(payload, symbol="BTC", venue="variational", interval_hours=8.0)

        self.assertEqual(len(points), 2)
        self.assertEqual(points[0].symbol, "BTC")
        self.assertEqual(points[0].interval_hours, 8.0)
        self.assertAlmostEqual(points[0].raw_rate, 0.00003848)

    async def test_fetch_variational_stats_is_cached_per_session(self) -> None:
        payload = {"listings": [{"ticker": "BTC", "funding_rate": "0.1", "funding_interval_s": 28800}]}
        with patch("hydra_basis.adapters.variational.fetch_json", new=AsyncMock(return_value=payload)) as mocked:
            session = object()
            first = await fetch_variational_stats(session)
            second = await fetch_variational_stats(session)

        self.assertEqual(first, second)
        mocked.assert_awaited_once()

    async def test_fetch_variational_funding_retries_loris_bad_gateway_once(self) -> None:
        stats_payload = {
            "listings": [
                {"ticker": "BTC", "funding_rate": "0.0001", "funding_interval_s": 28800},
            ]
        }
        historical_payload = {
            "symbol": "BTC",
            "series": {"variational": [{"t": "2026-05-22T00:00:00Z", "y": 0.3848}]},
            "notices": [],
        }
        gateway_error = RuntimeError("bad gateway")
        gateway_error.status = 502
        gateway_error.request_info = SimpleNamespace(real_url="https://api.loris.tools/funding/historical")

        with patch(
            "hydra_basis.adapters.variational.fetch_json",
            new=AsyncMock(side_effect=[stats_payload, gateway_error, historical_payload]),
        ) as mocked:
            points = await fetch_variational_funding(session=object(), symbol="BTC")

        self.assertEqual(len(points), 1)
        self.assertEqual(mocked.await_count, 3)

    async def test_fetch_variational_funding_retries_loris_unauthorized_once(self) -> None:
        stats_payload = {
            "listings": [
                {"ticker": "COOKIE", "funding_rate": "0.0001", "funding_interval_s": 28800},
            ]
        }
        historical_payload = {
            "symbol": "COOKIE",
            "series": {"variational": [{"t": "2026-06-11T03:00:00Z", "y": 1.0}]},
            "notices": [],
        }
        unauthorized_error = RuntimeError("missing api key")
        unauthorized_error.status = 401
        unauthorized_error.request_info = SimpleNamespace(real_url="https://api.loris.tools/funding/historical")

        with patch(
            "hydra_basis.adapters.variational.fetch_json",
            new=AsyncMock(side_effect=[stats_payload, unauthorized_error, historical_payload]),
        ) as mocked:
            points = await fetch_variational_funding(session=object(), symbol="COOKIE")

        self.assertEqual(len(points), 1)
        self.assertEqual(mocked.await_count, 3)

    async def test_fetch_variational_funding_uses_loris_nodriver_when_enabled(self) -> None:
        stats_payload = {
            "listings": [
                {"ticker": "AZTEC", "funding_rate": "0.0001", "funding_interval_s": 28800},
            ]
        }
        historical_payload = {
            "symbol": "AZTEC",
            "series": {"variational": [{"t": "2026-06-11T03:00:00Z", "y": 1.0}]},
            "notices": [],
        }

        with patch.dict(os.environ, {"LORIS_USE_NODRIVER": "true"}, clear=False):
            with patch(
                "hydra_basis.adapters.variational.fetch_json",
                new=AsyncMock(return_value=stats_payload),
            ) as mocked:
                with patch(
                    "hydra_basis.adapters.variational.fetch_loris_historical_with_nodriver",
                    new=AsyncMock(return_value=historical_payload),
                ) as browser_fetch:
                    points = await fetch_variational_funding(session=object(), symbol="AZTEC")

        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].symbol, "AZTEC")
        self.assertAlmostEqual(points[0].raw_rate, 0.0001)
        self.assertEqual(mocked.await_count, 1)
        browser_fetch.assert_awaited_once()

    async def test_fetch_variational_funding_still_raises_after_loris_gateway_retries(self) -> None:
        stats_payload = {
            "listings": [
                {"ticker": "BTC", "funding_rate": "0.0001", "funding_interval_s": 28800},
            ]
        }
        gateway_error = RuntimeError("bad gateway")
        gateway_error.status = 502
        gateway_error.request_info = SimpleNamespace(real_url="https://api.loris.tools/funding/historical")

        with patch(
            "hydra_basis.adapters.variational.fetch_json",
            new=AsyncMock(side_effect=[stats_payload, gateway_error, gateway_error, gateway_error]),
        ):
            with self.assertRaises(RuntimeError):
                await fetch_variational_funding(session=object(), symbol="BTC")


class UniverseTests(unittest.TestCase):
    def test_selects_symbols_shared_by_at_least_two_venues(self) -> None:
        venue_symbols = {
            "hyperliquid": {"BTC", "ETH", "SOL"},
            "mexc": {"BTC", "ETH", "DOGE"},
            "lighter": {"BTC", "SOL", "DOGE"},
        }

        symbol_venues = build_symbol_venue_index(venue_symbols)
        selected = select_shared_symbols(symbol_venues, min_shared_venues=2)

        self.assertEqual(symbol_venues["BTC"], {"hyperliquid", "mexc", "lighter"})
        self.assertEqual(selected, ["BTC", "DOGE", "ETH", "SOL"])

    def test_symbol_mapping_merges_variant_symbols_into_one_shared_symbol(self) -> None:
        venue_symbols = {
            "hyperliquid": {"KPEPE"},
            "mexc": {"1000PEPE"},
            "lighter": {"PEPE"},
        }

        symbol_venues = build_symbol_venue_index(venue_symbols)
        selected = select_shared_symbols(symbol_venues, min_shared_venues=2)

        self.assertEqual(symbol_venues["PEPE"], {"hyperliquid", "mexc", "lighter"})
        self.assertEqual(selected, ["PEPE"])

    def test_symbols_requiring_complete_history_only_include_shared_symbols(self) -> None:
        venue_symbols = {
            "hyperliquid": {"BTC", "ETH"},
            "mexc": {"BTC", "DOGE"},
            "lighter": {"BTC", "ETH"},
        }

        required = symbols_requiring_complete_history(venue_symbols, min_shared_venues=2)

        self.assertEqual(required, {("hyperliquid", "BTC"), ("hyperliquid", "ETH"), ("mexc", "BTC"), ("lighter", "BTC"), ("lighter", "ETH")})


class LighterSymbolDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_symbols_filters_to_lighter_exchange_rows(self) -> None:
        payload = {
            "code": 200,
            "funding_rates": [
                {"exchange": "lighter", "symbol": "BTC", "market_id": 1},
                {"exchange": "lighter", "symbol": "ETH", "market_id": 2},
                {"exchange": "binance", "symbol": "BTC", "market_id": 1},
            ],
        }
        with patch("hydra_basis.adapters.lighter.fetch_json", new=AsyncMock(return_value=payload)):
            symbols = await list_lighter_symbols(session=object())

        self.assertEqual(symbols, {"BTC", "ETH"})


class AsyncUtilsTests(unittest.IsolatedAsyncioTestCase):
    async def test_gather_limited_preserves_result_order(self) -> None:
        async def make_result(value: int) -> int:
            return value

        results = await gather_limited([make_result(1), make_result(2), make_result(3)], limit=2)

        self.assertEqual(results, [1, 2, 3])


class BackfillUtilsTests(unittest.TestCase):
    def test_chunk_sequence_splits_into_batches(self) -> None:
        chunks = chunk_sequence([1, 2, 3, 4, 5], chunk_size=2)
        self.assertEqual(chunks, [[1, 2], [3, 4], [5]])

    def test_split_loris_batched_keys_only_batches_variational(self) -> None:
        immediate, batched = split_loris_batched_keys(
            [("hyperliquid", "BTC"), ("variational", "BTC"), ("mexc", "ETH")]
        )
        self.assertEqual(immediate, [("hyperliquid", "BTC"), ("mexc", "ETH")])
        self.assertEqual(batched, [("variational", "BTC")])

    def test_spread_refresh_keys_include_complete_cached_symbols(self) -> None:
        keys = build_spread_refresh_keys(
            {
                "aster": {"BEAT", "BTC"},
                "variational": {"BEAT"},
                "disabled": {"BTC"},
            },
            enabled_venues=["aster", "variational", "disabled"],
            supported_venues={"aster", "variational"},
        )

        self.assertEqual(
            keys,
            [("aster", "BEAT"), ("aster", "BTC"), ("variational", "BEAT")],
        )

    def test_complete_history_still_needs_top_up_when_new_interval_has_passed(self) -> None:
        now_value = 1_700_000_000_000
        points = [
            FundingPoint("lighter", "BTC", now_value - 7 * 24 * 3_600_000, 0.0001, 1.0),
            FundingPoint("lighter", "BTC", now_value - 2 * 3_600_000, 0.0002, 1.0),
        ]

        self.assertTrue(backfill_needs_top_up(points, now_ms=now_value))
        self.assertEqual(backfill_incremental_start_ms(points), points[-1].ts_ms + 1)

    def test_complete_history_does_not_need_top_up_when_within_same_interval(self) -> None:
        now_value = 1_700_000_000_000
        points = [
            FundingPoint("lighter", "BTC", now_value - 7 * 24 * 3_600_000, 0.0001, 1.0),
            FundingPoint("lighter", "BTC", now_value - 30 * 60_000, 0.0002, 1.0),
        ]

        self.assertFalse(backfill_needs_top_up(points, now_ms=now_value))

    def test_no_new_points_warning_includes_key_window_and_coverage(self) -> None:
        warning = build_no_new_points_warning(
            venue="variational",
            symbol="ZRO",
            start_ms=1782072000001,
            end_ms=1782367200000,
            coverage={
                "samples": 11,
                "oldest_ts_ms": 1781794800000,
                "newest_ts_ms": 1782072000000,
                "missing_ms": 27353101,
            },
        )

        self.assertIn("backfill no new points ('variational', 'ZRO')", warning)
        self.assertIn("start=2026-06-21T20:00:00Z", warning)
        self.assertIn("end=2026-06-25T06:00:00Z", warning)
        self.assertIn("samples=11", warning)
        self.assertIn("newest_ts_ms=1782072000000", warning)


class BackfillSpreadSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_force_refresh_retries_previous_no_orderbook_sentinel(self) -> None:
        spreads: dict[tuple[str, str], dict[str, float | int]] = {
            ("lighter", "BOT"): {"status": "no_orderbook"}
        }

        with patch(
            "hydra_basis.backfill.fetch_orderbook_snapshot",
            new=AsyncMock(return_value={"bid": 10.0, "ask": 10.1, "ts_ms": 123}),
        ) as fetch_orderbook:
            stored = await capture_backfill_spread_snapshot(
                session=object(),
                spreads=spreads,
                venue="lighter",
                symbol="BOT",
                clip_usd=1000.0,
                force_refresh=True,
            )

        self.assertTrue(stored)
        fetch_orderbook.assert_awaited_once()
        self.assertEqual(spreads[("lighter", "BOT")]["bid"], 10.0)

    async def test_missing_orderbook_does_not_fail_backfill(self) -> None:
        spreads: dict[tuple[str, str], dict[str, float | int]] = {}

        with patch(
            "hydra_basis.backfill.fetch_orderbook_snapshot",
            new=AsyncMock(side_effect=RuntimeError("missing lighter orderbook for BOT")),
        ):
            stored = await capture_backfill_spread_snapshot(
                session=object(),
                spreads=spreads,
                venue="lighter",
                symbol="BOT",
                clip_usd=1000.0,
            )

        self.assertFalse(stored)
        self.assertEqual(spreads, {("lighter", "BOT"): {"status": "no_orderbook"}})

    async def test_missing_orderbook_is_not_returned_as_alertable_error(self) -> None:
        spreads: dict[tuple[str, str], dict[str, float | int]] = {}

        with patch(
            "hydra_basis.backfill.fetch_orderbook_snapshot",
            new=AsyncMock(side_effect=RuntimeError("missing trade_xyz orderbook for XYZ:TSLA")),
        ):
            result = await capture_backfill_spread_snapshot_with_error(
                session=object(),
                spreads=spreads,
                venue="trade_xyz",
                symbol="XYZ:TSLA",
                clip_usd=1000.0,
                force_refresh=True,
            )

        self.assertFalse(result["stored"])
        self.assertIsNone(result["error"])
        self.assertEqual(result["error_type"], "no_orderbook")
        self.assertEqual(spreads[("trade_xyz", "XYZ:TSLA")], {"status": "no_orderbook"})

    async def test_aster_depth_400_marks_invalid_symbol_without_alertable_error(self) -> None:
        spreads: dict[tuple[str, str], dict[str, float | int]] = {}
        bad_request = RuntimeError("400, message='Bad Request', url='https://fapi.asterdex.com/fapi/v1/depth?symbol=TONUSDT&limit=5'")
        bad_request.status = 400

        with patch(
            "hydra_basis.backfill.fetch_orderbook_snapshot",
            new=AsyncMock(side_effect=bad_request),
        ):
            result = await capture_backfill_spread_snapshot_with_error(
                session=object(),
                spreads=spreads,
                venue="aster",
                symbol="TON",
                clip_usd=1000.0,
                force_refresh=True,
            )

        self.assertFalse(result["stored"])
        self.assertIsNone(result["error"])
        self.assertEqual(result["error_type"], "invalid_symbol")
        self.assertEqual(spreads[("aster", "TON")], {"status": "invalid_symbol"})

    async def test_cached_invalid_symbol_is_skipped_even_when_force_refreshing(self) -> None:
        spreads: dict[tuple[str, str], dict[str, float | int]] = {
            ("aster", "TON"): {"status": "invalid_symbol"}
        }

        with patch("hydra_basis.backfill.fetch_orderbook_snapshot", new=AsyncMock()) as fetch_orderbook:
            result = await capture_backfill_spread_snapshot_with_error(
                session=object(),
                spreads=spreads,
                venue="aster",
                symbol="TON",
                clip_usd=1000.0,
                force_refresh=True,
            )

        self.assertFalse(result["stored"])
        self.assertIsNone(result["error"])
        self.assertEqual(result["error_type"], "cached_invalid_symbol")
        fetch_orderbook.assert_not_awaited()

    async def test_rate_limited_spread_capture_does_not_raise_or_overwrite_existing_spread(self) -> None:
        spreads: dict[tuple[str, str], dict[str, float | int]] = {
            ("lighter", "ETH"): {"bid": 100.0, "ask": 100.1, "spread_pct": 0.001, "ts_ms": 1}
        }
        rate_limited = WSServerHandshakeError(
            request_info=None,
            history=(),
            status=429,
            message="Invalid response status",
        )

        with patch(
            "hydra_basis.backfill.fetch_orderbook_snapshot",
            new=AsyncMock(side_effect=rate_limited),
        ):
            stored = await capture_backfill_spread_snapshot(
                session=object(),
                spreads=spreads,
                venue="lighter",
                symbol="ETH",
                clip_usd=1000.0,
                force_refresh=True,
            )

        self.assertFalse(stored)
        self.assertEqual(spreads[("lighter", "ETH")]["bid"], 100.0)

    async def test_rate_limited_spread_capture_returns_alertable_error(self) -> None:
        spreads: dict[tuple[str, str], dict[str, float | int]] = {}
        rate_limited = RuntimeError("429 Too Many Requests")

        with patch(
            "hydra_basis.backfill.fetch_orderbook_snapshot",
            new=AsyncMock(side_effect=rate_limited),
        ):
            result = await capture_backfill_spread_snapshot_with_error(
                session=object(),
                spreads=spreads,
                venue="lighter",
                symbol="ETH",
                clip_usd=1000.0,
                force_refresh=True,
            )

        self.assertFalse(result["stored"])
        self.assertEqual(result["error_type"], "transient")
        self.assertIn("429", result["error"])

    def test_persist_backfill_progress_saves_both_json_stores_immediately(self) -> None:
        history_store = unittest.mock.Mock()
        spread_store = unittest.mock.Mock()
        points = {("mexc", "BTC"): []}
        spreads = {("mexc", "BTC"): {"bid": 1.0, "ask": 1.1, "spread_pct": 0.1, "ts_ms": 1}}

        persist_backfill_progress(
            history_store=history_store,
            spread_store=spread_store,
            funding_points=points,
            spreads=spreads,
        )

        history_store.save.assert_called_once_with(points)
        spread_store.save.assert_called_once_with(spreads)


class FormattingTests(unittest.TestCase):
    def test_build_signal_message_uses_chinese_labels(self) -> None:
        stats = {
            "avg_hourly": 0.0001,
            "annualized_avg": 0.876,
            "positive_ratio": 0.8,
            "last_24h_positive_ratio": 0.75,
            "samples": 100,
            "score": 0.9,
        }

        message = build_signal_message("BTC", "hyperliquid", "lighter", stats)

        self.assertIn("資金費套利訊號", message)
        self.assertIn("年化估算(未扣成本)", message)
        self.assertIn("做空", message)
        self.assertIn("做多", message)

    def test_build_spot_perp_signal_message_uses_chinese_labels(self) -> None:
        stats = {
            "avg_hourly": 0.0001,
            "annualized_avg": 0.876,
            "positive_ratio": 0.8,
            "last_24h_positive_ratio": 0.75,
            "samples": 100,
            "score": 0.9,
        }

        message = build_spot_perp_signal_message("BTC", "mexc", stats)

        self.assertIn("期現套利訊號", message)
        self.assertIn("做空永續", message)
        self.assertIn("買入現貨", message)
        self.assertIn("年化估算(未扣成本)", message)


class MonitorErrorTests(unittest.TestCase):
    def test_build_exchange_error_message_includes_phase_venue_and_symbol(self) -> None:
        message = build_exchange_error_message(
            phase="fetch",
            venue="mexc",
            symbol="BTC",
            error=RuntimeError("403 Forbidden"),
        )

        self.assertIn("fetch error", message)
        self.assertIn("mexc", message)
        self.assertIn("BTC", message)
        self.assertIn("403 Forbidden", message)

    def test_raise_exchange_error_raises_runtime_error(self) -> None:
        with self.assertRaises(RuntimeError) as ctx:
            raise_exchange_error(
                phase="discovery",
                venue="aster",
                symbol=None,
                error=ValueError("418 blocked"),
            )

        self.assertIn("discovery error", str(ctx.exception))
        self.assertIn("aster", str(ctx.exception))
        self.assertIn("418 blocked", str(ctx.exception))

    def test_should_raise_immediately_for_rate_limit_and_timeout_errors(self) -> None:
        self.assertTrue(should_raise_immediately(RuntimeError("429 Too Many Requests")))
        self.assertTrue(should_raise_immediately(RuntimeError("request timeout")))
        self.assertFalse(should_raise_immediately(RuntimeError("403 Forbidden")))


class AlertsTests(unittest.TestCase):
    def test_selects_only_highest_annualized_signal_per_symbol_above_threshold(self) -> None:
        opportunities = [
            {
                "symbol": "BTC",
                "short_venue": "a",
                "long_venue": "b",
                "stats": {"signal": True, "annualized_avg": 0.30},
            },
            {
                "symbol": "BTC",
                "short_venue": "c",
                "long_venue": "d",
                "stats": {"signal": True, "annualized_avg": 0.40},
            },
            {
                "symbol": "ETH",
                "short_venue": "a",
                "long_venue": "b",
                "stats": {"signal": True, "annualized_avg": 0.20},
            },
            {
                "symbol": "SOL",
                "short_venue": "a",
                "long_venue": "b",
                "stats": {"signal": False, "annualized_avg": 0.90},
            },
        ]

        selected = select_best_alerts_by_symbol(opportunities, min_annualized_avg=0.25)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["symbol"], "BTC")
        self.assertEqual(selected[0]["short_venue"], "c")
        self.assertEqual(selected[0]["long_venue"], "d")

    def test_skips_cross_exchange_alert_when_both_venues_have_wide_spread(self) -> None:
        opportunities = [
            {
                "symbol": "BTC",
                "short_venue": "lighter",
                "long_venue": "mexc",
                "stats": {"signal": True, "annualized_avg": 0.50},
            },
            {
                "symbol": "ETH",
                "short_venue": "aster",
                "long_venue": "hyperliquid",
                "stats": {"signal": True, "annualized_avg": 0.45},
            },
        ]

        spreads = {
            ("lighter", "BTC"): {"bid": 99.0, "ask": 101.0, "spread_pct": 0.0020, "ts_ms": 1},
            ("mexc", "BTC"): {"bid": 99.0, "ask": 101.0, "spread_pct": 0.0015, "ts_ms": 1},
            ("aster", "ETH"): {"bid": 99.0, "ask": 101.0, "spread_pct": 0.0020, "ts_ms": 1},
            ("hyperliquid", "ETH"): {"bid": 99.9, "ask": 100.0, "spread_pct": 0.0010, "ts_ms": 1},
        }

        selected = select_best_alerts_by_symbol(
            opportunities,
            min_annualized_avg=0.25,
            spreads_by_venue_symbol=spreads,
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["symbol"], "ETH")

    def test_selects_only_highest_spot_perp_signal_per_symbol_above_threshold(self) -> None:
        opportunities = [
            {"symbol": "BTC", "venue": "a", "stats": {"signal": True, "annualized_avg": 0.30}},
            {"symbol": "BTC", "venue": "b", "stats": {"signal": True, "annualized_avg": 0.40}},
            {"symbol": "ETH", "venue": "a", "stats": {"signal": True, "annualized_avg": 0.20}},
        ]

        selected = select_best_spot_perp_alerts_by_symbol(opportunities, min_annualized_avg=0.25)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["symbol"], "BTC")
        self.assertEqual(selected[0]["venue"], "b")

    def test_build_ranked_alert_digest_sorts_by_roc_desc(self) -> None:
        digest = build_ranked_alert_digest(
            cross_exchange_alerts=[
                {"symbol": "BTC", "short_venue": "a", "long_venue": "b", "stats": {"annualized_avg": 0.40, "positive_ratio": 0.8}},
            ],
            spot_perp_alerts=[
                {"symbol": "ETH", "venue": "mexc", "stats": {"annualized_avg": 0.60, "positive_ratio": 0.9}},
            ],
        )

        self.assertIn("#1", digest)
        self.assertTrue(digest.index("BTC") < digest.index("ETH"))
        self.assertIn("SPOT-PERP", digest)
        self.assertIn("跨所", digest)


class HistoryStoreTests(unittest.TestCase):
    def test_save_and_load_funding_points(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = FundingHistoryStore(Path(temp_dir) / "funding_history.json")
            payload = {
                ("hyperliquid", "BTC"): [
                    FundingPoint("hyperliquid", "BTC", 1000, 0.0001, 1),
                    FundingPoint("hyperliquid", "BTC", 2000, 0.0002, 1),
                ]
            }

            store.save(payload)
            loaded = store.load()

            self.assertIn(("hyperliquid", "BTC"), loaded)
            self.assertEqual(len(loaded[("hyperliquid", "BTC")]), 2)
            self.assertEqual(loaded[("hyperliquid", "BTC")][0].ts_ms, 1000)

    def test_save_includes_readable_datetime_and_annualized_pct_without_hourly_rate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "funding_history.json"
            store = FundingHistoryStore(path)
            payload = {
                ("hyperliquid", "BTC"): [
                    FundingPoint("hyperliquid", "BTC", 1_700_000_000_000, 0.0008, 8),
                ]
            }

            store.save(payload)
            row = json.loads(path.read_text(encoding="utf-8"))["hyperliquid::BTC"][0]

            self.assertEqual(row["ts_ms"], 1_700_000_000_000)
            self.assertEqual(row["datetime_utc"], "2023-11-14T22:13:20Z")
            self.assertEqual(row["raw_rate"], 0.0008)
            self.assertAlmostEqual(row["annualized_pct"], 87.6)
            self.assertNotIn("hourly_rate", row)

    def test_trim_points_to_14_day_lookback(self) -> None:
        now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        fourteen_days_ms = 14 * 24 * 60 * 60 * 1000
        old_point = FundingPoint("hyperliquid", "BTC", now_ms - fourteen_days_ms - 1, 0.0001, 1)
        fresh_point = FundingPoint("hyperliquid", "BTC", now_ms - 1000, 0.0002, 1)

        trimmed = trim_points_to_lookback_ms([old_point, fresh_point], lookback_ms=fourteen_days_ms, now_ms=now_ms)

        self.assertEqual(len(trimmed), 1)
        self.assertEqual(trimmed[0].ts_ms, fresh_point.ts_ms)

    def test_trim_points_to_analysis_days_uses_recent_7_days(self) -> None:
        now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        eight_days_ms = 8 * 24 * 60 * 60 * 1000
        six_days_ms = 6 * 24 * 60 * 60 * 1000
        old_point = FundingPoint("mexc", "AAVE", now_ms - eight_days_ms, 0.01, 8)
        fresh_point = FundingPoint("mexc", "AAVE", now_ms - six_days_ms, 0.02, 8)

        trimmed = trim_points_to_analysis_days([old_point, fresh_point], analysis_days=7, now_ms=now_ms)

        self.assertEqual(len(trimmed), 1)
        self.assertEqual(trimmed[0].ts_ms, fresh_point.ts_ms)

    def test_merge_points_by_interval_bucket_keeps_latest_point_in_same_bucket(self) -> None:
        base_ts = 1_700_000_000_000
        older = FundingPoint("aster", "1000PEPE", base_ts, 0.0004, 4)
        newer_same_bucket = FundingPoint("aster", "1000PEPE", base_ts + 60_000, 0.0005, 4)
        next_bucket = FundingPoint("aster", "1000PEPE", base_ts + 4 * 3_600_000, 0.0006, 4)

        merged = merge_points_by_interval_bucket([older, newer_same_bucket, next_bucket])

        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[0].ts_ms, newer_same_bucket.ts_ms)
        self.assertEqual(merged[1].ts_ms, next_bucket.ts_ms)

    def test_funding_history_is_complete_for_7_days_when_oldest_point_is_old_enough(self) -> None:
        now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        points = [
            FundingPoint("lighter", "BTC", now_ms - 7 * 24 * 60 * 60 * 1000, 0.0001, 1),
            FundingPoint("lighter", "BTC", now_ms - 1_000, 0.0002, 1),
        ]

        complete = funding_history_is_complete(points, required_days=7, now_ms=now_ms)

        self.assertTrue(complete)

    def test_funding_history_is_incomplete_for_7_days_when_oldest_point_is_too_recent(self) -> None:
        now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        points = [
            FundingPoint("lighter", "BTC", now_ms - 3 * 24 * 60 * 60 * 1000, 0.0001, 1),
            FundingPoint("lighter", "BTC", now_ms - 1_000, 0.0002, 1),
        ]

        complete = funding_history_is_complete(points, required_days=7, now_ms=now_ms)

        self.assertFalse(complete)

    def test_funding_history_is_incomplete_when_newest_point_is_stale(self) -> None:
        now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        points = [
            FundingPoint("variational", "BTC", now_ms - 7 * 24 * 60 * 60 * 1000, 0.0001, 8),
            FundingPoint("variational", "BTC", now_ms - 3 * 24 * 60 * 60 * 1000, 0.0002, 8),
        ]

        complete = funding_history_is_complete(points, required_days=7, now_ms=now_ms)

        self.assertFalse(complete)

    def test_summarize_history_coverage_reports_missing_ms(self) -> None:
        now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        points = [
            FundingPoint("lighter", "BTC", now_ms - 3 * 24 * 60 * 60 * 1000, 0.0001, 1),
            FundingPoint("lighter", "BTC", now_ms - 1_000, 0.0002, 1),
        ]

        summary = summarize_history_coverage(points, required_days=7, now_ms=now_ms)

        self.assertEqual(summary["samples"], 2)
        self.assertEqual(summary["oldest_ts_ms"], points[0].ts_ms)
        self.assertGreater(summary["missing_ms"], 0)


class HyperliquidStreamTests(unittest.TestCase):
    def test_parse_all_mids_message_extracts_symbol_prices(self) -> None:
        payload = {
            "channel": "allMids",
            "data": {
                "mids": {
                    "BTC": "100000.0",
                    "ETH": "2500.5",
                }
            },
        }

        parsed = parse_all_mids_message(payload)

        self.assertEqual(parsed["BTC"], 100000.0)
        self.assertEqual(parsed["ETH"], 2500.5)

    def test_parse_all_dexs_asset_ctxs_message_extracts_funding_and_mark(self) -> None:
        payload = {
            "channel": "allDexsAssetCtxs",
            "data": {
                "ctxs": [
                    [
                        "",
                        [
                            {"funding": "0.0001", "markPx": "100000", "midPx": "100001", "oraclePx": "99999"},
                            {"funding": "-0.0002", "markPx": "2500", "midPx": "2501", "oraclePx": "2499"},
                        ],
                    ]
                ]
            },
        }
        symbols = ["BTC", "ETH"]

        parsed = parse_all_dexs_asset_ctxs_message(payload, symbols)

        self.assertEqual(parsed["BTC"]["funding"], 0.0001)
        self.assertEqual(parsed["BTC"]["markPx"], 100000.0)
        self.assertEqual(parsed["ETH"]["funding"], -0.0002)
        self.assertEqual(parsed["ETH"]["midPx"], 2501.0)


class LighterStreamTests(unittest.TestCase):
    def test_parse_market_stats_all_message_extracts_funding_and_mark(self) -> None:
        payload = {
            "channel": "market_stats:all",
            "market_stats": {
                "1": {
                    "symbol": "BTC",
                    "market_id": 1,
                    "mark_price": "100000.0",
                    "index_price": "99950.0",
                    "funding_rate": "0.0012",
                    "current_funding_rate": "0.0013",
                },
                "2": {
                    "symbol": "ETH",
                    "market_id": 2,
                    "mark_price": "2500.0",
                    "index_price": "2490.0",
                    "funding_rate": "-0.0020",
                    "current_funding_rate": "-0.0018",
                },
            },
        }

        parsed = parse_market_stats_all_message(payload)

        self.assertAlmostEqual(parsed["BTC"]["funding"], 0.000012)
        self.assertAlmostEqual(parsed["BTC"]["current_funding"], 0.000013)
        self.assertAlmostEqual(parsed["ETH"]["funding"], -0.00002)
        self.assertEqual(parsed["ETH"]["markPx"], 2500.0)
        self.assertEqual(parsed["BTC"]["midPx"], 100000.0)


class AsterStreamTests(unittest.TestCase):
    def test_parse_mark_price_array_message_extracts_funding_and_mark(self) -> None:
        payload = {
            "stream": "!markPrice@arr@1s",
            "data": [
                {
                    "e": "markPriceUpdate",
                    "E": 1_717_000_000_000,
                    "s": "BTCUSDT",
                    "p": "100000.0",
                    "i": "99950.0",
                    "r": "0.0001",
                },
                {
                    "e": "markPriceUpdate",
                    "E": 1_717_000_001_000,
                    "s": "DOGEUSDC",
                    "p": "0.1234",
                    "i": "0.1230",
                    "r": "-0.0002",
                },
            ],
        }

        parsed = parse_mark_price_array_message(payload)

        self.assertEqual(parsed["BTC"]["funding"], 0.0001)
        self.assertEqual(parsed["BTC"]["markPx"], 100000.0)
        self.assertEqual(parsed["BTC"]["oraclePx"], 99950.0)
        self.assertEqual(parsed["DOGE"]["funding"], -0.0002)


class MexcStreamTests(unittest.TestCase):
    def test_parse_push_tickers_message_extracts_symbols(self) -> None:
        payload = {
            "channel": "push.tickers",
            "data": [
                {"symbol": "BTC_USDT", "fairPrice": 100000.0},
                {"symbol": "DOGE_USDT", "fairPrice": 0.1234},
            ],
            "ts": 1_717_000_000_000,
        }

        parsed = parse_push_tickers_message(payload)

        self.assertEqual(parsed["BTC"]["symbol"], "BTC")
        self.assertEqual(parsed["DOGE"]["raw_symbol"], "DOGE_USDT")

    def test_parse_push_ticker_message_extracts_funding_and_prices(self) -> None:
        payload = {
            "channel": "push.ticker",
            "data": {
                "symbol": "BTC_USDT",
                "fairPrice": 100000.0,
                "indexPrice": 99950.0,
                "fundingRate": 0.0001,
                "lastPrice": 100010.0,
                "timestamp": 1_717_000_000_000,
            },
            "symbol": "BTC_USDT",
            "ts": 1_717_000_000_000,
        }

        parsed = parse_push_ticker_message(payload)

        self.assertEqual(parsed["BTC"]["funding"], 0.0001)
        self.assertEqual(parsed["BTC"]["markPx"], 100000.0)
        self.assertEqual(parsed["BTC"]["oraclePx"], 99950.0)
        self.assertEqual(parsed["BTC"]["midPx"], 100010.0)


class MarketStateStoreTests(unittest.TestCase):
    def test_updates_latest_mid_prices(self) -> None:
        store = MarketStateStore()

        store.update_mids("hyperliquid", {"BTC": 100000.0, "ETH": 2500.5}, timestamp_ms=123456)

        snapshot = store.get_mid_snapshot("hyperliquid")
        self.assertEqual(snapshot["BTC"]["price"], 100000.0)
        self.assertEqual(snapshot["BTC"]["ts_ms"], 123456)
        self.assertEqual(snapshot["ETH"]["price"], 2500.5)

    def test_returns_empty_snapshot_for_unknown_venue(self) -> None:
        store = MarketStateStore()
        self.assertEqual(store.get_mid_snapshot("unknown"), {})


if __name__ == "__main__":
    unittest.main()
