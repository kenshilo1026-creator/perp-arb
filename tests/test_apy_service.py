import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from hydra_basis.funding_engine.analysis import now_ms
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.history_store import FundingHistoryStore
from hydra_basis.execution_engine.apy_service import (
    build_spread_series,
    compute_pair_apy,
    compute_spot_perp_apy,
    get_default_venues,
    list_symbols,
    list_venues,
    load_points,
)


def _pt(venue, ts_ms, raw_rate, interval_hours=8.0, symbol="BTC"):
    return FundingPoint(venue=venue, symbol=symbol, ts_ms=ts_ms, raw_rate=raw_rate, interval_hours=interval_hours)


HOUR_MS = 3_600_000
EIGHT_H_MS = 8 * HOUR_MS


class BuildSpreadSeriesTests(unittest.TestCase):
    def test_series_is_short_minus_long_hourly_sorted_by_ts(self) -> None:
        short = [_pt("aster", 2_000, 0.0016), _pt("aster", 1_000, 0.0008)]
        long = [_pt("hyperliquid", 2_000, 0.0008), _pt("hyperliquid", 1_000, 0.0008)]
        series = build_spread_series(short, long)
        self.assertEqual([row["ts_ms"] for row in series], [1_000, 2_000])
        self.assertAlmostEqual(series[0]["spread_hourly"], 0.0)
        self.assertAlmostEqual(series[1]["spread_hourly"], 0.0001)

    def test_empty_when_no_overlap(self) -> None:
        self.assertEqual(build_spread_series([], []), [])


class ComputePairApyTests(unittest.TestCase):
    def test_returns_stats_and_series_for_positive_spread(self) -> None:
        base = now_ms() - 23 * EIGHT_H_MS
        short = [_pt("aster", base + i * EIGHT_H_MS, 0.0016) for i in range(24)]
        long = [_pt("hyperliquid", base + i * EIGHT_H_MS, 0.0008) for i in range(24)]
        result = compute_pair_apy(short, long, days=7)
        self.assertEqual(result["days"], 7)
        self.assertIsNotNone(result["stats"])
        self.assertGreater(result["stats"]["annualized_avg"], 0)
        self.assertGreater(len(result["spread_series"]), 0)

    def test_stats_none_when_insufficient_data(self) -> None:
        result = compute_pair_apy([], [], days=7)
        self.assertIsNone(result["stats"])
        self.assertEqual(result["spread_series"], [])


class ComputeSpotPerpApyTests(unittest.TestCase):
    def test_returns_stats_and_rate_series(self) -> None:
        base = now_ms() - 23 * EIGHT_H_MS
        points = [_pt("aster", base + i * EIGHT_H_MS, 0.0008) for i in range(24)]
        result = compute_spot_perp_apy(points, days=7)
        self.assertEqual(result["days"], 7)
        self.assertIsNotNone(result["stats"])
        self.assertGreater(len(result["rate_series"]), 0)
        self.assertAlmostEqual(result["rate_series"][0]["hourly"], 0.0001)

    def test_stats_none_when_empty(self) -> None:
        result = compute_spot_perp_apy([], days=7)
        self.assertIsNone(result["stats"])
        self.assertEqual(result["rate_series"], [])


class FileHelpersTests(unittest.TestCase):
    def test_list_venues_returns_enabled_only(self) -> None:
        venues = list_venues()
        self.assertIn("aster", venues)
        self.assertNotIn("mexc", venues)  # mexc is enabled=False in VENUE_CONFIG
        self.assertEqual(venues, sorted(venues))

    def test_load_points_and_list_symbols_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "funding_history.json"
            store = FundingHistoryStore(path)
            store.save({
                ("aster", "BTC"): [_pt("aster", 1_000, 0.0008)],
                ("hyperliquid", "ETH"): [_pt("hyperliquid", 1_000, 0.0008, symbol="ETH")],
            })
            points = load_points(history_path=path)
            self.assertIn(("aster", "BTC"), points)
            self.assertEqual(list_symbols(history_path=path), ["BTC", "ETH"])


def _write_signals(path, rows):
    payload = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "cross_exchange_signals": rows,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


class GetDefaultVenuesTests(unittest.TestCase):
    def test_perp_perp_returns_short_long_and_annualized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor_signals.json"
            _write_signals(path, [
                {"symbol": "BTC", "short_venue": "aster", "long_venue": "hyperliquid",
                 "annualized_avg": 0.42, "score": 1.8, "signal": True},
            ])
            result = get_default_venues("BTC", "perp_perp", signals_path=path)
            self.assertEqual(result["short_venue"], "aster")
            self.assertEqual(result["long_venue"], "hyperliquid")
            self.assertAlmostEqual(result["annualized"], 0.42)

    def test_spot_perp_uses_short_venue_as_perp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor_signals.json"
            _write_signals(path, [
                {"symbol": "BTC", "short_venue": "aster", "long_venue": "hyperliquid",
                 "annualized_avg": 0.42, "score": 1.8, "signal": True},
            ])
            result = get_default_venues("BTC", "spot_perp", signals_path=path)
            self.assertEqual(result["perp_venue"], "aster")

    def test_missing_symbol_returns_error_dict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor_signals.json"
            _write_signals(path, [])
            result = get_default_venues("BTC", "perp_perp", signals_path=path)
            self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
