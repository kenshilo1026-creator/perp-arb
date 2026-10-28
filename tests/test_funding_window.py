import unittest
from unittest.mock import patch

from hydra_basis.funding_engine import analysis
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.execution_engine.apy_service import compute_spot_perp_apy


HOUR = 3_600_000
DAY = 24 * HOUR
NOW = 100 * DAY


def point(age_hours, rate, venue="short"):
    return FundingPoint(venue, "BTC", NOW - age_hours * HOUR, rate, 1)


class FundingWindowTests(unittest.TestCase):
    def setUp(self):
        clock = patch.object(analysis, "now_ms", return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)

    def analyze_both(self, points, days=7, minimum=3):
        long_points = [FundingPoint("long", "BTC", p.ts_ms, 0.0, 1) for p in points]
        return (
            analysis.analyze_spread(points, long_points, analysis_days=days, min_observations=minimum),
            analysis.analyze_positive_funding(points, analysis_days=days, min_observations=minimum),
        )

    def test_no_last_day_data_does_not_invalidate_seven_day_signal(self):
        points = [point(hour, 0.0001) for hour in range(48, 78)]
        for stats in self.analyze_both(points, minimum=24):
            self.assertTrue(stats["signal"])
            self.assertEqual(stats["positive_ratio"], 1)
            self.assertEqual(stats["samples"], 30)
            self.assertEqual(stats["analysis_days"], 7)
            self.assertNotIn("last_24h_positive_ratio", stats)
        self.assertEqual(self.analyze_both(points, days=1), (None, None))

    def test_requested_window_changes_ratio_and_excludes_future_data(self):
        points = [point(hour, 0.0001) for hour in (25, 26, 27)]
        points += [point(hour, -0.00001) for hour in (1, 2, 3)]
        points += [point(-1, -1), point(8 * 24, -1)]
        for stats in self.analyze_both(points, days=7):
            self.assertEqual(stats["positive_ratio"], .5)
            self.assertEqual(stats["samples"], 6)
        for stats in self.analyze_both(points, days=1):
            self.assertEqual(stats["positive_ratio"], 0)
            self.assertEqual(stats["samples"], 3)

    def test_seventy_percent_is_inclusive_without_old_seventy_five_percent_gate(self):
        for positives, expected in ((7, True), (6, False)):
            points = [point(48 + i * 3, .0001 if i < positives else 0) for i in range(10)]
            for stats in self.analyze_both(points):
                self.assertEqual(stats["positive_ratio"], positives / 10)
                self.assertEqual(stats["signal"], expected)

    def test_missing_nonfinite_and_unmatched_samples_do_not_enter_denominator(self):
        points = [point(hour, rate) for hour, rate in zip(
            (30, 40, 50, 60, 70, 80, 90), (.0001, .0001, 0, -.00001, None, float("nan"), float("inf")),
        )]
        for stats in self.analyze_both(points):
            self.assertEqual(stats["samples"], 4)
            self.assertEqual(stats["positive_ratio"], .5)
        short = [point(hour, .0001) for hour in (30, 40, 50, 60)]
        long = [point(hour, 0, "long") for hour in (30, 40, 50)]
        stats = analysis.analyze_spread(short, long, min_observations=3)
        self.assertEqual(stats["samples"], 3)
        self.assertEqual(stats["positive_ratio"], 1)

    def test_empty_and_invalid_data_have_no_ratio_or_signal(self):
        self.assertIsNone(analysis.positive_ratio([]))
        self.assertIsNone(analysis.positive_ratio([None, float("nan")]))
        self.assertEqual(analysis.positive_ratio([1, None, 0, -1]), 1 / 3)
        for points in ([], [point(30, None)], [point(200, .0001)]):
            self.assertEqual(self.analyze_both(points), (None, None))
        self.assertEqual(analysis.explain_spread_skip([point(200, .0001)], [point(200, 0)]), "missing_points")

    def test_ignoring_missing_samples_does_not_bypass_minimum_observations(self):
        points = [point(30, .0001), point(40, .0001), point(50, None)]
        self.assertEqual(self.analyze_both(points, minimum=3), (None, None))

    def test_fourteen_day_window_and_exact_boundary(self):
        points = [point(hour, .0001) for hour in (8 * 24, 10 * 24, 14 * 24)]
        self.assertEqual(self.analyze_both(points, days=7), (None, None))
        for stats in self.analyze_both(points, days=14):
            self.assertEqual(stats["samples"], 3)
            self.assertTrue(stats["signal"])

    def test_spot_apy_passes_requested_days(self):
        with patch("hydra_basis.execution_engine.apy_service.trim_points_to_analysis_days", side_effect=lambda pts, **kw: pts):
            result = compute_spot_perp_apy([point(10 * 24, .0001)], days=14)
        self.assertEqual(result["stats"]["analysis_days"], 14)
        self.assertTrue(result["stats"]["signal"])
