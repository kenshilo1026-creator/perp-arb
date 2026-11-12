"""Checks that the parity gate cannot pass missing liquidity or a wrong tick."""
from decimal import Decimal as D
import unittest

from verify_parity import compare, fixtures, prepare


class VerificationTests(unittest.TestCase):
    def test_wrong_tick_and_wrong_liquidity_do_not_use_division_tolerance(self):
        cases = [{"op": "round"}, {"op": "marginal"}, {"op": "executable"}, {"op": "fresh"}]
        wanted = [D(1), D(1), None, False]
        actual = [{"value": "1.000000000000000000000000001"},
                  {"value": "1.000000000000000000000000001"}, {"value": "100"}, {"value": True}]
        bad, rounded = compare(cases, wanted, actual)
        self.assertEqual(len(bad), 4)
        self.assertEqual(rounded, [])

    def test_small_division_difference_is_recorded_but_large_one_fails(self):
        bad, rounded = compare([{"op": "vwap"}, {"op": "spread"}], [D(1), D(1)],
                               [{"value": "1.000000000000000000000000001"}, {"value": "1.0001"}])
        self.assertEqual([v["index"] for v in bad], [1])
        self.assertEqual([v["index"] for v in rounded], [0])

    def test_error_nonfinite_boolean_and_missing_results_do_not_pass_as_prices(self):
        for result in [{"error": "Arithmetic"}, {"value": "NaN"}, {"value": True}, {"value": None}]:
            bad, _ = compare([{"op": "vwap"}], [D(1)], [result])
            self.assertEqual(len(bad), 1)
        with self.assertRaises(AssertionError):
            compare([{"op": "vwap"}], [D(1)], [])

    def test_fixtures_are_deterministic_and_cover_both_sides_and_all_operations(self):
        cases = fixtures()
        self.assertEqual(cases, fixtures())
        self.assertEqual({c["op"] for c in cases}, {"spread", "adverse", "round", "vwap", "executable", "marginal", "fresh"})
        self.assertEqual({c["side"] for c in cases if "side" in c}, {"BUY", "SELL"})
        self.assertEqual(len([prepare(c)() for c in cases]), 571)


if __name__ == "__main__":
    unittest.main()
