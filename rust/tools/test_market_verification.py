from decimal import Decimal as D
import unittest
from verify_market import canonical, fixtures, reference


class MarketVerificationTests(unittest.TestCase):
    def test_real_python_parsers_process_both_venues_and_replacement_depth(self):
        records = fixtures()
        expected = reference(records)
        self.assertEqual(len(expected), 120)
        self.assertTrue(all(row["update"] == "accepted" and row["fresh"] for row in expected))
        self.assertEqual(len(expected[0]["book"]["bids"]), 20)
        self.assertEqual(len(expected[2]["book"]["bids"]), 5)

    def test_depth_comparison_is_exact_and_does_not_round_small_coins(self):
        row = reference(fixtures()[:1])[0]
        first = canonical(row)
        row["book"]["bids"][0][0] = str(D(row["book"]["bids"][0][0]) + D("0.000000000001"))
        self.assertNotEqual(first, canonical(row))

    def test_disconnect_resets_previous_sequence_and_duplicate_does_not_refresh_age(self):
        row = fixtures()[1]  # Hyperliquid source time 10000.
        second = dict(row, received_ms=12000)
        result = reference([row, second])
        self.assertTrue(result[0]["fresh"])
        self.assertFalse(result[1]["fresh"])
        aster = fixtures()[0]
        result = reference([aster, aster, dict(type="health", venue="aster", symbol="ETHUSDT", healthy=False), aster])
        self.assertEqual([item["update"] for item in result], ["accepted", "out_of_order", "accepted"])


if __name__ == "__main__":
    unittest.main()
