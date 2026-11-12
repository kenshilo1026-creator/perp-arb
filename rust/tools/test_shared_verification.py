import copy
import unittest
from verify_shared import fixtures, reference, mismatches


class SharedVerifierTests(unittest.TestCase):
    def test_reference_covers_stale_depth_and_insufficient_bbo_capacity(self):
        actual = reference(fixtures()[:14])
        self.assertTrue(actual[1]["fresh"])
        self.assertIsNotNone(actual[1]["prices"][0]["vwap"])
        self.assertFalse(actual[3]["fresh"])
        self.assertTrue(actual[4]["fresh"])
        self.assertIsNone(actual[4]["prices"][0]["vwap"])
        self.assertIsNotNone(actual[5]["prices"][0]["vwap"])
        self.assertFalse(actual[10]["fresh"])
        self.assertTrue(actual[13]["fresh"])

    def test_wrong_capacity_and_source_time_fail_exact_comparison(self):
        expected = reference(fixtures()[:14])
        actual = copy.deepcopy(expected)
        actual[4]["prices"][0]["vwap"] = "101"
        actual[2]["book"]["source_ms"] += 1
        self.assertEqual(mismatches(expected,actual),[2,4])
        with self.assertRaises(AssertionError):
            mismatches(expected,actual[:-1])
