import copy
import unittest
from verify_private import compare, fixtures, reference


class PrivateVerifierTests(unittest.TestCase):
    def test_accepts_reference_and_rejects_truncated_output(self):
        rows = reference(fixtures())
        compare(rows, rows)
        with self.assertRaises(AssertionError):
            compare(rows, rows[:-1])

    def test_rejects_decimal_drift_nonfinite_and_missing_fields(self):
        rows = reference(fixtures()[:2])
        for value in ["2.000000000000000000000000001", "NaN", "Infinity", 2.0]:
            altered = copy.deepcopy(rows)
            altered[1]["quantity"] = value
            with self.assertRaises(AssertionError):
                compare(rows, altered)
        altered = copy.deepcopy(rows)
        del altered[1]["delta"]
        with self.assertRaises(AssertionError):
            compare(rows, altered)
