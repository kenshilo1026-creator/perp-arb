import unittest
from verify_execution import compare, fixtures, reference


class ExecutionVerifierTests(unittest.TestCase):
    def test_valid_reference_and_reject_missing_or_wrong_retry(self):
        cases = fixtures()[:12]
        rows = [{k: str(v) for k, v in reference(c).items()} for c in cases]
        compare(cases, rows)
        with self.assertRaises(AssertionError):
            compare(cases, rows[:-1])
        rows[3]['retry'] = '99'
        with self.assertRaises(AssertionError):
            compare(cases, rows)

    def test_unknown_gap_never_authorizes_residual(self):
        for c in fixtures():
            if c['gap']:
                self.assertEqual(reference(c)['retry'], 0)
