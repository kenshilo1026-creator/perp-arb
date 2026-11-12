import unittest
from verify_hyperliquid_private import compare

class CompareTests(unittest.TestCase):
    def test_rejects_missing_rows_and_wrong_quantity_or_terminal(self):
        expected = [dict(order_id='9007199254740993',quantity='2',terminal=False)]
        for actual in [[],[dict(order_id='9007199254740993',quantity='3',terminal=False)],
                       [dict(order_id='9007199254740993',quantity='2',terminal=True)],
                       [dict(order_id='9007199254740993',quantity='NaN',terminal=False)]]:
            with self.assertRaises(AssertionError): compare(expected,actual)

    def test_decimal_equivalent_and_unknown_are_distinct(self):
        compare([dict(order_id='1',quantity='2.00',terminal=False)],
                [dict(order_id='1',quantity='2',terminal=False)])
        with self.assertRaises(AssertionError): compare([{'unknown':True}],[dict(quantity='0')])
