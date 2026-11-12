import asyncio
import copy
import unittest
from verify_instruments import fixtures,reference_case,synthetic_catalogs,mismatches


class InstrumentVerifierTests(unittest.TestCase):
    def test_native_selection_reaches_production_parser_without_base_suffix_fallback(self):
        catalogs=synthetic_catalogs()
        row=catalogs["aster"]["symbols"][0]
        row.update(symbol="ETHUSD1",quoteAsset="USD1",marginAsset="USD1")
        case=dict(op="resolve",catalog="aster",venue="aster",symbol="ETHUSD1",native_request=True)
        result=asyncio.run(reference_case(case,catalogs))["value"]
        self.assertEqual(result["native"],"ETHUSD1")
        self.assertEqual(result["settlement"],"USD1")
        self.assertEqual(result["limit"]["lot"],"0.001")

    def test_rejects_one_tick_error_and_false_rejection(self):
        expected=[{"value":{"down":"1.23","up":"1.24"}}]
        actual=copy.deepcopy(expected)
        actual[0]["value"]["down"]="1.24"
        self.assertEqual(mismatches(expected,actual),[0])
        self.assertEqual(mismatches(expected,[{"error":"unexpected"}]),[0])
        with self.assertRaises(AssertionError):
            mismatches(expected,[])

    def test_reference_preserves_hl_case_index_and_blocks_delisted(self):
        catalogs=synthetic_catalogs()
        case=dict(op="resolve",catalog="hyperliquid",venue="hyperliquid",symbol="kPEPE")
        result=asyncio.run(reference_case(case,catalogs))["value"]
        self.assertEqual(result["native"],"kPEPE")
        self.assertEqual(result["asset_id"],3)
        self.assertIsNone(result["base_per_native"])
        case["symbol"]="OLD"
        self.assertEqual(asyncio.run(reference_case(case,catalogs)),{"rejected":True})
        self.assertGreater(len(fixtures(catalogs)),900)
