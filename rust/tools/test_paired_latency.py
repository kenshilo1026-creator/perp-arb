import unittest
from summarize_paired_latency import summarize


class LatencyTests(unittest.TestCase):
    def test_monotonic_matching_missing_data_and_ack_is_not_fill(self):
        def row(stage, identity, t, **data): return dict(stage=stage,id=identity,at_us=t,data=data)
        result=summarize([
            {'schema':1,'dropped':0},
            row('maker_ws_candidate','trade',1000,parsed_us=1100,captured_wall_ms=999999999),
            row('maker_ws_commit_done','trade',2000),
            row('hedge_sources','h',3000,trade_ids=['trade']),
            row('hedge_submit_call','h',4000),
            row('hedge_ack','h',44000),
            row('hedge_coordinator_observation','h',45000,filled='0',requires_reconciliation=False),
            row('hedge_coordinator_observation','h',46000,filled='1',requires_reconciliation=True),
            row('hedge_rest_confirmation','h',70000,filled='1',terminal=True),
        ])['metrics']
        self.assertEqual(result['ws_trigger_to_submit_call_ms']['p99'],3)
        self.assertEqual(result['hedge_submit_call_to_ack_ms']['p99'],40)
        self.assertEqual(result['hedge_submit_call_to_rest_confirmed_fill_ms']['p99'],66)
        self.assertNotIn('hedge_submit_call_to_coordinator_observed_fill_ms',result)
        self.assertNotIn('ws_receive_to_rest_verified_ms',result)

    def test_drops_are_reported_and_empty_capture_is_not_zero_latency(self):
        r=summarize([{'schema':1,'dropped':3}])
        self.assertEqual(r['metrics'],{})
        self.assertEqual(r['dropped_records'],3)
