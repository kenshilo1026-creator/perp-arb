import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from hydra_basis.execution_engine.execution_trace import (
    mark, observation, timed_stage, traced_execution, record_execution,
)
from hydra_basis.execution_engine.order_stream import OrderEventStream


class TraceTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_traces_are_isolated_and_preserve_observation_time(self):
        @timed_stage("query")
        async def query(symbol):
            await asyncio.sleep(0)
            return {"source": "user_stream", "observed_monotonic_ns": 123,
                    "raw": {"tradeTime": 456, "eventTime": 457, "_received_unix_ms": 458,
                            "signature": "secret-not-for-log"}}

        @traced_execution
        async def run(*, symbol):
            mark(symbol)
            await query(symbol)
            return {"ok": True}

        a, b = await asyncio.gather(run(symbol="A"), run(symbol="B"))
        self.assertNotEqual(a["execution_trace"]["trace_id"], b["execution_trace"]["trace_id"])
        for result, own, other in ((a, "A", "B"), (b, "B", "A")):
            trace = result["execution_trace"]
            stages = [e["stage"] for e in trace["events"]]
            self.assertIn(own, stages)
            self.assertNotIn(other, stages)
            event = next(e for e in trace["events"] if e["stage"] == "query.observation")
            self.assertEqual(event["observed_monotonic_ns"], 123)
            self.assertEqual(event["tradeTime"], 456)
            self.assertEqual(event["_received_unix_ms"], 458)
            self.assertNotIn("secret-not-for-log", json.dumps(trace))

    async def test_failure_is_saved_after_cleanup_without_exception_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.jsonl"
            @timed_stage("cancel")
            async def cancel():
                raise RuntimeError("signature=secret-not-for-log")

            @record_execution
            @traced_execution
            async def run(**kwargs):
                try:
                    await cancel()
                finally:
                    self.assertFalse(path.exists())  # no disk I/O in dispatch/cleanup
                    mark("cleanup.complete")

            with patch("hydra_basis.execution_engine.execution_trace.TRACE_PATH", path):
                with self.assertRaisesRegex(RuntimeError, "signature"):
                    await run(symbol="SYN")
            trace = json.loads(path.read_text())
            self.assertEqual(trace["status"], "RuntimeError")
            self.assertEqual(trace["events"][-1]["stage"], "cleanup.complete")
            self.assertNotIn("secret-not-for-log", path.read_text())

    async def test_write_failure_never_retries_or_changes_success(self):
        calls = []
        @record_execution
        @traced_execution
        async def run():
            calls.append(1)
            return {"ok": True}
        with patch.object(Path, "open", side_effect=OSError("disk full")):
            self.assertTrue((await run())["ok"])
        self.assertEqual(calls, [1])

    async def test_unknown_observation_stays_null_and_long_traces_are_bounded(self):
        @traced_execution
        async def run():
            for i in range(5000):
                mark("quote", n=i)
            observation("reconciled", {"ok": True})
            return {"ok": True}
        trace = (await run())["execution_trace"]
        self.assertLessEqual(len(trace["events"]), 4096)
        self.assertGreater(trace["dropped_events"], 0)
        self.assertIsNone(trace["events"][-1]["observed_monotonic_ns"])

    async def test_stream_cached_receipt_time_does_not_change_when_consumed(self):
        stream = OrderEventStream(venue="aster", connect=None, renew=None, decode=None)
        stream.publish({"symbol": "SYNUSDT", "orderId": "1", "status": "FILLED",
                        "executedQty": "10", "origQty": "10", "eventTime": 999},
                       received_ns=123, received_unix_ms=456)
        for _ in range(2):
            result = stream.cached_terminal(symbol="SYNUSDT", order_id="1")
            self.assertEqual(result["observed_monotonic_ns"], 123)
            self.assertEqual(result["raw"]["_received_unix_ms"], 456)
