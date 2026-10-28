import asyncio
import io
import json
import unittest
from decimal import Decimal as D

from scripts.record_spread_market import RecordingFeed, write_events
from tests.test_spread_strategy import config


class RecordingTests(unittest.IsolatedAsyncioTestCase):
    async def test_records_source_receipt_sequence_depth_and_disconnect_in_order(self):
        queue = asyncio.Queue()
        feed = RecordingFeed(config(), queue, clock=lambda: 1700000000100)
        feed.update('aster', D(99), D(100), source_ms=1700000000000,
                    bids=((D(99), D(2)),), asks=((D(100), D(3)),))
        feed.set_health('aster', False)
        await queue.put(None)
        output = io.StringIO()
        await write_events(queue, output)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([e['sequence'] for e in events], [1, 2])
        self.assertEqual(events[0]['source_ms'], 1700000000000)
        self.assertEqual(events[0]['received_ms'], 1700000000100)
        self.assertEqual(events[0]['asks'], [['100', '3']])
        self.assertGreater(events[0]['received_monotonic_ns'], 0)
        self.assertEqual((events[1]['type'], events[1]['healthy']), ('health', False))

    async def test_overflow_is_reported_not_silently_presented_as_complete(self):
        queue = asyncio.Queue(maxsize=1)
        feed = RecordingFeed(config(), queue)
        for _ in range(2):
            feed.update('aster', D(99), D(100), source_ms=1)
        self.assertEqual(feed.dropped, 1)
        self.assertEqual(feed.counts['aster'], 2)
