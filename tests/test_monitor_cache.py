import asyncio
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

from web.monitor import Channel, History, MarketSubscription, MonitorHub, Selection, spreads
from web.monitor_cache import MINUTE, MonitorRateLimited, MonitorSession, ReferenceCache, retry_seconds
from decimal import Decimal as D


class ReferenceCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'reference.sqlite3'
        self.now = 1_800_000_000_000
        self.end = self.now // MINUTE * MINUTE
        self.start = self.end - 24 * 60 * MINUTE
        self.cache = ReferenceCache(self.path, clock=lambda: self.now)
        async def fetch(venue, symbol, start, end):
            return {stamp: 100.0 for stamp in range(start, end + 1, MINUTE)}
        self.fetch = AsyncMock(side_effect=fetch)

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def read(self, start=None, end=None):
        return await self.cache.get('aster', 'ETH', self.start if start is None else start,
                                    self.end if end is None else end, self.fetch)

    async def test_concurrent_repeated_and_narrower_views_download_once(self):
        rows = await asyncio.gather(*(self.read() for _ in range(6)))
        self.assertTrue(all(len(r) == 1440 for r in rows))
        self.assertEqual(len(await self.read(self.end - 60 * MINUTE)), 60)
        self.fetch.assert_awaited_once()

    async def test_reopen_only_fetches_new_closed_minute(self):
        await self.read()
        self.now += MINUTE
        self.cache = ReferenceCache(self.path, clock=lambda: self.now)
        rows = await self.read(self.start + MINUTE, self.end + MINUTE)
        self.assertEqual(len(rows), 1440)
        self.assertEqual(self.fetch.await_args.args[2:], (self.end, self.end + MINUTE - 1))
        self.assertEqual(self.fetch.await_count, 2)

    async def test_extended_range_only_fetches_missing_prefix(self):
        await self.read(self.end - 60 * MINUTE)
        await self.read()
        self.assertEqual(self.fetch.await_args.args[2:], (self.start, self.end - 60 * MINUTE - 1))

    async def test_holes_not_zero_filled_and_retry_only_missing_minutes(self):
        hole = self.start + MINUTE
        original = self.fetch.side_effect
        async def incomplete(*args):
            rows = await original(*args)
            rows.pop(hole, None)
            rows[self.end] = 999  # The still-open candle must never be cached.
            rows[self.start] = float('nan')
            return rows
        self.fetch.side_effect = incomplete
        rows = await self.read()
        self.assertNotIn(hole, rows)
        self.assertNotIn(self.start, rows)
        self.assertNotIn(self.end, rows)
        await self.read()
        self.assertEqual(self.fetch.await_count, 1)
        self.now += 5 * MINUTE
        self.fetch.side_effect = original
        rows = await self.read()
        self.assertEqual(len(rows), 1440)
        self.assertEqual(self.fetch.await_args.args[2:], (self.start, hole + MINUTE - 1))

    async def test_failure_keeps_old_bars_and_does_not_mark_missing_as_complete(self):
        await self.read(self.end - 60 * MINUTE)
        good = self.fetch.side_effect
        self.fetch.side_effect = OSError('offline')
        self.assertEqual(len(await self.read()), 60)
        await self.read()
        self.assertEqual(self.fetch.await_count, 2)
        self.now += MINUTE
        self.fetch.side_effect = good
        self.assertEqual(len(await self.read()), 1440)
        self.assertEqual(self.fetch.await_count, 3)

    async def test_reverse_direction_and_different_size_share_cached_prices(self):
        hub = MonitorHub(History(Path(self.tmp.name) / 'observations.sqlite3'))
        hub.session = object()  # All upstream requests are replaced below.
        hub.references.clock = lambda: self.now
        async def fetch(session, *args):
            prices = await self.fetch(*args)
            return {t: p + (2 if args[0] == 'hyperliquid' else 0) for t, p in prices.items()}
        with patch('web.monitor.now_ms', return_value=self.now), \
             patch('web.monitor.fetch_minute_closes', side_effect=fetch):
            a, b = await asyncio.gather(hub.reference(Selection.parse(), 24),
                hub.reference(Selection.parse(short='aster', long='hyperliquid', quantity='2'), 24))
            self.assertEqual(len(a['series']), 1440)
            self.assertEqual(len(b['series']), 1440)
            self.assertAlmostEqual(a['series'][0]['reference'], 2)
            self.assertAlmostEqual(b['series'][0]['reference'], -2 / 102 * 100)
            self.assertEqual(self.fetch.await_count, 2)  # One request per venue, not per pair.


class Response:
    def __init__(self, status=200, payload=None, headers=None):
        self.status, self.payload, self.headers = status, payload, headers or {}
        self.request_info, self.history = None, ()

    async def json(self, **kwargs):
        return self.payload


class RawSession:
    def __init__(self, responses):
        self.responses, self.calls = iter(responses), []

    @asynccontextmanager
    async def request(self, method, url, **kwargs):
        self.calls.append((method, url))
        yield next(self.responses)


class BudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_queued_ws_does_not_start_connection_or_leak_slot(self):
        waiting = asyncio.Event()
        async def sleep(delay):
            waiting.set()
            await asyncio.Event().wait()
        class Raw:
            def __init__(self):
                self.opened = self.closed = 0

            @asynccontextmanager
            async def ws_connect(self, url, **kwargs):
                self.opened += 1
                try:
                    yield object()
                finally:
                    self.closed += 1
        raw = Raw()
        session = MonitorSession(raw, clock=lambda: 1000, sleep=sleep)
        async def connect():
            async with session.ws_connect('wss://venue.test/ws'):
                pass
        await connect()
        task = asyncio.create_task(connect())
        await asyncio.wait_for(waiting.wait(), 1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual((raw.opened, raw.closed), (1, 1))
        session.ws_next.clear()
        await connect()
        self.assertEqual((raw.opened, raw.closed), (2, 2))

    async def test_variational_json_retry_blocks_other_endpoints_on_same_host(self):
        now = [1000.0]
        raw = RawSession([Response(429, {'retry_after_seconds': 95}), Response(200)])
        session = MonitorSession(raw, clock=lambda: now[0])
        with self.assertRaises(MonitorRateLimited) as caught:
            async with session.request('GET', 'https://venue.test/stats'):
                self.fail('must not return a limited response')
        self.assertEqual(caught.exception.retry_after_seconds, 95)
        self.assertIn('429', str(caught.exception))
        now[0] += 94
        with self.assertRaises(MonitorRateLimited):
            async with session.request('GET', 'https://venue.test/candles'):
                pass
        self.assertEqual(len(raw.calls), 1)
        now[0] += 2
        async with session.request('GET', 'https://venue.test/candles') as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(len(raw.calls), 2)

    async def test_same_host_requests_paced_but_other_host_independent(self):
        now, waits = [1000.0], []
        async def sleep(delay):
            waits.append(delay)
            now[0] += delay
        session = MonitorSession(RawSession([Response() for _ in range(3)]), clock=lambda: now[0], sleep=sleep)
        for url in ['https://a.test/one', 'https://a.test/two', 'https://b.test/one']:
            async with session.request('GET', url):
                pass
        self.assertEqual(waits, [1])

    def test_retry_after_header_date_and_json_choose_longest(self):
        self.assertEqual(retry_seconds({'Retry-After':'40'}, {'retry_after_seconds':90}), 90)
        self.assertEqual(retry_seconds({'Retry-After':'Thu, 01 Jan 1970 00:02:00 GMT'}, wall_time=100), 20)
        self.assertEqual(retry_seconds({'Retry-After':'nan'}, {'retry_after_seconds':'inf'}), 0)

    def test_hyperliquid_candles_reserve_response_weight(self):
        spacing = MonitorSession.request_spacing('api.hyperliquid.xyz', {'json': {
            'type':'candleSnapshot', 'req':{'interval':'1m','startTime':0,'endTime':1440 * MINUTE}}})
        self.assertEqual(spacing, 4.4)


class SharedMarketTests(unittest.IsolatedAsyncioTestCase):
    async def test_last_viewer_leaving_closes_idle_market(self):
        hub = MonitorHub()
        hub.market_idle_seconds = 0
        stopped = asyncio.Event()
        async def idle(venue, *args):
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        with patch('web.monitor.MonitorFeed._supervise', side_effect=idle), \
             patch.object(MarketSubscription, 'metadata', new=AsyncMock()):
            selection = Selection.parse()
            source = hub.acquire_market(selection, 'aster')
            await asyncio.sleep(0)
            hub.release_market(selection, 'aster', source)
            await asyncio.wait_for(stopped.wait(), 1)
            await source.idle_task
            self.assertFalse(hub.markets)
            self.assertTrue(all(task.done() for task in source.tasks))

    async def test_pair_size_changes_share_stream_but_preserve_depth_math_and_health(self):
        hub = MonitorHub()
        first = Channel(Selection.parse(quantity='1'), hub)
        second = Channel(Selection.parse(short='aster', long='hyperliquid', quantity='3'), hub)
        async def idle(*args):
            await asyncio.Event().wait()
        with patch('web.monitor.MonitorFeed._supervise', side_effect=idle) as streams, \
             patch.object(MarketSubscription, 'metadata', side_effect=idle), \
             patch.object(Channel, 'record', side_effect=idle):
            await first.start()
            await second.start()
            await asyncio.sleep(0)
            self.assertEqual(streams.call_count, 2)
            self.assertEqual(len(hub.markets), 2)
            for venue, source in first.feed.sources.items():
                self.assertIs(source, second.feed.sources[venue])
                source.feed.update(venue, 100, 101, source_ms=source.feed.clock(),
                    bids=((D(100),D(1)),(D(99),D(2))), asks=((D(101),D(1)),(D(102),D(2))))
            self.assertNotEqual(spreads(first.selection, first.feed, {})['taker']['entry'],
                                spreads(second.selection, second.feed, {})['taker']['entry'])
            source = first.feed.sources['aster']
            source.feed.set_health('aster', False)
            self.assertIsNone(first.feed.fresh('aster'))
            self.assertIsNone(second.feed.fresh('aster'))
            await first.close()
            self.assertEqual(source.refs, 1)
            await second.close()
            self.assertEqual(source.refs, 0)
            # A quick direction switch reuses the connection during the grace period.
            await first.start()
            self.assertIs(first.feed.sources['aster'], source)
            self.assertEqual(streams.call_count, 2)
            await first.close()
            await hub.close()
            self.assertTrue(all(t.done() for t in source.tasks))
            self.assertFalse(hub.markets)

    def test_variational_quote_size_is_not_shared(self):
        a = Selection.parse(short='variational', quantity='1')
        b = Selection.parse(short='variational', quantity='3')
        self.assertNotEqual(MonitorHub.market_key(a, 'variational'), MonitorHub.market_key(b, 'variational'))
        self.assertEqual(MonitorHub.market_key(a, 'aster'), MonitorHub.market_key(b, 'aster'))
