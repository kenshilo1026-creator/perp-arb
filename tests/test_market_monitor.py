import asyncio
import json
import tempfile
import time
import unittest
from contextlib import asynccontextmanager
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from hydra_basis.spread_strategy.feeds import MarketFeed
from hydra_basis.spread_strategy.telemetry import heartbeat
from tests.test_spread_strategy import Harness
from web.monitor import (Selection, History, MonitorHub, Channel, book_payload, spreads,
                         rate_payload, strategy_states, current_funding, variational_perps)


class MonitorMathTests(unittest.TestCase):
    def setUp(self):
        self.selection = Selection.parse('ETH', 'hyperliquid', 'aster', '2')
        self.now = 1_800_000_000_000
        self.feed = MarketFeed(self.selection.config(), clock=lambda: self.now)
        self.feed.update('hyperliquid', 102, 103, source_ms=self.now,
                         bids=((D(102), D(1)), (D(101), D(2))), asks=((D(103), D(3)),))
        self.feed.update('aster', 99, 100, source_ms=self.now,
                         bids=((D(99), D(3)),), asks=((D(100), D(1)), (D(101), D(2))))

    def test_direction_uses_depth_vwap_and_correct_denominator(self):
        values = spreads(self.selection, self.feed, {})
        self.assertAlmostEqual(values['taker']['entry'], (101.5-100.5)/100.5*100)
        self.assertAlmostEqual(values['taker']['exit'], (103-99)/99*100)
        self.assertAlmostEqual(values['maker_long']['entry'], (101.5-99)/99*100)
        self.assertAlmostEqual(values['maker_short']['exit'], (102-99)/99*100)

    def test_thin_depth_never_claims_full_size_execution(self):
        selection = Selection.parse('ETH', 'hyperliquid', 'aster', '4')
        values = spreads(selection, self.feed, {})
        self.assertIsNone(values['taker']['entry'])
        self.assertIn('深度不足', values['reason'])

    def test_stale_or_disconnected_data_does_not_generate_spread(self):
        self.now += 1001
        self.assertIsNone(spreads(self.selection, self.feed, {})['taker']['entry'])
        self.assertEqual(book_payload(self.feed, 'aster')['status'], 'stale')
        self.now -= 1001
        self.feed.set_health('aster', False)
        self.assertIsNone(spreads(self.selection, self.feed, {})['maker_long']['entry'])

    def test_missing_clob_sizes_are_not_treated_as_unlimited_depth(self):
        self.feed.update('aster', 99, 100, source_ms=self.now)
        self.assertIsNone(spreads(self.selection, self.feed, {})['taker']['entry'])

    def test_variational_quote_is_explicitly_indicative(self):
        selection = Selection.parse('ETH', 'variational', 'aster', '2')
        feed = MarketFeed(selection.config(), clock=lambda: self.now)
        feed.update('variational', 102, 103, source_ms=self.now)
        feed.update('aster', 99, 100, source_ms=self.now, bids=((D(99),D(3)),), asks=((D(100),D(3)),))
        self.assertEqual(book_payload(feed, 'variational')['status'], 'indicative')
        self.assertEqual(book_payload(feed, 'variational')['bids'], [])
        self.assertEqual(spreads(selection, feed, {})['taker']['entry'], 2)

    def test_rates_normalize_intervals_and_preserve_zero(self):
        self.assertEqual(rate_payload(.008, 8, 1)['hourly_pct'], .1)
        self.assertEqual(rate_payload(0, 1, 1)['hourly_pct'], 0)
        for rate, interval in [(float('nan'), 1), (1, 0), (1, -1)]:
            with self.assertRaises(ValueError):
                rate_payload(rate, interval, 1)

    def test_selection_limits_and_semantically_identical_keys(self):
        self.assertEqual(Selection.parse(quantity='.010').key, Selection.parse(quantity='0.01').key)
        for kwargs in [{'short':'mexc'}, {'long':'hyperliquid','short':'hyperliquid'},
                       {'symbol':'../x'}, {'quantity':'NaN'}, {'quantity':'Infinity'}, {'quantity':'0'}]:
            with self.assertRaises(ValueError):
                Selection.parse(**kwargs)


class MonitorHistoryTests(unittest.TestCase):
    def test_recorded_history_reopens_and_never_fills_missing_periods(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'history.sqlite3'
            history = History(path)
            for ts, value in [(10000, 1), (11000, 2), (30000, None)]:
                history.append('pair', {'ts_ms':ts, 'value':value})
            rows = History(path).read('pair', 0, 5000)
            self.assertEqual([(r['ts_ms'],r['value']) for r in rows], [(10000,2),(30000,None)])
            self.assertEqual(history.read('other', 0, 5000), [])

    def test_old_history_pruned(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = History(Path(tmp)/'history.sqlite3')
            history.append('pair', {'ts_ms':10000})
            history.append('pair', {'ts_ms':8*86400000})
            self.assertEqual(len(history.read('pair',0,5000)),1)

    def test_strategy_payload_excludes_remote_and_requires_heartbeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            selection = Selection.parse()
            path = Path(tmp)/'state.json'
            raw = {'identity': selection.config().identity(), 'strategy_id':'test', 'status':'RUNNING',
                   'short':{'quantity':'-1'},'long':{'quantity':'1'}, 'orders':[
                       {'id':'x','state':'OPEN','remote':{'private_key':'DO NOT RETURN'},'error':'private log'}]}
            path.write_text(json.dumps(raw))
            rows = strategy_states(selection, Path(tmp))
            self.assertFalse(rows[0]['running'])
            self.assertNotIn('private', json.dumps(rows))
            path.with_suffix('.heartbeat.json').write_text(json.dumps(
                {'strategy_id':'test','running':True,'ts_ms':int(time.time()*1000)}))
            self.assertTrue(strategy_states(selection, Path(tmp))[0]['running'])
            path.with_suffix('.heartbeat.json').write_text(json.dumps(
                {'strategy_id':'test','running':True,'ts_ms':0}))
            self.assertFalse(strategy_states(selection, Path(tmp))[0]['running'])


class MonitorLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_variational_rate_limit_backs_off_across_callers(self):
        import aiohttp
        hub = MonitorHub()
        clock = [1000.0]
        limited = aiohttp.ClientResponseError(None, (), status=429,
                                            headers={'Retry-After': '90'})
        fetch = AsyncMock(side_effect=[limited, {'listings': []}])
        with patch('web.monitor.time.monotonic', side_effect=lambda: clock[0]), \
             patch('web.monitor.fetch_json', fetch):
            with self.assertRaises(aiohttp.ClientResponseError):
                await hub.variational_stats()
            clock[0] += 89
            with self.assertRaises(RuntimeError):
                await hub.variational_stats()
            self.assertEqual(fetch.await_count, 1)
            clock[0] += 2
            self.assertEqual(await hub.variational_stats(), {'listings': []})
            self.assertEqual(fetch.await_count, 2)
            self.assertEqual(hub.variational_failures, 0)

    async def test_variational_v2_uses_interval_rate_and_excludes_swaps(self):
        data = {'listings':[
            {'ticker':'ETH-PERP','contract_type':'perpetual','funding':{
                'last':{'annualized_rate':'.1095','interval_rate':'.0001'},'interval_seconds':28800}},
            {'ticker':'XAU-SWAP','contract_type':'swap','funding':None}]}
        result = await current_funding(None,'variational','ETH',variational_data=data)
        self.assertEqual(result,{'funding_rate':'.0001','interval_hours':8})
        self.assertEqual(set(variational_perps(data)),{'ETH'})
        self.assertIsNone(await current_funding(None,'variational','XAU',variational_data=data))

    async def test_variational_stats_cache_shared_between_pairs(self):
        hub = MonitorHub()
        with patch('web.monitor.fetch_json',new=AsyncMock(return_value={'listings':[]})) as fetch:
            await asyncio.gather(hub.variational_stats(),hub.variational_stats())
            fetch.assert_awaited_once()

    async def test_aster_funding_matches_depth_contract_and_keeps_zero_rate(self):
        async def fetch(session, method, url, **kwargs):
            if url.endswith('fundingInfo'):
                return [{'symbol':'ETHUSDT','fundingIntervalHours':8},
                        {'symbol':'ETHUSD','fundingIntervalHours':2}]
            self.assertEqual(kwargs['params']['symbol'],'ETHUSDT')
            return {'lastFundingRate':'0'}
        with patch('web.monitor.aster_contract',new=AsyncMock(return_value={'symbol':'ETHUSDT'})), \
             patch('web.monitor.fetch_json',side_effect=fetch):
            result = await current_funding(None,'aster','ETH')
        self.assertEqual(result,{'funding_rate':'0','interval_hours':8})

    async def test_shared_subscription_closes_only_after_last_browser(self):
        hub = MonitorHub()
        hub.catalog = AsyncMock(return_value={'venues':[
            {'id':'aster','symbols':['ETH']},{'id':'hyperliquid','symbols':['ETH']}]})
        with patch.object(Channel,'start',new=AsyncMock()), patch.object(Channel,'close',new=AsyncMock()) as close:
            async with hub.subscribe(Selection.parse()) as first:
                async with hub.subscribe(Selection.parse()) as second:
                    self.assertIs(first, second)
                    self.assertEqual(first.refs, 2)
                close.assert_not_awaited()
            close.assert_awaited_once()
        self.assertFalse(hub.channels)

    async def test_unlisted_market_cannot_start_feed(self):
        hub = MonitorHub()
        hub.catalog = AsyncMock(return_value={'venues':[
            {'id':'aster','symbols':[]},{'id':'hyperliquid','symbols':['ETH']}]})
        with self.assertRaises(ValueError):
            async with hub.subscribe(Selection.parse()):
                self.fail('must not subscribe')
        self.assertFalse(hub.channels)

    async def test_heartbeat_stops_without_modifying_order_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.engine.save()
            original = h.store.path.read_bytes()
            path = h.store.path.with_suffix('.heartbeat.json')
            async with heartbeat(h.engine):
                async with asyncio.timeout(1):
                    while True:
                        try:
                            running = json.loads(path.read_text())['running']
                            break
                        except OSError:
                            pass
                        await asyncio.sleep(.01)
                self.assertTrue(running)
            self.assertFalse(json.loads(path.read_text())['running'])
            self.assertEqual(h.store.path.read_bytes(), original)


class MonitorApiTests(unittest.TestCase):
    def test_monitor_assets_and_api_validation(self):
        from web.app import create_app
        with TestClient(create_app()) as client:
            self.assertEqual(client.get('/monitor.html').status_code,200)
            self.assertIn('跨所價差',client.get('/monitor.html').text)
            self.assertEqual(client.get('/api/monitor/history',params={
                'symbol':'ETH','short':'mexc','long':'aster'}).status_code,400)
            self.assertEqual(client.get('/api/monitor/funding',params={
                'symbol':'ETH','short':'hyperliquid','long':'aster','days':999}).status_code,400)

    def test_foreign_origin_cannot_read_private_strategy_socket(self):
        from web.app import create_app
        from starlette.websockets import WebSocketDisconnect
        with TestClient(create_app()) as client:
            with self.assertRaises(WebSocketDisconnect):
                with client.websocket_connect('/api/monitor/stream',headers={'origin':'https://evil.example'}):
                    self.fail('foreign origin accepted')

    def test_stream_is_read_only_and_releases_subscription(self):
        from web.app import create_app
        app = create_app()
        released = []
        class Fake:
            def snapshot(self):
                return {'type':'snapshot','ts_ms':1,'spreads':{}}
        @asynccontextmanager
        async def subscribe(selection):
            try:
                yield Fake()
            finally:
                released.append(True)
        app.state.monitor.subscribe = subscribe
        with patch('hydra_basis.execution_engine.order_service.execute_open',new=AsyncMock()) as execute:
            with TestClient(app) as client:
                with client.websocket_connect('/api/monitor/stream') as ws:
                    self.assertEqual(ws.receive_json()['type'],'snapshot')
            execute.assert_not_awaited()
        self.assertEqual(released,[True])
