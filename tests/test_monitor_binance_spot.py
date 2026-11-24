import asyncio
import tempfile
import unittest
from contextlib import asynccontextmanager
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hydra_basis.spread_strategy.core import VENUES as TRADING_VENUES
from hydra_basis.spread_strategy.feeds import MarketFeed
from hydra_basis.spread_strategy.history import fetch_minute_closes, MINUTE_MS
from web import binance_market
from web.monitor import Selection, MonitorHub, History, current_funding, spreads


def market(**changes):
    return dict(symbol='SAGAUSDT', baseAsset='SAGA', quoteAsset='USDT', status='TRADING',
                isSpotTradingAllowed=True, **changes)


class SpotMonitorTests(unittest.IsolatedAsyncioTestCase):
    def test_public_catalog_excludes_non_spot_non_usdt_and_halted_markets(self):
        good = market()
        rows = [good] + [dict(good, **change) for change in (
            {'status': 'BREAK'}, {'quoteAsset': 'USDC'}, {'isSpotTradingAllowed': False},
            {'symbol': 'OTHERUSDT'}, {'baseAsset': ''})]
        self.assertEqual(binance_market.markets({'symbols': rows}), {'SAGA': good})

    def test_selection_is_read_only_and_spot_is_long_leg(self):
        selection = Selection.parse('SAGA', 'hyperliquid', 'binance_spot', '10000')
        self.assertEqual(selection.config().venues, ('hyperliquid', 'binance_spot'))
        self.assertNotIn('binance_spot', TRADING_VENUES)
        with self.assertRaises(ValueError):
            Selection.parse('SAGA', 'binance_spot', 'hyperliquid')

    def test_spot_perp_price_direction_and_depth(self):
        selection = Selection.parse('SAGA', 'hyperliquid', 'binance_spot', '10000')
        feed = MarketFeed(selection.config(), clock=lambda: 10000)
        feed.update('hyperliquid', '.01338', '.01339', source_ms=10000,
                    bids=((D('.01338'), D(20000)),), asks=((D('.01339'), D(20000)),))
        feed.update('binance_spot', '.01329', '.01330', source_ms=None,
                    bids=((D('.01329'), D(20000)),), asks=((D('.01330'), D(20000)),))
        result = spreads(selection, feed, {})
        self.assertAlmostEqual(result['maker_short']['entry'], (.01339-.01330)/.01330*100)
        self.assertAlmostEqual(result['maker_short']['exit'], (.01338-.01329)/.01329*100)
        feed.update('binance_spot', '.01329', '.01330', source_ms=None,
                    bids=((D('.01329'), D(1)),), asks=((D('.01330'), D(1)),))
        self.assertIsNone(spreads(selection, feed, {})['maker_short']['entry'])

    async def test_spot_funding_is_not_applicable_and_never_fetched(self):
        with patch('web.monitor.importlib.import_module') as load:
            self.assertIsNone(await current_funding(object(), 'binance_spot', 'SAGA'))
            load.assert_not_called()
        hub = MonitorHub()
        hub.funding_cache = {('binance_spot', 'SAGA'): [object()]}
        import time
        hub.funding_at = time.monotonic()
        result = await hub.funding_history(Selection.parse('SAGA', 'aster', 'binance_spot'), 1)
        self.assertEqual(result['series']['binance_spot'], [])

    async def test_candles_page_24_hours_and_exclude_unclosed_and_missing_minutes(self):
        start = 1_790_000_000_000 // MINUTE_MS * MINUTE_MS
        end = start + 1440 * MINUTE_MS
        calls = []
        async def fetch(session, method, url, **kwargs):
            params = kwargs['params']; calls.append(params)
            self.assertEqual((method, url), ('GET', 'https://api.binance.com/api/v3/klines'))
            rows = [[t, '1', '1', '1', '2', '1', t+MINUTE_MS-1]
                    for t in range(params['startTime'], params['endTime']+1, MINUTE_MS)
                    if t != start + MINUTE_MS]
            rows.append([end, '1', '1', '1', '999', '1', end+MINUTE_MS-1])
            return rows
        with patch('hydra_basis.spread_strategy.history._json', side_effect=fetch), \
             patch('hydra_basis.spread_strategy.history.time.time', return_value=end/1000+30):
            prices = await fetch_minute_closes(object(), 'binance_spot', 'SAGA', start, end+MINUTE_MS)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]['startTime'], start+1000*MINUTE_MS)
        self.assertEqual(calls[0]['symbol'], 'SAGAUSDT')
        self.assertEqual(len(prices), 1439)
        self.assertNotIn(start+MINUTE_MS, prices)
        self.assertNotIn(end, prices)

    async def test_reference_joins_common_minutes_and_reuses_cache_across_sizes(self):
        with tempfile.TemporaryDirectory() as tmp:
            hub = MonitorHub(History(Path(tmp)/'history.db'))
            hub.session = object()
            now = 1_790_000_000_000 // MINUTE_MS * MINUTE_MS
            hub.references.clock = lambda: now
            async def fetch(session, venue, symbol, start, end):
                return {start: 102 if venue == 'hyperliquid' else 100,
                        start+MINUTE_MS*(1 if venue == 'hyperliquid' else 2): 100}
            with patch('web.monitor.now_ms', return_value=now), \
                 patch('web.monitor.fetch_minute_closes', side_effect=fetch) as fetcher:
                a = await hub.reference(Selection.parse('SAGA', 'hyperliquid', 'binance_spot'), 1)
                b = await hub.reference(Selection.parse('SAGA', 'hyperliquid', 'binance_spot', '10000'), 1)
            self.assertEqual(a, b)
            self.assertEqual(a['series'], [{'ts_ms': now-3600000, 'reference': 2}])
            self.assertEqual(fetcher.await_count, 2)

    async def test_spot_instrument_uses_spot_filters(self):
        info = {'symbols': [market(filters=[{'filterType':'PRICE_FILTER','tickSize':'.00001'},
            {'filterType':'LOT_SIZE','stepSize':'.1','minQty':'.1'},
            {'filterType':'NOTIONAL','minNotional':'5'}])]}
        with patch('web.binance_market.fetch_json', new=AsyncMock(return_value=info)):
            value = await binance_market.instrument(object(), 'SAGA')
        self.assertEqual((value.venue, value.tick_size, value.lot_size), ('binance_spot', D('.00001'), D('.1')))

    async def test_stream_preserves_receipt_age_and_closes_on_disconnect(self):
        now = [10000]
        feed = MarketFeed(Selection.parse('SAGA', 'aster', 'binance_spot').config(), clock=lambda: now[0])
        closed = []
        class Session:
            @asynccontextmanager
            async def ws_connect(self, url, **kwargs):
                self.url = url
                yield object()
        class Depth:
            def __init__(self, **kwargs): self.healthy = True
            async def start(self): pass
            def snapshot(self):
                return {'bid':D(1),'ask':D(2),'bids':[(D(1),D(3))],
                        'asks':[(D(2),D(4))],'age_ms':250}
            async def wait_for_update(self):
                self.assertions()
                self.healthy = False
            def assertions(self):
                assert feed.ticker('binance_spot').received_ms == 9750
                now[0] = 10751
                assert feed.fresh('binance_spot') is None
            async def close(self): closed.append(True)
        session = Session()
        with patch('web.binance_market.BinanceSpotDepthStream', Depth):
            with self.assertRaisesRegex(RuntimeError, 'disconnected'):
                await binance_market.stream(feed, session)
        self.assertIn('sagausdt@depth20@100ms', session.url)
        self.assertEqual(closed, [True])
        self.assertFalse(feed._healthy['binance_spot'])
