import unittest
from contextlib import asynccontextmanager
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

from hydra_basis.spread_strategy.feeds import MarketFeed, StoreFeed
from hydra_basis.spread_strategy.dispatcher import HyperliquidBooksRunner, QuoteStore
from hydra_basis.spread_monitor.runtime import HyperliquidQuoteRunner
from tests.test_spread_strategy import config


def snapshot(coin='ETH', depth=5, stamp=10000):
    return {'channel':'l2Book', 'data':{'coin':coin, 'time':stamp, 'levels':[
        [{'px':str(100-i), 'sz':'1'} for i in range(depth)],
        [{'px':str(101+i), 'sz':'1'} for i in range(depth)]]}}


class FastBookTests(unittest.IsolatedAsyncioTestCase):
    async def test_direct_feed_uses_fast_replaces_old_depth_and_keeps_source_time(self):
        feed = MarketFeed(config(market_freshness_seconds=1, max_transport_lag_seconds=.5,
                                 future_tolerance_seconds=.25), clock=lambda: 10100)
        ws = type('Socket', (), {'send_json':AsyncMock()})()
        @asynccontextmanager
        async def connect(*args, **kwargs):
            yield ws
        session = type('Session', (), {'ws_connect':staticmethod(connect)})()
        async def messages(_):
            yield snapshot(depth=20, stamp=9900)
            yield snapshot(depth=5)
        with patch.object(feed, '_messages', messages):
            await feed._hyperliquid(session, coin='ETH')
        self.assertTrue(ws.send_json.await_args.args[0]['subscription']['fast'])
        book = feed.fresh('hyperliquid')
        self.assertEqual(book.source_ms, 10000)
        self.assertEqual(len(book.asks), 5)
        self.assertEqual(book.executable('BUY', D(5)), D(103))
        self.assertIsNone(book.executable('BUY', D(6)))
        self.assertIsNone(book.marginal('BUY', D(6)))
        feed.clock = lambda: 11001
        self.assertIsNone(feed.fresh('hyperliquid'))

    async def test_dispatcher_uses_fast_for_native_and_hip3_coins(self):
        store = QuoteStore(clock=lambda: 10100)
        runner = HyperliquidBooksRunner(None, store, ['ETH', 'kPEPE', 'io:OAI'])
        packets = []
        async def send(messages):
            packets.extend(messages)
        runner.send_spaced = send
        await runner.subscribe()
        self.assertEqual([p['subscription'] for p in packets], [
            {'type':'l2Book','coin':coin,'fast':True} for coin in runner.coins])
        runner.handle(snapshot(depth=20, stamp=9900))
        runner.handle(snapshot(depth=5))
        feed = StoreFeed(config(), store, {'hyperliquid':True}, clock=lambda:10100)
        self.assertEqual(len(feed.fresh('hyperliquid').asks), 5)
        self.assertIsNone(feed.fresh('hyperliquid').executable('BUY', D(6)))

    async def test_spread_monitor_uses_fast_and_preserves_exchange_symbol_spelling(self):
        ws = type('Socket', (), {'send_json':AsyncMock()})()
        session = type('Session', (), {'ws_connect':AsyncMock(return_value=ws)})()
        runner = HyperliquidQuoteRunner(session, None, ['KPEPE'])
        with patch('hydra_basis.spread_monitor.runtime.hyperliquid_coin_name', return_value='kPEPE'):
            await runner.initialize()
        self.assertEqual(ws.send_json.await_args.args[0]['subscription'],
                         {'type':'l2Book','coin':'kPEPE','fast':True})
