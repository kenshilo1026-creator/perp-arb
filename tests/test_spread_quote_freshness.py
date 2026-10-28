import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.market_data import variational_quote_source_ms
from hydra_basis.spread_strategy.core import Config
from hydra_basis.spread_strategy.dispatcher import QuoteStore, VariationalRunner
from hydra_basis.spread_strategy.feeds import StoreFeed
from tests.test_spread_strategy import Clock, config
from tests.test_spread_dispatcher import settings


class QuoteFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.store = QuoteStore(clock=self.clock)
        self.feed = StoreFeed(config(), self.store, {'aster': True}, clock=self.clock)
        self.store.update_depth('aster', 'ETH', ((D(99), D(10)),), ((D(100), D(10)),),
                                source_ms=self.clock())

    def bbo(self, bid='100', ask='101', size='1', with_size=True):
        self.clock.advance(10)
        q = {'bid': float(bid), 'ask': float(ask), 'ts_ms': self.clock()}
        if with_size:
            q.update(bids=((D(bid), D(size)),), asks=((D(ask), D(size)),))
        self.store.update_quotes('aster', {'ETH': q})

    def test_new_bbo_invalidates_older_depth_and_limits_size(self):
        self.bbo()
        book = self.feed.fresh('aster')
        self.assertEqual(book.executable('BUY', D(1)), D(101))
        self.assertIsNone(book.executable('BUY', D(2)))

    def test_same_price_smaller_top_does_not_reuse_old_capacity(self):
        self.bbo('99', '100', '.5')
        self.assertIsNone(self.feed.fresh('aster').executable('BUY', D(1)))

    def test_new_price_without_size_disables_trade_until_depth_recovers(self):
        self.bbo(with_size=False)
        self.assertIsNone(self.feed.fresh('aster'))
        self.clock.advance(10)
        self.store.update_depth('aster', 'ETH', ((D(100), D(10)),), ((D(101), D(10)),),
                                source_ms=self.clock())
        self.assertEqual(self.feed.fresh('aster').executable('BUY', D(2)), D(101))

    def test_old_delayed_bbo_does_not_override_newer_source_depth(self):
        old = self.clock()-100
        self.clock.advance(10)
        self.store.update_quotes('aster', {'ETH': {'bid': 80, 'ask': 81, 'ts_ms': old}})
        self.assertEqual(self.feed.fresh('aster').ask, D(100))

    def test_timestamp_is_timezone_aware_and_not_local_receipt(self):
        self.assertEqual(variational_quote_source_ms({'quotes': {'updated_at': '2023-11-14T22:13:20Z'}}),
                         1700000000000)
        for value in (None, '2023-11-14T22:13:20', 'invalid'):
            with self.assertRaises(RuntimeError):
                variational_quote_source_ms({'quotes': {'updated_at': value}})

    def test_default_execution_freshness_is_one_second(self):
        c = Config.load(Path('configs/spread_strategy.example.json'))
        self.assertEqual(c.market_freshness_seconds, 1)
        self.assertEqual(c.max_transport_lag_seconds, .5)


class VariationalTimestampTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatcher_preserves_source_age_and_invalidates_missing_timestamp(self):
        store = QuoteStore(clock=lambda: 1700000005000)
        runner = VariationalRunner(None, store, settings())
        listing = {'ticker': 'ETH', 'quotes': {'updated_at': '2023-11-14T22:13:20Z'}}
        with patch('hydra_basis.spread_strategy.dispatcher.fetch_json', AsyncMock(return_value={'listings': [listing]})), \
             patch('hydra_basis.spread_strategy.dispatcher.select_variational_quote_fields', return_value=(99., 100.)), \
             patch('hydra_basis.spread_strategy.dispatcher.asyncio.sleep', AsyncMock()):
            await runner.pump_once()
            self.assertEqual(store.get_quote('variational', 'ETH')['source_ms'], 1700000000000)
            listing['quotes'].clear()
            await runner.pump_once()
            self.assertIsNone(store.get_quote('variational', 'ETH'))
