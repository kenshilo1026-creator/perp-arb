import asyncio
import tempfile
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.order_stream import OrderEventStream, decode_aster_order
from hydra_basis.spread_strategy.broker import PaperVenue, submit_market
from tests.test_spread_strategy import Harness


class PushVenue(PaperVenue):
    def __init__(self, venue, feed):
        super().__init__(venue, feed)
        self.stream = OrderEventStream(venue=venue, connect=AsyncMock(), renew=AsyncMock(),
                                       decode=decode_aster_order)
        self.stream.healthy = True

    async def get_cached_order_update(self, *, order_result, symbol, after=0):
        return self.stream.cached_update(symbol=symbol, order_id=order_result['order_id'], after=after)

    async def wait_for_order_update(self, *, order_result, symbol, after=0):
        return await self.stream.wait_for_update(symbol=symbol, order_id=order_result['order_id'], after=after)

    def publish(self, order, qty, stamp):
        remote = self.orders[order.remote['order_id']]
        self._fill(order.side, D(qty)-remote['executedQty'])
        remote['executedQty'] = D(qty)
        remote['status'] = 'FILLED' if D(qty) == D(order.quantity) else 'PARTIALLY_FILLED'
        self.stream.publish({'symbol': 'ETH', 'orderId': order.remote['order_id'],
            'status': remote['status'], 'executedQty': qty, 'origQty': order.quantity,
            'avgPrice': order.price, 'eventTime': stamp})


class SpreadEventTests(unittest.IsolatedAsyncioTestCase):
    async def harness(self, tmp):
        h = Harness(tmp, execution_method='maker_taker', maker_venue='aster', total_quantity=D('.01'))
        h.adapters['aster'] = PushVenue('aster', h.feed)
        h.books(('2000', '2000.5'), ('1999.5', '2000'))
        await h.engine.start()
        await h.engine.step()
        await h.engine.step()  # establish the previous REST poll time
        return h

    async def test_quiet_market_push_wakes_and_hedges_without_poll_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            waiter = asyncio.create_task(h.engine.wait_for_update(h.feed.revision, 30))
            await asyncio.sleep(0)
            h.clock.advance(10)
            h.adapters['aster'].publish(order, '.01', 1)
            await asyncio.wait_for(waiter, .5)
            h.adapters['aster'].get_order_execution = AsyncMock(side_effect=AssertionError('REST not needed'))
            await h.engine.step()
            self.assertEqual(h.legs(), (D('-.01'), D('.01')))
            self.assertEqual(len([o for o in h.state.orders if o.purpose == 'repair']), 1)

    async def test_partial_duplicate_and_later_cumulative_fill_hedge_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            for qty, stamp in [('.005', 1), ('.005', 1), ('.01', 2)]:
                h.clock.advance(10)
                h.adapters['aster'].publish(order, qty, stamp)
                await h.engine.step()
            self.assertEqual(h.legs(), (D('-.01'), D('.01')))
            repairs = [D(o.executed_quantity) for o in h.state.orders if o.purpose == 'repair']
            self.assertEqual(repairs, [D('.005'), D('.005')])

    async def test_push_interrupts_inflight_read_only_rest_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            querying, cancelled = asyncio.Event(), asyncio.Event()
            async def query(**kwargs):
                querying.set()
                try:
                    await asyncio.Future()
                finally:
                    cancelled.set()
            h.adapters['aster'].get_order_execution = query
            h.clock.advance(501)
            step = asyncio.create_task(h.engine.step())
            await asyncio.wait_for(querying.wait(), .5)
            h.adapters['aster'].publish(order, '.01', 1)
            await asyncio.wait_for(step, .5)
            self.assertTrue(cancelled.is_set())
            self.assertEqual(h.legs(), (D('-.01'), D('.01')))

    async def test_disconnected_stream_waits_for_market_instead_of_spinning(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            h.adapters['aster'].stream.healthy = False
            task = asyncio.create_task(h.engine.wait_for_update(h.feed.revision, 30))
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertFalse(task.done())
            h.feed.update('hyperliquid', D('1999'), D('2000'), source_ms=h.clock())
            await asyncio.wait_for(task, .5)

    async def test_broker_fill_before_ack_bypasses_three_second_poll(self):
        adapter = AsterExecutionAdapter()
        adapter._resolve_raw_symbol = AsyncMock(return_value='ETHUSDT')
        stream = OrderEventStream(venue='aster', connect=AsyncMock(), renew=AsyncMock(), decode=decode_aster_order)
        stream.healthy = True
        adapter._fill_stream = stream
        async def submit(**kwargs):
            stream.publish({'symbol': 'ETHUSDT', 'orderId': '1', 'status': 'FILLED',
                'executedQty': '.01', 'origQty': '.01', 'avgPrice': '2000', 'eventTime': 1})
            return {'order_id': '1'}
        adapter.place_market_order = AsyncMock(side_effect=submit)
        adapter.get_order_execution = AsyncMock(side_effect=AssertionError('REST not needed'))
        result = await asyncio.wait_for(submit_market(adapter, symbol='ETH', side='BUY', quantity=D('.01'),
            reduce_only=False, reference_price=D(2000), timeout_seconds=5, poll_seconds=3), .5)
        self.assertEqual((result.state, result.filled, result.average), ('FILLED', D('.01'), D(2000)))
        adapter.place_market_order.assert_awaited_once()
        adapter.get_order_execution.assert_not_awaited()

    async def test_failed_stream_preserves_rest_confirmation(self):
        adapter = type('Adapter', (), {})()
        adapter.place_market_order = AsyncMock(return_value={'order_id': '1'})
        adapter.wait_for_hedge_terminal = AsyncMock(side_effect=RuntimeError('offline'))
        adapter.get_order_execution = AsyncMock(return_value={
            'status': 'FILLED', 'executedQty': '.01', 'avgPrice': '2000'})
        result = await submit_market(adapter, symbol='ETH', side='BUY', quantity=D('.01'),
            reduce_only=False, reference_price=D(2000), timeout_seconds=1, poll_seconds=.001)
        self.assertEqual(result.filled, D('.01'))
        adapter.place_market_order.assert_awaited_once()

    async def test_unsafe_price_is_cancelled_inside_requote_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            h.clock.advance(10)
            h.books(('2000', '2000.5'), ('2009.5', '2010'))
            await h.engine.step()
            self.assertEqual(order.state, 'CANCELED')
            self.assertEqual(h.legs(), (D(0), D(0)))

    async def test_depth_shrink_cancels_unchanged_price(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            h.clock.advance(10)
            h.feed.update('hyperliquid', D('1999.5'), D('2000'), source_ms=h.clock(),
                          bids=((D('1999.5'), D('.005')),), asks=((D(2000), D('.005')),))
            await h.engine.step()
            self.assertEqual(order.state, 'CANCELED')
            h.clock.advance(2000)
            await h.engine.step()
            self.assertEqual(D(h.engine.quotes()['entry'].quantity), D('.005'))

    async def test_fill_disclosed_by_safety_cancel_is_hedged_immediately(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            async def cancel(**kwargs):
                h.adapters['aster'].publish(order, '.005', 1)
                # PaperVenue only cancels NEW; report terminal cumulative fill explicitly.
                remote = h.adapters['aster'].orders[order.remote['order_id']]
                remote['status'] = 'CANCELED'
                return {'raw': h.adapters['aster']._status(order.remote['order_id'])}
            h.adapters['aster'].cancel_order = cancel
            h.clock.advance(10)
            h.books(('2000', '2000.5'), ('2009.5', '2010'))
            await h.engine.step()
            self.assertEqual(order.state, 'CANCELED')
            self.assertEqual(h.legs(), (D('-.005'), D('.005')))

    async def test_known_fill_is_hedged_even_if_public_feed_disconnects(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            h.clock.advance(10)
            h.feed.set_health('hyperliquid', False)
            h.adapters['aster'].publish(order, '.01', 1)
            hedge = AsyncMock(return_value={'terminal': True, 'filled_quantity': '.01', 'avg_price': '2001'})
            h.adapters['hyperliquid'].place_market_order = hedge
            await h.engine.step()
            self.assertEqual(h.legs(), (D('-.01'), D('.01')))
            hedge.assert_awaited_once()
            self.assertNotIn('limit_price', hedge.call_args.kwargs)
            self.assertTrue(h.events_named('repair_without_fresh_quote'))
            self.assertFalse(h.engine.quotes(), 'stale data must not create new risk')

    async def test_first_taker_result_is_durable_while_other_submission_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.wide()
            pending = asyncio.Event()
            release = asyncio.Event()
            async def slow(**kwargs):
                pending.set()
                await release.wait()
                return {'terminal': True, 'filled_quantity': '.01', 'avg_price': '2000'}
            h.adapters['hyperliquid'].place_market_order = slow
            task = asyncio.create_task(h.engine.step())
            await asyncio.wait_for(pending.wait(), .5)
            self.assertEqual(h.legs(), (D('-.01'), D(0)))
            saved = h.store.load(h.config, live=False)
            self.assertEqual(D(saved.short.quantity), D('-.01'))
            self.assertFalse([o for o in h.state.orders if o.purpose == 'repair'])
            release.set()
            await asyncio.wait_for(task, .5)
            self.assertEqual(h.legs(), (D('-.01'), D('.01')))

    async def test_unconfirmed_safety_cancel_blocks_new_quotes_but_keeps_fill_processing(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            h.adapters['aster'].publish(order, '.005', 1)
            h.adapters['aster'].cancel_order = AsyncMock(side_effect=TimeoutError('unknown cancel'))
            h.clock.advance(10)
            h.books(('2000', '2000.5'), ('2009.5', '2010'))
            await h.engine.step()
            self.assertEqual(h.legs(), (D('-.005'), D('.005')))
            self.assertEqual(list(h.engine.quotes()), ['entry'])
            self.assertIs(h.engine.quotes()['entry'], order)

    async def test_stale_rest_terminal_cannot_erase_newer_partial_fill(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = await self.harness(tmp)
            order = h.engine.quotes()['entry']
            h.engine.apply(order, D('.005'), D(order.price), None)
            h.engine.apply(order, D(0), None, 'CANCELED')
            self.assertEqual(order.state, 'OPEN')
            self.assertEqual(D(order.executed_quantity), D('.005'))
