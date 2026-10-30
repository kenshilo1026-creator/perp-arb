"""Exercise events arriving while exchange writes are still unresolved."""
import asyncio
import tempfile
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

from hydra_basis.spread_strategy.broker import Execution
from tests.test_spread_execution_events import PushVenue
from tests.test_spread_strategy import Harness


async def eventually(predicate):
    async with asyncio.timeout(1):
        while not predicate():
            await asyncio.sleep(.001)


class HedgeCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def setup_harness(self, directory):
        h = Harness(directory, execution_method='maker_taker', maker_venue='aster',
                    total_quantity=D('.02'), clip_quantity=D('.02'))
        maker = PushVenue('aster', h.feed)
        h.adapters['aster'] = maker
        h.books(('2000', '2000.5'), ('1999.5', '2000'))
        await h.engine.start()
        await h.engine.step()
        await h.engine.step()
        order = h.engine.quotes()['entry']

        async def cancel(**kwargs):
            remote = maker.orders[order.remote['order_id']]
            if remote['status'] != 'FILLED':
                remote['status'] = 'CANCELED'
            return maker._status(order.remote['order_id'])

        maker.cancel_order = AsyncMock(side_effect=cancel)
        return h, maker, order

    async def test_partial_fills_and_cancel_continue_while_hedge_ack_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            h, maker, order = await self.setup_harness(directory)
            ack, cancel_ack = asyncio.Event(), asyncio.Event()
            sends, cancel_started = [], asyncio.Event()
            cancel = maker.cancel_order

            async def slow_cancel(**kwargs):
                cancel_started.set()
                await cancel_ack.wait()
                return await cancel(**kwargs)

            async def hedge(**kwargs):
                sends.append(kwargs)
                if len(sends) == 1:
                    await ack.wait()
                return {'terminal': True, 'filled_quantity': kwargs['amount'], 'avg_price': '2000'}

            maker.cancel_order = AsyncMock(side_effect=slow_cancel)
            h.adapters['hyperliquid'].place_market_order = hedge
            maker.publish(order, '.005', 1)
            task = asyncio.create_task(h.engine.step())
            try:
                await eventually(lambda: len(sends) == 1)
                maker.publish(order, '.01', 2)
                h.books(('2000', '2000.5'), ('2009.5', '2010'))
                await asyncio.wait_for(cancel_started.wait(), 1)
                await eventually(lambda: D(order.executed_quantity) == D('.01'))
                self.assertEqual(len(sends), 1, 'unknown first outcome remains reserved')
                self.assertEqual(D(h.store.load(h.config, live=False).short.quantity), D('-.01'))
                maker.publish(order, '.015', 3)
                await eventually(lambda: D(order.executed_quantity) == D('.015'))
                ack.set()
                await eventually(lambda: len(sends) == 2)
                await eventually(lambda: h.engine.imbalance() == 0)
                self.assertFalse(cancel_ack.is_set(), 'slow cancel cannot block the next known hedge')
                self.assertEqual([D(s['amount']) for s in sends], [D('.005'), D('.01')])
                maker.cancel_order.assert_awaited_once()
            finally:
                ack.set()
                cancel_ack.set()
                await asyncio.wait_for(task, 1)
            self.assertEqual(h.legs(), (D('-.015'), D('.015')))

    async def test_unknown_hedge_withdraws_quotes_without_resending(self):
        with tempfile.TemporaryDirectory() as directory:
            h, maker, order = await self.setup_harness(directory)
            release = asyncio.Event()
            started = asyncio.Event()

            async def unresolved(*args, **kwargs):
                started.set()
                await release.wait()
                return Execution(D('.002'), D(2000), 'UNKNOWN', {'order_id': 'hedge-1'}, 'timeout')

            with patch('hydra_basis.spread_strategy.engine.submit_market', side_effect=unresolved) as submit:
                maker.publish(order, '.005', 1)
                task = asyncio.create_task(h.engine.step())
                try:
                    await asyncio.wait_for(started.wait(), 1)
                    maker.publish(order, '.01', 2)
                    await eventually(lambda: D(order.executed_quantity) == D('.01'))
                finally:
                    release.set()
                    await asyncio.wait_for(task, 1)
                await h.engine.step()
                submit.assert_awaited_once()
            self.assertEqual(h.state.status, 'PAUSED')
            self.assertEqual(order.state, 'CANCELED')
            self.assertEqual(h.legs(), (D('-.01'), D('.002')))
            unresolved_order = h.engine.non_quote_open_orders()[0]
            self.assertEqual(unresolved_order.state, 'UNKNOWN')
            self.assertEqual(D(unresolved_order.executed_quantity), D('.002'))

    async def test_stop_waits_for_sent_hedge_and_persists_result(self):
        with tempfile.TemporaryDirectory() as directory:
            h, maker, order = await self.setup_harness(directory)
            release, started = asyncio.Event(), asyncio.Event()

            async def hedge(**kwargs):
                started.set()
                await release.wait()
                return {'terminal': True, 'filled_quantity': kwargs['amount'], 'avg_price': '2000'}

            h.adapters['hyperliquid'].place_market_order = AsyncMock(side_effect=hedge)
            maker.publish(order, '.005', 1)
            task = asyncio.create_task(h.engine.step())
            await asyncio.wait_for(started.wait(), 1)
            task.cancel()
            try:
                await eventually(lambda: order.terminal)
                self.assertFalse(task.done(), 'submission must not be cancelled or forgotten')
            finally:
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 1)
            self.assertEqual(h.legs(), (D('-.005'), D('.005')))
            self.assertEqual(D(h.store.load(h.config, live=False).long.quantity), D('.005'))
            self.assertIsNone(h.engine._repair_task)
            h.adapters['hyperliquid'].place_market_order.assert_awaited_once()

    async def test_slow_rest_fallback_does_not_delay_safety_cancel(self):
        with tempfile.TemporaryDirectory() as directory:
            h, maker, order = await self.setup_harness(directory)
            hedge_ack, query_started, query_cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

            async def slow_query(**kwargs):
                query_started.set()
                try:
                    await asyncio.Future()
                finally:
                    query_cancelled.set()

            async def hedge(**kwargs):
                await hedge_ack.wait()
                return {'terminal': True, 'filled_quantity': kwargs['amount'], 'avg_price': '2000'}

            maker.get_order_execution = slow_query
            h.adapters['hyperliquid'].place_market_order = hedge
            maker.publish(order, '.005', 1)
            h.clock.advance(501)
            task = asyncio.create_task(h.engine.step())
            try:
                await asyncio.wait_for(query_started.wait(), 1)
                maker.publish(order, '.01', 2)
                h.books(('2000', '2000.5'), ('2009.5', '2010'))
                await eventually(lambda: order.terminal)
                self.assertEqual(D(order.executed_quantity), D('.01'))
            finally:
                hedge_ack.set()
                await asyncio.wait_for(task, 1)
            self.assertTrue(query_cancelled.is_set())
            self.assertEqual(h.legs(), (D('-.01'), D('.01')))

    async def test_concurrent_repair_requests_share_one_submission(self):
        with tempfile.TemporaryDirectory() as directory:
            h, maker, order = await self.setup_harness(directory)
            release, started = asyncio.Event(), asyncio.Event()

            async def hedge(**kwargs):
                started.set()
                await release.wait()
                return {'terminal': True, 'filled_quantity': kwargs['amount'], 'avg_price': '2000'}

            submit = AsyncMock(side_effect=hedge)
            h.adapters['hyperliquid'].place_market_order = submit
            maker.publish(order, '.005', 1)
            await h.engine.refresh_stream_orders()
            first = asyncio.create_task(h.engine.ensure_hedged())
            await asyncio.wait_for(started.wait(), 1)
            second = asyncio.create_task(h.engine.ensure_hedged())
            release.set()
            await asyncio.wait_for(asyncio.gather(first, second), 1)
            submit.assert_awaited_once()
            self.assertEqual(h.legs(), (D('-.005'), D('.005')))

    async def test_private_hedge_confirmation_wait_does_not_block_maker_events(self):
        with tempfile.TemporaryDirectory() as directory:
            h, maker, order = await self.setup_harness(directory)
            release, waiting = asyncio.Event(), asyncio.Event()
            hedge = h.adapters['hyperliquid']
            amounts = []

            async def submit(**kwargs):
                amounts.append(kwargs['amount'])
                if len(amounts) == 1:
                    return {'order_id': 'hedge-1'}
                return {'terminal': True, 'filled_quantity': kwargs['amount'], 'avg_price': '2000'}

            async def confirm(**kwargs):
                waiting.set()
                await release.wait()
                return {'status': 'FILLED', 'executedQty': '.005', 'avgPrice': '2000'}

            hedge.place_market_order = submit
            hedge.wait_for_hedge_terminal = confirm
            hedge.get_order_execution = AsyncMock(return_value={'status': 'NEW', 'executedQty': '0'})
            maker.publish(order, '.005', 1)
            task = asyncio.create_task(h.engine.step())
            try:
                await asyncio.wait_for(waiting.wait(), 1)
                maker.publish(order, '.01', 2)
                h.books(('2000', '2000.5'), ('2009.5', '2010'))
                await eventually(lambda: order.terminal)
                self.assertEqual(D(order.executed_quantity), D('.01'))
                self.assertEqual(amounts, ['0.005'])
            finally:
                release.set()
                await asyncio.wait_for(task, 1)
            self.assertEqual([D(q) for q in amounts], [D('.005'), D('.005')])
            self.assertEqual(h.engine.imbalance(), 0)

    async def test_rest_only_maker_updates_are_ingested_during_pending_hedge(self):
        with tempfile.TemporaryDirectory() as directory:
            h, maker, order = await self.setup_harness(directory)
            release, started = asyncio.Event(), asyncio.Event()
            maker.get_cached_order_update = None
            maker.wait_for_order_update = None

            async def hedge(**kwargs):
                started.set()
                await release.wait()
                return {'terminal': True, 'filled_quantity': kwargs['amount'], 'avg_price': '2000'}

            h.adapters['hyperliquid'].place_market_order = hedge
            maker.publish(order, '.005', 1)
            h.engine.apply(order, D('.005'), D(order.price), None)
            task = asyncio.create_task(h.engine.ensure_hedged())
            try:
                await asyncio.wait_for(started.wait(), 1)
                maker.publish(order, '.01', 2)
                h.clock.advance(501)
                h.books(('2000', '2000.5'), ('1999.5', '2000'))
                await eventually(lambda: D(order.executed_quantity) == D('.01'))
                self.assertEqual(h.legs(), (D('-.01'), D(0)))
            finally:
                release.set()
                await asyncio.wait_for(task, 1)
            self.assertEqual(h.legs(), (D('-.01'), D('.01')))
