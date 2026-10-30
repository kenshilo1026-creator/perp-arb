"""Offline deterministic execution scenarios, NOT a historical profitability backtest.

Run against each revision with the same scenario timeline; output contains only
simulated clock measurements. No credentials, network, production state or orders.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from decimal import Decimal as D
from pathlib import Path

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from scripts._bootstrap import ensure_project_root_on_path
ensure_project_root_on_path()

from hydra_basis.spread_strategy.broker import PaperVenue
from hydra_basis.spread_strategy.core import Config, StateStore
from hydra_basis.spread_strategy.engine import Engine
from hydra_basis.spread_strategy.feeds import MarketFeed
from hydra_basis.spread_strategy.instruments import Instrument


class ReplayVenue(PaperVenue):
    """Exchange state is independent of the strategy's public quote health."""

    def __init__(self, venue, feed):
        super().__init__(venue, feed)
        self.pushes = {}
        self.sequence = 0
        self.market_fills = []

    async def get_cached_order_update(self, *, order_result, symbol, after=0):
        item = self.pushes.get(order_result['order_id'])
        return item if item and item['stream_sequence'] > after else None

    def fill_quote(self, order_id, quantity, clock):
        order = self.orders[order_id]
        if order['status'] not in {'NEW', 'PARTIALLY_FILLED'}:
            return
        delta = quantity - order['executedQty']
        self._fill(order['side'], delta)
        order.update(executedQty=quantity,
                     status='FILLED' if quantity == order['quantity'] else 'PARTIALLY_FILLED')
        self.sequence += 1
        self.pushes[order_id] = {**self._status(order_id), 'stream_sequence': self.sequence,
                                'source': 'user_stream'}

    async def get_order_execution(self, *, order_result, symbol):
        return self._status(order_result['order_id'])

    async def cancel_order(self, *, order_result, **kwargs):
        order = self.orders[order_result['order_id']]
        if order['status'] in {'NEW', 'PARTIALLY_FILLED'}:
            order['status'] = 'CANCELED'
        return {'ok': True, 'raw': self._status(order_result['order_id'])}

    async def place_market_order(self, *, side, amount, **kwargs):
        # This simulated exchange remains available if our public feed disconnects.
        book = self.feed.ticker(self.venue)
        quantity = D(amount)
        price = book.ask if side == 'BUY' else book.bid
        self._fill(side, quantity)
        self.market_fills.append({'at_ms': self.feed.clock(), 'quantity': str(quantity),
                                  'price': str(price)})
        return {'terminal': True, 'filled_quantity': str(quantity), 'avg_price': str(price)}


async def scenario(name):
    now = [1_700_000_000_000]
    origin = now[0]
    fees = {v: {'maker': D(0), 'taker': D(0)} for v in ('aster', 'hyperliquid')}
    config = Config(symbol='ETH', short_venue='aster', long_venue='hyperliquid',
                    maker_venue='aster', total_quantity=D('.01'), clip_quantity=D('.01'),
                    entry_bps=D(40), take_profit_bps=D(10), fees=fees,
                    min_profit_bps=D(0), slippage_buffer_bps=D(0), funding_budget_bps=D(0),
                    # Freeze the original profile across revisions for fair comparisons.
                    market_freshness_seconds=15, max_transport_lag_seconds=3, future_tolerance_seconds=2)
    feed = MarketFeed(config, clock=lambda: now[0])

    def books(hedge_price='2000', size='.02'):
        for venue, bid, ask in [('aster', '2000', '2000.5'),
                                ('hyperliquid', str(D(hedge_price)-D('.5')), hedge_price)]:
            feed.update(venue, D(bid), D(ask), source_ms=now[0],
                        bids=((D(bid), D(size)),), asks=((D(ask), D(size)),))

    books()
    adapters = {v: ReplayVenue(v, feed) for v in config.venues}
    instruments = {v: Instrument(v, tick_size=D('.01'), lot_size=D('.001'),
                                min_size=D('.001'), min_notional=D(5)) for v in config.venues}
    events = []
    with tempfile.TemporaryDirectory() as directory:
        store = StateStore(Path(directory) / 'state.json')
        state = store.load(config, live=False)
        engine = Engine(config, state, store, feed, adapters, instruments, live=False,
                        clock=lambda: now[0], log=events.append)
        await engine.start()
        await engine.step()
        first = engine.quotes()['entry']
        # Establish a REST poll just before the fill, the worst polling phase.
        await engine.step()
        cancel_at = None
        for elapsed in range(10, 2110, 10):
            now[0] = origin + elapsed
            if name in {'fill_then_jump', 'fill_with_feed_loss'} and elapsed == 10:
                adapters['aster'].fill_quote(first.remote['order_id'], D('.01'), now[0])
                if name == 'fill_with_feed_loss':
                    feed._healthy['hyperliquid'] = False
            if name == 'fill_then_jump' and elapsed == 200:
                books('2010')
            if name == 'unsafe_quote' and elapsed == 100:
                books('2010')
            if name == 'depth_shrink' and elapsed == 100:
                books(size='.005')
            await engine.step()
            if first.state == 'CANCELED' and cancel_at is None:
                cancel_at = elapsed
        fills = adapters['hyperliquid'].market_fills
        return {'scenario': name,
                'maker_fill_to_hedge_ms': fills[0]['at_ms']-origin-10 if fills else None,
                'first_hedge_price': fills[0]['price'] if fills else None,
                'unsafe_quote_cancel_delay_ms': cancel_at-100 if cancel_at is not None else None,
                'remaining_exposure': str(engine.imbalance()),
                'hedge_orders': len(fills),
                'entry_quote_remaining': str(D(engine.quotes()['entry'].quantity) -
                                             D(engine.quotes()['entry'].executed_quantity))
                    if 'entry' in engine.quotes() else '0'}


async def pending_hedge_scenario():
    """500 ms ACK hold; further maker fill and unsafe price arrive at 100 ms."""
    now = [1_700_000_000_000]
    fees = {v: {'maker': D(0), 'taker': D(0)} for v in ('aster', 'hyperliquid')}
    config = Config(symbol='ETH', short_venue='aster', long_venue='hyperliquid',
                    maker_venue='aster', total_quantity=D('.02'), clip_quantity=D('.02'),
                    entry_bps=D(40), take_profit_bps=D(10), fees=fees,
                    min_profit_bps=D(0), slippage_buffer_bps=D(0), funding_budget_bps=D(0),
                    market_freshness_seconds=15, max_transport_lag_seconds=3, future_tolerance_seconds=2)
    feed = MarketFeed(config, clock=lambda: now[0])
    adapters = {v: ReplayVenue(v, feed) for v in config.venues}
    instruments = {v: Instrument(v, tick_size=D('.01'), lot_size=D('.001'),
                                min_size=D('.001'), min_notional=D(5)) for v in config.venues}

    def books(ask):
        feed.update('aster', D(2000), D('2000.5'), source_ms=now[0])
        feed.update('hyperliquid', D(ask)-D('.5'), D(ask), source_ms=now[0])

    books('2000')
    with tempfile.TemporaryDirectory() as directory:
        store = StateStore(Path(directory) / 'state.json')
        state = store.load(config, live=False)
        engine = Engine(config, state, store, feed, adapters, instruments, live=False,
                        clock=lambda: now[0], log=lambda event: None)
        await engine.start()
        await engine.step()
        await engine.step()
        maker, hedge = adapters['aster'], adapters['hyperliquid']
        order = engine.quotes()['entry']
        origin = now[0]
        release = asyncio.Event()
        started = asyncio.Event()
        original = hedge.place_market_order

        async def slow_ack(**kwargs):
            result = await original(**kwargs)
            if len(hedge.market_fills) == 1:
                started.set()
                await release.wait()
            return result

        hedge.place_market_order = slow_ack
        maker.fill_quote(order.remote['order_id'], D('.005'), now[0])
        step = asyncio.create_task(engine.step())
        await asyncio.wait_for(started.wait(), 1)
        cancel_at = observed_at = None
        try:
            for elapsed in range(0, 601, 10):
                now[0] = origin + elapsed
                if elapsed == 100:
                    maker.fill_quote(order.remote['order_id'], D('.01'), now[0])
                    books('2010')
                if elapsed == 500:
                    release.set()
                # Settle scheduled coroutines at this virtual timestamp.
                for _ in range(60):
                    await asyncio.sleep(0)
                if D(order.executed_quantity) >= D('.01') and observed_at is None:
                    observed_at = elapsed
                if order.state == 'CANCELED' and cancel_at is None:
                    cancel_at = elapsed
            await asyncio.wait_for(step, 1)
        finally:
            release.set()
            await step
        return {'scenario': 'pending_hedge_ack', 'first_hedge_ack_delay_ms': 500,
                'extra_fill_observation_delay_ms': observed_at-100 if observed_at is not None else None,
                'unsafe_quote_cancel_delay_ms': cancel_at-100 if cancel_at is not None else None,
                'hedge_quantities': [f['quantity'] for f in hedge.market_fills],
                'remaining_exposure': str(engine.imbalance())}


async def replay():
    return {'kind': 'synthetic_event_replay', 'historical_profit_backtest': False,
            'clock_resolution_ms': 10,
            'results': [await scenario(name) for name in
                        ('fill_then_jump', 'unsafe_quote', 'depth_shrink', 'fill_with_feed_loss')]
                       + [await pending_hedge_scenario()]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = asyncio.run(replay())
    content = json.dumps(result, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content+'\n', encoding='utf-8')
    print(content)


if __name__ == '__main__':
    main()
