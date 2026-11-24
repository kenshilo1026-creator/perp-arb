"""Public, read-only Binance USDT spot data for the monitor."""
from decimal import Decimal
import math

from hydra_basis.adapters.base import fetch_json
from hydra_basis.execution_engine.binance_spot_stream import BinanceSpotDepthStream
from hydra_basis.execution_engine.mexc_depth_stream import DepthQuoteUnavailable
from hydra_basis.spread_strategy.instruments import Instrument

BASE = 'https://api.binance.com'


def markets(info):
    return {r['baseAsset']: r for r in info.get('symbols', [])
            if r.get('status') == 'TRADING' and r.get('quoteAsset') == 'USDT'
            and r.get('isSpotTradingAllowed') is not False
            and r.get('baseAsset') and r.get('symbol') == r['baseAsset'] + 'USDT'}


async def instrument(session, symbol):
    info = await fetch_json(session, 'GET', BASE + '/api/v3/exchangeInfo', params={'symbol': symbol + 'USDT'})
    row = markets(info).get(symbol)
    if row is None:
        raise ValueError('Binance spot market unavailable')
    filters = {f['filterType']: f for f in row.get('filters', [])}
    def dec(value):
        return Decimal(str(value)) if value not in (None, '') else None
    lot = filters.get('LOT_SIZE', {})
    notional = filters.get('NOTIONAL', filters.get('MIN_NOTIONAL', {}))
    return Instrument('binance_spot', tick_size=dec(filters.get('PRICE_FILTER', {}).get('tickSize')),
                      lot_size=dec(lot.get('stepSize')), min_size=dec(lot.get('minQty')),
                      min_notional=dec(notional.get('minNotional')))


async def stream(feed, session):
    native = feed.config.symbol + 'USDT'
    if not native.isascii() or not native.isalnum():
        raise ValueError('invalid Binance spot symbol')
    stream_name = native.lower()
    url = f'wss://stream.binance.com:9443/stream?streams={stream_name}@bookTicker/{stream_name}@depth20@100ms'
    async with session.ws_connect(url, heartbeat=20) as ws:
        async def connect():
            return ws
        async def snapshot():
            return await fetch_json(session, 'GET', BASE + '/api/v3/depth', params={'symbol': native, 'limit': 20})
        depth = BinanceSpotDepthStream(connect=connect, symbol=native, fetch_snapshot=snapshot)
        try:
            await depth.start()
            while depth.healthy:
                try:
                    book = depth.snapshot()
                    # Preserve the original receipt/request-start age on every
                    # wake; an unchanged read must not manufacture freshness.
                    received = feed.clock() - math.ceil(book['age_ms'])
                    feed.update('binance_spot', book['bid'], book['ask'], source_ms=None,
                                received_ms=received, bids=tuple(book['bids']), asks=tuple(book['asks']))
                except DepthQuoteUnavailable:
                    feed.set_health('binance_spot', False)
                await depth.wait_for_update()
            raise RuntimeError('Binance public depth disconnected')
        finally:
            feed.set_health('binance_spot', False)
            await depth.close()
