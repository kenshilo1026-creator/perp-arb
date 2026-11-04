"""Read-only market dashboard. No execution adapters, order calls or Telegram.

Market subscriptions are shared across pairs/sizes. SQLite stores five-second observations;
it does not reconstruct executable prices from candles.
"""
from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import math
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect

from hydra_basis.adapters.base import fetch_json
from hydra_basis.execution_engine.apy_service import load_points
from hydra_basis.execution_engine.market_data import parse_variational_quote, variational_quote_source_ms
from hydra_basis.spread_strategy.core import Config, now_ms, spread_bps
from hydra_basis.spread_strategy.feeds import MarketFeed, vwap
from hydra_basis.spread_strategy.history import MinuteRecorder, fetch_minute_closes
from hydra_basis.spread_strategy.instruments import fetch_instrument, aster_contract
from hydra_basis.symbol_mapping import canonicalize_symbol
from web.monitor_cache import ReferenceCache, MonitorSession, retry_seconds

VENUES = {'aster': 'Aster', 'hyperliquid': 'Hyperliquid', 'lighter': 'Lighter',
          'variational': 'Variational', 'entropy': 'Entropy', 'arcus': 'Arcus', 'ondo': 'Ondo'}
DATA_DIR = Path('data/spread_strategies')
DB_PATH = Path('data/market_monitor/history.sqlite3')
SAMPLE_MS = 5000
MAX_ROWS = 150_000
VARIATIONAL_STATS = 'https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/v2/stats'


@dataclass(frozen=True)
class Selection:
    symbol: str
    short: str
    long: str
    quantity: Decimal

    @classmethod
    def parse(cls, symbol='ETH', short='hyperliquid', long='aster', quantity='0.01'):
        symbol = str(symbol).strip().upper()
        if not symbol or len(symbol) > 40 or not all(c.isalnum() or c in '-_' for c in symbol):
            raise ValueError('請選擇有效的交易對')
        if short not in VENUES or long not in VENUES or short == long:
            raise ValueError('請選擇兩個不同的永續 DEX')
        try:
            qty = Decimal(str(quantity))
            valid = qty.is_finite() and Decimal('0.000000001') <= qty <= Decimal('1000000000')
        except Exception:
            valid = False
        if not valid:
            raise ValueError('觀察數量必須介於 0.000000001 與 1,000,000,000')
        return cls(symbol, short, long, qty.normalize())

    @property
    def key(self):
        return hashlib.sha256(f'{self.symbol}:{self.short}:{self.long}:{self.quantity}'.encode()).hexdigest()[:24]

    def config(self):
        return Config(symbol=self.symbol, short_venue=self.short, long_venue=self.long,
                      total_quantity=self.quantity, clip_quantity=self.quantity,
                      execution_method='taker_taker', entry_bps=Decimal(40), take_profit_bps=Decimal(10),
                      fees={v: {'maker': Decimal(0), 'taker': Decimal(0)} for v in (self.short, self.long)})


def rate_payload(rate, interval, ts_ms):
    rate, interval = float(rate), float(interval)
    if not math.isfinite(rate) or not math.isfinite(interval) or interval <= 0:
        raise ValueError('無效的資金費率')
    return {'raw_pct': rate * 100, 'hourly_pct': rate / interval * 100,
            'interval_hours': interval, 'ts_ms': ts_ms}


def variational_perps(data):
    return {row['ticker'][:-5]: row for row in data.get('listings', [])
            if row.get('contract_type') == 'perpetual' and str(row.get('ticker', '')).endswith('-PERP')}


async def current_funding(session, venue, symbol, *, variational_data=None):
    if venue == 'aster':
        # A base can have USDT, USDC and inverse USD contracts with different
        # intervals. Match the exact contract used by MarketFeed, not the last
        # base-symbol entry in fundingInfo.
        contract = await aster_contract(session, symbol)
        rows = await fetch_json(session, 'GET', 'https://fapi.asterdex.com/fapi/v1/fundingInfo')
        meta = next((r for r in rows if r.get('symbol') == contract['symbol']), None)
        if meta is None:
            return None
        data = await fetch_json(session, 'GET', 'https://fapi.asterdex.com/fapi/v1/premiumIndex',
                                params={'symbol': contract['symbol']})
        rate = data.get('lastFundingRate', data.get('fundingRate'))
        return {'funding_rate': rate, 'interval_hours': meta['fundingIntervalHours']} if rate is not None else None
    if venue == 'variational':
        # v1 funding_rate is ANNUALIZED, not a per-interval rate. v2 explicitly
        # provides interval_rate and distinguishes perpetuals from swaps.
        data = variational_data if variational_data is not None else await fetch_json(session, 'GET', VARIATIONAL_STATS)
        funding = (variational_perps(data).get(symbol) or {}).get('funding') or {}
        rate = (funding.get('last') or {}).get('interval_rate')
        return {'funding_rate': rate, 'interval_hours': float(funding['interval_seconds'])/3600} if rate is not None else None
    module = importlib.import_module(f'hydra_basis.adapters.{venue}')
    fetch = getattr(module, f'fetch_{venue}_current_funding', None)
    return await fetch(session, symbol) if fetch else None


def book_payload(feed, venue):
    book = feed.ticker(venue)
    if book is None:
        return {'venue': venue, 'status': 'waiting', 'bids': [], 'asks': []}
    current = feed.clock()
    fresh = feed.fresh(venue) is not None
    return {'venue': venue, 'status': ('indicative' if venue == 'variational' else 'live') if fresh else 'stale',
            'bid': float(book.bid), 'ask': float(book.ask), 'source_ms': book.source_ms,
            'received_ms': book.received_ms, 'age_ms': current - (book.source_ms or book.received_ms),
            'transport_ms': book.received_ms - book.source_ms if book.source_ms is not None else None,
            'spread_pct': float((book.ask-book.bid)/((book.ask+book.bid)/2)*100),
            'bids': [[float(p), float(q)] for p, q in book.bids],
            'asks': [[float(p), float(q)] for p, q in book.asks]}


def spreads(selection, feed, instruments):
    """Gross percentages. Missing depth is never a size execution guarantee."""
    result = {'taker': {'entry': None, 'exit': None}, 'maker_short': {'entry': None, 'exit': None},
              'maker_long': {'entry': None, 'exit': None}, 'reason': None}
    books = feed.fresh_pair()
    if books is None:
        result['reason'] = '報價等待中或已過期'
        return result
    s, l = books[selection.short], books[selection.long]

    def price(venue, book, side):
        levels = book.asks if side == 'BUY' else book.bids
        if levels:
            return vwap(levels, selection.quantity)
        # Public Variational quotes are indicative; the UI carries this label.
        return (book.ask if side == 'BUY' else book.bid) if venue == 'variational' else None

    sb, sa = price(selection.short, s, 'SELL'), price(selection.short, s, 'BUY')
    lb, la = price(selection.long, l, 'SELL'), price(selection.long, l, 'BUY')

    def pct(a, b):
        return float(spread_bps(a, b)/100) if a is not None and b is not None and b > 0 else None

    def maker(venue, book, side):
        value = book.ask if side == 'SELL' else book.bid
        instrument = instruments.get(venue)
        return instrument.round_price(value, 'up' if side == 'SELL' else 'down') if instrument else value

    result['taker'] = {'entry': pct(sb, la), 'exit': pct(sa, lb)}
    result['maker_short'] = {'entry': pct(maker(selection.short, s, 'SELL'), la),
                             'exit': pct(maker(selection.short, s, 'BUY'), lb)}
    result['maker_long'] = {'entry': pct(sb, maker(selection.long, l, 'BUY')),
                            'exit': pct(sa, maker(selection.long, l, 'SELL'))}
    if None in (sb, sa, lb, la):
        result['reason'] = '部分方向深度不足，無法估算整筆成交價'
    return result


class History:
    def __init__(self, path=DB_PATH):
        self.path = path

    @contextmanager
    def connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=5)
        connection.execute('CREATE TABLE IF NOT EXISTS samples (key TEXT, ts INTEGER, payload TEXT, PRIMARY KEY(key, ts))')
        connection.execute('CREATE INDEX IF NOT EXISTS samples_ts ON samples(ts)')
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def append(self, key, snapshot):
        stamp = snapshot['ts_ms'] // SAMPLE_MS * SAMPLE_MS
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO samples VALUES (?,?,?)',
                       (key, stamp, json.dumps(snapshot, allow_nan=False)))
            db.execute('DELETE FROM samples WHERE ts < ?', (stamp - 7*86400000,))
            # Hard global bound even when many pairs have been visited.
            if stamp % 60000 < SAMPLE_MS:
                db.execute('DELETE FROM samples WHERE rowid IN (SELECT rowid FROM samples ORDER BY ts DESC LIMIT -1 OFFSET ?)', (MAX_ROWS,))

    def read(self, key, since, interval):
        if not self.path.exists():
            return []
        with self.connect() as db:
            rows = db.execute('SELECT ts,payload FROM samples WHERE key=? AND ts>=? ORDER BY ts', (key, since)).fetchall()
        buckets = {}
        for stamp, raw in rows:
            bucket = stamp // interval * interval
            item = json.loads(raw)
            item['ts_ms'] = bucket
            buckets[bucket] = item
        return list(buckets.values())


def strategy_states(selection, directory=DATA_DIR):
    """Read whitelisted ledger fields only; never return remote order responses."""
    groups = []
    for path in list(directory.glob('*.json')) + list((directory/'groups').glob('*.json')):
        try:
            raw = json.loads(path.read_text(encoding='utf-8'))
            if raw.get('identity') != selection.config().identity() or 'strategy_id' not in raw:
                continue
            heartbeat_path = path.with_suffix('.heartbeat.json')
            try:
                heartbeat = json.loads(heartbeat_path.read_text()) if heartbeat_path.exists() else {}
            except (OSError, ValueError):
                heartbeat = {}  # Atomic replacement can briefly deny reads on Windows.
            live = (heartbeat.get('strategy_id') == raw['strategy_id'] and heartbeat.get('running') is True
                    and 0 <= now_ms()-heartbeat.get('ts_ms', 0) < 12000)
            orders = [{k: o.get(k) for k in ('id', 'venue', 'side', 'purpose', 'quantity',
                       'executed_quantity', 'price', 'average_price', 'state', 'updated_ms')}
                      for o in raw.get('orders', [])[-40:]]
            groups.append({'id': raw['strategy_id'], 'mode': raw.get('mode'), 'status': raw.get('status'),
                           'running': live, 'updated_ms': int(path.stat().st_mtime*1000),
                           'heartbeat_ms': heartbeat.get('ts_ms'), 'short': raw.get('short'),
                           'long': raw.get('long'), 'fees': raw.get('fees_usd'), 'orders': orders})
        except (OSError, ValueError, TypeError, KeyError):
            continue
    return groups


class MonitorFeed(MarketFeed):
    def __init__(self, selection, hub):
        super().__init__(selection.config())
        self.hub = hub
        self.sources = {}

    def ticker(self, venue):
        source = self.sources.get(venue)
        return source.feed.ticker(venue) if source else super().ticker(venue)

    def fresh(self, venue):
        source = self.sources.get(venue)
        return source.feed.fresh(venue) if source else super().fresh(venue)

    async def _variational(self, session):
        while True:
            data = await self.hub.variational_stats()
            listing = variational_perps(data).get(self.config.symbol)
            if listing is None:
                raise RuntimeError('Variational perpetual market unavailable')
            mid = Decimal(str(listing['mark_price']))
            # Reuse the size-tier selector and source timestamp parser. Never
            # relabel source-cached quotes with a fresh receive timestamp.
            quote = parse_variational_quote({'listings': [{**listing, 'ticker': self.config.symbol}]},
                self.config.symbol, clip_usd=float(mid*self.config.clip_quantity))
            self.update('variational', quote['bid'], quote['ask'],
                        source_ms=variational_quote_source_ms(listing))
            await asyncio.sleep(5)


class MarketSubscription:
    """One venue/symbol stream, independent of direction and CLOB order size."""
    def __init__(self, selection, venue, hub):
        self.venue, self.hub = venue, hub
        self.feed = MonitorFeed(selection, hub)
        self.instrument, self.funding = None, None
        self.refs = 0
        self.idle_task = None
        self.tasks = [asyncio.create_task(self.feed._supervise(
            venue, getattr(self.feed, f'_{venue}'), hub.session)),
            asyncio.create_task(self.metadata())]

    async def metadata(self):
        venue, symbol = self.venue, self.feed.config.symbol
        try:
            self.instrument = await fetch_instrument(self.hub.session, venue, symbol)
        except Exception:
            pass
        while True:
            try:
                data = await self.hub.variational_stats() if venue == 'variational' else None
                rate = await current_funding(self.hub.session, venue, symbol, variational_data=data)
                self.funding = (rate_payload(rate['funding_rate'], rate['interval_hours'], now_ms())
                                if rate is not None else None)
            except Exception:
                self.funding = None
            await asyncio.sleep(30)

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


class Channel:
    def __init__(self, selection, hub):
        self.selection, self.hub = selection, hub
        self.feed = MonitorFeed(selection, hub)
        self.instruments, self.funding = {}, {}
        self.refs = 0
        self.tasks = []
        self.record_error = None

    async def start(self):
        for venue in self.feed.config.venues:
            self.feed.sources[venue] = self.hub.acquire_market(self.selection, venue)
        self.tasks = [asyncio.create_task(self.record())]

    def snapshot(self):
        self.instruments = {v: source.instrument for v, source in self.feed.sources.items() if source.instrument}
        self.funding = {v: source.funding for v, source in self.feed.sources.items() if source.funding}
        return {'type': 'snapshot', 'ts_ms': now_ms(), 'symbol': self.selection.symbol,
                'short': self.selection.short, 'long': self.selection.long, 'quantity': str(self.selection.quantity),
                'books': {v: book_payload(self.feed, v) for v in self.feed.config.venues},
                'spreads': spreads(self.selection, self.feed, self.instruments),
                'funding': {v: r for v, r in self.funding.items() if now_ms()-r['ts_ms'] <= 60000},
                'record_error': self.record_error}

    async def record(self):
        while True:
            try:
                await asyncio.to_thread(self.hub.history.append, self.selection.key, self.snapshot())
                self.record_error = None
            except Exception:
                self.record_error = '歷史錄製失敗；即時行情仍可查看'
            await asyncio.sleep(SAMPLE_MS/1000)

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for venue, source in self.feed.sources.items():
            self.hub.release_market(self.selection, venue, source)
        self.feed.sources.clear()


class MonitorHub:
    def __init__(self, history=None):
        self.session = None
        self.channels = {}
        self.history = history or History()
        self.references = ReferenceCache(self.history.path.with_name('reference.sqlite3'))
        self.markets = {}
        self.market_idle_seconds = 5
        self.catalog_cache = None
        self.catalog_at = 0
        self.catalog_lock = asyncio.Lock()
        self.funding_cache = None
        self.funding_at = 0
        self.funding_lock = asyncio.Lock()
        self.variational_lock = asyncio.Lock()
        self.variational_cache = None
        self.variational_at = 0
        self.variational_retry_at = 0
        self.variational_failures = 0

    async def variational_stats(self):
        async with self.variational_lock:
            if time.monotonic() < self.variational_retry_at:
                raise RuntimeError('Variational stats retry cooling down')
            if time.monotonic()-self.variational_at < 5:
                if self.variational_cache is None:
                    raise RuntimeError('Variational stats temporarily unavailable')
                return self.variational_cache
            self.variational_at = time.monotonic()
            self.variational_cache = None
            try:
                self.variational_cache = await fetch_json(self.session, 'GET', VARIATIONAL_STATS)
            except Exception as exc:
                self.variational_failures += 1
                delay = min(300, 30 * 2 ** min(self.variational_failures - 1, 4))
                if isinstance(exc, aiohttp.ClientResponseError) and exc.status == 429:
                    delay = max(delay, retry_seconds(exc.headers), getattr(exc, 'retry_after_seconds', 0))
                self.variational_retry_at = time.monotonic() + delay
                raise
            self.variational_failures = 0
            self.variational_retry_at = 0
            self.variational_at = time.monotonic()
            return self.variational_cache

    async def ensure_session(self):
        if self.session is None:
            self.session = MonitorSession(aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)))

    @staticmethod
    def market_key(selection, venue):
        # Variational chooses a public quote tier by size. Share its HTTP stats,
        # but do not reuse one size's selected quote for another size.
        return venue, selection.symbol, selection.quantity if venue == 'variational' else None

    def acquire_market(self, selection, venue):
        key = self.market_key(selection, venue)
        source = self.markets.get(key)
        if source is None:
            source = self.markets[key] = MarketSubscription(selection, venue, self)
        if source.idle_task:
            source.idle_task.cancel()
            source.idle_task = None
        source.refs += 1
        return source

    def release_market(self, selection, venue, source):
        source.refs -= 1
        if source.refs:
            return
        key = self.market_key(selection, venue)
        async def expire():
            await asyncio.sleep(self.market_idle_seconds)
            if source.refs == 0 and self.markets.get(key) is source:
                self.markets.pop(key)
                await source.close()
        source.idle_task = asyncio.create_task(expire())

    async def catalog(self):
        async with self.catalog_lock:
            if self.catalog_cache is not None and time.monotonic()-self.catalog_at < 300:
                return self.catalog_cache
            await self.ensure_session()
            async def one(venue):
                try:
                    if venue == 'aster':
                        data = await fetch_json(self.session, 'GET', 'https://fapi.asterdex.com/fapi/v1/exchangeInfo')
                        symbols = {r['baseAsset'] for r in data['symbols']
                                   if r.get('status') == 'TRADING' and r.get('contractType') == 'PERPETUAL'
                                   and r.get('quoteAsset') in {'USDT', 'USDC', 'USD'}}
                    elif venue == 'variational':
                        symbols = set(variational_perps(await self.variational_stats()))
                    else:
                        module = importlib.import_module(f'hydra_basis.adapters.{venue}')
                        symbols = await module.list_symbols(self.session)
                    symbols = sorted(s for s in symbols if canonicalize_symbol(s, venue=venue) == s)
                    return {'id': venue, 'name': VENUES[venue], 'symbols': symbols, 'available': True}
                except Exception:
                    return {'id': venue, 'name': VENUES[venue], 'symbols': [], 'available': False}
            self.catalog_cache = {'venues': await asyncio.gather(*(one(v) for v in VENUES))}
            self.catalog_at = time.monotonic()
            return self.catalog_cache

    @asynccontextmanager
    async def subscribe(self, selection):
        catalog = await self.catalog()
        symbols = {v['id']: set(v['symbols']) for v in catalog['venues']}
        if selection.symbol not in symbols[selection.short] & symbols[selection.long]:
            raise ValueError('目前無法確認兩所共同支援此交易對，請重試或選擇其他交易對')
        channel = self.channels.get(selection.key)
        if channel is None:
            if len(self.channels) >= 8:
                raise ValueError('已達 8 組行情訂閱上限，請關閉不需要的監控頁')
            channel = self.channels[selection.key] = Channel(selection, self)
            await channel.start()
        channel.refs += 1
        try:
            yield channel
        finally:
            channel.refs -= 1
            if channel.refs == 0:
                self.channels.pop(selection.key, None)
                await channel.close()

    async def funding_history(self, selection, days):
        async with self.funding_lock:
            if self.funding_cache is None or time.monotonic()-self.funding_at > 30:
                try:
                    self.funding_cache = await asyncio.to_thread(load_points)
                except (OSError, ValueError):
                    self.funding_cache = {}
                self.funding_at = time.monotonic()
        cutoff = now_ms()-days*86400000
        result = {}
        for venue in (selection.short, selection.long):
            rows = []
            for point in self.funding_cache.get((venue, selection.symbol), []):
                if cutoff <= point.ts_ms <= now_ms():
                    try:
                        rows.append(rate_payload(point.raw_rate, point.interval_hours, point.ts_ms))
                    except ValueError:
                        continue
            result[venue] = sorted(rows, key=lambda r: r['ts_ms'])
        return {'series': result, 'unit': '%/小時', 'source': '本地資金費率紀錄'}

    async def reference(self, selection, hours):
        await self.ensure_session()
        end = now_ms() // 60000 * 60000
        start = end-min(hours, 24)*3600000
        async def one(venue):
            try:
                async def fetch(v, symbol, first, last):
                    return await fetch_minute_closes(self.session, v, symbol, first, last)
                values = await self.references.get(venue, selection.symbol, start, end, fetch)
                if values is None:
                    recorder = await asyncio.to_thread(MinuteRecorder, DATA_DIR/'minute_mids.json.gz')
                    values = recorder.get(venue, selection.symbol)
                return {t: float(p) for t, p in values.items() if start <= t < end and p > 0}
            except Exception:
                return {}
        short, long = await asyncio.gather(one(selection.short), one(selection.long))
        return {'source': '分鐘收盤／中間價，非可成交價差',
                'series': [{'ts_ms': t, 'reference': (short[t]-long[t])/long[t]*100}
                           for t in sorted(short.keys() & long.keys())]}

    async def close(self):
        await asyncio.gather(*(c.close() for c in list(self.channels.values())))
        self.channels.clear()
        sources = list(self.markets.values())
        idle = [source.idle_task for source in sources if source.idle_task]
        for task in idle:
            task.cancel()
        await asyncio.gather(*idle, return_exceptions=True)
        await asyncio.gather(*(source.close() for source in sources))
        self.markets.clear()
        if self.session is not None:
            await self.session.close()
            self.session = None


def local_socket(websocket):
    host = websocket.headers.get('host', '').split(':')[0]
    origin = websocket.headers.get('origin')
    return host in {'localhost', '127.0.0.1', 'testserver'} and (
        not origin or urlsplit(origin).netloc == websocket.headers.get('host'))


def router(hub):
    routes = APIRouter(prefix='/api/monitor')

    def select(symbol, short, long, quantity):
        try:
            return Selection.parse(symbol, short, long, quantity)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @routes.get('/catalog')
    async def catalog(refresh: bool = False):
        if refresh:
            hub.catalog_at = 0
        return await hub.catalog()

    @routes.get('/history')
    async def history(symbol: str, short: str, long: str, quantity: str = '.01', hours: int = 24, interval: int = 60):
        selection = select(symbol, short, long, quantity)
        if hours not in (1, 6, 24, 72, 168) or interval not in (5, 30, 60, 300, 900, 3600):
            raise HTTPException(400, '不支援的歷史範圍或資料間隔')
        interval = max(interval, math.ceil(hours*3600/6000/5)*5)
        rows = await asyncio.to_thread(hub.history.read, selection.key, now_ms()-hours*3600000, interval*1000)
        return {'series': rows, 'source': '已錄製的實際報價', 'sample_seconds': 5, 'interval_seconds': interval}

    @routes.get('/reference')
    async def reference(symbol: str, short: str, long: str, hours: int = 24):
        if hours not in (1, 6, 24):
            raise HTTPException(400, '參考價差最多提供最近 24 小時')
        return await hub.reference(select(symbol, short, long, '.01'), hours)

    @routes.get('/funding')
    async def funding(symbol: str, short: str, long: str, days: int = 7):
        if not 1 <= days <= 30:
            raise HTTPException(400, '天數必須介於 1 至 30')
        return await hub.funding_history(select(symbol, short, long, '.01'), days)

    @routes.get('/strategies')
    async def strategies(symbol: str, short: str, long: str):
        return {'groups': await asyncio.to_thread(strategy_states, select(symbol, short, long, '.01'))}

    @routes.websocket('/stream')
    async def stream(ws: WebSocket):
        if not local_socket(ws):
            await ws.close(code=1008)
            return
        await ws.accept()
        try:
            selection = Selection.parse(**dict(ws.query_params))
            async with hub.subscribe(selection) as channel:
                # Receive concurrently so disconnected clients release subscriptions
                # even when the browser does not send another application message.
                receive = asyncio.create_task(ws.receive())
                try:
                    while not receive.done():
                        await ws.send_json(channel.snapshot())
                        await asyncio.wait({receive}, timeout=.25)
                finally:
                    receive.cancel()
                    await asyncio.gather(receive, return_exceptions=True)
        except (WebSocketDisconnect, RuntimeError):
            pass
        except (ValueError, TypeError):
            await ws.send_json({'type': 'error', 'message': '無法訂閱這個配對，請檢查共同市場及觀察數量'})
            await ws.close(code=1008)
    return routes
