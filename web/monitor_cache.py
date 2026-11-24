"""Persistent closed-minute prices and shared request budgets for the local UI."""
from __future__ import annotations

import asyncio
import math
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import aiohttp

MINUTE = 60_000


class ReferenceCache:
    def __init__(self, path, *, clock=lambda: int(time.time() * 1000)):
        self.path, self.clock = path, clock
        self.locks = {}
        self.unsupported = {}
        self.retry_at = {}

    @contextmanager
    def connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=10)
        try:
            with db:
                db.execute('CREATE TABLE IF NOT EXISTS candles '
                           '(venue TEXT, symbol TEXT, ts INTEGER, price REAL, checked INTEGER, '
                           'PRIMARY KEY(venue,symbol,ts))')
                db.execute('CREATE INDEX IF NOT EXISTS candle_ts ON candles(ts)')
                yield db
        finally:
            db.close()

    def read(self, venue, symbol, start, end):
        with self.connect() as db:
            return {t: (p, checked) for t, p, checked in db.execute(
                'SELECT ts,price,checked FROM candles WHERE venue=? AND symbol=? AND ts>=? AND ts<?',
                (venue, symbol, start, end))}

    def save(self, venue, symbol, start, end, prices, checked):
        rows = []
        for stamp in range(start, end, MINUTE):
            try:
                price = float(prices[stamp])
                if not math.isfinite(price) or price <= 0:
                    price = None
            except (KeyError, TypeError, ValueError, OverflowError):
                price = None
            rows.append((venue, symbol, stamp, price, checked))
        with self.connect() as db:
            db.executemany('INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?)', rows)
            db.execute('DELETE FROM candles WHERE ts<?', (checked - 7 * 86400000,))
            db.execute('DELETE FROM candles WHERE rowid IN '
                       '(SELECT rowid FROM candles ORDER BY ts DESC LIMIT -1 OFFSET 150000)')

    async def get(self, venue, symbol, start, end, fetcher):
        """Fetch missing closed minutes, never direction/size-dependent data.

        Null rows remember an empty response for five minutes (one minute at the
        live edge). They never become synthetic candles. Network failures use a
        separate short cooldown and never mark a missing range as downloaded.
        """
        key = venue, symbol
        async with self.locks.setdefault(key, asyncio.Lock()):
            now = self.clock()
            end = min(end, now // MINUTE * MINUTE)
            start = (start + MINUTE - 1) // MINUTE * MINUTE
            if self.unsupported.get(key, 0) > now:
                return None
            rows = await asyncio.to_thread(self.read, venue, symbol, start, end)
            missing = []
            for stamp in range(start, end, MINUTE):
                row = rows.get(stamp)
                ttl = MINUTE if stamp >= now - 2 * MINUTE else 5 * MINUTE
                if row is None or (row[0] is None and now - row[1] >= ttl):
                    missing.append(stamp)
            ranges = []
            for stamp in missing:
                if ranges and stamp == ranges[-1][1]:
                    ranges[-1][1] += MINUTE
                else:
                    ranges.append([stamp, stamp + MINUTE])
            # A fragmented source cannot trigger hundreds of calls in one view.
            for first, last in ranges[:4]:
                if self.retry_at.get(key, 0) > now:
                    break
                try:
                    prices = await fetcher(venue, symbol, first, last - 1)
                except Exception:
                    self.retry_at[key] = now + MINUTE
                    break
                if prices is None:
                    self.unsupported[key] = now + 5 * MINUTE
                    return None
                await asyncio.to_thread(self.save, venue, symbol, first, last, prices, now)
            rows = await asyncio.to_thread(self.read, venue, symbol, start, end)
            return {stamp: row[0] for stamp, row in rows.items() if row[0] is not None}


class MonitorRateLimited(aiohttp.ClientResponseError):
    retry_after_seconds = 0

    def __str__(self):
        return f'{self.status}: {self.message}'


def retry_seconds(headers, payload=None, *, wall_time=None):
    """Accept both standard Retry-After formats and Variational's JSON field."""
    values = [0.0]
    raw = (headers or {}).get('Retry-After')
    try:
        value = float(raw)
    except (TypeError, ValueError):
        try:
            value = parsedate_to_datetime(raw).timestamp() - (time.time() if wall_time is None else wall_time)
        except (TypeError, ValueError, OverflowError):
            value = 0
    if math.isfinite(value):
        values.append(value)
    if isinstance(payload, dict):
        try:
            value = float(payload.get('retry_after_seconds', 0))
            if math.isfinite(value):
                values.append(value)
        except (TypeError, ValueError):
            pass
    return max(values)


class MonitorSession:
    """Monitor-only HTTP pacing; does not touch trading clients or credentials.

    Serialize REST calls per host, at least one second apart. Hyperliquid
    additionally reserves estimated weights at ten units/second. A 429 cooldown
    covers catalog, funding and candle calls together. WS handshakes have a
    separate three-second spacing and an eight-connection cap per host.
    """
    def __init__(self, raw, *, clock=time.monotonic, sleep=asyncio.sleep):
        self.raw, self.clock, self.sleep = raw, clock, sleep
        self.locks, self.next_at, self.cooldown, self.failures = {}, {}, {}, {}
        self.ws_locks, self.ws_next, self.ws_slots = {}, {}, {}

    @staticmethod
    def request_spacing(host, kwargs):
        if host != 'api.hyperliquid.xyz':
            return 1
        body = kwargs.get('json') or {}
        kind = body.get('type') if isinstance(body, dict) else None
        weight = 2 if kind in {'l2Book', 'allMids', 'clearinghouseState', 'orderStatus',
                              'spotClearinghouseState', 'exchangeStatus'} else 20
        if kind == 'candleSnapshot':
            req = body.get('req') or {}
            count = 5000
            interval_minutes = {'1m': 1, '3m': 3, '5m': 5, '15m': 15}.get(req.get('interval'))
            if interval_minutes is not None:
                count = min(5000, max(1, math.ceil((req.get('endTime', 0) - req.get('startTime', 0)) / (MINUTE*interval_minutes))))
            weight += math.ceil(count / 60)
        elif kind in {'fundingHistory', 'userFills', 'userFillsByTime'}:
            weight += 25  # Reserve for the documented maximum history page.
        return max(1, weight / 10)

    @asynccontextmanager
    async def request(self, method, url, **kwargs):
        host = urlsplit(str(url)).netloc
        async with self.locks.setdefault(host, asyncio.Lock()):
            remaining = self.cooldown.get(host, 0) - self.clock()
            if remaining > 0:
                exc = MonitorRateLimited(None, (), status=429, message='Monitor request cooldown')
                exc.retry_after_seconds = remaining
                raise exc
            delay = self.next_at.get(host, 0) - self.clock()
            if delay > 0:
                await self.sleep(delay)
            self.next_at[host] = self.clock() + self.request_spacing(host, kwargs)
            async with self.raw.request(method, url, **kwargs) as response:
                if response.status in (418, 429) or response.status >= 500:
                    self.failures[host] = self.failures.get(host, 0) + 1
                    payload = None
                    try:
                        payload = await response.json(content_type=None)
                    except (ValueError, aiohttp.ClientError, asyncio.TimeoutError):
                        pass
                    delay = max(retry_seconds(response.headers, payload),
                                min(300, 30 * 2 ** min(self.failures[host] - 1, 4)))
                    self.cooldown[host] = self.clock() + delay
                    exc = MonitorRateLimited(response.request_info, response.history,
                        status=response.status, message='Monitor upstream temporarily unavailable', headers=response.headers)
                    exc.retry_after_seconds = delay
                    raise exc
                self.failures.pop(host, None)
                yield response

    @asynccontextmanager
    async def ws_connect(self, url, **kwargs):
        host = urlsplit(str(url)).netloc
        async with self.ws_slots.setdefault(host, asyncio.Semaphore(8)):
            async with self.ws_locks.setdefault(host, asyncio.Lock()):
                delay = self.ws_next.get(host, 0) - self.clock()
                if delay > 0:
                    await self.sleep(delay)
                self.ws_next[host] = self.clock() + 3
                context = self.raw.ws_connect(url, **kwargs)
                ws = await context.__aenter__()
            try:
                yield ws
            finally:
                await context.__aexit__(None, None, None)

    async def close(self):
        await self.raw.close()

    @property
    def closed(self):
        return self.raw.closed
