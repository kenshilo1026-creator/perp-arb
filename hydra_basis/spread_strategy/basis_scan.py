"""Read-only historical perp/Binance Spot range scan; never an execution backtest."""
from __future__ import annotations

import importlib
import json
import math
import sqlite3
import time
from pathlib import Path

from hydra_basis.config import VENUE_CONFIG
from hydra_basis.spread_strategy.history import MinuteRecorder, _json, fetch_minute_closes
from hydra_basis.symbol_mapping import canonicalize_symbol
from web.binance_market import markets as spot_markets

MINUTE = 60_000
DAY = 1440 * MINUTE
SUPPORTED = ('aster', 'hyperliquid', 'lighter', 'variational', 'entropy', 'trade_xyz', 'arcus', 'ondo', 'mexc')
LOCAL = {'lighter', 'variational', 'ondo'}


def enabled_venues():
    # Configuration is authoritative. Never silently intersect with adapters:
    # a newly enabled venue without history support must appear in the report.
    return [venue for venue, config in VENUE_CONFIG.items() if config.enabled]


def resolution(days: int) -> int:
    if not 1 <= days <= 30:
        raise ValueError('days must be between 1 and 30')
    # Leave headroom under Hyperliquid's latest-5000-candle retention.
    return 1 if days <= 3 else 5 if days <= 15 else 15


def analyse(symbol, venue, perp, spot, start, end, step, min_coverage=70):
    points = []
    for ts in sorted(perp.keys() & spot.keys()):
        p, s = float(perp[ts]), float(spot[ts])
        if start <= ts < end and ts % step == 0 and all(math.isfinite(x) and x > 0 for x in (p, s)):
            points.append((ts, (p-s)/s*100, p, s))
    coverage = len(points) / ((end-start)//step) * 100
    if len(points) < 2 or coverage < min_coverage:
        return None, {'symbol': symbol, 'venue': venue, 'reason': 'insufficient_history',
                      'points': len(points), 'coverage_pct': coverage}
    low = min(points, key=lambda x: x[1])
    high = max(points, key=lambda x: x[1])
    peak = points[0]
    convergence, entry, exit_ = 0.0, None, None
    for point in points[1:]:
        drop = peak[1] - point[1]
        if drop > convergence:
            convergence, entry, exit_ = drop, peak, point
        if point[1] > peak[1]:
            peak = point
    # Exact same-token quantity PnL for that historical convergence pair,
    # normalized by entry spot notional. Different from subtracting percentages.
    gross = None if entry is None else ((entry[2]-entry[3])-(exit_[2]-exit_[3]))/entry[3]*100
    return dict(symbol=symbol, venue=venue, spot_venue='binance_spot',
                max_gap_pct=high[1], min_gap_pct=low[1], range_pp=high[1]-low[1],
                max_at_ms=high[0], min_at_ms=low[0], latest_gap_pct=points[-1][1],
                latest_at_ms=points[-1][0], coverage_pct=coverage, points=len(points),
                chronological_convergence_pp=convergence,
                convergence_entry_ms=None if entry is None else entry[0],
                convergence_exit_ms=None if exit_ is None else exit_[0],
                convergence_pair_gross_return_pct=gross), None


def ranked(rows, top=10):
    return sorted(rows, key=lambda r: (-r['range_pp'], r['symbol'], r['venue']))[:top]


class CandleCache:
    """Fixed-page cache: complete closed pages persist; empty/gappy pages retry after 5m."""
    def __init__(self, path, clock=lambda: int(time.time()*1000)):
        self.path, self.clock = Path(path), clock
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS pages (key TEXT PRIMARY KEY, stop INTEGER, checked INTEGER, complete INTEGER, payload TEXT)')
            db.execute('DELETE FROM pages WHERE stop < ?', (self.clock()-31*DAY,))
            db.commit()

    def connect(self):
        # Used synchronously for small page payloads by this single-owner CLI.
        from contextlib import closing
        return closing(sqlite3.connect(self.path, timeout=10))

    def read(self, key):
        with self.connect() as db:
            row = db.execute('SELECT checked,complete,payload FROM pages WHERE key=?', (key,)).fetchone()
        if row and (row[1] or self.clock()-row[0] < 5*MINUTE):
            return {int(t): p for t, p in json.loads(row[2]).items()}
        return None

    def save(self, key, stop, complete, values):
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO pages VALUES (?,?,?,?,?)',
                       (key, stop, self.clock(), int(complete), json.dumps(values, allow_nan=False)))
            db.commit()


class PublicHistory:
    def __init__(self, session, cache, *, recorder_path=Path('data/spread_strategies/minute_mids.json.gz')):
        self.session, self.cache = session, cache
        self.recorder = MinuteRecorder(recorder_path)
        self.native = {}
        self.quote = {}

    async def catalog(self, venue):
        if venue == 'binance_spot':
            data = await _json(self.session, 'GET', 'https://api.binance.com/api/v3/exchangeInfo')
            mapping = {s: r['symbol'] for s, r in spot_markets(data).items()}
            quotes = {s: 'USDT' for s in mapping}
        elif venue == 'aster':
            data = await _json(self.session, 'GET', 'https://fapi.asterdex.com/fapi/v1/exchangeInfo')
            # Match execution's quote preference; select one linear perpetual per base.
            from hydra_basis.execution_engine.aster_adapter import ASTER_EXECUTION_SUFFIXES
            mapping, quotes = {}, {}
            for suffix in reversed(ASTER_EXECUTION_SUFFIXES):
                for row in data.get('symbols', []):
                    base = row.get('baseAsset', '')
                    if (row.get('status') == 'TRADING' and row.get('contractType') == 'PERPETUAL'
                            and row.get('quoteAsset') in {'USDT', 'USDC'} and row.get('symbol') == base+suffix):
                        mapping[base], quotes[base] = row['symbol'], row['quoteAsset']
        elif venue in {'hyperliquid', 'entropy', 'trade_xyz'}:
            dex = {'entropy': 'io', 'trade_xyz': 'xyz'}.get(venue)
            body = {'type': 'meta', **({'dex': dex} if dex else {})}
            data = await _json(self.session, 'POST', 'https://api.hyperliquid.xyz/info', json=body)
            mapping = {r['name'].split(':')[-1].upper(): r['name'] for r in data.get('universe', [])
                       if not r.get('isDelisted')}
            quotes = {s: 'USDC' if venue == 'hyperliquid' else 'HIP-3 collateral (not FX adjusted)' for s in mapping}
        else:
            module = importlib.import_module(f'hydra_basis.adapters.{venue}')
            mapping = {s: s for s in await module.list_symbols(self.session)}
            quotes = {s: 'venue native (not FX adjusted)' for s in mapping}
        mapping = {s: n for s, n in mapping.items() if s.isascii() and s.isalnum()
                   and canonicalize_symbol(s, venue=venue) == s}
        self.native[venue], self.quote[venue] = mapping, quotes
        return set(mapping)

    async def page(self, venue, symbol, first, stop, minutes):
        step = minutes*MINUTE
        native = self.native[venue][symbol]
        if venue in {'hyperliquid', 'entropy', 'trade_xyz'}:
            rows = await _json(self.session, 'POST', 'https://api.hyperliquid.xyz/info', json={
                'type': 'candleSnapshot', 'req': {'coin': native, 'interval': f'{minutes}m',
                                                'startTime': first, 'endTime': stop-1}})
            pairs = ((int(r['t']), float(r['c'])) for r in rows or [])
        elif venue in {'binance_spot', 'aster'}:
            base, path = ('https://api.binance.com', '/api/v3/klines') if venue == 'binance_spot' else (
                'https://fapi.asterdex.com', '/fapi/v1/klines')
            rows = await _json(self.session, 'GET', base+path, params={
                'symbol': native, 'interval': f'{minutes}m', 'startTime': first, 'endTime': stop-1, 'limit': 500})
            pairs = ((int(r[0]), float(r[4])) for r in rows if int(r[6]) < stop)
        else:  # Arcus/MEXC: page minute candles, then take the last minute of each bar.
            values = {}
            for cursor in range(first, stop, 500*MINUTE):
                values.update(await fetch_minute_closes(self.session, venue, symbol, cursor, min(stop, cursor+500*MINUTE)-1) or {})
            pairs = ((t-(step-MINUTE), p) for t, p in values.items() if (t+MINUTE) % step == 0)
        return {t: p for t, p in pairs if first <= t < stop and t % step == 0 and math.isfinite(p) and p > 0}

    async def series(self, venue, symbol, start, end, minutes):
        step = minutes*MINUTE
        if venue in LOCAL:
            values = self.recorder.get(venue, symbol)
            return {t-(step-MINUTE): p for t, p in values.items()
                    if (t+MINUTE) % step == 0 and start <= t-(step-MINUTE) < end}
        result = {}
        width = 500*step
        for cursor in range(start//width*width, end, width):
            first, stop = max(start, cursor), min(end, cursor+width)
            key = json.dumps([venue, self.native[venue][symbol], minutes, first, stop])
            values = self.cache.read(key)
            if values is None:
                values = await self.page(venue, symbol, first, stop, minutes)
                self.cache.save(key, stop, len(values) == (stop-first)//step, values)
            result.update(values)
        return result


async def scan(source, *, days=1, venues=None, symbols=None, top=10, min_coverage=70,
               now_ms=None, progress=lambda message: None):
    minutes = resolution(days)
    if not 0 <= min_coverage <= 100 or not 1 <= top <= 100:
        raise ValueError('invalid coverage or top')
    venues = enabled_venues() if venues is None else list(dict.fromkeys(venues))
    if not venues:
        raise ValueError('no enabled perp venues')
    step = minutes*MINUTE
    end = (int(time.time()*1000) if now_ms is None else now_ms)//step*step
    start = end-days*DAY
    skipped, rows, catalogs = [], [], {}
    spot_symbols = await source.catalog('binance_spot')
    for venue in dict.fromkeys(venues):
        if venue not in SUPPORTED:
            skipped.append(dict(venue=venue, reason='history_adapter_not_supported'))
            progress(f'[catalog] {venue}: history_adapter_not_supported')
            continue
        try:
            shared = spot_symbols & await source.catalog(venue)
            if symbols is not None:
                shared &= set(symbols)
            catalogs[venue] = shared
            if not shared:
                skipped.append(dict(venue=venue, reason='no_common_symbols'))
            progress(f'[catalog] {venue}: {len(shared)} common symbols')
        except Exception as exc:
            skipped.append(dict(venue=venue, reason='catalog_failed', error=type(exc).__name__))
            progress(f'[catalog] {venue}: failed ({type(exc).__name__})')
    all_symbols = sorted(set().union(*catalogs.values())) if catalogs else []
    for index, symbol in enumerate(all_symbols, 1):
        eligible = [v for v, available in catalogs.items() if symbol in available]
        # No history endpoint: don't spend spot API quota on an empty local series.
        histories = {}
        for venue in eligible:
            if venue in LOCAL:
                try:
                    histories[venue] = await source.series(venue, symbol, start, end, minutes)
                except Exception as exc:
                    histories[venue] = {}
                    skipped.append(dict(venue=venue, symbol=symbol, reason='recorded_history_failed', error=type(exc).__name__))
                    continue
                if not histories[venue]:
                    skipped.append(dict(venue=venue, symbol=symbol, reason='no_recorded_history'))
        eligible = [v for v in eligible if v not in LOCAL or histories[v]]
        if not eligible:
            continue
        try:
            spot = await source.series('binance_spot', symbol, start, end, minutes)
        except Exception as exc:
            for venue in eligible:
                skipped.append(dict(venue=venue, symbol=symbol, reason='spot_fetch_failed', error=type(exc).__name__))
            progress(f'[scan {index}/{len(all_symbols)}] {symbol}: spot fetch failed')
            continue
        for venue in eligible:
            try:
                perp = histories[venue] if venue in histories else await source.series(venue, symbol, start, end, minutes)
                row, failure = analyse(symbol, venue, perp, spot, start, end, step, min_coverage)
                if row:
                    row['perp_quote'] = source.quote[venue].get(symbol, 'unknown')
                    row['source'] = 'recorded_mid' if venue in LOCAL else 'candle_close'
                    rows.append(row)
                else:
                    skipped.append(failure)
            except Exception as exc:
                skipped.append(dict(venue=venue, symbol=symbol, reason='perp_fetch_failed', error=type(exc).__name__))
        progress(f'[scan {index}/{len(all_symbols)}] {symbol}: ranked={len(rows)} skipped={len(skipped)}')
    return dict(days=days, start_ms=start, end_ms=end, interval_minutes=minutes,
                formula='(perp_close - spot_close) / spot_close * 100',
                ranking='max_gap_pct - min_gap_pct (percentage points, not profit)',
                min_coverage_pct=min_coverage, venues=list(venues), requested_symbols=symbols,
                eligible_pairs=sum(len(s) for s in catalogs.values()), ranked_pairs=len(rows),
                top=ranked(rows, top), rows=ranked(rows, len(rows)), skipped=skipped,
                limitations=['Historical close/mid reference, not executable bid/ask or a backtest.',
                             'No fees, slippage, funding or stablecoin FX adjustment; extrema may be outliers.',
                             'Only currently listed shared symbols; missing data is never forward-filled.'])
