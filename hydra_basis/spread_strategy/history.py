"""24-hour spread history: was a spread there all day, or did it just appear?

Per-minute prices come from each venue's 1-minute candles (close of the minute).
Lighter's candle API is not public and Variational has none, so for those the
dispatcher records its own per-minute mid prices while it runs; their history
fills in over the first day.

The spread here is close/mid based: (short - long) / long. The tradable entry
spread (short bid - long ask) is lower by about half of each book's bid/ask.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import statistics
import time
from dataclasses import dataclass
from pathlib import Path

import aiohttp


MINUTE_MS = 60_000
FORWARD_FILL_MINUTES = 5          # a quiet market keeps its last price this long
CANDLE_CACHE_SECONDS = 300
# Venues whose mids are recorded locally: no public candles (Lighter, Variational) or candles
# that need an API key (Ondo, used when ONDO_API_KEY_ID/SECRET are set).
RECORDED_VENUES = {"lighter", "variational", "ondo"}

LABELS = {
    "insufficient": "資料不足",
    "persistent": "持續型",
    "reverting": "反覆收斂型",
    "new_spike": "瞬間型",
    "intermittent": "間歇型",
}


# ---------------------------------------------------------------------------
# Per-minute prices
# ---------------------------------------------------------------------------

async def _json(session, method: str, url: str, **kwargs):
    async with session.request(method, url, timeout=aiohttp.ClientTimeout(total=20), **kwargs) as resp:
        resp.raise_for_status()
        return await resp.json()


async def fetch_minute_closes(session, venue: str, symbol: str, start_ms: int, end_ms: int) -> dict[int, float] | None:
    """Minute open-time (ms) -> close price; None when the venue has no public candles."""
    symbol = symbol.upper()
    if venue in ("hyperliquid", "entropy"):
        coin = f"io:{symbol}" if venue == "entropy" else symbol
        rows = await _json(session, "POST", "https://api.hyperliquid.xyz/info", json={
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": "1m", "startTime": start_ms, "endTime": end_ms}})
        return {int(row["t"]): float(row["c"]) for row in rows or []}
    if venue == "aster":
        from hydra_basis.spread_strategy.instruments import aster_contract
        contract = await aster_contract(session, symbol)
        rows = await _json(session, "GET", "https://fapi.asterdex.com/fapi/v1/klines", params={
            "symbol": contract["symbol"], "interval": "1m", "startTime": start_ms, "endTime": end_ms, "limit": 1500})
        return {int(row[0]): float(row[4]) for row in rows or []}
    if venue == "mexc":
        from hydra_basis.adapters.mexc import mexc_contract_symbol
        data = await _json(session, "GET", f"https://contract.mexc.com/api/v1/contract/kline/{mexc_contract_symbol(symbol)}",
                           params={"interval": "Min1", "start": start_ms // 1000, "end": end_ms // 1000})
        rows = data.get("data") or {}
        return {int(t) * 1000: float(c) for t, c in zip(rows.get("time") or [], rows.get("close") or [])}
    if venue == "ondo":
        from hydra_basis.adapters.ondo import fetch_ondo_minute_closes
        return await fetch_ondo_minute_closes(session, symbol, start_ms, end_ms)
    if venue == "arcus":
        from hydra_basis.adapters.arcus import arcus_base_url, arcus_market_name
        minutes = max(1, min(1500, (end_ms - start_ms) // MINUTE_MS))
        data = await _json(session, "GET", f"{arcus_base_url()}/v1/candles", params={
            "market": arcus_market_name(symbol), "timeframe": "1m", "to": end_ms * 1000, "countback": minutes})
        return {int(row["openTime"]) // 1000: float(row["close"]) for row in data.get("candles") or []}
    return None


class MinuteRecorder:
    """Last mid price per minute for venues without public candles, kept for 24 hours."""

    def __init__(self, path: Path | None, *, keep_minutes: int = 1440):
        self.path, self.keep_minutes = path, keep_minutes
        self.series: dict[str, dict[int, float]] = {}
        self._saved_minute = 0
        if path is not None and path.exists():
            try:
                raw = json.loads(gzip.decompress(path.read_bytes()))
                self.series = {key: {int(m): float(p) for m, p in values.items()} for key, values in raw.items()}
            except (OSError, ValueError):
                self.series = {}  # history is advisory: a damaged file only costs coverage

    @staticmethod
    def key(venue: str, symbol: str) -> str:
        return f"{venue}:{symbol.upper()}"

    def record(self, venue: str, symbol: str, bid: float, ask: float, now_ms: int):
        minute = now_ms - now_ms % MINUTE_MS
        self.series.setdefault(self.key(venue, symbol), {})[minute] = (float(bid) + float(ask)) / 2

    def get(self, venue: str, symbol: str) -> dict[int, float]:
        return self.series.get(self.key(venue, symbol), {})

    def prune(self, now_ms: int):
        cutoff = now_ms - self.keep_minutes * MINUTE_MS
        for key in list(self.series):
            kept = {m: p for m, p in self.series[key].items() if m >= cutoff}
            if kept:
                self.series[key] = kept
            else:
                del self.series[key]

    def save(self, now_ms: int, *, force: bool = False):
        minute = now_ms // MINUTE_MS
        if self.path is None or (not force and minute - self._saved_minute < 5):
            return
        self.prune(now_ms)
        merged = dict(self.series)
        if self.path.exists():  # another dispatcher process may have recorded too
            try:
                for key, values in json.loads(gzip.decompress(self.path.read_bytes())).items():
                    merged[key] = {**{int(m): float(p) for m, p in values.items()}, **merged.get(key, {})}
            except (OSError, ValueError):
                pass
        payload = gzip.compress(json.dumps({k: {str(m): p for m, p in v.items()} for k, v in merged.items()}).encode())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_bytes(payload)
        tmp.replace(self.path)
        self._saved_minute = minute


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SpreadProfile:
    label: str
    coverage_pct: float          # minutes in the window with both prices
    median_bps: float | None
    p90_bps: float | None
    max_bps: float | None
    above_pct: float             # share of covered minutes at or above the threshold
    current_episode_minutes: int  # consecutive minutes at or above the threshold, up to now
    episodes: int                # separate runs at or above the threshold
    converged_episodes: int      # runs later followed by spread <= take profit
    median_minutes_to_converge: float | None
    minutes_since_converged: int | None  # since spread was last <= take profit
    sources: str = ""

    @property
    def label_text(self) -> str:
        return LABELS[self.label]


def _filled(series: dict[int, float], minutes: list[int]) -> list[float | None]:
    out, last, last_minute = [], None, None
    for minute in minutes:
        if minute in series:
            last, last_minute = series[minute], minute
        elif last_minute is not None and minute - last_minute > FORWARD_FILL_MINUTES * MINUTE_MS:
            last = None
        out.append(last)
    return out


def build_profile(short: dict[int, float], long: dict[int, float], *, now_ms: int, lookback_minutes: int,
                  threshold_bps: float, take_profit_bps: float, persistent_pct: float,
                  min_coverage_pct: float, sources: str = "") -> SpreadProfile:
    end = now_ms - now_ms % MINUTE_MS
    minutes = [end - (lookback_minutes - i) * MINUTE_MS for i in range(lookback_minutes)]
    spreads = [None if s is None or l is None or l <= 0 else (s - l) / l * 10_000
               for s, l in zip(_filled(short, minutes), _filled(long, minutes))]
    covered = [value for value in spreads if value is not None]
    coverage = len(covered) / lookback_minutes * 100
    if not covered:
        return SpreadProfile("insufficient", 0.0, None, None, None, 0.0, 0, 0, 0, None, None, sources)

    above = [value is not None and value >= threshold_bps for value in spreads]
    episodes, converge_times, start = 0, [], None
    for index, (is_above, value) in enumerate(zip(above, spreads)):
        if is_above and start is None:
            start = index
            episodes += 1
            # first later minute at or below take profit
            later = next((j for j in range(index + 1, len(spreads))
                          if spreads[j] is not None and spreads[j] <= take_profit_bps), None)
            if later is not None:
                converge_times.append(later - index)
        elif not is_above and value is not None:
            start = None
    current = 0
    for is_above, value in zip(reversed(above), reversed(spreads)):
        if value is None:
            continue
        if not is_above:
            break
        current += 1
    since = next((i for i, value in enumerate(reversed(spreads)) if value is not None and value <= take_profit_bps), None)
    ordered = sorted(covered)
    above_pct = sum(above) / len(covered) * 100
    profile = dict(
        coverage_pct=round(coverage, 1), median_bps=statistics.median(covered),
        p90_bps=ordered[min(len(ordered) - 1, int(len(ordered) * 0.9))], max_bps=ordered[-1],
        above_pct=round(above_pct, 1), current_episode_minutes=current, episodes=episodes,
        converged_episodes=len(converge_times),
        median_minutes_to_converge=statistics.median(converge_times) if converge_times else None,
        minutes_since_converged=since, sources=sources)
    if coverage < min_coverage_pct:
        label = "insufficient"
    elif above_pct >= persistent_pct and not converge_times:
        label = "persistent"   # structural: rarely leaves the threshold, never returned to take profit
    elif converge_times:
        label = "reverting"    # reached the threshold before and came back to take profit
    elif above_pct <= 5 and current <= 5:
        label = "new_spike"    # rare and only just appeared
    else:
        label = "intermittent"
    return SpreadProfile(label=label, **profile)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class SpreadHistory:
    def __init__(self, *, recorder: MinuteRecorder, lookback_hours: float = 24, threshold_bps: float = 30,
                 take_profit_bps: float = 10, persistent_pct: float = 70, min_coverage_pct: float = 50,
                 fetcher=fetch_minute_closes, clock=lambda: int(time.time() * 1000)):
        self.recorder, self.fetcher, self.clock = recorder, fetcher, clock
        self.lookback_minutes = int(lookback_hours * 60)
        self.threshold_bps, self.take_profit_bps = threshold_bps, take_profit_bps
        self.persistent_pct, self.min_coverage_pct = persistent_pct, min_coverage_pct
        self._cache: dict[tuple[str, str], tuple[float, dict[int, float] | None]] = {}
        self._session: aiohttp.ClientSession | None = None

    def record_store(self, store, now_ms: int):
        for venue in RECORDED_VENUES:
            for symbol, quote in store.live_quotes(venue).items():
                self.recorder.record(venue, symbol, quote["bid"], quote["ask"], now_ms)
        self.recorder.save(now_ms)

    async def _series(self, venue: str, symbol: str, now_ms: int) -> tuple[dict[int, float], str]:
        if venue in {"lighter", "variational"}:
            return self.recorder.get(venue, symbol), "recorded"
        cached = self._cache.get((venue, symbol))
        if cached is None or time.monotonic() - cached[0] > CANDLE_CACHE_SECONDS:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            start = now_ms - (self.lookback_minutes + FORWARD_FILL_MINUTES) * MINUTE_MS
            try:
                series = await self.fetcher(self._session, venue, symbol, start, now_ms)
            except Exception:
                series = {}
            cached = (time.monotonic(), series)
            self._cache[(venue, symbol)] = cached
        if not cached[1] and venue in RECORDED_VENUES:
            return self.recorder.get(venue, symbol), "recorded"  # e.g. Ondo without an API key
        return cached[1] or {}, "candles"

    async def profile(self, symbol: str, short_venue: str, long_venue: str) -> SpreadProfile:
        now = self.clock()
        (short, short_src), (long, long_src) = await asyncio.gather(
            self._series(short_venue, symbol, now), self._series(long_venue, symbol, now))
        return build_profile(short, long, now_ms=now, lookback_minutes=self.lookback_minutes,
                             threshold_bps=self.threshold_bps, take_profit_bps=self.take_profit_bps,
                             persistent_pct=self.persistent_pct, min_coverage_pct=self.min_coverage_pct,
                             sources=f"{short_venue}:{short_src}/{long_venue}:{long_src}")

    async def close(self):
        self.recorder.save(self.clock(), force=True)
        if self._session is not None:
            await self._session.close()
