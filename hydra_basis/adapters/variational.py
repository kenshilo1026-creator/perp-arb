from __future__ import annotations

import datetime as dt

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.loris_browser import (
    fetch_loris_historical_with_nodriver,
    loris_auth_headers,
    loris_nodriver_enabled,
)
from hydra_basis.adapters.request_limiters import run_serialized
from hydra_basis.config import VARIATIONAL_REQUEST_DELAY_SECONDS
from hydra_basis.funding_engine.analysis import now_ms
from hydra_basis.funding_engine.models import FundingPoint


VARIATIONAL_BASE_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io"
LORIS_HISTORICAL_URL = "https://api.loris.tools/funding/historical"
_VARIATIONAL_STATS_CACHE: dict[int, dict[str, dict[str, float]]] = {}
LORIS_GATEWAY_RETRIES = 2
LORIS_COMPARISON_INTERVAL_HOURS = 8.0
LORIS_BROWSER_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Content-Type": "application/json",
    "Origin": "https://loris.tools",
    "Referer": "https://loris.tools/",
    "Sec-CH-UA": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
}
def is_retryable_loris_historical_error(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    request_info = getattr(exc, "request_info", None)
    real_url = getattr(request_info, "real_url", "")
    return status in {401, 502, 503, 504} and "api.loris.tools/funding/historical" in str(real_url)


def parse_stats_listings(data: dict) -> dict[str, dict[str, float]]:
    listings = data.get("listings") or []
    parsed: dict[str, dict[str, float]] = {}
    for listing in listings:
        ticker = str(listing.get("ticker") or "").upper()
        if not ticker:
            continue
        funding_rate = listing.get("funding_rate")
        funding_interval_s = listing.get("funding_interval_s")
        if funding_rate is None or funding_interval_s is None:
            continue
        parsed[ticker] = {
            "funding_rate": float(funding_rate),
            "interval_hours": float(funding_interval_s) / 3600.0,
        }
    return parsed


def isoformat_z(ts_ms: int) -> str:
    return dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def parse_loris_historical_series(
    data: dict,
    *,
    symbol: str,
    venue: str,
    interval_hours: float,
) -> list[FundingPoint]:
    series = data.get("series", {}).get(venue, [])
    points: list[FundingPoint] = []
    for row in series:
        ts_raw = row.get("t")
        y_raw = row.get("y")
        if ts_raw is None or y_raw is None:
            continue
        ts_ms = int(dt.datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).timestamp() * 1000)
        # Loris historical y is returned in bps, so convert 0.3848 -> 0.00003848 decimal funding rate.
        raw_rate = float(y_raw) / 10000
        points.append(
            FundingPoint(
                venue=venue,
                symbol=symbol.upper(),
                ts_ms=ts_ms,
                raw_rate=raw_rate,
                interval_hours=interval_hours,
            )
        )
    return points


async def fetch_variational_stats(session) -> dict[str, dict[str, float]]:
    cached = _VARIATIONAL_STATS_CACHE.get(id(session))
    if cached is not None:
        return cached
    data = await fetch_json(session, "GET", f"{VARIATIONAL_BASE_URL}/metadata/stats")
    parsed = parse_stats_listings(data)
    _VARIATIONAL_STATS_CACHE[id(session)] = parsed
    return parsed


async def list_symbols(session) -> set[str]:
    stats = await fetch_variational_stats(session)
    return set(stats.keys())


async def fetch_variational_current_funding(session, symbol: str) -> dict[str, float] | None:
    stats = await fetch_variational_stats(session)
    entry = stats.get(symbol.upper())
    if entry is None:
        return None
    return {
        "funding_rate": float(entry["funding_rate"]),
        "interval_hours": float(entry["interval_hours"]),
    }


async def fetch_variational_funding(session, symbol: str) -> list[FundingPoint]:
    end_ms = now_ms()
    start_ms = end_ms - 7 * 24 * 60 * 60 * 1000
    return await fetch_variational_funding_since(session, symbol, start_time_ms=start_ms, end_time_ms=end_ms)


async def fetch_variational_funding_since(session, symbol: str, start_time_ms: int, end_time_ms: int | None = None) -> list[FundingPoint]:
    stats = await fetch_variational_stats(session)
    entry = stats.get(symbol.upper())
    if entry is None:
        return []
    end_ms = end_time_ms if end_time_ms is not None else now_ms()
    start_iso = isoformat_z(start_time_ms)
    end_iso = isoformat_z(end_ms)
    if loris_nodriver_enabled():
        data = await run_serialized(
            "variational",
            lambda: fetch_loris_historical_with_nodriver(
                symbol=symbol.upper(),
                start=start_iso,
                end=end_iso,
            ),
            delay_seconds=VARIATIONAL_REQUEST_DELAY_SECONDS,
        )
        return parse_loris_historical_series(
            data or {},
            symbol=symbol,
            venue="variational",
            interval_hours=max(float(entry["interval_hours"]), LORIS_COMPARISON_INTERVAL_HOURS),
        )

    loris_headers = dict(LORIS_BROWSER_HEADERS)
    loris_headers.update(loris_auth_headers())
    data = None
    for attempt in range(LORIS_GATEWAY_RETRIES + 1):
        try:
            data = await run_serialized(
                "variational",
                lambda: fetch_json(
                    session,
                    "GET",
                    LORIS_HISTORICAL_URL,
                    params={
                        "symbol": symbol.upper(),
                        "start": start_iso,
                        "end": end_iso,
                    },
                    headers=loris_headers,
                ),
                delay_seconds=VARIATIONAL_REQUEST_DELAY_SECONDS,
            )
            break
        except Exception as exc:
            if loris_nodriver_enabled() and is_retryable_loris_historical_error(exc):
                data = await run_serialized(
                    "variational",
                    lambda: fetch_loris_historical_with_nodriver(
                        symbol=symbol.upper(),
                        start=start_iso,
                        end=end_iso,
                    ),
                    delay_seconds=VARIATIONAL_REQUEST_DELAY_SECONDS,
                )
                break
            if not is_retryable_loris_historical_error(exc) or attempt >= LORIS_GATEWAY_RETRIES:
                raise
    return parse_loris_historical_series(
        data or {},
        symbol=symbol,
        venue="variational",
        interval_hours=max(float(entry["interval_hours"]), LORIS_COMPARISON_INTERVAL_HOURS),
    )
