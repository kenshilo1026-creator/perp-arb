from __future__ import annotations

import asyncio

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.request_limiters import run_serialized
from hydra_basis.config import LOOKBACK_DAYS, VENUE_CONFIG
from hydra_basis.config import HYPERLIQUID_REQUEST_DELAY_SECONDS
from hydra_basis.funding_engine.analysis import now_ms
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps

HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_RETRY_ATTEMPTS = 4
HYPERLIQUID_RATE_LIMIT_BACKOFF_SECONDS = 3.0
HYPERLIQUID_FUNDING_INTERVAL_HOURS = 1.0


def ms_days_ago(days: int) -> int:
    return now_ms() - days * 24 * 60 * 60 * 1000


def build_funding_history_payload(symbol: str, start_time_ms: int) -> dict:
    return {
        "type": "fundingHistory",
        "coin": symbol,
        "startTime": start_time_ms,
    }


def _is_retryable_hyperliquid_error(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    if status == 429:
        return True
    message = str(exc).lower()
    return "429" in message or "too many requests" in message or "rate limit" in message


async def _post_hyperliquid_info(session, payload: dict):
    last_error: Exception | None = None
    for attempt in range(HYPERLIQUID_RETRY_ATTEMPTS + 1):
        try:
            return await run_serialized(
                "hyperliquid",
                lambda: fetch_json(session, "POST", HYPERLIQUID_INFO_URL, json=payload),
                delay_seconds=HYPERLIQUID_REQUEST_DELAY_SECONDS,
            )
        except Exception as exc:
            last_error = exc
            if not _is_retryable_hyperliquid_error(exc) or attempt >= HYPERLIQUID_RETRY_ATTEMPTS:
                raise
            backoff_seconds = HYPERLIQUID_RATE_LIMIT_BACKOFF_SECONDS * (attempt + 1)
            await asyncio.sleep(backoff_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError("hyperliquid info request failed without an error")


async def fetch_hyperliquid_universe(session) -> list[str]:
    """Return ALL assets in raw order (including delisted) so indices match Hyperliquid's API."""
    payload = {"type": "meta"}
    data = await _post_hyperliquid_info(session, payload)
    universe = data.get("universe") or []
    return [str(row.get("name") or "").upper() for row in universe]


async def list_symbols(session) -> set[str]:
    payload = {"type": "meta"}
    data = await _post_hyperliquid_info(session, payload)
    universe = data.get("universe") or []
    return {str(row.get("name") or "").upper() for row in universe if not row.get("isDelisted")}


async def fetch_hyperliquid_funding(session, symbol: str) -> list[FundingPoint]:
    return await fetch_hyperliquid_funding_since(session, symbol, start_time_ms=ms_days_ago(LOOKBACK_DAYS))


async def fetch_hyperliquid_funding_since(session, symbol: str, start_time_ms: int) -> list[FundingPoint]:
    payload = build_funding_history_payload(symbol, start_time_ms=start_time_ms)
    data = await _post_hyperliquid_info(session, payload)

    rows_data = [
        (int(row.get("time") or row.get("timestamp")), float(row["fundingRate"]))
        for row in data
    ]
    inferred = infer_interval_hours_from_timestamps([ts for ts, _ in rows_data])
    interval_hours = inferred if inferred is not None else HYPERLIQUID_FUNDING_INTERVAL_HOURS
    return [
        FundingPoint("hyperliquid", symbol, ts, rate, interval_hours)
        for ts, rate in rows_data
    ]
