from __future__ import annotations

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.request_limiters import run_serialized
from hydra_basis.config import LOOKBACK_DAYS, VENUE_CONFIG
from hydra_basis.config import HYPERLIQUID_REQUEST_DELAY_SECONDS
from hydra_basis.funding_engine.analysis import now_ms
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps


def ms_days_ago(days: int) -> int:
    return now_ms() - days * 24 * 60 * 60 * 1000


def build_funding_history_payload(symbol: str, start_time_ms: int) -> dict:
    return {
        "type": "fundingHistory",
        "coin": symbol,
        "startTime": start_time_ms,
    }


async def fetch_hyperliquid_universe(session) -> list[str]:
    """Return ALL assets in raw order (including delisted) so indices match Hyperliquid's API."""
    url = "https://api.hyperliquid.xyz/info"
    payload = {"type": "meta"}
    data = await run_serialized(
        "hyperliquid",
        lambda: fetch_json(session, "POST", url, json=payload),
        delay_seconds=HYPERLIQUID_REQUEST_DELAY_SECONDS,
    )
    universe = data.get("universe") or []
    return [str(row.get("name") or "").upper() for row in universe]


async def list_symbols(session) -> set[str]:
    url = "https://api.hyperliquid.xyz/info"
    payload = {"type": "meta"}
    data = await run_serialized(
        "hyperliquid",
        lambda: fetch_json(session, "POST", url, json=payload),
        delay_seconds=HYPERLIQUID_REQUEST_DELAY_SECONDS,
    )
    universe = data.get("universe") or []
    return {str(row.get("name") or "").upper() for row in universe if not row.get("isDelisted")}


async def fetch_hyperliquid_funding(session, symbol: str) -> list[FundingPoint]:
    return await fetch_hyperliquid_funding_since(session, symbol, start_time_ms=ms_days_ago(LOOKBACK_DAYS))


async def fetch_hyperliquid_funding_since(session, symbol: str, start_time_ms: int) -> list[FundingPoint]:
    url = "https://api.hyperliquid.xyz/info"
    payload = build_funding_history_payload(symbol, start_time_ms=start_time_ms)
    data = await run_serialized(
        "hyperliquid",
        lambda: fetch_json(session, "POST", url, json=payload),
        delay_seconds=HYPERLIQUID_REQUEST_DELAY_SECONDS,
    )

    rows_data = [
        (int(row.get("time") or row.get("timestamp")), float(row["fundingRate"]))
        for row in data
    ]
    inferred = infer_interval_hours_from_timestamps([ts for ts, _ in rows_data])
    if inferred is None:
        raise RuntimeError(f"unable to infer funding interval from history for hyperliquid:{symbol}")
    interval_hours = inferred
    return [
        FundingPoint("hyperliquid", symbol, ts, rate, interval_hours)
        for ts, rate in rows_data
    ]
