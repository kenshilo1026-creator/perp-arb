"""Entropy (https://entropy.io): a HIP-3 builder-deployed perp dex on Hyperliquid.

Markets live on Hyperliquid under the dex name ``io`` (coins such as ``io:OAI``),
so data comes from Hyperliquid's info API with ``dex: "io"``. Funding is hourly.
Funding-monitor symbols keep the dex prefix (``IO:OAI``), like trade.xyz's ``XYZ:``.
"""
from __future__ import annotations

from hydra_basis.adapters.hyperliquid import (
    HYPERLIQUID_FUNDING_INTERVAL_HOURS,
    _post_hyperliquid_info,
    ms_days_ago,
)
from hydra_basis.config import LOOKBACK_DAYS
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps

ENTROPY_VENUE = "entropy"
ENTROPY_DEX = "io"


def entropy_api_coin(symbol: str) -> str:
    normalized = symbol.strip().upper()
    dex, separator, ticker = normalized.partition(":")
    if separator:
        return f"{dex.lower()}:{ticker}"
    return f"{ENTROPY_DEX}:{normalized}"


def build_entropy_meta_payload() -> dict[str, str]:
    return {"type": "meta", "dex": ENTROPY_DEX}


def build_entropy_funding_history_payload(symbol: str, start_time_ms: int) -> dict[str, str | int]:
    return {"type": "fundingHistory", "coin": entropy_api_coin(symbol), "startTime": start_time_ms,
            "dex": ENTROPY_DEX}


async def fetch_entropy_universe(session) -> list[str]:
    data = await _post_hyperliquid_info(session, build_entropy_meta_payload())
    return [str(row.get("name") or "").upper() for row in data.get("universe") or []]


async def list_symbols(session) -> set[str]:
    data = await _post_hyperliquid_info(session, build_entropy_meta_payload())
    return {str(row.get("name") or "").upper() for row in data.get("universe") or [] if not row.get("isDelisted")}


async def fetch_entropy_funding(session, symbol: str) -> list[FundingPoint]:
    return await fetch_entropy_funding_since(session, symbol, start_time_ms=ms_days_ago(LOOKBACK_DAYS))


async def fetch_entropy_funding_since(session, symbol: str, start_time_ms: int) -> list[FundingPoint]:
    data = await _post_hyperliquid_info(
        session, build_entropy_funding_history_payload(symbol, start_time_ms=start_time_ms))
    rows = [(int(row.get("time") or row.get("timestamp")), float(row["fundingRate"])) for row in data]
    inferred = infer_interval_hours_from_timestamps([ts for ts, _ in rows])
    interval_hours = inferred if inferred is not None else HYPERLIQUID_FUNDING_INTERVAL_HOURS
    return [FundingPoint(ENTROPY_VENUE, symbol, ts, rate, interval_hours) for ts, rate in rows]
