from __future__ import annotations

from hydra_basis.adapters.hyperliquid import (
    HYPERLIQUID_FUNDING_INTERVAL_HOURS,
    _post_hyperliquid_info,
    ms_days_ago,
)
from hydra_basis.config import LOOKBACK_DAYS
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps

TRADE_XYZ_VENUE = "trade_xyz"
TRADE_XYZ_DEX = "xyz"


def tradexyz_api_coin(symbol: str) -> str:
    normalized = symbol.strip().upper()
    dex, separator, ticker = normalized.partition(":")
    if separator:
        return f"{dex.lower()}:{ticker}"
    return f"{TRADE_XYZ_DEX}:{normalized}"


def build_tradexyz_meta_payload() -> dict[str, str]:
    return {"type": "meta", "dex": TRADE_XYZ_DEX}


def build_tradexyz_funding_history_payload(symbol: str, start_time_ms: int) -> dict[str, str | int]:
    return {
        "type": "fundingHistory",
        "coin": tradexyz_api_coin(symbol),
        "startTime": start_time_ms,
        "dex": TRADE_XYZ_DEX,
    }


async def fetch_tradexyz_universe(session) -> list[str]:
    data = await _post_hyperliquid_info(session, build_tradexyz_meta_payload())
    universe = data.get("universe") or []
    return [str(row.get("name") or "").upper() for row in universe]


async def list_symbols(session) -> set[str]:
    data = await _post_hyperliquid_info(session, build_tradexyz_meta_payload())
    universe = data.get("universe") or []
    return {str(row.get("name") or "").upper() for row in universe if not row.get("isDelisted")}


async def fetch_tradexyz_funding(session, symbol: str) -> list[FundingPoint]:
    return await fetch_tradexyz_funding_since(session, symbol, start_time_ms=ms_days_ago(LOOKBACK_DAYS))


async def fetch_tradexyz_funding_since(session, symbol: str, start_time_ms: int) -> list[FundingPoint]:
    data = await _post_hyperliquid_info(
        session,
        build_tradexyz_funding_history_payload(symbol, start_time_ms=start_time_ms),
    )
    rows_data = [
        (int(row.get("time") or row.get("timestamp")), float(row["fundingRate"]))
        for row in data
    ]
    inferred = infer_interval_hours_from_timestamps([ts for ts, _ in rows_data])
    interval_hours = inferred if inferred is not None else HYPERLIQUID_FUNDING_INTERVAL_HOURS
    return [
        FundingPoint(TRADE_XYZ_VENUE, symbol, ts, rate, interval_hours)
        for ts, rate in rows_data
    ]
