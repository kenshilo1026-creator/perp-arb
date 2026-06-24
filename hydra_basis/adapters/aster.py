from __future__ import annotations

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.hyperliquid import ms_days_ago
from hydra_basis.config import LOOKBACK_DAYS
from hydra_basis.funding_engine.models import FundingPoint


ASTER_STABLE_SUFFIXES = ("USDT", "USDC", "USD")
_ASTER_SYMBOL_METADATA_CACHE: dict[int, dict[str, dict]] = {}


def normalize_aster_symbol(raw_symbol: str) -> str:
    normalized = raw_symbol.upper()
    for suffix in ASTER_STABLE_SUFFIXES:
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


async def fetch_aster_symbol_metadata(session) -> dict[str, dict]:
    cached = _ASTER_SYMBOL_METADATA_CACHE.get(id(session))
    if cached is not None:
        return cached

    url = "https://fapi.asterdex.com/fapi/v1/fundingInfo"
    rows = await fetch_json(session, "GET", url)

    symbol_metadata: dict[str, dict] = {}
    for row in rows:
        raw_symbol = str(row.get("symbol") or "").upper()
        canonical_symbol = normalize_aster_symbol(raw_symbol)
        if not raw_symbol or not canonical_symbol:
            continue
        interval_hours = row.get("fundingIntervalHours")
        if interval_hours is None:
            raise RuntimeError(f"missing fundingIntervalHours from aster fundingInfo for {raw_symbol}")
        symbol_metadata[canonical_symbol] = {
            "raw_symbol": raw_symbol,
            "interval_hours": float(interval_hours),
        }
    _ASTER_SYMBOL_METADATA_CACHE[id(session)] = symbol_metadata
    return symbol_metadata


async def list_symbols(session) -> set[str]:
    symbol_metadata = await fetch_aster_symbol_metadata(session)
    return set(symbol_metadata.keys())


async def fetch_aster_funding(session, symbol: str) -> list[FundingPoint]:
    return await fetch_aster_funding_since(session, symbol, start_time_ms=ms_days_ago(LOOKBACK_DAYS))


async def fetch_aster_current_funding(session, symbol: str) -> dict[str, float] | None:
    symbol_metadata = await fetch_aster_symbol_metadata(session)
    metadata = symbol_metadata.get(symbol.upper())
    if metadata is None:
        return None

    url = "https://fapi.asterdex.com/fapi/v1/premiumIndex"
    data = await fetch_json(session, "GET", url, params={"symbol": metadata["raw_symbol"]})
    funding_rate = data.get("lastFundingRate") or data.get("fundingRate")
    if funding_rate is None:
        return None
    return {
        "funding_rate": float(funding_rate),
        "interval_hours": float(metadata["interval_hours"]),
    }


async def fetch_aster_funding_since(session, symbol: str, start_time_ms: int) -> list[FundingPoint]:
    symbol_metadata = await fetch_aster_symbol_metadata(session)
    metadata = symbol_metadata.get(symbol.upper())
    if metadata is None:
        return []

    url = "https://fapi.asterdex.com/fapi/v1/fundingRate"
    params = {
        "symbol": metadata["raw_symbol"],
        "startTime": start_time_ms,
        "endTime": ms_days_ago(0),
        "limit": 1000,
    }
    rows = await fetch_json(session, "GET", url, params=params)
    if not isinstance(rows, list):
        print(f"aster unexpected response for {symbol}: {rows!r}")
        return []

    points: list[FundingPoint] = []
    for row in rows:
        funding_time = row.get("fundingTime")
        funding_rate = row.get("fundingRate")
        if funding_time is None or funding_rate is None:
            continue
        points.append(
            FundingPoint(
                "aster",
                symbol.upper(),
                int(funding_time),
                float(funding_rate),
                float(metadata["interval_hours"]),
            )
        )
    return points
