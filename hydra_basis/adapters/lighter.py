from __future__ import annotations

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.request_limiters import run_serialized
from hydra_basis.adapters.hyperliquid import ms_days_ago
from hydra_basis.config import LIGHTER_REQUEST_DELAY_SECONDS, LOOKBACK_DAYS
from hydra_basis.funding_engine.models import FundingPoint


def signed_rate_from_history_row(row: dict) -> float:
    # Lighter historical "rate" values are reported in percent units, so convert to decimal.
    rate = float(row.get("rate") or row.get("funding_rate") or row.get("fundingRate") or 0.0) / 100
    direction = str(row.get("direction") or "").lower()
    if direction == "short":
        return -rate
    return rate


async def fetch_lighter_market_map(session) -> dict[str, int]:
    url = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"
    data = await run_serialized(
        "lighter",
        lambda: fetch_json(session, "GET", url),
        delay_seconds=LIGHTER_REQUEST_DELAY_SECONDS,
    )
    rows = data.get("funding_rates") or []

    market_map: dict[str, int] = {}
    for row in rows:
        if str(row.get("exchange") or "").lower() != "lighter":
            continue
        symbol = str(row.get("symbol") or "").upper()
        market_id = row.get("market_id")
        if symbol and market_id is not None:
            market_map[symbol] = int(market_id)
    return market_map


async def list_symbols(session) -> set[str]:
    market_map = await fetch_lighter_market_map(session)
    return set(market_map.keys())


async def fetch_lighter_funding(session, symbol: str) -> list[FundingPoint]:
    market_map = await fetch_lighter_market_map(session)
    market_id = market_map.get(symbol.upper())
    if market_id is None:
        return []

    end_timestamp_ms = ms_days_ago(0)
    start_timestamp_ms = ms_days_ago(LOOKBACK_DAYS)
    url = "https://mainnet.zklighter.elliot.ai/api/v1/fundings"
    params = {
        "market_id": market_id,
        "resolution": "1h",
        "start_timestamp": start_timestamp_ms // 1000,
        "end_timestamp": end_timestamp_ms // 1000,
        "count_back": LOOKBACK_DAYS * 24,
    }
    data = await run_serialized(
        "lighter",
        lambda: fetch_json(session, "GET", url, params=params),
        delay_seconds=LIGHTER_REQUEST_DELAY_SECONDS,
    )

    rows = data.get("fundings") or []
    points: list[FundingPoint] = []

    for row in rows:
        ts_seconds = row.get("timestamp") or row.get("time")
        if ts_seconds is None:
            continue
        ts_ms = int(ts_seconds) * 1000
        points.append(
            FundingPoint(
                "lighter",
                symbol,
                ts_ms,
                signed_rate_from_history_row(row),
                1.0,
            )
        )
    return points
