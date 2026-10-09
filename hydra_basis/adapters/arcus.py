"""Arcus (https://arcus.xyz) public data: markets, funding history, order book.

Perp markets are named ``<BASE>-USD`` (BTC-USD, NVDA-USD); project symbols are the
base asset. Funding is hourly; API timestamps are epoch MICROseconds.
"""
from __future__ import annotations

import os

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.hyperliquid import ms_days_ago
from hydra_basis.config import LOOKBACK_DAYS
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps

ARCUS_VENUE = "arcus"
ARCUS_FUNDING_INTERVAL_HOURS = 1.0
FUNDING_PAGE_LIMIT = 1000


def arcus_base_url() -> str:
    # ARCUS_BASE_URL=https://api.testnet.arcus.xyz switches every Arcus call to testnet.
    return os.getenv("ARCUS_BASE_URL", "https://api.arcus.xyz").rstrip("/")


def arcus_market_name(symbol: str) -> str:
    normalized = symbol.strip().upper()
    return normalized if normalized.endswith("-USD") else f"{normalized}-USD"


async def fetch_arcus_markets(session) -> list[dict]:
    data = await fetch_json(session, "GET", f"{arcus_base_url()}/v1/markets")
    return list(data.get("markets") or [])


async def list_symbols(session) -> set[str]:
    return {str(m["baseAsset"]).upper() for m in await fetch_arcus_markets(session)
            if m.get("status") == "ONLINE" and m.get("type", "PERPETUAL") == "PERPETUAL"}


async def fetch_arcus_funding(session, symbol: str) -> list[FundingPoint]:
    return await fetch_arcus_funding_since(session, symbol, start_time_ms=ms_days_ago(LOOKBACK_DAYS))


async def fetch_arcus_funding_since(session, symbol: str, start_time_ms: int) -> list[FundingPoint]:
    """Page backwards (newest first) with ``to`` until the window start is covered."""
    start_us = max(int(start_time_ms) * 1000, 10**14)
    rows: dict[int, float] = {}
    to_us = None
    while True:
        params = {"market": arcus_market_name(symbol), "from": start_us, "limit": FUNDING_PAGE_LIMIT}
        if to_us is not None:
            params["to"] = to_us
        data = await fetch_json(session, "GET", f"{arcus_base_url()}/v1/fundingRates", params=params)
        page = data.get("fundingRates") or []
        for row in page:
            rows[int(row["time"]) // 1000] = float(row["fundingRate"])
        if len(page) < FUNDING_PAGE_LIMIT:
            break
        oldest = min(int(row["time"]) for row in page)
        if oldest <= start_us:
            break
        to_us = oldest - 1000  # funding ticks are whole milliseconds
    timestamps = sorted(rows)
    inferred = infer_interval_hours_from_timestamps(timestamps)
    interval_hours = inferred if inferred is not None else ARCUS_FUNDING_INTERVAL_HOURS
    return [FundingPoint(ARCUS_VENUE, symbol, ts, rows[ts], interval_hours) for ts in timestamps]


async def fetch_arcus_orderbook(session, symbol: str) -> dict[str, float | int]:
    data = await fetch_json(session, "GET", f"{arcus_base_url()}/v1/bbo/{arcus_market_name(symbol)}")
    bid, ask = data.get("bestBid"), data.get("bestAsk")
    if not bid or not ask:
        raise RuntimeError(f"missing arcus orderbook for {symbol}")
    return {"bid": float(bid["price"]), "ask": float(ask["price"]), "ts_ms": int(data.get("timestamp") or 0) // 1000}
