from __future__ import annotations

import asyncio
import datetime as dt
import time

import aiohttp

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.variational import VARIATIONAL_BASE_URL
from hydra_basis.config import BACKFILL_SPREAD_CLIP_USD
from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot, parse_variational_quote


QUOTE_TIMEOUT_SECONDS = 20


async def fetch_monitor_entry_books(opportunities: list[dict], *, min_annualized_avg: float = 0.4) -> dict:
    """Refresh only funding-qualified candidates, without modifying stored books."""
    keys = {(venue, row["symbol"]) for row in opportunities
            if row["stats"]["signal"] and row["stats"]["annualized_avg"] > min_annualized_avg
            for venue in (row["short_venue"], row["long_venue"])}
    if not keys:
        return {}
    books = {}
    semaphore = asyncio.Semaphore(4)
    async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-monitor/0.1"}) as session:
        async def regular(venue, symbol):
            async with semaphore:
                try:
                    book = await asyncio.wait_for(fetch_orderbook_snapshot(
                        session, venue=venue, symbol=symbol, clip_usd=BACKFILL_SPREAD_CLIP_USD,
                    ), timeout=QUOTE_TIMEOUT_SECONDS)
                    # A fresh request without a venue timestamp uses local receipt time.
                    books[(venue, symbol)] = {**book, "ts_ms": book.get("ts_ms") or int(time.time() * 1000)}
                except Exception as exc:
                    print(f"entry_quote_unavailable venue={venue} symbol={symbol} error={type(exc).__name__}")

        async def variational():
            symbols = sorted(symbol for venue, symbol in keys if venue == "variational")
            if not symbols:
                return
            try:
                data = await asyncio.wait_for(fetch_json(
                    session, "GET", f"{VARIATIONAL_BASE_URL}/metadata/stats",
                ), timeout=QUOTE_TIMEOUT_SECONDS)
                if not isinstance(data, dict) or not isinstance(data.get("listings"), list):
                    raise ValueError("invalid listings response")
                listings = {str(row.get("ticker", "")).upper(): row for row in data["listings"]}
            except Exception as exc:
                print(f"entry_quote_unavailable venue=variational error={type(exc).__name__}")
                return
            for symbol in symbols:
                try:
                    book = parse_variational_quote(data, symbol, clip_usd=BACKFILL_SPREAD_CLIP_USD)
                    # The public stats endpoint is a snapshot; receipt time is not quote time.
                    updated = listings[symbol]["quotes"]["updated_at"]
                    if not isinstance(updated, str):
                        raise ValueError("invalid quote timestamp")
                    timestamp = dt.datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    if timestamp.tzinfo is None:
                        raise ValueError("quote timestamp has no timezone")
                    books[("variational", symbol)] = {**book, "ts_ms": int(timestamp.timestamp() * 1000)}
                except (KeyError, TypeError, ValueError, RuntimeError):
                    print(f"entry_quote_unavailable venue=variational symbol={symbol} reason=invalid_quote_or_timestamp")

        await asyncio.gather(variational(), *(regular(venue, symbol) for venue, symbol in sorted(keys) if venue != "variational"))
    return books
