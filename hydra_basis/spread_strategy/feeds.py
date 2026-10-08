"""Live top-of-book cache fed by venue WebSockets (Variational: REST polling).

Freshness follows the original ``freshMarket``: an unhealthy connection, a
quote older than the freshness window, a quote from the future beyond the
clock-skew tolerance, or a slow source-to-receipt transport all make the venue
unusable for triggering.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from decimal import Decimal

import aiohttp

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.variational import VARIATIONAL_BASE_URL
from hydra_basis.execution_engine.market_data import parse_variational_quote
from hydra_basis.spread_strategy.core import Config, now_ms
from hydra_basis.spread_strategy.instruments import aster_contract
from hydra_basis.symbol_mapping import canonicalize_symbol

ASTER_WS = "wss://fstream.asterdex.com/ws/{stream}@bookTicker"
HYPERLIQUID_WS = "wss://api.hyperliquid.xyz/ws"


@dataclass(frozen=True)
class Ticker:
    bid: Decimal
    ask: Decimal
    received_ms: int
    # Exchange timestamp. Variational's public stats carry none.
    source_ms: int | None = None


class MarketFeed:
    def __init__(self, config: Config, *, clock=now_ms):
        self.config = config
        self.clock = clock
        self._tickers: dict[str, Ticker] = {}
        self._healthy: dict[str, bool] = {venue: False for venue in config.venues}

    def update(self, venue: str, bid, ask, *, source_ms: int | None, received_ms: int | None = None):
        self._tickers[venue] = Ticker(Decimal(str(bid)), Decimal(str(ask)),
                                      received_ms if received_ms is not None else self.clock(), source_ms)
        self._healthy[venue] = True

    def set_health(self, venue: str, healthy: bool):
        self._healthy[venue] = healthy

    def fresh(self, venue: str) -> Ticker | None:
        if not self._healthy.get(venue):
            return None
        ticker = self._tickers.get(venue)
        if ticker is None or not 0 < ticker.bid <= ticker.ask:
            return None
        c = self.config
        source = ticker.source_ms if ticker.source_ms is not None else ticker.received_ms
        age = self.clock() - source
        lag = ticker.received_ms - source
        if (age > c.market_freshness_seconds * 1000 or age < -c.future_tolerance_seconds * 1000
                or lag > c.max_transport_lag_seconds * 1000):
            return None
        return ticker

    def fresh_pair(self) -> dict[str, Ticker] | None:
        books = {venue: self.fresh(venue) for venue in self.config.venues}
        return None if any(book is None for book in books.values()) else books

    def last_mid(self) -> Decimal | None:
        mids = [(t.bid + t.ask) / 2 for t in self._tickers.values()]
        return max(mids) if mids else None

    # ------------------------------------------------------------------ streams

    async def run(self, session: aiohttp.ClientSession):
        loops = {"aster": self._aster, "hyperliquid": self._hyperliquid, "variational": self._variational}
        await asyncio.gather(*(self._supervise(venue, loops[venue], session) for venue in self.config.venues))

    async def _supervise(self, venue, loop, session):
        delay = 1.0
        while True:
            try:
                await loop(session)
                delay = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(json.dumps({"event": "feed_error", "venue": venue, "error": str(exc)[:200]}), flush=True)
            self.set_health(venue, False)
            await asyncio.sleep(delay)
            delay = min(30.0, delay * 2)

    async def _aster(self, session):
        contract = await aster_contract(session, self.config.symbol)
        url = ASTER_WS.format(stream=str(contract["symbol"]).lower())
        async with session.ws_connect(url, heartbeat=20) as ws:
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                        break
                    continue
                data = message.json()
                data = data.get("data", data)
                if data.get("b") is None or data.get("a") is None:
                    continue
                self.update("aster", data["b"], data["a"], source_ms=int(data.get("T") or data.get("E")))

    async def _hyperliquid(self, session):
        async with session.ws_connect(HYPERLIQUID_WS, heartbeat=20) as ws:
            await ws.send_json({"method": "subscribe",
                                "subscription": {"type": "l2Book", "coin": self.config.symbol}})
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                        break
                    continue
                payload = message.json()
                if payload.get("channel") != "l2Book":
                    continue
                data = payload.get("data") or {}
                levels = data.get("levels") or []
                if len(levels) < 2 or not levels[0] or not levels[1]:
                    continue
                self.update("hyperliquid", levels[0][0]["px"], levels[1][0]["px"],
                            source_ms=int(data["time"]))

    async def _variational(self, session):
        symbol = canonicalize_symbol(self.config.symbol, venue="variational")
        while True:
            mid = self.last_mid()
            clip_usd = float(self.config.clip_quantity * mid) if mid else 0.0
            data = await fetch_json(session, "GET", f"{VARIATIONAL_BASE_URL}/metadata/stats")
            quote = parse_variational_quote(data, symbol, clip_usd=clip_usd)
            self.update("variational", quote["bid"], quote["ask"], source_ms=None)
            await asyncio.sleep(self.config.variational_poll_seconds)
