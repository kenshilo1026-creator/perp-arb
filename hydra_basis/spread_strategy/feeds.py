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
from hydra_basis.adapters.lighter import fetch_lighter_market_map
from hydra_basis.adapters.mexc import mexc_contract_symbol
from hydra_basis.adapters.variational import VARIATIONAL_BASE_URL
from hydra_basis.execution_engine.market_data import parse_variational_quote
from hydra_basis.spread_strategy.core import Config, now_ms
from hydra_basis.spread_strategy.instruments import aster_contract
from hydra_basis.symbol_mapping import canonicalize_symbol

ASTER_WS = "wss://fstream.asterdex.com/ws/{stream}@bookTicker"
HYPERLIQUID_WS = "wss://api.hyperliquid.xyz/ws"
LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
ARCUS_WS_PATH = "/v1/ws"
MEXC_WS = "wss://contract.mexc.com/edge"
MEXC_PING_SECONDS = 15


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

    def ticker(self, venue: str) -> Ticker | None:
        return self._tickers.get(venue)

    def fresh(self, venue: str) -> Ticker | None:
        if not self._healthy.get(venue):
            return None
        ticker = self.ticker(venue)
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
        tickers = [self.ticker(venue) for venue in self.config.venues]
        mids = [(t.bid + t.ask) / 2 for t in tickers if t is not None]
        return max(mids) if mids else None

    # ------------------------------------------------------------------ streams

    async def run(self, session: aiohttp.ClientSession):
        loops = {"aster": self._aster, "arcus": self._arcus, "hyperliquid": self._hyperliquid,
                 "entropy": self._entropy,
                 "lighter": self._lighter,
                 "mexc": self._mexc, "variational": self._variational}
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

    async def _hyperliquid(self, session, venue: str = "hyperliquid", coin: str | None = None):
        async with session.ws_connect(HYPERLIQUID_WS, heartbeat=20) as ws:
            await ws.send_json({"method": "subscribe",
                                "subscription": {"type": "l2Book", "coin": coin or self.config.symbol}})
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
                self.update(venue, levels[0][0]["px"], levels[1][0]["px"], source_ms=int(data["time"]))

    async def _entropy(self, session):
        # Entropy markets are Hyperliquid HIP-3 coins named "io:<SYMBOL>".
        await self._hyperliquid(session, venue="entropy", coin=f"io:{self.config.symbol}")

    async def _arcus(self, session):
        from hydra_basis.adapters.arcus import arcus_base_url, arcus_market_name
        url = arcus_base_url().replace("https://", "wss://") + ARCUS_WS_PATH
        async with session.ws_connect(url, heartbeat=20) as ws:
            await ws.send_json({"type": "subscribe", "channel": "bbo", "id": arcus_market_name(self.config.symbol)})
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                        break
                    continue
                book = parse_arcus_bbo(message.json())
                if book is not None:
                    self.update("arcus", book["bid"], book["ask"], source_ms=book["ts_ms"])

    async def _lighter(self, session):
        market_id = (await fetch_lighter_market_map(session)).get(self.config.symbol)
        if market_id is None:
            raise RuntimeError(f"symbol not found on lighter: {self.config.symbol}")
        async with session.ws_connect(LIGHTER_WS, heartbeat=20) as ws:
            await ws.send_json({"type": "subscribe", "channel": f"ticker/{market_id}"})
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                        break
                    continue
                payload = message.json()
                if payload.get("type") == "ping":
                    await ws.send_json({"type": "pong"})
                    continue
                ticker = payload.get("ticker") or {}
                bid, ask = (ticker.get("b") or {}).get("price"), (ticker.get("a") or {}).get("price")
                if bid is None or ask is None or payload.get("timestamp") is None:
                    continue
                self.update("lighter", bid, ask, source_ms=int(payload["timestamp"]))

    async def _mexc(self, session):
        async with session.ws_connect(MEXC_WS, heartbeat=20) as ws:
            # Depth, not the ticker: ticker snapshots run 1-3 s behind the book.
            await ws.send_json({"method": "sub.depth.full",
                                "param": {"symbol": mexc_contract_symbol(self.config.symbol), "limit": 5}})
            # MEXC drops connections without application-level pings.
            pinger = asyncio.create_task(_ping_forever(ws, {"method": "ping"}, MEXC_PING_SECONDS))
            try:
                async for message in ws:
                    if message.type != aiohttp.WSMsgType.TEXT:
                        if message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                            break
                        continue
                    book = parse_mexc_depth(message.json())
                    if book is not None:
                        self.update("mexc", book["bid"], book["ask"], source_ms=book["ts_ms"])
            finally:
                pinger.cancel()

    async def _variational(self, session):
        symbol = canonicalize_symbol(self.config.symbol, venue="variational")
        while True:
            mid = self.last_mid()
            clip_usd = float(self.config.clip_quantity * mid) if mid else 0.0
            data = await fetch_json(session, "GET", f"{VARIATIONAL_BASE_URL}/metadata/stats")
            quote = parse_variational_quote(data, symbol, clip_usd=clip_usd)
            self.update("variational", quote["bid"], quote["ask"], source_ms=None)
            await asyncio.sleep(self.config.variational_poll_seconds)


def parse_arcus_bbo(payload: dict) -> dict | None:
    """Arcus ``bbo`` channel frame (snapshot or update); timestamps are microseconds."""
    if payload.get("channel") != "bbo" or payload.get("type") not in {"subscribed", "channel_data"}:
        return None
    contents = payload.get("contents") or {}
    bid, ask = contents.get("bestBid"), contents.get("bestAsk")
    if not bid or not ask or contents.get("timestamp") is None:
        return None
    return {"symbol": str(payload.get("id", "")).upper().removesuffix("-USD"),
            "bid": bid["price"], "ask": ask["price"], "ts_ms": int(contents["timestamp"]) // 1000}


def parse_mexc_depth(payload: dict) -> dict | None:
    if payload.get("channel") != "push.depth.full":
        return None
    data = payload.get("data") or {}
    bids, asks = data.get("bids") or [], data.get("asks") or []
    if not bids or not asks or payload.get("ts") is None:
        return None
    return {"symbol": str(payload.get("symbol", "")).upper().removesuffix("_USDT"),
            "bid": bids[0][0], "ask": asks[0][0], "ts_ms": int(payload["ts"])}


async def _ping_forever(ws, payload: dict, interval: float):
    while True:
        await asyncio.sleep(interval)
        await ws.send_json(payload)


class StoreFeed(MarketFeed):
    """One group's view of the dispatcher's shared quote store (one connection set for all groups)."""

    def __init__(self, config: Config, store, health: dict[str, bool], *, clock=now_ms):
        super().__init__(config, clock=clock)
        self.store, self.health = store, health

    def update(self, venue, bid, ask, *, source_ms, received_ms=None):
        raise RuntimeError("StoreFeed is read-only; quotes come from the shared store")

    def ticker(self, venue: str) -> Ticker | None:
        quote = self.store.get_quote(venue, self.config.symbol)
        if quote is None:
            return None
        return Ticker(Decimal(str(quote["bid"])), Decimal(str(quote["ask"])), int(quote["received_ms"]),
                      None if quote.get("source_ms") is None else int(quote["source_ms"]))

    def fresh(self, venue: str) -> Ticker | None:
        self._healthy[venue] = self.health.get(venue, False)
        return super().fresh(venue)
