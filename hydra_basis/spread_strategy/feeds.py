"""Live order books fed by venue WebSockets (Variational: REST polling).

Each venue's quote carries its visible depth (best first, base units) so
decisions use the average fill price for the full order size, not just the top
of book. Freshness follows the original ``freshMarket``: an unhealthy
connection, a quote older than the freshness window, a quote from the future
beyond the clock-skew tolerance, or a slow source-to-receipt transport all make
the venue unusable for triggering.
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

ASTER_DEPTH_WS = "wss://fstream.asterdex.com/ws/{stream}@depth10@100ms"
HYPERLIQUID_WS = "wss://api.hyperliquid.xyz/ws"
LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
ARCUS_WS_PATH = "/v1/ws"
MEXC_WS = "wss://contract.mexc.com/edge"
MEXC_PING_SECONDS = 15
DEPTH_LEVELS = 20
DEPTH_MAX_AGE_MS = 5_000   # a shared-store depth snapshot older than this falls back to top of book

Levels = tuple[tuple[Decimal, Decimal], ...]


def to_levels(rows, size_scale: Decimal = Decimal(1)) -> Levels:
    """[[price, size], ...] or [{"price"/"px", "size"/"sz"}, ...] -> ((price, size), ...)."""
    out = []
    for row in rows or []:
        if isinstance(row, dict):
            price, size = row.get("price", row.get("px")), row.get("size", row.get("sz"))
        else:
            price, size = row[0], row[1]
        price, size = Decimal(str(price)), Decimal(str(size)) * size_scale
        if price > 0 and size > 0:
            out.append((price, size))
        if len(out) >= DEPTH_LEVELS:
            break
    return tuple(out)


def vwap(levels: Levels, quantity: Decimal) -> Decimal | None:
    """Average fill price for ``quantity`` across levels; None when the visible depth is too thin."""
    remaining, cost = quantity, Decimal(0)
    for price, size in levels:
        take = min(size, remaining)
        cost += take * price
        remaining -= take
        if remaining <= 0:
            return cost / quantity
    return None


@dataclass(frozen=True)
class Ticker:
    bid: Decimal
    ask: Decimal
    received_ms: int
    # Exchange timestamp. Variational's public stats carry none.
    source_ms: int | None = None
    bids: Levels = ()
    asks: Levels = ()

    def executable(self, side: str, quantity: Decimal | None = None) -> Decimal | None:
        """Average price to BUY (lift asks) or SELL (hit bids) ``quantity``.

        Venues without published sizes (Variational quotes are already priced for
        the order's size tier) return the top of book.
        """
        top = self.ask if side == "BUY" else self.bid
        levels = self.asks if side == "BUY" else self.bids
        if quantity is None or not levels:
            return top
        return vwap(levels, quantity)

    def marginal(self, side: str, quantity: Decimal) -> Decimal | None:
        """Price of the deepest level ``quantity`` reaches: the worst single fill if the book holds.
        Sizing on it (not the average) keeps every fill profitable and lets an IOC limit at the
        profit boundary fill the whole clip."""
        levels = self.asks if side == "BUY" else self.bids
        if not levels:
            return self.ask if side == "BUY" else self.bid
        remaining = quantity
        for price, size in levels:
            remaining -= size
            if remaining <= 0:
                return price
        return None


class MarketFeed:
    def __init__(self, config: Config, *, clock=now_ms, contract_sizes: dict[str, Decimal] | None = None):
        self.config = config
        self.clock = clock
        # MEXC books count contracts; sizes are converted to base units.
        self.contract_sizes = contract_sizes or {}
        self._tickers: dict[str, Ticker] = {}
        self._healthy: dict[str, bool] = {venue: False for venue in config.venues}
        self._revision = 0
        self._updated = asyncio.Event()

    @property
    def revision(self):
        return self._revision

    async def wait_for_update(self, revision, timeout: float):
        """Coalesce incoming frames; wake immediately, with a timer for housekeeping."""
        if self.revision != revision:
            await asyncio.sleep(0)
            return self.revision
        self._updated.clear()
        try:
            await asyncio.wait_for(self._updated.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        return self.revision

    def update(self, venue: str, bid, ask, *, source_ms: int | None, received_ms: int | None = None,
               bids: Levels = (), asks: Levels = ()):
        self._tickers[venue] = Ticker(Decimal(str(bid)), Decimal(str(ask)),
                                      received_ms if received_ms is not None else self.clock(), source_ms,
                                      tuple(bids), tuple(asks))
        self._healthy[venue] = True
        self._revision += 1
        self._updated.set()

    def update_book(self, venue: str, bids: Levels, asks: Levels, *, source_ms: int | None):
        if bids and asks:
            self.update(venue, bids[0][0], asks[0][0], source_ms=source_ms, bids=bids, asks=asks)

    def set_health(self, venue: str, healthy: bool):
        if self._healthy.get(venue) != healthy:
            self._revision += 1
            self._updated.set()
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
                 "entropy": self._entropy, "lighter": self._lighter, "mexc": self._mexc, "ondo": self._ondo,
                 "variational": self._variational}
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

    @staticmethod
    async def _messages(ws):
        async for message in ws:
            if message.type == aiohttp.WSMsgType.TEXT:
                yield message.json()
            elif message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                return

    async def _aster(self, session):
        contract = await aster_contract(session, self.config.symbol)
        async with session.ws_connect(ASTER_DEPTH_WS.format(stream=str(contract["symbol"]).lower()),
                                      heartbeat=20) as ws:
            async for data in self._messages(ws):
                book = parse_aster_depth(data.get("data", data))
                if book is not None:
                    self.update_book("aster", book["bids"], book["asks"], source_ms=book["ts_ms"])

    async def _hyperliquid(self, session, venue: str = "hyperliquid", coin: str | None = None):
        if coin is None:
            from hydra_basis.adapters.hyperliquid import resolve_hyperliquid_coin
            coin = await resolve_hyperliquid_coin(session, self.config.symbol)  # kPEPE, not KPEPE
        async with session.ws_connect(HYPERLIQUID_WS, heartbeat=20) as ws:
            await ws.send_json({"method": "subscribe",
                                "subscription": {"type": "l2Book", "coin": coin}})
            async for payload in self._messages(ws):
                book = parse_hyperliquid_book(payload)
                if book is not None:
                    self.update_book(venue, book["bids"], book["asks"], source_ms=book["ts_ms"])

    async def _entropy(self, session):
        # Entropy markets are Hyperliquid HIP-3 coins named "io:<SYMBOL>".
        await self._hyperliquid(session, venue="entropy", coin=f"io:{self.config.symbol}")

    async def _arcus(self, session):
        from hydra_basis.adapters.arcus import arcus_base_url, arcus_market_name
        url = arcus_base_url().replace("https://", "wss://") + ARCUS_WS_PATH
        async with session.ws_connect(url, heartbeat=20) as ws:
            await ws.send_json({"type": "subscribe", "channel": "l2Orderbook",
                                "id": arcus_market_name(self.config.symbol), "nLevels": DEPTH_LEVELS})
            async for payload in self._messages(ws):
                book = parse_arcus_book(payload)
                if book is not None:
                    self.update_book("arcus", book["bids"], book["asks"], source_ms=book["ts_ms"])

    async def _lighter(self, session):
        market_id = (await fetch_lighter_market_map(session)).get(self.config.symbol)
        if market_id is None:
            raise RuntimeError(f"symbol not found on lighter: {self.config.symbol}")
        book = LighterBook()
        async with session.ws_connect(LIGHTER_WS, heartbeat=20) as ws:
            await ws.send_json({"type": "subscribe", "channel": f"order_book/{market_id}"})
            async for payload in self._messages(ws):
                if payload.get("type") == "ping":
                    await ws.send_json({"type": "pong"})
                    continue
                if book.apply(payload):
                    bids, asks = book.levels()
                    self.update_book("lighter", bids, asks, source_ms=book.ts_ms)

    async def _mexc(self, session):
        size = self.contract_sizes.get("mexc")
        if size is None:
            raise RuntimeError("mexc feed needs the contract size to read depth in base units")
        async with session.ws_connect(MEXC_WS, heartbeat=20) as ws:
            # Depth, not the ticker: ticker snapshots run 1-3 s behind the book.
            await ws.send_json({"method": "sub.depth.full",
                                "param": {"symbol": mexc_contract_symbol(self.config.symbol), "limit": 5}})
            # MEXC drops connections without application-level pings.
            pinger = asyncio.create_task(_ping_forever(ws, {"method": "ping"}, MEXC_PING_SECONDS))
            try:
                async for payload in self._messages(ws):
                    book = parse_mexc_depth(payload, size)
                    if book is not None:
                        self.update_book("mexc", book["bids"], book["asks"], source_ms=book["ts_ms"])
            finally:
                pinger.cancel()

    async def _ondo(self, session):
        from hydra_basis.adapters.ondo import ondo_market_name, ondo_ssl, ondo_ws_url
        async with session.ws_connect(ondo_ws_url(), heartbeat=20, ssl=ondo_ssl()) as ws:
            await ws.send_json({"op": "subscribe", "channel": "depthBooksPerps",
                                "markets": [ondo_market_name(self.config.symbol)], "limit": DEPTH_LEVELS})
            pinger = asyncio.create_task(_ping_forever(ws, {"op": "ping"}, 15))
            try:
                async for payload in self._messages(ws):
                    for book in parse_ondo_books(payload):
                        self.update_book("ondo", book["bids"], book["asks"], source_ms=book["ts_ms"])
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


# ---------------------------------------------------------------------------
# Message parsers: {"symbol", "bids", "asks", "ts_ms"} with levels best first
# ---------------------------------------------------------------------------

def _book(symbol: str, bids: Levels, asks: Levels, ts_ms: int) -> dict | None:
    if not bids or not asks:
        return None
    return {"symbol": symbol, "bid": bids[0][0], "ask": asks[0][0], "bids": bids, "asks": asks, "ts_ms": ts_ms}


def parse_aster_depth(data: dict) -> dict | None:
    if data.get("e") != "depthUpdate":
        return None
    return _book(str(data.get("s", "")).upper(), to_levels(data.get("b")), to_levels(data.get("a")),
                 int(data.get("T") or data.get("E")))


def parse_aster_book_ticker(data: dict) -> dict | None:
    if data.get("b") is None or data.get("a") is None or data.get("e") not in (None, "bookTicker"):
        return None
    return _book(str(data.get("s", "")).upper(), to_levels([[data["b"], data.get("B", 0)]]),
                 to_levels([[data["a"], data.get("A", 0)]]), int(data.get("T") or data.get("E") or 0))


def parse_hyperliquid_book(payload: dict) -> dict | None:
    if payload.get("channel") != "l2Book":
        return None
    data = payload.get("data") or {}
    levels = data.get("levels") or []
    if len(levels) < 2:
        return None
    return _book(str(data.get("coin", "")).upper(), to_levels(levels[0]), to_levels(levels[1]), int(data["time"]))


def parse_arcus_bbo(payload: dict) -> dict | None:
    """Arcus ``bbo`` frame (snapshot or update); timestamps are microseconds."""
    if payload.get("channel") != "bbo" or payload.get("type") not in {"subscribed", "channel_data"}:
        return None
    contents = payload.get("contents") or {}
    bid, ask = contents.get("bestBid"), contents.get("bestAsk")
    if not bid or not ask or contents.get("timestamp") is None:
        return None
    return _book(str(payload.get("id", "")).upper().removesuffix("-USD"), to_levels([bid]), to_levels([ask]),
                 int(contents["timestamp"]) // 1000)


def parse_arcus_book(payload: dict) -> dict | None:
    """Arcus ``l2Orderbook`` frame: a full snapshot of the requested depth every update."""
    if payload.get("channel") != "l2Orderbook" or payload.get("type") not in {"subscribed", "channel_data"}:
        return None
    contents = payload.get("contents") or {}
    if contents.get("timestamp") is None:
        return None
    return _book(str(payload.get("id", "")).upper().removesuffix("-USD"), to_levels(contents.get("bids")),
                 to_levels(contents.get("asks")), int(contents["timestamp"]) // 1000)


def parse_mexc_depth(payload: dict, contract_size: Decimal | None = None) -> dict | None:
    """MEXC ``push.depth.full``: rows are [price, contracts, orders]; sizes scale to base units.

    Without a contract size the sizes cannot be read, so only the top prices are returned.
    """
    if payload.get("channel") != "push.depth.full" or payload.get("ts") is None:
        return None
    data = payload.get("data") or {}
    bids, asks = data.get("bids") or [], data.get("asks") or []
    if not bids or not asks:
        return None
    symbol = str(payload.get("symbol", "")).upper().removesuffix("_USDT")
    if contract_size:
        return _book(symbol, to_levels(bids, contract_size), to_levels(asks, contract_size), int(payload["ts"]))
    return {"symbol": symbol, "bid": Decimal(str(bids[0][0])), "ask": Decimal(str(asks[0][0])),
            "bids": (), "asks": (), "ts_ms": int(payload["ts"])}


def parse_ondo_books(payload: dict) -> list[dict]:
    """Ondo ``topOfBooksPerps`` / ``depthBooksPerps`` update: a list of book snapshots per market."""
    from hydra_basis.adapters.ondo import ondo_symbol, parse_iso_ms
    if payload.get("type") != "update" or not isinstance(payload.get("data"), list):
        return []
    books = []
    for row in payload["data"]:
        book = _book(ondo_symbol(row.get("market", "")), to_levels(row.get("bids")), to_levels(row.get("asks")),
                     parse_iso_ms(row["time"]))
        if book is not None:
            books.append({**book, "channel": payload.get("channel")})
    return books


class LighterBook:
    """Lighter ``order_book`` channel: a full snapshot, then per-level updates (size 0 removes)."""

    def __init__(self):
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.ts_ms: int | None = None

    def apply(self, payload: dict) -> bool:
        kind = payload.get("type")
        if kind not in {"subscribed/order_book", "update/order_book"}:
            return False
        book = payload.get("order_book") or {}
        if kind == "subscribed/order_book":
            self.bids, self.asks = {}, {}
        for side, rows in ((self.bids, book.get("bids")), (self.asks, book.get("asks"))):
            for row in rows or []:
                price, size = Decimal(str(row["price"])), Decimal(str(row["size"]))
                if size == 0:
                    side.pop(price, None)
                else:
                    side[price] = size
        self.ts_ms = int(payload.get("timestamp") or 0) or self.ts_ms
        return True

    def levels(self) -> tuple[Levels, Levels]:
        bids = tuple(sorted(self.bids.items(), reverse=True)[:DEPTH_LEVELS])
        asks = tuple(sorted(self.asks.items())[:DEPTH_LEVELS])
        if bids and asks and bids[0][0] >= asks[0][0]:
            return (), ()  # crossed after a missed update: unusable until the next snapshot
        return bids, asks


async def _ping_forever(ws, payload: dict, interval: float):
    while True:
        await asyncio.sleep(interval)
        await ws.send_json(payload)


class StoreFeed(MarketFeed):
    """One group's view of the dispatcher's shared quote store (one connection set for all groups)."""

    def __init__(self, config: Config, store, health: dict[str, bool], *, clock=now_ms):
        super().__init__(config, clock=clock)
        self.store, self.health = store, health

    @property
    def revision(self):
        return self.store.version_for(self.config.symbol, self.config.venues)

    async def wait_for_update(self, revision, timeout: float):
        return await self.store.wait_for_update(self.config.symbol, self.config.venues, revision, timeout)

    def update(self, *args, **kwargs):
        raise RuntimeError("StoreFeed is read-only; quotes come from the shared store")

    def ticker(self, venue: str) -> Ticker | None:
        # A recent depth snapshot (subscribed for active symbols) beats the top-of-book quote.
        depth = self.store.get_depth(venue, self.config.symbol)
        if depth is not None and self.clock() - depth["received_ms"] <= DEPTH_MAX_AGE_MS:
            return Ticker(depth["bids"][0][0], depth["asks"][0][0], int(depth["received_ms"]),
                          depth.get("source_ms"), depth["bids"], depth["asks"])
        quote = self.store.get_quote(venue, self.config.symbol)
        if quote is None:
            return None
        return Ticker(Decimal(str(quote["bid"])), Decimal(str(quote["ask"])), int(quote["received_ms"]),
                      None if quote.get("source_ms") is None else int(quote["source_ms"]),
                      tuple(quote.get("bids") or ()), tuple(quote.get("asks") or ()))

    def fresh(self, venue: str) -> Ticker | None:
        self._healthy[venue] = self.health.get(venue, False)
        return super().fresh(venue)
