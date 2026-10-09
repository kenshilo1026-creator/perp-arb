"""Spread dispatcher: scan every enabled venue for cross-venue spreads and run one
strategy group (an ``Engine``) per opportunity, up to ``max_groups`` at a time.

All groups share one set of market-data connections (``QuoteStore``) and, in live
mode, one authenticated adapter per venue.
"""
from __future__ import annotations

import asyncio
import json

from aiohttp import WSMsgType as aiohttp_ws
import math
import uuid
from dataclasses import dataclass, field
from decimal import ROUND_FLOOR, Decimal
from itertools import permutations
from pathlib import Path

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.variational import VARIATIONAL_BASE_URL
from hydra_basis.execution_engine.market_data import select_variational_quote_fields
from hydra_basis.risk_management.persistence import atomic_write_json
from hydra_basis.spread_strategy.broker import (
    PaperVenue, assert_registry_owner, build_venue_adapter, registry_units, strategy_adapter, sync_registry,
)
from hydra_basis.spread_strategy.core import (
    BPS, EPSILON, MAKER_VENUES, VENUES, ZERO, Config, StateStore, entry_allowed, entry_ratio_required, now_ms,
    number, spread_bps,
)
from hydra_basis.spread_strategy.engine import ACTIVE, Engine
from hydra_basis.spread_strategy.feeds import StoreFeed
from hydra_basis.spread_strategy.instruments import Instrument, common_lot, fetch_instrument
from hydra_basis.spread_strategy.locks import SymbolLock, lock_path
from hydra_basis.streams.manager import MarketStateStore
from hydra_basis.symbol_mapping import canonicalize_symbol

MAX_ENGINE_ERRORS = 5
# MEXC ticker snapshots run 1-3 s behind; acceptable for discovery only.
SCAN_ONLY_MAX_LAG_MS = 5_000
SUBSCRIBE_SPACING_SECONDS = 0.05
HISTORY_DEFAULTS = {
    "enabled": True,          # 24h spread history in the dry run and at launch
    "check_bps": 30,          # profile opportunities at or above this spread
    "lookback_hours": 24,
    "persistent_pct": 70,     # >= this % of minutes above check_bps, never converged: structural
    "min_coverage_pct": 50,   # below this, the history is "insufficient"
    "block_persistent": True,  # refuse to launch a group on a structural spread
}
STRATEGY_KEYS = {"clip_interval_seconds", "tick_seconds", "order_timeout_seconds", "requote_interval_seconds",
                 "market_freshness_seconds", "max_transport_lag_seconds", "future_tolerance_seconds",
                 "repair_cooldown_seconds", "variational_poll_seconds", "order_poll_seconds", "stop_loss_usd"}


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Settings:
    venues: tuple[str, ...]
    fees: dict[str, dict[str, Decimal]]
    max_groups: int = 5
    group_notional_usd: Decimal = Decimal("100")
    clip_notional_usd: Decimal = Decimal("50")
    # Clips shrink to the size the books fill profitably, down to this notional.
    min_clip_notional_usd: Decimal = Decimal("20")
    execution_method: str = "taker_taker"
    maker_preference: tuple[str, ...] = ("arcus", "hyperliquid", "entropy", "aster", "lighter", "mexc")
    leverage: dict[str, int] = field(default_factory=dict)
    entry_bps: Decimal = Decimal("40")
    take_profit_bps: Decimal = Decimal("10")
    min_profit_bps: Decimal = Decimal("5")
    slippage_buffer_bps: Decimal = Decimal("5")
    funding_budget_bps: Decimal = Decimal("5")
    confirm_seconds: float = 3.0
    max_book_spread_bps: Decimal = Decimal("20")
    max_price_deviation_pct: Decimal = Decimal("1")
    max_abs_funding_rate_pct: Decimal = Decimal("0.1")
    symbol_allowlist: tuple[str, ...] = ()
    symbol_blocklist: tuple[str, ...] = ()
    reject_cooldown_seconds: float = 600.0
    idle_retire_seconds: float = 1800.0
    scan_seconds: float = 1.0
    status_seconds: float = 30.0
    strategy: dict = field(default_factory=dict)
    history: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.venues or not set(self.venues) <= VENUES or len(set(self.venues)) < 2:
            raise ValueError(f"venues must be two or more of: {', '.join(sorted(VENUES))}")
        if self.max_groups < 1:
            raise ValueError("max_groups must be at least 1")
        if not ZERO < self.clip_notional_usd <= self.group_notional_usd:
            raise ValueError("clip_notional_usd must be positive and at most group_notional_usd")
        if self.execution_method == "maker_taker" and not set(self.maker_preference) & MAKER_VENUES:
            raise ValueError("maker_preference needs a venue that supports post-only quotes")
        unknown = set(self.strategy) - STRATEGY_KEYS
        if unknown:
            raise ValueError(f"unsupported strategy keys: {', '.join(sorted(unknown))}")
        unknown = set(self.history) - set(HISTORY_DEFAULTS)
        if unknown:
            raise ValueError(f"unsupported history keys: {', '.join(sorted(unknown))}")
        object.__setattr__(self, "history", {**HISTORY_DEFAULTS, **self.history})
        missing = [venue for venue in self.venues if venue not in self.fees]
        if missing:
            raise ValueError(f"fees missing for: {', '.join(missing)}")
        # Validate thresholds and fees once through the strategy's own config rules.
        self.group_config("CHECK", self.venues[0], self.venues[1], Decimal("1"), Decimal("1"))

    @classmethod
    def load(cls, path: Path) -> "Settings":
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        for key in ("group_notional_usd", "clip_notional_usd", "min_clip_notional_usd", "entry_bps", "take_profit_bps", "min_profit_bps",
                    "slippage_buffer_bps", "funding_budget_bps", "max_book_spread_bps",
                    "max_price_deviation_pct", "max_abs_funding_rate_pct"):
            if key in payload:
                payload[key] = number(payload[key])
        payload["fees"] = {v: {role: number(rate) for role, rate in rates.items()}
                           for v, rates in payload["fees"].items()}
        for key in ("venues", "maker_preference"):
            if key in payload:
                payload[key] = tuple(str(item).lower() for item in payload[key])
        for key in ("symbol_allowlist", "symbol_blocklist"):
            if key in payload:
                payload[key] = tuple(str(item).upper() for item in payload[key])
        return cls(**payload)

    def maker_for(self, short_venue: str, long_venue: str) -> str | None:
        if self.execution_method != "maker_taker":
            return None
        for venue in self.maker_preference:
            if venue in (short_venue, long_venue) and venue in MAKER_VENUES:
                return venue
        return next((venue for venue in (short_venue, long_venue) if venue in MAKER_VENUES), None)

    def group_config(self, symbol: str, short_venue: str, long_venue: str, total: Decimal, clip: Decimal) -> Config:
        maker = self.maker_for(short_venue, long_venue)
        payload = dict(
            symbol=symbol, short_venue=short_venue, long_venue=long_venue,
            total_quantity=total, clip_quantity=clip,
            entry_bps=self.entry_bps, take_profit_bps=self.take_profit_bps,
            fees={venue: self.fees[venue] for venue in (short_venue, long_venue)},
            execution_method="maker_taker" if maker else "taker_taker", maker_venue=maker,
            min_profit_bps=self.min_profit_bps, slippage_buffer_bps=self.slippage_buffer_bps,
            funding_budget_bps=self.funding_budget_bps, min_clip_notional_usd=self.min_clip_notional_usd,
            short_leverage=int(self.leverage.get(short_venue, 1)),
            long_leverage=int(self.leverage.get(long_venue, 1)),
        )
        overrides = dict(self.strategy)
        if overrides.get("stop_loss_usd") is not None:
            overrides["stop_loss_usd"] = number(overrides["stop_loss_usd"])
        return Config(**payload, **overrides)


# ---------------------------------------------------------------------------
# Shared quotes
# ---------------------------------------------------------------------------

class QuoteStore(MarketStateStore):
    """The scanner's market state plus each quote's receipt time, for strategy freshness checks."""

    def __init__(self, *, clock=now_ms):
        super().__init__()
        self.clock = clock
        self._live: dict[str, dict[str, dict]] = {}
        self._scan: dict[str, dict[str, dict]] = {}
        self._depth: dict[str, dict[str, dict]] = {}

    def update_depth(self, venue: str, symbol: str, bids, asks, *, source_ms: int | None):
        """A multi-level book for a watched symbol (best first, base units)."""
        if bids and asks:
            self._depth.setdefault(venue, {})[str(symbol).upper()] = {
                "bids": tuple(bids), "asks": tuple(asks), "received_ms": self.clock(), "source_ms": source_ms}

    def get_depth(self, venue: str, symbol: str) -> dict | None:
        return self._depth.get(venue, {}).get(symbol)

    def update_quotes(self, venue, quotes, *, timestamp_ms=None):
        super().update_quotes(venue, quotes, timestamp_ms=timestamp_ms)
        received = self.clock()
        for symbol, quote in quotes.items():
            bid, ask = quote.get("bid"), quote.get("ask")
            if not bid or not ask:
                continue
            source = int(quote.get("ts_ms") or 0) or None
            entry = {"bid": bid, "ask": ask, "received_ms": received, "source_ms": source,
                     "scan_only": bool(quote.get("scan_only")),
                     # Visible levels with sizes (top of book at least), when the venue publishes them.
                     "bids": tuple(quote.get("bids") or ()), "asks": tuple(quote.get("asks") or ())}
            # Scan-only quotes (lagging tickers) help discovery but never drive orders.
            target = self._scan if entry["scan_only"] else self._live
            target.setdefault(venue, {})[str(symbol).upper()] = entry

    def get_quote(self, venue: str, symbol: str) -> dict | None:
        return self._live.get(venue, {}).get(symbol)

    def live_quotes(self, venue: str) -> dict[str, dict]:
        return self._live.get(venue, {})

    def scan_quotes(self, venue: str) -> dict[str, dict]:
        return {**self._scan.get(venue, {}), **self._live.get(venue, {})}

    def funding(self, venue: str, symbol: str) -> float | None:
        ctx = self.get_asset_ctx_snapshot(venue).get(symbol)
        return None if ctx is None else ctx.get("funding")


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Opportunity:
    symbol: str
    short_venue: str
    long_venue: str
    entry_bps: Decimal
    mid: Decimal
    short_bid: Decimal = ZERO
    short_ask: Decimal = ZERO
    long_bid: Decimal = ZERO
    long_ask: Decimal = ZERO
    # False only in dry-run listings of near misses; ``blocked_by`` says why.
    qualifies: bool = True
    blocked_by: str = ""

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.symbol, self.short_venue, self.long_venue)


def _fresh(quote: dict, now: int, settings: Settings) -> bool:
    template = settings.strategy
    freshness = float(template.get("market_freshness_seconds", 15)) * 1000
    future = float(template.get("future_tolerance_seconds", 2)) * 1000
    max_lag = float(template.get("max_transport_lag_seconds", 3)) * 1000
    if quote.get("scan_only"):
        max_lag = max(max_lag, SCAN_ONLY_MAX_LAG_MS)
    source = quote["source_ms"] if quote["source_ms"] is not None else quote["received_ms"]
    age, lag = now - source, quote["received_ms"] - source
    return -future <= age <= freshness and lag <= max_lag


def native_symbol(symbol: str, venue: str) -> bool:
    """Only trade a symbol whose name means the same contract everywhere.

    Aliases such as 1000PEPE / kPEPE / PEPE map to one canonical name but differ
    in units by 1000x, which would break equal-quantity hedging.
    """
    return canonicalize_symbol(symbol, venue=venue) == symbol


def find_opportunities(settings: Settings, store: QuoteStore, health: dict[str, bool], now: int,
                       *, exclude: set[str] = frozenset(), include_near_misses: bool = False) -> list[Opportunity]:
    books: dict[str, dict[str, tuple[Decimal, Decimal]]] = {}
    for venue in settings.venues:
        if not health.get(venue):
            continue
        for symbol, quote in store.scan_quotes(venue).items():
            if symbol in exclude or symbol in settings.symbol_blocklist:
                continue
            if settings.symbol_allowlist and symbol not in settings.symbol_allowlist:
                continue
            if not native_symbol(symbol, venue) or not _fresh(quote, now, settings):
                continue
            bid, ask = Decimal(str(quote["bid"])), Decimal(str(quote["ask"]))
            if not ZERO < bid <= ask or (ask - bid) / ((ask + bid) / 2) * BPS > settings.max_book_spread_bps:
                continue
            books.setdefault(symbol, {})[venue] = (bid, ask)
    ratios: dict[tuple[str, str], Decimal] = {}
    funding_cap = float(settings.max_abs_funding_rate_pct) / 100
    found = []
    for symbol, venue_books in books.items():
        for short_venue, long_venue in permutations(venue_books, 2):
            short_bid, short_ask = venue_books[short_venue]
            long_bid, long_ask = venue_books[long_venue]
            short_mid, long_mid = (short_bid + short_ask) / 2, (long_bid + long_ask) / 2
            # Units/contract sanity: genuinely identical contracts trade close together.
            if abs(short_mid - long_mid) / ((short_mid + long_mid) / 2) * 100 > settings.max_price_deviation_pct:
                continue
            pair = (short_venue, long_venue)
            if pair not in ratios:
                ratios[pair] = entry_ratio_required(settings.group_config("CHECK", *pair, ONE_, ONE_))
            blocked_by = ""
            if short_bid / long_ask < ratios[pair]:
                blocked_by = "entry_gate"
            elif any(abs(rate) > funding_cap for venue in pair
                     if (rate := store.funding(venue, symbol)) is not None):
                blocked_by = "funding"
            if blocked_by and not (include_near_misses and short_bid > long_ask):
                continue
            found.append(Opportunity(symbol, short_venue, long_venue,
                                     (short_bid - long_ask) / long_ask * BPS, (short_mid + long_mid) / 2,
                                     short_bid, short_ask, long_bid, long_ask,
                                     qualifies=not blocked_by, blocked_by=blocked_by))
    found.sort(key=lambda item: item.entry_bps, reverse=True)
    return found


ONE_ = Decimal("1")


def size_group(settings: Settings, mid: Decimal, instruments: list[Instrument]) -> tuple[Decimal, Decimal]:
    lot = common_lot(instruments) or Decimal("0.000001")
    def floor_lots(usd: Decimal) -> Decimal:
        return (usd / mid / lot).to_integral_value(rounding=ROUND_FLOOR) * lot
    # A whole number of equal clips: a leftover sliver would fall below venue minimums.
    clip = floor_lots(settings.clip_notional_usd)
    if clip <= ZERO:
        raise RuntimeError(f"clip notional is below one lot ({lot}) at price {mid}")
    count = int(settings.group_notional_usd / settings.clip_notional_usd)
    return clip * count, clip


# ---------------------------------------------------------------------------
# Market data runners
# ---------------------------------------------------------------------------

async def _build_runners(venue: str, session, store: QuoteStore, settings: Settings, watched) -> list:
    from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_meta
    from hydra_basis.adapters.lighter import fetch_lighter_market_map
    from hydra_basis.streams.manager import AsterStreamRunner, HyperliquidStreamRunner, LighterStreamRunner
    if venue == "hyperliquid":
        rows = await fetch_hyperliquid_meta(session)
        symbols = [str(row.get("name") or "").upper() for row in rows]
        active = [str(row["name"]) for row in rows if row.get("name") and not row.get("isDelisted")]
        runners = [HyperliquidStreamRunner(session, store, symbols), HyperliquidBooksRunner(session, store, active)]
    elif venue == "entropy":
        rows = await fetch_hyperliquid_meta(session, "io")
        runners = [EntropyBooksRunner(session, store, [str(row["name"]) for row in rows
                                                      if row.get("name") and not row.get("isDelisted")])]
    elif venue == "arcus":
        from hydra_basis.adapters.arcus import fetch_arcus_markets
        markets = [str(m["marketDisplayName"]) for m in await fetch_arcus_markets(session)
                   if m.get("status") == "ONLINE"]
        runners = [ArcusRunner(session, store, markets, watched)]
    elif venue == "lighter":
        runners = [LighterStreamRunner(session, store),
                   LighterRunner(session, store, await fetch_lighter_market_map(session), watched)]
    elif venue == "aster":
        runners = [AsterStreamRunner(session, store), AsterRunner(session, store, watched)]
    elif venue == "mexc":
        details = await fetch_json(session, "GET", "https://contract.mexc.com/api/v1/contract/detail")
        sizes = {str(row["baseCoin"]).upper(): Decimal(str(row["contractSize"])) for row in details.get("data") or []
                 if row.get("state") == 0 and row.get("apiAllowed", True)
                 and str(row.get("quoteCoin") or "").upper() == "USDT" and row.get("contractSize")}
        runners = [MexcRunner(session, store, sizes, watched)]
    else:
        return [VariationalRunner(session, store, settings)]
    for runner in runners:
        await runner.initialize()
    return runners


class _SubscribingRunner:
    """WebSocket runner that subscribes gradually (bursts make venues drop the connection) and adds
    a depth subscription for each newly watched symbol (active groups, pending launches)."""
    url = ""
    ping_message: dict | None = None
    ping_seconds = 15

    def __init__(self, session, store: QuoteStore, watched=lambda: ()):
        self.session, self.store, self.ws, self._tasks = session, store, None, []
        self.watched, self.depth_subscribed = watched, set()

    async def initialize(self):
        self.ws = await self.session.ws_connect(self.url, heartbeat=20)
        self._tasks.append(asyncio.create_task(self.subscribe()))
        self._tasks.append(asyncio.create_task(self.watch_depth()))

    async def subscribe(self):
        raise NotImplementedError

    def depth_messages(self, symbol: str) -> list[dict] | None:
        """Messages subscribing ``symbol``'s depth; None while that is not possible yet."""
        return []

    async def watch_depth(self):
        seconds = 0
        while True:
            for symbol in set(self.watched()) - self.depth_subscribed:
                messages = self.depth_messages(symbol)
                if messages is None:
                    continue
                for message in messages:
                    await self.ws.send_json(message)
                self.depth_subscribed.add(symbol)
            seconds += 1
            if self.ping_message is not None and seconds % self.ping_seconds == 0:
                await self.ws.send_json(self.ping_message)
            await asyncio.sleep(1)

    async def send_spaced(self, messages):
        for message in messages:
            await self.ws.send_json(message)
            await asyncio.sleep(SUBSCRIBE_SPACING_SECONDS)

    async def pump_once(self):
        for task in self._tasks:
            if task.done() and task.exception():
                raise task.exception()
        message = await self.ws.receive()
        if message.type in {aiohttp_ws.CLOSED, aiohttp_ws.CLOSING, aiohttp_ws.ERROR}:
            raise RuntimeError(f"{type(self).__name__} websocket closed")
        if message.type == aiohttp_ws.TEXT:
            self.handle(message.json())

    def handle(self, payload: dict):
        raise NotImplementedError

    async def close(self):
        for task in self._tasks:
            task.cancel()
        if self.ws is not None:
            await self.ws.close()


class HyperliquidBooksRunner(_SubscribingRunner):
    """l2Book for every coin: the book already carries 20 levels a side."""
    url = "wss://api.hyperliquid.xyz/ws"
    venue = "hyperliquid"

    def __init__(self, session, store, coins: list[str]):
        super().__init__(session, store)
        self.coins = coins  # exchange spelling (e.g. kPEPE, io:OAI), delisted excluded

    async def subscribe(self):
        await self.send_spaced({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}
                               for coin in self.coins)

    def handle(self, payload):
        from hydra_basis.spread_strategy.feeds import parse_hyperliquid_book
        book = parse_hyperliquid_book(payload)
        if book is not None:
            # HIP-3 coins carry a dex prefix ("IO:OAI"); the strategy symbol is the part after it.
            symbol = book["symbol"].partition(":")[2] or book["symbol"]
            self.store.update_quotes(self.venue, {symbol: book})


class EntropyBooksRunner(HyperliquidBooksRunner):
    """Entropy's HIP-3 dex "io": order books by WebSocket, funding by polling asset contexts."""
    venue = "entropy"

    async def subscribe(self):
        self._tasks.append(asyncio.create_task(self.poll_funding()))
        await super().subscribe()

    async def poll_funding(self):
        while True:
            meta, ctxs = await fetch_json(self.session, "POST", "https://api.hyperliquid.xyz/info",
                                          json={"type": "metaAndAssetCtxs", "dex": "io"})
            self.store.update_asset_ctxs("entropy", {
                str(row["name"]).upper().partition(":")[2]: {"funding": float(ctx.get("funding") or 0)}
                for row, ctx in zip(meta.get("universe", []), ctxs)})
            await asyncio.sleep(60)


class AsterRunner(_SubscribingRunner):
    """All-symbol bookTicker (top of book with size) plus depth10 for watched symbols."""
    url = "wss://fstream.asterdex.com/stream?streams=!bookTicker"

    def __init__(self, session, store, watched=lambda: ()):
        super().__init__(session, store, watched)
        self.raw_by_symbol: dict[str, str] = {}
        self._request_id = 0

    async def subscribe(self):
        return None

    def depth_messages(self, symbol):
        raw = self.raw_by_symbol.get(symbol)
        if raw is None:
            return None  # not seen on the ticker stream yet
        self._request_id += 1
        return [{"method": "SUBSCRIBE", "params": [f"{raw.lower()}@depth10@100ms"], "id": self._request_id}]

    def handle(self, payload):
        from hydra_basis.adapters.aster import normalize_aster_symbol
        from hydra_basis.spread_strategy.feeds import parse_aster_book_ticker, parse_aster_depth
        data = payload.get("data") or {}
        if str(payload.get("stream", "")).endswith("@depth10@100ms"):
            book = parse_aster_depth(data)
            if book is not None:
                self.store.update_depth("aster", normalize_aster_symbol(book["symbol"]), book["bids"], book["asks"],
                                        source_ms=book["ts_ms"])
            return
        book = parse_aster_book_ticker(data)
        if book is not None:
            symbol = normalize_aster_symbol(book["symbol"])
            if symbol:
                self.raw_by_symbol[symbol] = book["symbol"]
                self.store.update_quotes("aster", {symbol: book})


class LighterRunner(_SubscribingRunner):
    """Tickers for every market (top of book with size) plus order_book deltas for watched markets."""
    url = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"

    def __init__(self, session, store, market_map: dict[str, int], watched=lambda: ()):
        super().__init__(session, store, watched)
        self.market_map = market_map
        self.symbol_by_id = {market_id: symbol for symbol, market_id in market_map.items()}
        self.books: dict[str, object] = {}

    async def subscribe(self):
        await self.send_spaced({"type": "subscribe", "channel": f"ticker/{market_id}"}
                               for market_id in self.market_map.values())

    def depth_messages(self, symbol):
        from hydra_basis.spread_strategy.feeds import LighterBook
        market_id = self.market_map.get(symbol)
        if market_id is None:
            return []
        self.books[symbol] = LighterBook()
        return [{"type": "subscribe", "channel": f"order_book/{market_id}"}]

    def handle(self, payload):
        from hydra_basis.spread_strategy.feeds import to_levels
        kind = str(payload.get("type") or "")
        if kind == "ping":
            self._tasks.append(asyncio.create_task(self.ws.send_json({"type": "pong"})))
            return
        channel = str(payload.get("channel") or "")
        market_id = channel.partition(":")[2]
        symbol = self.symbol_by_id.get(int(market_id)) if market_id.isdigit() else None
        if symbol is None:
            return
        if kind.endswith("/order_book"):
            book = self.books.get(symbol)
            if book is not None and book.apply(payload):
                bids, asks = book.levels()
                self.store.update_depth("lighter", symbol, bids, asks, source_ms=book.ts_ms)
            return
        ticker = payload.get("ticker") or {}
        bid, ask = ticker.get("b") or {}, ticker.get("a") or {}
        if bid.get("price") is None or ask.get("price") is None or payload.get("timestamp") is None:
            return
        self.store.update_quotes("lighter", {symbol: {
            "bid": bid["price"], "ask": ask["price"], "ts_ms": int(payload["timestamp"]),
            "bids": to_levels([bid]), "asks": to_levels([ask])}})


class MexcRunner(_SubscribingRunner):
    """Tickers for every symbol (scan-only: no sizes, 1-3 s behind) plus depth for watched symbols."""
    url = "wss://contract.mexc.com/edge"
    ping_message = {"method": "ping"}

    def __init__(self, session, store, contract_sizes: dict[str, Decimal], watched=lambda: ()):
        super().__init__(session, store, watched)
        self.contract_sizes = contract_sizes

    async def subscribe(self):
        from hydra_basis.adapters.mexc import mexc_contract_symbol
        await self.send_spaced({"method": "sub.ticker", "param": {"symbol": mexc_contract_symbol(symbol)}}
                               for symbol in sorted(self.contract_sizes))

    def depth_messages(self, symbol):
        from hydra_basis.adapters.mexc import mexc_contract_symbol
        if symbol not in self.contract_sizes:
            return []
        return [{"method": "sub.depth.full", "param": {"symbol": mexc_contract_symbol(symbol), "limit": 5}}]

    def handle(self, payload):
        from hydra_basis.spread_strategy.feeds import parse_mexc_depth
        from hydra_basis.streams.mexc import parse_push_ticker_message
        if payload.get("channel") == "push.ticker":
            parsed = parse_push_ticker_message(payload)
            self.store.update_asset_ctxs("mexc", parsed)
            self.store.update_quotes("mexc", {symbol: {**row, "scan_only": True} for symbol, row in parsed.items()})
            return
        symbol = str(payload.get("symbol", "")).upper().removesuffix("_USDT")
        book = parse_mexc_depth(payload, self.contract_sizes.get(symbol))
        if book is not None:
            # Depth is MEXC's only trade-grade quote; sizes are converted from contracts.
            self.store.update_quotes("mexc", {book["symbol"]: book})


class ArcusRunner(_SubscribingRunner):
    """BBO (top of book with size) per market over one socket (cap 100 subscriptions), l2Orderbook for
    watched markets, funding by polling markets."""
    MAX_SUBSCRIPTIONS = 100
    DEPTH_SLOTS = 30

    def __init__(self, session, store, markets: list[str], watched=lambda: ()):
        super().__init__(session, store, watched)
        from hydra_basis.adapters.arcus import arcus_base_url
        self.url = arcus_base_url().replace("https://", "wss://") + "/v1/ws"
        self.markets = markets[: self.MAX_SUBSCRIPTIONS - self.DEPTH_SLOTS]

    async def subscribe(self):
        self._tasks.append(asyncio.create_task(self.poll_funding()))
        await self.send_spaced({"type": "subscribe", "channel": "bbo", "id": market} for market in self.markets)

    def depth_messages(self, symbol):
        from hydra_basis.adapters.arcus import arcus_market_name
        from hydra_basis.spread_strategy.feeds import DEPTH_LEVELS
        if len(self.depth_subscribed) >= self.DEPTH_SLOTS:
            return []
        return [{"type": "subscribe", "channel": "l2Orderbook", "id": arcus_market_name(symbol), "nLevels": DEPTH_LEVELS}]

    async def poll_funding(self):
        from hydra_basis.adapters.arcus import fetch_arcus_markets
        while True:
            self.store.update_asset_ctxs("arcus", {
                str(m["baseAsset"]).upper(): {"funding": float(m.get("fundingRate") or 0)}
                for m in await fetch_arcus_markets(self.session)})
            await asyncio.sleep(60)

    def handle(self, payload):
        from hydra_basis.spread_strategy.feeds import parse_arcus_bbo, parse_arcus_book
        book = parse_arcus_book(payload)
        if book is not None:
            self.store.update_depth("arcus", book["symbol"], book["bids"], book["asks"], source_ms=book["ts_ms"])
            return
        book = parse_arcus_bbo(payload)
        if book is not None:
            self.store.update_quotes("arcus", {book["symbol"]: book})


class VariationalRunner:
    """Variational has no public stream: poll every listing's size-tiered quote."""

    def __init__(self, session, store: QuoteStore, settings: Settings):
        self.session, self.store, self.settings = session, store, settings

    async def initialize(self):
        return None

    async def pump_once(self):
        data = await fetch_json(self.session, "GET", f"{VARIATIONAL_BASE_URL}/metadata/stats")
        quotes = {}
        for listing in data.get("listings") or []:
            ticker = str(listing.get("ticker") or "").upper()
            try:
                bid, ask = select_variational_quote_fields(listing, float(self.settings.clip_notional_usd))
            except RuntimeError:
                continue
            quotes[ticker] = {"bid": bid, "ask": ask, "ts_ms": 0}
        self.store.update_quotes("variational", quotes)
        await asyncio.sleep(float(self.settings.strategy.get("variational_poll_seconds", 2)))

    async def close(self):
        return None


async def run_venue_feed(venue: str, session, store: QuoteStore, health: dict[str, bool], settings: Settings,
                         emit, watched=lambda: ()) -> None:
    delay = 1.0
    while True:
        runners, tasks = [], []
        try:
            runners = await _build_runners(venue, session, store, settings, watched)
            health[venue] = True
            emit({"event": "feed_connected", "venue": venue})
            delay = 1.0
            tasks = [asyncio.create_task(_pump(runner)) for runner in runners]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                task.result()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            emit({"event": "feed_error", "venue": venue, "error": str(exc)[:200]})
        finally:
            health[venue] = False
            for task in tasks:
                task.cancel()
            for runner in runners:
                try:
                    await runner.close()
                except Exception:
                    pass
        await asyncio.sleep(delay)
        delay = min(30.0, delay * 2)


async def _pump(runner):
    while True:
        await runner.pump_once()
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------

class Group:
    def __init__(self, dispatcher: "Dispatcher", gid: str, config: Config, state_path: Path, started_ms: int):
        self.dispatcher, self.id, self.config = dispatcher, gid, config
        self.state_path, self.started_ms = state_path, started_ms
        self.lock = SymbolLock(lock_path(config.symbol, dispatcher.mode))
        self.engine: Engine | None = None
        self.task: asyncio.Task | None = None
        self.error: str | None = None

    @property
    def status(self) -> str:
        if self.engine is None or (self.task is None and self.error):
            return "BLOCKED"
        return self.engine.state.status

    async def start(self, instruments: dict[str, Instrument], *, resume: bool = False, run: bool = True):
        d, c = self.dispatcher, self.config
        self.lock.acquire()
        try:
            store = StateStore(self.state_path)
            state = store.load(c, live=d.live)
            on_exposure = None
            if d.live:
                assert_registry_owner(d.registry_path, c, state)
                units = registry_units(instruments)
                on_exposure = lambda current: sync_registry(d.registry_path, c, current, units)
                adapters = {venue: strategy_adapter(venue, d.venue_adapter(venue), instruments[venue])
                            for venue in c.venues}
            feed = StoreFeed(c, d.store, d.health, clock=d.clock)
            if not d.live:
                adapters = {venue: PaperVenue(venue, feed) for venue in c.venues}
            engine = Engine(c, state, store, feed, adapters, instruments, live=d.live, on_exposure=on_exposure,
                            clock=d.clock, log=lambda event: d.emit({"group": self.id, "symbol": c.symbol, **event}))
            if state.status == "STOPPED" or (state.status == "PAUSED" and not resume):
                # A paused group keeps its slot and symbol lock until it is resumed and finishes.
                self.engine = engine
                if state.status == "STOPPED":
                    self.lock.release()
                return
            self.engine = engine  # kept on failure so unresolved orders can be settled
            await engine.start(resume=resume)
        except BaseException as exc:
            self.error = str(exc)[:300]
            self.lock.release()
            raise
        self.error = None
        if run:
            self.task = asyncio.create_task(self.run())

    async def run(self):
        engine, errors = self.engine, 0
        while engine.state.status in ACTIVE:
            try:
                await engine.step()
                errors = 0
                if self.retire_due():
                    await self.retire()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors += 1
                engine.log("engine_error", error=str(exc)[:200], consecutive=errors)
                if errors >= MAX_ENGINE_ERRORS:
                    await engine.pause(f"{MAX_ENGINE_ERRORS} consecutive engine errors: {str(exc)[:160]}")
            if engine.state.status in ACTIVE:
                await asyncio.sleep(self.config.tick_seconds)
        self.dispatcher.group_finished(self)

    def last_activity_ms(self) -> int:
        fills = [order.updated_ms for order in self.engine.state.orders if number(order.executed_quantity) > 0]
        return max([self.started_ms, *fills])

    def retire_due(self) -> bool:
        engine = self.engine
        return (engine.state.status == "RUNNING" and engine.matched() <= EPSILON
                and abs(engine.imbalance()) <= EPSILON and not engine.non_quote_open_orders()
                and self.dispatcher.clock() - self.last_activity_ms()
                >= self.dispatcher.settings.idle_retire_seconds * 1000)

    async def retire(self):
        engine = self.engine
        for order in engine.quotes().values():
            if not await engine.cancel_quote(order):
                return
        if engine.matched() > EPSILON or abs(engine.imbalance()) > EPSILON:
            return  # a quote filled while cancelling: keep managing the position
        engine.state.status = "STOPPED"
        engine.state.reason = f"retired: flat with no fills for {self.dispatcher.settings.idle_retire_seconds:.0f}s"
        engine.log("retired", reason=engine.state.reason)
        engine.save()

    async def shutdown(self):
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except (asyncio.CancelledError, Exception):
                pass
        if self.engine is not None and self.engine.state.status in ACTIVE:
            await self.engine.shutdown()
        self.lock.release()

    def snapshot(self) -> dict:
        payload = {"group": self.id, "symbol": self.config.symbol, "short": self.config.short_venue,
                   "long": self.config.long_venue, "status": self.status}
        if self.engine is not None:
            payload |= {key: value for key, value in self.engine.snapshot().items() if key != "status"}
            payload["reason"] = self.engine.state.reason
        if self.error:
            payload["error"] = self.error
        return payload


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

class Dispatcher:
    def __init__(self, settings: Settings, *, live: bool, data_dir: Path, registry_path: Path,
                 store: QuoteStore | None = None, clock=now_ms, emit=None, notify=None,
                 instrument_loader=None, adapter_factory=None, history=None):
        self.settings, self.live = settings, live
        self.mode = "live" if live else "paper"
        self.data_dir, self.registry_path = data_dir, registry_path
        self.clock = clock
        self.store = store or QuoteStore(clock=clock)
        self.health: dict[str, bool] = {venue: False for venue in settings.venues}
        self.emit = emit or (lambda payload: print(json.dumps(payload, default=str), flush=True))
        self.notify = notify or (lambda message: None)
        self.instrument_loader = instrument_loader
        self.adapter_factory = adapter_factory
        self.groups: dict[str, Group] = {}
        self.adapters: dict[str, object] = {}
        self.first_seen: dict[tuple, int] = {}
        self.rejected_until: dict[str, int] = {}
        self.pending: set[str] = set()
        self.dry_run_watch: set[str] = set()  # dry-run candidates whose depth is subscribed
        self.trade_quote_timeout_seconds = 10.0
        self.index_path = data_dir / f"dispatcher.{self.mode}.json"
        self.history = history
        if self.history is None and settings.history["enabled"]:
            from hydra_basis.spread_strategy.history import MinuteRecorder, SpreadHistory
            h = settings.history
            self.history = SpreadHistory(
                recorder=MinuteRecorder(data_dir / "minute_mids.json.gz"), lookback_hours=float(h["lookback_hours"]),
                threshold_bps=float(h["check_bps"]), take_profit_bps=float(settings.take_profit_bps),
                persistent_pct=float(h["persistent_pct"]), min_coverage_pct=float(h["min_coverage_pct"]),
                clock=clock)

    # -------------------------------------------------------------- persistence

    def save_index(self):
        atomic_write_json(self.index_path, {"groups": {
            gid: {"config": group.config.to_dict(), "state": str(group.state_path), "started_ms": group.started_ms}
            for gid, group in self.groups.items()}})

    async def restore(self):
        if not self.index_path.exists():
            return
        saved = json.loads(self.index_path.read_text(encoding="utf-8"))["groups"]
        for gid, item in saved.items():
            config = Config.from_dict(item["config"])
            group = Group(self, gid, config, Path(item["state"]), int(item["started_ms"]))
            self.groups[gid] = group
            try:
                instruments = await self.load_instruments(config)
                await group.start(instruments)
            except Exception as exc:
                group.error = str(exc)[:300]
                self.emit({"event": "group_blocked", "group": gid, "symbol": config.symbol, "error": group.error})
                self.notify(f"價差組 {gid} {config.symbol} 無法恢復：{group.error}")
                continue
            if group.engine.state.status == "STOPPED":
                del self.groups[gid]
        self.save_index()

    async def settle(self, gid: str, order_id: str, quantity: Decimal, price: Decimal | None):
        group = self.groups.get(gid)
        if group is None or group.engine is None or group.task is not None:
            raise ValueError(f"group {gid} is not stopped/blocked; nothing to settle")
        group.engine.settle_order(order_id, quantity, price)

    async def resume(self, gid: str):
        group = self.groups.get(gid)
        if group is None or group.task is not None:
            raise ValueError(f"group {gid} is not paused or blocked")
        await group.start(await self.load_instruments(group.config), resume=True)
        self.emit({"event": "group_resumed", "group": gid, "symbol": group.config.symbol})

    # -------------------------------------------------------------- venues

    def venue_adapter(self, venue: str):
        if venue not in self.adapters:
            factory = self.adapter_factory or self._build_live_adapter
            self.adapters[venue] = factory(venue)
        return self.adapters[venue]

    def _build_live_adapter(self, venue: str):
        loader = None
        if venue == "lighter":
            async def loader(symbol):
                from hydra_basis.execution_engine.lighter_live import fetch_lighter_orderbook_live
                quote = self.store.get_quote("lighter", symbol)
                if quote is None or not _fresh(quote, self.clock(), self.settings):
                    return await fetch_lighter_orderbook_live(symbol)
                return {"bid": quote["bid"], "ask": quote["ask"], "ts_ms": quote["received_ms"]}
        return build_venue_adapter(venue, leverage=int(self.settings.leverage.get(venue, 1)),
                                   order_timeout_seconds=float(self.settings.strategy.get("order_timeout_seconds", 20)),
                                   broker_url=getattr(self, "variational_broker_url", None),
                                   orderbook_loader=loader)

    async def load_instruments(self, config: Config) -> dict[str, Instrument]:
        loader = self.instrument_loader or self._fetch_instrument
        return {venue: await loader(venue, config.symbol) for venue in config.venues}

    async def _fetch_instrument(self, venue: str, symbol: str) -> Instrument:
        import aiohttp
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            return await fetch_instrument(session, venue, symbol)

    # -------------------------------------------------------------- scanning

    def active_symbols(self) -> set[str]:
        """Symbols that need trade-grade quotes (e.g. MEXC depth): active groups and launches."""
        return {group.config.symbol for group in self.groups.values()} | self.pending | self.dry_run_watch

    async def wait_for_trade_quotes(self, config: Config):
        feed = StoreFeed(config, self.store, self.health, clock=self.clock)
        for _ in range(max(1, int(self.trade_quote_timeout_seconds / 0.2))):
            if feed.fresh_pair() is not None:
                return
            await asyncio.sleep(0.2)
        raise RuntimeError("no fresh tradeable quotes from both venues")

    def depth_entry(self, config: Config, quantity: Decimal) -> tuple[tuple[Decimal, Decimal] | None, str]:
        """Average entry fill prices (short sell, long buy) for ``quantity`` from visible depth."""
        books = StoreFeed(config, self.store, self.health, clock=self.clock).fresh_pair()
        if books is None:
            return None, "no fresh quotes"
        short = books[config.short_venue].executable("SELL", quantity)
        long = books[config.long_venue].executable("BUY", quantity)
        if short is None or long is None:
            return None, "visible depth too thin for the clip"
        return (short, long), "ok"

    async def wait_for_depth(self, config: Config, instruments: dict, quantity: Decimal):
        """Launch only if some clip (at least the minimum size) is profitable against visible depth;
        depth subscriptions take a moment to arrive."""
        from hydra_basis.spread_strategy.core import Leg
        from hydra_basis.spread_strategy.engine import fit_clip
        reason = "no fresh quotes"
        for _ in range(max(1, int(self.trade_quote_timeout_seconds / 0.2))):
            books = StoreFeed(config, self.store, self.health, clock=self.clock).fresh_pair()
            if books is not None:
                if fit_clip(config, instruments, books, "entry", quantity, Leg(), Leg()) is not None:
                    return
                reason = "no order size the books fill profitably"
            await asyncio.sleep(0.2)
        raise RuntimeError(f"not tradeable (clip up to {quantity}): {reason}")

    async def scan_once(self):
        now = self.clock()
        if self.history is not None:
            self.history.record_store(self.store, now)
        excluded = self.active_symbols() | {symbol for symbol, until in self.rejected_until.items() if until > now}
        found = find_opportunities(self.settings, self.store, self.health, now, exclude=excluded)
        seen = {opportunity.key for opportunity in found}
        self.first_seen = {key: since for key, since in self.first_seen.items() if key in seen}
        launched: set[str] = set()
        for opportunity in found:
            since = self.first_seen.setdefault(opportunity.key, now)
            if len(self.groups) >= self.settings.max_groups:
                break
            if opportunity.symbol in launched or now - since < self.settings.confirm_seconds * 1000:
                continue
            launched.add(opportunity.symbol)
            await self.launch(opportunity)
        return found

    async def launch(self, opportunity: Opportunity):
        symbol = opportunity.symbol
        gid = f"{symbol}-{opportunity.short_venue[:3]}-{opportunity.long_venue[:3]}-{uuid.uuid4().hex[:6]}"
        self.pending.add(symbol)
        try:
            config = self.settings.group_config(symbol, opportunity.short_venue, opportunity.long_venue,
                                                Decimal("1"), Decimal("1"))
            profile = await self.check_history(opportunity)
            await self.wait_for_trade_quotes(config)
            instruments = await self.load_instruments(config)
            total, clip = size_group(self.settings, opportunity.mid, list(instruments.values()))
            config = self.settings.group_config(symbol, opportunity.short_venue, opportunity.long_venue, total, clip)
            await self.wait_for_depth(config, instruments, clip)
            group = Group(self, gid, config, self.data_dir / "groups" / f"{gid}.{self.mode}.json", self.clock())
            await group.start(instruments)
        except Exception as exc:
            self.pending.discard(symbol)
            self.rejected_until[symbol] = self.clock() + int(self.settings.reject_cooldown_seconds * 1000)
            self.emit({"event": "launch_rejected", "symbol": symbol, "short": opportunity.short_venue,
                       "long": opportunity.long_venue, "error": str(exc)[:300]})
            return
        self.groups[gid] = group
        self.pending.discard(symbol)
        self.save_index()
        self.emit({"event": "group_started", "group": gid, "symbol": symbol, "short": config.short_venue,
                   "long": config.long_venue, "entry_bps": f"{opportunity.entry_bps:.1f}",
                   "total": str(total), "clip": str(clip), "method": config.execution_method,
                   "history": None if profile is None else {"label": profile.label, "above_pct": profile.above_pct,
                                                            "converged": profile.converged_episodes,
                                                            "coverage_pct": profile.coverage_pct}})
        self.notify(f"價差組啟動 {symbol}：空 {config.short_venue} / 多 {config.long_venue}，"
                    f"價差 {opportunity.entry_bps:.1f} bps，數量 {total}")

    async def check_history(self, opportunity: Opportunity):
        """24h history of this pair's spread; a structural (never-converging) spread blocks the launch."""
        h = self.settings.history
        if self.history is None or opportunity.entry_bps < Decimal(str(h["check_bps"])):
            return None
        profile = await self.history.profile(opportunity.symbol, opportunity.short_venue, opportunity.long_venue)
        if h["block_persistent"] and profile.label == "persistent":
            raise RuntimeError(f"24h history: spread >= {h['check_bps']} bps for {profile.above_pct}% of the time "
                               f"and never returned to take profit; likely structural")
        return profile

    def group_finished(self, group: Group):
        status = group.status
        if status == "STOPPED":
            group.lock.release()
            self.groups.pop(group.id, None)
            self.save_index()
        self.emit({"event": "group_finished", "group": group.id, "symbol": group.config.symbol,
                   "status": status, "reason": group.engine.state.reason})
        self.notify(f"價差組 {group.id} {group.config.symbol} 已{'結束' if status == 'STOPPED' else '暫停'}："
                    f"{group.engine.state.reason}")

    # -------------------------------------------------------------- dry run

    async def dry_run_report(self, *, top: int = 10) -> str:
        """Opportunities and existing-group estimates; never launches, locks or trades."""
        import time as _time
        from hydra_basis.spread_strategy.core import State
        from hydra_basis.spread_strategy.estimates import estimate_group, estimate_opportunity, format_report
        found = find_opportunities(self.settings, self.store, self.health, self.clock(), include_near_misses=True)
        # Subscribe depth for the leading candidates so later reports can price them at clip size.
        self.dry_run_watch |= {o.symbol for o in found[: top * 2]}
        if len(self.dry_run_watch) > 40:
            self.dry_run_watch = {o.symbol for o in found[: top * 2]}

        def estimate(opportunity):
            config = self.settings.group_config(opportunity.symbol, opportunity.short_venue,
                                                opportunity.long_venue, ONE_, ONE_)
            clip = self.settings.clip_notional_usd / opportunity.mid
            prices, reason = self.depth_entry(config, clip)
            status = "ok" if prices else ("thin" if "thin" in reason else "no_data")
            return estimate_opportunity(config, opportunity, self.settings.group_notional_usd,
                                        depth_prices=prices, depth_status=status)
        qualifying = [estimate(o) for o in found if o.qualifies][:top]
        near = [estimate(o) for o in found if not o.qualifies][:top]
        histories = []
        if self.history is not None:
            self.history.record_store(self.store, self.clock())
            check = Decimal(str(self.settings.history["check_bps"]))
            candidates = [o for o in found if o.entry_bps >= check][:top]
            profiles = await asyncio.gather(*(self.history.profile(o.symbol, o.short_venue, o.long_venue)
                                              for o in candidates))
            histories = list(zip(candidates, profiles))
        groups = []
        if self.index_path.exists():
            saved = json.loads(self.index_path.read_text(encoding="utf-8"))["groups"]
            for gid, item in saved.items():
                state_path = Path(item["state"])
                if not state_path.exists():
                    continue
                config = Config.from_dict(item["config"])
                state = State.from_dict(json.loads(state_path.read_text(encoding="utf-8")))
                books = {}
                for venue in config.venues:
                    quote = self.store.scan_quotes(venue).get(config.symbol)
                    if quote is None:
                        books = None
                        break
                    books[venue] = (Decimal(str(quote["bid"])), Decimal(str(quote["ask"])))
                groups.append(estimate_group(gid, config, state, books))
        return format_report(now_text=_time.strftime("%Y-%m-%d %H:%M:%S"), feeds=dict(self.health),
                             take_profit_bps=self.settings.take_profit_bps, qualifying=qualifying, near=near,
                             groups=groups, histories=histories,
                             history_check_bps=self.settings.history["check_bps"])

    # -------------------------------------------------------------- main loop

    def status(self, found: list[Opportunity]) -> dict:
        return {"event": "status", "groups": [group.snapshot() for group in self.groups.values()],
                "slots": f"{len(self.groups)}/{self.settings.max_groups}",
                "feeds": {venue: self.health.get(venue, False) for venue in self.settings.venues},
                "top": [{"symbol": o.symbol, "short": o.short_venue, "long": o.long_venue,
                         "entry_bps": f"{o.entry_bps:.1f}"} for o in found[:5]]}

    async def run(self, *, max_seconds: float = 0):
        started = last_status = self.clock()
        found: list[Opportunity] = []
        while True:
            await asyncio.sleep(self.settings.scan_seconds)
            try:
                found = await self.scan_once()
            except Exception as exc:
                self.emit({"event": "scan_error", "error": str(exc)[:200]})
            now = self.clock()
            if now - last_status >= self.settings.status_seconds * 1000:
                self.emit(self.status(found))
                last_status = now
            if max_seconds and now - started >= max_seconds * 1000:
                return

    async def shutdown(self):
        for group in list(self.groups.values()):
            await group.shutdown()
        self.save_index()
        if self.history is not None:
            await self.history.close()
        for adapter in self.adapters.values():
            close = getattr(adapter, "close", None)
            if callable(close):
                result = close()
                if asyncio.iscoroutine(result):
                    await result
