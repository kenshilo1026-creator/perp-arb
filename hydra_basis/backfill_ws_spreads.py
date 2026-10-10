"""Top of book for every backfill symbol in bulk, instead of one REST snapshot per symbol.

Aster publishes every symbol's best bid/ask in one REST call. The other venues reuse the
dispatcher's WebSocket runners: one all-symbol channel (Ondo topOfBooksPerps) or gradually spaced
per-symbol subscriptions (Hyperliquid/HIP-3 l2Book, Lighter ticker, Arcus bbo) on one socket per
venue. Symbols not covered in time are left to the REST snapshot path.
"""
from __future__ import annotations

import asyncio
import time

from hydra_basis.adapters.base import fetch_json
from hydra_basis.spread_strategy.dispatcher import (
    ArcusRunner,
    HyperliquidBooksRunner,
    LighterRunner,
    OndoRunner,
    QuoteStore,
    lighter_market_shards,
)

ASTER_BOOK_TICKER_URL = "https://fapi.asterdex.com/fapi/v1/ticker/bookTicker"
WS_SPREAD_VENUES = frozenset({"hyperliquid", "trade_xyz", "entropy", "lighter", "arcus", "ondo"})
BULK_SPREAD_VENUES = WS_SPREAD_VENUES | {"aster"}
HIP3_DEXES = {"trade_xyz": "xyz", "entropy": "io"}
WS_SPREAD_TIMEOUT_SECONDS = 60.0
# Stop once coverage has not grown for this long (subscriptions are spaced ~50 ms apart, so a
# venue still subscribing keeps coverage growing).
WS_SPREAD_SETTLE_SECONDS = 5.0
WS_SPREAD_MIN_SECONDS = 3.0


class Hip3BooksRunner(HyperliquidBooksRunner):
    """l2Book for one HIP-3 dex, stored under the backfill's prefixed symbol (XYZ:NVDA, IO:OAI)."""

    def __init__(self, session, store, coins: list[str], venue: str):
        super().__init__(session, store, coins)
        self.venue = venue

    def handle(self, payload):
        from hydra_basis.spread_strategy.feeds import parse_hyperliquid_book
        book = parse_hyperliquid_book(payload)
        if book is not None:
            self.store.update_quotes(self.venue, {book["symbol"]: book})


async def build_ws_spread_runners(venue: str, session, store: QuoteStore) -> list:
    from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_meta
    if venue == "hyperliquid":
        rows = await fetch_hyperliquid_meta(session)
        runners = [HyperliquidBooksRunner(session, store, [str(row["name"]) for row in rows
                                                           if row.get("name") and not row.get("isDelisted")])]
    elif venue in HIP3_DEXES:
        rows = await fetch_hyperliquid_meta(session, HIP3_DEXES[venue])
        runners = [Hip3BooksRunner(session, store, [str(row["name"]) for row in rows
                                                    if row.get("name") and not row.get("isDelisted")], venue)]
    elif venue == "lighter":
        from hydra_basis.adapters.lighter import fetch_lighter_market_map
        runners = [LighterRunner(session, store, shard)
                   for shard in lighter_market_shards(await fetch_lighter_market_map(session))]
    elif venue == "arcus":
        from hydra_basis.adapters.arcus import fetch_arcus_markets
        runners = [ArcusRunner(session, store, [str(m["marketDisplayName"]) for m in await fetch_arcus_markets(session)
                                                if m.get("status") == "ONLINE"])]
    elif venue == "ondo":
        runners = [OndoRunner(session, store)]
    else:
        raise ValueError(f"no websocket spread runner for {venue}")
    for runner in runners:
        await runner.initialize()
    return runners


def _usable(quote: dict | None) -> bool:
    if not quote:
        return False
    try:
        bid, ask = float(quote["bid"]), float(quote["ask"])
    except (KeyError, TypeError, ValueError):
        return False
    return 0 < bid <= ask


def _quote(bid, ask, ts_ms) -> dict[str, float | int] | None:
    quote = {"bid": bid, "ask": ask}
    if not _usable(quote):
        return None
    return {"bid": float(bid), "ask": float(ask), "ts_ms": int(ts_ms)}


async def fetch_aster_top_of_book(session, symbols: set[str]) -> dict[str, dict[str, float | int]]:
    """Every Aster symbol's best bid/ask from one request, choosing the contract the REST depth
    snapshot would (the listed raw symbol, then its USDT twin)."""
    from hydra_basis.adapters.aster import fetch_aster_symbol_metadata
    metadata = await fetch_aster_symbol_metadata(session)
    rows = await fetch_json(session, "GET", ASTER_BOOK_TICKER_URL)
    by_raw = {str(row.get("symbol") or "").upper(): row for row in rows or [] if isinstance(row, dict)}
    quotes: dict[str, dict[str, float | int]] = {}
    for symbol in symbols:
        meta = metadata.get(symbol.upper())
        if meta is None:
            continue
        raw = meta["raw_symbol"]
        candidates = [raw, raw + "T"] if raw.endswith("USD") and not raw.endswith("USDT") else [raw]
        for candidate in candidates:
            row = by_raw.get(candidate)
            quote = row and _quote(row.get("bidPrice"), row.get("askPrice"), row.get("time") or 0)
            if quote:
                quotes[symbol] = quote
                break
    return quotes


async def collect_top_of_book(
    session,
    wanted: dict[str, set[str]],
    **ws_options,
) -> tuple[dict[tuple[str, str], dict[str, float | int]], dict[str, str]]:
    """Aster by bulk REST and the rest by WebSocket, concurrently."""
    errors: dict[str, str] = {}
    results: dict[tuple[str, str], dict[str, float | int]] = {}

    async def aster():
        symbols = set(wanted.get("aster") or ())
        if not symbols:
            return
        try:
            for symbol, quote in (await fetch_aster_top_of_book(session, symbols)).items():
                results[("aster", symbol)] = quote
        except Exception as exc:
            errors["aster"] = repr(exc)[:200]

    async def streams():
        ws_results, ws_errors = await collect_ws_top_of_book(session, wanted, **ws_options)
        results.update(ws_results)
        errors.update(ws_errors)

    await asyncio.gather(aster(), streams())
    return results, errors


def _covered(store: QuoteStore, wanted: dict[str, set[str]]) -> dict[str, int]:
    counts = {}
    for venue, symbols in wanted.items():
        quotes = store.scan_quotes(venue)
        counts[venue] = sum(1 for symbol in symbols if _usable(quotes.get(symbol.upper())))
    return counts


async def collect_ws_top_of_book(
    session,
    wanted: dict[str, set[str]],
    *,
    timeout_seconds: float = WS_SPREAD_TIMEOUT_SECONDS,
    settle_seconds: float = WS_SPREAD_SETTLE_SECONDS,
    min_seconds: float = WS_SPREAD_MIN_SECONDS,
    build_runners=build_ws_spread_runners,
    clock=time.monotonic,
) -> tuple[dict[tuple[str, str], dict[str, float | int]], dict[str, str]]:
    """Stream each venue's books until every wanted symbol has a quote, coverage stops growing, or
    the timeout passes. Returns ({(venue, symbol): {bid, ask, ts_ms}}, {venue: error})."""
    wanted = {venue: set(symbols) for venue, symbols in wanted.items() if venue in WS_SPREAD_VENUES and symbols}
    if not wanted:
        return {}, {}
    store = QuoteStore()
    errors: dict[str, str] = {}
    runners: list = []

    async def pump(runner):
        while True:
            await runner.pump_once()

    async def stream(venue: str):
        try:
            venue_runners = await build_runners(venue, session, store)
            runners.extend(venue_runners)
            await asyncio.gather(*(pump(runner) for runner in venue_runners))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # one venue's stream failing leaves its symbols to REST
            errors[venue] = repr(exc)[:200]

    tasks = [asyncio.create_task(stream(venue)) for venue in wanted]
    started = last_growth = clock()
    best = -1
    try:
        while True:
            await asyncio.sleep(0.25)
            counts = _covered(store, wanted)
            total = sum(counts.values())
            now = clock()
            if total > best:
                best, last_growth = total, now
            live = [venue for venue, task in zip(wanted, tasks) if not task.done()]
            complete = all(counts[venue] >= len(wanted[venue]) for venue in wanted)
            if complete or not live or now - started >= timeout_seconds:
                break
            if now - started >= min_seconds and now - last_growth >= settle_seconds:
                break
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for runner in runners:
            try:
                await runner.close()
            except Exception:
                pass

    results: dict[tuple[str, str], dict[str, float | int]] = {}
    for venue, symbols in wanted.items():
        quotes = store.scan_quotes(venue)
        for symbol in symbols:
            quote = quotes.get(symbol.upper())
            if _usable(quote):
                results[(venue, symbol)] = _quote(quote["bid"], quote["ask"],
                                                  quote.get("source_ms") or quote["received_ms"])
    return results, errors
