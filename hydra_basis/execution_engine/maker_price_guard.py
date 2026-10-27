"""Limit adverse maker/taker spread; favourable spreads are unrestricted."""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Awaitable, Callable
from hydra_basis.execution_engine.mexc_depth_stream import DepthQuoteUnavailable


MAKER_TAKER_MAX_GAP = Decimal("0.002")
MAKER_PRICE_CHECK_SECONDS = 0.1
MAKER_QUOTE_TIMEOUT_SECONDS = 3.0


class MakerPriceRecheck(RuntimeError):
    """Cancel and reconcile the resting order before attempting another quote."""


async def create_maker_guard(*, maker_adapter, taker_adapter, symbol, fetch_books,
                             maker_side, taker_side, quantity):
    """Only prices a resting maker. It must never gate a hedge after a fill."""
    tick = getattr(maker_adapter, "get_price_tick_size", None)
    if not callable(tick):
        return None
    for adapter in (maker_adapter, taker_adapter):
        prepare = getattr(adapter, "prepare_hedge_quotes", None)
        if callable(prepare):
            await prepare(symbol)
    maker_quote = getattr(maker_adapter, "hedge_quote", None)
    taker_quote = getattr(taker_adapter, "hedge_quote", None)
    async def books():
        if callable(maker_quote) and callable(taker_quote):
            return await maker_quote(), await taker_quote()
        maker, taker = await fetch_books()
        return (await maker_quote() if callable(maker_quote) else maker,
                await taker_quote() if callable(taker_quote) else taker)

    async def ready_books():
        last = None
        try:
            async with asyncio.timeout(MAKER_QUOTE_TIMEOUT_SECONDS):
                while True:
                    try:
                        return await books()
                    except DepthQuoteUnavailable as exc:
                        last = exc
                    refreshers = [getattr(adapter, "refresh_hedge_quote", None)
                                  for adapter in (maker_adapter, taker_adapter)]
                    refreshers = [refresh for refresh in refreshers if callable(refresh)]
                    if not refreshers:
                        raise last
                    # Both recover concurrently; then re-read BOTH caches so a
                    # slow reconnect cannot leave the other leg's quote stale.
                    results = await asyncio.gather(*(refresh() for refresh in refreshers), return_exceptions=True)
                    for result in results:
                        if isinstance(result, BaseException):
                            raise result
        except TimeoutError as exc:
            raise DepthQuoteUnavailable(f"maker quote preparation timed out; last_error={last}") from exc

    return MakerPriceGuard(fetch_books=books, tick_size=lambda: tick(symbol),
        maker_side=maker_side, taker_side=taker_side, quantity=quantity,
        fetch_taker_book=taker_quote if callable(taker_quote) else None,
        wait_for_update=getattr(taker_adapter, "wait_for_hedge_quote", None), prepare_books=ready_books)


def adverse_price_gap(maker: Decimal, taker: Decimal, side: str) -> Decimal:
    if any(not value.is_finite() or value <= 0 for value in (maker, taker)):
        raise RuntimeError("invalid maker/taker quote")
    if side.upper() == "BUY":
        return (maker - taker) / maker
    if side.upper() == "SELL":
        return (taker - maker) / maker
    raise RuntimeError(f"unsupported maker side: {side}")


def bounded_maker_price(*, desired: Decimal, taker: Decimal, bid: Decimal,
                        ask: Decimal, side: str, tick: Decimal,
                        max_gap: Decimal = MAKER_TAKER_MAX_GAP) -> Decimal:
    values = (desired, taker, bid, ask, tick)
    if any(not value.is_finite() or value <= 0 for value in values):
        raise RuntimeError("invalid maker guard price or tick size")
    if not max_gap.is_finite() or not 0 <= max_gap < 1 or bid >= ask:
        raise RuntimeError("invalid maker guard band or crossed orderbook")
    # Only constrain the adverse side. Keep the user's maker-price denominator.
    nearest = (desired / tick).to_integral_value(rounding=ROUND_HALF_UP)
    if side.upper() == "BUY":
        lower = Decimal(1)
        upper = (taker / (1 - max_gap) / tick).to_integral_value(rounding=ROUND_CEILING) - 1
        upper = min(upper, (ask / tick).to_integral_value(rounding=ROUND_CEILING) - 1)
    elif side.upper() == "SELL":
        lower = (taker / (1 + max_gap) / tick).to_integral_value(rounding=ROUND_FLOOR) + 1
        lower = max(lower, (bid / tick).to_integral_value(rounding=ROUND_FLOOR) + 1)
        upper = max(lower, nearest)
    else:
        raise RuntimeError(f"unsupported maker side: {side}")
    if lower > upper:
        raise RuntimeError(f"no passive maker price with adverse gap <= {max_gap:.2%} of taker={taker}")
    return min(upper, max(lower, nearest)) * tick


class MakerPriceGuard:
    def __init__(self, *, fetch_books: Callable[[], Awaitable[tuple[dict, dict]]],
                 tick_size: Callable[[], Awaitable[str]], maker_side: str,
                 taker_side: str, max_gap: Decimal = MAKER_TAKER_MAX_GAP,
                 check_seconds: float = MAKER_PRICE_CHECK_SECONDS,
                 quantity: Decimal | None = None,
                 fetch_taker_book=None, wait_for_update=None, prepare_books=None) -> None:
        self.fetch_books = fetch_books
        self.tick_size = tick_size
        self.maker_side = maker_side
        self.taker_side = taker_side
        self.max_gap = max_gap
        self.check_seconds = check_seconds
        self.quantity = quantity
        self.fetch_taker_book = fetch_taker_book
        self.wait_for_update = wait_for_update
        self.prepare_books = prepare_books
        self.taker_book: dict | None = None
        if {maker_side.upper(), taker_side.upper()} != {"BUY", "SELL"}:
            raise ValueError("maker price guard requires opposite BUY/SELL sides")
        if not max_gap.is_finite() or not 0 <= max_gap < 1 or check_seconds <= 0:
            raise ValueError("invalid maker price guard limits")

    def taker_price(self, book: dict) -> Decimal:
        price = self._depth_price(book)
        fast_quote = book.get("fast_quote")
        if fast_quote is not None:
            # A newer best price can prove the old VWAP too optimistic. It
            # cannot prove the whole clip executable at an improved price.
            fast_price = self._depth_price(fast_quote)
            price = max(price, fast_price) if self.taker_side.upper() == "BUY" else min(price, fast_price)
        return price

    def _depth_price(self, book: dict) -> Decimal:
        if self.taker_side.upper() not in {"BUY", "SELL"}:
            raise RuntimeError(f"unsupported taker side: {self.taker_side}")
        age = Decimal(str(book.get("age_ms", 0)))
        source = book.get("ts_ms", 0)
        if not age.is_finite() or age < 0 or age > 1000:
            raise RuntimeError("stale hedge quote")
        if source and int(source) > 946684800000 and abs(time.time()*1000 - int(source)) > 1000:
            raise RuntimeError("stale hedge source timestamp")
        levels = book.get("asks" if self.taker_side.upper() == "BUY" else "bids")
        if self.quantity is not None and levels:
            remaining, quote = self.quantity, Decimal(0)
            for p, q in levels:
                p, q = Decimal(str(p)), Decimal(str(q))
                if not p.is_finite() or not q.is_finite() or p <= 0 or q < 0:
                    raise RuntimeError("invalid hedge depth")
                take = min(remaining, q)
                quote += take * p
                remaining -= take
                if remaining == 0:
                    return quote / self.quantity
            raise RuntimeError("insufficient hedge depth for full maker quantity")
        price = Decimal(str(book["ask" if self.taker_side.upper() == "BUY" else "bid"]))
        if not price.is_finite() or price <= 0:
            raise RuntimeError("invalid taker quote")
        return price

    async def _books(self) -> tuple[dict, dict]:
        async with asyncio.timeout(MAKER_QUOTE_TIMEOUT_SECONDS):
            return await self.fetch_books()

    async def prepare(self, desired: str) -> str:
        tick = Decimal(await self.tick_size())
        maker, taker = await self.prepare_books() if self.prepare_books is not None else await self._books()
        price = bounded_maker_price(
            desired=Decimal(desired), taker=self.taker_price(taker),
            bid=Decimal(str(maker["bid"])), ask=Decimal(str(maker["ask"])),
            side=self.maker_side, tick=tick, max_gap=self.max_gap,
        )
        self.taker_book = taker
        if price != Decimal(desired):
            print(f"[maker-band] price {desired} -> {price}; max gap={self.max_gap:.2%}", flush=True)
        return format(price, "f")

    async def check(self, submitted_price: str) -> None:
        maker = Decimal(submitted_price)
        try:
            if self.fetch_taker_book is not None:
                async with asyncio.timeout(MAKER_QUOTE_TIMEOUT_SECONDS):
                    book = await self.fetch_taker_book()
            else:
                _, book = await self._books()
            taker = self.taker_price(book)
        except Exception as exc:
            raise MakerPriceRecheck(f"maker quote unavailable; cancel before refresh: {exc}") from exc
        gap = adverse_price_gap(maker, taker, self.maker_side)
        if gap >= self.max_gap:
            raise MakerPriceRecheck(
                f"adverse maker/taker gap {gap:.4%} > {self.max_gap:.2%}: maker={maker} taker={taker}"
            )

    async def watch(self, submitted_price: str) -> None:
        while True:
            await self.check(submitted_price)
            if self.wait_for_update is not None:
                await self.wait_for_update()
            else:
                await asyncio.sleep(self.check_seconds)


async def wait_with_price_guard(fill_wait: Awaitable[dict], guard: MakerPriceGuard,
                                submitted_price: str) -> dict:
    fill = asyncio.ensure_future(fill_wait)
    watch = asyncio.create_task(guard.watch(submitted_price))
    try:
        done, _ = await asyncio.wait((fill, watch), return_when=asyncio.FIRST_COMPLETED)
        # A confirmed fill wins a simultaneous price change: hedge it immediately.
        if fill in done:
            return await fill
        await watch
        raise RuntimeError("maker price watcher unexpectedly stopped")
    finally:
        for task in (fill, watch):
            if not task.done():
                task.cancel()
        await asyncio.gather(fill, watch, return_exceptions=True)
