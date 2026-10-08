"""Limit adverse maker/taker spread; favourable spreads are unrestricted."""
from __future__ import annotations

import asyncio
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Awaitable, Callable


MAKER_TAKER_MAX_GAP = Decimal("0.002")
MAKER_PRICE_CHECK_SECONDS = 1.0
MAKER_QUOTE_TIMEOUT_SECONDS = 3.0


class MakerPriceRecheck(RuntimeError):
    """Cancel and reconcile the resting order before attempting another quote."""


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
        upper = (taker / (1 - max_gap) / tick).to_integral_value(rounding=ROUND_FLOOR)
        upper = min(upper, (ask / tick).to_integral_value(rounding=ROUND_CEILING) - 1)
    elif side.upper() == "SELL":
        lower = (taker / (1 + max_gap) / tick).to_integral_value(rounding=ROUND_CEILING)
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
                 check_seconds: float = MAKER_PRICE_CHECK_SECONDS) -> None:
        self.fetch_books = fetch_books
        self.tick_size = tick_size
        self.maker_side = maker_side
        self.taker_side = taker_side
        self.max_gap = max_gap
        self.check_seconds = check_seconds
        self.taker_book: dict | None = None
        if {maker_side.upper(), taker_side.upper()} != {"BUY", "SELL"}:
            raise ValueError("maker price guard requires opposite BUY/SELL sides")
        if not max_gap.is_finite() or not 0 <= max_gap < 1 or check_seconds <= 0:
            raise ValueError("invalid maker price guard limits")

    def taker_price(self, book: dict) -> Decimal:
        if self.taker_side.upper() not in {"BUY", "SELL"}:
            raise RuntimeError(f"unsupported taker side: {self.taker_side}")
        price = Decimal(str(book["ask" if self.taker_side.upper() == "BUY" else "bid"]))
        if not price.is_finite() or price <= 0:
            raise RuntimeError("invalid taker quote")
        return price

    async def _books(self) -> tuple[dict, dict]:
        async with asyncio.timeout(MAKER_QUOTE_TIMEOUT_SECONDS):
            return await self.fetch_books()

    async def prepare(self, desired: str) -> str:
        tick = Decimal(await self.tick_size())
        maker, taker = await self._books()
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
            _, book = await self._books()
            taker = self.taker_price(book)
        except Exception as exc:
            raise MakerPriceRecheck(f"maker quote unavailable; cancel before refresh: {exc}") from exc
        gap = adverse_price_gap(maker, taker, self.maker_side)
        if gap > self.max_gap:
            raise MakerPriceRecheck(
                f"adverse maker/taker gap {gap:.4%} > {self.max_gap:.2%}: maker={maker} taker={taker}"
            )

    async def watch(self, submitted_price: str) -> None:
        while True:
            await self.check(submitted_price)
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
