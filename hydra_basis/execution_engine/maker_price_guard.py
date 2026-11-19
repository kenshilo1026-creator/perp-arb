"""Limit adverse maker/taker spread; favourable spreads are unrestricted."""
from __future__ import annotations

import asyncio
import os
import time
from hydra_basis.env import load_environment
from hydra_basis.execution_engine.execution_trace import mark
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Awaitable, Callable
from hydra_basis.execution_engine.mexc_depth_stream import DepthQuoteUnavailable


def configured_maker_taker_gap():
    """Environment value is percentage points: 0.25 means 0.25%, not 25%."""
    try:
        percent = Decimal(os.environ.get("MAKER_TAKER_MAX_GAP_PCT", "0.25"))
    except InvalidOperation as exc:
        raise ValueError("MAKER_TAKER_MAX_GAP_PCT must be a percentage between 0 and 100 (e.g. 0.25)") from exc
    if not percent.is_finite() or not 0 < percent < 100:
        raise ValueError("MAKER_TAKER_MAX_GAP_PCT must be a percentage between 0 and 100 (e.g. 0.25)")
    return percent / Decimal(100)


load_environment()
MAKER_TAKER_MAX_GAP = configured_maker_taker_gap()
MAKER_PRICE_CHECK_SECONDS = 0.1
MAKER_QUOTE_TIMEOUT_SECONDS = 3.0


class MakerPriceRecheck(RuntimeError):
    """Cancel and reconcile the resting order before attempting another quote."""


class MakerGapTimeout(RuntimeError):
    """No new maker may be submitted after the entry waiting deadline."""


def passive_entry_price(book, *, side, tick, improve=False):
    # Adapters attach fast_quote only when its BBO is newer than full depth.
    book = book.get("fast_quote") or book
    age = Decimal(str(book.get("age_ms", 0)))
    if not age.is_finite() or not 0 <= age <= 1000:
        raise RuntimeError("stale maker quote")
    bid, ask = Decimal(str(book["bid"])), Decimal(str(book["ask"]))
    if any(not v.is_finite() or v <= 0 for v in (bid, ask, tick)) or bid >= ask:
        raise ValueError("invalid passive entry book/tick")
    if side.upper() == "BUY":
        price = min(bid + (tick if improve else 0), ask - tick)
        price = (price / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    else:
        price = max(ask - (tick if improve else 0), bid + tick)
        price = (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    if price <= 0:
        raise ValueError("no positive passive entry price")
    return price


async def wait_for_quote_update(waiter):
    # A silent/disconnected WS must not suspend the deadline indefinitely.
    try:
        async with asyncio.timeout(0.5):
            if waiter is not None:
                await waiter()
            else:
                await asyncio.sleep(0.1)
    except TimeoutError:
        pass
    await asyncio.sleep(0.01)


async def create_maker_guard(*, maker_adapter, taker_adapter, symbol, fetch_books,
                             maker_side, taker_side, quantity, improve_maker_by_one_tick=False,
                             wait_for_gap_seconds=None, follow_maker_quote=False,
                             top_level_only=False, size_rules=None, remaining_quantity=None):
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

    async def either_quote_changed():
        waiters = [getattr(adapter, "wait_for_hedge_quote", None) for adapter in (maker_adapter, taker_adapter)]
        tasks = [asyncio.create_task(waiter()) for waiter in waiters if callable(waiter)]
        try:
            if tasks:
                done, _ = await asyncio.wait(tasks, timeout=MAKER_PRICE_CHECK_SECONDS,
                                             return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    await task
            else:
                await asyncio.sleep(MAKER_PRICE_CHECK_SECONDS)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return MakerPriceGuard(fetch_books=books, tick_size=lambda: tick(symbol),
        maker_side=maker_side, taker_side=taker_side, quantity=quantity,
        improve_maker_by_one_tick=improve_maker_by_one_tick,
        wait_for_gap_seconds=wait_for_gap_seconds,
        follow_maker_quote=follow_maker_quote,
        top_level_only=top_level_only, size_rules=size_rules, remaining_quantity=remaining_quantity,
        fetch_taker_book=taker_quote if callable(taker_quote) else None,
        wait_for_update=either_quote_changed if follow_maker_quote else getattr(taker_adapter, "wait_for_hedge_quote", None), prepare_books=ready_books)


def adverse_price_gap(maker: Decimal, taker: Decimal, side: str) -> Decimal:
    if any(not value.is_finite() or value <= 0 for value in (maker, taker)):
        raise RuntimeError("invalid maker/taker quote")
    if side.upper() == "BUY":
        return (maker - taker) / maker
    if side.upper() == "SELL":
        return (taker - maker) / maker
    raise RuntimeError(f"unsupported maker side: {side}")


def taker_best_level(book: dict, side: str) -> tuple[Decimal, Decimal]:
    """Price and capacity from ONE fresh quote; never mix BBO and depth versions."""
    quote = book.get("fast_quote") or book
    side = side.upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported taker side: {side}")
    age = Decimal(str(quote.get("age_ms", 0)))
    source = quote.get("ts_ms", 0)
    if not age.is_finite() or not 0 <= age <= 1000:
        raise DepthQuoteUnavailable("stale hedge best quote")
    if source and int(source) > 946684800000 and abs(time.time()*1000 - int(source)) > 1000:
        raise DepthQuoteUnavailable("stale hedge best source timestamp")
    bid, ask = Decimal(str(quote["bid"])), Decimal(str(quote["ask"]))
    if any(not v.is_finite() or v <= 0 for v in (bid, ask)) or bid >= ask:
        raise DepthQuoteUnavailable("invalid hedge best quote")
    price = bid if side == "SELL" else ask
    levels = quote.get("bids" if side == "SELL" else "asks")
    if levels:
        level_price, size = map(lambda v: Decimal(str(v)), levels[0])
        if level_price != price:
            raise DepthQuoteUnavailable("hedge best price/depth mismatch")
    else:
        raw_size = quote.get("bid_size" if side == "SELL" else "ask_size")
        if raw_size is None:
            raise DepthQuoteUnavailable("hedge best quote missing quantity")
        size = Decimal(str(raw_size))
    if not size.is_finite() or size < 0:
        raise DepthQuoteUnavailable("invalid hedge best quantity")
    return price, size


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
                 fetch_taker_book=None, wait_for_update=None, prepare_books=None,
                 improve_maker_by_one_tick: bool = False, wait_for_gap_seconds=None,
                 follow_maker_quote=False, top_level_only=False, size_rules=None,
                 remaining_quantity=None) -> None:
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
        self.improve_maker_by_one_tick = improve_maker_by_one_tick
        self.wait_for_gap_seconds = wait_for_gap_seconds
        self.follow_maker_quote = follow_maker_quote
        self.top_level_only = top_level_only
        self.size_rules = size_rules
        self.quantity_cap = quantity
        self.remaining_quantity = remaining_quantity if remaining_quantity is not None else quantity
        self.prepared_quantity = None
        self._tick = None
        self._last_chase_hold_log = None
        if wait_for_gap_seconds is not None and wait_for_gap_seconds <= 0:
            raise ValueError("gap wait timeout must be positive")
        self.taker_book: dict | None = None
        if {maker_side.upper(), taker_side.upper()} != {"BUY", "SELL"}:
            raise ValueError("maker price guard requires opposite BUY/SELL sides")
        if not max_gap.is_finite() or not 0 <= max_gap < 1 or check_seconds <= 0:
            raise ValueError("invalid maker price guard limits")

    def taker_price(self, book: dict) -> Decimal:
        if self.top_level_only:
            return taker_best_level(book, self.taker_side)[0]
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
        if self.top_level_only and self.wait_for_gap_seconds is None:
            raise ValueError("best-level execution requires an entry waiting deadline")
        tick = Decimal(await self.tick_size())
        self._tick = tick
        if self.wait_for_gap_seconds is not None:
            return await self._wait_for_entry(tick)
        maker, taker = await self.prepare_books() if self.prepare_books is not None else await self._books()
        target = Decimal(desired)
        if self.improve_maker_by_one_tick:
            # Rebase on the latest book for every submission/replacement;
            # never keep adding ticks to a previous order's price.
            target = (Decimal(str(maker["bid"])) + tick if self.maker_side.upper() == "BUY"
                      else Decimal(str(maker["ask"])) - tick)
        price = bounded_maker_price(
            desired=target, taker=self.taker_price(taker),
            bid=Decimal(str(maker["bid"])), ask=Decimal(str(maker["ask"])),
            side=self.maker_side, tick=tick, max_gap=self.max_gap,
        )
        self.taker_book = taker
        hedge_price = self.taker_price(taker)
        mark("maker.pre_submit_quote", maker_price=str(price), hedge_vwap=str(hedge_price),
             gap=str(adverse_price_gap(price, hedge_price, self.maker_side)),
             quote_age_ms=taker.get("age_ms"), quote_source_ms=taker.get("ts_ms"),
             quantity=str(self.quantity) if self.quantity is not None else None)
        if price != Decimal(desired):
            print(f"[maker-band] price {desired} -> {price}; max gap={self.max_gap:.2%}", flush=True)
        return format(price, "f")

    async def _wait_for_entry(self, tick):
        last, last_log, waiting = "行情尚未可用", None, False
        try:
            async with asyncio.timeout(self.wait_for_gap_seconds):
                while True:
                    try:
                        maker, taker = await self.prepare_books() if self.prepare_books is not None else await self._books()
                        price = passive_entry_price(maker, side=self.maker_side, tick=tick,
                                                    improve=self.improve_maker_by_one_tick)
                        hedge = self.taker_price(taker)
                        selected_quantity = self.quantity
                        if self.top_level_only:
                            if self.size_rules is not None:
                                # Local import avoids the pricing/sizing module cycle.
                                from .depth_sizing import depth_clip
                                selected_quantity, price, _ = depth_clip(
                                    remaining=self.remaining_quantity, maker_book=maker, taker_book=taker,
                                    maker_side=self.maker_side, tick=tick, rules=self.size_rules,
                                    improve=self.improve_maker_by_one_tick, max_gap=self.max_gap,
                                    wait_for_gap=True, top_level_only=True, clip_limit=self.quantity_cap)
                            elif self.quantity is None or self.quantity > taker_best_level(taker, self.taker_side)[1]:
                                selected_quantity = Decimal(0)
                        gap = adverse_price_gap(price, hedge, self.maker_side)
                        label = "hedge_best" if self.top_level_only else "hedge_vwap"
                        last = f"gap={gap:.4%} maker={price} {label}={hedge}"
                        if self.top_level_only:
                            last += f" best_qty={taker_best_level(taker, self.taker_side)[1]} clip_qty={selected_quantity}"
                        quote = (taker.get("fast_quote") or taker) if self.top_level_only else taker
                        mark("maker.entry_gap", maker_price=str(price), hedge_vwap=str(hedge),
                             hedge_quote_mode="best_level" if self.top_level_only else "vwap",
                             hedge_best_quantity=str(taker_best_level(taker, self.taker_side)[1]) if self.top_level_only else None,
                             gap=str(gap), quote_age_ms=quote.get("age_ms"), quantity=str(selected_quantity))
                        if gap < self.max_gap and (not self.top_level_only or selected_quantity > 0):
                            if self.top_level_only:
                                self.quantity = self.prepared_quantity = selected_quantity
                            self.taker_book = taker
                            if waiting:
                                print(f"[gap-ready] 價差已低於 {self.max_gap:.2%}，恢復掛單；{last}", flush=True)
                            elif self.top_level_only:
                                print(f"[maker-entry] {last}", flush=True)
                            return format(price, "f")
                    except (RuntimeError, TimeoutError) as exc:
                        last = str(exc)
                    waiting = True
                    now = time.monotonic()
                    if last_log is None or now - last_log >= 5:
                        print(f"[gap-wait] 暫不掛單，等待價差低於 {self.max_gap:.2%}；{last}", flush=True)
                        last_log = now
                    await wait_for_quote_update(self.wait_for_update)
        except TimeoutError as exc:
            requirement = "有效價差及足夠的單一價位深度" if self.top_level_only else "有效價差"
            raise MakerGapTimeout(f"等待 {self.wait_for_gap_seconds:g} 秒仍未取得低於 {self.max_gap:.2%} 的{requirement}，停止掛單；{last}") from exc

    async def check(self, submitted_price: str) -> None:
        maker = Decimal(submitted_price)
        current_target = None
        try:
            if self.follow_maker_quote:
                maker_book, book = await self._books()
                if self._tick is None:
                    self._tick = Decimal(await self.tick_size())
                current_target = passive_entry_price(maker_book, side=self.maker_side, tick=self._tick,
                                                     improve=self.improve_maker_by_one_tick)
            elif self.fetch_taker_book is not None:
                async with asyncio.timeout(MAKER_QUOTE_TIMEOUT_SECONDS):
                    book = await self.fetch_taker_book()
            else:
                _, book = await self._books()
            taker = self.taker_price(book)
            if self.top_level_only:
                # Capacity sizes a NEW order in prepare(). Once accepted, keep
                # its quantity fixed until a price/safety recheck requires a
                # replacement; fluctuating queue sizes alone must not cancel it.
                _, capacity = taker_best_level(book, self.taker_side)
        except Exception as exc:
            raise MakerPriceRecheck(f"maker quote unavailable; cancel before refresh: {exc}") from exc
        gap = adverse_price_gap(maker, taker, self.maker_side)
        quote = (book.get("fast_quote") or book) if self.top_level_only else book
        mark("maker.gap_check", maker_price=str(maker), hedge_vwap=str(taker), gap=str(gap),
             hedge_quote_mode="best_level" if self.top_level_only else "vwap",
             hedge_best_quantity=str(capacity) if self.top_level_only else None,
             quote_age_ms=quote.get("age_ms"), quote_source_ms=quote.get("ts_ms"),
             quantity=str(self.quantity) if self.quantity is not None else None)
        if gap >= self.max_gap:
            raise MakerPriceRecheck(
                f"adverse maker/taker gap {gap:.4%} >= {self.max_gap:.2%}: maker={maker} taker={taker}"
            )
        if current_target is not None and current_target != maker:
            # Compare both prices against the SAME validated hedge quote.
            # Keep a safe resting order when chasing the BBO would breach the
            # entry band. The next check still cancels if the original is unsafe.
            target_gap = adverse_price_gap(current_target, taker, self.maker_side)
            if target_gap >= self.max_gap:
                now = time.monotonic()
                if self._last_chase_hold_log is None or now - self._last_chase_hold_log >= 5:
                    label = "hedge_best" if self.top_level_only else "hedge_vwap"
                    print(f"[maker-keep] 原單價差合格，追價將達到或超過 {self.max_gap:.2%}，保留原單；"
                          f"maker={maker} gap={gap:.4%} latest={current_target} "
                          f"latest_gap={target_gap:.4%} {label}={taker}", flush=True)
                    self._last_chase_hold_log = now
                return
            self._last_chase_hold_log = None
            raise MakerPriceRecheck(f"maker best quote changed: submitted={maker} latest={current_target}")

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
