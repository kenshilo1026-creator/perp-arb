from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from math import ceil
from typing import Any

import aiohttp

from hydra_basis.config import MONITOR_SIGNALS_PATH
from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
from hydra_basis.execution_engine.models import ExecutionSignal
from hydra_basis.execution_engine.runtime import prepare_execution_preview_for_size
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol

VALID_EXEC_MODES = {"maker_taker", "taker"}


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass
class OpenParams:
    kind: str
    symbol: str
    short_venue: str
    long_venue: str  # for spot_perp this is the spot leg (always "mexc")
    exec_mode: str
    leverage: int
    total_size: Decimal
    clip_size: Decimal
    interval_ms: int
    debounce_ms: int


@dataclass
class CloseParams:
    kind: str
    symbol: str
    total_size: Decimal
    clip_size: Decimal
    interval_ms: int
    debounce_ms: int


@dataclass
class Preview:
    symbol: str
    kind: str
    maker_venue: str
    taker_venue: str
    short_venue: str
    long_venue: str
    total_usd: float
    clip_usd: float
    batch_count: int
    maker_spread_pct: float
    taker_spread_pct: float
    requires_confirm: bool
    exec_mode: str


def validate_open_params(params: OpenParams) -> None:
    if params.exec_mode not in VALID_EXEC_MODES:
        raise ValueError(f"unknown exec_mode: {params.exec_mode}")
    if params.total_size <= 0 or params.clip_size <= 0:
        raise ValueError("total_size and clip_size must be positive")
    if params.clip_size > params.total_size:
        raise ValueError("clip_size cannot exceed total_size")
    if params.interval_ms < 0 or params.debounce_ms < 0:
        raise ValueError("interval_ms and debounce_ms must be >= 0")
    if params.kind == "perp_perp" and params.short_venue == params.long_venue:
        raise ValueError("short_venue and long_venue must differ")


def compute_token_batch_count(total_size: Decimal, clip_size: Decimal) -> int:
    return int(ceil(total_size / clip_size))


# --------------------------------------------------------------------------- #
# Deps (injectable seams) + debounce
# --------------------------------------------------------------------------- #
class DebounceTimeout(RuntimeError):
    pass


async def _default_fetch_orderbook(venue: str, symbol: str, clip_usd: float) -> dict:
    async with aiohttp.ClientSession() as session:
        return await fetch_orderbook_snapshot(session, venue=venue, symbol=symbol, clip_usd=clip_usd)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@dataclass
class Deps:
    fetch_orderbook: Callable[..., Any] = None  # (venue, symbol, clip_usd) -> dict|awaitable
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    now: Callable[[], float] = time.monotonic
    run_clip: Callable[..., Awaitable[dict]] = None
    record_open: Callable[..., Any] = None

    def __post_init__(self) -> None:
        if self.fetch_orderbook is None:
            self.fetch_orderbook = lambda venue, symbol, clip_usd: _default_fetch_orderbook(venue, symbol, clip_usd)


def _mid(book: dict) -> float:
    return (float(book["bid"]) + float(book["ask"])) / 2


async def wait_for_stable_orderbook(
    *,
    venues: list[str],
    symbol: str,
    clip_usd: float,
    deps: Deps,
    debounce_ms: int,
    timeout_ms: int,
    threshold_pct: float = 0.0005,
) -> float:
    if debounce_ms <= 0:
        return 0.0
    start = deps.now()
    last_mid: dict[str, float] = {}
    stable_since: float | None = None
    poll_seconds = min(debounce_ms, 100) / 1000.0
    while True:
        moved = False
        for venue in venues:
            book = await _maybe_await(deps.fetch_orderbook(venue=venue, symbol=symbol, clip_usd=clip_usd))
            mid = _mid(book)
            prev = last_mid.get(venue)
            if prev is not None and prev > 0 and abs(mid - prev) / prev >= threshold_pct:
                moved = True
            last_mid[venue] = mid
        now = deps.now()
        if moved or stable_since is None:
            stable_since = now
        if (now - stable_since) * 1000.0 >= debounce_ms:
            return (now - start) * 1000.0
        if (now - start) * 1000.0 >= timeout_ms:
            raise DebounceTimeout(f"orderbook not stable within {timeout_ms}ms for {symbol}")
        await deps.sleep(poll_seconds)


# --------------------------------------------------------------------------- #
# Preview
# --------------------------------------------------------------------------- #
async def build_open_preview(params: OpenParams, *, signal: ExecutionSignal | None = None) -> Preview:
    validate_open_params(params)
    if signal is None:
        signal = load_best_signal_for_symbol(path=MONITOR_SIGNALS_PATH, symbol=params.symbol)
    signal.short_venue = params.short_venue
    signal.long_venue = params.long_venue
    _sig, exec_preview, _short_book, _long_book = await prepare_execution_preview_for_size(
        symbol=params.symbol,
        total_size=params.total_size,
        clip_size=params.clip_size,
        signal=signal,
    )
    return Preview(
        symbol=exec_preview.symbol,
        kind=params.kind,
        maker_venue=exec_preview.maker_venue,
        taker_venue=exec_preview.taker_venue,
        short_venue=signal.short_venue,
        long_venue=signal.long_venue,
        total_usd=exec_preview.total_usd,
        clip_usd=exec_preview.clip_usd,
        batch_count=exec_preview.batch_count,
        maker_spread_pct=exec_preview.maker_spread_pct,
        taker_spread_pct=exec_preview.taker_spread_pct,
        requires_confirm=exec_preview.requires_confirm,
        exec_mode=params.exec_mode,
    )


# --------------------------------------------------------------------------- #
# Batch loop
# --------------------------------------------------------------------------- #
async def _emit(on_progress, event: dict) -> None:
    await _maybe_await(on_progress(event))


async def run_batched_execution(
    *,
    symbol: str,
    total_size: Decimal,
    clip_size: Decimal,
    batch_count: int,
    clip_usd: float,
    venues: list[str],
    interval_ms: int,
    debounce_ms: int,
    run_batch: Callable[[Decimal], Awaitable[dict]],
    on_progress,
    deps: Deps,
) -> dict:
    """Drive the per-batch loop: debounce, run one batch, emit progress, wait interval.

    ``run_batch(this_clip) -> result`` supplies the real execution for one clip.
    Callers (CLI or web) inject their own ``run_batch`` so the actual order code
    can live where its dependencies already resolve.
    """
    remaining = total_size
    executed_total = Decimal("0")
    index = 0
    debounce_timeout_ms = max(debounce_ms * 10, 5000)
    while remaining > 0:
        index += 1
        this_clip = min(clip_size, remaining)
        await _emit(on_progress, {"type": "batch_start", "index": index,
                                  "total": batch_count, "clip_size": str(this_clip)})
        waited = await wait_for_stable_orderbook(
            venues=venues, symbol=symbol, clip_usd=clip_usd, deps=deps,
            debounce_ms=debounce_ms, timeout_ms=debounce_timeout_ms)
        await _emit(on_progress, {"type": "debounce_wait", "waited_ms": waited})
        result = await run_batch(this_clip)
        if not result.get("ok", False):
            await _emit(on_progress, {"type": "batch_failed", "index": index,
                                      "error": str(result.get("error", "batch failed"))})
            return {"ok": False, "batches": index - 1, "error": result.get("error", "batch failed")}
        summary = result.get("summary", {}) or {}
        qty = Decimal(str(result.get("executed_quantity") or this_clip))
        if qty <= 0:
            # Guard against an infinite loop if a batch reports ok but zero fill.
            await _emit(on_progress, {"type": "batch_failed", "index": index,
                                      "error": "executed zero quantity"})
            return {"ok": False, "batches": index - 1, "error": "executed zero quantity"}
        await _emit(on_progress, {"type": "batch_filled", "index": index,
                                  "maker_price": summary.get("maker_avg_price"),
                                  "taker_price": summary.get("taker_avg_price"), "qty": str(qty)})
        executed_total += qty
        remaining = max(Decimal("0"), remaining - qty)
        if remaining > 0 and interval_ms > 0:
            await deps.sleep(interval_ms / 1000.0)
    await _emit(on_progress, {"type": "done", "executed_qty": str(executed_total), "batches": index})
    return {"ok": True, "batches": index, "executed_qty": str(executed_total)}


async def execute_open(params: OpenParams, *, live: bool, on_progress, deps: Deps | None = None) -> dict:
    validate_open_params(params)
    if params.exec_mode == "taker":
        return {"ok": False, "error": "taker-only 尚未實作 (TODO)", "not_implemented": True}
    deps = deps or Deps()
    preview = await build_open_preview(params)
    venues = [preview.maker_venue, preview.taker_venue]
    injected = deps.run_clip

    def _make_run_batch(broker_url: str | None):
        async def run_batch(this_clip: Decimal) -> dict:
            if injected is not None:
                # Test/injected path: caller supplies the per-batch executor.
                result = await injected(params=params, preview=preview, this_clip=this_clip, live=live)
                if live and deps.record_open is not None and result.get("ok", False):
                    await _maybe_await(deps.record_open(result=result, params=params, preview=preview))
                return result
            if not live:
                return {"ok": True, "executed_quantity": str(this_clip), "dry_run": True,
                        "summary": {"maker_avg_price": None, "taker_avg_price": None}}
            # Real path: reuse the same clip executor the CLI uses (recording is
            # done inside execute_open_clip). Lazy import to avoid an import cycle.
            from scripts.place_order import execute_open_clip
            return await execute_open_clip(
                symbol=params.symbol,
                short_venue=preview.short_venue,
                long_venue=preview.long_venue,
                maker_venue=preview.maker_venue,
                taker_venue=preview.taker_venue,
                leverage=params.leverage,
                clip_size=params.clip_size,
                clip_usd=preview.clip_usd,
                batch_clip_size=this_clip,
                broker_url=broker_url,
            )
        return run_batch

    async def _run(broker_url: str | None = None) -> dict:
        return await run_batched_execution(
            symbol=params.symbol, total_size=params.total_size, clip_size=params.clip_size,
            batch_count=preview.batch_count, clip_usd=preview.clip_usd, venues=venues,
            interval_ms=params.interval_ms, debounce_ms=params.debounce_ms,
            run_batch=_make_run_batch(broker_url), on_progress=on_progress, deps=deps)

    # Real live path with a Variational leg needs the embedded broker running for
    # the whole loop (same as the CLI). Only for the default executor (not tests).
    if live and injected is None and "variational" in {preview.maker_venue, preview.taker_venue}:
        from scripts.place_order import (
            MAKER_FILL_TIMEOUT_SECONDS,
            VARIATIONAL_BROKER_HOST,
            VARIATIONAL_BROKER_PORT,
            VARIATIONAL_EXTENSION_TIMEOUT_SECONDS,
            VARIATIONAL_FILL_PORT,
        )
        from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer

        async with VariationalCommandBrokerServer(
            host=VARIATIONAL_BROKER_HOST,
            port=VARIATIONAL_BROKER_PORT,
            fill_host=VARIATIONAL_BROKER_HOST,
            fill_port=VARIATIONAL_FILL_PORT,
            order_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
        ) as server:
            await server.wait_for_extension(timeout_seconds=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
            await server.wait_for_portfolio(timeout_seconds=15.0)
            return await _run(broker_url=server.ws_url)
    return await _run()


# --------------------------------------------------------------------------- #
# Spot-perp entrypoints (reuse the working helpers in run_spot_perp_arbitrage)
# --------------------------------------------------------------------------- #
# NOTE: run_spot_perp_arbitrage imports place_order which imports this module, so
# these imports MUST stay lazy (inside the functions) to avoid an import cycle.
async def build_spot_perp_preview(params: OpenParams) -> Preview:
    validate_open_params(params)
    from scripts.run_spot_perp_arbitrage import (
        MEXC_SPOT_VENUE,
        PREVIEW_ORDERBOOK_CLIP_USD,
        SPOT_PERP_MAX_PRE_TRADE_PRICE_GAP,
        build_spot_perp_plan,
        fetch_plan_books,
    )

    first_clip = min(params.total_size, params.clip_size)
    spot_book, perp_book = await fetch_plan_books(
        symbol=params.symbol, short_venue=params.short_venue, clip_usd=PREVIEW_ORDERBOOK_CLIP_USD)
    plan = build_spot_perp_plan(
        symbol=params.symbol, mode="open", short_venue=params.short_venue,
        quantity=first_clip, clip_usd=None, spot_book=spot_book, perp_book=perp_book)
    batch_count = compute_token_batch_count(params.total_size, params.clip_size)
    total_usd = plan.clip_usd * (float(params.total_size / first_clip) if first_clip > 0 else 1.0)
    return Preview(
        symbol=params.symbol,
        kind="spot_perp",
        maker_venue=plan.maker_venue,
        taker_venue=plan.taker_venue,
        short_venue=plan.short_venue,
        long_venue=MEXC_SPOT_VENUE,
        total_usd=total_usd,
        clip_usd=plan.clip_usd,
        batch_count=batch_count,
        maker_spread_pct=float(plan.perp_spread_pct),
        taker_spread_pct=float(plan.spot_spread_pct),
        requires_confirm=plan.maker_taker_price_gap_pct > SPOT_PERP_MAX_PRE_TRADE_PRICE_GAP,
        exec_mode=params.exec_mode,
    )


async def execute_spot_perp_open(params: OpenParams, *, live: bool, on_progress, deps: Deps | None = None) -> dict:
    validate_open_params(params)
    if params.exec_mode == "taker":
        return {"ok": False, "error": "taker-only 尚未實作 (TODO)", "not_implemented": True}
    deps = deps or Deps()
    from scripts.run_spot_perp_arbitrage import (
        PREVIEW_ORDERBOOK_CLIP_USD,
        VARIATIONAL_BROKER_HOST,
        VARIATIONAL_BROKER_PORT,
        VARIATIONAL_EXTENSION_TIMEOUT_SECONDS,
        build_spot_perp_plan,
        estimate_clip_usd_from_books,
        execute_spot_perp_plan,
        fetch_plan_books,
    )
    from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer

    preview = await build_spot_perp_preview(params)
    spot_book, perp_book = await fetch_plan_books(
        symbol=params.symbol, short_venue=params.short_venue, clip_usd=PREVIEW_ORDERBOOK_CLIP_USD)
    ref = [spot_book, perp_book]

    def _make_run_batch(broker_url: str | None):
        async def _run_batch(this_clip: Decimal) -> dict:
            est = estimate_clip_usd_from_books(clip_size=this_clip, spot_book=ref[0], perp_book=ref[1])
            fresh_spot, fresh_perp = await fetch_plan_books(
                symbol=params.symbol, short_venue=params.short_venue, clip_usd=est)
            ref[0], ref[1] = fresh_spot, fresh_perp
            plan = build_spot_perp_plan(
                symbol=params.symbol, mode="open", short_venue=params.short_venue,
                quantity=this_clip, clip_usd=None, spot_book=fresh_spot, perp_book=fresh_perp)
            if not live:
                return {"ok": True, "executed_quantity": str(this_clip), "dry_run": True,
                        "summary": {"maker_avg_price": plan.maker_price, "taker_avg_price": None}}
            return await execute_spot_perp_plan(
                plan=plan, leverage=params.leverage, broker_url=broker_url, allow_large_price_gap=False)
        return _run_batch

    async def _run(broker_url: str | None = None) -> dict:
        return await run_batched_execution(
            symbol=params.symbol, total_size=params.total_size, clip_size=params.clip_size,
            batch_count=preview.batch_count, clip_usd=preview.clip_usd,
            venues=[preview.maker_venue, preview.taker_venue],
            interval_ms=params.interval_ms, debounce_ms=params.debounce_ms,
            run_batch=_make_run_batch(broker_url), on_progress=on_progress, deps=deps)

    if live and "variational" in {preview.maker_venue, preview.taker_venue, params.short_venue}:
        async with VariationalCommandBrokerServer(
            host=VARIATIONAL_BROKER_HOST, port=VARIATIONAL_BROKER_PORT,
        ) as server:
            await server.wait_for_extension(timeout_seconds=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
            await server.wait_for_portfolio(timeout_seconds=15.0)
            return await _run(broker_url=server.ws_url)
    return await _run()


async def execute_close(params: CloseParams, *, live: bool, on_progress, deps: Deps | None = None) -> dict:
    """Scan live positions for the symbol, auto-select the single SHORT+LONG pair,
    and close it batch-by-batch. Multiple pairs are deferred to the CLI. Reuses
    the same close plan/execute helpers the CLI uses; lazy imports avoid a cycle."""
    deps = deps or Deps()
    from scripts.place_order import (
        ALL_CLOSE_VENUES,
        VARIATIONAL_BROKER_HOST,
        VARIATIONAL_BROKER_PORT,
        VARIATIONAL_EXTENSION_TIMEOUT_SECONDS,
        VARIATIONAL_FILL_PORT,
        build_adapter_for_venue,
        build_close_position_plan,
        check_pre_trade_price_gap,
        close_adapter_if_supported,
        execute_close_position_plan,
        fetch_close_orderbooks,
        find_close_pairs,
        orderbook_mid,
        scan_open_positions,
    )
    from hydra_basis.config import EXECUTION_VENUES_PATH
    from hydra_basis.execution_engine.priority import load_execution_priorities
    from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer

    symbol = params.symbol
    async with VariationalCommandBrokerServer(
        host=VARIATIONAL_BROKER_HOST,
        port=VARIATIONAL_BROKER_PORT,
        fill_host=VARIATIONAL_BROKER_HOST,
        fill_port=VARIATIONAL_FILL_PORT,
        order_fill_timeout_seconds=None,
    ) as server:
        await server.wait_for_extension(timeout_seconds=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
        await server.wait_for_portfolio(timeout_seconds=15.0)
        broker_url = server.ws_url

        legs = await scan_open_positions(
            symbol=symbol, venues=ALL_CLOSE_VENUES + ["variational"], broker_url=broker_url)
        if not legs:
            return {"ok": False, "error": f"找不到 {symbol} 的未平倉部位"}
        pairs = find_close_pairs(legs)
        if not pairs:
            return {"ok": False, "error": f"{symbol} 沒有 SHORT+LONG 配對可平"}
        if len(pairs) > 1:
            return {"ok": False, "error": f"{symbol} 有多個配對，請用 CLI 選擇後平倉"}

        short_leg, long_leg = pairs[0]
        selected_legs = [short_leg, long_leg]
        venues = [leg.venue for leg in selected_legs]
        await _emit(on_progress, {
            "type": "close_pair", "short_venue": short_leg.venue, "long_venue": long_leg.venue,
            "short_qty": str(short_leg.quantity), "long_qty": str(long_leg.quantity)})

        priorities = load_execution_priorities(EXECUTION_VENUES_PATH)
        ref_books = await fetch_close_orderbooks(symbol=symbol, venues=venues, clip_usd=1000.0)
        ref_mid = (orderbook_mid(ref_books[venues[0]]) + orderbook_mid(ref_books[venues[1]])) / 2
        ref_clip_usd = float(params.clip_size) * ref_mid

        async def _run_batch(this_clip: Decimal) -> dict:
            if not live:
                return {"ok": True, "executed_quantity": str(this_clip), "dry_run": True, "summary": {}}
            fresh_books = await fetch_close_orderbooks(
                symbol=symbol, venues=venues, clip_usd=float(this_clip) * ref_mid)
            plan = build_close_position_plan(
                legs=selected_legs, clip_size=this_clip, priorities=priorities, orderbooks=fresh_books)
            await check_pre_trade_price_gap(
                maker_venue=plan.maker_venue, taker_venue=plan.taker_venue,
                maker_book=fresh_books[plan.maker_venue], taker_book=fresh_books[plan.taker_venue])
            exec_adapters = {
                v: build_adapter_for_venue(v, broker_url=broker_url, skip_margin_setup=True) for v in venues}
            try:
                for adapter in exec_adapters.values():
                    warm_up = getattr(adapter, "warm_up", None)
                    if callable(warm_up):
                        await warm_up()
                return await execute_close_position_plan(
                    plan=plan, adapters=exec_adapters, symbol=symbol, venues=venues)
            finally:
                for adapter in exec_adapters.values():
                    await close_adapter_if_supported(adapter)

        return await run_batched_execution(
            symbol=symbol, total_size=params.total_size, clip_size=params.clip_size,
            batch_count=compute_token_batch_count(params.total_size, params.clip_size),
            clip_usd=ref_clip_usd, venues=venues,
            interval_ms=params.interval_ms, debounce_ms=params.debounce_ms,
            run_batch=_run_batch, on_progress=on_progress, deps=deps)


# --------------------------------------------------------------------------- #
# Progress printer (CLI adapter)
# --------------------------------------------------------------------------- #
def progress_printer(*, emit=print, include_filled: bool = True):
    async def _printer(event: dict) -> None:
        t = event.get("type")
        if t == "batch_start":
            emit(f"\nbatch {event['index']}/{event['total']}  clip_size_token={event['clip_size']}")
        elif t == "debounce_wait":
            waited = float(event["waited_ms"])
            if waited > 0:
                emit(f"防抖等待 {waited:.0f}ms")
        elif t == "batch_filled" and include_filled:
            emit(f"限價方成交: {event['maker_price']}  市價方成交: {event['taker_price']}  qty {event['qty']}")
        elif t == "batch_failed":
            emit(f"batch {event['index']} failed: {event['error']}")
        elif t == "done":
            emit(f"\n完成: {event['batches']} 批次已執行")
    return _printer
