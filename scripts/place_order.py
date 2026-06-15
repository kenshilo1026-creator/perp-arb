from __future__ import annotations

import argparse
import asyncio
import math
import time
from dataclasses import dataclass
from decimal import Decimal

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
import aiohttp

from hydra_basis.execution_engine.executor import execute_single_clip, execute_single_clip_with_sides, passive_limit_price_from_orderbook, execution_sides_for_signal
from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter
from hydra_basis.execution_engine.lighter_adapter import LighterExecutionAdapter
from hydra_basis.execution_engine.lighter_live import (
    build_lighter_client_factory_from_env,
    fetch_lighter_market_config,
    fetch_lighter_orderbook_live,
)
from hydra_basis.execution_engine.runtime import prepare_execution_preview_for_size
from hydra_basis.execution_engine.priority import load_execution_priorities, resolve_execution_legs
from hydra_basis.execution_engine.risk import compute_spread_pct
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter
from hydra_basis.formatting import fmt_pct
from hydra_basis.risk_management.models import PositionLeg, close_side_for_position
from hydra_basis.risk_management.recording import record_successful_live_legs
from hydra_basis.config import EXECUTION_VENUES_PATH, MONITOR_SIGNALS_PATH, POSITION_REGISTRY_PATH
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol

load_environment()

VARIATIONAL_MAKER_REPRICE_MIN_CHANGE_PCT = 0.0005


def compute_token_batch_count(total_size: Decimal, clip_size: Decimal) -> int:
    if total_size <= 0 or clip_size <= 0:
        raise RuntimeError("total_size and clip_size must be positive")
    return math.ceil(float(total_size / clip_size))


def prompt_text(label: str) -> str:
    value = input(f"{label}: ").strip().lstrip("﻿")
    if not value:
        raise RuntimeError(f"{label} cannot be empty")
    return value


def prompt_decimal(label: str) -> Decimal:
    value = prompt_text(label)
    try:
        number = Decimal(value)
    except Exception as exc:
        raise RuntimeError(f"{label} must be a number") from exc
    if number <= 0:
        raise RuntimeError(f"{label} must be positive")
    return number


def prompt_int(label: str) -> int:
    value = prompt_text(label)
    try:
        number = int(value)
    except ValueError as exc:
        raise RuntimeError(f"{label} must be an integer") from exc
    if number <= 0:
        raise RuntimeError(f"{label} must be positive")
    return number


MAKER_REPRICE_ATTEMPTS = -1  # -1 = infinite: cancel + reprice every 60s until filled

VARIATIONAL_BROKER_HOST = "127.0.0.1"
VARIATIONAL_BROKER_PORT = 8768
VARIATIONAL_FILL_PORT = 8766
VARIATIONAL_EXTENSION_TIMEOUT_SECONDS = 30.0
VARIATIONAL_ORDER_TIMEOUT_SECONDS = 30.0
MAKER_FILL_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True)
class ClosePositionPlan:
    symbol: str
    quantity: Decimal
    maker_venue: str
    taker_venue: str
    maker_price: str
    clip_usd: float
    side_by_venue: dict[str, str]
    spread_by_venue: dict[str, float]


def build_adapter_for_venue(venue: str, *, leverage: int = 1, broker_url: str | None = None, skip_margin_setup: bool = False):
    v = venue.lower()
    if v == "lighter":
        return LighterExecutionAdapter(
            signer_client_factory=build_lighter_client_factory_from_env(),
            market_config_loader=lambda symbol: fetch_lighter_market_config(symbol),
            orderbook_loader=lambda symbol: fetch_lighter_orderbook_live(symbol),
        )
    if v == "variational":
        if broker_url is not None:
            return VariationalBrowserExecutionAdapter(broker_url=broker_url, timeout_seconds=VARIATIONAL_ORDER_TIMEOUT_SECONDS, fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS)
        return VariationalBrowserExecutionAdapter(timeout_seconds=VARIATIONAL_ORDER_TIMEOUT_SECONDS, fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS)
    if v == "aster":
        return AsterExecutionAdapter(leverage=leverage, skip_margin_setup=skip_margin_setup)
    if v == "hyperliquid":
        return HyperliquidExecutionAdapter(leverage=leverage, skip_margin_setup=skip_margin_setup)
    if v == "mexc":
        return MexcExecutionAdapter(leverage=leverage)
    raise RuntimeError(f"no execution adapter for venue: {venue}")


MAX_PRE_TRADE_PRICE_GAP = 0.01  # 1%


def orderbook_mid(book: dict[str, float | int]) -> float:
    return (float(book["bid"]) + float(book["ask"])) / 2


async def check_pre_trade_price_gap(
    *,
    maker_venue: str,
    taker_venue: str,
    maker_book: dict[str, float | int],
    taker_book: dict[str, float | int],
) -> bool:
    """Returns True if user overrode the gap check (skip executor-level check too)."""
    maker_mid = orderbook_mid(maker_book)
    taker_mid = orderbook_mid(taker_book)
    if maker_mid <= 0 or taker_mid <= 0:
        return False
    gap = abs(maker_mid - taker_mid) / ((maker_mid + taker_mid) / 2)
    print(f"pre-trade: {maker_venue}={maker_mid:g}  {taker_venue}={taker_mid:g}  gap={gap:.2%}")
    if gap > MAX_PRE_TRADE_PRICE_GAP:
        answer = input(f"price gap {gap:.2%} > {MAX_PRE_TRADE_PRICE_GAP:.0%}, continue anyway? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            raise RuntimeError(
                f"pre-trade price gap {gap:.2%} > {MAX_PRE_TRADE_PRICE_GAP:.0%} "
                f"maker={maker_mid:g} taker={taker_mid:g} — order cancelled"
            )
        return True
    return False


def print_execution_prices(result: dict, *, maker_venue: str, taker_venue: str) -> None:
    summary = result.get("execution_price_summary", {})
    if not summary:
        return
    maker_price = summary.get("maker_avg_price", "N/A")
    taker_price = summary.get("taker_avg_price", "N/A")
    gap = summary.get("price_gap_pct")
    print(f"限價方成交({maker_venue}): {maker_price}")
    print(f"市價方成交({taker_venue}): {taker_price}")
    if gap:
        print(f"成交價差: {float(gap):.2%}")


def normalize_execution_mode(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"open", "開倉", "开仓"}:
        return "open"
    if normalized in {"close", "平倉", "平仓"}:
        return "close"
    raise RuntimeError("mode must be open/close or 開倉/平倉")


async def close_adapter_if_supported(adapter: object) -> None:
    close = getattr(adapter, "close", None)
    if close is None:
        return
    result = close()
    if asyncio.iscoroutine(result):
        await result


async def fetch_required_perp_live_leg(
    adapter: object,
    *,
    venue: str,
    symbol: str,
    expected_side: str,
) -> dict:
    getter = getattr(adapter, "get_open_position", None)
    if not callable(getter):
        raise RuntimeError(f"{venue} does not support live position query")
    position = await getter(symbol=symbol, market_type="perp")
    if not position:
        raise RuntimeError(f"no live perp position found for {venue}:{symbol}")
    side = str(position.get("side", "")).strip().upper()
    quantity = str(position.get("quantity", "")).strip()
    if side != expected_side:
        raise RuntimeError(f"unexpected live side for {venue}:{symbol}: {side}, expected {expected_side}")
    try:
        if Decimal(quantity) <= 0:
            raise ValueError
    except Exception as exc:
        raise RuntimeError(f"invalid live quantity for {venue}:{symbol}: {quantity}") from exc
    return {
        "venue": venue,
        "symbol": symbol,
        "market_type": "perp",
        "side": side,
        "quantity": quantity,
    }


async def record_open_execution_from_live_positions(
    *,
    execution_result: dict,
    adapters_by_venue: dict[str, object],
    symbol: str,
    short_venue: str,
    long_venue: str,
    registry_path=POSITION_REGISTRY_PATH,
) -> str:
    short_leg = await fetch_required_perp_live_leg(
        adapters_by_venue[short_venue],
        venue=short_venue,
        symbol=symbol,
        expected_side="SHORT",
    )
    long_leg = await fetch_required_perp_live_leg(
        adapters_by_venue[long_venue],
        venue=long_venue,
        symbol=symbol,
        expected_side="LONG",
    )
    strategy_id = f"manual-{symbol.upper()}-{int(time.time() * 1000)}"
    return record_successful_live_legs(
        path=registry_path,
        symbol=symbol,
        execution_result=execution_result,
        legs=[short_leg, long_leg],
        strategy_id=strategy_id,
    )


def build_close_position_plan(
    *,
    legs: list[PositionLeg],
    clip_size: Decimal,
    priorities: dict[str, int],
    orderbooks: dict[str, dict[str, float | int]],
) -> ClosePositionPlan:
    if clip_size <= 0:
        raise RuntimeError("clip_size must be positive")
    if len(legs) != 2:
        raise RuntimeError("expected exactly 2 legs")

    venues = [leg.venue for leg in legs]
    missing_books = [v for v in venues if v not in orderbooks]
    if missing_books:
        raise RuntimeError(f"missing orderbook for close venues: {missing_books}")

    for leg in legs:
        if clip_size > Decimal(str(leg.quantity)):
            raise RuntimeError(f"clip_size exceeds open quantity on {leg.venue}")

    short_leg = next((leg for leg in legs if leg.side == "SHORT"), None)
    long_leg = next((leg for leg in legs if leg.side == "LONG"), None)
    if short_leg is None or long_leg is None:
        raise RuntimeError(f"could not determine short/long legs from sides: {[leg.side for leg in legs]}")

    spreads = {v: compute_spread_pct(orderbooks[v]) for v in venues}
    maker_venue, taker_venue = resolve_execution_legs(
        short_venue=short_leg.venue,
        long_venue=long_leg.venue,
        priorities=priorities,
        spreads=spreads,
    )
    side_by_venue = {leg.venue: close_side_for_position(leg.side) for leg in legs}
    maker_price = passive_limit_price_from_orderbook(orderbooks[maker_venue], side_by_venue[maker_venue])
    taker_book = orderbooks[taker_venue]
    taker_mid = (float(taker_book["bid"]) + float(taker_book["ask"])) / 2
    return ClosePositionPlan(
        symbol=legs[0].symbol.strip().upper(),
        quantity=clip_size,
        maker_venue=maker_venue,
        taker_venue=taker_venue,
        maker_price=maker_price,
        clip_usd=float(clip_size) * taker_mid,
        side_by_venue=side_by_venue,
        spread_by_venue=spreads,
    )


async def fetch_close_orderbooks(*, symbol: str, venues: list[str], clip_usd: float) -> dict[str, dict[str, float | int]]:
    async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-execution-close/0.1"}) as session:
        results = await asyncio.gather(
            *[
                fetch_orderbook_snapshot(
                    session,
                    venue=venue,
                    symbol=symbol,
                    clip_usd=clip_usd,
                )
                for venue in venues
            ]
        )
    return dict(zip(venues, results))


async def execute_close_position_plan(
    *,
    plan: ClosePositionPlan,
    adapters: dict[str, object],
    symbol: str,
    venues: list[str],
) -> dict[str, object]:
    maker_side = plan.side_by_venue[plan.maker_venue]

    async def _refresh_close_maker_price() -> str:
        fresh_books = await fetch_close_orderbooks(symbol=symbol, venues=venues, clip_usd=plan.clip_usd)
        return passive_limit_price_from_orderbook(fresh_books[plan.maker_venue], maker_side)

    close_price_refresher = None if plan.maker_venue == "variational" else _refresh_close_maker_price

    taker_side = plan.side_by_venue[plan.taker_venue]
    taker_adapter = adapters[plan.taker_venue]
    taker_pre_hook = None
    prepare_fn = getattr(taker_adapter, "prepare_market_order", None)
    if plan.taker_venue == "variational" and callable(prepare_fn):
        async def _prepare_close_taker():
            await prepare_fn(symbol=plan.symbol, side=taker_side, amount=str(plan.quantity), clip_usd=plan.clip_usd)
        taker_pre_hook = _prepare_close_taker

    return await execute_single_clip_with_sides(
        symbol=plan.symbol,
        clip_usd=plan.clip_usd,
        quantity=plan.quantity,
        maker_venue=plan.maker_venue,
        taker_venue=plan.taker_venue,
        maker_side=maker_side,
        taker_side=taker_side,
        maker_adapter=adapters[plan.maker_venue],
        taker_adapter=taker_adapter,
        max_hedge_retries=0,
        state_machine=ExecutionStateMachine(),
        maker_price=None if plan.maker_venue == "variational" else plan.maker_price,
        require_maker_fill_confirmation=True,
        maker_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
        max_maker_reprice_attempts=MAKER_REPRICE_ATTEMPTS,
        maker_reprice_min_change_pct=(
            VARIATIONAL_MAKER_REPRICE_MIN_CHANGE_PCT
            if plan.maker_venue == "variational"
            else 0.0
        ),
        maker_price_refresher=close_price_refresher,
        taker_pre_hook=taker_pre_hook,
    )


ALL_CLOSE_VENUES = ["hyperliquid", "lighter", "aster", "mexc"]


async def scan_open_positions(
    *,
    symbol: str,
    venues: list[str],
    broker_url: str | None = None,
    leverage: int = 1,
) -> list[PositionLeg]:
    legs: list[PositionLeg] = []
    for venue in venues:
        adapter = None
        try:
            adapter = build_adapter_for_venue(venue, leverage=leverage, broker_url=broker_url)
            getter = getattr(adapter, "get_open_position", None)
            if not callable(getter):
                continue
            payload = await getter(symbol=symbol, market_type="perp")
            if not payload:
                print(f"[scan] {venue}: no position")
                continue
            side = str(payload.get("side", "")).strip().upper()
            qty = Decimal(str(payload.get("quantity") or "0"))
            if side not in {"LONG", "SHORT"} or qty <= 0:
                continue
            live_symbol = str(payload.get("symbol") or symbol).strip().upper()
            print(f"[scan] {venue}: {side} {qty} {live_symbol}")
            legs.append(PositionLeg(
                strategy_id="live",
                leg_id=f"live:{venue}:{live_symbol}",
                venue=venue,
                symbol=live_symbol,
                market_type="perp",
                side=side,
                quantity=format(qty.normalize(), "f"),
                status="open",
            ))
        except Exception as exc:
            print(f"[scan] {venue}: error — {exc}")
        finally:
            if adapter is not None:
                await close_adapter_if_supported(adapter)
    return legs


def find_close_pairs(legs: list[PositionLeg]) -> list[tuple[PositionLeg, PositionLeg]]:
    short_legs = [leg for leg in legs if leg.side == "SHORT"]
    long_legs = [leg for leg in legs if leg.side == "LONG"]
    return [(s, l) for s in short_legs for l in long_legs]


async def run_open_execution_once(
    *,
    cli_ticker: str | None = None,
    cli_short_venue: str | None = None,
    cli_long_venue: str | None = None,
    cli_maker_venue: str | None = None,
    cli_taker_venue: str | None = None,
) -> None:
    symbol = cli_ticker.upper() if cli_ticker else prompt_text("ticker").upper()
    total_size = prompt_decimal("total_size_token")
    clip_size = prompt_decimal("clip_size_token")
    leverage = prompt_int("leverage_x")

    _sig = load_best_signal_for_symbol(path=MONITOR_SIGNALS_PATH, symbol=symbol)

    if cli_short_venue and cli_long_venue:
        _sig.short_venue = cli_short_venue.lower()
        _sig.long_venue = cli_long_venue.lower()
        print(f"SHORT {_sig.short_venue} / LONG {_sig.long_venue}")
    else:
        print(f"1. SHORT {_sig.short_venue} / LONG {_sig.long_venue}")
        print(f"2. SHORT {_sig.long_venue} / LONG {_sig.short_venue}")
        direction = prompt_int("direction [1/2]")
        if direction not in {1, 2}:
            raise RuntimeError("direction must be 1 or 2")
        if direction == 2:
            _sig.short_venue, _sig.long_venue = _sig.long_venue, _sig.short_venue

    signal, preview, *_ = await prepare_execution_preview_for_size(
        symbol=symbol,
        total_size=total_size,
        clip_size=clip_size,
        signal=_sig,
    )

    _both_venues = {signal.short_venue, signal.long_venue}
    if cli_maker_venue:
        preview.maker_venue = cli_maker_venue.lower()
        if not cli_taker_venue:
            other = _both_venues - {preview.maker_venue}
            if other:
                preview.taker_venue = other.pop()
    if cli_taker_venue:
        preview.taker_venue = cli_taker_venue.lower()
        if not cli_maker_venue:
            other = _both_venues - {preview.taker_venue}
            if other:
                preview.maker_venue = other.pop()

    print("execution once")
    print(f"ticker: {preview.symbol}")
    print(f"signal_annualized: {fmt_pct(signal.annualized_avg)}")
    print(f"short_venue: {signal.short_venue}")
    print(f"long_venue: {signal.long_venue}")
    print(f"限價方: {preview.maker_venue}")
    print(f"市價方: {preview.taker_venue}")
    print(f"{signal.short_venue}_spread: {fmt_pct(preview.maker_spread_pct if preview.maker_venue == signal.short_venue else preview.taker_spread_pct)}")
    print(f"{signal.long_venue}_spread: {fmt_pct(preview.maker_spread_pct if preview.maker_venue == signal.long_venue else preview.taker_spread_pct)}")
    print(f"estimated_total_usd: {preview.total_usd:.2f}")
    print(f"estimated_clip_usd: {preview.clip_usd:.2f}")
    print(f"total_size_token: {total_size}")
    print(f"clip_size_token: {clip_size}")
    print(f"token_batches: {compute_token_batch_count(total_size, clip_size)}")
    print(f"leverage_x: {leverage}")

    if preview.requires_confirm:
        answer = input("spread > 0.1%, continue? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            print("execution cancelled")
            return

    num_batches = compute_token_batch_count(total_size, clip_size)
    answer = input(f"execute {num_batches} batch(es)? [y/N]: ").strip().lower()
    if answer not in {"y", "yes"}:
        print("execution cancelled")
        return

    clip_usd = preview.clip_usd

    async def execute_one_batch(*, broker_url: str | None = None, batch_clip_size: Decimal) -> dict[str, object]:
        batch_clip_usd = float(batch_clip_size) / float(clip_size) * clip_usd
        maker_adapter = build_adapter_for_venue(preview.maker_venue, leverage=leverage, broker_url=broker_url)
        taker_adapter = build_adapter_for_venue(preview.taker_venue, leverage=leverage, broker_url=broker_url)
        try:
            warm_up = getattr(taker_adapter, "warm_up", None)
            if callable(warm_up):
                await warm_up()
            async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-execution-open/0.1"}) as _sess:
                _fresh = await asyncio.gather(
                    fetch_orderbook_snapshot(_sess, venue=preview.maker_venue, symbol=signal.symbol, clip_usd=batch_clip_usd),
                    fetch_orderbook_snapshot(_sess, venue=preview.taker_venue, symbol=signal.symbol, clip_usd=batch_clip_usd),
                )
            maker_book, taker_book = _fresh[0], _fresh[1]
            gap_overridden = await check_pre_trade_price_gap(
                maker_venue=preview.maker_venue,
                taker_venue=preview.taker_venue,
                maker_book=maker_book,
                taker_book=taker_book,
            )
            maker_side, taker_side = execution_sides_for_signal(
                maker_venue=preview.maker_venue,
                short_venue=signal.short_venue,
                long_venue=signal.long_venue,
            )
            use_maker_orderbook = None if preview.maker_venue == "variational" else maker_book

            async def _refresh_open_maker_price() -> str:
                async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-execution-open/0.1"}) as _s:
                    fresh_book = await fetch_orderbook_snapshot(_s, venue=preview.maker_venue, symbol=signal.symbol, clip_usd=batch_clip_usd)
                return passive_limit_price_from_orderbook(fresh_book, maker_side)

            open_price_refresher = None if preview.maker_venue == "variational" else _refresh_open_maker_price

            taker_pre_hook = None
            prepare_fn = getattr(taker_adapter, "prepare_market_order", None)
            if preview.taker_venue == "variational" and callable(prepare_fn):
                async def _prepare_open_taker():
                    await prepare_fn(symbol=signal.symbol, side=taker_side, amount=str(batch_clip_size), clip_usd=batch_clip_usd)
                taker_pre_hook = _prepare_open_taker

            result = await execute_single_clip(
                symbol=signal.symbol,
                clip_usd=batch_clip_usd,
                quantity=batch_clip_size,
                maker_venue=preview.maker_venue,
                taker_venue=preview.taker_venue,
                short_venue=signal.short_venue,
                long_venue=signal.long_venue,
                maker_adapter=maker_adapter,
                taker_adapter=taker_adapter,
                max_hedge_retries=0,
                state_machine=ExecutionStateMachine(),
                maker_orderbook=use_maker_orderbook,
                taker_orderbook=taker_book,
                require_maker_fill_confirmation=True,
                maker_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
                max_maker_reprice_attempts=MAKER_REPRICE_ATTEMPTS,
                maker_reprice_min_change_pct=(
                    VARIATIONAL_MAKER_REPRICE_MIN_CHANGE_PCT
                    if preview.maker_venue == "variational"
                    else 0.0
                ),
                maker_price_refresher=open_price_refresher,
                taker_pre_hook=taker_pre_hook,
                max_execution_price_gap_pct=float("inf") if gap_overridden else MAX_PRE_TRADE_PRICE_GAP,
            )
            if result.get("ok", False):
                strategy_id = await record_open_execution_from_live_positions(
                    execution_result=result,
                    adapters_by_venue={
                        preview.maker_venue: maker_adapter,
                        preview.taker_venue: taker_adapter,
                    },
                    symbol=signal.symbol,
                    short_venue=signal.short_venue,
                    long_venue=signal.long_venue,
                )
                result["recorded_strategy_id"] = strategy_id
                print(f"position_registry recorded strategy_id={strategy_id}")
            return result
        finally:
            await close_adapter_if_supported(maker_adapter)
            if taker_adapter is not maker_adapter:
                await close_adapter_if_supported(taker_adapter)

    async def run_batches(*, broker_url: str | None = None) -> None:
        remaining = total_size
        batch_idx = 0
        while remaining > 0:
            batch_idx += 1
            this_clip = min(clip_size, remaining)
            print(f"\nbatch {batch_idx}/{num_batches}  clip_size_token={this_clip}")
            result = await execute_one_batch(broker_url=broker_url, batch_clip_size=this_clip)
            print_execution_prices(result, maker_venue=preview.maker_venue, taker_venue=preview.taker_venue)
            if not result.get("ok", False):
                raise RuntimeError(f"batch {batch_idx} failed — stopping")
            executed_quantity = Decimal(str(result.get("executed_quantity") or this_clip))
            if executed_quantity <= 0:
                raise RuntimeError(f"batch {batch_idx} executed zero quantity — stopping")
            remaining = max(Decimal("0"), remaining - executed_quantity)
        print(f"\nopen complete: {batch_idx} batch(es) executed")

    if "variational" in {preview.maker_venue, preview.taker_venue}:
        print(
            "starting embedded Variational broker. "
            "Open Variational page with the Chrome extension connected to "
            f"command=ws://{VARIATIONAL_BROKER_HOST}:{VARIATIONAL_BROKER_PORT} "
            f"and ws_frames=ws://{VARIATIONAL_BROKER_HOST}:{VARIATIONAL_FILL_PORT}"
        )
        async with VariationalCommandBrokerServer(
            host=VARIATIONAL_BROKER_HOST,
            port=VARIATIONAL_BROKER_PORT,
            fill_host=VARIATIONAL_BROKER_HOST,
            fill_port=VARIATIONAL_FILL_PORT,
            order_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
        ) as server:
            print(
                "waiting for Variational extension command client "
                f"timeout={VARIATIONAL_EXTENSION_TIMEOUT_SECONDS:.1f}s"
            )
            await server.wait_for_extension(timeout_seconds=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
            print("extension connected — waiting for portfolio data...")
            await server.wait_for_portfolio(timeout_seconds=15.0)
            print("portfolio data received")
            await run_batches(broker_url=server.ws_url)
    else:
        await run_batches()


async def run_close_execution_once(*, cli_ticker: str | None = None) -> None:
    symbol = cli_ticker.upper() if cli_ticker else prompt_text("ticker").upper()

    async with VariationalCommandBrokerServer(
        host=VARIATIONAL_BROKER_HOST,
        port=VARIATIONAL_BROKER_PORT,
        fill_host=VARIATIONAL_BROKER_HOST,
        fill_port=VARIATIONAL_FILL_PORT,
        order_fill_timeout_seconds=None,
    ) as server:
        print(
            "waiting for Variational extension "
            f"(command=ws://{VARIATIONAL_BROKER_HOST}:{VARIATIONAL_BROKER_PORT} "
            f"ws_frames=ws://{VARIATIONAL_BROKER_HOST}:{VARIATIONAL_FILL_PORT}) "
            f"timeout={VARIATIONAL_EXTENSION_TIMEOUT_SECONDS:.0f}s"
        )
        await server.wait_for_extension(timeout_seconds=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
        print("variational extension connected — waiting for portfolio data...")
        await server.wait_for_portfolio(timeout_seconds=15.0)
        print("portfolio data received")
        broker_url: str = server.ws_url
        scan_venues = ALL_CLOSE_VENUES + ["variational"]

        print(f"scanning for open {symbol} positions...")
        legs = await scan_open_positions(symbol=symbol, venues=scan_venues, broker_url=broker_url)

        if not legs:
            print(f"no open positions found for {symbol}")
            return

        pairs = find_close_pairs(legs)
        if not pairs:
            found = ", ".join(f"{leg.venue} {leg.side}" for leg in legs)
            print(f"no SHORT+LONG pair found — positions: {found}")
            return

        if len(pairs) == 1:
            short_leg, long_leg = pairs[0]
        else:
            print("multiple pairs found:")
            for i, (s, l) in enumerate(pairs, 1):
                print(f"  {i}. {s.venue} SHORT {s.quantity} / {l.venue} LONG {l.quantity}")
            choice = prompt_int(f"select pair [1-{len(pairs)}]")
            if choice < 1 or choice > len(pairs):
                raise RuntimeError(f"invalid pair selection: {choice}")
            short_leg, long_leg = pairs[choice - 1]

        selected_legs = [short_leg, long_leg]
        venues = [leg.venue for leg in selected_legs]
        print(f"pair: {short_leg.venue} SHORT {short_leg.quantity} / {long_leg.venue} LONG {long_leg.quantity}")

        total_size = prompt_decimal("total_size_token")
        clip_size = prompt_decimal("clip_size_token")
        num_batches = compute_token_batch_count(total_size, clip_size)

        priorities = load_execution_priorities(EXECUTION_VENUES_PATH)

        ref_books = await fetch_close_orderbooks(symbol=symbol, venues=venues, clip_usd=1000.0)
        ref_mid = (orderbook_mid(ref_books[venues[0]]) + orderbook_mid(ref_books[venues[1]])) / 2
        ref_clip_usd = float(clip_size) * ref_mid
        ref_plan = build_close_position_plan(
            legs=selected_legs,
            clip_size=clip_size,
            priorities=priorities,
            orderbooks=ref_books,
        )

        print("平倉預覽")
        print(f"ticker: {ref_plan.symbol}")
        print(f"total_size_token: {total_size}  clip_size_token: {clip_size}  batches: {num_batches}")
        print(f"estimated_clip_usd: ~{ref_clip_usd:.2f}")
        print(f"限價方: {ref_plan.maker_venue} {ref_plan.side_by_venue[ref_plan.maker_venue]}")
        print(f"市價方: {ref_plan.taker_venue} {ref_plan.side_by_venue[ref_plan.taker_venue]}")
        for venue, spread in ref_plan.spread_by_venue.items():
            print(f"{venue}_spread: {fmt_pct(spread)}")

        answer = input(f"execute {num_batches} close batch(es)? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            print("close cancelled")
            return

        remaining = total_size
        batch_idx = 0
        while remaining > 0:
            batch_idx += 1
            this_clip = min(clip_size, remaining)
            print(f"\nbatch {batch_idx}/{num_batches}  clip_size_token={this_clip}")

            fresh_books = await fetch_close_orderbooks(symbol=symbol, venues=venues, clip_usd=float(this_clip) * ref_mid)

            plan = build_close_position_plan(
                legs=selected_legs,
                clip_size=this_clip,
                priorities=priorities,
                orderbooks=fresh_books,
            )

            await check_pre_trade_price_gap(
                maker_venue=plan.maker_venue,
                taker_venue=plan.taker_venue,
                maker_book=fresh_books[plan.maker_venue],
                taker_book=fresh_books[plan.taker_venue],
            )

            exec_adapters = {
                v: build_adapter_for_venue(v, broker_url=broker_url, skip_margin_setup=True)
                for v in venues
            }
            try:
                for adapter in exec_adapters.values():
                    warm_up = getattr(adapter, "warm_up", None)
                    if callable(warm_up):
                        await warm_up()
                result = await execute_close_position_plan(plan=plan, adapters=exec_adapters, symbol=symbol, venues=venues)
            finally:
                for adapter in exec_adapters.values():
                    await close_adapter_if_supported(adapter)

            print_execution_prices(result, maker_venue=plan.maker_venue, taker_venue=plan.taker_venue)

            if not result.get("ok", False):
                raise RuntimeError(f"batch {batch_idx} failed — stopping")
            executed_quantity = Decimal(str(result.get("executed_quantity") or this_clip))
            if executed_quantity <= 0:
                raise RuntimeError(f"batch {batch_idx} executed zero quantity — stopping")
            remaining = max(Decimal("0"), remaining - executed_quantity)

        print(f"\nclose complete: {batch_idx} batch(es) executed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Place a perpetual arb order")
    parser.add_argument("--ticker", type=str, default=None, help="token symbol, e.g. BTC")
    parser.add_argument("--short_venue", type=str, default=None, help="venue to go short on")
    parser.add_argument("--long_venue", type=str, default=None, help="venue to go long on")
    parser.add_argument("--maker_venue", type=str, default=None, help="force this venue as maker (limit order)")
    parser.add_argument("--taker_venue", type=str, default=None, help="force this venue as taker (market order)")
    return parser.parse_args()


async def run_place_order(args: argparse.Namespace) -> None:
    mode = normalize_execution_mode(prompt_text("mode [open/close/開倉/平倉]"))
    if mode == "open":
        await run_open_execution_once(
            cli_ticker=args.ticker,
            cli_short_venue=args.short_venue,
            cli_long_venue=args.long_venue,
            cli_maker_venue=args.maker_venue,
            cli_taker_venue=args.taker_venue,
        )
    else:
        await run_close_execution_once(cli_ticker=args.ticker)


def main() -> None:
    args = parse_args()
    asyncio.run(run_place_order(args))


if __name__ == "__main__":
    main()
