from __future__ import annotations

import asyncio
import math
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

from hydra_basis.execution_engine.executor import execute_single_clip, execute_single_clip_with_sides, passive_limit_price_from_orderbook
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
from hydra_basis.config import EXECUTION_VENUES_PATH

load_environment()


def compute_batch_count(total_usd: float, clip_usd: float) -> int:
    if total_usd <= 0 or clip_usd <= 0:
        raise RuntimeError("total_usd and clip_usd must be positive")
    return math.ceil(total_usd / clip_usd)


def compute_token_batch_count(total_size: Decimal, clip_size: Decimal) -> int:
    if total_size <= 0 or clip_size <= 0:
        raise RuntimeError("total_size and clip_size must be positive")
    return math.ceil(float(total_size / clip_size))


def prompt_text(label: str) -> str:
    value = input(f"{label}: ").strip().lstrip("﻿")
    if not value:
        raise RuntimeError(f"{label} cannot be empty")
    return value


def prompt_float(label: str) -> float:
    value = prompt_text(label)
    try:
        number = float(value)
    except ValueError as exc:
        raise RuntimeError(f"{label} must be a number") from exc
    if number <= 0:
        raise RuntimeError(f"{label} must be positive")
    return number


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


def build_adapter_for_venue(venue: str, *, leverage: int = 1, broker_url: str | None = None):
    v = venue.lower()
    if v == "lighter":
        return LighterExecutionAdapter(
            signer_client_factory=build_lighter_client_factory_from_env(),
            market_config_loader=lambda symbol: fetch_lighter_market_config(symbol),
            orderbook_loader=lambda symbol: fetch_lighter_orderbook_live(symbol),
        )
    if v == "variational":
        if broker_url is not None:
            return VariationalBrowserExecutionAdapter(broker_url=broker_url, timeout_seconds=VARIATIONAL_ORDER_TIMEOUT_SECONDS)
        return VariationalBrowserExecutionAdapter(timeout_seconds=VARIATIONAL_ORDER_TIMEOUT_SECONDS)
    if v == "aster":
        return AsterExecutionAdapter(leverage=leverage)
    if v == "hyperliquid":
        return HyperliquidExecutionAdapter(leverage=leverage)
    if v == "mexc":
        return MexcExecutionAdapter(leverage=leverage)
    raise RuntimeError(f"no execution adapter for venue: {venue}")


MAX_PRE_TRADE_PRICE_GAP = 0.01  # 1%


def validate_maker_fill_supported(_maker_venue: str) -> None:
    return None


def orderbook_mid(book: dict[str, float | int]) -> float:
    return (float(book["bid"]) + float(book["ask"])) / 2


async def check_pre_trade_price_gap(
    *,
    maker_venue: str,
    taker_venue: str,
    maker_book: dict[str, float | int],
    taker_book: dict[str, float | int],
) -> None:
    maker_mid = orderbook_mid(maker_book)
    taker_mid = orderbook_mid(taker_book)
    if maker_mid <= 0 or taker_mid <= 0:
        return
    gap = abs(maker_mid - taker_mid) / ((maker_mid + taker_mid) / 2)
    print(f"pre-trade: {maker_venue}={maker_mid:g}  {taker_venue}={taker_mid:g}  gap={gap:.2%}")
    if gap > MAX_PRE_TRADE_PRICE_GAP:
        raise RuntimeError(
            f"pre-trade price gap {gap:.2%} > {MAX_PRE_TRADE_PRICE_GAP:.0%} "
            f"maker={maker_mid:g} taker={taker_mid:g} — order cancelled"
        )


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
) -> dict[str, object]:
    return await execute_single_clip_with_sides(
        symbol=plan.symbol,
        clip_usd=plan.clip_usd,
        quantity=plan.quantity,
        maker_venue=plan.maker_venue,
        taker_venue=plan.taker_venue,
        maker_side=plan.side_by_venue[plan.maker_venue],
        taker_side=plan.side_by_venue[plan.taker_venue],
        maker_adapter=adapters[plan.maker_venue],
        taker_adapter=adapters[plan.taker_venue],
        max_hedge_retries=2,
        state_machine=ExecutionStateMachine(),
        maker_price=plan.maker_price,
        require_maker_fill_confirmation=True,
        maker_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
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
        adapter = build_adapter_for_venue(venue, leverage=leverage, broker_url=broker_url)
        try:
            getter = getattr(adapter, "get_open_position", None)
            if not callable(getter):
                print(f"  {venue}: no get_open_position support")
                continue
            payload = await getter(symbol=symbol, market_type="perp")
            print(f"  {venue}: raw={payload}")
            if not payload:
                print(f"  {venue}: no position")
                continue
            side = str(payload.get("side", "")).strip().upper()
            qty = Decimal(str(payload.get("quantity") or "0"))
            if side not in {"LONG", "SHORT"} or qty <= 0:
                print(f"  {venue}: skipped side={side!r} qty={qty}")
                continue
            live_symbol = str(payload.get("symbol") or symbol).strip().upper()
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
            print(f"  {venue}: found {side} {qty}")
        except Exception as exc:
            print(f"  {venue}: ERROR — {exc}")
        finally:
            await close_adapter_if_supported(adapter)
    return legs


def find_close_pairs(legs: list[PositionLeg]) -> list[tuple[PositionLeg, PositionLeg]]:
    short_legs = [leg for leg in legs if leg.side == "SHORT"]
    long_legs = [leg for leg in legs if leg.side == "LONG"]
    return [(s, l) for s in short_legs for l in long_legs]


async def run_open_execution_once() -> None:
    symbol = prompt_text("ticker").upper()
    total_size = prompt_decimal("total_size_token")
    clip_size = prompt_decimal("clip_size_token")
    leverage = prompt_int("leverage_x")

    signal, preview, *_ = await prepare_execution_preview_for_size(
        symbol=symbol,
        total_size=total_size,
        clip_size=clip_size,
    )

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

    answer = input("place real order for one clip? [y/N]: ").strip().lower()
    if answer not in {"y", "yes"}:
        print("execution cancelled")
        return

    quantity = clip_size
    clip_usd = preview.clip_usd
    validate_maker_fill_supported(preview.maker_venue)

    async def execute_with_adapters(*, broker_url: str | None = None) -> dict[str, object]:
        maker_adapter = build_adapter_for_venue(preview.maker_venue, leverage=leverage, broker_url=broker_url)
        taker_adapter = build_adapter_for_venue(preview.taker_venue, leverage=leverage, broker_url=broker_url)
        warm_up = getattr(taker_adapter, "warm_up", None)
        if callable(warm_up):
            await warm_up()
        async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-execution-open/0.1"}) as _sess:
            _fresh = await asyncio.gather(
                fetch_orderbook_snapshot(_sess, venue=preview.maker_venue, symbol=signal.symbol, clip_usd=clip_usd),
                fetch_orderbook_snapshot(_sess, venue=preview.taker_venue, symbol=signal.symbol, clip_usd=clip_usd),
            )
        maker_book, taker_book = _fresh[0], _fresh[1]
        await check_pre_trade_price_gap(
            maker_venue=preview.maker_venue,
            taker_venue=preview.taker_venue,
            maker_book=maker_book,
            taker_book=taker_book,
        )
        # Variational uses the Mid button in the UI — don't pass an explicit price
        use_maker_orderbook = None if preview.maker_venue == "variational" else maker_book
        return await execute_single_clip(
            symbol=signal.symbol,
            clip_usd=clip_usd,
            quantity=quantity,
            maker_venue=preview.maker_venue,
            taker_venue=preview.taker_venue,
            short_venue=signal.short_venue,
            long_venue=signal.long_venue,
            maker_adapter=maker_adapter,
            taker_adapter=taker_adapter,
            max_hedge_retries=2,
            state_machine=ExecutionStateMachine(),
            maker_orderbook=use_maker_orderbook,
            taker_orderbook=taker_book,
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
        )

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
            order_fill_timeout_seconds=None,
        ) as server:
            print(
                "waiting for Variational extension command client "
                f"timeout={VARIATIONAL_EXTENSION_TIMEOUT_SECONDS:.1f}s"
            )
            await server.wait_for_extension(timeout_seconds=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
            print("extension connected — waiting for portfolio data...")
            await server.wait_for_portfolio(timeout_seconds=15.0)
            print("portfolio data received")
            result = await execute_with_adapters(broker_url=server.ws_url)
    else:
        result = await execute_with_adapters()
    print("execution result")
    print_execution_prices(result, maker_venue=preview.maker_venue, taker_venue=preview.taker_venue)


async def run_close_execution_once() -> None:
    symbol = prompt_text("ticker").upper()

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

        print(f"scanning {scan_venues} for open {symbol} positions...")
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

        clip_size = prompt_decimal("clip_size_token")

        priorities = load_execution_priorities(EXECUTION_VENUES_PATH)
        initial_books = await fetch_close_orderbooks(symbol=symbol, venues=venues, clip_usd=1000.0)
        initial_plan = build_close_position_plan(
            legs=selected_legs,
            clip_size=clip_size,
            priorities=priorities,
            orderbooks=initial_books,
        )
        orderbooks = await fetch_close_orderbooks(symbol=symbol, venues=venues, clip_usd=initial_plan.clip_usd)
        plan = build_close_position_plan(
            legs=selected_legs,
            clip_size=clip_size,
            priorities=priorities,
            orderbooks=orderbooks,
        )

        print("平倉預覽")
        print(f"ticker: {plan.symbol}")
        print(f"限價方: {plan.maker_venue} {plan.side_by_venue[plan.maker_venue]} price={plan.maker_price}")
        print(f"市價方: {plan.taker_venue} {plan.side_by_venue[plan.taker_venue]}")
        print(f"clip_size_token: {plan.quantity}")
        print(f"estimated_clip_usd: {plan.clip_usd:.2f}")
        for venue, spread in plan.spread_by_venue.items():
            print(f"{venue}_spread: {fmt_pct(spread)}")
        answer = input("place real close order for one clip? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            print("close cancelled")
            return

        exec_adapters = {
            v: build_adapter_for_venue(v, broker_url=broker_url)
            for v in venues
        }
        try:
            for adapter in exec_adapters.values():
                warm_up = getattr(adapter, "warm_up", None)
                if callable(warm_up):
                    await warm_up()
            fresh_books = await fetch_close_orderbooks(symbol=plan.symbol, venues=venues, clip_usd=plan.clip_usd)
            await check_pre_trade_price_gap(
                maker_venue=plan.maker_venue,
                taker_venue=plan.taker_venue,
                maker_book=fresh_books[plan.maker_venue],
                taker_book=fresh_books[plan.taker_venue],
            )
            result = await execute_close_position_plan(plan=plan, adapters=exec_adapters)
        finally:
            for adapter in exec_adapters.values():
                await close_adapter_if_supported(adapter)

        print("close execution result")
        print_execution_prices(result, maker_venue=plan.maker_venue, taker_venue=plan.taker_venue)


async def run_execution_once() -> None:
    mode = normalize_execution_mode(prompt_text("mode [open/close/開倉/平倉]"))
    if mode == "open":
        await run_open_execution_once()
        return
    await run_close_execution_once()


def main() -> None:
    asyncio.run(run_execution_once())


if __name__ == "__main__":
    main()
