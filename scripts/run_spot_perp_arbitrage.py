from __future__ import annotations

import argparse
import asyncio
import math
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.config import POSITION_REGISTRY_PATH
from hydra_basis.execution_engine.executor import (
    execute_single_clip,
    price_gap_pct,
    taker_price_from_orderbook,
)
from hydra_basis.execution_engine.market_data import (
    fetch_mexc_spot_orderbook,
    fetch_orderbook_snapshot,
)
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.risk import compute_spread_pct
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
from hydra_basis.formatting import fmt_pct
from hydra_basis.risk_management.recording import record_successful_live_legs
from scripts.place_order import (
    MAKER_FILL_TIMEOUT_SECONDS,
    MAKER_REPRICE_ATTEMPTS,
    VARIATIONAL_MAKER_REPRICE_MIN_CHANGE_PCT,
    build_adapter_for_venue,
)


load_environment()

MEXC_SPOT_VENUE = "mexc_spot"
SUPPORTED_MODES = {"open", "close"}
SUPPORTED_SHORT_VENUES = {"aster", "hyperliquid", "lighter", "variational"}
LIVE_CONFIRMATION_PHRASE = "PLACE LIVE SPOT PERP ORDER"
VARIATIONAL_BROKER_HOST = "127.0.0.1"
VARIATIONAL_BROKER_PORT = 8768
VARIATIONAL_EXTENSION_TIMEOUT_SECONDS = 30.0
SPOT_PERP_MAX_PRE_TRADE_PRICE_GAP = Decimal("0.01")
SPOT_PERP_MIN_HEDGE_NOTIONAL_USD = Decimal("5.0")
PREVIEW_ORDERBOOK_CLIP_USD = 1_000.0


@dataclass(frozen=True)
class SpotPerpPlan:
    symbol: str
    mode: str
    short_venue: str
    maker_venue: str
    taker_venue: str
    maker_side: str
    taker_side: str
    maker_price: str
    quantity: Decimal
    clip_usd: float
    spot_spread_pct: float
    perp_spread_pct: float
    maker_orderbook: dict[str, float | int]
    taker_orderbook: dict[str, float | int]
    maker_execution_price: Decimal
    taker_execution_price: Decimal
    maker_taker_price_gap_pct: Decimal


def normalize_mode(mode: str) -> str:
    normalized = mode.strip().lower()
    if normalized not in SUPPORTED_MODES:
        raise RuntimeError(f"unsupported mode: {mode}")
    return normalized


def normalize_short_venue(venue: str) -> str:
    normalized = venue.strip().lower()
    if normalized not in SUPPORTED_SHORT_VENUES:
        raise RuntimeError(f"unsupported short venue: {venue}")
    return normalized


def spot_perp_sides(*, mode: str) -> dict[str, str]:
    normalized = normalize_mode(mode)
    if normalized == "open":
        return {MEXC_SPOT_VENUE: "BUY", "perp": "SELL"}
    return {MEXC_SPOT_VENUE: "SELL", "perp": "BUY"}


def decimal_to_plain(value: Decimal | float | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    plain = format(decimal_value.normalize(), "f")
    if "." in plain:
        plain = plain.rstrip("0").rstrip(".")
    return plain or "0"


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


def compute_token_batch_count(total_size: Decimal, clip_size: Decimal) -> int:
    if total_size <= 0 or clip_size <= 0:
        raise RuntimeError("total_size and clip_size must be positive")
    return math.ceil(float(total_size / clip_size))


def orderbook_mid(book: dict[str, float | int]) -> Decimal:
    return (Decimal(str(book["bid"])) + Decimal(str(book["ask"]))) / Decimal("2")


def estimate_clip_usd_from_books(
    *,
    clip_size: Decimal,
    spot_book: dict[str, float | int],
    perp_book: dict[str, float | int],
) -> float:
    reference_mid = (orderbook_mid(spot_book) + orderbook_mid(perp_book)) / Decimal("2")
    return float(clip_size * reference_mid)


def assert_min_spot_perp_notional(*, clip_size: Decimal, clip_usd: float) -> None:
    notional = Decimal(str(clip_usd))
    if notional >= SPOT_PERP_MIN_HEDGE_NOTIONAL_USD:
        return
    raise RuntimeError(
        "spot-perp clip notional below exchange minimum: "
        f"clip_size={decimal_to_plain(clip_size)} "
        f"estimated_notional={notional:.6f} "
        f"min={decimal_to_plain(SPOT_PERP_MIN_HEDGE_NOTIONAL_USD)}"
    )


def maker_limit_price(book: dict[str, float | int], side: str) -> str:
    normalized_side = side.strip().upper()
    if normalized_side == "BUY":
        return decimal_to_plain(book["bid"])
    if normalized_side == "SELL":
        return decimal_to_plain(book["ask"])
    raise RuntimeError(f"unsupported side: {side}")


def compute_base_quantity(
    *,
    quantity: Decimal | None,
    clip_usd: float | None,
    taker_book: dict[str, float | int],
) -> Decimal:
    if quantity is not None:
        if quantity <= 0:
            raise RuntimeError("quantity must be positive")
        return quantity
    if clip_usd is None or clip_usd <= 0:
        raise RuntimeError("either positive quantity or clip_usd is required")
    mid = (Decimal(str(taker_book["bid"])) + Decimal(str(taker_book["ask"]))) / Decimal("2")
    if mid <= 0:
        raise RuntimeError("invalid taker book mid price")
    return Decimal(str(clip_usd)) / mid


def build_spot_perp_plan(
    *,
    symbol: str,
    mode: str,
    short_venue: str,
    quantity: Decimal | None,
    clip_usd: float | None,
    spot_book: dict[str, float | int],
    perp_book: dict[str, float | int],
) -> SpotPerpPlan:
    normalized_mode = normalize_mode(mode)
    normalized_short_venue = normalize_short_venue(short_venue)
    spot_spread_pct = compute_spread_pct(spot_book)
    perp_spread_pct = compute_spread_pct(perp_book)
    taker_venue = MEXC_SPOT_VENUE if spot_spread_pct <= perp_spread_pct else normalized_short_venue
    maker_venue = normalized_short_venue if taker_venue == MEXC_SPOT_VENUE else MEXC_SPOT_VENUE
    sides = spot_perp_sides(mode=normalized_mode)
    side_by_venue = {
        MEXC_SPOT_VENUE: sides[MEXC_SPOT_VENUE],
        normalized_short_venue: sides["perp"],
    }
    taker_book = spot_book if taker_venue == MEXC_SPOT_VENUE else perp_book
    maker_book = spot_book if maker_venue == MEXC_SPOT_VENUE else perp_book
    base_quantity = compute_base_quantity(
        quantity=quantity,
        clip_usd=clip_usd,
        taker_book=taker_book,
    )
    effective_clip_usd = float(clip_usd) if clip_usd is not None else float(
        base_quantity * (
            Decimal(str(taker_book["bid"])) + Decimal(str(taker_book["ask"]))
        ) / Decimal("2")
    )
    maker_side = side_by_venue[maker_venue]
    maker_execution_price = Decimal(maker_limit_price(maker_book, maker_side))
    taker_execution_price = Decimal(taker_price_from_orderbook(taker_book, side_by_venue[taker_venue]))
    return SpotPerpPlan(
        symbol=symbol.strip().upper(),
        mode=normalized_mode,
        short_venue=normalized_short_venue,
        maker_venue=maker_venue,
        taker_venue=taker_venue,
        maker_side=maker_side,
        taker_side=side_by_venue[taker_venue],
        maker_price=maker_limit_price(maker_book, maker_side),
        quantity=base_quantity,
        clip_usd=effective_clip_usd,
        spot_spread_pct=spot_spread_pct,
        perp_spread_pct=perp_spread_pct,
        maker_orderbook=maker_book,
        taker_orderbook=taker_book,
        maker_execution_price=maker_execution_price,
        taker_execution_price=taker_execution_price,
        maker_taker_price_gap_pct=price_gap_pct(maker_execution_price, taker_execution_price),
    )


def build_spot_perp_adapter(venue: str, *, leverage: int, broker_url: str | None = None):
    if venue == MEXC_SPOT_VENUE:
        return MexcSpotExecutionAdapter()
    return build_adapter_for_venue(venue, leverage=leverage, broker_url=broker_url)


async def refresh_spot_perp_maker_price(plan: SpotPerpPlan) -> str:
    spot_book, perp_book = await fetch_plan_books(
        symbol=plan.symbol,
        short_venue=plan.short_venue,
        clip_usd=plan.clip_usd,
    )
    maker_book = spot_book if plan.maker_venue == MEXC_SPOT_VENUE else perp_book
    price = maker_limit_price(maker_book, plan.maker_side)
    print(
        "refreshed maker book: "
        f"{plan.maker_venue} {plan.maker_side} "
        f"bid={maker_book['bid']} ask={maker_book['ask']} price={price}",
        flush=True,
    )
    return price


def display_venue_name(venue: str) -> str:
    if venue == MEXC_SPOT_VENUE:
        return "mexc"
    return venue


def price_for_venue_from_execution_summary(
    *,
    plan: SpotPerpPlan,
    result: dict[str, object],
    venue: str,
) -> object:
    summary = result.get("execution_price_summary")
    if not isinstance(summary, dict):
        return "N/A"
    key = "maker_avg_price" if venue == plan.maker_venue else "taker_avg_price"
    return summary.get(key) or "N/A"


def format_spot_perp_execution_summary(
    *,
    plan: SpotPerpPlan,
    result: dict[str, object],
) -> list[str]:
    spot_price = price_for_venue_from_execution_summary(
        plan=plan,
        result=result,
        venue=MEXC_SPOT_VENUE,
    )
    short_price = price_for_venue_from_execution_summary(
        plan=plan,
        result=result,
        venue=plan.short_venue,
    )
    return [
        f"下單成功 {display_venue_name(MEXC_SPOT_VENUE)}現貨成交價: {spot_price}",
        f"{display_venue_name(plan.short_venue)}做空成交價: {short_price}",
    ]


def format_spot_perp_price_gap_alert(plan: SpotPerpPlan) -> str:
    return (
        "價差過大，是否停止下單(Y/N): "
        f"maker={decimal_to_plain(plan.maker_execution_price)} "
        f"taker={decimal_to_plain(plan.taker_execution_price)} "
        f"pre_trade_price_gap={float(plan.maker_taker_price_gap_pct):.2%} "
        f"> {float(SPOT_PERP_MAX_PRE_TRADE_PRICE_GAP):.2%}"
    )


def assert_maker_limit_supported(adapter, venue: str) -> None:
    if not hasattr(adapter, "place_limit_order"):
        raise RuntimeError(f"maker venue {venue} does not support limit orders yet")


async def fetch_required_live_position(
    adapter,
    *,
    venue: str,
    symbol: str,
    market_type: str,
    expected_side: str,
) -> dict:
    getter = getattr(adapter, "get_open_position", None)
    if not callable(getter):
        raise RuntimeError(f"{venue} does not support live position query")
    position = await getter(symbol=symbol, market_type=market_type)
    if not position:
        raise RuntimeError(f"no live {market_type} position found for {venue}:{symbol}")
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
        "market_type": market_type,
        "side": side,
        "quantity": quantity,
    }


async def fetch_plan_books(
    *,
    symbol: str,
    short_venue: str,
    clip_usd: float,
) -> tuple[dict[str, float | int], dict[str, float | int]]:
    async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-spot-perp/0.1"}) as session:
        spot_book, perp_book = await asyncio.gather(
            fetch_mexc_spot_orderbook(session, symbol),
            fetch_orderbook_snapshot(
                session,
                venue=short_venue,
                symbol=symbol,
                clip_usd=clip_usd,
            ),
        )
    return spot_book, perp_book


async def execute_spot_perp_plan(
    *,
    plan: SpotPerpPlan,
    leverage: int,
    broker_url: str | None = None,
    registry_path: Path = POSITION_REGISTRY_PATH,
    allow_large_price_gap: bool = False,
) -> dict[str, object]:
    maker_adapter = build_spot_perp_adapter(plan.maker_venue, leverage=leverage, broker_url=broker_url)
    taker_adapter = build_spot_perp_adapter(plan.taker_venue, leverage=leverage, broker_url=broker_url)
    assert_maker_limit_supported(maker_adapter, plan.maker_venue)
    try:
        fresh_maker_price = await refresh_spot_perp_maker_price(plan)
        # For close mode, treating spot as the "short venue" makes the existing
        # side mapper emit spot SELL and perp BUY.
        side_short_venue = plan.short_venue if plan.mode == "open" else MEXC_SPOT_VENUE
        side_long_venue = MEXC_SPOT_VENUE if plan.mode == "open" else plan.short_venue
        result = await execute_single_clip(
            symbol=plan.symbol,
            clip_usd=plan.clip_usd,
            quantity=plan.quantity,
            maker_venue=plan.maker_venue,
            taker_venue=plan.taker_venue,
            short_venue=side_short_venue,
            long_venue=side_long_venue,
            maker_adapter=maker_adapter,
            taker_adapter=taker_adapter,
            max_hedge_retries=2,
            state_machine=ExecutionStateMachine(),
            maker_price=fresh_maker_price,
            maker_orderbook=plan.maker_orderbook,
            taker_orderbook=plan.taker_orderbook,
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
            max_maker_reprice_attempts=MAKER_REPRICE_ATTEMPTS,
            maker_reprice_min_change_pct=(
                VARIATIONAL_MAKER_REPRICE_MIN_CHANGE_PCT
                if plan.maker_venue == "variational"
                else 0.0
            ),
            maker_price_refresher=lambda: refresh_spot_perp_maker_price(plan),
            max_execution_price_gap_pct=(
                plan.maker_taker_price_gap_pct
                if allow_large_price_gap
                else float(SPOT_PERP_MAX_PRE_TRADE_PRICE_GAP)
            ),
            min_hedge_notional_usd=float(SPOT_PERP_MIN_HEDGE_NOTIONAL_USD),
        )
        if plan.mode != "open" or not result.get("ok", False):
            return result

        adapters_by_venue = {
            plan.maker_venue: maker_adapter,
            plan.taker_venue: taker_adapter,
        }
        spot_leg = await fetch_required_live_position(
            adapters_by_venue[MEXC_SPOT_VENUE],
            venue=MEXC_SPOT_VENUE,
            symbol=plan.symbol,
            market_type="spot",
            expected_side="LONG",
        )
        perp_leg = await fetch_required_live_position(
            adapters_by_venue[plan.short_venue],
            venue=plan.short_venue,
            symbol=plan.symbol,
            market_type="perp",
            expected_side="SHORT",
        )
        strategy_id = f"spot-perp-{plan.symbol}-{int(time.time() * 1000)}"
        recorded_strategy_id = record_successful_live_legs(
            path=registry_path,
            symbol=plan.symbol,
            execution_result=result,
            legs=[perp_leg, spot_leg],
            strategy_id=strategy_id,
        )
        result["strategy_id"] = strategy_id
        result["recorded_strategy_id"] = recorded_strategy_id
        return result
    finally:
        for adapter in (maker_adapter, taker_adapter):
            close = getattr(adapter, "close", None)
            if close is None:
                continue
            result = close()
            if asyncio.iscoroutine(result):
                await result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one MEXC spot vs perp arbitrage clip.")
    parser.add_argument("--mode", default=None, choices=sorted(SUPPORTED_MODES))
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--short-venue", default=None, choices=sorted(SUPPORTED_SHORT_VENUES))
    parser.add_argument("--total-size", default=None, help="Total base asset size to execute.")
    parser.add_argument("--clip-size", default=None, help="Base asset size per batch.")
    parser.add_argument("--leverage", type=int, default=None)
    parser.add_argument("--variational-broker-host", default=VARIATIONAL_BROKER_HOST)
    parser.add_argument("--variational-broker-port", type=int, default=VARIATIONAL_BROKER_PORT)
    parser.add_argument("--variational-extension-timeout", type=float, default=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
    parser.add_argument("--live", action="store_true")
    return parser.parse_args()


async def run_spot_perp_arbitrage() -> None:
    args = parse_args()
    mode = normalize_mode(args.mode or prompt_text("mode [open/close]"))
    symbol = (args.symbol or prompt_text("ticker")).strip().upper()
    short_venue = normalize_short_venue(args.short_venue or prompt_text("short_venue"))
    total_size = Decimal(args.total_size) if args.total_size is not None else prompt_decimal("total_size_token")
    clip_size = Decimal(args.clip_size) if args.clip_size is not None else prompt_decimal("clip_size_token")
    if total_size <= 0 or clip_size <= 0:
        raise RuntimeError("total_size and clip_size must be positive")
    leverage = args.leverage if args.leverage is not None else (1 if mode == "close" else prompt_int("leverage_x"))
    first_clip_size = min(total_size, clip_size)
    spot_book, perp_book = await fetch_plan_books(
        symbol=symbol,
        short_venue=short_venue,
        clip_usd=PREVIEW_ORDERBOOK_CLIP_USD,
    )
    plan = build_spot_perp_plan(
        symbol=symbol,
        mode=mode,
        short_venue=short_venue,
        quantity=first_clip_size,
        clip_usd=None,
        spot_book=spot_book,
        perp_book=perp_book,
    )
    num_batches = compute_token_batch_count(total_size, clip_size)
    final_clip_size = total_size % clip_size
    if final_clip_size == 0:
        final_clip_size = clip_size if total_size >= clip_size else total_size
    final_clip_usd = estimate_clip_usd_from_books(
        clip_size=final_clip_size,
        spot_book=spot_book,
        perp_book=perp_book,
    )
    assert_min_spot_perp_notional(clip_size=final_clip_size, clip_usd=final_clip_usd)

    print("spot-perp arbitrage preview")
    print(f"mode: {plan.mode}")
    print(f"symbol: {plan.symbol}")
    print(f"spot_venue: {MEXC_SPOT_VENUE}")
    print(f"short_venue: {plan.short_venue}")
    print(f"spot_spread: {fmt_pct(plan.spot_spread_pct)}")
    print(f"perp_spread: {fmt_pct(plan.perp_spread_pct)}")
    print(f"maker: {plan.maker_venue} {plan.maker_side} limit price={plan.maker_price}")
    print(f"taker: {plan.taker_venue} {plan.taker_side} market")
    print(f"pre_trade_price_gap: {float(plan.maker_taker_price_gap_pct):.2%}")
    print(f"total_size_token: {decimal_to_plain(total_size)}")
    print(f"clip_size_token: {decimal_to_plain(clip_size)}")
    print(f"token_batches: {num_batches}")
    print(f"estimated_first_clip_usd: {plan.clip_usd:.2f}")
    if mode != "close" or args.leverage is not None:
        print(f"leverage_x: {leverage}")

    allow_large_price_gap = False
    if plan.maker_taker_price_gap_pct > SPOT_PERP_MAX_PRE_TRADE_PRICE_GAP:
        print(format_spot_perp_price_gap_alert(plan))
        if args.live:
            answer = input("> ").strip().lower()
            if answer in {"y", "yes"}:
                print("execution cancelled")
                return
            allow_large_price_gap = True

    if not args.live:
        print("dry-run only. Add --live to place real orders.")
        return

    print(f"Type exactly {LIVE_CONFIRMATION_PHRASE!r} to continue:")
    if input("> ") != LIVE_CONFIRMATION_PHRASE:
        print("cancelled")
        return

    async def execute_batches(*, broker_url: str | None = None) -> dict[str, object]:
        remaining = total_size
        batch_index = 0
        last_result: dict[str, object] = {"ok": True}
        reference_spot_book = spot_book
        reference_perp_book = perp_book
        while remaining > 0:
            batch_index += 1
            this_clip_size = min(clip_size, remaining)
            estimated_clip_usd = estimate_clip_usd_from_books(
                clip_size=this_clip_size,
                spot_book=reference_spot_book,
                perp_book=reference_perp_book,
            )
            assert_min_spot_perp_notional(clip_size=this_clip_size, clip_usd=estimated_clip_usd)
            fresh_spot_book, fresh_perp_book = await fetch_plan_books(
                symbol=symbol,
                short_venue=short_venue,
                clip_usd=estimated_clip_usd,
            )
            reference_spot_book = fresh_spot_book
            reference_perp_book = fresh_perp_book
            batch_plan = build_spot_perp_plan(
                symbol=symbol,
                mode=mode,
                short_venue=short_venue,
                quantity=this_clip_size,
                clip_usd=None,
                spot_book=fresh_spot_book,
                perp_book=fresh_perp_book,
            )
            assert_min_spot_perp_notional(clip_size=this_clip_size, clip_usd=batch_plan.clip_usd)
            print(f"\nbatch {batch_index}/{num_batches}  clip_size_token={decimal_to_plain(this_clip_size)}")
            print(f"estimated_clip_usd: {batch_plan.clip_usd:.2f}")
            print(f"maker: {batch_plan.maker_venue} {batch_plan.maker_side} limit price={batch_plan.maker_price}")
            print(f"taker: {batch_plan.taker_venue} {batch_plan.taker_side} market")
            print(f"pre_trade_price_gap: {float(batch_plan.maker_taker_price_gap_pct):.2%}")
            last_result = await execute_spot_perp_plan(
                plan=batch_plan,
                leverage=leverage,
                broker_url=broker_url,
                allow_large_price_gap=allow_large_price_gap,
            )
            for line in format_spot_perp_execution_summary(plan=batch_plan, result=last_result):
                print(line)
            if not last_result.get("ok", False):
                raise RuntimeError(f"batch {batch_index} failed — stopping")
            executed_quantity = Decimal(str(last_result.get("executed_quantity") or this_clip_size))
            if executed_quantity <= 0:
                raise RuntimeError(f"batch {batch_index} executed zero quantity — stopping")
            remaining = max(Decimal("0"), remaining - executed_quantity)
        print(f"\nspot-perp complete: {batch_index} batch(es) executed")
        return last_result

    if "variational" in {plan.maker_venue, plan.taker_venue}:
        print(
            "starting embedded Variational broker. "
            "Open Variational page with the Chrome extension connected to "
            f"ws://{args.variational_broker_host}:{args.variational_broker_port}"
        )
        async with VariationalCommandBrokerServer(
            host=args.variational_broker_host,
            port=args.variational_broker_port,
        ) as server:
            print(
                "waiting for Variational extension command client "
                f"timeout={args.variational_extension_timeout:.1f}s"
            )
            await server.wait_for_extension(timeout_seconds=args.variational_extension_timeout)
            await execute_batches(broker_url=server.ws_url)
    else:
        await execute_batches()


def main() -> None:
    asyncio.run(run_spot_perp_arbitrage())


if __name__ == "__main__":
    main()
