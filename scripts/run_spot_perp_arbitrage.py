from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from decimal import Decimal

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.execution_engine.executor import execute_single_clip
from hydra_basis.execution_engine.market_data import (
    fetch_mexc_spot_orderbook,
    fetch_orderbook_snapshot,
)
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.risk import compute_spread_pct
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
from hydra_basis.formatting import fmt_pct
from scripts.run_execution_once import build_adapter_for_venue


load_environment()

MEXC_SPOT_VENUE = "mexc_spot"
SUPPORTED_MODES = {"open", "close"}
SUPPORTED_SHORT_VENUES = {"aster", "hyperliquid", "lighter", "variational"}
LIVE_CONFIRMATION_PHRASE = "PLACE LIVE SPOT PERP ORDER"
VARIATIONAL_BROKER_HOST = "127.0.0.1"
VARIATIONAL_BROKER_PORT = 8768
VARIATIONAL_EXTENSION_TIMEOUT_SECONDS = 30.0


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
        raise RuntimeError("either positive --quantity or --clip-usd is required")
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
    )


def build_spot_perp_adapter(venue: str, *, leverage: int, broker_url: str | None = None):
    if venue == MEXC_SPOT_VENUE:
        return MexcSpotExecutionAdapter()
    return build_adapter_for_venue(venue, leverage=leverage, broker_url=broker_url)


def assert_maker_limit_supported(adapter, venue: str) -> None:
    if not hasattr(adapter, "place_limit_order"):
        raise RuntimeError(f"maker venue {venue} does not support limit orders yet")


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
) -> dict[str, object]:
    maker_adapter = build_spot_perp_adapter(plan.maker_venue, leverage=leverage, broker_url=broker_url)
    taker_adapter = build_spot_perp_adapter(plan.taker_venue, leverage=leverage, broker_url=broker_url)
    assert_maker_limit_supported(maker_adapter, plan.maker_venue)
    try:
        # For close mode, treating spot as the "short venue" makes the existing
        # side mapper emit spot SELL and perp BUY.
        side_short_venue = plan.short_venue if plan.mode == "open" else MEXC_SPOT_VENUE
        side_long_venue = MEXC_SPOT_VENUE if plan.mode == "open" else plan.short_venue
        return await execute_single_clip(
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
            maker_price=plan.maker_price,
        )
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
    parser.add_argument("--mode", required=True, choices=sorted(SUPPORTED_MODES))
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--short-venue", required=True, choices=sorted(SUPPORTED_SHORT_VENUES))
    parser.add_argument("--quantity", default=None, help="Base asset quantity. If omitted, use --clip-usd / mid.")
    parser.add_argument("--clip-usd", type=float, default=None)
    parser.add_argument("--leverage", type=int, default=1)
    parser.add_argument("--variational-broker-host", default=VARIATIONAL_BROKER_HOST)
    parser.add_argument("--variational-broker-port", type=int, default=VARIATIONAL_BROKER_PORT)
    parser.add_argument("--variational-extension-timeout", type=float, default=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
    parser.add_argument("--live", action="store_true")
    return parser.parse_args()


async def run_spot_perp_arbitrage() -> None:
    args = parse_args()
    symbol = args.symbol.strip().upper()
    short_venue = normalize_short_venue(args.short_venue)
    quantity = Decimal(args.quantity) if args.quantity is not None else None
    preview_clip_usd = args.clip_usd if args.clip_usd is not None else 1_000.0
    spot_book, perp_book = await fetch_plan_books(
        symbol=symbol,
        short_venue=short_venue,
        clip_usd=preview_clip_usd,
    )
    plan = build_spot_perp_plan(
        symbol=symbol,
        mode=args.mode,
        short_venue=short_venue,
        quantity=quantity,
        clip_usd=args.clip_usd,
        spot_book=spot_book,
        perp_book=perp_book,
    )

    print("spot-perp arbitrage preview")
    print(f"mode: {plan.mode}")
    print(f"symbol: {plan.symbol}")
    print(f"spot_venue: {MEXC_SPOT_VENUE}")
    print(f"short_venue: {plan.short_venue}")
    print(f"spot_spread: {fmt_pct(plan.spot_spread_pct)}")
    print(f"perp_spread: {fmt_pct(plan.perp_spread_pct)}")
    print(f"maker: {plan.maker_venue} {plan.maker_side} limit price={plan.maker_price}")
    print(f"taker: {plan.taker_venue} {plan.taker_side} market")
    print(f"quantity: {decimal_to_plain(plan.quantity)}")
    print(f"clip_usd: {plan.clip_usd:.2f}")
    print(f"leverage_x: {args.leverage}")

    if not args.live:
        print("dry-run only. Add --live to place real orders.")
        return

    print(f"Type exactly {LIVE_CONFIRMATION_PHRASE!r} to continue:")
    if input("> ") != LIVE_CONFIRMATION_PHRASE:
        print("cancelled")
        return

    async def execute_with_optional_broker(*, broker_url: str | None = None) -> dict[str, object]:
        return await execute_spot_perp_plan(
            plan=plan,
            leverage=args.leverage,
            broker_url=broker_url,
        )

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
            result = await execute_with_optional_broker(broker_url=server.ws_url)
    else:
        result = await execute_with_optional_broker()

    print("spot-perp execution result")
    print(result)


def main() -> None:
    asyncio.run(run_spot_perp_arbitrage())


if __name__ == "__main__":
    main()
