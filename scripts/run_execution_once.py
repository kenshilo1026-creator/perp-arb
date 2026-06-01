from __future__ import annotations

import asyncio
import math
from decimal import Decimal

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.executor import execute_single_clip
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
from hydra_basis.execution_engine.lighter_adapter import LighterExecutionAdapter, compute_base_quantity_from_clip_usd
from hydra_basis.execution_engine.lighter_live import (
    build_lighter_client_factory_from_env,
    fetch_lighter_market_config,
    fetch_lighter_orderbook_live,
)
from hydra_basis.execution_engine.runtime import prepare_execution_preview
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter
from hydra_basis.formatting import fmt_pct

load_environment()


def compute_batch_count(total_usd: float, clip_usd: float) -> int:
    if total_usd <= 0 or clip_usd <= 0:
        raise RuntimeError("total_usd and clip_usd must be positive")
    return math.ceil(total_usd / clip_usd)


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


def build_adapter_for_venue(venue: str):
    v = venue.lower()
    if v == "lighter":
        return LighterExecutionAdapter(
            signer_client_factory=build_lighter_client_factory_from_env(),
            market_config_loader=lambda symbol: fetch_lighter_market_config(symbol),
            orderbook_loader=lambda symbol: fetch_lighter_orderbook_live(symbol),
        )
    if v == "variational":
        return VariationalBrowserExecutionAdapter()
    if v == "aster":
        return AsterExecutionAdapter()
    if v == "hyperliquid":
        return HyperliquidExecutionAdapter()
    raise RuntimeError(f"no execution adapter for venue: {venue}")


async def run_execution_once() -> None:
    symbol = prompt_text("ticker").upper()
    total_usd = prompt_float("total_usd")
    clip_usd = prompt_float("clip_usd")

    signal, preview, short_book, long_book = await prepare_execution_preview(
        symbol=symbol,
        total_usd=total_usd,
        clip_usd=clip_usd,
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
    print(f"clip_usd: {clip_usd:.2f}")

    if preview.requires_confirm:
        answer = input("spread > 0.1%, continue? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            print("execution cancelled")
            return

    answer = input("place real order for one clip? [y/N]: ").strip().lower()
    if answer not in {"y", "yes"}:
        print("execution cancelled")
        return

    taker_book = short_book if preview.taker_venue == signal.short_venue else long_book
    quantity = compute_base_quantity_from_clip_usd(clip_usd=clip_usd, orderbook=taker_book)

    maker_adapter = build_adapter_for_venue(preview.maker_venue)
    taker_adapter = build_adapter_for_venue(preview.taker_venue)

    result = await execute_single_clip(
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
    )
    print("execution result")
    print(result)


def main() -> None:
    asyncio.run(run_execution_once())


if __name__ == "__main__":
    main()
