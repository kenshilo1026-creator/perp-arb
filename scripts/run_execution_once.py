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
from hydra_basis.config import POSITION_REGISTRY_PATH
from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.executor import execute_single_clip
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter
from hydra_basis.execution_engine.lighter_adapter import LighterExecutionAdapter
from hydra_basis.execution_engine.lighter_live import (
    build_lighter_client_factory_from_env,
    fetch_lighter_market_config,
    fetch_lighter_orderbook_live,
)
from hydra_basis.execution_engine.runtime import prepare_execution_preview_for_size
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter
from hydra_basis.formatting import fmt_pct
from hydra_basis.risk_management.recording import record_successful_execution

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
VARIATIONAL_EXTENSION_TIMEOUT_SECONDS = 30.0
MAKER_FILL_TIMEOUT_SECONDS = 60.0


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
            return VariationalBrowserExecutionAdapter(broker_url=broker_url)
        return VariationalBrowserExecutionAdapter()
    if v == "aster":
        return AsterExecutionAdapter(leverage=leverage)
    if v == "hyperliquid":
        return HyperliquidExecutionAdapter(leverage=leverage)
    if v == "mexc":
        return MexcExecutionAdapter(leverage=leverage)
    raise RuntimeError(f"no execution adapter for venue: {venue}")


def validate_maker_fill_supported(maker_venue: str) -> None:
    if maker_venue == "variational":
        raise RuntimeError(
            "variational maker limit order is not safe yet: the browser extension "
            "can submit the order but cannot confirm FILLED status. Use a non-Variational "
            "maker venue, or wait until Variational websocket fill watcher is implemented."
        )


async def run_execution_once() -> None:
    symbol = prompt_text("ticker").upper()
    total_size = prompt_decimal("total_size_token")
    clip_size = prompt_decimal("clip_size_token")
    leverage = prompt_int("leverage_x")

    signal, preview, short_book, long_book = await prepare_execution_preview_for_size(
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
        maker_book = short_book if preview.maker_venue == signal.short_venue else long_book
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
            maker_orderbook=maker_book,
            require_maker_fill_confirmation=True,
            maker_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
        )

    if "variational" in {preview.maker_venue, preview.taker_venue}:
        print(
            "starting embedded Variational broker. "
            "Open Variational page with the Chrome extension connected to "
            f"ws://{VARIATIONAL_BROKER_HOST}:{VARIATIONAL_BROKER_PORT}"
        )
        async with VariationalCommandBrokerServer(
            host=VARIATIONAL_BROKER_HOST,
            port=VARIATIONAL_BROKER_PORT,
        ) as server:
            print(
                "waiting for Variational extension command client "
                f"timeout={VARIATIONAL_EXTENSION_TIMEOUT_SECONDS:.1f}s"
            )
            await server.wait_for_extension(timeout_seconds=VARIATIONAL_EXTENSION_TIMEOUT_SECONDS)
            result = await execute_with_adapters(broker_url=server.ws_url)
    else:
        result = await execute_with_adapters()
    print("execution result")
    print(result)
    strategy_id = record_successful_execution(
        path=POSITION_REGISTRY_PATH,
        symbol=signal.symbol,
        quantity=str(quantity),
        short_venue=signal.short_venue,
        long_venue=signal.long_venue,
        execution_result=result,
    )
    print(f"risk registry recorded: {strategy_id} -> {POSITION_REGISTRY_PATH}")


def main() -> None:
    asyncio.run(run_execution_once())


if __name__ == "__main__":
    main()
