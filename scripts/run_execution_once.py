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
from hydra_basis.config import POSITION_REGISTRY_PATH
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
from hydra_basis.risk_management.recording import record_successful_execution
from hydra_basis.risk_management.models import PositionLeg, close_side_for_position
from hydra_basis.risk_management.registry import PositionRegistry
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
    strategy_id: str
    symbol: str
    quantity: Decimal
    maker_venue: str
    taker_venue: str
    maker_price: str
    clip_usd: float
    side_by_venue: dict[str, str]
    spread_by_venue: dict[str, float]
    legs_by_venue: dict[str, PositionLeg]


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


def validate_maker_fill_supported(_maker_venue: str) -> None:
    return None


def normalize_execution_mode(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"open", "開倉", "开仓"}:
        return "open"
    if normalized in {"close", "平倉", "平仓"}:
        return "close"
    raise RuntimeError("mode must be open/close or 開倉/平倉")


def format_open_positions_zh(registry: PositionRegistry) -> str:
    strategy_ids = registry.open_strategy_ids()
    if not strategy_ids:
        return "目前沒有未平倉位。"

    lines = ["未平倉位："]
    for index, strategy_id in enumerate(strategy_ids, start=1):
        open_legs = [leg for leg in registry.legs_for_strategy(strategy_id) if leg.status == "open"]
        symbol = open_legs[0].symbol if open_legs else "-"
        leg_summary = " / ".join(
            f"{leg.venue} {leg.side} {leg.quantity} {leg.market_type}"
            for leg in open_legs
        )
        lines.append(f"{index}. strategy_id={strategy_id} | {symbol} | {leg_summary}")
    return "\n".join(lines)


def _open_legs_for_close(registry: PositionRegistry, strategy_id: str) -> list[PositionLeg]:
    legs = [leg for leg in registry.legs_for_strategy(strategy_id) if leg.status == "open"]
    if len(legs) != 2:
        raise RuntimeError(f"strategy {strategy_id} must have exactly 2 open legs to close")
    symbols = {leg.symbol.strip().upper() for leg in legs}
    if len(symbols) != 1:
        raise RuntimeError(f"strategy {strategy_id} has mismatched symbols")
    return legs


def _position_leg_from_live_payload(template: PositionLeg, payload: dict) -> PositionLeg:
    quantity = Decimal(str(payload.get("quantity", "0")))
    if quantity <= 0:
        raise RuntimeError(f"live position is not open on {template.venue}:{template.symbol}")
    side = str(payload.get("side", "")).strip().upper()
    if side not in {"LONG", "SHORT"}:
        raise RuntimeError(f"live position has unsupported side on {template.venue}:{template.symbol}: {side}")
    return PositionLeg(
        strategy_id=template.strategy_id,
        leg_id=template.leg_id,
        venue=template.venue,
        symbol=str(payload.get("symbol") or template.symbol).strip().upper(),
        market_type=str(payload.get("market_type") or template.market_type),  # type: ignore[arg-type]
        side=side,  # type: ignore[arg-type]
        quantity=format(quantity.normalize(), "f"),
        status="open",
        margin_topups=template.margin_topups,
        last_margin_topup_ts_ms=template.last_margin_topup_ts_ms,
    )


async def load_live_close_legs(
    *,
    registry: PositionRegistry,
    strategy_id: str,
    adapters: dict[str, object],
) -> list[PositionLeg]:
    registry_legs = _open_legs_for_close(registry, strategy_id)
    live_legs: list[PositionLeg] = []
    for leg in registry_legs:
        adapter = adapters.get(leg.venue)
        if adapter is None:
            raise RuntimeError(f"missing live adapter for {leg.venue}")
        getter = getattr(adapter, "get_open_position", None)
        if not callable(getter):
            raise RuntimeError(f"live position query unavailable for {leg.venue}")
        payload = await getter(symbol=leg.symbol, market_type=leg.market_type)
        if not payload:
            raise RuntimeError(f"no live open position for {leg.venue}:{leg.symbol}")
        live_legs.append(_position_leg_from_live_payload(leg, payload))
    return live_legs


def registry_from_live_close_legs(live_legs: list[PositionLeg]) -> PositionRegistry:
    return PositionRegistry(legs=live_legs)


def format_live_positions_zh(live_legs_by_strategy: dict[str, list[PositionLeg]], errors: dict[str, str] | None = None) -> str:
    lines = ["未平倉位（以交易所實時資料為準）："]
    if not live_legs_by_strategy and not errors:
        return "目前沒有可確認的實時未平倉位。"
    for index, (strategy_id, legs) in enumerate(sorted(live_legs_by_strategy.items()), start=1):
        symbol = legs[0].symbol if legs else "-"
        leg_summary = " / ".join(
            f"{leg.venue} {leg.side} {leg.quantity} {leg.market_type}"
            for leg in legs
        )
        lines.append(f"{index}. strategy_id={strategy_id} | {symbol} | {leg_summary}")
    for strategy_id, error in sorted((errors or {}).items()):
        lines.append(f"- strategy_id={strategy_id} | 無法確認實時倉位：{error}")
    return "\n".join(lines)


async def close_adapter_if_supported(adapter: object) -> None:
    close = getattr(adapter, "close", None)
    if close is None:
        return
    result = close()
    if asyncio.iscoroutine(result):
        await result


async def load_live_close_candidates(
    *,
    registry: PositionRegistry,
    leverage: int = 1,
) -> tuple[dict[str, list[PositionLeg]], dict[str, str]]:
    live: dict[str, list[PositionLeg]] = {}
    errors: dict[str, str] = {}
    for strategy_id in registry.open_strategy_ids():
        adapters: dict[str, object] = {}
        try:
            registry_legs = _open_legs_for_close(registry, strategy_id)
            adapters = {
                leg.venue: build_adapter_for_venue(leg.venue, leverage=leverage)
                for leg in registry_legs
            }
            live[strategy_id] = await load_live_close_legs(
                registry=registry,
                strategy_id=strategy_id,
                adapters=adapters,
            )
        except Exception as exc:
            errors[strategy_id] = str(exc)
        finally:
            for adapter in adapters.values():
                await close_adapter_if_supported(adapter)
    return live, errors


def build_close_position_plan(
    *,
    registry: PositionRegistry,
    strategy_id: str,
    clip_size: Decimal,
    priorities: dict[str, int],
    orderbooks: dict[str, dict[str, float | int]],
) -> ClosePositionPlan:
    if clip_size <= 0:
        raise RuntimeError("clip_size must be positive")

    legs = _open_legs_for_close(registry, strategy_id)
    venues = [leg.venue for leg in legs]
    missing_books = [venue for venue in venues if venue not in orderbooks]
    if missing_books:
        raise RuntimeError(f"missing orderbook for close venues: {missing_books}")

    for leg in legs:
        if clip_size > Decimal(str(leg.quantity)):
            raise RuntimeError(f"clip_size exceeds open quantity on {leg.venue}")

    spreads = {venue: compute_spread_pct(orderbooks[venue]) for venue in venues}
    maker_venue, taker_venue = resolve_execution_legs(
        short_venue=venues[0],
        long_venue=venues[1],
        priorities=priorities,
        spreads=spreads,
    )
    side_by_venue = {leg.venue: close_side_for_position(leg.side) for leg in legs}
    maker_price = passive_limit_price_from_orderbook(orderbooks[maker_venue], side_by_venue[maker_venue])
    taker_book = orderbooks[taker_venue]
    taker_mid = (float(taker_book["bid"]) + float(taker_book["ask"])) / 2
    return ClosePositionPlan(
        strategy_id=strategy_id,
        symbol=legs[0].symbol.strip().upper(),
        quantity=clip_size,
        maker_venue=maker_venue,
        taker_venue=taker_venue,
        maker_price=maker_price,
        clip_usd=float(clip_size) * taker_mid,
        side_by_venue=side_by_venue,
        spread_by_venue=spreads,
        legs_by_venue={leg.venue: leg for leg in legs},
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


def _apply_close_fill_to_registry(registry: PositionRegistry, plan: ClosePositionPlan) -> None:
    for leg in plan.legs_by_venue.values():
        remaining = Decimal(str(leg.quantity)) - plan.quantity
        if remaining <= 0:
            registry.mark_status(leg.leg_id, "closed")
            leg.quantity = "0"
        else:
            leg.quantity = format(remaining.normalize(), "f")


async def execute_close_position_plan(
    *,
    plan: ClosePositionPlan,
    registry: PositionRegistry,
    adapters: dict[str, object],
) -> dict[str, object]:
    maker_adapter = adapters[plan.maker_venue]
    taker_adapter = adapters[plan.taker_venue]
    result = await execute_single_clip_with_sides(
        symbol=plan.symbol,
        clip_usd=plan.clip_usd,
        quantity=plan.quantity,
        maker_venue=plan.maker_venue,
        taker_venue=plan.taker_venue,
        maker_side=plan.side_by_venue[plan.maker_venue],
        taker_side=plan.side_by_venue[plan.taker_venue],
        maker_adapter=maker_adapter,
        taker_adapter=taker_adapter,
        max_hedge_retries=2,
        state_machine=ExecutionStateMachine(),
        maker_price=plan.maker_price,
        require_maker_fill_confirmation=True,
        maker_fill_timeout_seconds=MAKER_FILL_TIMEOUT_SECONDS,
    )
    if result.get("ok", False):
        _apply_close_fill_to_registry(registry, plan)
    return result


async def run_open_execution_once() -> None:
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
        warm_up = getattr(taker_adapter, "warm_up", None)
        if callable(warm_up):
            await warm_up()
        maker_book = short_book if preview.maker_venue == signal.short_venue else long_book
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
            print("extension connected — waiting for page reload to settle...")
            await asyncio.sleep(4.0)
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


async def run_close_execution_once() -> None:
    registry = PositionRegistry.load(POSITION_REGISTRY_PATH)
    if not registry.open_strategy_ids():
        print("目前沒有本地候選未平倉位。")
        return

    live_candidates, live_errors = await load_live_close_candidates(registry=registry)
    print(format_live_positions_zh(live_candidates, live_errors))
    if not live_candidates:
        print("沒有可確認的實時倉位，拒絕平倉。")
        return

    strategy_id = prompt_text("要平倉的 strategy_id")
    if strategy_id not in live_candidates:
        raise RuntimeError(f"strategy {strategy_id} has no confirmed live position; refusing to close")
    clip_size = prompt_decimal("clip_size_token")
    leverage = prompt_int("leverage_x")
    priorities = load_execution_priorities(EXECUTION_VENUES_PATH)
    live_registry = registry_from_live_close_legs(live_candidates[strategy_id])
    legs = _open_legs_for_close(live_registry, strategy_id)
    symbol = legs[0].symbol.strip().upper()
    venues = [leg.venue for leg in legs]

    initial_books = await fetch_close_orderbooks(symbol=symbol, venues=venues, clip_usd=1000.0)
    initial_plan = build_close_position_plan(
        registry=live_registry,
        strategy_id=strategy_id,
        clip_size=clip_size,
        priorities=priorities,
        orderbooks=initial_books,
    )
    orderbooks = await fetch_close_orderbooks(symbol=symbol, venues=venues, clip_usd=initial_plan.clip_usd)
    plan = build_close_position_plan(
        registry=live_registry,
        strategy_id=strategy_id,
        clip_size=clip_size,
        priorities=priorities,
        orderbooks=orderbooks,
    )

    print("平倉預覽")
    print(f"strategy_id: {plan.strategy_id}")
    print(f"ticker: {plan.symbol}")
    for leg in legs:
        print(f"倉位: {leg.venue} {leg.side} {leg.quantity} {leg.market_type}")
    print(f"限價方: {plan.maker_venue} {plan.side_by_venue[plan.maker_venue]} price={plan.maker_price}")
    print(f"市價方: {plan.taker_venue} {plan.side_by_venue[plan.taker_venue]}")
    print(f"clip_size_token: {plan.quantity}")
    print(f"estimated_clip_usd: {plan.clip_usd:.2f}")
    for venue, spread in plan.spread_by_venue.items():
        print(f"{venue}_spread: {fmt_pct(spread)}")
    print(f"leverage_x: {leverage}")

    answer = input("place real close order for one clip? [y/N]: ").strip().lower()
    if answer not in {"y", "yes"}:
        print("close cancelled")
        return

    async def execute_with_adapters(*, broker_url: str | None = None) -> dict[str, object]:
        adapters = {
            venue: build_adapter_for_venue(venue, leverage=leverage, broker_url=broker_url)
            for venue in venues
        }
        try:
            for adapter in adapters.values():
                warm_up = getattr(adapter, "warm_up", None)
                if callable(warm_up):
                    await warm_up()
            return await execute_close_position_plan(
                plan=plan,
                registry=live_registry,
                adapters=adapters,
            )
        finally:
            for adapter in adapters.values():
                await close_adapter_if_supported(adapter)

    if "variational" in set(venues):
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
            print("extension connected — waiting for page reload to settle...")
            await asyncio.sleep(4.0)
            result = await execute_with_adapters(broker_url=server.ws_url)
    else:
        result = await execute_with_adapters()

    if result.get("ok", False):
        _apply_close_fill_to_registry(registry, plan)
        registry.save(POSITION_REGISTRY_PATH)
    print("close execution result")
    print(result)
    print(f"risk registry updated: {POSITION_REGISTRY_PATH}")


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
