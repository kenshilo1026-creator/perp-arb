from __future__ import annotations

import argparse
import asyncio
import math
import time
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.execution_engine.execution_trace import record_execution
from hydra_basis.env import load_environment
from hydra_basis.config import POSITION_REGISTRY_PATH
from hydra_basis.execution_engine.executor import (
    execute_single_clip,
    price_gap_pct,
    taker_price_from_orderbook,
)
from hydra_basis.execution_engine.market_data import (
    fetch_binance_spot_orderbook,
    fetch_orderbook_snapshot,
)
from hydra_basis.execution_engine.maker_price_guard import MakerPriceGuard, create_maker_guard, adverse_price_gap, MakerGapTimeout, wait_for_quote_update, MAKER_TAKER_MAX_GAP
from hydra_basis.execution_engine.binance_spot_adapter import BinanceSpotExecutionAdapter
from hydra_basis.execution_engine.order_stream import prepare_private_order_streams
from hydra_basis.execution_engine.order_service import Deps, progress_printer, run_batched_execution
from hydra_basis.execution_engine.priority import default_maker_for_pair
from hydra_basis.execution_engine.risk import compute_spread_pct
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.depth_sizing import combined_size_rules, depth_clip
from hydra_basis.execution_engine.mexc_depth_stream import DepthQuoteUnavailable
from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
from hydra_basis.formatting import fmt_pct
from hydra_basis.risk_management.recording import record_successful_live_legs
from scripts.place_order import (
    MAKER_FILL_TIMEOUT_SECONDS,
    VARIATIONAL_MAKER_REPRICE_MIN_CHANGE_PCT,
    maker_reprice_attempts_for_venue,
    _executed_quantity_from_result,
    _registry_fallback_leg,
    build_adapter_for_venue,
)


load_environment()

BINANCE_SPOT_VENUE = "binance_spot"
GAP_WAIT_TIMEOUT_SECONDS = 60.0
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
        return {BINANCE_SPOT_VENUE: "BUY", "perp": "SELL"}
    return {BINANCE_SPOT_VENUE: "SELL", "perp": "BUY"}


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
    maker_venue_override: str | None = None,
) -> SpotPerpPlan:
    normalized_mode = normalize_mode(mode)
    normalized_short_venue = normalize_short_venue(short_venue)
    spot_quote, perp_quote = spot_book.get("fast_quote") or spot_book, perp_book.get("fast_quote") or perp_book
    spot_spread_pct = compute_spread_pct(spot_quote)
    perp_spread_pct = compute_spread_pct(perp_quote)
    sides = spot_perp_sides(mode=normalized_mode)
    side_by_venue = {
        BINANCE_SPOT_VENUE: sides[BINANCE_SPOT_VENUE],
        normalized_short_venue: sides["perp"],
    }
    preferred_maker = default_maker_for_pair(normalized_short_venue, BINANCE_SPOT_VENUE)
    if normalized_short_venue == "aster":
        quotes = {BINANCE_SPOT_VENUE: spot_quote, "aster": perp_quote}
        def role_score(maker):
            taker = BINANCE_SPOT_VENUE if maker == "aster" else "aster"
            maker_price = Decimal(maker_limit_price(quotes[maker], side_by_venue[maker]))
            taker_price = Decimal(taker_price_from_orderbook(quotes[taker], side_by_venue[taker]))
            return adverse_price_gap(maker_price, taker_price, side_by_venue[maker]), compute_spread_pct(quotes[taker])
        preferred_maker = min(("aster", BINANCE_SPOT_VENUE), key=role_score)
    if maker_venue_override is not None:
        if normalized_short_venue != "aster" or maker_venue_override not in {"aster", BINANCE_SPOT_VENUE}:
            raise ValueError("maker role override is only supported for Aster/Binance")
        preferred_maker = maker_venue_override
    if preferred_maker is not None:
        maker_venue = preferred_maker
        taker_venue = BINANCE_SPOT_VENUE if maker_venue == normalized_short_venue else normalized_short_venue
    else:
        taker_venue = BINANCE_SPOT_VENUE if spot_spread_pct <= perp_spread_pct else normalized_short_venue
        maker_venue = normalized_short_venue if taker_venue == BINANCE_SPOT_VENUE else BINANCE_SPOT_VENUE
    taker_book = spot_book if taker_venue == BINANCE_SPOT_VENUE else perp_book
    maker_book = spot_book if maker_venue == BINANCE_SPOT_VENUE else perp_book
    maker_quote, taker_quote = maker_book.get("fast_quote") or maker_book, taker_book.get("fast_quote") or taker_book
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
    maker_execution_price = Decimal(maker_limit_price(maker_quote, maker_side))
    taker_execution_price = Decimal(taker_price_from_orderbook(taker_quote, side_by_venue[taker_venue]))
    return SpotPerpPlan(
        symbol=symbol.strip().upper(),
        mode=normalized_mode,
        short_venue=normalized_short_venue,
        maker_venue=maker_venue,
        taker_venue=taker_venue,
        maker_side=maker_side,
        taker_side=side_by_venue[taker_venue],
        maker_price=maker_limit_price(maker_quote, maker_side),
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


def build_spot_perp_adapter(venue: str, *, leverage: int, mode: str, broker_url: str | None = None):
    if venue == BINANCE_SPOT_VENUE:
        return BinanceSpotExecutionAdapter()
    skip_margin_setup = normalize_mode(mode) == "close"
    return build_adapter_for_venue(
        venue,
        leverage=leverage,
        broker_url=broker_url,
        skip_margin_setup=skip_margin_setup,
    )


async def refresh_spot_perp_maker_price(plan: SpotPerpPlan) -> str:
    spot_book, perp_book = await fetch_plan_books(
        symbol=plan.symbol,
        short_venue=plan.short_venue,
        clip_usd=plan.clip_usd,
    )
    maker_book = spot_book if plan.maker_venue == BINANCE_SPOT_VENUE else perp_book
    price = maker_limit_price(maker_book, plan.maker_side)
    print(
        "refreshed maker book: "
        f"{plan.maker_venue} {plan.maker_side} "
        f"bid={maker_book['bid']} ask={maker_book['ask']} price={price}",
        flush=True,
    )
    return price


def display_venue_name(venue: str) -> str:
    if venue == BINANCE_SPOT_VENUE:
        return "binance"
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
    if key == "maker_avg_price" and summary.get("maker_avg_price_confirmed") is False:
        return "N/A"
    return summary.get(key) or "N/A"


def format_spot_perp_execution_summary(
    *,
    plan: SpotPerpPlan,
    result: dict[str, object],
) -> list[str]:
    spot_price = price_for_venue_from_execution_summary(
        plan=plan,
        result=result,
        venue=BINANCE_SPOT_VENUE,
    )
    short_price = price_for_venue_from_execution_summary(
        plan=plan,
        result=result,
        venue=plan.short_venue,
    )
    lines = [
        f"下單成功 {display_venue_name(BINANCE_SPOT_VENUE)}現貨成交價: {spot_price}",
        f"{display_venue_name(plan.short_venue)}{'做空' if plan.mode == 'open' else '買入平空'}成交價: {short_price}",
    ]
    if result.get("spot_base_asset_fee"):
        lines.append(f"Binance 現貨扣幣手續費: {result['spot_base_asset_fee']} {plan.symbol}；淨持倉已扣除此數量")
    summary = result.get("execution_price_summary") or {}
    maker, taker = summary.get("maker_avg_price"), summary.get("taker_avg_price")
    if (summary.get("maker_avg_price_confirmed") is False
            or maker in (None, "", "N/A") or taker in (None, "", "N/A")):
        lines.append("成交均價未齊：無法驗證最終價差；pre_trade_price_gap 不是實際成交價差")
    else:
        gap = adverse_price_gap(Decimal(str(maker)), Decimal(str(taker)), plan.maker_side)
        lines.append(f"實際不利成交價差（以 Maker 均價為分母）: {gap:.4%}")
        if gap > MAKER_TAKER_MAX_GAP:
            lines.append(f"警告：實際成交價差超過 {MAKER_TAKER_MAX_GAP:.2%}；Maker 掛單保護不限制市價對沖的滑點")
    timing = result.get("execution_timing")
    if isinstance(timing, dict):
        def duration(key):
            value = timing.get(key)
            return "未知" if value is None else f"{value:.2f} ms"
        lines.append(
            f"對沖延遲 [{timing['maker_confirmation_source']}]："
            f"本機觀測成交 → 呼叫對沖 {duration('maker_observed_to_hedge_call_ms')}；"
            f"對沖呼叫至回傳 {duration('hedge_call_duration_ms')}"
            "（非兩所實際成交時間差）"
        )
        if timing.get("maker_resolved_to_hedge_call_ms") is not None:
            lines.append(f"成交結果解析完成 → 呼叫對沖: {duration('maker_resolved_to_hedge_call_ms')}（不含此前查單／撤單等待）")
        if timing.get("maker_observed_to_submit_ms") is not None:
            lines.append(f"本機收到成交 → 開始送出對沖請求: {timing['maker_observed_to_submit_ms']:.2f} ms")
        if timing.get("hedge_confirmation_source"):
            lines.append(f"對沖成交確認來源: {timing['hedge_confirmation_source']}")
    trace = result.get("execution_trace")
    if isinstance(trace, dict):
        for event in trace["events"]:
            stage = event["stage"]
            labels = {"maker.cancel_with_retries.end": "Maker 撤單及重試",
                      "order.terminal_reconciliation.end": "終態查證",
                      "aster.order_query.end": "Aster 查單",
                      "binance_spot.order_query.end": "Binance 查單",
                      "aster.http.POST.end": "Aster HTTP POST 至回傳（含連線等待）",
                      "binance_spot.request.end": "Binance 請求準備至回傳（含簽名／連線等待）",
                      "pair.position_confirmation.end": "雙腿持倉確認"}
            if stage in labels:
                lines.append(f"時間記錄 {labels[stage]}: {event['duration_ms']:.2f} ms [{event['status']}]")
    return lines


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
    fallback_quantity: str | None = None,
) -> dict:
    getter = getattr(adapter, "get_open_position", None)
    if not callable(getter):
        if venue.strip().lower() == "variational" and fallback_quantity:
            return _registry_fallback_leg(
                venue=venue,
                symbol=symbol,
                market_type=market_type,
                expected_side=expected_side,
                quantity=fallback_quantity,
            )
        raise RuntimeError(f"{venue} does not support live position query")
    position = await getter(symbol=symbol, market_type=market_type)
    if not position:
        if venue.strip().lower() == "variational" and fallback_quantity:
            return _registry_fallback_leg(
                venue=venue,
                symbol=symbol,
                market_type=market_type,
                expected_side=expected_side,
                quantity=fallback_quantity,
            )
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
            fetch_binance_spot_orderbook(session, symbol),
            fetch_orderbook_snapshot(
                session,
                venue=short_venue,
                symbol=symbol,
                clip_usd=clip_usd,
            ),
        )
    return spot_book, perp_book


@record_execution
async def execute_spot_perp_plan(
    *,
    plan: SpotPerpPlan,
    leverage: int,
    broker_url: str | None = None,
    registry_path: Path = POSITION_REGISTRY_PATH,
    allow_large_price_gap: bool = False,
    adapters: dict | None = None,
    sizing_rules=None,
    remaining_quantity: Decimal | None = None,
) -> dict[str, object]:
    maker_adapter = adapters[plan.maker_venue] if adapters is not None else build_spot_perp_adapter(
        plan.maker_venue,
        leverage=leverage,
        mode=plan.mode,
        broker_url=broker_url,
    )
    taker_adapter = adapters[plan.taker_venue] if adapters is not None else build_spot_perp_adapter(
        plan.taker_venue,
        leverage=leverage,
        mode=plan.mode,
        broker_url=broker_url,
    )
    assert_maker_limit_supported(maker_adapter, plan.maker_venue)
    try:
        # Do setup while both legs are still unfilled. In particular, an Aster
        # taker must not fetch metadata/set margin/leverage after a spot fill.
        for adapter in (maker_adapter, taker_adapter):
            prepare = getattr(adapter, "prepare_execution", None)
            warm_up = getattr(adapter, "warm_up", None)
            if callable(prepare):
                await prepare(plan.symbol)
            elif callable(warm_up):
                await warm_up()
        spot_adapter = maker_adapter if plan.maker_venue == BINANCE_SPOT_VENUE else taker_adapter
        validate_spot = getattr(spot_adapter, "validate_order_inputs", None)
        min_hedge_notional = SPOT_PERP_MIN_HEDGE_NOTIONAL_USD
        if callable(validate_spot):
            spot_is_maker = plan.maker_venue == BINANCE_SPOT_VENUE
            # Validate Binance's own filters before the other leg can fill.
            # Never silently round only one leg to a different quantity.
            spot_price = plan.maker_price if spot_is_maker else str(plan.taker_execution_price)
            validate_spot(symbol=plan.symbol, amount=str(plan.quantity),
                          price=spot_price, market=not spot_is_maker)
            if not spot_is_maker:
                min_hedge_notional = max(min_hedge_notional,
                    spot_adapter.minimum_order_notional(plan.symbol, market=True))
        await prepare_private_order_streams(((plan.maker_venue, maker_adapter), (plan.taker_venue, taker_adapter)))
        top_level_only = {plan.maker_venue, plan.taker_venue} == {"aster", BINANCE_SPOT_VENUE}
        if top_level_only and sizing_rules is None:
            perp_adapter = taker_adapter if plan.taker_venue == "aster" else maker_adapter
            sizing_rules = await load_spot_perp_size_rules(spot_adapter, perp_adapter, plan.symbol)
        fresh_maker_price = await refresh_spot_perp_maker_price(plan)
        async def guard_books() -> tuple[dict, dict]:
            spot, perp = await fetch_plan_books(
                symbol=plan.symbol, short_venue=plan.short_venue, clip_usd=plan.clip_usd,
            )
            return (spot, perp) if plan.maker_venue == BINANCE_SPOT_VENUE else (perp, spot)

        maker_guard = await create_maker_guard(
            maker_adapter=maker_adapter, taker_adapter=taker_adapter, symbol=plan.symbol,
            fetch_books=guard_books, maker_side=plan.maker_side, taker_side=plan.taker_side,
            quantity=plan.quantity,
            improve_maker_by_one_tick=False,
            follow_maker_quote=True,
            wait_for_gap_seconds=GAP_WAIT_TIMEOUT_SECONDS,
            top_level_only=top_level_only, size_rules=sizing_rules,
            remaining_quantity=remaining_quantity,
        )
        if top_level_only and maker_guard is None:
            raise RuntimeError("Aster/Binance best-level execution requires a maker quote guard")
        # For close mode, treating spot as the "short venue" makes the existing
        # side mapper emit spot SELL and perp BUY.
        side_short_venue = plan.short_venue if plan.mode == "open" else BINANCE_SPOT_VENUE
        side_long_venue = BINANCE_SPOT_VENUE if plan.mode == "open" else plan.short_venue
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
            maker_fill_poll_interval_seconds=0.2,
            max_maker_reprice_attempts=maker_reprice_attempts_for_venue(plan.maker_venue),
            maker_reprice_min_change_pct=(
                VARIATIONAL_MAKER_REPRICE_MIN_CHANGE_PCT
                if plan.maker_venue == "variational"
                else 0.0
            ),
            maker_price_refresher=lambda: refresh_spot_perp_maker_price(plan),
            maker_price_guard=maker_guard,
            max_execution_price_gap_pct=(
                plan.maker_taker_price_gap_pct
                if allow_large_price_gap
                else float(SPOT_PERP_MAX_PRE_TRADE_PRICE_GAP)
            ),
            min_hedge_notional_usd=float(min_hedge_notional),
            verify_hedge_fill=True,
            require_balanced_positions=False,
            maker_market_type="spot" if plan.maker_venue == BINANCE_SPOT_VENUE else "perp",
            taker_market_type="spot" if plan.taker_venue == BINANCE_SPOT_VENUE else "perp",
            maker_reduce_only=plan.mode == "close" and plan.maker_venue != BINANCE_SPOT_VENUE,
            taker_reduce_only=plan.mode == "close" and plan.taker_venue != BINANCE_SPOT_VENUE,

        )
        fee = getattr(spot_adapter, "last_base_asset_fee", Decimal(0))
        if isinstance(fee, Decimal) and fee > 0:
            result["spot_base_asset_fee"] = str(fee)
        if plan.mode != "open" or not result.get("ok", False):
            return result

        adapters_by_venue = {
            plan.maker_venue: maker_adapter,
            plan.taker_venue: taker_adapter,
        }
        fallback_quantity = (
            _executed_quantity_from_result(result)
            if plan.short_venue.strip().lower() == "variational"
            else None
        )
        spot_leg = await fetch_required_live_position(
            adapters_by_venue[BINANCE_SPOT_VENUE],
            venue=BINANCE_SPOT_VENUE,
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
            fallback_quantity=fallback_quantity,
        )
        # This clip's deltas were verified by the executor. Historical position
        # differences must not reject an otherwise completed, user-sized batch.
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
        for adapter in ((maker_adapter, taker_adapter) if adapters is None else ()):
            close = getattr(adapter, "close", None)
            if close is None:
                continue
            result = close()
            if asyncio.iscoroutine(result):
                await result


async def load_spot_perp_size_rules(spot, perp, symbol):
    spot_rules = await spot._load_symbol_rules(symbol)
    raw_symbol = await perp._resolve_raw_symbol(symbol)
    perp_info = (await perp._load_exchange_info_by_symbol())[raw_symbol]
    return combined_size_rules(spot_rules["filters"],
                               {f["filterType"]: f for f in perp_info["filters"]})


async def wait_for_pair_quotes(adapters):
    tasks = [asyncio.create_task(waiter()) for adapter in adapters.values()
             if callable(waiter := getattr(adapter, "wait_for_hedge_quote", None))]
    try:
        if tasks:
            done, _ = await asyncio.wait(tasks, timeout=.1, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
        else:
            await asyncio.sleep(.1)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def wait_for_depth_sized_plan(*, adapters, symbol, mode, remaining, ticks, rules):
    last, last_log = "行情尚未可用", None
    try:
        async with asyncio.timeout(GAP_WAIT_TIMEOUT_SECONDS):
            while True:
                waiter = lambda: wait_for_pair_quotes(adapters)
                try:
                    spot_book, perp_book = await asyncio.gather(
                        adapters[BINANCE_SPOT_VENUE].refresh_hedge_quote(), adapters["aster"].refresh_hedge_quote())
                    candidates = []
                    for maker_venue in ("aster", BINANCE_SPOT_VENUE):
                        plan = build_spot_perp_plan(symbol=symbol, mode=mode, short_venue="aster", quantity=remaining,
                            clip_usd=None, spot_book=spot_book, perp_book=perp_book, maker_venue_override=maker_venue)
                        quantity, price, hedge = depth_clip(remaining=remaining, maker_book=plan.maker_orderbook,
                            taker_book=plan.taker_orderbook, maker_side=plan.maker_side,
                            tick=ticks[plan.maker_venue], rules=rules, improve=False, wait_for_gap=True,
                            top_level_only=True)
                        if quantity > 0:
                            gap = adverse_price_gap(price, hedge, plan.maker_side)
                            spread = plan.spot_spread_pct if plan.taker_venue == BINANCE_SPOT_VENUE else plan.perp_spread_pct
                            candidate = replace(plan, quantity=quantity, clip_usd=plan.clip_usd * float(quantity / remaining),
                                maker_price=str(price), maker_execution_price=price, taker_execution_price=hedge,
                                maker_taker_price_gap_pct=price_gap_pct(price, hedge))
                            candidates.append((gap, spread, candidate, price, hedge))
                    if candidates:
                        _, _, plan, price, hedge = min(candidates, key=lambda item: item[:2])
                        return plan, price, hedge
                    last = f"價差／可成交深度未符合 {MAKER_TAKER_MAX_GAP:.2%} 及最低下單要求"
                except (DepthQuoteUnavailable, TimeoutError) as exc:
                    last = str(exc)
                now = time.monotonic()
                if last_log is None or now - last_log >= 5:
                    print(f"[gap-wait] 暫不掛單；remaining={remaining}；{last}", flush=True)
                    last_log = now
                await wait_for_quote_update(waiter)
    except TimeoutError as exc:
        raise MakerGapTimeout(f"等待 {GAP_WAIT_TIMEOUT_SECONDS:g} 秒仍未取得可交易價差／深度，退出；remaining={remaining}；{last}") from exc


async def execute_depth_sized_batches(*, symbol, mode, total_size, leverage):
    """Binance/Aster auto clips share live books and sessions across batches."""
    adapters = {venue: build_spot_perp_adapter(venue, leverage=leverage, mode=mode)
                for venue in (BINANCE_SPOT_VENUE, "aster")}
    spot, perp = adapters[BINANCE_SPOT_VENUE], adapters["aster"]
    remaining, completed, batches = total_size, Decimal(0), 0
    try:
        rules = await load_spot_perp_size_rules(spot, perp, symbol)
        if not total_size.is_finite() or total_size <= 0 or total_size % rules.step:
            raise ValueError(f"total_size_token must be a positive multiple of both venues' step {rules.step}")
        ticks = {BINANCE_SPOT_VENUE: Decimal(await spot.get_price_tick_size(symbol)),
                 "aster": Decimal(await perp.get_price_tick_size(symbol))}
        for adapter in adapters.values():
            await adapter.prepare_execution(symbol)
            await adapter.prepare_hedge_quotes(symbol)
        while remaining > 0:
            plan, price, vwap = await wait_for_depth_sized_plan(adapters=adapters, symbol=symbol, mode=mode,
                                                              remaining=remaining, ticks=ticks, rules=rules)
            quantity = plan.quantity
            batches += 1
            print(f"batch {batches}/auto clip_size_token={quantity} remaining={remaining} "
                  f"maker={plan.maker_venue} {plan.maker_side} taker={plan.taker_venue} {plan.taker_side} "
                  f"maker_reference={price} hedge_best={vwap} "
                  f"adverse_gap={adverse_price_gap(price, vwap, plan.maker_side):.4%}", flush=True)
            result = await execute_spot_perp_plan(plan=plan, leverage=leverage, adapters=adapters,
                                                  sizing_rules=rules, remaining_quantity=remaining)
            if not result.get("ok"):
                raise RuntimeError(f"auto batch failed; completed={completed} remaining={remaining}: {result.get('error')}")
            executed = Decimal(str(result.get("executed_quantity", "0")))
            if not executed.is_finite() or not 0 < executed <= quantity:
                raise RuntimeError(f"invalid confirmed auto batch quantity: {executed}; requested={quantity}")
            completed += executed
            remaining -= executed
            for line in format_spot_perp_execution_summary(plan=plan, result=result):
                print(line)
            spot.reset_completed_batch_accounting()
            print(f"[auto-size] confirmed={completed}/{total_size} remaining={remaining}", flush=True)
        print(f"完成: {batches} 批次，實際成交 {completed} 枚", flush=True)
        return {"ok": True, "batches": batches, "executed_qty": str(completed)}
    finally:
        await asyncio.gather(*(adapter.close() for adapter in adapters.values()))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one Binance spot vs perp arbitrage clip.")
    parser.add_argument("--mode", default=None, choices=sorted(SUPPORTED_MODES))
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--short-venue", default=None, choices=sorted(SUPPORTED_SHORT_VENUES))
    parser.add_argument("--total-size", default=None, help="Total base asset size to execute.")
    parser.add_argument("--clip-size", default=None, help="Optional fixed batch size; omitted = automatic depth sizing for Binance/Aster.")
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
    auto_size = args.clip_size is None
    clip_size = Decimal(args.clip_size) if not auto_size else total_size
    if not total_size.is_finite() or not clip_size.is_finite() or total_size <= 0 or clip_size <= 0:
        raise RuntimeError("total_size and clip_size must be positive")
    if auto_size and short_venue != "aster":
        raise RuntimeError("automatic depth sizing currently supports Binance/Aster; specify --clip-size for other perp venues")
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
    if not auto_size:
        assert_min_spot_perp_notional(clip_size=final_clip_size, clip_usd=final_clip_usd)

    print("spot-perp arbitrage preview")
    print(f"mode: {plan.mode}")
    print(f"symbol: {plan.symbol}")
    print(f"spot_venue: {BINANCE_SPOT_VENUE}")
    print(f"short_venue: {plan.short_venue}")
    print(f"spot_spread: {fmt_pct(plan.spot_spread_pct)}")
    print(f"perp_spread: {fmt_pct(plan.perp_spread_pct)}")
    print(f"maker: {plan.maker_venue} {plan.maker_side} limit price={plan.maker_price}")
    print(f"taker: {plan.taker_venue} {plan.taker_side} market")
    print(f"pre_trade_price_gap: {float(plan.maker_taker_price_gap_pct):.2%}")
    print(f"total_size_token: {decimal_to_plain(total_size)}")
    if auto_size:
        print(f"clip_size_token: auto (live depth, max adverse gap {MAKER_TAKER_MAX_GAP:.2%})")
        print("token_batches: dynamic; actual clip size selected before each batch")
        print(f"estimated_total_usd: {plan.clip_usd:.2f}")
    else:
        print(f"clip_size_token: {decimal_to_plain(clip_size)}")
        print(f"token_batches: {num_batches}")
        print(f"estimated_first_clip_usd: {plan.clip_usd:.2f}")
    if mode != "close" or args.leverage is not None:
        print(f"leverage_x: {leverage}")

    allow_large_price_gap = False
    if not auto_size and plan.maker_venue != "aster" and plan.maker_taker_price_gap_pct > SPOT_PERP_MAX_PRE_TRADE_PRICE_GAP:
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

    if auto_size:
        await execute_depth_sized_batches(symbol=symbol, mode=mode, total_size=total_size, leverage=leverage)
        return

    async def execute_batches(*, broker_url: str | None = None) -> dict[str, object]:
        # The per-batch spot-perp execution is unchanged; order_service only drives
        # the loop (progress + interval/debounce, left at 0 here to match prior CLI).
        reference_books = [spot_book, perp_book]
        batch_remaining = total_size

        async def _run_batch(this_clip_size: Decimal) -> dict[str, object]:
            nonlocal batch_remaining
            estimated_clip_usd = estimate_clip_usd_from_books(
                clip_size=this_clip_size,
                spot_book=reference_books[0],
                perp_book=reference_books[1],
            )
            assert_min_spot_perp_notional(clip_size=this_clip_size, clip_usd=estimated_clip_usd)
            fresh_spot_book, fresh_perp_book = await fetch_plan_books(
                symbol=symbol,
                short_venue=short_venue,
                clip_usd=estimated_clip_usd,
            )
            reference_books[0] = fresh_spot_book
            reference_books[1] = fresh_perp_book
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
            print(f"estimated_clip_usd: {batch_plan.clip_usd:.2f}")
            print(f"maker: {batch_plan.maker_venue} {batch_plan.maker_side} limit price={batch_plan.maker_price}")
            print(f"taker: {batch_plan.taker_venue} {batch_plan.taker_side} market")
            print(f"pre_trade_price_gap: {float(batch_plan.maker_taker_price_gap_pct):.2%}")
            result = await execute_spot_perp_plan(
                plan=batch_plan,
                leverage=leverage,
                broker_url=broker_url,
                allow_large_price_gap=allow_large_price_gap,
                remaining_quantity=batch_remaining,
            )
            if result.get("ok"):
                confirmed = Decimal(str(result.get("executed_quantity", "0")))
                if not confirmed.is_finite() or not 0 < confirmed <= this_clip_size:
                    raise RuntimeError("invalid confirmed batch quantity; refusing another maker")
                batch_remaining -= confirmed
            for line in format_spot_perp_execution_summary(plan=batch_plan, result=result):
                print(line)
            return result

        result = await run_batched_execution(
            symbol=symbol,
            total_size=total_size,
            clip_size=clip_size,
            batch_count=num_batches,
            clip_usd=plan.clip_usd,
            venues=[plan.maker_venue, plan.taker_venue],
            interval_ms=0,
            debounce_ms=0,
            run_batch=_run_batch,
            on_progress=progress_printer(include_filled=False),
            deps=Deps(),
        )
        if not result.get("ok", False):
            raise RuntimeError(result.get("error", "spot-perp execution failed"))
        return result

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
            await server.wait_for_portfolio(timeout_seconds=15.0)
            await execute_batches(broker_url=server.ws_url)
    else:
        await execute_batches()


def main() -> None:
    try:
        asyncio.run(run_spot_perp_arbitrage())
    except MakerGapTimeout as exc:
        print(f"[gap-timeout] {exc}", flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
