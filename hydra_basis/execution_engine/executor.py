from __future__ import annotations

import asyncio
from decimal import Decimal
import inspect
from typing import Awaitable, Callable

from hydra_basis.execution_engine.order_fill import extract_filled_quantity
from hydra_basis.execution_engine.hedge_safety import (
    confirm_pair_execution, execute_confirmed_market_order, position_quantity, terminal_fill_quantity,
    wait_for_terminal_order,
)


def passive_limit_price_from_orderbook(orderbook: dict[str, float | int], side: str) -> str:
    normalized = side.strip().upper()
    if normalized == "BUY":
        return f"{float(orderbook['bid']):g}"
    if normalized == "SELL":
        return f"{float(orderbook['ask']):g}"
    raise RuntimeError(f"unsupported order side: {side}")


def taker_price_from_orderbook(orderbook: dict[str, float | int], side: str) -> str:
    normalized = side.strip().upper()
    if normalized == "BUY":
        return f"{float(orderbook['ask']):g}"
    if normalized == "SELL":
        return f"{float(orderbook['bid']):g}"
    raise RuntimeError(f"unsupported order side: {side}")


def price_gap_pct(price_a: Decimal, price_b: Decimal) -> Decimal:
    if price_a <= 0 or price_b <= 0:
        raise RuntimeError("price gap requires positive prices")
    mid = (price_a + price_b) / Decimal("2")
    return abs(price_a - price_b) / mid


def price_change_pct(old_price: Decimal, new_price: Decimal) -> Decimal:
    if old_price <= 0 or new_price <= 0:
        raise RuntimeError("price change requires positive prices")
    return abs(new_price - old_price) / old_price


def format_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def extract_used_limit_price(order_result: object) -> str | None:
    """Read the submitted limit price from direct or broker-wrapped details."""
    if not isinstance(order_result, dict):
        return None
    details = order_result.get("details")
    if not isinstance(details, dict):
        return None
    direct = details.get("usedLimitPrice")
    if direct not in (None, ""):
        return str(direct)
    submitted = details.get("submitted")
    if not isinstance(submitted, dict):
        return None
    submitted_details = submitted.get("details")
    if not isinstance(submitted_details, dict):
        return None
    nested = submitted_details.get("usedLimitPrice")
    return str(nested) if nested not in (None, "") else None


def extract_baseline_position(order_result: object) -> tuple[Decimal, int] | None:
    if not isinstance(order_result, dict):
        return None
    details = order_result.get("details")
    if not isinstance(details, dict):
        return None
    quantity = details.get("baselinePositionQty")
    if quantity in (None, ""):
        return None
    try:
        return Decimal(str(quantity)), int(details.get("baselinePortfolioVersion") or 0)
    except Exception:
        return None


def signed_position_quantity(position: object) -> Decimal:
    if position is None:
        return Decimal("0")
    if not isinstance(position, dict):
        raise RuntimeError("invalid position snapshot")
    quantity = Decimal(str(position.get("quantity")))
    side = str(position.get("side") or "").upper()
    if side not in {"LONG", "SHORT"} or not quantity.is_finite() or quantity < 0:
        raise RuntimeError("invalid position side or quantity")
    return -quantity if side == "SHORT" else quantity


async def reconcile_disappeared_variational_order(
    maker_adapter,
    *,
    order_result: dict[str, object],
    symbol: str,
    side: str,
    requested_quantity: Decimal,
    attempts: int = 5,
    retry_delay_seconds: float = 1.0,
    settle: bool = False,
) -> dict[str, object]:
    """Classify an absent order from live position movement before replacing it."""
    baseline = extract_baseline_position(order_result)
    if baseline is None:
        return {"status": "ambiguous", "reason": "missing_position_baseline"}
    baseline_quantity, baseline_version = baseline
    snapshot_getter = getattr(maker_adapter, "get_open_position_snapshot", None)
    position_getter = getattr(maker_adapter, "get_open_position", None)
    if not callable(snapshot_getter) and not callable(position_getter):
        return {"status": "ambiguous", "reason": "position_query_unavailable"}

    last_quantity: Decimal | None = None
    last_version = baseline_version
    successful_queries = 0
    latest_fill: dict[str, object] | None = None
    for attempt in range(max(attempts, 1)):
        try:
            if callable(snapshot_getter):
                snapshot = await snapshot_getter(symbol=symbol, market_type="perp")
                if not isinstance(snapshot, dict) or "position" not in snapshot:
                    raise RuntimeError("position snapshot missing position field")
                position = snapshot["position"]
                version = int(snapshot.get("portfolio_version") or 0)
            else:
                position = await position_getter(symbol=symbol, market_type="perp")
                version = baseline_version
            current_quantity = signed_position_quantity(position)
            successful_queries += 1
            last_quantity = current_quantity
            last_version = max(last_version, version)
            delta = current_quantity - baseline_quantity
            filled_quantity = delta if side.strip().upper() == "BUY" else -delta
            if filled_quantity > 0:
                if not settle:
                    filled_quantity = min(filled_quantity, requested_quantity)
                latest_fill = {
                    "status": "filled",
                    "fill_result": {
                        "ok": True,
                        "filled": True,
                        "status": "FILLED",
                        "filled_quantity": format_decimal(filled_quantity),
                        "source": "post_timeout_position_delta",
                        "baseline_position_quantity": format_decimal(baseline_quantity),
                        "current_position_quantity": format_decimal(current_quantity),
                        "portfolio_version": version,
                    },
                }
                if not settle or filled_quantity >= requested_quantity:
                    return latest_fill
            if filled_quantity <= 0 and current_quantity != baseline_quantity:
                return {
                    "status": "ambiguous",
                    "reason": "position_moved_opposite_order_direction",
                    "baseline_position_quantity": format_decimal(baseline_quantity),
                    "current_position_quantity": format_decimal(current_quantity),
                }
        except Exception as query_exc:
            if attempt >= max(attempts, 1) - 1:
                return {"status": "ambiguous", "reason": f"position_query_failed: {query_exc}"}
        if attempt < max(attempts, 1) - 1 and retry_delay_seconds > 0:
            await asyncio.sleep(retry_delay_seconds)

    if latest_fill is not None:
        return latest_fill
    if (
        successful_queries > 0
        and last_quantity == baseline_quantity
        and last_version > baseline_version
    ):
        return {
            "status": "unchanged",
            "baseline_position_quantity": format_decimal(baseline_quantity),
            "current_position_quantity": format_decimal(last_quantity),
            "baseline_portfolio_version": baseline_version,
            "current_portfolio_version": last_version,
        }
    if successful_queries > 0 and last_quantity == baseline_quantity:
        return {
            "status": "ambiguous",
            "reason": "no_fresh_portfolio_update_after_submit",
            "baseline_portfolio_version": baseline_version,
            "current_portfolio_version": last_version,
        }
    return {"status": "ambiguous", "reason": "position_reconciliation_inconclusive"}


def filled_notional_usd(*, clip_usd: float, requested_quantity: Decimal, executed_quantity: Decimal) -> Decimal:
    if requested_quantity <= 0:
        raise RuntimeError("requested quantity must be positive")
    return Decimal(str(clip_usd)) * executed_quantity / requested_quantity


def _decimal_from_mapping(payload: dict, keys: set[str]) -> Decimal | None:
    for key, value in payload.items():
        normalized_key = str(key).replace("-", "_").lower()
        if normalized_key not in keys or value in (None, "", 0, "0"):
            continue
        try:
            parsed = Decimal(str(value))
        except Exception:
            continue
        if parsed > 0:
            return parsed
    return None


def _average_price_from_quote_and_quantity(payload: dict) -> Decimal | None:
    quantity = _decimal_from_mapping(
        payload,
        {
            "executedqty",
            "executed_qty",
            "cumqty",
            "cum_qty",
            "filledqty",
            "filled_qty",
            "quantity",
            "qty",
        },
    )
    quote = _decimal_from_mapping(
        payload,
        {
            "cummulativequoteqty",
            "cumulativequoteqty",
            "cumulative_quote_qty",
            "cumquote",
            "cum_quote",
            "cumquoteqty",
            "executedquoteqty",
            "executed_quote_qty",
            "quoteqty",
            "quote_qty",
        },
    )
    if quantity is None or quote is None or quantity <= 0:
        return None
    return quote / quantity


def _average_price_from_fills(fills: object) -> Decimal | None:
    if not isinstance(fills, list):
        return None
    total_qty = Decimal("0")
    total_quote = Decimal("0")
    for fill in fills:
        if not isinstance(fill, dict):
            continue
        price = _decimal_from_mapping(fill, {"price", "fillprice", "fill_price"})
        qty = _decimal_from_mapping(fill, {"qty", "quantity", "executedqty", "executed_qty"})
        if price is None or qty is None:
            continue
        total_qty += qty
        total_quote += price * qty
    if total_qty <= 0:
        return None
    return total_quote / total_qty


def extract_average_price(payload: object) -> Decimal | None:
    average_price_keys = {
        "avgprice",
        "avg_price",
        "averageprice",
        "average_price",
        "executedprice",
        "executed_price",
        "fillprice",
        "fill_price",
    }
    fallback_price_keys = {
        "price",
    }
    if isinstance(payload, dict):
        average_price = _decimal_from_mapping(payload, average_price_keys)
        if average_price is not None:
            return average_price
        computed_price = _average_price_from_quote_and_quantity(payload)
        if computed_price is not None:
            return computed_price
        fills_price = _average_price_from_fills(payload.get("fills"))
        if fills_price is not None:
            return fills_price
        for value in payload.values():
            found = extract_average_price(value)
            if found is not None:
                return found
        fallback_price = _decimal_from_mapping(payload, fallback_price_keys)
        if fallback_price is not None:
            return fallback_price
    elif isinstance(payload, list):
        for item in payload:
            found = extract_average_price(item)
            if found is not None:
                return found
    return None


def execution_sides_for_signal(*, maker_venue: str, short_venue: str, long_venue: str) -> tuple[str, str]:
    maker_side = "SELL" if maker_venue == short_venue else "BUY"
    taker_side = "BUY" if maker_venue == short_venue else "SELL"
    return maker_side, taker_side


def order_result_looks_filled(order_result: dict[str, object]) -> bool:
    if bool(order_result.get("filled")):
        return True

    status = str(order_result.get("status", "")).strip().upper()
    if status in {"FILLED", "CLOSED", "EXECUTED"}:
        return True

    raw = order_result.get("raw")
    if isinstance(raw, dict):
        raw_status = str(raw.get("status", "")).strip().upper()
        if raw_status in {"FILLED", "CLOSED", "EXECUTED"}:
            return True
        response = raw.get("response")
        if isinstance(response, dict):
            statuses = response.get("data", {}).get("statuses", [])
            if statuses and isinstance(statuses[0], dict) and "filled" in statuses[0]:
                return True

    order_type = str(order_result.get("type", "")).strip().upper()
    if order_type == "ORDER_RESULT" and bool(order_result.get("filled")):
        return True

    return False


def maker_fill_error_is_repriceable(error: Exception) -> bool:
    message = str(error).strip().lower()
    return (
        "timeout" in message
        or "timed out" in message
        or "fill confirmation unavailable" in message
        or "limit submit click did not create a matching open order" in message
    )


def maker_order_failure(*, venue: str, symbol: str, side: str, result: object) -> RuntimeError:
    """Retain the response so rejection handling can reconcile/cancel this order."""
    if isinstance(result, dict):
        reason = result.get("error") or result.get("message") or result.get("msg")
        raw = result.get("raw")
        if not reason and isinstance(raw, dict):
            reason = raw.get("error") or raw.get("message") or raw.get("msg")
        details = result.get("details")
        if not reason and isinstance(details, dict):
            reason = details.get("error") or details.get("message")
        context = (
            f"error={reason or 'response missing successful acknowledgement'} "
            f"status={result.get('status')} type={result.get('type')} "
            f"requestId={result.get('requestId')} "
            f"orderId={result.get('orderId') or result.get('order_id')}"
        )
    else:
        context = f"invalid response type={type(result).__name__}"
    error = RuntimeError(f"maker order failed on {venue}: symbol={symbol} side={side} {context}")
    if isinstance(result, dict):
        error.order_result = result
    return error


def resolve_executed_quantity(
    *,
    requested_quantity: Decimal,
    maker_result: dict[str, object] | None,
    maker_fill_result: dict[str, object] | None,
) -> Decimal:
    for payload in (maker_fill_result, maker_result):
        if not isinstance(payload, dict):
            continue
        filled_quantity = extract_filled_quantity(payload, allow_terminal_quantity_fallback=True)
        if filled_quantity is not None and filled_quantity > 0:
            return min(filled_quantity, requested_quantity)
    return requested_quantity


async def wait_for_maker_fill(
    maker_adapter,
    *,
    maker_result: dict[str, object],
    symbol: str,
    side: str,
    amount: str,
    timeout_seconds: float,
) -> dict[str, object]:
    if order_result_looks_filled(maker_result):
        return {"ok": True, "source": "order_result", "raw": maker_result}

    waiter = getattr(maker_adapter, "wait_for_order_fill", None)
    if callable(waiter):
        result = waiter(
            order_result=maker_result,
            symbol=symbol,
            side=side,
            amount=amount,
            timeout_seconds=timeout_seconds,
            allow_partial_fill=True,
        )
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, dict) or not result.get("ok", False):
            raise RuntimeError(f"maker fill wait failed: {result}")
        return result

    raise RuntimeError(
        "maker fill confirmation unavailable: adapter has no wait_for_order_fill "
        "and the order response is not already FILLED"
    )


async def cancel_maker_order(
    maker_adapter,
    *,
    maker_result: dict[str, object],
    symbol: str,
    side: str,
    amount: str,
) -> dict[str, object]:
    cancel = getattr(maker_adapter, "cancel_order", None)
    if not callable(cancel):
        raise RuntimeError("maker cancel unavailable: adapter has no cancel_order")
    result = cancel(
        order_result=maker_result,
        symbol=symbol,
        side=side,
        amount=amount,
    )
    if inspect.isawaitable(result):
        result = await result
    if not isinstance(result, dict) or not result.get("ok", False):
        raise RuntimeError(f"maker cancel failed: {result}")
    return result


async def cancel_maker_order_with_retries(
    maker_adapter,
    *,
    maker_result: dict[str, object],
    symbol: str,
    side: str,
    amount: str,
    max_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> dict[str, object]:
    last_error: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await cancel_maker_order(
                maker_adapter,
                maker_result=maker_result,
                symbol=symbol,
                side=side,
                amount=amount,
            )
        except BaseException as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            print(
                "[maker-cleanup] cancel failed; retrying "
                f"attempt={attempt}/{max_attempts} symbol={symbol} side={side} error={exc}",
                flush=True,
            )
            await asyncio.sleep(retry_delay_seconds)
    raise RuntimeError(
        f"maker cancel failed after {max_attempts} attempts: {last_error}"
    ) from last_error


async def execute_single_clip(
    *,
    symbol: str,
    clip_usd: float,
    quantity: Decimal,
    maker_venue: str,
    taker_venue: str,
    short_venue: str,
    long_venue: str,
    maker_adapter,
    taker_adapter,
    max_hedge_retries: int,
    state_machine,
    maker_price: str | None = None,
    maker_orderbook: dict[str, float | int] | None = None,
    use_maker_orderbook_price: bool = True,
    taker_orderbook: dict[str, float | int] | None = None,
    require_maker_fill_confirmation: bool = False,
    maker_fill_timeout_seconds: float = 60.0,
    max_maker_reprice_attempts: int = 0,
    max_execution_price_gap_pct: float = 0.01,
    maker_reprice_min_change_pct: float = 0.0,
    maker_price_refresher: Callable[[], Awaitable[str]] | None = None,
    maker_keep_existing_check_delay_seconds: float = 10.0,
    taker_pre_hook: Callable[[], Awaitable[None]] | None = None,
    min_hedge_notional_usd: float = 0.0,
    maker_reduce_only: bool = False,
    taker_reduce_only: bool = False,
    verify_hedge_fill: bool = False,
) -> dict[str, object]:
    maker_side, taker_side = execution_sides_for_signal(
        maker_venue=maker_venue,
        short_venue=short_venue,
        long_venue=long_venue,
    )
    return await execute_single_clip_with_sides(
        symbol=symbol,
        clip_usd=clip_usd,
        quantity=quantity,
        maker_venue=maker_venue,
        taker_venue=taker_venue,
        maker_side=maker_side,
        taker_side=taker_side,
        maker_adapter=maker_adapter,
        taker_adapter=taker_adapter,
        max_hedge_retries=max_hedge_retries,
        state_machine=state_machine,
        maker_price=maker_price,
        maker_orderbook=maker_orderbook,
        use_maker_orderbook_price=use_maker_orderbook_price,
        taker_orderbook=taker_orderbook,
        require_maker_fill_confirmation=require_maker_fill_confirmation,
        maker_fill_timeout_seconds=maker_fill_timeout_seconds,
        max_maker_reprice_attempts=max_maker_reprice_attempts,
        max_execution_price_gap_pct=max_execution_price_gap_pct,
        maker_reprice_min_change_pct=maker_reprice_min_change_pct,
        maker_price_refresher=maker_price_refresher,
        maker_keep_existing_check_delay_seconds=maker_keep_existing_check_delay_seconds,
        taker_pre_hook=taker_pre_hook,
        min_hedge_notional_usd=min_hedge_notional_usd,
        maker_reduce_only=maker_reduce_only,
        taker_reduce_only=taker_reduce_only,
        verify_hedge_fill=verify_hedge_fill,
    )


async def execute_single_clip_with_sides(
    *,
    symbol: str,
    clip_usd: float,
    quantity: Decimal,
    maker_venue: str,
    taker_venue: str,
    maker_side: str,
    taker_side: str,
    maker_adapter,
    taker_adapter,
    max_hedge_retries: int,
    state_machine,
    maker_price: str | None = None,
    maker_orderbook: dict[str, float | int] | None = None,
    use_maker_orderbook_price: bool = True,
    taker_orderbook: dict[str, float | int] | None = None,
    require_maker_fill_confirmation: bool = False,
    maker_fill_timeout_seconds: float = 60.0,
    max_maker_reprice_attempts: int = 0,
    max_execution_price_gap_pct: float = 0.01,
    maker_reprice_min_change_pct: float = 0.0,
    maker_price_refresher: Callable[[], Awaitable[str]] | None = None,
    maker_keep_existing_check_delay_seconds: float = 10.0,
    taker_pre_hook: Callable[[], Awaitable[None]] | None = None,
    min_hedge_notional_usd: float = 0.0,
    maker_reduce_only: bool = False,
    taker_reduce_only: bool = False,
    verify_hedge_fill: bool = False,
) -> dict[str, object]:
    state_machine.to_preview_ready()
    state_machine.to_awaiting_confirm()
    maker_kwargs = {
        "symbol": symbol,
        "side": maker_side,
        "amount": str(quantity),
        "clip_usd": clip_usd,
    }
    if maker_reduce_only:
        maker_kwargs["reduce_only"] = True
    resolved_maker_price = maker_price
    if resolved_maker_price is None:
        if maker_orderbook is None:
            get_orderbook = getattr(maker_adapter, "get_orderbook", None)
            if callable(get_orderbook):
                maker_orderbook = get_orderbook(symbol)
        if maker_orderbook is not None:
            resolved_maker_price = passive_limit_price_from_orderbook(maker_orderbook, maker_side)

    preview_limit_order_price = getattr(maker_adapter, "preview_limit_order_price", None)
    if callable(preview_limit_order_price) and maker_price is None and not use_maker_orderbook_price:
        preview_result = preview_limit_order_price(
            symbol=symbol,
            side=maker_side,
            amount=str(quantity),
        )
        if inspect.isawaitable(preview_result):
            preview_result = await preview_result
        if not isinstance(preview_result, dict) or not preview_result.get("ok", False):
            raise RuntimeError(f"maker price preview failed on {maker_venue}: {preview_result}")
        preview_price = preview_result.get("price")
        if preview_price in (None, ""):
            raise RuntimeError(f"maker price preview missing price on {maker_venue}: {preview_result}")
        resolved_maker_price = str(preview_price)

    if resolved_maker_price is not None and (maker_price is not None or use_maker_orderbook_price):
        maker_kwargs["price"] = resolved_maker_price

    pre_trade_price_summary: dict[str, str] | None = None
    if resolved_maker_price is not None and taker_orderbook is not None:
        maker_pre_price = Decimal(str(resolved_maker_price))
        taker_pre_price = Decimal(taker_price_from_orderbook(taker_orderbook, taker_side))
        pre_trade_gap = price_gap_pct(maker_pre_price, taker_pre_price)
        pre_trade_price_summary = {
            "maker_price": format_decimal(maker_pre_price),
            "taker_price": format_decimal(taker_pre_price),
            "price_gap_pct": format_decimal(pre_trade_gap),
        }
        if pre_trade_gap > Decimal(str(max_execution_price_gap_pct)):
            raise RuntimeError(
                "pre-trade maker/taker price gap exceeds limit: "
                f"maker={format_decimal(maker_pre_price)} "
                f"taker={format_decimal(taker_pre_price)} "
                f"gap={format_decimal(pre_trade_gap)}"
            )

    if taker_pre_hook is not None:
        await taker_pre_hook()

    maker_position_baseline = None
    taker_position_baseline = None
    if verify_hedge_fill:
        for setup_name in ("ensure_isolated_margin", "ensure_leverage"):
            setup = getattr(taker_adapter, setup_name, None)
            if callable(setup):
                await setup(symbol)
        # Fail before submitting the maker if either live position is unavailable.
        maker_position_baseline, taker_position_baseline = await asyncio.gather(
            position_quantity(maker_adapter, symbol), position_quantity(taker_adapter, symbol),
        )
        if abs(maker_position_baseline + taker_position_baseline) > max(
            Decimal(str(quantity)) * Decimal("0.001"), Decimal("0.00000001")
        ):
            raise RuntimeError("FAIL-CLOSED existing position imbalance before maker dispatch: "
                               f"{maker_venue}={maker_position_baseline} {taker_venue}={taker_position_baseline}")
        for baseline, side, reduce_only in (
            (maker_position_baseline, maker_side, maker_reduce_only),
            (taker_position_baseline, taker_side, taker_reduce_only),
        ):
            if reduce_only and (abs(baseline) < Decimal(str(quantity))
                                or baseline * (1 if side.upper() == "BUY" else -1) >= 0):
                raise RuntimeError("FAIL-CLOSED close quantity/direction exceeds live position")

    maker_fill_result: dict[str, object] | None = None
    maker_result: dict[str, object] | None = None
    maker_cancel_result: dict[str, object] | None = None
    maker_attempts: list[dict[str, object]] = []
    maker_attempt = 0
    reuse_existing_maker_result = False
    active_maker_orders: list[dict[str, object]] = []
    deferred_interrupt: BaseException | None = None

    def register_active_maker(order_result: dict[str, object]) -> None:
        if not any(item is order_result for item in active_maker_orders):
            active_maker_orders.append(order_result)

    def mark_maker_closed(order_result: dict[str, object] | None) -> None:
        if order_result is None:
            return
        active_maker_orders[:] = [
            item for item in active_maker_orders if item is not order_result
        ]

    async def cleanup_active_makers() -> list[str]:
        nonlocal maker_cancel_result
        errors: list[str] = []
        for active_order in reversed(active_maker_orders.copy()):
            try:
                # Shield the cancel so a Ctrl+C / task-cancellation cannot
                # interrupt the cleanup itself mid-flight.
                cancelled = await asyncio.shield(cancel_maker_order_with_retries(
                    maker_adapter,
                    maker_result=active_order,
                    symbol=symbol,
                    side=maker_side,
                    amount=str(quantity),
                ))
                if active_order is maker_result:
                    maker_cancel_result = cancelled
                mark_maker_closed(active_order)
                print(
                    "[maker-cleanup] cancelled active maker before exit "
                    f"venue={maker_venue} symbol={symbol} side={maker_side}",
                    flush=True,
                )
            except BaseException as cleanup_exc:
                errors.append(
                    f"{maker_venue}:{symbol}:{maker_side}: {cleanup_exc}"
                )
        return errors

    async def raise_after_maker_cleanup(exc: BaseException) -> None:
        cleanup_errors = await cleanup_active_makers()
        if cleanup_errors and isinstance(exc, Exception):
            raise RuntimeError(
                f"{exc}; unresolved maker orders after cleanup: "
                + " | ".join(cleanup_errors)
            ) from exc
        raise exc

    while True:
        state_machine.to_placing_maker_leg()
        attempt_record: dict[str, object] = {"attempt": maker_attempt + 1}
        try:
            if reuse_existing_maker_result and maker_result is not None:
                reuse_existing_maker_result = False
                attempt_record["maker_result"] = maker_result
                attempt_record["reused_existing_order"] = True
            else:
                maker_result = None
                maker_cancel_result = None
                maker_fill_result = None
                maker_result = await maker_adapter.place_limit_order(**maker_kwargs)
                attempt_record["maker_result"] = maker_result
                # If the order was placed without an explicit price (e.g. variational Mid click),
                # capture the actual price used so reprice comparisons have a baseline.
                if maker_kwargs.get("price") is None:
                    used_price = extract_used_limit_price(maker_result)
                    if used_price:
                        maker_kwargs["price"] = str(used_price)
            attempt_record["maker_result"] = maker_result
            if not isinstance(maker_result, dict) or not maker_result.get("ok", False):
                raise maker_order_failure(
                    venue=maker_venue, symbol=symbol, side=maker_side, result=maker_result,
                )
            register_active_maker(maker_result)

            if not require_maker_fill_confirmation:
                maker_attempts.append(attempt_record)
                break

            maker_fill_result = await wait_for_maker_fill(
                maker_adapter,
                maker_result=maker_result,
                symbol=symbol,
                side=maker_side,
                amount=str(quantity),
                timeout_seconds=maker_fill_timeout_seconds,
            )
            attempt_record["maker_fill_result"] = maker_fill_result
            maker_attempts.append(attempt_record)
            break
        except BaseException as exc:
            exception_order_result = getattr(exc, "order_result", None)
            if not isinstance(exception_order_result, dict) and isinstance(maker_result, dict):
                # A fill waiter can raise without carrying the accepted order.
                exception_order_result = maker_result
            if verify_hedge_fill and maker_venue == "variational":
                # Keep real identifiers/error/price. Only supplement a missing
                # baseline; never replace the actual response with a synthetic one.
                if not isinstance(exception_order_result, dict):
                    exception_order_result = {"ok": False}
                if extract_baseline_position(exception_order_result) is None:
                    details = exception_order_result.get("details")
                    details = dict(details) if isinstance(details, dict) else {}
                    details.update({"potentiallySubmitted": True,
                                    "baselinePositionQty": str(maker_position_baseline),
                                    "baselinePortfolioVersion": 0})
                    exception_order_result = {**exception_order_result, "details": details}
                if isinstance(exc, Exception):
                    exc.order_result = exception_order_result
            if isinstance(exception_order_result, dict):
                attempt_record["maker_result"] = exception_order_result
                # Enriching a previously accepted response creates a new dict,
                # but it still represents the same active maker order.
                if maker_result is not exception_order_result:
                    mark_maker_closed(maker_result)
                maker_result = exception_order_result
                if maker_kwargs.get("price") is None:
                    used_price = extract_used_limit_price(exception_order_result)
                    if used_price:
                        maker_kwargs["price"] = used_price
                order_id = (
                    exception_order_result.get("order_id")
                    or exception_order_result.get("orderId")
                    or exception_order_result.get("client_order_index")
                )
                # Variational frequently has no usable order id. It can still
                # cancel safely by symbol + side + amount, so every dispatched
                # Variational error must be treated as a potentially live order.
                if maker_venue.strip().lower() == "variational" or order_id not in (None, ""):
                    register_active_maker(exception_order_result)
            attempt_record["maker_fill_error"] = str(exc)
            maker_attempts.append(attempt_record)
            print(f"[maker-failure] venue={maker_venue} symbol={symbol} side={maker_side} error={exc}", flush=True)
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                if verify_hedge_fill and maker_venue in {"aster", "lighter"}:
                    cleanup_errors = await cleanup_active_makers()
                    final_fill = terminal_fill_quantity(maker_cancel_result)
                    if not cleanup_errors and final_fill is not None and final_fill > 0:
                        # Finish only the counterpart of an already executed
                        # maker, then honor the interrupt without a new batch.
                        deferred_interrupt = exc
                        maker_fill_result = {"ok": True, "filled_quantity": str(final_fill)}
                        print(f"[maker-cleanup] completing hedge for confirmed fill={final_fill} before exit", flush=True)
                        break
                await raise_after_maker_cleanup(exc)
            if (
                maker_venue.strip().lower() == "variational"
                and isinstance(exception_order_result, dict)
                and extract_baseline_position(exception_order_result) is not None
            ):
                immediate_reconciliation = await reconcile_disappeared_variational_order(
                    maker_adapter,
                    order_result=exception_order_result,
                    symbol=symbol,
                    side=maker_side,
                    requested_quantity=Decimal(str(quantity)),
                )
                attempt_record["immediate_position_reconciliation"] = immediate_reconciliation
                if immediate_reconciliation.get("status") == "filled":
                    maker_fill_result = immediate_reconciliation["fill_result"]
                    print(
                        "[timeout-reconcile] Variational fill detected before price/cancel logic "
                        f"symbol={symbol} side={maker_side} "
                        f"qty={maker_fill_result.get('filled_quantity')}",
                        flush=True,
                    )
                    break
            exhausted = max_maker_reprice_attempts >= 0 and maker_attempt >= max_maker_reprice_attempts
            if exhausted or not maker_fill_error_is_repriceable(exc):
                if verify_hedge_fill and maker_venue in {"aster", "lighter"} and isinstance(maker_result, dict):
                    cleanup_errors = await cleanup_active_makers()
                    final_fill = terminal_fill_quantity(maker_cancel_result)
                    if not cleanup_errors and final_fill is not None and final_fill > 0:
                        maker_fill_result = {"ok": True, "filled_quantity": str(final_fill)}
                        break
                if verify_hedge_fill and maker_venue == "variational" and isinstance(maker_result, dict):
                    cleanup_errors = await cleanup_active_makers()
                    if not cleanup_errors:
                        reconciliation = await reconcile_disappeared_variational_order(
                            maker_adapter, order_result=maker_result, symbol=symbol,
                            side=maker_side, requested_quantity=Decimal(str(quantity)), settle=True,
                        )
                        attempt_record["post_cleanup_position_reconciliation"] = reconciliation
                        print(
                            f"[maker-reconcile] after cleanup venue={maker_venue} symbol={symbol} "
                            f"side={maker_side} result={reconciliation}", flush=True,
                        )
                        if reconciliation.get("status") == "filled":
                            maker_fill_result = reconciliation["fill_result"]
                            maker_cancel_result = {"ok": True, "status": "cancelled"}
                            break
                await raise_after_maker_cleanup(exc)
            placed_result = attempt_record.get("maker_result") or maker_result or {}
            fresh_price: str | None = None
            if maker_price_refresher is not None:
                try:
                    fresh_price = await maker_price_refresher()
                except Exception as refresh_exc:
                    await raise_after_maker_cleanup(
                        RuntimeError(
                            "failed to refresh maker price; refusing replacement: "
                            f"{refresh_exc}"
                        )
                    )
                current_price = maker_kwargs.get("price")
                if maker_reprice_min_change_pct > 0:
                    if current_price in (None, ""):
                        await raise_after_maker_cleanup(
                            RuntimeError(
                                "submitted maker price unavailable; refusing replacement"
                            )
                        )
                    try:
                        change = price_change_pct(
                            Decimal(str(current_price)), Decimal(str(fresh_price))
                        )
                    except Exception as price_exc:
                        await raise_after_maker_cleanup(
                            RuntimeError(
                                "invalid maker price for reprice decision; "
                                f"refusing replacement: {price_exc}"
                            )
                        )
                    attempt_record["fresh_price"] = fresh_price
                    attempt_record["price_change_pct"] = format_decimal(change)
                    if change < Decimal(str(maker_reprice_min_change_pct)):
                        attempt_record["reprice_skipped"] = True
                        # Only keep waiting when the original order is confirmed
                        # present. An absent order may have filled, so never replace it.
                        order_still_exists = True
                        has_check = getattr(maker_adapter, "has_open_order", None)
                        if callable(has_check):
                            try:
                                order_still_exists = await has_check(
                                    order_result=placed_result,
                                    symbol=symbol,
                                    side=maker_side,
                                    amount=str(quantity),
                                )
                            except Exception as check_exc:
                                print(
                                    f"[reprice] order-existence check failed ({check_exc}); "
                                    "assuming order still exists",
                                    flush=True,
                                )
                                order_still_exists = True
                        attempt_record["order_still_exists"] = order_still_exists
                        if not order_still_exists:
                            maker_cancel_result = {
                                "ok": True,
                                "raw": {"status": "already_absent"},
                            }
                            reconciliation = await reconcile_disappeared_variational_order(
                                maker_adapter,
                                order_result=placed_result,
                                symbol=symbol,
                                side=maker_side,
                                requested_quantity=Decimal(str(quantity)),
                            )
                            attempt_record["position_reconciliation"] = reconciliation
                            if reconciliation.get("status") == "filled":
                                maker_fill_result = reconciliation["fill_result"]
                                if isinstance(placed_result, dict):
                                    mark_maker_closed(placed_result)
                                print(
                                    "[reprice] absent maker reconciled as filled from live position "
                                    f"venue={maker_venue} symbol={symbol} "
                                    f"qty={maker_fill_result.get('filled_quantity')}",
                                    flush=True,
                                )
                                break
                            if reconciliation.get("status") == "unchanged":
                                if isinstance(placed_result, dict):
                                    mark_maker_closed(placed_result)
                                print(
                                    "[reprice] maker absent and live position unchanged — "
                                    f"safe to place replacement at {fresh_price}",
                                    flush=True,
                                )
                                maker_kwargs["price"] = fresh_price
                                reuse_existing_maker_result = False
                                maker_attempt += 1
                                continue
                            await raise_after_maker_cleanup(
                                RuntimeError(
                                    "maker order disappeared and live position reconciliation "
                                    f"was inconclusive: {reconciliation}"
                                )
                            )
                        print(
                            "[reprice] refreshed price barely moved "
                            f"old={current_price} new={fresh_price} "
                            f"change={format_decimal(change)} — keep existing order",
                            flush=True,
                        )
                        maker_kwargs["price"] = fresh_price
                        reuse_existing_maker_result = True
                        if maker_keep_existing_check_delay_seconds > 0:
                            await asyncio.sleep(maker_keep_existing_check_delay_seconds)
                        continue
            print(f"[reprice] attempt {maker_attempt + 1} timed out — cancelling {maker_venue} {maker_side} {symbol}", flush=True)
            try:
                cancel_result = await cancel_maker_order_with_retries(
                    maker_adapter,
                    maker_result=placed_result,
                    symbol=symbol,
                    side=maker_side,
                    amount=str(quantity),
                )
                attempt_record["cancel_result"] = cancel_result
                maker_cancel_result = cancel_result
                if isinstance(placed_result, dict):
                    mark_maker_closed(placed_result)
            except Exception as cancel_exc:
                await raise_after_maker_cleanup(
                    RuntimeError(
                        f"[reprice] cancel failed — stopping to avoid duplicate orders: {cancel_exc}"
                    )
                )
            cancel_raw = cancel_result.get("raw") if isinstance(cancel_result, dict) else None
            cancel_status = str(
                cancel_raw.get("status", "") if isinstance(cancel_raw, dict) else ""
            ).lower()
            baseline_available = extract_baseline_position(placed_result) is not None
            if maker_venue in {"aster", "lighter"}:
                final_fill = terminal_fill_quantity(cancel_result)
                if final_fill is None:
                    await raise_after_maker_cleanup(RuntimeError(
                        "maker cancellation has no terminal fill quantity; refusing replacement"
                    ))
                if final_fill > 0:
                    maker_fill_result = {"ok": True, "filled_quantity": str(final_fill)}
                    break
            elif maker_venue.strip().lower() == "variational" and baseline_available:
                reconciliation = await reconcile_disappeared_variational_order(
                    maker_adapter,
                    order_result=placed_result,
                    symbol=symbol,
                    side=maker_side,
                    requested_quantity=Decimal(str(quantity)),
                )
                attempt_record["position_reconciliation"] = reconciliation
                if reconciliation.get("status") == "filled":
                    maker_fill_result = reconciliation["fill_result"]
                    print(
                        "[reprice] cancelled/absent maker had a position fill — "
                        f"hedging qty={maker_fill_result.get('filled_quantity')}",
                        flush=True,
                    )
                    break
                if reconciliation.get("status") != "unchanged":
                    await raise_after_maker_cleanup(
                        RuntimeError(
                            "maker cancel completed but live position reconciliation "
                            f"was inconclusive: {reconciliation}"
                        )
                    )
            elif cancel_status == "already_absent":
                await raise_after_maker_cleanup(
                    RuntimeError(
                        "maker was already absent and no position baseline was available; "
                        "refusing replacement"
                    )
                )
            print(f"[reprice] cancel ok — placing new order (attempt {maker_attempt + 2})", flush=True)
            await asyncio.sleep(1.0)
            if fresh_price is not None:
                maker_kwargs["price"] = fresh_price
                print(f"[reprice] repriced to {fresh_price} (attempt {maker_attempt + 2})", flush=True)
            maker_attempt += 1

    if maker_result is None:
        raise RuntimeError("maker order was not submitted")

    requested_quantity = Decimal(str(quantity))
    executed_quantity = resolve_executed_quantity(
        requested_quantity=requested_quantity,
        maker_result=maker_result,
        maker_fill_result=maker_fill_result,
    )
    if executed_quantity <= 0:
        await raise_after_maker_cleanup(
            RuntimeError("maker execution produced zero filled quantity")
        )

    min_hedge_notional = Decimal(str(min_hedge_notional_usd))
    while (executed_quantity < requested_quantity and min_hedge_notional > 0
           and maker_cancel_result is None):
        current_notional = filled_notional_usd(
            clip_usd=clip_usd,
            requested_quantity=requested_quantity,
            executed_quantity=executed_quantity,
        )
        if current_notional >= min_hedge_notional:
            break
        print(
            "[partial-fill] filled notional below hedge minimum; waiting for more maker fill "
            f"filled_qty={format_decimal(executed_quantity)} "
            f"notional={format_decimal(current_notional)} "
            f"min={format_decimal(min_hedge_notional)}",
            flush=True,
        )
        try:
            maker_fill_result = await wait_for_maker_fill(
                maker_adapter,
                maker_result=maker_result,
                symbol=symbol,
                side=maker_side,
                amount=str(quantity),
                timeout_seconds=maker_fill_timeout_seconds,
            )
        except BaseException as exc:
            await raise_after_maker_cleanup(exc)
        updated_executed_quantity = resolve_executed_quantity(
            requested_quantity=requested_quantity,
            maker_result=maker_result,
            maker_fill_result=maker_fill_result,
        )
        if updated_executed_quantity <= executed_quantity:
            print(
                "[partial-fill] still below hedge minimum with no additional fill progress; keep waiting "
                f"filled_qty={format_decimal(executed_quantity)} "
                f"notional={format_decimal(current_notional)} "
                f"min={format_decimal(min_hedge_notional)}",
                flush=True,
            )
            await asyncio.sleep(1.0)
            continue
        executed_quantity = updated_executed_quantity

    partial_fill = executed_quantity < requested_quantity
    if partial_fill:
        if maker_cancel_result is None:
            try:
                maker_cancel_result = await cancel_maker_order_with_retries(
                    maker_adapter,
                    maker_result=maker_result,
                    symbol=symbol,
                    side=maker_side,
                    amount=str(quantity),
                )
                mark_maker_closed(maker_result)
            except BaseException as exc:
                await raise_after_maker_cleanup(exc)
        cancel_filled_quantity = extract_filled_quantity(
            maker_cancel_result,
            allow_terminal_quantity_fallback=True,
        )
        if cancel_filled_quantity is not None and cancel_filled_quantity > executed_quantity:
            executed_quantity = min(cancel_filled_quantity, requested_quantity)
            partial_fill = executed_quantity < requested_quantity
    elif require_maker_fill_confirmation or order_result_looks_filled(maker_result):
        mark_maker_closed(maker_result)

    if verify_hedge_fill:
        # Even a broker FILLED message may describe only the first trade.
        # Resolve any remaining maker before reading its final position delta.
        has_open_order = getattr(maker_adapter, "has_open_order", None)
        if maker_cancel_result is None and callable(has_open_order):
            register_active_maker(maker_result)
            try:
                if await has_open_order(order_result=maker_result, symbol=symbol,
                                        side=maker_side, amount=str(quantity)):
                    maker_cancel_result = await cancel_maker_order_with_retries(
                        maker_adapter, maker_result=maker_result, symbol=symbol,
                        side=maker_side, amount=str(quantity),
                    )
                mark_maker_closed(maker_result)
            except BaseException as exc:
                await raise_after_maker_cleanup(exc)
        if maker_venue in {"aster", "lighter"}:
            final_fill = None
            for response in (maker_cancel_result, maker_fill_result, maker_result):
                final_fill = terminal_fill_quantity(response)
                if final_fill is not None:
                    break
            if final_fill is None:
                query = getattr(maker_adapter, "get_order_execution", None)
                if not callable(query):
                    raise RuntimeError("FAIL-CLOSED maker final order query unavailable")
                final_status = await wait_for_terminal_order(
                    lambda: query(order_result=maker_result, symbol=symbol),
                )
                final_fill = terminal_fill_quantity(final_status)
            if final_fill is None or not 0 < final_fill <= requested_quantity:
                raise RuntimeError(f"FAIL-CLOSED invalid final maker fill: {final_fill}")
            # Use the order's terminal total, never a lagging partial position
            # snapshot. Verify both live positions after completing the hedge.
            final_reconciliation = {"status": "filled", "fill_result": {
                "ok": True, "filled_quantity": str(final_fill), "source": "terminal_order",
            }}
        else:
            final_reconciliation = await reconcile_disappeared_variational_order(
                maker_adapter,
                order_result={"details": {"baselinePositionQty": str(maker_position_baseline),
                                          "baselinePortfolioVersion": 0}},
                symbol=symbol, side=maker_side, requested_quantity=requested_quantity,
                attempts=10, settle=True,
            )
        if final_reconciliation.get("status") != "filled":
            raise RuntimeError(f"FAIL-CLOSED {maker_venue} final maker fill unresolved: "
                               f"{final_reconciliation}")
        previous_fill_result = maker_fill_result
        maker_fill_result = {**final_reconciliation["fill_result"], "raw": previous_fill_result}
        executed_quantity = Decimal(str(maker_fill_result["filled_quantity"]))
        maker_result = {
            "ok": True, "filled": True, "filled_quantity": str(executed_quantity),
            "source": maker_fill_result.get("source", "live_position_delta"), "raw": maker_result,
        }
        partial_fill = executed_quantity < requested_quantity
        print(f"[maker-reconcile] final {maker_venue} quantity={executed_quantity}", flush=True)

    state_machine.to_hedging_taker_leg()
    last_error: Exception | None = None
    for attempt in range(max_hedge_retries + 1):
        try:
            hedge_clip_usd = clip_usd * float(executed_quantity / requested_quantity)
            taker_kwargs = {
                "symbol": symbol,
                "side": taker_side,
                "amount": str(executed_quantity),
                "clip_usd": hedge_clip_usd,
            }
            if taker_reduce_only:
                taker_kwargs["reduce_only"] = True
            if verify_hedge_fill:
                # Recovery orders only go to the original hedge leg. Never
                # reverse the filled maker to compensate for a failed hedge.
                hedge_result = await execute_confirmed_market_order(
                    taker_adapter, symbol=symbol, side=taker_side,
                    quantity=executed_quantity, clip_usd=hedge_clip_usd,
                    baseline=taker_position_baseline, reduce_only=taker_reduce_only,
                    max_attempts=max(3, max_hedge_retries + 1),
                )
            else:
                hedge_result = await taker_adapter.place_market_order(**taker_kwargs)
                if not isinstance(hedge_result, dict) or not hedge_result.get("ok", False):
                    raise RuntimeError(f"hedge order rejected: {hedge_result}")
            if verify_hedge_fill:
                await confirm_pair_execution(
                    maker_adapter, taker_adapter, symbol=symbol,
                    maker_baseline=maker_position_baseline, taker_baseline=taker_position_baseline,
                    maker_side=maker_side, quantity=executed_quantity,
                )
            maker_avg_price = (
                extract_average_price(maker_fill_result)
                or extract_average_price(maker_result)
                or (Decimal(str(resolved_maker_price)) if resolved_maker_price is not None else None)
            )
            taker_avg_price = extract_average_price(hedge_result)
            execution_price_summary = {
                "maker_avg_price": format_decimal(maker_avg_price) if maker_avg_price is not None else None,
                "taker_avg_price": format_decimal(taker_avg_price) if taker_avg_price is not None else None,
                "price_gap_pct": (
                    format_decimal(price_gap_pct(maker_avg_price, taker_avg_price))
                    if maker_avg_price is not None and taker_avg_price is not None
                    else None
                ),
                "pre_trade": pre_trade_price_summary,
            }
            state_machine.to_completed()
            if deferred_interrupt is not None:
                raise deferred_interrupt
            return {
                "ok": True,
                "hedge_verified": verify_hedge_fill,
                "maker_result": maker_result,
                "maker_attempts": maker_attempts,
                "maker_fill_result": maker_fill_result,
                "maker_cancel_result": maker_cancel_result,
                "hedge_result": hedge_result,
                "requested_quantity": format(requested_quantity.normalize(), "f"),
                "executed_quantity": format(executed_quantity.normalize(), "f"),
                "remaining_quantity": format(max(Decimal("0"), requested_quantity - executed_quantity).normalize(), "f"),
                "partial_fill": partial_fill,
                "execution_price_summary": execution_price_summary,
            }
        except Exception as exc:
            last_error = exc
            if verify_hedge_fill or attempt >= max_hedge_retries:
                state_machine.to_emergency_exit()
                await raise_after_maker_cleanup(
                    RuntimeError(f"hedge failed on {taker_venue}: {exc}")
                )
            state_machine.to_retrying_hedge()
            await asyncio.sleep(0)
            state_machine.to_hedging_taker_leg()

    state_machine.to_emergency_exit()
    await raise_after_maker_cleanup(
        RuntimeError(f"hedge failed on {taker_venue}: {last_error}")
    )
    raise RuntimeError("unreachable")
