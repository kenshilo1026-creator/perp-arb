from __future__ import annotations

import asyncio
from decimal import Decimal
import inspect
from typing import Awaitable, Callable

from hydra_basis.execution_engine.order_fill import extract_filled_quantity


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
    return "timeout" in message or "timed out" in message


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
    taker_pre_hook: Callable[[], Awaitable[None]] | None = None,
    min_hedge_notional_usd: float = 0.0,
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
        taker_pre_hook=taker_pre_hook,
        min_hedge_notional_usd=min_hedge_notional_usd,
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
    taker_pre_hook: Callable[[], Awaitable[None]] | None = None,
    min_hedge_notional_usd: float = 0.0,
) -> dict[str, object]:
    state_machine.to_preview_ready()
    state_machine.to_awaiting_confirm()
    maker_kwargs = {
        "symbol": symbol,
        "side": maker_side,
        "amount": str(quantity),
        "clip_usd": clip_usd,
    }
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

    maker_fill_result: dict[str, object] | None = None
    maker_result: dict[str, object] | None = None
    maker_cancel_result: dict[str, object] | None = None
    maker_attempts: list[dict[str, object]] = []
    maker_attempt = 0
    reuse_existing_maker_result = False
    active_maker_orders: list[dict[str, object]] = []

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
        errors: list[str] = []
        for active_order in reversed(active_maker_orders.copy()):
            try:
                await cancel_maker_order_with_retries(
                    maker_adapter,
                    maker_result=active_order,
                    symbol=symbol,
                    side=maker_side,
                    amount=str(quantity),
                )
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
                maker_result = await maker_adapter.place_limit_order(**maker_kwargs)
                attempt_record["maker_result"] = maker_result
            attempt_record["maker_result"] = maker_result
            if not maker_result.get("ok", False):
                raise RuntimeError(f"maker order failed on {maker_venue}")
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
            if isinstance(exception_order_result, dict):
                attempt_record.setdefault("maker_result", exception_order_result)
                maker_result = exception_order_result
                order_id = (
                    exception_order_result.get("order_id")
                    or exception_order_result.get("orderId")
                )
                if order_id not in (None, ""):
                    register_active_maker(exception_order_result)
            attempt_record["maker_fill_error"] = str(exc)
            maker_attempts.append(attempt_record)
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                await raise_after_maker_cleanup(exc)
            exhausted = max_maker_reprice_attempts >= 0 and maker_attempt >= max_maker_reprice_attempts
            if exhausted or not maker_fill_error_is_repriceable(exc):
                await raise_after_maker_cleanup(exc)
            placed_result = attempt_record.get("maker_result") or maker_result or {}
            fresh_price: str | None = None
            if maker_price_refresher is not None:
                try:
                    fresh_price = await maker_price_refresher()
                    current_price = maker_kwargs.get("price")
                    if maker_reprice_min_change_pct > 0 and current_price not in (None, ""):
                        change = price_change_pct(Decimal(str(current_price)), Decimal(str(fresh_price)))
                        attempt_record["fresh_price"] = fresh_price
                        attempt_record["price_change_pct"] = format_decimal(change)
                        if change < Decimal(str(maker_reprice_min_change_pct)):
                            attempt_record["reprice_skipped"] = True
                            print(
                                "[reprice] refreshed price barely moved "
                                f"old={current_price} new={fresh_price} "
                                f"change={format_decimal(change)} — keep existing order",
                                flush=True,
                            )
                            reuse_existing_maker_result = True
                            continue
                except Exception as refresh_exc:
                    print(f"[reprice] failed to refresh maker price before cancel: {refresh_exc}", flush=True)
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
                if isinstance(placed_result, dict):
                    mark_maker_closed(placed_result)
                print(f"[reprice] cancel ok — placing new order (attempt {maker_attempt + 2})", flush=True)
                await asyncio.sleep(1.0)
            except Exception as cancel_exc:
                await raise_after_maker_cleanup(
                    RuntimeError(
                        f"[reprice] cancel failed — stopping to avoid duplicate orders: {cancel_exc}"
                    )
                )
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
    while executed_quantity < requested_quantity and min_hedge_notional > 0:
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
        updated_executed_quantity = resolve_executed_quantity(
            requested_quantity=requested_quantity,
            maker_result=maker_result,
            maker_fill_result=maker_cancel_result,
        )
        if updated_executed_quantity > executed_quantity:
            executed_quantity = updated_executed_quantity
            partial_fill = executed_quantity < requested_quantity
    elif require_maker_fill_confirmation or order_result_looks_filled(maker_result):
        mark_maker_closed(maker_result)

    state_machine.to_hedging_taker_leg()
    last_error: Exception | None = None
    for attempt in range(max_hedge_retries + 1):
        try:
            hedge_clip_usd = clip_usd * float(executed_quantity / requested_quantity)
            hedge_result = await taker_adapter.place_market_order(
                symbol=symbol,
                side=taker_side,
                amount=str(executed_quantity),
                clip_usd=hedge_clip_usd,
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
            return {
                "ok": True,
                "maker_result": maker_result,
                "maker_attempts": maker_attempts,
                "maker_fill_result": maker_fill_result,
                "maker_cancel_result": maker_cancel_result,
                "hedge_result": hedge_result,
                "requested_quantity": format(requested_quantity.normalize(), "f"),
                "executed_quantity": format(executed_quantity.normalize(), "f"),
                "remaining_quantity": format((requested_quantity - executed_quantity).normalize(), "f"),
                "partial_fill": partial_fill,
                "execution_price_summary": execution_price_summary,
            }
        except Exception as exc:
            last_error = exc
            if attempt >= max_hedge_retries:
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
