from __future__ import annotations

import asyncio
from decimal import Decimal
import inspect


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


def format_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def extract_average_price(payload: object) -> Decimal | None:
    price_keys = {
        "avgprice",
        "avg_price",
        "averageprice",
        "average_price",
        "executedprice",
        "executed_price",
        "fillprice",
        "fill_price",
        "price",
    }
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key).replace("-", "_").lower() in price_keys and value not in (None, "", 0, "0"):
                try:
                    parsed = Decimal(str(value))
                except Exception:
                    continue
                if parsed > 0:
                    return parsed
        for value in payload.values():
            found = extract_average_price(value)
            if found is not None:
                return found
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

    maker_fill_result: dict[str, object] | None = None
    maker_result: dict[str, object] | None = None
    maker_attempts: list[dict[str, object]] = []
    for maker_attempt in range(max_maker_reprice_attempts + 1):
        state_machine.to_placing_maker_leg()
        maker_result = await maker_adapter.place_limit_order(**maker_kwargs)
        maker_attempts.append({"attempt": maker_attempt + 1, "maker_result": maker_result})
        if not maker_result.get("ok", False):
            raise RuntimeError(f"maker order failed on {maker_venue}")

        if not require_maker_fill_confirmation:
            break

        try:
            maker_fill_result = await wait_for_maker_fill(
                maker_adapter,
                maker_result=maker_result,
                symbol=symbol,
                side=maker_side,
                amount=str(quantity),
                timeout_seconds=maker_fill_timeout_seconds,
            )
            maker_attempts[-1]["maker_fill_result"] = maker_fill_result
            break
        except Exception as exc:
            maker_attempts[-1]["maker_fill_error"] = str(exc)
            if maker_attempt >= max_maker_reprice_attempts or not maker_fill_error_is_repriceable(exc):
                raise
            cancel_result = await cancel_maker_order(
                maker_adapter,
                maker_result=maker_result,
                symbol=symbol,
                side=maker_side,
                amount=str(quantity),
            )
            maker_attempts[-1]["cancel_result"] = cancel_result

    if maker_result is None:
        raise RuntimeError("maker order was not submitted")

    state_machine.to_hedging_taker_leg()
    last_error: Exception | None = None
    for attempt in range(max_hedge_retries + 1):
        try:
            hedge_result = await taker_adapter.place_market_order(
                symbol=symbol,
                side=taker_side,
                amount=str(quantity),
                clip_usd=clip_usd,
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
                "hedge_result": hedge_result,
                "execution_price_summary": execution_price_summary,
            }
        except Exception as exc:
            last_error = exc
            if attempt >= max_hedge_retries:
                state_machine.to_emergency_exit()
                raise RuntimeError(f"hedge failed on {taker_venue}: {exc}") from exc
            state_machine.to_retrying_hedge()
            await asyncio.sleep(0)
            state_machine.to_hedging_taker_leg()

    state_machine.to_emergency_exit()
    raise RuntimeError(f"hedge failed on {taker_venue}: {last_error}")
