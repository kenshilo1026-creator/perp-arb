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
    require_maker_fill_confirmation: bool = False,
    maker_fill_timeout_seconds: float = 60.0,
) -> dict[str, object]:
    maker_side, taker_side = execution_sides_for_signal(
        maker_venue=maker_venue,
        short_venue=short_venue,
        long_venue=long_venue,
    )

    state_machine.to_preview_ready()
    state_machine.to_awaiting_confirm()
    state_machine.to_placing_maker_leg()
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
    if resolved_maker_price is not None:
        maker_kwargs["price"] = resolved_maker_price
    maker_result = await maker_adapter.place_limit_order(**maker_kwargs)
    if not maker_result.get("ok", False):
        raise RuntimeError(f"maker order failed on {maker_venue}")

    maker_fill_result: dict[str, object] | None = None
    if require_maker_fill_confirmation:
        maker_fill_result = await wait_for_maker_fill(
            maker_adapter,
            maker_result=maker_result,
            symbol=symbol,
            side=maker_side,
            amount=str(quantity),
            timeout_seconds=maker_fill_timeout_seconds,
        )

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
            state_machine.to_completed()
            return {
                "ok": True,
                "maker_result": maker_result,
                "maker_fill_result": maker_fill_result,
                "hedge_result": hedge_result,
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
