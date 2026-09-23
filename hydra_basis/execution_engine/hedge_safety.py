"""Confirm market fills before retrying; an acknowledgement is not a fill."""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Awaitable, Callable

class HedgeFailure(RuntimeError):
    def __init__(self, message: str, *, confirmed: Decimal, uncertain: bool, attempts: list):
        super().__init__(message)
        self.confirmed_quantity = confirmed
        self.uncertain = uncertain
        self.attempts = attempts


async def position_quantity(adapter, symbol: str) -> Decimal:
    position = await adapter.get_open_position(symbol=symbol, market_type="perp")
    if position is None:
        return Decimal("0")
    if not isinstance(position, dict) or position.get("side") not in {"LONG", "SHORT"}:
        raise RuntimeError(f"invalid live position for {symbol}: {position}")
    quantity = Decimal(str(position["quantity"]))
    if not quantity.is_finite() or quantity < 0:
        raise RuntimeError(f"invalid live position quantity for {symbol}")
    return -quantity if position["side"] == "SHORT" else quantity


def terminal_fill_quantity(result: object) -> Decimal | None:
    """Only terminal order responses authorize a residual order.

    Do not descend into arbitrary diagnostics / submitted orders: those can
    contain an earlier fill or the requested size, rather than this order's fill.
    """
    if not isinstance(result, dict):
        return None
    if result.get("terminal") is False:
        return None
    terminal = result.get("terminal") is True or str(result.get("status", "")).upper() in {
        "FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED",
    }
    if terminal:
        # Never use a historical trade buried in diagnostics as this order's
        # terminal total. Only explicit scalar fill fields are accepted here.
        for key in ("filled_quantity", "executedQty", "executed_qty", "filled_qty",
                    "filledQty", "filledBaseAmount", "filled_base_amount", "dealVol"):
            if result.get(key) is not None:
                try:
                    value = Decimal(str(result[key]))
                except Exception:
                    return None
                return value if value.is_finite() and value >= 0 else None
        return None
    for key in ("raw", "order"):
        value = terminal_fill_quantity(result.get(key))
        if value is not None:
            return value
    return None


async def confirm_pair_execution(
    maker_adapter, taker_adapter, *, symbol: str, maker_baseline: Decimal,
    taker_baseline: Decimal, maker_side: str, quantity: Decimal,
    attempts: int = 20, poll_delay: float = 0.5,
) -> None:
    direction = Decimal("1") if maker_side.upper() == "BUY" else Decimal("-1")
    expected_maker = maker_baseline + direction * quantity
    expected_taker = taker_baseline - direction * quantity
    last = None
    for attempt in range(max(1, attempts)):
        try:
            maker, taker = await asyncio.gather(
                position_quantity(maker_adapter, symbol), position_quantity(taker_adapter, symbol),
            )
            last = f"maker={maker} taker={taker} expected={expected_maker}/{expected_taker}"
            if maker == expected_maker and taker == expected_taker:
                return
        except Exception as exc:
            last = str(exc)
        if attempt + 1 < attempts:
            await asyncio.sleep(poll_delay)
    raise RuntimeError(f"FAIL-CLOSED hedge live position confirmation failed: {last}")


async def wait_for_terminal_order(
    fetch_status: Callable[[], Awaitable[dict]], *, initial: dict | None = None,
    attempts: int = 20, poll_delay: float = 0.5,
) -> dict:
    """A cancel acknowledgement is insufficient: obtain the final cumulative fill."""
    result = initial
    last_error = None
    for attempt in range(max(1, attempts)):
        if terminal_fill_quantity(result) is not None:
            return result
        try:
            result = await fetch_status()
        except Exception as exc:
            last_error = str(exc)
            result = None
        if terminal_fill_quantity(result) is not None:
            return result
        if attempt + 1 < attempts:
            await asyncio.sleep(poll_delay)
    raise RuntimeError(f"order terminal state/fill unresolved; refusing replacement; "
                       f"last_status={result} last_error={last_error}")


async def execute_confirmed_market_order(
    adapter, *, symbol: str, side: str, quantity: Decimal, clip_usd: float,
    baseline: Decimal, reduce_only: bool = False, max_attempts: int = 3,
    confirmation_attempts: int = 20, poll_delay: float = 0.5,
) -> dict:
    """Retry only a proven terminal remainder; never retry an unknown submission.

    The baseline must be captured before either leg is dispatched. Full live
    position movement can resolve a lost response. Partial/unchanged movement
    alone cannot prove that a timed out order will not fill later.
    """
    if not quantity.is_finite() or quantity <= 0 or side.upper() not in {"BUY", "SELL"}:
        raise ValueError("confirmed market order requires positive quantity and BUY/SELL side")
    confirmed = Decimal("0")
    direction = Decimal("1") if side.upper() == "BUY" else Decimal("-1")
    attempts: list[dict] = []
    for attempt in range(max(1, max_attempts)):
        remaining = quantity - confirmed
        kwargs = dict(symbol=symbol, side=side, amount=str(remaining),
                      clip_usd=clip_usd * float(remaining / quantity))
        if reduce_only:
            kwargs["reduce_only"] = True
        result = None
        error = None
        try:
            result = await adapter.place_market_order(**kwargs)
        except Exception as exc:
            error = str(exc)
            result = getattr(exc, "order_result", None)
        record = {"amount": str(remaining), "result": result, "error": error}
        attempts.append(record)
        terminal_quantity = terminal_fill_quantity(result)
        if terminal_quantity is None:
            query = getattr(adapter, "get_order_execution", None)
            # Keep the submitted identity: status responses need not contain it.
            for check in range(max(1, confirmation_attempts)):
                if callable(query) and isinstance(result, dict):
                    try:
                        status = await query(order_result=result, symbol=symbol)
                        record["order_status"] = status
                        terminal_quantity = terminal_fill_quantity(status)
                        if terminal_quantity is not None:
                            break
                    except Exception as exc:
                        record["status_error"] = str(exc)
                try:
                    delta = (await position_quantity(adapter, symbol) - baseline) * direction
                    record["live_filled_quantity"] = str(delta)
                    if delta == quantity:
                        return {"ok": True, "filled_quantity": str(delta),
                                "source": "live_position_delta", "attempts": attempts}
                    if delta < 0 or delta > quantity:
                        record["position_error"] = "unexpected position movement"
                        break
                except Exception as exc:
                    record["position_error"] = str(exc)
                if check + 1 < confirmation_attempts:
                    await asyncio.sleep(poll_delay)
        if terminal_quantity is not None:
            if terminal_quantity > remaining:
                raise HedgeFailure("hedge overfilled requested remainder", confirmed=confirmed,
                                   uncertain=True, attempts=attempts)
            confirmed += terminal_quantity
            if confirmed == quantity:
                return {"ok": True, "terminal": True, "filled_quantity": str(confirmed),
                        "attempts": attempts, "raw": result}
            print(f"[hedge-reconcile] terminal fill={terminal_quantity} remaining={quantity - confirmed}", flush=True)
        else:
            raise HedgeFailure(
                f"hedge outcome unknown for {symbol}; confirmed={confirmed} target={quantity}; "
                f"observed_position_fill={record.get('live_filled_quantity', 'unavailable')}; "
                "refusing duplicate submission while the order may still fill",
                confirmed=confirmed, uncertain=True, attempts=attempts,
            )
        if attempt + 1 < max_attempts:
            await asyncio.sleep(poll_delay)
    raise HedgeFailure(
        f"hedge incomplete for {symbol}; confirmed={confirmed} remaining={quantity - confirmed}",
        confirmed=confirmed, uncertain=False, attempts=attempts,
    )
