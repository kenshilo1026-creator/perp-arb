"""Confirm market fills before retrying; an acknowledgement is not a fill."""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Awaitable, Callable


async def race_order_confirmation(rest_wait, stream_wait):
    """A push may interrupt REST I/O or polling sleep; REST bounds the fallback.

    Only read-only confirmation tasks are cancelled, never an order submission.
    A stream failure cannot cancel the independent REST reconciliation.
    """
    stream = asyncio.create_task(stream_wait)
    tasks = [stream]
    try:
        # Let an already-cached terminal push finish before starting REST/signing.
        # This is one scheduler turn, not a grace-period timer before fallback.
        await asyncio.sleep(0)
        if stream.done():
            try:
                return stream.result()
            except Exception:
                pass
        rest = asyncio.create_task(rest_wait())
        tasks.append(rest)
        done, _ = await asyncio.wait((stream, rest), return_when=asyncio.FIRST_COMPLETED)
        if stream in done:
            try:
                return stream.result()
            except Exception:
                pass
        return await rest
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

class HedgeFailure(RuntimeError):
    def __init__(self, message: str, *, confirmed: Decimal, uncertain: bool, attempts: list):
        super().__init__(message)
        self.confirmed_quantity = confirmed
        self.uncertain = uncertain
        self.attempts = attempts


def merge_hedge_fills(results: list[dict]) -> dict:
    """Combine disjoint, confirmed hedge quantities (including early partials)."""
    from hydra_basis.execution_engine.executor import extract_actual_average_price
    total, quote = Decimal(0), Decimal(0)
    priced = True
    attempts, timings = [], []
    for result in results:
        quantity = Decimal(result["filled_quantity"])
        total += quantity
        price = extract_actual_average_price(result)
        if quantity > 0:
            if price is None:
                priced = False
            else:
                quote += quantity * price
        attempts.extend(result.get("attempts", []))
        timings.extend(result.get("request_timings", []))
    combined = {"ok": True, "terminal": True, "aggregate_fill": True,
                "filled_quantity": str(total), "attempts": attempts, "request_timings": timings}
    sources = {result.get("source", "rest") for result in results}
    combined["source"] = next(iter(sources)) if len(sources) == 1 else "mixed"
    if priced and total > 0:
        combined["avg_price"] = str(quote / total)
    for key in ("first_dispatch_ns", "first_submit_ns"):
        values = [r[key] for r in results if r.get(key) is not None]
        if values:
            combined[key] = min(values)
    return combined


async def position_quantity(adapter, symbol: str, *, market_type: str = "perp") -> Decimal:
    position = await adapter.get_open_position(symbol=symbol, market_type=market_type)
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
        "FILLED", "CANCELED", "CANCELLED", "PARTIALLY_CANCELED", "REJECTED", "EXPIRED",
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
    maker_market_type: str = "perp", taker_market_type: str = "perp",
) -> None:
    direction = Decimal("1") if maker_side.upper() == "BUY" else Decimal("-1")
    expected_maker = maker_baseline + direction * quantity
    expected_taker = taker_baseline - direction * quantity
    last = None
    for attempt in range(max(1, attempts)):
        try:
            maker, taker = await asyncio.gather(
                position_quantity(maker_adapter, symbol, market_type=maker_market_type),
                position_quantity(taker_adapter, symbol, market_type=taker_market_type),
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
    market_type: str = "perp", retry_delay: float = 0.0,
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
    def completed(*, source=None):
        # Import lazily to share the strict fill-price parser without a module cycle.
        from hydra_basis.execution_engine.executor import extract_actual_average_price
        total_quote, priced_quantity = Decimal(0), Decimal(0)
        timings = []
        for record in attempts:
            fill = record.get("order_status") or record.get("result")
            qty = terminal_fill_quantity(fill)
            avg = extract_actual_average_price(fill)
            if qty is not None and qty > 0 and avg is not None:
                total_quote += qty * avg
                priced_quantity += qty
            timing = (record.get("result") or {}).get("request_timing")
            if isinstance(timing, dict):
                timings.append(timing)
        result = {"ok": True, "terminal": True, "filled_quantity": str(quantity),
                  "aggregate_fill": True, "attempts": attempts,
                  "first_dispatch_ns": attempts[0]["dispatch_ns"],
                  "request_timings": timings}
        sources = [(record.get("order_status") or record.get("result") or {}).get("source", "rest")
                   for record in attempts]
        result["source"] = source or (sources[0] if len(set(sources)) == 1 else "mixed")
        if priced_quantity == quantity:
            result["avg_price"] = str(total_quote / quantity)
        if timings:
            result["request_timing"] = {
                "order_submit_ms": sum(t.get("order_submit_ms", 0) for t in timings),
                "post_order_status_ms": sum(t.get("post_order_status_ms", 0) for t in timings),
                "status_query_performed": any(t.get("status_query_performed") for t in timings),
            }
            if "order_submit_started_ns" in timings[0]:
                result["first_submit_ns"] = timings[0]["order_submit_started_ns"]
        return result

    for attempt in range(max(1, max_attempts)):
        remaining = quantity - confirmed
        kwargs = dict(symbol=symbol, side=side, amount=str(remaining),
                      clip_usd=clip_usd * float(remaining / quantity))
        if reduce_only:
            kwargs["reduce_only"] = True
        result = None
        error = None
        dispatch_ns = time.monotonic_ns()
        try:
            result = await adapter.place_market_order(**kwargs)
        except Exception as exc:
            error = str(exc)
            result = getattr(exc, "order_result", None)
        record = {"amount": str(remaining), "result": result, "error": error, "dispatch_ns": dispatch_ns}
        attempts.append(record)
        terminal_quantity = terminal_fill_quantity(result)
        if terminal_quantity is None:
            query = getattr(adapter, "get_order_execution", None)
            # Keep the submitted identity: status responses need not contain it.
            async def reconcile_rest():
                for check in range(max(1, confirmation_attempts)):
                    if callable(query) and isinstance(result, dict):
                        try:
                            status = await query(order_result=result, symbol=symbol)
                            record["order_status"] = status
                            qty = terminal_fill_quantity(status)
                            if qty is not None:
                                return {"status": status, "quantity": qty}
                        except Exception as exc:
                            record["status_error"] = str(exc)
                    try:
                        delta = (await position_quantity(adapter, symbol, market_type=market_type) - baseline) * direction
                        record["live_filled_quantity"] = str(delta)
                        if delta == quantity:
                            return {"position_complete": True}
                        if delta < 0 or delta > quantity:
                            record["position_error"] = "unexpected position movement"
                            break
                    except Exception as exc:
                        record["position_error"] = str(exc)
                    if check + 1 < confirmation_attempts:
                        await asyncio.sleep(poll_delay)
                return {}

            waiter = getattr(adapter, "wait_for_hedge_terminal", None)
            async def reconcile_stream():
                status = await waiter(order_result=result, symbol=symbol)
                qty = terminal_fill_quantity(status)
                if qty is None:
                    raise RuntimeError("hedge stream returned nonterminal status")
                return {"status": status, "quantity": qty}

            if callable(waiter) and isinstance(result, dict):
                resolution = await race_order_confirmation(reconcile_rest, reconcile_stream())
            else:
                resolution = await reconcile_rest()
            if resolution.get("position_complete"):
                return completed(source="live_position_delta")
            if "status" in resolution:
                record["order_status"] = resolution["status"]
                terminal_quantity = resolution["quantity"]
        if terminal_quantity is not None:
            if terminal_quantity > remaining:
                raise HedgeFailure("hedge overfilled requested remainder", confirmed=confirmed,
                                   uncertain=True, attempts=attempts)
            confirmed += terminal_quantity
            if confirmed == quantity:
                return completed()
            print(f"[hedge-reconcile] terminal fill={terminal_quantity} remaining={quantity - confirmed}", flush=True)
        else:
            raise HedgeFailure(
                f"hedge outcome unknown for {symbol}; confirmed={confirmed} target={quantity}; "
                f"observed_position_fill={record.get('live_filled_quantity', 'unavailable')}; "
                "refusing duplicate submission while the order may still fill",
                confirmed=confirmed, uncertain=True, attempts=attempts,
            )
        if attempt + 1 < max_attempts:
            await asyncio.sleep(retry_delay)
    raise HedgeFailure(
        f"hedge incomplete for {symbol}; confirmed={confirmed} remaining={quantity - confirmed}",
        confirmed=confirmed, uncertain=False, attempts=attempts,
    )
