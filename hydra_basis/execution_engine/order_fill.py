from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Awaitable, Callable


FILLED_STATUSES = {"FILLED", "FILLED_ALL", "EXECUTED", "CLOSED"}
OPEN_STATUSES = {"NEW", "OPEN", "PARTIALLY_FILLED", "PENDING", "UNTRIGGERED"}
_FILLED_QUANTITY_KEYS = {
    "executedqty",
    "executed_qty",
    "cumqty",
    "cum_qty",
    "filledqty",
    "filled_qty",
    "filledbaseamount",
    "filled_base_amount",
    "remainingbaseamount",
    "cumfilledqty",
    "cum_filled_qty",
    "dealvol",
    "dealqty",
    "sizefilled",
    "filledsize",
    "filled_size",
}
_TERMINAL_QUANTITY_FALLBACK_KEYS = {
    "quantity",
    "qty",
    "origqty",
    "orig_qty",
    "size",
    "base_amount",
    "baseamount",
    "vol",
}


def status_text(value: object) -> str:
    return str(value or "").strip().upper()


def status_dict_looks_filled(payload: dict[str, object]) -> bool:
    for key in ("status", "state", "orderStatus"):
        value = payload.get(key)
        if status_text(value) in FILLED_STATUSES:
            return True
    state = payload.get("state")
    if state in {3, "3"}:
        return True
    order = payload.get("order")
    if isinstance(order, dict):
        return status_dict_looks_filled(order)
    return False


def _decimal_from_mapping(payload: dict[str, object], keys: set[str]) -> Decimal | None:
    for key, value in payload.items():
        normalized = str(key).replace("-", "_").lower()
        if normalized not in keys or value in (None, "", "0", 0):
            continue
        try:
            parsed = Decimal(str(value))
        except Exception:
            continue
        if parsed != 0:
            return abs(parsed)
    return None


def extract_filled_quantity(
    payload: object,
    *,
    allow_terminal_quantity_fallback: bool = False,
) -> Decimal | None:
    if isinstance(payload, dict):
        explicit = _decimal_from_mapping(payload, _FILLED_QUANTITY_KEYS)
        if explicit is not None:
            return explicit
        order = payload.get("order")
        if isinstance(order, dict):
            nested = extract_filled_quantity(
                order,
                allow_terminal_quantity_fallback=allow_terminal_quantity_fallback,
            )
            if nested is not None:
                return nested
        for value in payload.values():
            nested = extract_filled_quantity(
                value,
                allow_terminal_quantity_fallback=allow_terminal_quantity_fallback,
            )
            if nested is not None:
                return nested
        if allow_terminal_quantity_fallback and status_dict_looks_filled(payload):
            return _decimal_from_mapping(payload, _TERMINAL_QUANTITY_FALLBACK_KEYS)
    elif isinstance(payload, list):
        for item in payload:
            nested = extract_filled_quantity(
                item,
                allow_terminal_quantity_fallback=allow_terminal_quantity_fallback,
            )
            if nested is not None:
                return nested
    return None


async def poll_until_filled(
    *,
    fetch_status: Callable[[], Awaitable[dict[str, object]]],
    timeout_seconds: float,
    poll_interval_seconds: float,
    timeout_message: str,
    return_on_partial_fill: bool = False,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    last_status: dict[str, object] | None = None

    while True:
        last_status = await fetch_status()
        if status_dict_looks_filled(last_status):
            filled_quantity = extract_filled_quantity(last_status, allow_terminal_quantity_fallback=True)
            result = {"ok": True, "raw": last_status}
            if filled_quantity is not None:
                result["filled_quantity"] = format(filled_quantity.normalize(), "f")
            return result
        if return_on_partial_fill:
            partial_quantity = extract_filled_quantity(last_status)
            if partial_quantity is not None and partial_quantity > 0:
                return {
                    "ok": True,
                    "raw": last_status,
                    "partial": True,
                    "filled_quantity": format(partial_quantity.normalize(), "f"),
                }
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{timeout_message}: last_status={last_status}")
        await asyncio.sleep(max(0.0, poll_interval_seconds))
