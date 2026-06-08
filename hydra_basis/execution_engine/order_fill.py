from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable


FILLED_STATUSES = {"FILLED", "FILLED_ALL", "EXECUTED", "CLOSED"}
OPEN_STATUSES = {"NEW", "OPEN", "PARTIALLY_FILLED", "PENDING", "UNTRIGGERED"}


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


async def poll_until_filled(
    *,
    fetch_status: Callable[[], Awaitable[dict[str, object]]],
    timeout_seconds: float,
    poll_interval_seconds: float,
    timeout_message: str,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    last_status: dict[str, object] | None = None

    while True:
        last_status = await fetch_status()
        if status_dict_looks_filled(last_status):
            return {"ok": True, "raw": last_status}
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{timeout_message}: last_status={last_status}")
        await asyncio.sleep(max(0.0, poll_interval_seconds))
