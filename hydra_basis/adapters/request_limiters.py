from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar


T = TypeVar("T")

_venue_locks: dict[str, asyncio.Lock] = {}


async def run_serialized(
    venue: str,
    func: Callable[[], Awaitable[T]],
    *,
    delay_seconds: float,
) -> T:
    lock = _venue_locks.setdefault(venue, asyncio.Lock())
    async with lock:
        result = await func()
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
        return result
