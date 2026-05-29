from __future__ import annotations

import asyncio
from typing import Awaitable, TypeVar


T = TypeVar("T")


async def gather_limited(
    awaitables: list[Awaitable[T]],
    limit: int,
    *,
    return_exceptions: bool = False,
) -> list[T]:
    semaphore = asyncio.Semaphore(limit)

    async def run_with_limit(awaitable: Awaitable[T]) -> T:
        async with semaphore:
            return await awaitable

    return await asyncio.gather(
        *(run_with_limit(awaitable) for awaitable in awaitables),
        return_exceptions=return_exceptions,
    )
