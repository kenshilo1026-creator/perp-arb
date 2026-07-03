from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any


LoopFactory = Callable[[], Awaitable[Any]]
ErrorHandler = Callable[[str, BaseException], Any]


async def supervise(
    *,
    name: str,
    loop_factory: LoopFactory,
    on_error: ErrorHandler | None = None,
    restart_delay_seconds: float = 30.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Run ``loop_factory`` forever, restarting it if it raises.

    The risk manager runs several independent loops under one ``asyncio.gather``.
    Without supervision, a single unhandled exception in any loop propagates
    through ``gather`` and cancels *all* of them, silently taking down every
    watcher. ``supervise`` isolates each loop: a crash is logged, reported via
    ``on_error`` (e.g. a Telegram alert), and the loop is restarted after a
    backoff instead of bringing the whole process down.

    ``CancelledError`` is always propagated so normal shutdown still works.
    """
    while True:
        try:
            await loop_factory()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 - supervisor must catch everything
            print(
                f"risk manager loop {name!r} crashed: {exc!r}; "
                f"restarting in {restart_delay_seconds:.0f}s",
                flush=True,
            )
            if on_error is not None:
                try:
                    result = on_error(name, exc)
                    if inspect.isawaitable(result):
                        await result
                except Exception as callback_exc:  # noqa: BLE001
                    print(
                        f"risk manager loop {name!r} on_error callback failed: {callback_exc!r}",
                        flush=True,
                    )
            await sleep(restart_delay_seconds)
