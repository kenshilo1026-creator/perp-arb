from __future__ import annotations

import asyncio
import os
import sys


def configure_windows_event_loop_policy() -> None:
    if sys.platform != "win32":
        return
    if os.getenv("LORIS_USE_NODRIVER", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        return
    selector_policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if selector_policy is None:
        return
    asyncio.set_event_loop_policy(selector_policy())
