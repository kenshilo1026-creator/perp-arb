from __future__ import annotations

from scripts.run_funding_monitor import run_once


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_once())
