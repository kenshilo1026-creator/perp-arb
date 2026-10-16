"""Check listen-key creation and the private-order subscription without trading.

Run from the repository root: python -m scripts.check_mexc_order_stream
"""
from __future__ import annotations

import asyncio

from hydra_basis.env import load_environment
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.order_stream import format_stream_error


async def main() -> int:
    load_environment()
    adapter = MexcSpotExecutionAdapter()
    try:
        await adapter.prepare_maker_fill_stream()
        await asyncio.sleep(1)
        if adapter._fill_stream is None or not adapter._fill_stream.healthy:
            print("FAIL: private order stream disconnected after subscription")
            return 1
        print("PASS: MEXC listen key created; private order subscription acknowledged")
        print("No orders submitted. Closing the diagnostic connection and its listen key.")
        return 0
    except Exception as exc:
        print(f"FAIL: {format_stream_error(exc)}")
        return 1
    finally:
        await adapter.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
