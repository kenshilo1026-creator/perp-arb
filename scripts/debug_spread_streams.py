from __future__ import annotations

import asyncio
import json

import aiohttp

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_universe
from hydra_basis.adapters.lighter import fetch_lighter_market_map
from hydra_basis.adapters.mexc import list_symbols as list_mexc_symbols
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.spread_monitor.runtime import AsterQuoteRunner, HyperliquidQuoteRunner, LighterQuoteRunner
from hydra_basis.streams.manager import (
    AsterStreamRunner,
    HyperliquidStreamRunner,
    LighterStreamRunner,
    MarketStateStore,
    MexcStreamRunner,
)


def preview(payload) -> str:
    try:
        text = json.dumps(payload)
    except TypeError:
        text = repr(payload)
    return text[:400]


async def main() -> None:
    configure_windows_event_loop_policy()
    async with aiohttp.ClientSession(headers={"User-Agent": "spread-stream-debug/0.1"}) as session:
        state = MarketStateStore()
        hl_symbols = await fetch_hyperliquid_universe(session)
        lighter_market_map = await fetch_lighter_market_map(session)
        mexc_symbols = sorted(await list_mexc_symbols(session))

        runners = [
            ("hyperliquid:funding", HyperliquidStreamRunner(session, state, hl_symbols)),
            ("hyperliquid:quote", HyperliquidQuoteRunner(session, state, hl_symbols)),
            ("lighter:funding", LighterStreamRunner(session, state)),
            ("lighter:quote", LighterQuoteRunner(session, state, lighter_market_map)),
            ("aster:funding", AsterStreamRunner(session, state)),
            ("aster:quote", AsterQuoteRunner(session, state)),
            ("mexc", MexcStreamRunner(session, state)),
        ]

        for name, runner in runners:
            print(f"start {name}", flush=True)
            await asyncio.wait_for(runner.initialize(), timeout=20)
            if name == "mexc":
                await asyncio.wait_for(runner.subscribe_symbols(mexc_symbols), timeout=20)
            payload = await asyncio.wait_for(runner.pump_once(), timeout=20)
            print(f"first message {name}: {preview(payload)}", flush=True)
            print(
                f"state counts {name}: "
                f"hl_ctx={len(state.get_asset_ctx_snapshot('hyperliquid'))} "
                f"hl_quotes={len(state.get_quote_snapshot('hyperliquid'))} "
                f"lighter_ctx={len(state.get_asset_ctx_snapshot('lighter'))} "
                f"lighter_quotes={len(state.get_quote_snapshot('lighter'))} "
                f"aster_ctx={len(state.get_asset_ctx_snapshot('aster'))} "
                f"aster_quotes={len(state.get_quote_snapshot('aster'))} "
                f"mexc_ctx={len(state.get_asset_ctx_snapshot('mexc'))} "
                f"mexc_quotes={len(state.get_quote_snapshot('mexc'))}",
                flush=True,
            )
            await runner.close()


if __name__ == "__main__":
    asyncio.run(main())
