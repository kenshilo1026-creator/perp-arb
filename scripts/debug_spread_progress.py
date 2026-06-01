from __future__ import annotations

import asyncio
from pathlib import Path

import aiohttp

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.spread_monitor.config import load_spread_monitor_config
from hydra_basis.spread_monitor.runtime import close_runners, pump_runner_forever, start_spread_monitor_runners
from hydra_basis.streams.manager import MarketStateStore


async def main() -> None:
    configure_windows_event_loop_policy()
    config = load_spread_monitor_config(Path("configs/spread_monitor.jsonc"))
    state_store = MarketStateStore()
    async with aiohttp.ClientSession(headers={"User-Agent": "spread-progress-debug/0.1"}) as session:
        runners = await start_spread_monitor_runners(
            session=session,
            state_store=state_store,
            venues=config.venues,
        )
        tasks = {
            venue: asyncio.create_task(pump_runner_forever(venue, runner), name=venue)
            for venue, runner in runners.items()
        }
        try:
            for tick in range(1, 11):
                await asyncio.sleep(2)
                print(f"tick={tick}", flush=True)
                print({name: task.done() for name, task in tasks.items()}, flush=True)
                for venue in config.venues:
                    print(
                        venue,
                        "asset_ctx",
                        len(state_store.get_asset_ctx_snapshot(venue)),
                        "quotes",
                        len(state_store.get_quote_snapshot(venue)),
                        flush=True,
                    )
                for name, task in tasks.items():
                    if task.done():
                        try:
                            await task
                        except Exception as exc:
                            print(f"task_error {name}: {exc!r}", flush=True)
        finally:
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            await close_runners(runners)


if __name__ == "__main__":
    asyncio.run(main())
