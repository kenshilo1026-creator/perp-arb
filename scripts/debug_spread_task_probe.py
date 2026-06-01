from __future__ import annotations

import asyncio
from pathlib import Path

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.spread_monitor.config import load_spread_monitor_config
from hydra_basis.spread_monitor.runtime import close_runners, pump_runner_forever, start_spread_monitor_runners
from hydra_basis.streams.manager import MarketStateStore


load_environment()


async def main() -> None:
    config = load_spread_monitor_config(Path("configs/spread_monitor.jsonc"))
    state_store = MarketStateStore()
    async with aiohttp.ClientSession(headers={"User-Agent": "spread-task-probe/0.1"}) as session:
        runners = await start_spread_monitor_runners(
            session=session,
            state_store=state_store,
            venues=config.venues,
        )
        print(f"runners={list(runners.keys())}", flush=True)
        tasks = {
            name: asyncio.create_task(pump_runner_forever(name, runner), name=name)
            for name, runner in runners.items()
        }
        try:
            for delay_seconds in (1, 3, 6, 10):
                await asyncio.sleep(delay_seconds)
                print(f"--- after {delay_seconds}s ---", flush=True)
                for name, task in tasks.items():
                    state = "done" if task.done() else "pending"
                    print(f"{name} {state}", flush=True)
                    if task.done():
                        try:
                            await task
                        except Exception as exc:
                            print(f"task_error {name}: {exc!r}", flush=True)
                for venue in config.venues:
                    print(
                        f"{venue} asset_ctx={len(state_store.get_asset_ctx_snapshot(venue))} "
                        f"quotes={len(state_store.get_quote_snapshot(venue))}",
                        flush=True,
                    )
        finally:
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            await close_runners(runners)


if __name__ == "__main__":
    configure_windows_event_loop_policy()
    asyncio.run(main())
