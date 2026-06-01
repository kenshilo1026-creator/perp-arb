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
from hydra_basis.notifications.telegram import send_telegram
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.spread_monitor.config import load_spread_monitor_config
from hydra_basis.spread_monitor.engine import build_symbol_snapshots, select_best_spread_from_state
from hydra_basis.spread_monitor.formatting import (
    format_spread_live_status_console,
    format_spread_opportunity,
    format_spread_opportunity_console,
    format_spread_progress_console,
)
from hydra_basis.spread_monitor.runtime import close_runners, pump_runner_forever, start_spread_monitor_runners
from hydra_basis.streams.manager import MarketStateStore, now_ms


load_environment()

SPREAD_MONITOR_CONFIG_PATH = Path("configs/spread_monitor.jsonc")
SPREAD_MONITOR_POLL_SECONDS = 1.0
SPREAD_MONITOR_DEBUG_LIMIT = 20
TELEGRAM_SEND_TIMEOUT_SECONDS = 10
SPREAD_MONITOR_HEARTBEAT_SECONDS = 10.0
SPREAD_MONITOR_REALERT_DELTA_PCT = 0.1


def emit_console(message: str) -> None:
    print(message.encode("ascii", "backslashreplace").decode("ascii"), flush=True)


async def push_telegram(message: str, *, label: str) -> None:
    try:
        await asyncio.wait_for(send_telegram(message), timeout=TELEGRAM_SEND_TIMEOUT_SECONDS)
    except Exception as exc:
        emit_console(f"telegram send failed | {label} | error={exc!r}")


def schedule_telegram(
    pending_tasks: set[asyncio.Task],
    message: str,
    *,
    label: str,
) -> None:
    task = asyncio.create_task(push_telegram(message, label=label), name=f"telegram-{label}")
    pending_tasks.add(task)

    def _cleanup(done_task: asyncio.Task) -> None:
        pending_tasks.discard(done_task)

    task.add_done_callback(_cleanup)


async def run_spread_monitor() -> None:
    config = load_spread_monitor_config(SPREAD_MONITOR_CONFIG_PATH)
    state_store = MarketStateStore()
    last_rendered: str | None = None
    last_progress_line: str | None = None
    last_progress_emit_at = 0.0
    pending_telegram_tasks: set[asyncio.Task] = set()
    last_live_status_by_venue: dict[str, tuple[int, int]] = {}
    last_alert_spread_by_key: dict[tuple[str, str, str], float] = {}

    async with aiohttp.ClientSession(headers={"User-Agent": "spread-monitor/0.1"}) as session:
        runners = await start_spread_monitor_runners(
            session=session,
            state_store=state_store,
            venues=config.venues,
        )
        emit_console(f"spread monitor ready: {', '.join(runners)}")
        await push_telegram("價差監控已啟動", label="startup")
        pump_tasks = [
            asyncio.create_task(pump_runner_forever(venue, runner), name=f"spread-pump-{venue}")
            for venue, runner in runners.items()
        ] 

        try:
            loop = asyncio.get_running_loop()
            while True:
                opportunity = select_best_spread_from_state(
                    config=config,
                    market_state_store=state_store,
                    now_ms=now_ms(),
                )
                rendered_console = (
                    "spread monitor running: waiting for qualifying live opportunity"
                    if opportunity is None
                    else format_spread_opportunity_console(opportunity)
                )
                if rendered_console != last_rendered:
                    emit_console(rendered_console)
                    last_rendered = rendered_console
                if opportunity is not None:
                    telegram_key = (
                        opportunity.symbol,
                        opportunity.short_venue,
                        opportunity.long_venue,
                    )
                    previous_alert_spread = last_alert_spread_by_key.get(telegram_key)
                    min_realert_delta = SPREAD_MONITOR_REALERT_DELTA_PCT / 100
                    if (
                        previous_alert_spread is None
                        or abs(opportunity.cross_exchange_spread_pct - previous_alert_spread) >= min_realert_delta
                    ):
                        schedule_telegram(
                            pending_telegram_tasks,
                            format_spread_opportunity(opportunity),
                            label="opportunity",
                        )
                        last_alert_spread_by_key[telegram_key] = opportunity.cross_exchange_spread_pct

                asset_ctx_snapshots_by_venue = {
                    venue: state_store.get_asset_ctx_snapshot(venue)
                    for venue in config.venues
                }
                quote_snapshots_by_venue = {
                    venue: state_store.get_quote_snapshot(venue)
                    for venue in config.venues
                }
                asset_ctx_counts = {
                    venue: len(asset_ctx_snapshots_by_venue[venue])
                    for venue in config.venues
                }
                quote_counts = {
                    venue: len(quote_snapshots_by_venue[venue])
                    for venue in config.venues
                }
                symbol_snapshots = build_symbol_snapshots(
                    venues=config.venues,
                    asset_ctx_snapshots_by_venue=asset_ctx_snapshots_by_venue,
                    quote_snapshots_by_venue=quote_snapshots_by_venue,
                )
                progress_line = format_spread_progress_console(
                    venues=config.venues,
                    asset_ctx_counts=asset_ctx_counts,
                    quote_counts=quote_counts,
                    shared_symbols=len(symbol_snapshots),
                )
                now_loop_time = loop.time()
                progress_changed = progress_line != last_progress_line
                heartbeat_due = (now_loop_time - last_progress_emit_at) >= SPREAD_MONITOR_HEARTBEAT_SECONDS
                if progress_changed or heartbeat_due:
                    emit_console(progress_line)
                    last_progress_line = progress_line
                    last_progress_emit_at = now_loop_time
                for venue in config.venues:
                    current_status = (asset_ctx_counts[venue], quote_counts[venue])
                    previous_status = last_live_status_by_venue.get(venue, (0, 0))
                    if current_status != previous_status and (
                        (previous_status[0] == 0 and current_status[0] > 0)
                        or (previous_status[1] == 0 and current_status[1] > 0)
                    ):
                        emit_console(
                            format_spread_live_status_console(
                                venue=venue,
                                asset_ctx_count=current_status[0],
                                quote_count=current_status[1],
                            )
                        )
                    last_live_status_by_venue[venue] = current_status

                finished = [task for task in pump_tasks if task.done()]
                if finished:
                    await finished[0]
                await asyncio.sleep(SPREAD_MONITOR_POLL_SECONDS)
        finally:
            for task in pump_tasks:
                task.cancel()
            await asyncio.gather(*pump_tasks, return_exceptions=True)
            if pending_telegram_tasks:
                await asyncio.gather(*pending_telegram_tasks, return_exceptions=True)
            await close_runners(runners)


def main() -> None:
    asyncio.run(run_spread_monitor())


if __name__ == "__main__":
    configure_windows_event_loop_policy()
    main()
