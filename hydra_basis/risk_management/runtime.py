from __future__ import annotations

from pathlib import Path

from hydra_basis.risk_management.manager import EmergencyRiskManager, PositionCloser
from hydra_basis.risk_management.registry import PositionRegistry
from hydra_basis.risk_management.watchers import RiskEventWatcher, consume_watcher_events


async def process_watcher_once(
    *,
    registry_path: Path,
    watcher: RiskEventWatcher,
    closers: dict[str, PositionCloser],
    dry_run: bool = False,
) -> dict[str, object]:
    registry = PositionRegistry.load(registry_path)
    manager = EmergencyRiskManager(registry=registry, closers=closers, dry_run=dry_run)
    async for event in consume_watcher_events(registry=registry, watcher=watcher):
        result = await manager.handle_event(event)
        registry.save(registry_path)
        return result
    return {"ok": True, "closed_leg_ids": [], "failed_leg_ids": [], "reason": "no_events"}


async def run_risk_watchers(
    *,
    registry_path: Path,
    watchers: list[RiskEventWatcher],
    closers: dict[str, PositionCloser],
    dry_run: bool = False,
) -> None:
    # A long-running process should keep each venue stream in its own task. This
    # helper stays simple so tests and scripts can decide retry/reconnect policy.
    for watcher in watchers:
        while True:
            await process_watcher_once(
                registry_path=registry_path,
                watcher=watcher,
                closers=closers,
                dry_run=dry_run,
            )
