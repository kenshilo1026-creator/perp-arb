from __future__ import annotations

from pathlib import Path
import time
from typing import Protocol

import aiohttp

from hydra_basis.adapters.registry import FETCHERS, FETCHERS_SINCE
from hydra_basis.risk_management.funding_risk import (
    FundingRiskConfig,
    FundingRiskManager,
    FundingRiskState,
    FundingSettlement,
    ProjectedFundingRate,
)
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.risk_management.manager import EmergencyRiskManager, PositionCloser
from hydra_basis.risk_management.registry import PositionRegistry


class FundingRiskDataProvider(Protocol):
    async def fetch_settlements(
        self,
        registry: PositionRegistry,
        state: FundingRiskState,
    ) -> list[FundingSettlement]:
        ...

    async def fetch_projected_rates(
        self,
        registry: PositionRegistry,
    ) -> dict[str, list[ProjectedFundingRate]]:
        ...


class FundingHistoryRiskDataProvider:
    def __init__(self, *, session: aiohttp.ClientSession) -> None:
        self.session = session
        self._latest_rates_by_strategy: dict[str, list[ProjectedFundingRate]] = {}

    async def fetch_settlements(
        self,
        registry: PositionRegistry,
        state: FundingRiskState,
    ) -> list[FundingSettlement]:
        settlements: list[FundingSettlement] = []
        latest_rates_by_strategy: dict[str, list[ProjectedFundingRate]] = {}
        for strategy_id in registry.open_strategy_ids():
            for leg in registry.legs_for_strategy(strategy_id):
                if leg.status != "open" or leg.market_type == "spot":
                    continue
                points = await self._fetch_recent_points(leg, state)
                latest_point = points[-1] if points else None
                latest_state_row = state.settlements_by_leg.get(leg.leg_id, [])[-1:] or []
                latest = latest_point or (latest_state_row[0] if latest_state_row else None)
                if latest is not None:
                    latest_rates_by_strategy.setdefault(strategy_id, []).append(
                        ProjectedFundingRate(
                            leg_id=leg.leg_id,
                            funding_rate=latest.raw_rate if isinstance(latest, FundingPoint) else latest.funding_rate,
                            interval_hours=latest.interval_hours,
                        )
                    )
                for point in points:
                    settlements.append(
                        FundingSettlement(
                            leg_id=leg.leg_id,
                            settlement_ts_ms=point.ts_ms,
                            funding_rate=point.raw_rate,
                            interval_hours=point.interval_hours,
                        )
                    )
        self._latest_rates_by_strategy = latest_rates_by_strategy
        return settlements

    async def fetch_projected_rates(
        self,
        registry: PositionRegistry,
    ) -> dict[str, list[ProjectedFundingRate]]:
        return self._latest_rates_by_strategy

    async def _fetch_recent_points(
        self,
        leg,
        state: FundingRiskState,
    ) -> list[FundingPoint]:
        venue = leg.venue.strip().lower()
        rows = state.settlements_by_leg.get(leg.leg_id, [])
        start_time_ms = rows[-1].settlement_ts_ms + 1 if rows else int((time.time() - 48 * 3600) * 1000)
        if venue in FETCHERS_SINCE:
            points = await FETCHERS_SINCE[venue](self.session, leg.symbol, start_time_ms)
            return sorted(points, key=lambda point: point.ts_ms)
        if venue not in FETCHERS:
            return []
        points = await FETCHERS[venue](self.session, leg.symbol)
        return sorted([point for point in points if point.ts_ms >= start_time_ms], key=lambda point: point.ts_ms)


async def process_funding_risk_once(
    *,
    registry_path: Path,
    state_path: Path,
    provider: FundingRiskDataProvider,
    closers: dict[str, PositionCloser],
    config: FundingRiskConfig,
    dry_run: bool,
) -> dict[str, object]:
    registry = PositionRegistry.load(registry_path)
    state = FundingRiskState.load(state_path)
    manager = FundingRiskManager(registry=registry, state=state, config=config)
    emergency_manager = EmergencyRiskManager(registry=registry, closers=closers, dry_run=dry_run)

    messages: list[str] = []
    auto_close_results: list[dict[str, object]] = []

    settlement_result = manager.ingest_settlements(await provider.fetch_settlements(registry, state))
    if settlement_result.get("action") == "notify_consecutive_negative":
        messages.append(
            "funding risk: consecutive negative funding windows "
            f"strategy={settlement_result.get('strategy_id', '')} "
            f"net={float(settlement_result.get('net_cashflow_pct', 0.0)):.6%}"
        )
    if settlement_result.get("risk_event") is not None:
        auto_close_results.append(await emergency_manager.handle_event(settlement_result["risk_event"]))

    projected_rates_by_strategy = await provider.fetch_projected_rates(registry)
    for strategy_id, rates in projected_rates_by_strategy.items():
        result = manager.evaluate_projection(strategy_id=strategy_id, rates=rates)
        if result.get("risk_event") is not None:
            messages.append(
                "funding risk: projected negative funding auto close "
                f"strategy={strategy_id} net={float(result.get('net_cashflow_pct', 0.0)):.6%}"
            )
            auto_close_results.append(await emergency_manager.handle_event(result["risk_event"]))

    registry.save(registry_path)
    state.save(state_path)
    return {
        "ok": all(result.get("ok", False) for result in auto_close_results) if auto_close_results else True,
        "messages": messages,
        "auto_close_results": auto_close_results,
        "settlement_result": settlement_result,
    }
