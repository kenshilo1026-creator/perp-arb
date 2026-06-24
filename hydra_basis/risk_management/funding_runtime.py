from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import time
from collections.abc import Awaitable, Callable
from typing import Protocol

import aiohttp

from hydra_basis.adapters.registry import FETCHERS, FETCHERS_SINCE
from hydra_basis.adapters.aster import fetch_aster_current_funding
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_current_funding
from hydra_basis.adapters.lighter import fetch_lighter_current_funding
from hydra_basis.adapters.variational import fetch_variational_current_funding
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


CURRENT_FUNDING_FETCHERS = {
    "aster": fetch_aster_current_funding,
    "hyperliquid": fetch_hyperliquid_current_funding,
    "lighter": fetch_lighter_current_funding,
    "variational": fetch_variational_current_funding,
}
CURRENT_FUNDING_ONLY_VENUES = {"variational"}


class CurrentFundingCacheStore:
    def __init__(self, path: Path, *, lookback_days: int = 7) -> None:
        self.path = path
        self.lookback_ms = lookback_days * 24 * 60 * 60 * 1000

    def append(
        self,
        *,
        venue: str,
        symbol: str,
        funding_rate: float,
        interval_hours: float,
        ts_ms: int,
    ) -> None:
        payload = self.load()
        normalized_venue = venue.strip().lower()
        normalized_symbol = symbol.strip().upper()
        key = f"{normalized_venue}::{normalized_symbol}"
        cutoff_ms = ts_ms - self.lookback_ms
        rows = [
            row
            for row in payload.get(key, [])
            if int(row.get("ts_ms", 0)) >= cutoff_ms and int(row.get("ts_ms", 0)) != ts_ms
        ]
        rows.append(
            {
                "venue": normalized_venue,
                "symbol": normalized_symbol,
                "ts_ms": ts_ms,
                "date": dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "funding_rate": float(funding_rate),
                "interval_hours": float(interval_hours),
                "source": f"{normalized_venue}_current",
            }
        )
        payload[key] = sorted(rows, key=lambda row: int(row["ts_ms"]))
        self.save(payload)

    def load(self) -> dict[str, list[dict[str, object]]]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, payload: dict[str, list[dict[str, object]]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )


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
    def __init__(
        self,
        *,
        session: aiohttp.ClientSession,
        current_funding_cache_path: Path | None = None,
        current_funding_cache_lookback_days: int = 7,
        now_ms_func=None,
    ) -> None:
        self.session = session
        self._latest_rates_by_strategy: dict[str, list[ProjectedFundingRate]] = {}
        self.current_funding_cache = (
            CurrentFundingCacheStore(
                current_funding_cache_path,
                lookback_days=current_funding_cache_lookback_days,
            )
            if current_funding_cache_path is not None
            else None
        )
        self._now_ms_func = now_ms_func or (lambda: int(time.time() * 1000))

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
        rates_by_strategy = {
            strategy_id: list(rates)
            for strategy_id, rates in self._latest_rates_by_strategy.items()
        }
        for strategy_id in registry.open_strategy_ids():
            for leg in registry.legs_for_strategy(strategy_id):
                if leg.status != "open" or leg.market_type == "spot":
                    continue
                fetcher = CURRENT_FUNDING_FETCHERS.get(leg.venue.strip().lower())
                if fetcher is None:
                    continue
                try:
                    current = await fetcher(self.session, leg.symbol)
                except Exception as exc:
                    print(
                        f"funding risk current fetch skipped {(leg.venue, leg.symbol)}: {exc!r}",
                        flush=True,
                    )
                    continue
                projected = _projected_rate_for_leg(leg.leg_id, current)
                if projected is None:
                    continue
                if self.current_funding_cache is not None:
                    self.current_funding_cache.append(
                        venue=leg.venue,
                        symbol=leg.symbol,
                        funding_rate=projected.funding_rate,
                        interval_hours=projected.interval_hours,
                        ts_ms=int(self._now_ms_func()),
                    )
                rows = [
                    row
                    for row in rates_by_strategy.setdefault(strategy_id, [])
                    if row.leg_id != leg.leg_id
                ]
                rows.append(projected)
                rates_by_strategy[strategy_id] = rows
        return rates_by_strategy

    async def _fetch_recent_points(
        self,
        leg,
        state: FundingRiskState,
    ) -> list[FundingPoint]:
        venue = leg.venue.strip().lower()
        if venue in CURRENT_FUNDING_ONLY_VENUES:
            return []
        rows = state.settlements_by_leg.get(leg.leg_id, [])
        start_time_ms = rows[-1].settlement_ts_ms + 1 if rows else int((time.time() - 48 * 3600) * 1000)
        try:
            if venue in FETCHERS_SINCE:
                points = await FETCHERS_SINCE[venue](self.session, leg.symbol, start_time_ms)
                return sorted(points, key=lambda point: point.ts_ms)
            if venue not in FETCHERS:
                return []
            points = await FETCHERS[venue](self.session, leg.symbol)
            return sorted([point for point in points if point.ts_ms >= start_time_ms], key=lambda point: point.ts_ms)
        except Exception as exc:
            print(f"funding risk settlement fetch skipped {(leg.venue, leg.symbol)}: {exc!r}", flush=True)
            return []


def _projected_rate_for_leg(leg_id: str, current: object) -> ProjectedFundingRate | None:
    if current is None:
        return None
    if isinstance(current, ProjectedFundingRate):
        return ProjectedFundingRate(
            leg_id=leg_id,
            funding_rate=current.funding_rate,
            interval_hours=current.interval_hours,
        )
    if isinstance(current, dict):
        funding_rate = current.get("funding_rate")
        interval_hours = current.get("interval_hours")
        if funding_rate is None or interval_hours is None:
            return None
        return ProjectedFundingRate(
            leg_id=leg_id,
            funding_rate=float(funding_rate),
            interval_hours=float(interval_hours),
        )
    funding_rate = getattr(current, "funding_rate", None)
    interval_hours = getattr(current, "interval_hours", None)
    if funding_rate is None or interval_hours is None:
        return None
    return ProjectedFundingRate(
        leg_id=leg_id,
        funding_rate=float(funding_rate),
        interval_hours=float(interval_hours),
    )


async def process_funding_risk_once(
    *,
    registry_path: Path,
    state_path: Path,
    provider: FundingRiskDataProvider,
    closers: dict[str, PositionCloser],
    config: FundingRiskConfig,
    dry_run: bool,
    funding_auto_closer: Callable[..., Awaitable[dict[str, object]]] | None = None,
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
        if funding_auto_closer is not None and settlement_result["risk_event"].event_type == "FUNDING_AUTO_CLOSE":
            auto_close_results.append(
                await funding_auto_closer(registry=registry, event=settlement_result["risk_event"], dry_run=dry_run)
            )
        else:
            auto_close_results.append(await emergency_manager.handle_event(settlement_result["risk_event"]))

    projected_rates_by_strategy = await provider.fetch_projected_rates(registry)
    for strategy_id, rates in projected_rates_by_strategy.items():
        result = manager.evaluate_projection(strategy_id=strategy_id, rates=rates)
        if result.get("risk_event") is not None:
            messages.append(
                "funding risk: projected negative funding auto close "
                f"strategy={strategy_id} net={float(result.get('net_cashflow_pct', 0.0)):.6%}"
            )
            if funding_auto_closer is not None:
                auto_close_results.append(
                    await funding_auto_closer(registry=registry, event=result["risk_event"], dry_run=dry_run)
                )
            else:
                auto_close_results.append(await emergency_manager.handle_event(result["risk_event"]))

    registry.save(registry_path)
    state.save(state_path)
    return {
        "ok": all(result.get("ok", False) for result in auto_close_results) if auto_close_results else True,
        "messages": messages,
        "auto_close_results": auto_close_results,
        "settlement_result": settlement_result,
        "projected_rates_by_strategy": projected_rates_by_strategy,
    }
