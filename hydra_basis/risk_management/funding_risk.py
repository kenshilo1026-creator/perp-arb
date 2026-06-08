from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from hydra_basis.risk_management.margin_topup import _strip_json_line_comments
from hydra_basis.risk_management.models import PositionLeg, PositionSide, RiskEvent
from hydra_basis.risk_management.registry import PositionRegistry


@dataclass
class FundingRiskConfig:
    enabled: bool
    check_interval_seconds: int
    consecutive_negative_windows: int
    auto_close_negative_funding_pct: float


@dataclass(frozen=True)
class FundingSettlement:
    leg_id: str
    settlement_ts_ms: int
    funding_rate: float
    interval_hours: float


@dataclass(frozen=True)
class ProjectedFundingRate:
    leg_id: str
    funding_rate: float
    interval_hours: float


@dataclass
class StrategyFundingRiskState:
    last_anchor_ts_ms: int | None = None
    consecutive_negative_windows: int = 0
    last_net_cashflow_pct: float = 0.0


@dataclass
class FundingRiskState:
    settlements_by_leg: dict[str, list[FundingSettlement]] = field(default_factory=dict)
    strategies: dict[str, StrategyFundingRiskState] = field(default_factory=dict)

    def add_settlement(self, settlement: FundingSettlement) -> None:
        rows = self.settlements_by_leg.setdefault(settlement.leg_id, [])
        if any(row.settlement_ts_ms == settlement.settlement_ts_ms for row in rows):
            return
        rows.append(settlement)
        rows.sort(key=lambda row: row.settlement_ts_ms)

    def strategy(self, strategy_id: str) -> StrategyFundingRiskState:
        return self.strategies.setdefault(strategy_id, StrategyFundingRiskState())

    def to_payload(self) -> dict[str, Any]:
        return {
            "settlements_by_leg": {
                leg_id: [asdict(row) for row in rows]
                for leg_id, rows in self.settlements_by_leg.items()
            },
            "strategies": {
                strategy_id: asdict(row)
                for strategy_id, row in self.strategies.items()
            },
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "FundingRiskState":
        state = cls()
        for leg_id, rows in payload.get("settlements_by_leg", {}).items():
            state.settlements_by_leg[leg_id] = [
                FundingSettlement(
                    leg_id=str(row["leg_id"]),
                    settlement_ts_ms=int(row["settlement_ts_ms"]),
                    funding_rate=float(row["funding_rate"]),
                    interval_hours=float(row["interval_hours"]),
                )
                for row in rows
            ]
        for strategy_id, row in payload.get("strategies", {}).items():
            state.strategies[strategy_id] = StrategyFundingRiskState(
                last_anchor_ts_ms=row.get("last_anchor_ts_ms"),
                consecutive_negative_windows=int(row.get("consecutive_negative_windows", 0)),
                last_net_cashflow_pct=float(row.get("last_net_cashflow_pct", 0.0)),
            )
        return state

    @classmethod
    def load(cls, path: Path) -> "FundingRiskState":
        if not path.exists():
            return cls()
        return cls.from_payload(json.loads(path.read_text(encoding="utf-8")))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_payload(), indent=2, sort_keys=True),
            encoding="utf-8",
        )


def load_funding_risk_config(path: Path) -> FundingRiskConfig:
    payload = json.loads(_strip_json_line_comments(path.read_text(encoding="utf-8")))
    return FundingRiskConfig(
        enabled=bool(payload["enabled"]),
        check_interval_seconds=int(payload["check_interval_seconds"]),
        consecutive_negative_windows=int(payload["consecutive_negative_windows"]),
        auto_close_negative_funding_pct=float(payload["auto_close_negative_funding_pct"]),
    )


def funding_cashflow_pct(*, side: PositionSide | str, funding_rate: float) -> float:
    normalized = side.strip().upper()
    if normalized == "SHORT":
        return funding_rate
    if normalized == "LONG":
        return -funding_rate
    raise RuntimeError(f"unsupported position side: {side}")


def _anchor_leg(legs: list[PositionLeg], interval_by_leg: dict[str, float]) -> PositionLeg | None:
    candidates = [leg for leg in legs if leg.leg_id in interval_by_leg]
    if not candidates:
        return None
    return max(candidates, key=lambda leg: interval_by_leg[leg.leg_id])


class FundingRiskManager:
    def __init__(
        self,
        *,
        registry: PositionRegistry,
        state: FundingRiskState,
        config: FundingRiskConfig,
    ) -> None:
        self.registry = registry
        self.state = state
        self.config = config

    def ingest_settlements(self, settlements: list[FundingSettlement]) -> dict[str, Any]:
        if not self.config.enabled:
            return {"ok": True, "action": "disabled"}
        for settlement in settlements:
            self.state.add_settlement(settlement)

        results = [
            result
            for strategy_id in self._open_strategy_ids()
            for result in [self._evaluate_settled_strategy(strategy_id)]
            if result["action"] != "no_new_anchor_settlement"
        ]
        if not results:
            return {"ok": True, "action": "no_new_anchor_settlement"}
        return results[-1]

    def evaluate_projection(
        self,
        *,
        strategy_id: str,
        rates: list[ProjectedFundingRate],
    ) -> dict[str, Any]:
        if not self.config.enabled:
            return {"ok": True, "action": "disabled"}
        legs = self._open_legs_for_strategy(strategy_id)
        rate_by_leg = {rate.leg_id: rate for rate in rates}
        interval_by_leg = {rate.leg_id: rate.interval_hours for rate in rates}
        anchor = _anchor_leg(legs, interval_by_leg)
        if anchor is None:
            return {"ok": True, "action": "missing_funding_rates"}
        anchor_interval = interval_by_leg[anchor.leg_id]
        net_cashflow = 0.0
        for leg in legs:
            if leg.market_type == "spot":
                continue
            rate = rate_by_leg.get(leg.leg_id)
            if rate is None:
                return {"ok": True, "action": "missing_funding_rates"}
            multiplier = anchor_interval / rate.interval_hours
            net_cashflow += funding_cashflow_pct(side=leg.side, funding_rate=rate.funding_rate) * multiplier

        threshold = self.config.auto_close_negative_funding_pct / 100
        if net_cashflow < -threshold:
            return {
                "ok": False,
                "action": "auto_close_projected_negative_funding",
                "net_cashflow_pct": net_cashflow,
                "risk_event": self._risk_event_for_strategy(strategy_id, anchor.leg_id, "projected funding loss exceeded threshold"),
            }
        return {"ok": True, "action": "projected_funding_ok", "net_cashflow_pct": net_cashflow}

    def _evaluate_settled_strategy(self, strategy_id: str) -> dict[str, Any]:
        legs = self._open_legs_for_strategy(strategy_id)
        interval_by_leg = self._latest_interval_by_leg(legs)
        anchor = _anchor_leg(legs, interval_by_leg)
        if anchor is None:
            return {"ok": True, "action": "missing_settlements"}

        strategy_state = self.state.strategy(strategy_id)
        anchor_rows = [
            row
            for row in self.state.settlements_by_leg.get(anchor.leg_id, [])
            if strategy_state.last_anchor_ts_ms is None or row.settlement_ts_ms > strategy_state.last_anchor_ts_ms
        ]
        if not anchor_rows:
            return {"ok": True, "action": "no_new_anchor_settlement"}

        result: dict[str, Any] = {"ok": True, "action": "no_new_anchor_settlement"}
        for anchor_row in anchor_rows:
            result = self._evaluate_anchor_window(strategy_id, legs, anchor_row)
        return result

    def _evaluate_anchor_window(
        self,
        strategy_id: str,
        legs: list[PositionLeg],
        anchor_row: FundingSettlement,
    ) -> dict[str, Any]:
        strategy_state = self.state.strategy(strategy_id)
        interval_ms = int(anchor_row.interval_hours * 3_600_000)
        window_start = strategy_state.last_anchor_ts_ms
        if window_start is None:
            window_start = anchor_row.settlement_ts_ms - interval_ms

        net_cashflow = 0.0
        for leg in legs:
            if leg.market_type == "spot":
                continue
            rows = [
                row
                for row in self.state.settlements_by_leg.get(leg.leg_id, [])
                if window_start < row.settlement_ts_ms <= anchor_row.settlement_ts_ms
            ]
            if not rows:
                return {"ok": True, "action": "missing_window_settlements"}
            net_cashflow += sum(
                funding_cashflow_pct(side=leg.side, funding_rate=row.funding_rate)
                for row in rows
            )

        strategy_state.last_anchor_ts_ms = anchor_row.settlement_ts_ms
        strategy_state.last_net_cashflow_pct = net_cashflow
        if net_cashflow < 0:
            strategy_state.consecutive_negative_windows += 1
        else:
            strategy_state.consecutive_negative_windows = 0

        threshold = self.config.auto_close_negative_funding_pct / 100
        if net_cashflow < -threshold:
            return {
                "ok": False,
                "action": "auto_close_settled_negative_funding",
                "strategy_id": strategy_id,
                "net_cashflow_pct": net_cashflow,
                "consecutive_negative_windows": strategy_state.consecutive_negative_windows,
                "risk_event": self._risk_event_for_strategy(strategy_id, anchor_row.leg_id, "settled funding loss exceeded threshold"),
            }
        if strategy_state.consecutive_negative_windows >= self.config.consecutive_negative_windows:
            return {
                "ok": True,
                "action": "notify_consecutive_negative",
                "strategy_id": strategy_id,
                "net_cashflow_pct": net_cashflow,
                "consecutive_negative_windows": strategy_state.consecutive_negative_windows,
            }
        if net_cashflow < 0:
            return {
                "ok": True,
                "action": "negative_window_recorded",
                "strategy_id": strategy_id,
                "net_cashflow_pct": net_cashflow,
                "consecutive_negative_windows": strategy_state.consecutive_negative_windows,
            }
        return {
            "ok": True,
            "action": "funding_window_positive",
            "strategy_id": strategy_id,
            "net_cashflow_pct": net_cashflow,
            "consecutive_negative_windows": strategy_state.consecutive_negative_windows,
        }

    def _open_strategy_ids(self) -> list[str]:
        return self.registry.open_strategy_ids()

    def _open_legs_for_strategy(self, strategy_id: str) -> list[PositionLeg]:
        return [
            leg
            for leg in self.registry.legs_for_strategy(strategy_id)
            if leg.status == "open"
        ]

    def _latest_interval_by_leg(self, legs: list[PositionLeg]) -> dict[str, float]:
        intervals: dict[str, float] = {}
        for leg in legs:
            rows = self.state.settlements_by_leg.get(leg.leg_id, [])
            if rows:
                intervals[leg.leg_id] = rows[-1].interval_hours
        return intervals

    def _risk_event_for_strategy(self, strategy_id: str, trigger_leg_id: str, message: str) -> RiskEvent:
        leg = self.registry.get_leg(trigger_leg_id)
        return RiskEvent(
            strategy_id=strategy_id,
            leg_id=trigger_leg_id,
            venue=leg.venue,
            symbol=leg.symbol,
            event_type="FUNDING_AUTO_CLOSE",
            message=message,
        )
