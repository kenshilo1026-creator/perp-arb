from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from hydra_basis.risk_management.models import PositionSide, RiskEvent
from hydra_basis.risk_management.registry import PositionRegistry


@dataclass
class MarginTopupConfig:
    enabled: bool
    liq_distance_trigger_pct: float
    topup_amount_usd: float
    max_topups_per_leg: int
    cooldown_seconds: int


@dataclass
class MarginHealthSnapshot:
    venue: str
    symbol: str
    leg_id: str
    mark_price: float
    liquidation_price: float
    side: PositionSide


@dataclass
class VenueMarginHealthSignal:
    venue: str
    symbol: str
    side: PositionSide
    mark_price: float
    liquidation_price: float


class MarginTopupper(Protocol):
    async def add_isolated_margin(self, **kwargs) -> dict:
        ...


class MarginHealthWatcher(Protocol):
    async def watch(self):
        ...


def _strip_json_line_comments(text: str) -> str:
    lines: list[str] = []
    for line in text.splitlines():
        comment_index = line.find("//")
        if comment_index >= 0:
            line = line[:comment_index]
        lines.append(line)
    return "\n".join(lines)


def load_margin_topup_config(path: Path) -> MarginTopupConfig:
    payload = json.loads(_strip_json_line_comments(path.read_text(encoding="utf-8")))
    return MarginTopupConfig(
        enabled=bool(payload["enabled"]),
        liq_distance_trigger_pct=float(payload["liq_distance_trigger_pct"]),
        topup_amount_usd=float(payload["topup_amount_usd"]),
        max_topups_per_leg=int(payload["max_topups_per_leg"]),
        cooldown_seconds=int(payload["cooldown_seconds"]),
    )


def liquidation_distance_pct(*, side: str, mark_price: float, liquidation_price: float) -> float:
    if mark_price <= 0:
        raise RuntimeError("mark_price must be positive")
    if liquidation_price <= 0:
        return float("inf")
    normalized = side.strip().upper()
    if normalized == "LONG":
        return ((mark_price - liquidation_price) / mark_price) * 100
    if normalized == "SHORT":
        return ((liquidation_price - mark_price) / mark_price) * 100
    raise RuntimeError(f"unsupported position side: {side}")


def now_ms() -> int:
    return int(time.time() * 1000)


def build_snapshots_for_signal(
    *,
    registry: PositionRegistry,
    signal: VenueMarginHealthSignal,
) -> list[MarginHealthSnapshot]:
    return [
        MarginHealthSnapshot(
            venue=leg.venue,
            symbol=leg.symbol,
            leg_id=leg.leg_id,
            mark_price=signal.mark_price,
            liquidation_price=signal.liquidation_price,
            side=leg.side,
        )
        for leg in registry.open_legs_for_venue_symbol(
            venue=signal.venue,
            symbol=signal.symbol,
        )
        if leg.side == signal.side
    ]


class MarginTopupManager:
    def __init__(
        self,
        *,
        registry: PositionRegistry,
        toppers: dict[str, MarginTopupper],
        config: MarginTopupConfig,
        now_ms: Callable[[], int] = now_ms,
        dry_run: bool = False,
    ) -> None:
        self.registry = registry
        self.toppers = toppers
        self.config = config
        self._now_ms = now_ms
        self.dry_run = dry_run

    async def handle_snapshot(self, snapshot: MarginHealthSnapshot) -> dict[str, object]:
        leg = self.registry.get_leg(snapshot.leg_id)
        distance_pct = liquidation_distance_pct(
            side=snapshot.side,
            mark_price=snapshot.mark_price,
            liquidation_price=snapshot.liquidation_price,
        )
        if not self.config.enabled:
            return {"ok": True, "action": "disabled", "distance_pct": distance_pct}
        if leg.status != "open":
            return {"ok": True, "action": "leg_not_open", "distance_pct": distance_pct}
        if distance_pct > self.config.liq_distance_trigger_pct:
            return {"ok": True, "action": "healthy", "distance_pct": distance_pct}
        if leg.margin_topups >= self.config.max_topups_per_leg:
            return {"ok": True, "action": "max_topups_reached", "distance_pct": distance_pct}

        current_ms = self._now_ms()
        cooldown_ms = self.config.cooldown_seconds * 1000
        if leg.last_margin_topup_ts_ms is not None and current_ms - leg.last_margin_topup_ts_ms < cooldown_ms:
            return {"ok": True, "action": "cooldown", "distance_pct": distance_pct}

        topper = self.toppers.get(leg.venue.lower())
        if topper is None:
            return self._topup_failed(snapshot, distance_pct, f"missing margin topper for {leg.venue}")

        if self.dry_run:
            return {
                "ok": True,
                "action": "topup_dry_run",
                "distance_pct": distance_pct,
                "topup_amount_usd": self.config.topup_amount_usd,
            }

        try:
            result = await topper.add_isolated_margin(
                strategy_id=leg.strategy_id,
                leg_id=leg.leg_id,
                venue=leg.venue,
                symbol=leg.symbol,
                side=leg.side,
                amount_usd=self.config.topup_amount_usd,
            )
        except Exception as exc:
            return self._topup_failed(snapshot, distance_pct, str(exc))

        leg.margin_topups += 1
        leg.last_margin_topup_ts_ms = current_ms
        return {
            "ok": True,
            "action": "topup_done",
            "distance_pct": distance_pct,
            "topup_amount_usd": self.config.topup_amount_usd,
            "topup_result": result,
        }

    def _topup_failed(
        self,
        snapshot: MarginHealthSnapshot,
        distance_pct: float,
        error: str,
    ) -> dict[str, object]:
        leg = self.registry.get_leg(snapshot.leg_id)
        event = RiskEvent(
            strategy_id=leg.strategy_id,
            leg_id=leg.leg_id,
            venue=leg.venue,
            symbol=leg.symbol,
            event_type="MANUAL_EMERGENCY",
            message=f"margin top-up failed: {error}",
        )
        return {
            "ok": False,
            "action": "topup_failed",
            "distance_pct": distance_pct,
            "error": error,
            "risk_event": event,
        }
