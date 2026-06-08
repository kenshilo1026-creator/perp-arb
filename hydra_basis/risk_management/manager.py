from __future__ import annotations

from typing import Protocol

from hydra_basis.risk_management.models import RiskEvent, close_side_for_position
from hydra_basis.risk_management.registry import PositionRegistry


class PositionCloser(Protocol):
    async def close_position(self, **kwargs) -> dict:
        ...


class EmergencyRiskManager:
    def __init__(
        self,
        *,
        registry: PositionRegistry,
        closers: dict[str, PositionCloser],
        dry_run: bool = False,
    ) -> None:
        self.registry = registry
        self.closers = closers
        self.dry_run = dry_run

    async def handle_event(self, event: RiskEvent) -> dict[str, object]:
        if event.event_type == "FUNDING_AUTO_CLOSE":
            legs_to_close = [
                leg
                for leg in self.registry.legs_for_strategy(event.strategy_id)
                if leg.status == "open"
            ]
        else:
            legs_to_close = self.registry.open_counterparty_legs(
                strategy_id=event.strategy_id,
                trigger_leg_id=event.leg_id,
            )
        closed_leg_ids: list[str] = []
        failed_leg_ids: list[str] = []
        close_results: dict[str, dict] = {}

        for leg in legs_to_close:
            side = close_side_for_position(leg.side)
            closer = self.closers.get(leg.venue)
            if closer is None:
                self.registry.mark_status(leg.leg_id, "close_failed")
                failed_leg_ids.append(leg.leg_id)
                close_results[leg.leg_id] = {"ok": False, "error": f"missing closer for {leg.venue}"}
                continue

            if self.dry_run:
                result = {"ok": True, "dry_run": True}
            else:
                result = await closer.close_position(
                    strategy_id=leg.strategy_id,
                    leg_id=leg.leg_id,
                    venue=leg.venue,
                    symbol=leg.symbol,
                    market_type=leg.market_type,
                    side=side,
                    quantity=leg.quantity,
                    trigger_event=event.event_type,
                )

            close_results[leg.leg_id] = result
            if result.get("ok", False):
                self.registry.mark_status(leg.leg_id, "emergency_closed")
                closed_leg_ids.append(leg.leg_id)
            else:
                self.registry.mark_status(leg.leg_id, "close_failed")
                failed_leg_ids.append(leg.leg_id)

        return {
            "ok": not failed_leg_ids,
            "event_type": event.event_type,
            "trigger_leg_id": event.leg_id,
            "closed_leg_ids": closed_leg_ids,
            "failed_leg_ids": failed_leg_ids,
            "close_results": close_results,
        }
