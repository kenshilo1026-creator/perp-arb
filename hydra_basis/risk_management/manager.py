from __future__ import annotations

from typing import Protocol

from hydra_basis.risk_management.models import RiskEvent, close_side_for_position
from hydra_basis.risk_management.registry import PositionRegistry


class PositionCloser(Protocol):
    async def get_open_position(self, **kwargs) -> dict | None:
        ...

    async def close_position(self, **kwargs) -> dict:
        ...


class EmergencyRiskManager:
    def __init__(
        self,
        *,
        registry: PositionRegistry,
        closers: dict[str, PositionCloser],
        dry_run: bool = False,
        max_close_retries: int = 2,
    ) -> None:
        self.registry = registry
        self.closers = closers
        self.dry_run = dry_run
        self.max_close_retries = max_close_retries

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
            closer = self.closers.get(leg.venue)
            if closer is None:
                self.registry.mark_status(leg.leg_id, "close_failed")
                failed_leg_ids.append(leg.leg_id)
                close_results[leg.leg_id] = {"ok": False, "error": f"missing closer for {leg.venue}"}
                continue

            result = await self._close_leg_with_retries(closer=closer, leg=leg, event=event)

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

    async def _fetch_live_position(self, *, closer: PositionCloser, leg) -> dict | None:
        getter = getattr(closer, "get_open_position", None)
        if not callable(getter):
            return {
                "ok": False,
                "error": f"live position query unavailable for {leg.venue}",
            }
        try:
            return await getter(symbol=leg.symbol, market_type=leg.market_type)
        except Exception as exc:
            return {
                "ok": False,
                "error": f"live position query failed for {leg.venue}:{leg.symbol}: {exc}",
            }

    async def _close_leg_with_retries(self, *, closer: PositionCloser, leg, event: RiskEvent) -> dict:
        attempts: list[dict] = []
        max_attempts = max(1, self.max_close_retries + 1)
        for attempt in range(1, max_attempts + 1):
            live_position = await self._fetch_live_position(closer=closer, leg=leg)
            if live_position is None:
                return {
                    "ok": False,
                    "attempts": attempts,
                    "error": f"no live open position for {leg.venue}:{leg.symbol}",
                }
            if isinstance(live_position, dict) and live_position.get("ok") is False:
                live_position["attempts"] = attempts
                return live_position

            live_side = str(live_position.get("side", "")).strip().upper()
            live_quantity = str(live_position.get("quantity", "0")).strip()
            if live_side not in {"LONG", "SHORT"} or not live_quantity:
                return {
                    "ok": False,
                    "attempts": attempts,
                    "error": f"invalid live position payload for {leg.venue}:{leg.symbol}: {live_position}",
                }
            side = close_side_for_position(live_side)

            if self.dry_run:
                result = {"ok": True, "dry_run": True, "attempt": attempt, "quantity": live_quantity}
            else:
                try:
                    result = await closer.close_position(
                        strategy_id=leg.strategy_id,
                        leg_id=leg.leg_id,
                        venue=leg.venue,
                        symbol=leg.symbol,
                        market_type=leg.market_type,
                        side=side,
                        quantity=live_quantity,
                        trigger_event=event.event_type,
                    )
                except Exception as exc:
                    result = {"ok": False, "error": str(exc)}
            attempt_result = {
                "attempt": attempt,
                "quantity": live_quantity,
                "side": side,
                "result": result,
            }
            attempts.append(attempt_result)
            if result.get("ok", False):
                result["attempts"] = attempts
                return result
        return {
            "ok": False,
            "attempts": attempts,
            "error": attempts[-1]["result"].get("error", "close failed after retries") if attempts else "close failed",
        }
