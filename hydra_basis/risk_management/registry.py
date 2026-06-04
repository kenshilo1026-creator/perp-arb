from __future__ import annotations

import json
from pathlib import Path

from hydra_basis.risk_management.models import PositionLeg, PositionStatus


class PositionRegistry:
    def __init__(self, *, legs: list[PositionLeg] | None = None) -> None:
        self._legs: dict[str, PositionLeg] = {}
        for leg in legs or []:
            self.add_leg(leg)

    def add_leg(self, leg: PositionLeg) -> None:
        self._legs[leg.leg_id] = leg

    def get_leg(self, leg_id: str) -> PositionLeg:
        try:
            return self._legs[leg_id]
        except KeyError as exc:
            raise RuntimeError(f"position leg not found: {leg_id}") from exc

    def legs_for_strategy(self, strategy_id: str) -> list[PositionLeg]:
        return [
            leg
            for leg in self._legs.values()
            if leg.strategy_id == strategy_id
        ]

    def open_counterparty_legs(self, *, strategy_id: str, trigger_leg_id: str) -> list[PositionLeg]:
        return [
            leg
            for leg in self.legs_for_strategy(strategy_id)
            if leg.leg_id != trigger_leg_id and leg.status == "open"
        ]

    def open_legs_for_venue_symbol(self, *, venue: str, symbol: str) -> list[PositionLeg]:
        venue_normalized = venue.strip().lower()
        symbol_normalized = symbol.strip().upper()
        return [
            leg
            for leg in self._legs.values()
            if (
                leg.status == "open"
                and leg.venue.strip().lower() == venue_normalized
                and leg.symbol.strip().upper() == symbol_normalized
            )
        ]

    def mark_status(self, leg_id: str, status: PositionStatus) -> None:
        self.get_leg(leg_id).status = status

    def to_payload(self) -> dict[str, list[dict[str, str]]]:
        return {"legs": [leg.to_dict() for leg in self._legs.values()]}

    @classmethod
    def from_payload(cls, payload: dict) -> "PositionRegistry":
        return cls(legs=[PositionLeg.from_dict(item) for item in payload.get("legs", [])])

    @classmethod
    def load(cls, path: Path) -> "PositionRegistry":
        if not path.exists():
            return cls()
        return cls.from_payload(json.loads(path.read_text(encoding="utf-8")))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_payload(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
