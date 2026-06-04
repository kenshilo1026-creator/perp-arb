from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


MarketType = Literal["perp", "spot"]
PositionSide = Literal["LONG", "SHORT"]
PositionStatus = Literal["open", "closed", "emergency_closed", "close_failed"]
RiskEventType = Literal["LIQUIDATION", "ADL", "POSITION_MISSING", "MANUAL_EMERGENCY"]


@dataclass
class PositionLeg:
    strategy_id: str
    leg_id: str
    venue: str
    symbol: str
    market_type: MarketType
    side: PositionSide
    quantity: str
    status: PositionStatus = "open"
    margin_topups: int = 0
    last_margin_topup_ts_ms: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict) -> "PositionLeg":
        return cls(
            strategy_id=payload["strategy_id"],
            leg_id=payload["leg_id"],
            venue=payload["venue"],
            symbol=payload["symbol"],
            market_type=payload["market_type"],  # type: ignore[arg-type]
            side=payload["side"],  # type: ignore[arg-type]
            quantity=str(payload["quantity"]),
            status=payload.get("status", "open"),  # type: ignore[arg-type]
            margin_topups=int(payload.get("margin_topups", 0)),
            last_margin_topup_ts_ms=payload.get("last_margin_topup_ts_ms"),
        )


@dataclass
class RiskEvent:
    strategy_id: str
    leg_id: str
    venue: str
    symbol: str
    event_type: RiskEventType
    message: str = ""


def close_side_for_position(side: PositionSide) -> str:
    if side == "LONG":
        return "SELL"
    if side == "SHORT":
        return "BUY"
    raise RuntimeError(f"unsupported position side: {side}")
