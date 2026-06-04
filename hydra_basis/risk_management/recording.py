from __future__ import annotations

import time
from pathlib import Path

from hydra_basis.risk_management.models import PositionLeg
from hydra_basis.risk_management.registry import PositionRegistry


def _default_strategy_id(symbol: str) -> str:
    return f"manual-{symbol.upper()}-{int(time.time() * 1000)}"


def _assert_successful_two_leg_execution(execution_result: dict) -> None:
    if not execution_result.get("ok", False):
        raise RuntimeError("execution result is not successful; refusing to record open risk legs")
    maker_result = execution_result.get("maker_result", {})
    hedge_result = execution_result.get("hedge_result", {})
    if not maker_result.get("ok", False) or not hedge_result.get("ok", False):
        raise RuntimeError("both maker and hedge results must be ok before recording risk legs")


def record_successful_execution(
    *,
    path: Path,
    symbol: str,
    quantity: str,
    short_venue: str,
    long_venue: str,
    execution_result: dict,
    strategy_id: str | None = None,
    market_type: str = "perp",
) -> str:
    _assert_successful_two_leg_execution(execution_result)

    symbol_normalized = symbol.strip().upper()
    resolved_strategy_id = strategy_id or _default_strategy_id(symbol_normalized)
    registry = PositionRegistry.load(path)
    registry.add_leg(
        PositionLeg(
            strategy_id=resolved_strategy_id,
            leg_id=f"{resolved_strategy_id}:{short_venue}:short",
            venue=short_venue,
            symbol=symbol_normalized,
            market_type=market_type,  # type: ignore[arg-type]
            side="SHORT",
            quantity=quantity,
            status="open",
        )
    )
    registry.add_leg(
        PositionLeg(
            strategy_id=resolved_strategy_id,
            leg_id=f"{resolved_strategy_id}:{long_venue}:long",
            venue=long_venue,
            symbol=symbol_normalized,
            market_type=market_type,  # type: ignore[arg-type]
            side="LONG",
            quantity=quantity,
            status="open",
        )
    )
    registry.save(path)
    return resolved_strategy_id
