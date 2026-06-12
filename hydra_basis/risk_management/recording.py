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


def _normalize_live_leg(payload: dict) -> dict[str, str]:
    venue = str(payload.get("venue", "")).strip().lower()
    symbol = str(payload.get("symbol", "")).strip().upper()
    market_type = str(payload.get("market_type", "")).strip().lower()
    side = str(payload.get("side", "")).strip().upper()
    quantity = str(payload.get("quantity", "")).strip()
    if not venue:
        raise RuntimeError(f"live leg missing venue: {payload}")
    if not symbol:
        raise RuntimeError(f"live leg missing symbol: {payload}")
    if market_type not in {"perp", "spot"}:
        raise RuntimeError(f"live leg has invalid market_type: {payload}")
    if side not in {"LONG", "SHORT"}:
        raise RuntimeError(f"live leg has invalid side: {payload}")
    try:
        if float(quantity) <= 0:
            raise ValueError
    except ValueError as exc:
        raise RuntimeError(f"live leg has invalid quantity: {payload}") from exc
    return {
        "venue": venue,
        "symbol": symbol,
        "market_type": market_type,
        "side": side,
        "quantity": quantity,
    }


def record_successful_live_legs(
    *,
    path: Path,
    symbol: str,
    execution_result: dict,
    legs: list[dict],
    strategy_id: str | None = None,
) -> str:
    _assert_successful_two_leg_execution(execution_result)

    symbol_normalized = symbol.strip().upper()
    resolved_strategy_id = strategy_id or _default_strategy_id(symbol_normalized)
    normalized_legs = [_normalize_live_leg({**leg, "symbol": leg.get("symbol") or symbol_normalized}) for leg in legs]
    if len(normalized_legs) < 2:
        raise RuntimeError("at least two live legs are required before recording risk legs")

    registry = PositionRegistry.load(path)
    for leg in normalized_legs:
        venue = leg["venue"]
        market_type = leg["market_type"]
        side = leg["side"]
        registry.add_leg(
            PositionLeg(
                strategy_id=resolved_strategy_id,
                leg_id=f"{resolved_strategy_id}:{venue}:{market_type}:{side.lower()}",
                venue=venue,
                symbol=leg["symbol"],
                market_type=market_type,  # type: ignore[arg-type]
                side=side,  # type: ignore[arg-type]
                quantity=leg["quantity"],
                status="open",
            )
        )
    registry.save(path)
    return resolved_strategy_id
