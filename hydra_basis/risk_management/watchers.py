from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Protocol

from hydra_basis.risk_management.models import RiskEvent, RiskEventType
from hydra_basis.risk_management.registry import PositionRegistry


@dataclass
class VenueRiskSignal:
    venue: str
    symbol: str
    event_type: RiskEventType
    message: str = ""


class RiskEventWatcher(Protocol):
    async def watch(self) -> AsyncIterator[VenueRiskSignal]:
        ...


def _strip_usdt_suffix(symbol: str) -> str:
    symbol = symbol.strip().upper()
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def parse_hyperliquid_risk_signal(payload: dict) -> VenueRiskSignal | None:
    data = payload.get("data", payload)
    liquidation = data.get("liquidation") if isinstance(data, dict) else None
    if not isinstance(liquidation, dict):
        return None
    symbol = liquidation.get("coin") or liquidation.get("symbol")
    if not symbol:
        return None
    return VenueRiskSignal(
        venue="hyperliquid",
        symbol=str(symbol).upper(),
        event_type="LIQUIDATION",
        message=str(liquidation),
    )


def parse_aster_risk_signal(payload: dict) -> VenueRiskSignal | None:
    event_type = payload.get("e")
    order = payload.get("o", {})
    if not isinstance(order, dict):
        return None
    symbol = order.get("s") or payload.get("symbol")
    if not symbol:
        return None

    if event_type == "forceOrder":
        return VenueRiskSignal(
            venue="aster",
            symbol=_strip_usdt_suffix(str(symbol)),
            event_type="LIQUIDATION",
            message=str(payload),
        )

    if event_type == "ORDER_TRADE_UPDATE":
        client_order_id = str(order.get("c", ""))
        status = str(order.get("X", ""))
        if status and status != "FILLED":
            return None
        if client_order_id.startswith("adl_autoclose"):
            event = "ADL"
        elif client_order_id.startswith("autoclose-"):
            event = "LIQUIDATION"
        else:
            return None
        return VenueRiskSignal(
            venue="aster",
            symbol=_strip_usdt_suffix(str(symbol)),
            event_type=event,  # type: ignore[arg-type]
            message=str(payload),
        )

    return None


def build_registry_events_for_venue_symbol(
    *,
    registry: PositionRegistry,
    venue: str,
    symbol: str,
    event_type: RiskEventType,
    message: str = "",
) -> list[RiskEvent]:
    return [
        RiskEvent(
            strategy_id=leg.strategy_id,
            leg_id=leg.leg_id,
            venue=leg.venue,
            symbol=leg.symbol,
            event_type=event_type,
            message=message,
        )
        for leg in registry.open_legs_for_venue_symbol(venue=venue, symbol=symbol)
    ]


async def consume_watcher_events(
    *,
    registry: PositionRegistry,
    watcher: RiskEventWatcher,
) -> AsyncIterator[RiskEvent]:
    async for signal in watcher.watch():
        for event in build_registry_events_for_venue_symbol(
            registry=registry,
            venue=signal.venue,
            symbol=signal.symbol,
            event_type=signal.event_type,
            message=signal.message,
        ):
            yield event
