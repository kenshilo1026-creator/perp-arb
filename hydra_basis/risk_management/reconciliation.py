from __future__ import annotations

from hydra_basis.risk_management.registry import PositionRegistry


def _position_key(*, venue: str, symbol: str, market_type: str, side: str) -> tuple[str, str, str, str]:
    return (
        venue.strip().lower(),
        symbol.strip().upper(),
        market_type.strip().lower(),
        side.strip().upper(),
    )


def _closer_key_for_venue(venue: str) -> str:
    normalized = venue.strip().lower()
    if normalized == "mexc_spot":
        return "mexc"
    return normalized


async def reconcile_registry_positions(
    *,
    registry: PositionRegistry,
    closers: dict[str, object],
) -> dict[str, object]:
    messages: list[str] = []
    updated_leg_ids: list[str] = []
    mismatch_count = 0
    registered_live_keys: set[tuple[str, str, str, str]] = set()

    open_legs = [leg for strategy_id in registry.open_strategy_ids() for leg in registry.legs_for_strategy(strategy_id) if leg.status == "open"]
    for leg in open_legs:
        closer = closers.get(_closer_key_for_venue(leg.venue))
        if closer is None:
            mismatch_count += 1
            messages.append(f"registry mismatch: missing closer for {leg.venue}:{leg.symbol} leg={leg.leg_id}")
            continue
        getter = getattr(closer, "get_open_position", None)
        if not callable(getter):
            mismatch_count += 1
            messages.append(f"registry mismatch: live query unavailable for {leg.venue}:{leg.symbol} leg={leg.leg_id}")
            continue

        try:
            live = await getter(symbol=leg.symbol, market_type=leg.market_type)
        except Exception as exc:
            mismatch_count += 1
            messages.append(f"registry mismatch: live query failed for {leg.venue}:{leg.symbol} leg={leg.leg_id}: {exc}")
            continue
        if not live:
            leg.status = "closed"
            updated_leg_ids.append(leg.leg_id)
            messages.append(f"registry closed missing live position for {leg.venue}:{leg.symbol} leg={leg.leg_id}")
            continue

        live_side = str(live.get("side", "")).strip().upper()
        live_quantity = str(live.get("quantity", "")).strip()
        registered_live_keys.add(
            _position_key(
                venue=leg.venue,
                symbol=leg.symbol,
                market_type=leg.market_type,
                side=live_side,
            )
        )
        if live_side != leg.side:
            mismatch_count += 1
            messages.append(
                f"registry mismatch: side mismatch for {leg.venue}:{leg.symbol} "
                f"leg={leg.leg_id} registry={leg.side} live={live_side}"
            )
            continue
        if live_quantity and live_quantity != leg.quantity:
            old_quantity = leg.quantity
            leg.quantity = live_quantity
            updated_leg_ids.append(leg.leg_id)
            messages.append(
                f"registry quantity updated: {leg.venue}:{leg.symbol} leg={leg.leg_id} "
                f"registry={old_quantity} live={live_quantity}"
            )

    unregistered_count = 0
    for venue, closer in closers.items():
        lister = getattr(closer, "list_open_positions", None)
        if not callable(lister):
            continue
        try:
            live_positions = await lister()
        except Exception as exc:
            mismatch_count += 1
            messages.append(f"registry mismatch: list_open_positions failed for {venue}: {exc}")
            continue
        for live in live_positions or []:
            symbol = str(live.get("symbol", "")).strip().upper()
            market_type = str(live.get("market_type", "")).strip().lower()
            side = str(live.get("side", "")).strip().upper()
            quantity = str(live.get("quantity", "")).strip()
            if not symbol or market_type not in {"perp", "spot"} or side not in {"LONG", "SHORT"}:
                continue
            key = _position_key(venue=venue, symbol=symbol, market_type=market_type, side=side)
            registry_matches = [
                leg
                for strategy_id in registry.open_strategy_ids()
                for leg in registry.legs_for_strategy(strategy_id)
                if _position_key(venue=leg.venue, symbol=leg.symbol, market_type=leg.market_type, side=leg.side) == key
            ]
            if registry_matches or key in registered_live_keys:
                continue
            unregistered_count += 1
            messages.append(
                f"unregistered live position: {venue}:{symbol} {market_type} {side} quantity={quantity}"
            )

    return {
        "ok": mismatch_count == 0 and unregistered_count == 0,
        "messages": messages,
        "mismatch_count": mismatch_count,
        "unregistered_count": unregistered_count,
        "updated_leg_ids": updated_leg_ids,
    }
