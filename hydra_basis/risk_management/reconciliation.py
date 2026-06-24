from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation

from hydra_basis.risk_management.models import PositionLeg
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


def _is_registry_fallback_venue(venue: str) -> bool:
    return venue.strip().lower() == "variational"


def _quantity_key(quantity: str) -> Decimal | None:
    try:
        return abs(Decimal(str(quantity).strip()))
    except (InvalidOperation, ValueError):
        return None


def _live_pair_strategy_id(*, symbol: str, quantity: Decimal, ts_ms: int) -> str:
    quantity_text = format(quantity, "f").replace(".", "_")
    return f"live-pair-{symbol.lower()}-{quantity_text}-{ts_ms}"


def _is_auto_registerable_pair(*, long_position: dict[str, str], short_position: dict[str, str]) -> bool:
    if short_position["symbol"] != long_position["symbol"]:
        return False
    market_pair = (long_position["market_type"], short_position["market_type"])
    return market_pair in {
        ("perp", "perp"),
        ("spot", "perp"),
    }


def _auto_register_matching_live_pairs(
    *,
    registry: PositionRegistry,
    live_positions: list[dict[str, str]],
    now_ms: int,
) -> tuple[list[str], list[str], set[int]]:
    messages: list[str] = []
    strategy_ids: list[str] = []
    used_indexes: set[int] = set()
    indexed = list(enumerate(live_positions))

    for long_index, long_position in indexed:
        if long_index in used_indexes or long_position["side"] != "LONG":
            continue
        long_quantity = _quantity_key(long_position["quantity"])
        if long_quantity is None:
            continue
        for short_index, short_position in indexed:
            if short_index in used_indexes or short_index == long_index or short_position["side"] != "SHORT":
                continue
            if not _is_auto_registerable_pair(
                long_position=long_position,
                short_position=short_position,
            ):
                continue
            short_quantity = _quantity_key(short_position["quantity"])
            if short_quantity is None or short_quantity != long_quantity:
                continue

            strategy_id = _live_pair_strategy_id(
                symbol=long_position["symbol"],
                quantity=long_quantity,
                ts_ms=now_ms + len(strategy_ids),
            )
            for position in (long_position, short_position):
                leg_id = (
                    f"{strategy_id}:{position['venue']}:{position['market_type']}:"
                    f"{position['side'].lower()}"
                )
                registry.add_leg(
                    PositionLeg(
                        strategy_id=strategy_id,
                        leg_id=leg_id,
                        venue=position["venue"],
                        symbol=position["symbol"],
                        market_type=position["market_type"],  # type: ignore[arg-type]
                        side=position["side"],  # type: ignore[arg-type]
                        quantity=position["quantity"],
                        status="open",
                    )
                )
            used_indexes.update({long_index, short_index})
            strategy_ids.append(strategy_id)
            messages.append(
                f"已登記新倉位: {long_position['symbol']} - "
                f"做空方 {short_position['venue']}:{short_position['market_type']} / "
                f"做多方 {long_position['venue']}:{long_position['market_type']} / "
                f"數量={long_position['quantity']} "
                f"策略={strategy_id}"
            )
            break

    return messages, strategy_ids, used_indexes


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
        if _is_registry_fallback_venue(leg.venue):
            registered_live_keys.add(
                _position_key(
                    venue=leg.venue,
                    symbol=leg.symbol,
                    market_type=leg.market_type,
                    side=leg.side,
                )
            )
            messages.append(
                f"variational registry fallback: keeping {leg.venue}:{leg.symbol} "
                f"leg={leg.leg_id} side={leg.side} quantity={leg.quantity}"
            )
            continue
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

    unregistered_live_positions: list[dict[str, str]] = []
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
            live_venue = str(live.get("venue") or venue).strip().lower()
            symbol = str(live.get("symbol", "")).strip().upper()
            market_type = str(live.get("market_type", "")).strip().lower()
            side = str(live.get("side", "")).strip().upper()
            quantity = str(live.get("quantity", "")).strip()
            if not symbol or market_type not in {"perp", "spot"} or side not in {"LONG", "SHORT"}:
                continue
            key = _position_key(venue=live_venue, symbol=symbol, market_type=market_type, side=side)
            registry_matches = [
                leg
                for strategy_id in registry.open_strategy_ids()
                for leg in registry.legs_for_strategy(strategy_id)
                if _position_key(venue=leg.venue, symbol=leg.symbol, market_type=leg.market_type, side=leg.side) == key
            ]
            if registry_matches or key in registered_live_keys:
                continue
            unregistered_live_positions.append(
                {
                    "venue": live_venue,
                    "symbol": symbol,
                    "market_type": market_type,
                    "side": side,
                    "quantity": quantity,
                }
            )

    auto_messages, auto_registered_strategy_ids, used_indexes = _auto_register_matching_live_pairs(
        registry=registry,
        live_positions=unregistered_live_positions,
        now_ms=int(time.time() * 1000),
    )
    messages.extend(auto_messages)

    for index, live in enumerate(unregistered_live_positions):
        if index in used_indexes:
            continue
        messages.append(
            f"unregistered live position: {live['venue']}:{live['symbol']} "
            f"{live['market_type']} {live['side']} quantity={live['quantity']}"
        )
    unregistered_count = len(unregistered_live_positions) - len(used_indexes)

    return {
        "ok": mismatch_count == 0 and unregistered_count == 0,
        "messages": messages,
        "mismatch_count": mismatch_count,
        "unregistered_count": unregistered_count,
        "auto_registered_count": len(auto_registered_strategy_ids),
        "auto_registered_strategy_ids": auto_registered_strategy_ids,
        "updated_leg_ids": updated_leg_ids,
    }
