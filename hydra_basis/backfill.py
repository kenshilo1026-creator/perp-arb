from __future__ import annotations

from typing import Sequence, TypeVar

from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
from hydra_basis.execution_engine.risk import compute_spread_pct
from hydra_basis.history_store import funding_history_is_complete
from hydra_basis.monitor_errors import should_raise_immediately


T = TypeVar("T")
LORIS_BATCHED_VENUES = {"variational"}


def chunk_sequence(items: Sequence[T], *, chunk_size: int) -> list[list[T]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [list(items[index:index + chunk_size]) for index in range(0, len(items), chunk_size)]


def split_loris_batched_keys(
    keys: Sequence[tuple[str, str]],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    immediate: list[tuple[str, str]] = []
    batched: list[tuple[str, str]] = []
    for key in keys:
        if key[0] in LORIS_BATCHED_VENUES:
            batched.append(key)
        else:
            immediate.append(key)
    return immediate, batched


def build_spread_refresh_keys(
    venue_symbols: dict[str, set[str]],
    *,
    enabled_venues: Sequence[str],
    supported_venues: set[str] | None = None,
) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    for venue in enabled_venues:
        if supported_venues is not None and venue not in supported_venues:
            continue
        for symbol in sorted(venue_symbols.get(venue, set())):
            keys.append((venue, symbol))
    return keys


def backfill_incremental_start_ms(points: Sequence) -> int | None:
    if not points:
        return None
    return max(point.ts_ms for point in points) + 1


def backfill_needs_top_up(points: Sequence, *, now_ms: int) -> bool:
    if not points:
        return False
    if not funding_history_is_complete(list(points), required_days=7, now_ms=now_ms):
        return False
    newest_ts = max(point.ts_ms for point in points)
    max_interval_ms = int(max(point.interval_hours for point in points) * 3_600_000)
    return newest_ts <= now_ms - max_interval_ms


NO_ORDERBOOK_SENTINEL = "no_orderbook"


async def capture_backfill_spread_snapshot(
    *,
    session,
    spreads: dict[tuple[str, str], dict[str, float | int]],
    venue: str,
    symbol: str,
    clip_usd: float,
    force_refresh: bool = False,
) -> bool:
    if not force_refresh and spreads.get((venue, symbol), {}).get("status") == NO_ORDERBOOK_SENTINEL:
        return False

    try:
        orderbook = await fetch_orderbook_snapshot(
            session,
            venue=venue,
            symbol=symbol,
            clip_usd=clip_usd,
        )
    except Exception as exc:
        if should_raise_immediately(exc):
            raise
        print(f"backfill spread skipped {(venue, symbol)}: {exc}")
        spreads[(venue, symbol)] = {"status": NO_ORDERBOOK_SENTINEL}
        return False

    spreads[(venue, symbol)] = {
        "bid": float(orderbook["bid"]),
        "ask": float(orderbook["ask"]),
        "spread_pct": compute_spread_pct(orderbook),
        "ts_ms": int(orderbook["ts_ms"]),
    }
    return True


def persist_backfill_progress(
    *,
    history_store,
    spread_store,
    funding_points: dict[tuple[str, str], list],
    spreads: dict[tuple[str, str], dict[str, float | int]],
) -> None:
    history_store.save(funding_points)
    spread_store.save(spreads)
