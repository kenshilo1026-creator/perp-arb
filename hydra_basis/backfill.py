from __future__ import annotations

from typing import Sequence, TypeVar

from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
from hydra_basis.execution_engine.risk import compute_spread_pct
from hydra_basis.history_store import funding_history_is_complete


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


def _safe_error_text(error: Exception) -> str:
    try:
        return str(error)
    except Exception:
        return repr(error)


def spread_error_is_transient(error: Exception) -> bool:
    message = _safe_error_text(error).lower()
    return (
        "429" in message
        or "too many requests" in message
        or "rate limit" in message
        or "timeout" in message
        or "timed out" in message
        or "invalid response status" in message
    )


async def capture_backfill_spread_snapshot(
    *,
    session,
    spreads: dict[tuple[str, str], dict[str, float | int]],
    venue: str,
    symbol: str,
    clip_usd: float,
    force_refresh: bool = False,
) -> bool:
    result = await capture_backfill_spread_snapshot_with_error(
        session=session,
        spreads=spreads,
        venue=venue,
        symbol=symbol,
        clip_usd=clip_usd,
        force_refresh=force_refresh,
    )
    return bool(result["stored"])


async def capture_backfill_spread_snapshot_with_error(
    *,
    session,
    spreads: dict[tuple[str, str], dict[str, float | int]],
    venue: str,
    symbol: str,
    clip_usd: float,
    force_refresh: bool = False,
) -> dict[str, object]:
    if not force_refresh and spreads.get((venue, symbol), {}).get("status") == NO_ORDERBOOK_SENTINEL:
        return {
            "stored": False,
            "venue": venue,
            "symbol": symbol,
            "error": None,
            "error_type": "cached_no_orderbook",
        }

    try:
        orderbook = await fetch_orderbook_snapshot(
            session,
            venue=venue,
            symbol=symbol,
            clip_usd=clip_usd,
        )
    except Exception as exc:
        message = _safe_error_text(exc)
        if "missing " in message.lower() and " orderbook for " in message.lower():
            spreads[(venue, symbol)] = {"status": NO_ORDERBOOK_SENTINEL}
            return {
                "stored": False,
                "venue": venue,
                "symbol": symbol,
                "error": None,
                "error_type": "no_orderbook",
            }
        if spread_error_is_transient(exc):
            print(f"backfill spread error transient {(venue, symbol)}: {message}")
            return {
                "stored": False,
                "venue": venue,
                "symbol": symbol,
                "error": message,
                "error_type": "transient",
            }
        print(f"backfill spread skipped {(venue, symbol)}: {message}")
        if (venue, symbol) not in spreads:
            spreads[(venue, symbol)] = {"status": NO_ORDERBOOK_SENTINEL}
        return {
            "stored": False,
            "venue": venue,
            "symbol": symbol,
            "error": message,
            "error_type": "permanent",
        }

    spreads[(venue, symbol)] = {
        "bid": float(orderbook["bid"]),
        "ask": float(orderbook["ask"]),
        "spread_pct": compute_spread_pct(orderbook),
        "ts_ms": int(orderbook["ts_ms"]),
    }
    return {
        "stored": True,
        "venue": venue,
        "symbol": symbol,
        "error": None,
        "error_type": None,
    }


def persist_backfill_progress(
    *,
    history_store,
    spread_store,
    funding_points: dict[tuple[str, str], list],
    spreads: dict[tuple[str, str], dict[str, float | int]],
) -> None:
    history_store.save(funding_points)
    spread_store.save(spreads)
