from __future__ import annotations

import json
from pathlib import Path


def default_maker_for_pair(first_venue: str, second_venue: str) -> str | None:
    """Keep browser-driven Variational submissions off the automatic hedge leg."""
    for venue in (first_venue, second_venue):
        if venue.strip().lower() == "variational":
            return venue
    return None


def resolve_execution_legs(
    *,
    short_venue: str,
    long_venue: str,
    priorities: dict[str, int],
    spreads: dict[str, float],
    spread_priority_threshold: float = 0.001,
) -> tuple[str, str]:
    preferred_maker = default_maker_for_pair(short_venue, long_venue)
    if preferred_maker is not None:
        return preferred_maker, long_venue if preferred_maker == short_venue else short_venue
    short_spread = spreads[short_venue]
    long_spread = spreads[long_venue]
    short_above = short_spread > spread_priority_threshold
    long_above = long_spread > spread_priority_threshold

    # If either venue has a wide spread, the wider one should be maker (place limit to avoid paying it)
    if short_above or long_above:
        if long_spread > short_spread:
            return long_venue, short_venue
        return short_venue, long_venue

    # Both spreads are tight: use priority (higher number = preferred maker)
    short_priority = priorities[short_venue]
    long_priority = priorities[long_venue]
    if short_priority > long_priority:
        return short_venue, long_venue
    if long_priority > short_priority:
        return long_venue, short_venue

    # Equal priority: tiebreak by spread (wider = maker)
    if long_spread > short_spread:
        return long_venue, short_venue
    return short_venue, long_venue


def load_execution_priorities(path: Path) -> dict[str, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(key): int(value) for key, value in payload.items()}
