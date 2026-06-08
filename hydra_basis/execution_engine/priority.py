from __future__ import annotations

import json
from pathlib import Path


def resolve_execution_legs(
    *,
    short_venue: str,
    long_venue: str,
    priorities: dict[str, int],
    spreads: dict[str, float],
    spread_priority_threshold: float = 0.001,
) -> tuple[str, str]:
    short_spread = spreads[short_venue]
    long_spread = spreads[long_venue]
    short_above = short_spread > spread_priority_threshold
    long_above = long_spread > spread_priority_threshold

    if short_above and long_above:
        if short_spread < long_spread:
            return long_venue, short_venue
        if long_spread < short_spread:
            return short_venue, long_venue
        return short_venue, long_venue

    short_priority = priorities[short_venue]
    long_priority = priorities[long_venue]
    if short_priority > long_priority:
        return short_venue, long_venue
    if long_priority > short_priority:
        return long_venue, short_venue

    if short_spread < long_spread:
        return long_venue, short_venue
    if long_spread < short_spread:
        return short_venue, long_venue
    return short_venue, long_venue


def load_execution_priorities(path: Path) -> dict[str, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(key): int(value) for key, value in payload.items()}
