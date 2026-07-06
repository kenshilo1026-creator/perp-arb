from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path


DEFAULT_SYMBOL_MAPPINGS_PATH = Path("configs/symbol_mappings.json")


@lru_cache(maxsize=1)
def load_symbol_mappings(path: str = str(DEFAULT_SYMBOL_MAPPINGS_PATH)) -> dict:
    file_path = Path(path)
    if not file_path.exists():
        return {"global": {}, "venues": {}}
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    return {
        "global": {
            str(symbol).upper(): str(canonical).upper()
            for symbol, canonical in (payload.get("global") or {}).items()
        },
        "venues": {
            str(venue).lower(): {
                str(symbol).upper(): str(canonical).upper()
                for symbol, canonical in mappings.items()
            }
            for venue, mappings in (payload.get("venues") or {}).items()
            if isinstance(mappings, dict)
        },
        "venue_symbols": {
            str(venue).lower(): {
                str(canonical).upper(): str(native).upper()
                for canonical, native in mappings.items()
            }
            for venue, mappings in (payload.get("venue_symbols") or {}).items()
            if isinstance(mappings, dict)
        },
    }


def canonicalize_symbol(symbol: str, *, venue: str | None = None) -> str:
    normalized = str(symbol).upper()
    mappings = load_symbol_mappings()
    if venue is not None:
        venue_map = mappings["venues"].get(str(venue).lower(), {})
        if normalized in venue_map:
            return venue_map[normalized]
    return mappings["global"].get(normalized, normalized)


def venue_symbol(symbol: str, *, venue: str) -> str:
    normalized = str(symbol).upper()
    mappings = load_symbol_mappings()
    venue_map = mappings.get("venue_symbols", {}).get(str(venue).lower(), {})
    return venue_map.get(normalized, normalized)
