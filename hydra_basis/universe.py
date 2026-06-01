from __future__ import annotations

from hydra_basis.symbol_mapping import canonicalize_symbol


def build_symbol_venue_index(venue_symbols: dict[str, set[str]]) -> dict[str, set[str]]:
    symbol_venues: dict[str, set[str]] = {}
    for venue, symbols in venue_symbols.items():
        for symbol in symbols:
            canonical_symbol = canonicalize_symbol(symbol, venue=venue)
            symbol_venues.setdefault(canonical_symbol, set()).add(venue)
    return symbol_venues


def select_shared_symbols(symbol_venues: dict[str, set[str]], min_shared_venues: int = 2) -> list[str]:
    return sorted(
        symbol
        for symbol, venues in symbol_venues.items()
        if len(venues) >= min_shared_venues
    )


def symbols_requiring_complete_history(
    venue_symbols: dict[str, set[str]],
    *,
    min_shared_venues: int = 2,
) -> set[tuple[str, str]]:
    symbol_venues = build_symbol_venue_index(venue_symbols)
    shared_symbols = select_shared_symbols(symbol_venues, min_shared_venues=min_shared_venues)
    required: set[tuple[str, str]] = set()
    for symbol in shared_symbols:
        for venue in symbol_venues.get(symbol, set()):
            required.add((venue, symbol))
    return required
