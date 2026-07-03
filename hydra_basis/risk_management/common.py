from __future__ import annotations

from hydra_basis.funding_engine.analysis import now_ms  # re-exported for convenience

__all__ = ["now_ms", "closer_key_for_venue", "position_key"]


def closer_key_for_venue(venue: str) -> str:
    """Map a leg venue onto the key used to look up its closer/adapter.

    ``mexc_spot`` positions are closed through the shared ``mexc`` router.
    """
    normalized = venue.strip().lower()
    if normalized == "mexc_spot":
        return "mexc"
    return normalized


def position_key(*, venue: str, symbol: str, market_type: str, side: str) -> tuple[str, str, str, str]:
    return (
        venue.strip().lower(),
        symbol.strip().upper(),
        market_type.strip().lower(),
        side.strip().upper(),
    )
