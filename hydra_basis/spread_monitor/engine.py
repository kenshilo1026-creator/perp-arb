from __future__ import annotations

from hydra_basis.spread_monitor.models import SpreadMonitorConfig, VenueSpreadSnapshot
from hydra_basis.spread_monitor.selector import select_best_spread_opportunity
from hydra_basis.symbol_mapping import canonicalize_symbol


def build_symbol_snapshots(
    *,
    venues: list[str],
    asset_ctx_snapshots_by_venue: dict[str, dict[str, dict[str, float | int]]],
    quote_snapshots_by_venue: dict[str, dict[str, dict[str, float | int]]],
) -> dict[str, list[VenueSpreadSnapshot]]:
    by_symbol: dict[str, list[VenueSpreadSnapshot]] = {}
    for venue in venues:
        venue_snapshot = asset_ctx_snapshots_by_venue.get(venue, {})
        for symbol, ctx in venue_snapshot.items():
            quote_ctx = quote_snapshots_by_venue.get(venue, {}).get(symbol)
            if quote_ctx is None:
                continue
            canonical_symbol = canonicalize_symbol(symbol, venue=venue)
            snapshot = VenueSpreadSnapshot(
                venue=venue,
                symbol=canonical_symbol,
                mark_price=float(ctx.get("markPx") or ctx.get("midPx") or 0.0),
                bid_price=float(quote_ctx.get("bid") or 0.0),
                ask_price=float(quote_ctx.get("ask") or 0.0),
                funding_rate=float(ctx.get("funding") or 0.0),
                ts_ms=int(quote_ctx.get("ts_ms") or ctx.get("ts_ms") or 0),
            )
            by_symbol.setdefault(canonical_symbol, []).append(snapshot)
    return by_symbol


def select_best_spread_from_state(
    *,
    config: SpreadMonitorConfig,
    market_state_store,
    now_ms: int,
):
    asset_ctxs = {
        venue: market_state_store.get_asset_ctx_snapshot(venue)
        for venue in config.venues
    }
    quotes = {
        venue: market_state_store.get_quote_snapshot(venue)
        for venue in config.venues
    }
    symbol_snapshots = build_symbol_snapshots(
        venues=config.venues,
        asset_ctx_snapshots_by_venue=asset_ctxs,
        quote_snapshots_by_venue=quotes,
    )
    return select_best_spread_opportunity(
        symbol_snapshots=symbol_snapshots,
        config=config,
        now_ms=now_ms,
    )
