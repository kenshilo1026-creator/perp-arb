from __future__ import annotations

import asyncio
import argparse

import aiohttp

if __package__:
    from ._bootstrap import ensure_project_root_on_path
else:
    from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.adapters.registry import SYMBOL_DISCOVERERS
from hydra_basis.alerts import build_ranked_alert_digest, select_best_alerts_by_symbol, select_best_spot_perp_alerts_by_symbol
from hydra_basis.config import (
    FUNDING_HISTORY_PATH,
    MIN_SHARED_VENUES,
    MONITOR_SIGNALS_PATH,
    VENUE_CONFIG,
)
from hydra_basis.execution_engine.signal_store import save_monitor_signals
from hydra_basis.formatting import fmt_pct
from hydra_basis.funding_engine.analysis import analyze_positive_funding, analyze_spread, explain_spread_skip
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.history_store import (
    FundingHistoryStore,
    funding_history_is_complete,
    merge_points_by_interval_bucket,
    summarize_history_coverage,
    trim_points_to_analysis_days,
)
from hydra_basis.notifications.telegram import send_telegram
from hydra_basis.monitor_quotes import fetch_monitor_entry_books
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.symbol_mapping import canonicalize_symbol
from hydra_basis.universe import build_symbol_venue_index, select_shared_symbols, symbols_requiring_complete_history


load_environment()

LISTING_TIMEOUT_SECONDS = 20


async def filter_current_listings(venue_symbols: dict[str, set[str]]) -> dict[str, set[str]]:
    """Keep historical candidates only when their current listing is verified."""
    async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-monitor/0.1"}) as session:
        async def discover(venue: str, historical: set[str]) -> tuple[str, set[str]]:
            discoverer = SYMBOL_DISCOVERERS.get(venue)
            if discoverer is None:
                print(f"listing_unavailable venue={venue} reason=no_discoverer; skipping venue")
                return venue, set()
            try:
                listed = await asyncio.wait_for(discoverer(session), timeout=LISTING_TIMEOUT_SECONDS)
                if not isinstance(listed, set) or any(not isinstance(symbol, str) or not symbol for symbol in listed):
                    raise ValueError("invalid symbol listing")
                if not listed:
                    # Several adapters also return an empty set for incomplete API payloads.
                    raise ValueError("empty symbol listing")
                current = {canonicalize_symbol(symbol, venue=venue) for symbol in listed}
            except Exception as exc:
                print(f"listing_unavailable venue={venue} error={type(exc).__name__}: {exc}; skipping venue")
                return venue, set()
            for symbol in sorted(historical - current):
                print(f"listing_skip venue={venue} symbol={symbol} reason=not_in_current_listing")
            return venue, historical & current

        results = await asyncio.gather(*(
            discover(venue, symbols)
            for venue, symbols in venue_symbols.items()
            if venue in VENUE_CONFIG and VENUE_CONFIG[venue].enabled
        ))
    return dict(results)

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Analyze funding opportunities from stored history.")
    parser.add_argument("analysis_days", nargs="?", type=int, default=7, help="Analysis window in days (default: 7).")
    parser.add_argument("--required-venue", type=str.lower,
                        choices=[venue for venue, config in VENUE_CONFIG.items() if config.enabled],
                        help="Require this venue on either leg of each cross-exchange pair; "
                             "spot-perp results must use this perpetual venue.")
    args = parser.parse_args(argv)
    if args.analysis_days <= 0:
        parser.error("analysis_days must be positive")
    return args


async def run_once(analysis_days: int = 7, required_venue: str | None = None) -> None:
    if required_venue is not None:
        required_venue = required_venue.lower()
        if required_venue not in VENUE_CONFIG or not VENUE_CONFIG[required_venue].enabled:
            raise ValueError(f"required venue is unknown or disabled: {required_venue}")
        print(f"required venue: {required_venue} (either leg)")
    history_store = FundingHistoryStore(FUNDING_HISTORY_PATH)
    all_points: dict[tuple[str, str], list[FundingPoint]] = {
        key: merge_points_by_interval_bucket(points)
        for key, points in history_store.load().items()
    }

    venue_symbols: dict[str, set[str]] = {}
    for venue, symbol in all_points:
        venue_symbols.setdefault(venue, set()).add(symbol)
    venue_symbols = await filter_current_listings(venue_symbols)

    symbol_venues = build_symbol_venue_index(venue_symbols)
    symbols = select_shared_symbols(symbol_venues, min_shared_venues=MIN_SHARED_VENUES)
    if required_venue is not None:
        symbols = [symbol for symbol in symbols if symbol in venue_symbols.get(required_venue, set())]
    print(f"shared symbols ({len(symbols)}): {', '.join(symbols[:25])}{' ...' if len(symbols) > 25 else ''}")

    analysis_points = {
        key: trim_points_to_analysis_days(merge_points_by_interval_bucket(points), analysis_days=analysis_days)
        for key, points in all_points.items()
    }

    venues = [venue for venue, config in VENUE_CONFIG.items() if config.enabled]

    required_history_pairs = symbols_requiring_complete_history(
        venue_symbols,
        min_shared_venues=MIN_SHARED_VENUES,
    )
    for venue, symbol in sorted(required_history_pairs):
        if required_venue is not None and symbol not in symbols:
            continue
        points = analysis_points.get((venue, symbol), [])
        if not funding_history_is_complete(points, required_days=analysis_days):
            coverage = summarize_history_coverage(points, required_days=analysis_days)
            print(
                f"history_incomplete venue={venue} symbol={symbol}: "
                f"samples={coverage['samples']} "
                f"oldest_ts_ms={coverage['oldest_ts_ms']} "
                f"newest_ts_ms={coverage['newest_ts_ms']} "
                f"missing_ms={coverage['missing_ms']}"
            )

    opportunities: list[dict] = []
    spot_perp_opportunities: list[dict] = []

    scaled_min_obs = max(3, round(24 * analysis_days / 7))

    for venue in venues:
        if required_venue is not None and venue != required_venue:
            continue
        for symbol in venue_symbols.get(venue, set()):
            stats = analyze_positive_funding(
                analysis_points.get((venue, symbol), []),
                min_observations=scaled_min_obs,
                analysis_days=analysis_days,
            )
            if not stats:
                continue
            spot_perp_opportunities.append({"symbol": symbol, "venue": venue, "stats": stats})
            print(
                symbol,
                f"SPOT-PERP {venue}",
                f"avg_hourly={fmt_pct(stats['avg_hourly'])}",
                f"apy={fmt_pct(stats['annualized_avg'])}",
                f"pos={stats['positive_ratio']:.1%}",
                f"score={stats['score']:.2f}",
                f"signal={stats['signal']}",
            )

    for symbol in symbols:
        for short_venue in venues:
            for long_venue in venues:
                if short_venue == long_venue:
                    continue
                if required_venue is not None and required_venue not in (short_venue, long_venue):
                    continue
                if symbol not in venue_symbols.get(short_venue, set()):
                    continue
                if symbol not in venue_symbols.get(long_venue, set()):
                    continue

                stats = analyze_spread(
                    analysis_points.get((short_venue, symbol), []),
                    analysis_points.get((long_venue, symbol), []),
                    analysis_days=analysis_days,
                )
                if not stats:
                    reason = explain_spread_skip(
                        analysis_points.get((short_venue, symbol), []),
                        analysis_points.get((long_venue, symbol), []),
                        analysis_days=analysis_days,
                    )
                    print(symbol, f"SHORT {short_venue} / LONG {long_venue}", f"reason={reason}")
                    continue

                print(
                    symbol,
                    f"SHORT {short_venue} / LONG {long_venue}",
                    f"avg_hourly={fmt_pct(stats['avg_hourly'])}",
                    f"apy={fmt_pct(stats['annualized_avg'])}",
                    f"pos={stats['positive_ratio']:.1%}",
                    f"score={stats['score']:.2f}",
                    f"signal={stats['signal']}",
                )
                opportunities.append(
                    {"symbol": symbol, "short_venue": short_venue, "long_venue": long_venue, "stats": stats}
                )

    entry_books = await fetch_monitor_entry_books(opportunities)
    best_alerts = select_best_alerts_by_symbol(
        opportunities,
        min_annualized_avg=0.4,
        spreads_by_venue_symbol=entry_books,
    )
    best_spot_perp_alerts = select_best_spot_perp_alerts_by_symbol(spot_perp_opportunities, min_annualized_avg=0.4)
    funding_candidates = sum(row["stats"]["signal"] and row["stats"]["annualized_avg"] > 0.4 for row in opportunities)
    unconfirmed = sum(row["entry_estimate"]["status"] != "estimated" for row in best_alerts)
    print(f"alert_summary funding_candidates={funding_candidates} cross_after_gap_filter_and_ranking={len(best_alerts)} "
          f"entry_unconfirmed={unconfirmed} spot_perp={len(best_spot_perp_alerts)} maker_taker_gap_limit=0.30%")
    save_monitor_signals(
        path=MONITOR_SIGNALS_PATH,
        cross_exchange_signals=opportunities,
    )
    digest = build_ranked_alert_digest(
        cross_exchange_alerts=best_alerts,
        spot_perp_alerts=best_spot_perp_alerts,
    )
    if digest:
        await send_telegram(digest)


if __name__ == "__main__":
    args = parse_args()
    configure_windows_event_loop_policy()
    asyncio.run(run_once(analysis_days=args.analysis_days, required_venue=args.required_venue))
