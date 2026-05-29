from __future__ import annotations

import asyncio

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.alerts import build_ranked_alert_digest, select_best_alerts_by_symbol, select_best_spot_perp_alerts_by_symbol
from hydra_basis.config import (
    FUNDING_HISTORY_PATH,
    MIN_SHARED_VENUES,
    MONITOR_SIGNALS_PATH,
    ORDERBOOK_SPREADS_PATH,
    VENUE_CONFIG,
)
from hydra_basis.execution_engine.orderbook_spread_store import OrderbookSpreadStore
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
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.universe import build_symbol_venue_index, select_shared_symbols, symbols_requiring_complete_history


load_environment()

async def run_once() -> None:
    history_store = FundingHistoryStore(FUNDING_HISTORY_PATH)
    spread_store = OrderbookSpreadStore(ORDERBOOK_SPREADS_PATH)
    all_points: dict[tuple[str, str], list[FundingPoint]] = {
        key: merge_points_by_interval_bucket(points)
        for key, points in history_store.load().items()
    }
    all_spreads = spread_store.load()

    venue_symbols: dict[str, set[str]] = {}
    for venue, symbol in all_points:
        venue_symbols.setdefault(venue, set()).add(symbol)

    symbol_venues = build_symbol_venue_index(venue_symbols)
    symbols = select_shared_symbols(symbol_venues, min_shared_venues=MIN_SHARED_VENUES)
    print(f"shared symbols ({len(symbols)}): {', '.join(symbols[:25])}{' ...' if len(symbols) > 25 else ''}")

    analysis_points = {
        key: trim_points_to_analysis_days(merge_points_by_interval_bucket(points), analysis_days=7)
        for key, points in all_points.items()
    }

    venues = [venue for venue, config in VENUE_CONFIG.items() if config.enabled]

    required_history_pairs = symbols_requiring_complete_history(
        venue_symbols,
        min_shared_venues=MIN_SHARED_VENUES,
    )
    for venue, symbol in sorted(required_history_pairs):
        points = analysis_points.get((venue, symbol), [])
        if not funding_history_is_complete(points, required_days=7):
            coverage = summarize_history_coverage(points, required_days=7)
            print(
                f"history_incomplete venue={venue} symbol={symbol}: "
                f"samples={coverage['samples']} "
                f"oldest_ts_ms={coverage['oldest_ts_ms']} "
                f"newest_ts_ms={coverage['newest_ts_ms']} "
                f"missing_ms={coverage['missing_ms']}"
            )

    opportunities: list[dict] = []
    spot_perp_opportunities: list[dict] = []

    for venue in venues:
        for symbol in venue_symbols.get(venue, set()):
            stats = analyze_positive_funding(analysis_points.get((venue, symbol), []))
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
                if symbol not in venue_symbols.get(short_venue, set()):
                    continue
                if symbol not in venue_symbols.get(long_venue, set()):
                    continue

                stats = analyze_spread(
                    analysis_points.get((short_venue, symbol), []),
                    analysis_points.get((long_venue, symbol), []),
                )
                if not stats:
                    reason = explain_spread_skip(
                        analysis_points.get((short_venue, symbol), []),
                        analysis_points.get((long_venue, symbol), []),
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

    best_alerts = select_best_alerts_by_symbol(
        opportunities,
        min_annualized_avg=0.4,
        spreads_by_venue_symbol=all_spreads,
    )
    best_spot_perp_alerts = select_best_spot_perp_alerts_by_symbol(spot_perp_opportunities, min_annualized_avg=0.4)
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
    configure_windows_event_loop_policy()
    asyncio.run(run_once())
