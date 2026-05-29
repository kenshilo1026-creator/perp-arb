from __future__ import annotations

import asyncio

import aiohttp

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.alerts import build_ranked_alert_digest, select_best_alerts_by_symbol, select_best_spot_perp_alerts_by_symbol
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_funding_since, fetch_hyperliquid_universe
from hydra_basis.adapters.registry import FETCHERS, SYMBOL_DISCOVERERS
from hydra_basis.async_utils import gather_limited
from hydra_basis.config import (
    FETCH_CONCURRENCY_LIMIT,
    FUNDING_HISTORY_LOOKBACK_DAYS,
    FUNDING_HISTORY_PATH,
    HYPERLIQUID_MAX_SYMBOLS_PER_RUN,
    MIN_SHARED_VENUES,
    VENUE_CONFIG,
)
from hydra_basis.formatting import fmt_pct
from hydra_basis.funding_engine.analysis import analyze_positive_funding, analyze_spread, explain_spread_skip
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.history_store import (
    FundingHistoryStore,
    funding_history_is_complete,
    merge_points_by_interval_bucket,
    summarize_history_coverage,
    trim_points_to_analysis_days,
    trim_points_to_lookback_ms,
)
from hydra_basis.monitor_errors import raise_exchange_error
from hydra_basis.notifications.telegram import send_telegram
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.streams.manager import (
    AsterStreamRunner,
    HyperliquidStreamRunner,
    LighterStreamRunner,
    MarketStateStore,
    MexcStreamRunner,
)
from hydra_basis.universe import build_symbol_venue_index, select_shared_symbols, symbols_requiring_complete_history


load_environment()

async def run_once() -> None:
    history_store = FundingHistoryStore(FUNDING_HISTORY_PATH)
    all_points: dict[tuple[str, str], list[FundingPoint]] = {
        key: merge_points_by_interval_bucket(points)
        for key, points in history_store.load().items()
    }
    venue_symbols: dict[str, set[str]] = {}
    opportunities: list[dict] = []
    spot_perp_opportunities: list[dict] = []
    fetched_keys_this_run: set[tuple[str, str]] = set()
    market_state = MarketStateStore()

    async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-monitor-demo/0.1"}) as session:
        hyperliquid_ordered_symbols = await fetch_hyperliquid_universe(session)
        hyperliquid_stream = HyperliquidStreamRunner(session, market_state, hyperliquid_ordered_symbols)
        lighter_stream = LighterStreamRunner(session, market_state)
        aster_stream = AsterStreamRunner(session, market_state)
        mexc_stream = MexcStreamRunner(session, market_state)
        streams = [hyperliquid_stream, lighter_stream, aster_stream, mexc_stream]
        try:
            await hyperliquid_stream.initialize()
            await lighter_stream.initialize()
            await aster_stream.initialize()
            await mexc_stream.initialize()

            async def pump_stream(stream, *, timeout_seconds: float = 3.0) -> None:
                try:
                    await asyncio.wait_for(stream.pump_once(), timeout=timeout_seconds)
                except Exception as exc:
                    venue_name = stream.__class__.__name__.replace("StreamRunner", "").lower()
                    raise_exchange_error(phase="websocket", venue=venue_name, symbol=None, error=exc)

            for _ in range(6):
                await pump_stream(hyperliquid_stream)
                await pump_stream(lighter_stream)
                await pump_stream(aster_stream)
                await pump_stream(mexc_stream)
                if (
                    market_state.get_asset_ctx_snapshot("hyperliquid")
                    and market_state.get_asset_ctx_snapshot("lighter")
                    and market_state.get_asset_ctx_snapshot("aster")
                    and market_state.get_asset_ctx_snapshot("mexc")
                ):
                    break

            enabled_venues = [venue for venue, config in VENUE_CONFIG.items() if config.enabled]

            discovered_venues = []
            discovery_tasks = []
            for venue in enabled_venues:
                if venue not in SYMBOL_DISCOVERERS:
                    continue
                if venue in {"aster", "mexc"} and market_state.get_symbols(venue):
                    continue
                discovered_venues.append(venue)
                discovery_tasks.append(SYMBOL_DISCOVERERS[venue](session))

            discovery_results = await asyncio.gather(*discovery_tasks, return_exceptions=True)

            for venue in enabled_venues:
                if venue in {"aster", "mexc"}:
                    ws_symbols = market_state.get_symbols(venue)
                    if ws_symbols:
                        venue_symbols[venue] = ws_symbols
                        print(f"discovered {venue} via websocket: {len(ws_symbols)} symbols")

            for venue, result in zip(discovered_venues, discovery_results):
                if isinstance(result, Exception):
                    raise_exchange_error(phase="discovery", venue=venue, symbol=None, error=result)
                else:
                    venue_symbols[venue] = result
                    print(f"discovered {venue}: {len(result)} symbols")

            symbol_venues = build_symbol_venue_index(venue_symbols)
            symbols = select_shared_symbols(symbol_venues, min_shared_venues=MIN_SHARED_VENUES)
            print(f"shared symbols ({len(symbols)}): {', '.join(symbols[:25])}{' ...' if len(symbols) > 25 else ''}")
            await mexc_stream.subscribe_symbols(sorted(set(symbols) & venue_symbols.get("mexc", set())))
            for _ in range(6):
                await pump_stream(mexc_stream)
                if market_state.get_asset_ctx_snapshot("mexc"):
                    break

            tasks = []
            keys = []
            hyperliquid_symbols_seen = 0
            for venue in enabled_venues:
                supported_symbols = venue_symbols.get(venue, set())
                for symbol in symbols:
                    if symbol not in supported_symbols:
                        continue
                    if venue == "hyperliquid":
                        if hyperliquid_symbols_seen >= HYPERLIQUID_MAX_SYMBOLS_PER_RUN:
                            continue
                        existing_points = all_points.get((venue, symbol), [])
                        if existing_points:
                            continue
                        start_time_ms = None
                        keys.append((venue, symbol))
                        if start_time_ms is None:
                            tasks.append(FETCHERS[venue](session, symbol))
                        hyperliquid_symbols_seen += 1
                        continue
                    if venue == "lighter":
                        existing_points = all_points.get((venue, symbol), [])
                        if existing_points:
                            continue
                    if venue == "mexc":
                        continue
                    if venue == "aster":
                        existing_points = all_points.get((venue, symbol), [])
                        if existing_points:
                            continue
                    keys.append((venue, symbol))
                    tasks.append(FETCHERS[venue](session, symbol))

            results = await gather_limited(tasks, limit=FETCH_CONCURRENCY_LIMIT, return_exceptions=True)
        finally:
            for stream in streams:
                try:
                    await stream.close()
                except Exception as exc:
                    print(f"stream close error {stream.__class__.__name__}: {exc!r}")

    for key, result in zip(keys, results):
        if isinstance(result, Exception):
            raise_exchange_error(phase="fetch", venue=key[0], symbol=key[1], error=result)
        else:
            existing = all_points.get(key, [])
            merged_by_ts = {point.ts_ms: point for point in existing}
            for point in result:
                merged_by_ts[point.ts_ms] = point
            merged_points = merge_points_by_interval_bucket(
                sorted(merged_by_ts.values(), key=lambda point: point.ts_ms)
            )
            all_points[key] = trim_points_to_lookback_ms(
                merged_points,
                lookback_ms=FUNDING_HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
            )
            fetched_keys_this_run.add(key)
            print(f"fetched {key}: {len(result)} new points, {len(all_points[key])} stored")

    trimmed_all_points = {
        key: trim_points_to_lookback_ms(
            points,
            lookback_ms=FUNDING_HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
        )
        for key, points in all_points.items()
    }
    all_points = trimmed_all_points

    hyperliquid_asset_ctx_snapshot = market_state.get_asset_ctx_snapshot("hyperliquid")
    for symbol, ctx in hyperliquid_asset_ctx_snapshot.items():
        key = ("hyperliquid", symbol)
        existing = all_points.get(key, [])
        latest_ts = existing[-1].ts_ms if existing else 0
        ctx_ts = int(ctx["ts_ms"])
        if ctx_ts <= latest_ts:
            continue
        interval_hours = existing[-1].interval_hours if existing else 1.0
        existing.append(
            FundingPoint(
                venue="hyperliquid",
                symbol=symbol,
                ts_ms=ctx_ts,
                raw_rate=float(ctx["funding"]),
                interval_hours=interval_hours,
            )
        )
        all_points[key] = trim_points_to_lookback_ms(
            merge_points_by_interval_bucket(sorted(existing, key=lambda point: point.ts_ms)),
            lookback_ms=FUNDING_HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
        )

    lighter_asset_ctx_snapshot = market_state.get_asset_ctx_snapshot("lighter")
    for symbol, ctx in lighter_asset_ctx_snapshot.items():
        key = ("lighter", symbol)
        existing = all_points.get(key, [])
        latest_ts = existing[-1].ts_ms if existing else 0
        ctx_ts = int(ctx["ts_ms"])
        if ctx_ts <= latest_ts:
            continue
        interval_hours = existing[-1].interval_hours if existing else 1.0
        existing.append(
            FundingPoint(
                venue="lighter",
                symbol=symbol,
                ts_ms=ctx_ts,
                raw_rate=float(ctx["funding"]),
                interval_hours=interval_hours,
            )
        )
        all_points[key] = trim_points_to_lookback_ms(
            merge_points_by_interval_bucket(sorted(existing, key=lambda point: point.ts_ms)),
            lookback_ms=FUNDING_HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
        )
        fetched_keys_this_run.add(key)

    aster_asset_ctx_snapshot = market_state.get_asset_ctx_snapshot("aster")
    for symbol, ctx in aster_asset_ctx_snapshot.items():
        key = ("aster", symbol)
        existing = all_points.get(key, [])
        latest_ts = existing[-1].ts_ms if existing else 0
        ctx_ts = int(ctx["ts_ms"])
        if ctx_ts <= latest_ts:
            continue
        interval_hours = existing[-1].interval_hours if existing else 8.0
        existing.append(
            FundingPoint(
                venue="aster",
                symbol=symbol,
                ts_ms=ctx_ts,
                raw_rate=float(ctx["funding"]),
                interval_hours=interval_hours,
            )
        )
        all_points[key] = trim_points_to_lookback_ms(
            merge_points_by_interval_bucket(sorted(existing, key=lambda point: point.ts_ms)),
            lookback_ms=FUNDING_HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
        )
        fetched_keys_this_run.add(key)

    mexc_asset_ctx_snapshot = market_state.get_asset_ctx_snapshot("mexc")
    for symbol, ctx in mexc_asset_ctx_snapshot.items():
        key = ("mexc", symbol)
        existing = all_points.get(key, [])
        latest_ts = existing[-1].ts_ms if existing else 0
        ctx_ts = int(ctx["ts_ms"])
        if ctx_ts <= latest_ts:
            continue
        interval_hours = existing[-1].interval_hours if existing else 8.0
        existing.append(
            FundingPoint(
                venue="mexc",
                symbol=symbol,
                ts_ms=ctx_ts,
                raw_rate=float(ctx["funding"]),
                interval_hours=interval_hours,
            )
        )
        all_points[key] = trim_points_to_lookback_ms(
            merge_points_by_interval_bucket(sorted(existing, key=lambda point: point.ts_ms)),
            lookback_ms=FUNDING_HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
        )
        fetched_keys_this_run.add(key)

    history_store.save(all_points)

    analysis_points = {
        key: trim_points_to_analysis_days(merge_points_by_interval_bucket(points), analysis_days=7)
        for key, points in all_points.items()
    }

    hyperliquid_mid_snapshot = market_state.get_mid_snapshot("hyperliquid")
    if hyperliquid_mid_snapshot:
        btc_mid = hyperliquid_mid_snapshot.get("BTC")
        if btc_mid:
            print(f"hyperliquid ws mid BTC={btc_mid['price']} ts={btc_mid['ts_ms']}")

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

    for venue in venues:
        for symbol in venue_symbols.get(venue, set()):
            stats = analyze_positive_funding(analysis_points.get((venue, symbol), []))
            if not stats:
                continue
            spot_perp_opportunities.append(
                {
                    "symbol": symbol,
                    "venue": venue,
                    "stats": stats,
                }
            )
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
                    print(
                        symbol,
                        f"SHORT {short_venue} / LONG {long_venue}",
                        f"reason={reason}",
                    )
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
                    {
                        "symbol": symbol,
                        "short_venue": short_venue,
                        "long_venue": long_venue,
                        "stats": stats,
                    }
                )

    best_alerts = select_best_alerts_by_symbol(opportunities, min_annualized_avg=0.4)

    best_spot_perp_alerts = select_best_spot_perp_alerts_by_symbol(
        spot_perp_opportunities,
        min_annualized_avg=0.4,
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
