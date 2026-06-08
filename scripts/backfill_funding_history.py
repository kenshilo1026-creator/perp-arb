from __future__ import annotations

import asyncio

import aiohttp

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.adapters.registry import FETCHERS, FETCHERS_SINCE, SYMBOL_DISCOVERERS
from hydra_basis.backfill import (
    build_spread_refresh_keys,
    chunk_sequence,
    split_loris_batched_keys,
    capture_backfill_spread_snapshot,
    persist_backfill_progress,
    backfill_incremental_start_ms,
    backfill_needs_top_up,
)
from hydra_basis.config import (
    BACKFILL_SPREAD_CLIP_USD,
    FETCH_CONCURRENCY_LIMIT,
    FUNDING_HISTORY_LOOKBACK_DAYS,
    FUNDING_HISTORY_PATH,
    ORDERBOOK_SPREADS_PATH,
    VENUE_CONFIG,
)
from hydra_basis.env import load_environment
from hydra_basis.async_utils import gather_limited
from hydra_basis.execution_engine.orderbook_spread_store import OrderbookSpreadStore
from hydra_basis.history_store import (
    FundingHistoryStore,
    funding_history_is_complete,
    merge_points_by_interval_bucket,
    summarize_history_coverage,
    trim_points_to_analysis_days,
    trim_points_to_lookback_ms,
)
from hydra_basis.monitor_errors import should_raise_immediately
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.funding_engine.analysis import now_ms


load_environment()

BACKFILL_BATCH_SIZE = 30
BACKFILL_BATCH_SLEEP_SECONDS = 15
PERSIST_EVERY_N = 200


async def run_backfill() -> None:
    store = FundingHistoryStore(FUNDING_HISTORY_PATH)
    spread_store = OrderbookSpreadStore(ORDERBOOK_SPREADS_PATH)
    all_points = {
        key: merge_points_by_interval_bucket(points)
        for key, points in store.load().items()
    }
    all_spreads = spread_store.load()
    current_now_ms = now_ms()

    async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-backfill/0.1"}) as session:
        enabled_venues = [venue for venue, config in VENUE_CONFIG.items() if config.enabled]
        venue_symbols: dict[str, set[str]] = {}

        for venue in enabled_venues:
            if venue not in SYMBOL_DISCOVERERS:
                continue
            symbols = await SYMBOL_DISCOVERERS[venue](session)
            venue_symbols[venue] = symbols
            print(f"backfill discovered {venue}: {len(symbols)} symbols")

        pending_keys = []
        incremental_starts: dict[tuple[str, str], int] = {}
        skipped_complete = 0
        top_up_scheduled = 0
        full_backfill_scheduled = 0
        for venue in enabled_venues:
            if venue not in FETCHERS:
                continue
            for symbol in sorted(venue_symbols.get(venue, set())):
                merged_cached_points = merge_points_by_interval_bucket(all_points.get((venue, symbol), []))
                cached_points = trim_points_to_analysis_days(
                    merged_cached_points,
                    analysis_days=7,
                )
                if funding_history_is_complete(cached_points, required_days=7):
                    if backfill_needs_top_up(cached_points, now_ms=current_now_ms):
                        start_ms = backfill_incremental_start_ms(merged_cached_points)
                        if start_ms is not None:
                            pending_keys.append((venue, symbol))
                            incremental_starts[(venue, symbol)] = start_ms
                            top_up_scheduled += 1
                    else:
                        skipped_complete += 1
                    continue
                pending_keys.append((venue, symbol))
                full_backfill_scheduled += 1
                if venue in FETCHERS_SINCE:
                    start_ms = backfill_incremental_start_ms(merged_cached_points)
                    if start_ms is not None:
                        incremental_starts[(venue, symbol)] = start_ms

        print(
            "backfill summary "
            f"skip_complete={skipped_complete} "
            f"top_up={top_up_scheduled} "
            f"full={full_backfill_scheduled}"
        )
        spread_refresh_keys = build_spread_refresh_keys(
            venue_symbols,
            enabled_venues=enabled_venues,
            supported_venues=set(FETCHERS),
        )
        print(f"backfill spread refresh size={len(spread_refresh_keys)}")

        immediate_keys, loris_batched_keys = split_loris_batched_keys(pending_keys)

        if immediate_keys:
            print(f"backfill direct size={len(immediate_keys)}")
            tasks = []
            for venue, symbol in immediate_keys:
                start_ms = incremental_starts.get((venue, symbol))
                if start_ms is not None and venue in FETCHERS_SINCE:
                    tasks.append(FETCHERS_SINCE[venue](session, symbol, start_ms))
                else:
                    tasks.append(FETCHERS[venue](session, symbol))
            results = await gather_limited(tasks, limit=FETCH_CONCURRENCY_LIMIT, return_exceptions=True)

            dirty = 0
            for key, result in zip(immediate_keys, results):
                if isinstance(result, Exception):
                    if should_raise_immediately(result):
                        raise result
                    coverage = summarize_history_coverage(
                        trim_points_to_analysis_days(
                            merge_points_by_interval_bucket(all_points.get(key, [])),
                            analysis_days=7,
                        ),
                        required_days=7,
                    )
                    print(
                        f"backfill failed {key}: {result!r} "
                        f"samples={coverage['samples']} "
                        f"oldest_ts_ms={coverage['oldest_ts_ms']} "
                        f"newest_ts_ms={coverage['newest_ts_ms']} "
                        f"missing_ms={coverage['missing_ms']}"
                    )
                    continue
                merged = merge_points_by_interval_bucket(all_points.get(key, []) + result)
                all_points[key] = trim_points_to_lookback_ms(
                    merged,
                    lookback_ms=FUNDING_HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
                )
                dirty += 1
                if dirty % PERSIST_EVERY_N == 0:
                    persist_backfill_progress(
                        history_store=store,
                        spread_store=spread_store,
                        funding_points=all_points,
                        spreads=all_spreads,
                    )
            if dirty % PERSIST_EVERY_N != 0:
                persist_backfill_progress(
                    history_store=store,
                    spread_store=spread_store,
                    funding_points=all_points,
                    spreads=all_spreads,
                )
                #print(f"backfill stored {key}: {len(all_points[key])} points")

        batches = chunk_sequence(loris_batched_keys, chunk_size=BACKFILL_BATCH_SIZE)
        for batch_index, batch in enumerate(batches, start=1):
            print(f"backfill loris-batch {batch_index}/{len(batches)} size={len(batch)}")
            tasks = []
            for venue, symbol in batch:
                start_ms = incremental_starts.get((venue, symbol))
                if start_ms is not None and venue in FETCHERS_SINCE:
                    tasks.append(FETCHERS_SINCE[venue](session, symbol, start_ms))
                else:
                    tasks.append(FETCHERS[venue](session, symbol))
            results = await gather_limited(tasks, limit=1, return_exceptions=True)

            dirty = 0
            for key, result in zip(batch, results):
                if isinstance(result, Exception):
                    if should_raise_immediately(result):
                        raise result
                    coverage = summarize_history_coverage(
                        trim_points_to_analysis_days(
                            merge_points_by_interval_bucket(all_points.get(key, [])),
                            analysis_days=7,
                        ),
                        required_days=7,
                    )
                    print(
                        f"backfill failed {key}: {result!r} "
                        f"samples={coverage['samples']} "
                        f"oldest_ts_ms={coverage['oldest_ts_ms']} "
                        f"newest_ts_ms={coverage['newest_ts_ms']} "
                        f"missing_ms={coverage['missing_ms']}"
                    )
                    continue
                merged = merge_points_by_interval_bucket(all_points.get(key, []) + result)
                all_points[key] = trim_points_to_lookback_ms(
                    merged,
                    lookback_ms=FUNDING_HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
                )
                dirty += 1
                if dirty % PERSIST_EVERY_N == 0:
                    persist_backfill_progress(
                        history_store=store,
                        spread_store=spread_store,
                        funding_points=all_points,
                        spreads=all_spreads,
                    )
            if dirty % PERSIST_EVERY_N != 0:
                persist_backfill_progress(
                    history_store=store,
                    spread_store=spread_store,
                    funding_points=all_points,
                    spreads=all_spreads,
                )
                #print(f"backfill stored {key}: {len(all_points[key])} points")
            if batch_index < len(batches):
                print(f"backfill sleep {BACKFILL_BATCH_SLEEP_SECONDS}s")
                await asyncio.sleep(BACKFILL_BATCH_SLEEP_SECONDS)

        if spread_refresh_keys:
            spread_batches = chunk_sequence(spread_refresh_keys, chunk_size=PERSIST_EVERY_N)
            for batch_index, batch in enumerate(spread_batches, start=1):
                print(f"backfill spread-batch {batch_index}/{len(spread_batches)} size={len(batch)}")
                tasks = [
                    capture_backfill_spread_snapshot(
                        session=session,
                        spreads=all_spreads,
                        venue=venue,
                        symbol=symbol,
                        clip_usd=BACKFILL_SPREAD_CLIP_USD,
                        force_refresh=True,
                    )
                    for venue, symbol in batch
                ]
                results = await gather_limited(
                    tasks,
                    limit=FETCH_CONCURRENCY_LIMIT,
                    return_exceptions=True,
                )
                for key, result in zip(batch, results):
                    if isinstance(result, Exception):
                        if should_raise_immediately(result):
                            raise result
                        print(f"backfill spread failed {key}: {result!r}")
                persist_backfill_progress(
                    history_store=store,
                    spread_store=spread_store,
                    funding_points=all_points,
                    spreads=all_spreads,
                )


if __name__ == "__main__":
    configure_windows_event_loop_policy()
    asyncio.run(run_backfill())
