from __future__ import annotations

import asyncio

import aiohttp

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.adapters.registry import FETCHERS, SYMBOL_DISCOVERERS
from hydra_basis.backfill import chunk_sequence, split_loris_batched_keys
from hydra_basis.config import FETCH_CONCURRENCY_LIMIT, FUNDING_HISTORY_LOOKBACK_DAYS, FUNDING_HISTORY_PATH, VENUE_CONFIG
from hydra_basis.env import load_environment
from hydra_basis.async_utils import gather_limited
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


load_environment()

BACKFILL_BATCH_SIZE = 30
BACKFILL_BATCH_SLEEP_SECONDS = 15


async def run_backfill() -> None:
    store = FundingHistoryStore(FUNDING_HISTORY_PATH)
    all_points = {
        key: merge_points_by_interval_bucket(points)
        for key, points in store.load().items()
    }

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
        for venue in enabled_venues:
            if venue not in FETCHERS:
                continue
            for symbol in sorted(venue_symbols.get(venue, set())):
                cached_points = trim_points_to_analysis_days(
                    merge_points_by_interval_bucket(all_points.get((venue, symbol), [])),
                    analysis_days=7,
                )
                if funding_history_is_complete(cached_points, required_days=7):
                    print(f"backfill skip cached complete {(venue, symbol)}")
                    continue
                pending_keys.append((venue, symbol))

        immediate_keys, loris_batched_keys = split_loris_batched_keys(pending_keys)

        if immediate_keys:
            print(f"backfill direct size={len(immediate_keys)}")
            tasks = [FETCHERS[venue](session, symbol) for venue, symbol in immediate_keys]
            results = await gather_limited(tasks, limit=FETCH_CONCURRENCY_LIMIT, return_exceptions=True)

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
                print(f"backfill stored {key}: {len(all_points[key])} points")

            store.save(all_points)

        batches = chunk_sequence(loris_batched_keys, chunk_size=BACKFILL_BATCH_SIZE)
        for batch_index, batch in enumerate(batches, start=1):
            print(f"backfill loris-batch {batch_index}/{len(batches)} size={len(batch)}")
            tasks = [FETCHERS[venue](session, symbol) for venue, symbol in batch]
            results = await gather_limited(tasks, limit=1, return_exceptions=True)

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
                print(f"backfill stored {key}: {len(all_points[key])} points")

            store.save(all_points)
            if batch_index < len(batches):
                print(f"backfill sleep {BACKFILL_BATCH_SLEEP_SECONDS}s")
                await asyncio.sleep(BACKFILL_BATCH_SLEEP_SECONDS)


if __name__ == "__main__":
    configure_windows_event_loop_policy()
    asyncio.run(run_backfill())
