from __future__ import annotations

import asyncio
import html

import aiohttp

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.adapters.registry import FETCHERS, FETCHERS_SINCE, SYMBOL_DISCOVERERS
from hydra_basis.backfill import (
    build_spread_refresh_keys,
    build_no_new_points_warning,
    chunk_sequence,
    split_loris_batched_keys,
    capture_backfill_spread_snapshot_with_error,
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
from hydra_basis.notifications.telegram import send_telegram
from hydra_basis.runtime import configure_windows_event_loop_policy
from hydra_basis.funding_engine.analysis import now_ms


load_environment()

BACKFILL_BATCH_SIZE = 30
BACKFILL_BATCH_SLEEP_SECONDS = 0
PERSIST_EVERY_N = 200
SPREAD_REFRESH_CONCURRENCY_BY_VENUE = {
    "lighter": 1,
}
SPREAD_REFRESH_DELAY_BY_VENUE_SECONDS = {
    "lighter": 1.0,
}
SPREAD_ERROR_ALERT_MAX_ITEMS = 10


async def capture_spread_snapshot_with_venue_delay(
    *,
    session,
    spreads,
    venue: str,
    symbol: str,
) -> dict[str, object]:
    result = await capture_backfill_spread_snapshot_with_error(
        session=session,
        spreads=spreads,
        venue=venue,
        symbol=symbol,
        clip_usd=BACKFILL_SPREAD_CLIP_USD,
        force_refresh=True,
    )
    delay = SPREAD_REFRESH_DELAY_BY_VENUE_SECONDS.get(venue, 0.0)
    if delay > 0:
        await asyncio.sleep(delay)
    return result


async def send_spread_error_alert(
    *,
    venue: str,
    batch_index: int,
    batch_count: int,
    errors: list[dict[str, object]],
) -> None:
    if not errors:
        return
    transient_errors = [
        item for item in errors
        if item.get("error_type") == "transient"
    ]
    if not transient_errors:
        return

    lines = [
        "<b>Backfill spread 抓取錯誤</b>",
        f"交易所: <code>{html.escape(venue)}</code>",
        f"批次: {batch_index}/{batch_count}",
        f"錯誤數: {len(transient_errors)}",
        "處理: 已保留舊 spread 快取，backfill 繼續；Lighter 已單線程並節流。",
    ]
    for item in transient_errors[:SPREAD_ERROR_ALERT_MAX_ITEMS]:
        symbol = html.escape(str(item.get("symbol", "")))
        error = html.escape(str(item.get("error", "")))[:240]
        lines.append(f"<code>{symbol}</code>: {error}")
    if len(transient_errors) > SPREAD_ERROR_ALERT_MAX_ITEMS:
        lines.append(f"...另外 {len(transient_errors) - SPREAD_ERROR_ALERT_MAX_ITEMS} 個錯誤")

    try:
        await send_telegram("\n".join(lines))
    except Exception as exc:
        print(f"backfill spread telegram alert failed: {exc!r}")


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
                if not result:
                    coverage = summarize_history_coverage(
                        trim_points_to_analysis_days(
                            merge_points_by_interval_bucket(all_points.get(key, [])),
                            analysis_days=7,
                        ),
                        required_days=7,
                    )
                    warning = build_no_new_points_warning(
                        venue=key[0],
                        symbol=key[1],
                        start_ms=incremental_starts.get(key),
                        end_ms=current_now_ms,
                        coverage=coverage,
                    )
                    print(warning)
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
                if not result:
                    coverage = summarize_history_coverage(
                        trim_points_to_analysis_days(
                            merge_points_by_interval_bucket(all_points.get(key, [])),
                            analysis_days=7,
                        ),
                        required_days=7,
                    )
                    warning = build_no_new_points_warning(
                        venue=key[0],
                        symbol=key[1],
                        start_ms=incremental_starts.get(key),
                        end_ms=current_now_ms,
                        coverage=coverage,
                    )
                    print(warning)
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

        if spread_refresh_keys:
            spread_keys_by_venue: dict[str, list[tuple[str, str]]] = {}
            for key in spread_refresh_keys:
                spread_keys_by_venue.setdefault(key[0], []).append(key)
            for venue, venue_keys in spread_keys_by_venue.items():
                spread_batches = chunk_sequence(venue_keys, chunk_size=PERSIST_EVERY_N)
                spread_limit = SPREAD_REFRESH_CONCURRENCY_BY_VENUE.get(venue, FETCH_CONCURRENCY_LIMIT)
                for batch_index, batch in enumerate(spread_batches, start=1):
                    print(
                        f"backfill spread-batch venue={venue} "
                        f"{batch_index}/{len(spread_batches)} size={len(batch)} "
                        f"concurrency={spread_limit}"
                    )
                    tasks = [
                        capture_spread_snapshot_with_venue_delay(
                            session=session,
                            spreads=all_spreads,
                            venue=item_venue,
                            symbol=symbol,
                        )
                        for item_venue, symbol in batch
                    ]
                    results = await gather_limited(
                        tasks,
                        limit=spread_limit,
                        return_exceptions=True,
                    )
                    spread_errors: list[dict[str, object]] = []
                    for key, result in zip(batch, results):
                        if isinstance(result, Exception):
                            print(f"backfill spread failed {key}: {result!r}")
                            spread_errors.append({
                                "venue": key[0],
                                "symbol": key[1],
                                "error": repr(result),
                                "error_type": "task_exception",
                            })
                            continue
                        if result.get("error"):
                            print(
                                "backfill spread error "
                                f"{key}: type={result.get('error_type')} "
                                f"{result.get('error')}"
                            )
                            spread_errors.append(result)
                    await send_spread_error_alert(
                        venue=venue,
                        batch_index=batch_index,
                        batch_count=len(spread_batches),
                        errors=spread_errors,
                    )
                    persist_backfill_progress(
                        history_store=store,
                        spread_store=spread_store,
                        funding_points=all_points,
                        spreads=all_spreads,
                    )


if __name__ == "__main__":
    configure_windows_event_loop_policy()
    asyncio.run(run_backfill())
