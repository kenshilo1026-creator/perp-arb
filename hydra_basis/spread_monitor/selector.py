from __future__ import annotations

from hydra_basis.spread_monitor.models import (
    SpreadFilterDiagnostic,
    SpreadMonitorConfig,
    SpreadOpportunity,
    SpreadPairFilterDiagnostic,
    VenueSpreadSnapshot,
)


def compute_cross_exchange_spread_pct(*, short_price: float, long_price: float) -> float:
    if short_price <= 0 or long_price <= 0:
        raise RuntimeError("prices must be positive")
    return (short_price - long_price) / long_price


def estimate_expected_profit_usd(*, execution_notional_usd: float, cross_exchange_spread_pct: float) -> float:
    return execution_notional_usd * cross_exchange_spread_pct


def snapshot_is_fresh(snapshot: VenueSpreadSnapshot, *, now_ms: int, max_stale_seconds: int) -> bool:
    return snapshot.ts_ms >= now_ms - max_stale_seconds * 1000


def compute_orderbook_spread_pct(snapshot: VenueSpreadSnapshot) -> float | None:
    if snapshot.bid_price <= 0 or snapshot.ask_price <= 0:
        return None
    mid = (snapshot.bid_price + snapshot.ask_price) / 2
    if mid <= 0:
        return None
    return (snapshot.ask_price - snapshot.bid_price) / mid


def compute_price_deviation_pct(left_price: float, right_price: float) -> float | None:
    if left_price <= 0 or right_price <= 0:
        return None
    reference = (left_price + right_price) / 2
    if reference <= 0:
        return None
    return abs(left_price - right_price) / reference


def snapshot_passes_filters(snapshot: VenueSpreadSnapshot, *, config: SpreadMonitorConfig) -> bool:
    if abs(snapshot.funding_rate) > config.max_funding_rate_pct / 100:
        return False
    if compute_orderbook_spread_pct(snapshot) is None:
        return False
    return True


def select_best_spread_opportunity(
    *,
    symbol_snapshots: dict[str, list[VenueSpreadSnapshot]],
    config: SpreadMonitorConfig,
    now_ms: int,
) -> SpreadOpportunity | None:
    best: SpreadOpportunity | None = None
    min_spread_limit = config.max_orderbook_spread_pct / 100
    min_cross_threshold = config.min_cross_exchange_spread_pct / 100
    max_price_deviation = config.max_price_deviation_pct / 100

    for symbol, snapshots in symbol_snapshots.items():
        eligible = [
            snapshot
            for snapshot in snapshots
            if snapshot.venue in config.venues
            and snapshot_is_fresh(snapshot, now_ms=now_ms, max_stale_seconds=config.max_stale_seconds)
            and snapshot_passes_filters(snapshot, config=config)
        ]
        if len(eligible) < 2:
            continue

        for left in eligible:
            for right in eligible:
                if left.venue == right.venue:
                    continue
                price_deviation_pct = compute_price_deviation_pct(left.mark_price, right.mark_price)
                if price_deviation_pct is None or price_deviation_pct > max_price_deviation:
                    continue
                if left.bid_price <= right.ask_price:
                    continue
                left_spread_pct = compute_orderbook_spread_pct(left)
                right_spread_pct = compute_orderbook_spread_pct(right)
                if not (
                    (left_spread_pct is not None and left_spread_pct <= min_spread_limit)
                    or (right_spread_pct is not None and right_spread_pct <= min_spread_limit)
                ):
                    continue
                spread_pct = compute_cross_exchange_spread_pct(
                    short_price=left.bid_price,
                    long_price=right.ask_price,
                )
                if spread_pct < min_cross_threshold:
                    continue
                candidate = SpreadOpportunity(
                    symbol=symbol,
                    short_venue=left.venue,
                    long_venue=right.venue,
                    short_price=left.bid_price,
                    long_price=right.ask_price,
                    cross_exchange_spread_pct=spread_pct,
                    short_funding_rate=left.funding_rate,
                    long_funding_rate=right.funding_rate,
                    short_orderbook_spread_pct=left_spread_pct,
                    long_orderbook_spread_pct=right_spread_pct,
                    execution_notional_usd=config.execution_notional_usd,
                    expected_profit_usd=estimate_expected_profit_usd(
                        execution_notional_usd=config.execution_notional_usd,
                        cross_exchange_spread_pct=spread_pct,
                    ),
                )
                if best is None or candidate.cross_exchange_spread_pct > best.cross_exchange_spread_pct:
                    best = candidate
    return best


def collect_spread_filter_diagnostics(
    *,
    symbol_snapshots: dict[str, list[VenueSpreadSnapshot]],
    config: SpreadMonitorConfig,
    now_ms: int,
) -> list[SpreadFilterDiagnostic]:
    diagnostics: list[SpreadFilterDiagnostic] = []
    min_spread_limit = config.max_orderbook_spread_pct / 100
    min_cross_threshold = config.min_cross_exchange_spread_pct / 100
    max_price_deviation = config.max_price_deviation_pct / 100

    for symbol, snapshots in symbol_snapshots.items():
        venue_snapshots = [snapshot for snapshot in snapshots if snapshot.venue in config.venues]
        if len(venue_snapshots) < 2:
            diagnostics.append(SpreadFilterDiagnostic(symbol, "less_than_2_venues"))
            continue

        fresh_snapshots = [
            snapshot
            for snapshot in venue_snapshots
            if snapshot_is_fresh(snapshot, now_ms=now_ms, max_stale_seconds=config.max_stale_seconds)
        ]
        if len(fresh_snapshots) < 2:
            diagnostics.append(SpreadFilterDiagnostic(symbol, "stale_quotes"))
            continue

        funding_snapshots = [
            snapshot
            for snapshot in fresh_snapshots
            if abs(snapshot.funding_rate) <= config.max_funding_rate_pct / 100
        ]
        if len(funding_snapshots) < 2:
            diagnostics.append(SpreadFilterDiagnostic(symbol, "funding_rate_too_high"))
            continue

        valid_quote_snapshots = [
            snapshot
            for snapshot in funding_snapshots
            if compute_orderbook_spread_pct(snapshot) is not None
        ]
        if len(valid_quote_snapshots) < 2:
            diagnostics.append(SpreadFilterDiagnostic(symbol, "missing_bid_ask"))
            continue

        symbol_has_positive_executable_spread = False
        symbol_has_tight_spread_side = False
        symbol_has_threshold_pass = False

        for left in valid_quote_snapshots:
            for right in valid_quote_snapshots:
                if left.venue == right.venue:
                    continue
                left_spread_pct = compute_orderbook_spread_pct(left)
                right_spread_pct = compute_orderbook_spread_pct(right)
                if left.bid_price > right.ask_price:
                    symbol_has_positive_executable_spread = True
                price_deviation_pct = compute_price_deviation_pct(left.mark_price, right.mark_price)
                if price_deviation_pct is None or price_deviation_pct > max_price_deviation:
                    continue
                if (
                    (left_spread_pct is not None and left_spread_pct <= min_spread_limit)
                    or (right_spread_pct is not None and right_spread_pct <= min_spread_limit)
                ):
                    symbol_has_tight_spread_side = True
                if left.bid_price <= right.ask_price:
                    continue
                if not (
                    (left_spread_pct is not None and left_spread_pct <= min_spread_limit)
                    or (right_spread_pct is not None and right_spread_pct <= min_spread_limit)
                ):
                    continue
                spread_pct = compute_cross_exchange_spread_pct(
                    short_price=left.bid_price,
                    long_price=right.ask_price,
                )
                if spread_pct >= min_cross_threshold:
                    symbol_has_threshold_pass = True
                    break
            if symbol_has_threshold_pass:
                break

        if not symbol_has_positive_executable_spread:
            diagnostics.append(SpreadFilterDiagnostic(symbol, "no_executable_cross_spread"))
            continue
        if not symbol_has_tight_spread_side:
            diagnostics.append(SpreadFilterDiagnostic(symbol, "orderbook_spread_too_wide"))
            continue
        if not symbol_has_threshold_pass:
            diagnostics.append(SpreadFilterDiagnostic(symbol, "cross_spread_below_threshold"))

    return diagnostics


def collect_spread_pair_filter_diagnostics(
    *,
    symbol_snapshots: dict[str, list[VenueSpreadSnapshot]],
    config: SpreadMonitorConfig,
    now_ms: int,
) -> list[SpreadPairFilterDiagnostic]:
    diagnostics: list[SpreadPairFilterDiagnostic] = []
    min_spread_limit = config.max_orderbook_spread_pct / 100
    min_cross_threshold = config.min_cross_exchange_spread_pct / 100
    max_price_deviation = config.max_price_deviation_pct / 100

    for symbol, snapshots in symbol_snapshots.items():
        venue_snapshots = [snapshot for snapshot in snapshots if snapshot.venue in config.venues]
        for left in venue_snapshots:
            for right in venue_snapshots:
                if left.venue == right.venue:
                    continue

                spread_pct: float | None = None
                if left.bid_price > 0 and right.ask_price > 0:
                    spread_pct = compute_cross_exchange_spread_pct(
                        short_price=left.bid_price,
                        long_price=right.ask_price,
                    )

                if not snapshot_is_fresh(left, now_ms=now_ms, max_stale_seconds=config.max_stale_seconds) or not snapshot_is_fresh(
                    right,
                    now_ms=now_ms,
                    max_stale_seconds=config.max_stale_seconds,
                ):
                    diagnostics.append(
                        SpreadPairFilterDiagnostic(symbol, left.venue, right.venue, spread_pct, "stale_quotes")
                    )
                    continue

                if abs(left.funding_rate) > config.max_funding_rate_pct / 100 or abs(right.funding_rate) > config.max_funding_rate_pct / 100:
                    diagnostics.append(
                        SpreadPairFilterDiagnostic(symbol, left.venue, right.venue, spread_pct, "funding_rate_too_high")
                    )
                    continue

                left_spread_pct = compute_orderbook_spread_pct(left)
                right_spread_pct = compute_orderbook_spread_pct(right)
                if left_spread_pct is None or right_spread_pct is None:
                    diagnostics.append(
                        SpreadPairFilterDiagnostic(symbol, left.venue, right.venue, spread_pct, "missing_bid_ask")
                    )
                    continue
                price_deviation_pct = compute_price_deviation_pct(left.mark_price, right.mark_price)
                if price_deviation_pct is None or price_deviation_pct > max_price_deviation:
                    diagnostics.append(
                        SpreadPairFilterDiagnostic(symbol, left.venue, right.venue, spread_pct, "price_deviation_too_high")
                    )
                    continue

                if left.bid_price <= right.ask_price:
                    diagnostics.append(
                        SpreadPairFilterDiagnostic(symbol, left.venue, right.venue, spread_pct, "no_executable_cross_spread")
                    )
                    continue

                if not (
                    left_spread_pct <= min_spread_limit
                    or right_spread_pct <= min_spread_limit
                ):
                    diagnostics.append(
                        SpreadPairFilterDiagnostic(symbol, left.venue, right.venue, spread_pct, "orderbook_spread_too_wide")
                    )
                    continue

                if spread_pct is None or spread_pct < min_cross_threshold:
                    diagnostics.append(
                        SpreadPairFilterDiagnostic(symbol, left.venue, right.venue, spread_pct, "cross_spread_below_threshold")
                    )

    return diagnostics
