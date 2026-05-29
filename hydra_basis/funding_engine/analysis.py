from __future__ import annotations

import datetime as dt
import statistics
from typing import Any

from hydra_basis.funding_engine.models import FundingPoint


DEFAULT_MIN_AVG_SPREAD_HOURLY = 0.000002
DEFAULT_MIN_POSITIVE_RATIO = 0.75
DEFAULT_MIN_LAST_24H_POSITIVE_RATIO = 0.70
DEFAULT_MAX_STD_MULTIPLE_OF_AVG = 2.5
DEFAULT_MIN_OBSERVATIONS = 24
DEFAULT_MIXED_8H_MIN_OBSERVATIONS = 18
DEFAULT_MAX_SINGLE_INTERVAL_HOURLY_RATE = 0.001
DEFAULT_MAX_MARK_PRICE_RELATIVE_DIFF = 0.2


def now_ms() -> int:
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


def positive_ratio(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(1 for value in values if value > 0) / len(values)


def annualized_from_hourly(hourly: float) -> float:
    return hourly * 24 * 365


def resolve_pair_min_observations(
    short_venue_points: list[FundingPoint],
    long_venue_points: list[FundingPoint],
    *,
    default_min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    mixed_8h_min_observations: int = DEFAULT_MIXED_8H_MIN_OBSERVATIONS,
) -> int:
    if default_min_observations != DEFAULT_MIN_OBSERVATIONS:
        return default_min_observations
    if not short_venue_points or not long_venue_points:
        return default_min_observations

    interval_hours = {
        float(short_venue_points[0].interval_hours),
        float(long_venue_points[0].interval_hours),
    }
    if interval_hours == {1.0}:
        return default_min_observations
    if 8.0 in interval_hours:
        return mixed_8h_min_observations
    return default_min_observations


def prices_are_compatible(
    left_ctx: dict[str, float | int] | None,
    right_ctx: dict[str, float | int] | None,
    *,
    max_relative_diff: float = DEFAULT_MAX_MARK_PRICE_RELATIVE_DIFF,
) -> bool:
    if not left_ctx or not right_ctx:
        return True
    left_mark = float(left_ctx.get("markPx") or 0.0)
    right_mark = float(right_ctx.get("markPx") or 0.0)
    if left_mark <= 0 or right_mark <= 0:
        return True
    baseline = max(min(left_mark, right_mark), 1e-12)
    relative_diff = abs(left_mark - right_mark) / baseline
    return relative_diff <= max_relative_diff


def align_on_coarser_interval(
    short_points: list[FundingPoint],
    long_points: list[FundingPoint],
    max_lag_minutes: int = 90,
) -> list[tuple[FundingPoint, FundingPoint]]:
    if not short_points or not long_points:
        return []

    if short_points[0].interval_hours >= long_points[0].interval_hours:
        primary_points = sorted(short_points, key=lambda point: point.ts_ms)
        secondary_points = sorted(long_points, key=lambda point: point.ts_ms)
        primary_is_short = True
    else:
        primary_points = sorted(long_points, key=lambda point: point.ts_ms)
        secondary_points = sorted(short_points, key=lambda point: point.ts_ms)
        primary_is_short = False

    max_lag_ms = max_lag_minutes * 60 * 1000
    pairs: list[tuple[FundingPoint, FundingPoint]] = []
    secondary_index = 0

    for primary_point in primary_points:
        while (
            secondary_index + 1 < len(secondary_points)
            and abs(secondary_points[secondary_index + 1].ts_ms - primary_point.ts_ms)
            <= abs(secondary_points[secondary_index].ts_ms - primary_point.ts_ms)
        ):
            secondary_index += 1

        secondary_point = secondary_points[secondary_index]
        if abs(secondary_point.ts_ms - primary_point.ts_ms) > max_lag_ms:
            continue

        if primary_is_short:
            pairs.append((primary_point, secondary_point))
        else:
            pairs.append((secondary_point, primary_point))

    return pairs


def analyze_spread(
    short_venue_points: list[FundingPoint],
    long_venue_points: list[FundingPoint],
    *,
    min_avg_spread_hourly: float = DEFAULT_MIN_AVG_SPREAD_HOURLY,
    min_positive_ratio: float = DEFAULT_MIN_POSITIVE_RATIO,
    min_last_24h_positive_ratio: float = DEFAULT_MIN_LAST_24H_POSITIVE_RATIO,
    max_std_multiple_of_avg: float = DEFAULT_MAX_STD_MULTIPLE_OF_AVG,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    max_single_interval_hourly_rate: float = DEFAULT_MAX_SINGLE_INTERVAL_HOURLY_RATE,
) -> dict[str, Any] | None:
    all_points = short_venue_points + long_venue_points
    if any(p.hourly_rate < -max_single_interval_hourly_rate for p in all_points):
        return None
    effective_min_observations = resolve_pair_min_observations(
        short_venue_points,
        long_venue_points,
        default_min_observations=min_observations,
    )
    pairs = align_on_coarser_interval(short_venue_points, long_venue_points)
    spreads = [short_point.hourly_rate - long_point.hourly_rate for short_point, long_point in pairs]
    if len(spreads) < effective_min_observations:
        return None

    average = statistics.mean(spreads)
    median = statistics.median(spreads)
    std_dev = statistics.pstdev(spreads) if len(spreads) > 1 else 0.0
    pos_ratio = positive_ratio(spreads)

    cutoff_24h = now_ms() - 24 * 60 * 60 * 1000
    recent_spreads = [
        short_point.hourly_rate - long_point.hourly_rate
        for short_point, long_point in pairs
        if max(short_point.ts_ms, long_point.ts_ms) >= cutoff_24h
    ]
    recent_pos_ratio = positive_ratio(recent_spreads)
    stable_volatility = True if average <= 0 else std_dev <= abs(average) * max_std_multiple_of_avg
    score = (
        0.35 * pos_ratio
        + 0.25 * min(max(average / max(min_avg_spread_hourly, 1e-12), 0), 1)
        + 0.20 * (1 if stable_volatility else 0)
        + 0.10 * recent_pos_ratio
        + 0.10 * min(len(spreads) / 168, 1)
    )
    signal = (
        average > min_avg_spread_hourly
        and median > 0
        and pos_ratio >= min_positive_ratio
        and recent_pos_ratio >= min_last_24h_positive_ratio
        and stable_volatility
    )

    return {
        "samples": len(spreads),
        "avg_hourly": average,
        "median_hourly": median,
        "std_hourly": std_dev,
        "positive_ratio": pos_ratio,
        "last_24h_positive_ratio": recent_pos_ratio,
        "annualized_avg": annualized_from_hourly(average),
        "score": score,
        "signal": signal,
    }


def explain_spread_skip(
    short_venue_points: list[FundingPoint],
    long_venue_points: list[FundingPoint],
    *,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> str:
    if not short_venue_points or not long_venue_points:
        return "missing_points"
    effective_min_observations = resolve_pair_min_observations(
        short_venue_points,
        long_venue_points,
        default_min_observations=min_observations,
    )
    pairs = align_on_coarser_interval(short_venue_points, long_venue_points)
    if not pairs:
        return "no_aligned_pairs"
    if len(pairs) < effective_min_observations:
        return f"insufficient_samples:{len(pairs)}/{effective_min_observations}"
    return "filtered_by_signal_rules"


def analyze_positive_funding(
    venue_points: list[FundingPoint],
    *,
    min_avg_spread_hourly: float = DEFAULT_MIN_AVG_SPREAD_HOURLY,
    min_positive_ratio: float = DEFAULT_MIN_POSITIVE_RATIO,
    min_last_24h_positive_ratio: float = DEFAULT_MIN_LAST_24H_POSITIVE_RATIO,
    max_std_multiple_of_avg: float = DEFAULT_MAX_STD_MULTIPLE_OF_AVG,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    max_single_interval_hourly_rate: float = DEFAULT_MAX_SINGLE_INTERVAL_HOURLY_RATE,
) -> dict[str, Any] | None:
    if any(abs(p.hourly_rate) > max_single_interval_hourly_rate for p in venue_points):
        return None
    if len(venue_points) < min_observations:
        return None

    hourly_rates = [point.hourly_rate for point in venue_points]
    average = statistics.mean(hourly_rates)
    median = statistics.median(hourly_rates)
    std_dev = statistics.pstdev(hourly_rates) if len(hourly_rates) > 1 else 0.0
    pos_ratio = positive_ratio(hourly_rates)

    cutoff_24h = now_ms() - 24 * 60 * 60 * 1000
    recent_rates = [point.hourly_rate for point in venue_points if point.ts_ms >= cutoff_24h]
    recent_pos_ratio = positive_ratio(recent_rates)
    stable_volatility = True if average <= 0 else std_dev <= abs(average) * max_std_multiple_of_avg
    score = (
        0.35 * pos_ratio
        + 0.25 * min(max(average / max(min_avg_spread_hourly, 1e-12), 0), 1)
        + 0.20 * (1 if stable_volatility else 0)
        + 0.10 * recent_pos_ratio
        + 0.10 * min(len(hourly_rates) / 168, 1)
    )
    signal = (
        average > min_avg_spread_hourly
        and median > 0
        and pos_ratio >= min_positive_ratio
        and recent_pos_ratio >= min_last_24h_positive_ratio
        and stable_volatility
    )

    return {
        "samples": len(hourly_rates),
        "avg_hourly": average,
        "median_hourly": median,
        "std_hourly": std_dev,
        "positive_ratio": pos_ratio,
        "last_24h_positive_ratio": recent_pos_ratio,
        "annualized_avg": annualized_from_hourly(average),
        "score": score,
        "signal": signal,
    }
