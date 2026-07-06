from __future__ import annotations

from pathlib import Path

from hydra_basis.config import FUNDING_HISTORY_PATH, MONITOR_SIGNALS_PATH, VENUE_CONFIG
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol
from hydra_basis.funding_engine.analysis import (
    align_on_coarser_interval,
    analyze_positive_funding,
    analyze_spread,
)
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.history_store import (
    FundingHistoryStore,
    merge_points_by_interval_bucket,
    trim_points_to_analysis_days,
)


def build_spread_series(
    short_points: list[FundingPoint],
    long_points: list[FundingPoint],
) -> list[dict]:
    """Return per-timestamp funding spread (short.hourly - long.hourly), sorted."""
    pairs = align_on_coarser_interval(short_points, long_points)
    series = [
        {
            "ts_ms": max(short_point.ts_ms, long_point.ts_ms),
            "spread_hourly": short_point.hourly_rate - long_point.hourly_rate,
        }
        for short_point, long_point in pairs
    ]
    series.sort(key=lambda row: row["ts_ms"])
    return series


def compute_pair_apy(
    short_points: list[FundingPoint],
    long_points: list[FundingPoint],
    *,
    days: int,
) -> dict:
    """Historical APY stats + spread series for a short/long venue pair."""
    trimmed_short = trim_points_to_analysis_days(
        merge_points_by_interval_bucket(short_points), analysis_days=days
    )
    trimmed_long = trim_points_to_analysis_days(
        merge_points_by_interval_bucket(long_points), analysis_days=days
    )
    stats = analyze_spread(
        trimmed_short,
        trimmed_long,
        analysis_days=days,
        min_observations=1,
    )
    return {
        "days": days,
        "stats": stats,
        "spread_series": build_spread_series(trimmed_short, trimmed_long),
    }


def compute_spot_perp_apy(venue_points: list[FundingPoint], *, days: int) -> dict:
    """Historical positive-funding stats + hourly-rate series for one perp venue."""
    trimmed = trim_points_to_analysis_days(
        merge_points_by_interval_bucket(venue_points), analysis_days=days
    )
    stats = analyze_positive_funding(trimmed, min_observations=1)
    rate_series = sorted(
        ({"ts_ms": point.ts_ms, "hourly": point.hourly_rate} for point in trimmed),
        key=lambda row: row["ts_ms"],
    )
    return {"days": days, "stats": stats, "rate_series": rate_series}


def load_points(*, history_path: Path = FUNDING_HISTORY_PATH) -> dict[tuple[str, str], list[FundingPoint]]:
    return FundingHistoryStore(history_path).load()


def list_symbols(*, history_path: Path = FUNDING_HISTORY_PATH) -> list[str]:
    points = load_points(history_path=history_path)
    return sorted({symbol.upper() for _venue, symbol in points})


def list_venues() -> list[str]:
    return sorted(venue for venue, config in VENUE_CONFIG.items() if config.enabled)


def get_default_venues(symbol: str, kind: str, *, signals_path: Path = MONITOR_SIGNALS_PATH) -> dict:
    try:
        signal = load_best_signal_for_symbol(path=signals_path, symbol=symbol)
    except Exception as exc:  # missing file, stale, or symbol not found
        return {"error": str(exc)}
    if kind == "spot_perp":
        return {"perp_venue": signal.short_venue, "annualized": signal.annualized_avg}
    return {
        "short_venue": signal.short_venue,
        "long_venue": signal.long_venue,
        "annualized": signal.annualized_avg,
    }
