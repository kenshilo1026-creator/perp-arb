from __future__ import annotations

import json
import datetime as dt
from pathlib import Path

from hydra_basis.funding_engine.models import FundingPoint


def now_ms() -> int:
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


def trim_points_to_lookback_ms(
    points: list[FundingPoint],
    *,
    lookback_ms: int,
    now_ms: int | None = None,
) -> list[FundingPoint]:
    current_ms = now_ms if now_ms is not None else globals()["now_ms"]()
    cutoff = current_ms - lookback_ms
    return [point for point in points if point.ts_ms >= cutoff]


def trim_points_to_analysis_days(
    points: list[FundingPoint],
    *,
    analysis_days: int,
    now_ms: int | None = None,
) -> list[FundingPoint]:
    return trim_points_to_lookback_ms(
        points,
        lookback_ms=analysis_days * 24 * 60 * 60 * 1000,
        now_ms=now_ms,
    )


def merge_points_by_interval_bucket(points: list[FundingPoint]) -> list[FundingPoint]:
    if not points:
        return []

    merged: dict[tuple[str, str, int], FundingPoint] = {}
    for point in sorted(points, key=lambda item: item.ts_ms):
        interval_ms = int(point.interval_hours * 3_600_000)
        bucket = point.ts_ms // interval_ms if interval_ms > 0 else point.ts_ms
        merged[(point.venue, point.symbol, bucket)] = point
    return sorted(merged.values(), key=lambda item: item.ts_ms)


def funding_history_is_complete(
    points: list[FundingPoint],
    *,
    required_days: int,
    now_ms: int | None = None,
) -> bool:
    if not points:
        return False
    current_ms = now_ms if now_ms is not None else globals()["now_ms"]()
    required_start_ms = current_ms - required_days * 24 * 60 * 60 * 1000
    oldest_ts = min(point.ts_ms for point in points)
    max_interval_ms = int(max(p.interval_hours for p in points) * 3_600_000)
    return oldest_ts <= required_start_ms + max_interval_ms


def summarize_history_coverage(
    points: list[FundingPoint],
    *,
    required_days: int,
    now_ms: int | None = None,
) -> dict[str, int | float | None]:
    current_ms = now_ms if now_ms is not None else globals()["now_ms"]()
    required_start_ms = current_ms - required_days * 24 * 60 * 60 * 1000
    if not points:
        return {
            "samples": 0,
            "oldest_ts_ms": None,
            "newest_ts_ms": None,
            "missing_ms": required_days * 24 * 60 * 60 * 1000,
        }

    oldest_ts = min(point.ts_ms for point in points)
    newest_ts = max(point.ts_ms for point in points)
    missing_ms = max(oldest_ts - required_start_ms, 0)
    return {
        "samples": len(points),
        "oldest_ts_ms": oldest_ts,
        "newest_ts_ms": newest_ts,
        "missing_ms": missing_ms,
    }


class FundingHistoryStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[tuple[str, str], list[FundingPoint]]:
        if not self.path.exists():
            return {}

        raw = json.loads(self.path.read_text(encoding="utf-8"))
        loaded: dict[tuple[str, str], list[FundingPoint]] = {}
        for key, points in raw.items():
            venue, symbol = key.split("::", 1)
            loaded[(venue, symbol)] = [
                FundingPoint(
                    venue=point["venue"],
                    symbol=point["symbol"],
                    ts_ms=int(point["ts_ms"]),
                    raw_rate=float(point["raw_rate"]),
                    interval_hours=float(point["interval_hours"]),
                )
                for point in points
            ]
        return loaded

    def save(self, funding_points: dict[tuple[str, str], list[FundingPoint]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serialized: dict[str, list[dict]] = {}
        for (venue, symbol), points in funding_points.items():
            key = f"{venue}::{symbol}"
            rows = []
            for point in points:
                try:
                    row = {
                        "venue": point.venue,
                        "symbol": point.symbol,
                        "ts_ms": point.ts_ms,
                        "raw_rate": point.raw_rate,
                        "interval_hours": point.interval_hours,
                    }
                    json.dumps(row)  # validate before adding
                    rows.append(row)
                except Exception as exc:
                    print(f"history_store skipping unserializable point {key} ts={getattr(point, 'ts_ms', '?')}: {exc!r}")
            serialized[key] = rows
        self.path.write_text(json.dumps(serialized, ensure_ascii=True, indent=2), encoding="utf-8")
