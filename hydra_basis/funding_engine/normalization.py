from __future__ import annotations

import statistics
from typing import Optional


def infer_interval_hours_from_timestamps(points: list[int]) -> Optional[float]:
    if len(points) < 3:
        return None

    sorted_ts = sorted(set(points))
    gaps_hours = [
        (current - previous) / 3_600_000
        for previous, current in zip(sorted_ts, sorted_ts[1:])
        if current > previous
    ]
    if not gaps_hours:
        return None

    median_gap = statistics.median(gaps_hours)
    common_intervals = [1, 2, 4, 8, 12, 24]
    nearest = min(common_intervals, key=lambda value: abs(value - median_gap))
    if abs(nearest - median_gap) <= 0.35:
        return float(nearest)
    return round(float(median_gap), 2)
