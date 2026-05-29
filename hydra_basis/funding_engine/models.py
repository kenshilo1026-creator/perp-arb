from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class FundingPoint:
    venue: str
    symbol: str
    ts_ms: int
    raw_rate: float
    interval_hours: float

    @property
    def hourly_rate(self) -> float:
        return self.raw_rate / self.interval_hours


@dataclasses.dataclass(frozen=True)
class FundingConfig:
    venue: str
    enabled: bool = True
