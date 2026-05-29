from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ExecutionSignal:
    symbol: str
    short_venue: str
    long_venue: str
    annualized_avg: float
    score: float
    updated_at: str


@dataclass
class ExecutionRequest:
    symbol: str
    total_usd: float
    clip_usd: float


@dataclass
class ExecutionPreview:
    symbol: str
    maker_venue: str
    taker_venue: str
    total_usd: float
    clip_usd: float
    batch_count: int
    maker_spread_pct: float
    taker_spread_pct: float
    requires_confirm: bool
