from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SpreadMonitorConfig:
    venues: list[str]
    max_funding_rate_pct: float
    max_orderbook_spread_pct: float
    min_cross_exchange_spread_pct: float
    execution_notional_usd: float
    max_stale_seconds: int
    max_price_deviation_pct: float
    max_short_term_move_pct: float
    hedge_retry_count: int


@dataclass(frozen=True)
class VenueSpreadSnapshot:
    venue: str
    symbol: str
    mark_price: float
    bid_price: float
    ask_price: float
    funding_rate: float
    ts_ms: int


@dataclass(frozen=True)
class SpreadOpportunity:
    symbol: str
    short_venue: str
    long_venue: str
    short_price: float
    long_price: float
    cross_exchange_spread_pct: float
    short_funding_rate: float
    long_funding_rate: float
    short_orderbook_spread_pct: float | None
    long_orderbook_spread_pct: float | None
    execution_notional_usd: float
    expected_profit_usd: float


@dataclass(frozen=True)
class SpreadFilterDiagnostic:
    symbol: str
    reason: str


@dataclass(frozen=True)
class SpreadPairFilterDiagnostic:
    symbol: str
    left_venue: str
    right_venue: str
    cross_exchange_spread_pct: float | None
    reason: str
