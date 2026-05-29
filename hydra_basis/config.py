from __future__ import annotations

from pathlib import Path

from hydra_basis.funding_engine.models import FundingConfig


VENUE_CONFIG: dict[str, FundingConfig] = {
    "hyperliquid": FundingConfig("hyperliquid"),
    "lighter": FundingConfig("lighter"),
    "mexc": FundingConfig("mexc"),
    "aster": FundingConfig("aster", enabled=True),
    "variational": FundingConfig("variational", enabled=True),
}

LOOKBACK_DAYS = 7

SYMBOL_ALIASES: dict[str, dict[str, str]] = {
    "hyperliquid": {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"},
    "mexc": {"BTC": "BTC_USDT", "ETH": "ETH_USDT", "SOL": "SOL_USDT"},
    "lighter": {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"},
}

FUNDING_INTERVAL_OVERRIDES: dict[tuple[str, str], float] = {}
MIN_SHARED_VENUES = 2
FETCH_CONCURRENCY_LIMIT = 6
FUNDING_HISTORY_PATH = Path("data/funding_history.json")
FUNDING_HISTORY_LOOKBACK_DAYS = 14
HYPERLIQUID_REQUEST_DELAY_SECONDS = 1.0
LIGHTER_REQUEST_DELAY_SECONDS = 0.35
VARIATIONAL_REQUEST_DELAY_SECONDS = 2.0
HYPERLIQUID_MAX_SYMBOLS_PER_RUN = 40
