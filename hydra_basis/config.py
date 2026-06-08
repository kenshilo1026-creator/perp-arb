from __future__ import annotations

from pathlib import Path

from hydra_basis.funding_engine.models import FundingConfig


VENUE_CONFIG: dict[str, FundingConfig] = {
    "hyperliquid": FundingConfig("hyperliquid"),
    "lighter": FundingConfig("lighter"),
    "mexc": FundingConfig("mexc", enabled=False),
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
ASSUMED_LEVERAGE = 3
FILTER_NEGATIVE_FUNDING_SPIKE = False
FETCH_CONCURRENCY_LIMIT = 6
FUNDING_HISTORY_PATH = Path("data/funding_history.json")
MONITOR_SIGNALS_PATH = Path("data/monitor_signals.json")
EXECUTION_VENUES_PATH = Path("configs/execution_venues.json")
MARGIN_TOPUP_CONFIG_PATH = Path("configs/margin_topup.jsonc")
FUNDING_RISK_CONFIG_PATH = Path("configs/funding_risk.jsonc")
ORDERBOOK_SPREADS_PATH = Path("data/orderbook_spreads.json")
POSITION_REGISTRY_PATH = Path("data/position_registry.json")
FUNDING_RISK_STATE_PATH = Path("data/funding_risk_state.json")
FUNDING_HISTORY_LOOKBACK_DAYS = 14
HYPERLIQUID_REQUEST_DELAY_SECONDS = 1.0
LIGHTER_REQUEST_DELAY_SECONDS = 0.35
VARIATIONAL_REQUEST_DELAY_SECONDS = 2.0
HYPERLIQUID_MAX_SYMBOLS_PER_RUN = 40
BACKFILL_SPREAD_CLIP_USD = 1000.0
