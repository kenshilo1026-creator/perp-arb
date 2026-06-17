from __future__ import annotations

from hydra_basis.adapters.aster import fetch_aster_funding, fetch_aster_funding_since, list_symbols as list_aster_symbols
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_funding, fetch_hyperliquid_funding_since, list_symbols as list_hyperliquid_symbols
from hydra_basis.adapters.lighter import fetch_lighter_funding, fetch_lighter_funding_since, list_symbols as list_lighter_symbols
from hydra_basis.adapters.mexc import fetch_mexc_funding, fetch_mexc_funding_since, list_symbols as list_mexc_symbols
from hydra_basis.adapters.tradexyz import fetch_tradexyz_funding, fetch_tradexyz_funding_since, list_symbols as list_tradexyz_symbols
from hydra_basis.adapters.variational import (
    fetch_variational_funding,
    fetch_variational_funding_since,
    list_symbols as list_variational_symbols,
)


FETCHERS = {
    "hyperliquid": fetch_hyperliquid_funding,
    "mexc": fetch_mexc_funding,
    "lighter": fetch_lighter_funding,
    "aster": fetch_aster_funding,
    "variational": fetch_variational_funding,
    "trade_xyz": fetch_tradexyz_funding,
}

FETCHERS_SINCE = {
    "hyperliquid": fetch_hyperliquid_funding_since,
    "mexc": fetch_mexc_funding_since,
    "lighter": fetch_lighter_funding_since,
    "aster": fetch_aster_funding_since,
    "variational": fetch_variational_funding_since,
    "trade_xyz": fetch_tradexyz_funding_since,
}

SYMBOL_DISCOVERERS = {
    "hyperliquid": list_hyperliquid_symbols,
    "mexc": list_mexc_symbols,
    "lighter": list_lighter_symbols,
    "aster": list_aster_symbols,
    "variational": list_variational_symbols,
    "trade_xyz": list_tradexyz_symbols,
}
