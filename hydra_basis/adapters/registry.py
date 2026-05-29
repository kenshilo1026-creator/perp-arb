from __future__ import annotations

from hydra_basis.adapters.aster import fetch_aster_funding, list_symbols as list_aster_symbols
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_funding, list_symbols as list_hyperliquid_symbols
from hydra_basis.adapters.lighter import fetch_lighter_funding, list_symbols as list_lighter_symbols
from hydra_basis.adapters.mexc import fetch_mexc_funding, list_symbols as list_mexc_symbols
from hydra_basis.adapters.variational import fetch_variational_funding, list_symbols as list_variational_symbols


FETCHERS = {
    "hyperliquid": fetch_hyperliquid_funding,
    "mexc": fetch_mexc_funding,
    "lighter": fetch_lighter_funding,
    "aster": fetch_aster_funding,
    "variational": fetch_variational_funding,
}

SYMBOL_DISCOVERERS = {
    "hyperliquid": list_hyperliquid_symbols,
    "mexc": list_mexc_symbols,
    "lighter": list_lighter_symbols,
    "aster": list_aster_symbols,
    "variational": list_variational_symbols,
}
