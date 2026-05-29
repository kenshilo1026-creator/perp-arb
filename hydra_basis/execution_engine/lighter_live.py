from __future__ import annotations

import ctypes.util
import os
import platform
import sys
from typing import Any

import aiohttp

from hydra_basis.adapters.base import fetch_json
from hydra_basis.execution_engine.market_data import fetch_lighter_orderbook


LIGHTER_BASE_URL = "https://mainnet.zklighter.elliot.ai"


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def _required_int_env(name: str) -> int:
    value = _required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc


def import_lighter_signer_client():
    if platform.system() != "Windows":
        from lighter.signer_client import SignerClient

        return SignerClient

    original_find_library = ctypes.util.find_library

    def patched_find_library(name: str):
        if name == "c":
            return original_find_library("msvcrt") or original_find_library("ucrtbase") or "msvcrt.dll"
        return original_find_library(name)

    ctypes.util.find_library = patched_find_library
    try:
        for module_name in list(sys.modules):
            if module_name == "lighter" or module_name.startswith("lighter."):
                sys.modules.pop(module_name, None)
        from lighter.signer_client import SignerClient

        return SignerClient
    finally:
        ctypes.util.find_library = original_find_library


def build_lighter_client_factory_from_env(*, base_url: str = LIGHTER_BASE_URL):
    account_index = _required_int_env("LIGHTER_ACCOUNT_INDEX")
    api_key_index = _required_int_env("LIGHTER_API_KEY_INDEX")
    api_key_private_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip() or _required_env("LIGHTER_PRIVATE_KEY")

    def factory():
        SignerClient = import_lighter_signer_client()
        client = SignerClient(
            url=base_url,
            account_index=account_index,
            api_private_keys={api_key_index: api_key_private_key},
        )
        error = client.check_client()
        if error is not None:
            raise RuntimeError(f"CheckClient error: {error}")
        return client

    return factory


async def fetch_lighter_market_config(
    symbol: str,
    *,
    base_url: str = LIGHTER_BASE_URL,
) -> tuple[int, int, int]:
    async with aiohttp.ClientSession() as session:
        data = await fetch_json(
            session,
            "GET",
            f"{base_url}/api/v1/orderBooks",
            headers={"accept": "application/json"},
        )
    for market in data.get("order_books", []):
        if str(market.get("symbol") or "").upper() != symbol.upper():
            continue
        price_decimals = int(market["supported_price_decimals"])
        size_decimals = int(market["supported_size_decimals"])
        return int(market["market_id"]), pow(10, size_decimals), pow(10, price_decimals)
    raise RuntimeError(f"Ticker {symbol} not found in Lighter order books")


async def fetch_lighter_orderbook_live(
    symbol: str,
    *,
    session: aiohttp.ClientSession | None = None,
) -> dict[str, float | int]:
    if session is not None:
        return await fetch_lighter_orderbook(session, symbol)
    async with aiohttp.ClientSession() as own_session:
        return await fetch_lighter_orderbook(own_session, symbol)
