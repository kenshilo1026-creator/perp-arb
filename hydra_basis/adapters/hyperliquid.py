from __future__ import annotations

import asyncio

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.request_limiters import run_serialized
from hydra_basis.config import LOOKBACK_DAYS, VENUE_CONFIG
from hydra_basis.config import HYPERLIQUID_REQUEST_DELAY_SECONDS
from hydra_basis.funding_engine.analysis import now_ms
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps

HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_RETRY_ATTEMPTS = 4
HYPERLIQUID_RATE_LIMIT_BACKOFF_SECONDS = 3.0
HYPERLIQUID_FUNDING_INTERVAL_HOURS = 1.0
COIN_NAMES_TTL_MS = 60 * 60 * 1000

# Project symbols are upper-cased, but the info API only accepts Hyperliquid's own spelling of
# a coin ("kPEPE", not "KPEPE"). Every main-dex meta response refreshes this map.
_coin_names: dict[str, str] = {}
_coin_names_ms = 0


def remember_hyperliquid_coin_names(rows) -> None:
    global _coin_names_ms
    names = {str(row["name"]).upper(): str(row["name"]) for row in rows or [] if row.get("name")}
    if names:
        _coin_names.update(names)
        _coin_names_ms = now_ms()


def hyperliquid_coin_name(symbol: str) -> str:
    """Hyperliquid's spelling of a main-dex coin from the last meta response (unknown: as given)."""
    normalized = symbol.strip().upper()
    return _coin_names.get(normalized, normalized)


async def resolve_hyperliquid_coin(session, symbol: str) -> str:
    """Like ``hyperliquid_coin_name``, fetching meta first when it is missing or over an hour old."""
    if not _coin_names or now_ms() - _coin_names_ms > COIN_NAMES_TTL_MS:
        await fetch_hyperliquid_meta(session)
    return hyperliquid_coin_name(symbol)


def ms_days_ago(days: int) -> int:
    return now_ms() - days * 24 * 60 * 60 * 1000


def build_funding_history_payload(symbol: str, start_time_ms: int) -> dict:
    return {
        "type": "fundingHistory",
        "coin": symbol,
        "startTime": start_time_ms,
    }


def _is_retryable_hyperliquid_error(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    if status == 429:
        return True
    message = str(exc).lower()
    return "429" in message or "too many requests" in message or "rate limit" in message


async def _post_hyperliquid_info(session, payload: dict):
    last_error: Exception | None = None
    for attempt in range(HYPERLIQUID_RETRY_ATTEMPTS + 1):
        try:
            return await run_serialized(
                "hyperliquid",
                lambda: fetch_json(session, "POST", HYPERLIQUID_INFO_URL, json=payload),
                delay_seconds=HYPERLIQUID_REQUEST_DELAY_SECONDS,
            )
        except Exception as exc:
            last_error = exc
            if not _is_retryable_hyperliquid_error(exc) or attempt >= HYPERLIQUID_RETRY_ATTEMPTS:
                raise
            backoff_seconds = HYPERLIQUID_RATE_LIMIT_BACKOFF_SECONDS * (attempt + 1)
            await asyncio.sleep(backoff_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError("hyperliquid info request failed without an error")


async def fetch_hyperliquid_meta(session, dex: str | None = None) -> list[dict]:
    """Return ALL perp asset rows (name, szDecimals, ...) in raw order, including delisted.

    ``dex`` selects a HIP-3 builder-deployed perp dex (e.g. "io" for Entropy); its
    asset names carry the dex prefix ("io:OAI").
    """
    payload = {"type": "meta"} if not dex else {"type": "meta", "dex": dex}
    data = await _post_hyperliquid_info(session, payload)
    rows = list(data.get("universe") or [])
    if not dex:
        remember_hyperliquid_coin_names(rows)
    return rows


async def fetch_hyperliquid_perp_dex_index(session, dex: str) -> int:
    """Position of a HIP-3 dex in ``perpDexs`` (index 0 is the main dex)."""
    dexs = await _post_hyperliquid_info(session, {"type": "perpDexs"})
    for index, item in enumerate(dexs or []):
        if isinstance(item, dict) and item.get("name") == dex:
            return index
    raise RuntimeError(f"hyperliquid perp dex not found: {dex}")


def hyperliquid_asset_id(index_in_meta: int, dex_index: int = 0) -> int:
    """Order asset id: main-dex perps use the meta index; HIP-3 perps use
    100000 + perp_dex_index * 10000 + index_in_meta."""
    return index_in_meta if dex_index == 0 else 100000 + dex_index * 10000 + index_in_meta


async def fetch_hyperliquid_universe(session) -> list[str]:
    """Return ALL assets in raw order (including delisted) so indices match Hyperliquid's API."""
    return [str(row.get("name") or "").upper() for row in await fetch_hyperliquid_meta(session)]


async def list_symbols(session) -> set[str]:
    payload = {"type": "meta"}
    data = await _post_hyperliquid_info(session, payload)
    universe = data.get("universe") or []
    remember_hyperliquid_coin_names(universe)
    return {str(row.get("name") or "").upper() for row in universe if not row.get("isDelisted")}


async def fetch_hyperliquid_funding(session, symbol: str) -> list[FundingPoint]:
    return await fetch_hyperliquid_funding_since(session, symbol, start_time_ms=ms_days_ago(LOOKBACK_DAYS))


async def fetch_hyperliquid_current_funding(session, symbol: str) -> dict[str, float] | None:
    payload = {"type": "metaAndAssetCtxs"}
    data = await _post_hyperliquid_info(session, payload)
    if not isinstance(data, list) or len(data) < 2:
        return None

    universe = (data[0] or {}).get("universe") or []
    asset_contexts = data[1] or []
    target_symbol = symbol.upper()
    for index, row in enumerate(universe):
        if str(row.get("name") or "").upper() != target_symbol:
            continue
        if index >= len(asset_contexts):
            return None
        context = asset_contexts[index] or {}
        funding_rate = context.get("funding") or context.get("fundingRate")
        if funding_rate is None:
            return None
        return {
            "funding_rate": float(funding_rate),
            "interval_hours": HYPERLIQUID_FUNDING_INTERVAL_HOURS,
        }
    return None


async def fetch_hyperliquid_funding_since(session, symbol: str, start_time_ms: int) -> list[FundingPoint]:
    # Symbol discovery (list_symbols) has already loaded the coin spellings.
    payload = build_funding_history_payload(hyperliquid_coin_name(symbol), start_time_ms=start_time_ms)
    data = await _post_hyperliquid_info(session, payload)

    rows_data = [
        (int(row.get("time") or row.get("timestamp")), float(row["fundingRate"]))
        for row in data
    ]
    inferred = infer_interval_hours_from_timestamps([ts for ts, _ in rows_data])
    interval_hours = inferred if inferred is not None else HYPERLIQUID_FUNDING_INTERVAL_HOURS
    return [
        FundingPoint("hyperliquid", symbol, ts, rate, interval_hours)
        for ts, rate in rows_data
    ]
