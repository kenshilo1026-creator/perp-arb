"""Ondo Perps (https://ondoperps.xyz) public data, funding history and request signing.

Perp markets are named ``<BASE>-USD.P`` (BTC-USD.P, NVDA-USD.P); project symbols are
the base. Funding is hourly. Timestamps are ISO-8601 strings with nanoseconds.

TLS: some Windows trust stores hold an expired cross-signed root that breaks Python's
verification of this host (curl succeeds). Every Ondo connection therefore verifies
against certifi's CA bundle. Verification stays fully enabled.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import ssl
import time
from datetime import datetime, timezone
from functools import lru_cache
from urllib.parse import urlencode

import aiohttp

from hydra_basis.adapters.hyperliquid import ms_days_ago
from hydra_basis.config import LOOKBACK_DAYS
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps

ONDO_VENUE = "ondo"
ONDO_MARKET_SUFFIX = "-USD.P"
ONDO_FUNDING_INTERVAL_HOURS = 1.0
FUNDING_PAGE_LIMIT = 1000


def ondo_base_url() -> str:
    return os.getenv("ONDO_BASE_URL", "https://api.ondoperps.xyz").rstrip("/")


def ondo_ws_url() -> str:
    return ondo_base_url().replace("https://", "wss://") + "/ws"


@lru_cache(maxsize=1)
def ondo_ssl() -> ssl.SSLContext:
    import certifi
    return ssl.create_default_context(cafile=certifi.where())


def ondo_market_name(symbol: str) -> str:
    normalized = symbol.strip().upper()
    return normalized if normalized.endswith(ONDO_MARKET_SUFFIX) else f"{normalized}{ONDO_MARKET_SUFFIX}"


def ondo_symbol(market: str) -> str:
    return str(market).upper().removesuffix(ONDO_MARKET_SUFFIX)


def parse_iso_ms(value: str) -> int:
    """'2026-10-09T08:28:00.486875829Z' (or an epoch number) -> epoch ms."""
    text = str(value).strip()
    if text.isdigit():
        number = int(text)
        return number * 1000 if number < 10**11 else number  # seconds or milliseconds
    text = text.replace("Z", "")
    if "." in text:
        head, fraction = text.split(".", 1)
        text = f"{head}.{fraction[:6]}"
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def sign_request(method: str, path_with_query: str, body: str, secret: str, timestamp_ms: int) -> str:
    """ONDO-SIGN: hex HMAC-SHA256 over timestamp + METHOD + path?query + body, keyed by the secret."""
    message = f"{timestamp_ms}{method.upper()}{path_with_query}{body}"
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def ondo_credentials() -> tuple[str, str] | None:
    key_id, secret = os.getenv("ONDO_API_KEY_ID", ""), os.getenv("ONDO_API_SECRET", "")
    return (key_id, secret) if key_id and secret else None


async def ondo_request(session, method: str, path: str, *, params: dict | None = None, body=None,
                       auth: bool = False, label: str = "request"):
    """One Ondo REST call; returns ``result``. Signed requests use the exact path+query sent."""
    query = f"?{urlencode(params)}" if params else ""
    payload = json.dumps(body, separators=(",", ":")) if body is not None else ""
    headers = {"Content-Type": "application/json"}
    if auth:
        credentials = ondo_credentials()
        if credentials is None:
            raise RuntimeError("ONDO_API_KEY_ID / ONDO_API_SECRET are not set")
        timestamp = int(time.time() * 1000)
        headers |= {"ONDO-KEY-ID": credentials[0], "ONDO-TIMESTAMP": str(timestamp),
                    "ONDO-SIGN": sign_request(method, path + query, payload, credentials[1], timestamp)}
    async with session.request(method, f"{ondo_base_url()}{path}{query}", data=payload or None, headers=headers,
                               ssl=ondo_ssl(), timeout=aiohttp.ClientTimeout(total=15)) as resp:
        text = await resp.text()
        try:
            data = json.loads(text) if text else {}
        except ValueError:
            data = {"error": text[:300]}
        if resp.status >= 400 or data.get("success") is False:
            raise RuntimeError(f"ondo {label} {resp.status}: {data.get('error_code') or ''} {data.get('error') or data}")
        return data.get("result", data), data


async def fetch_ondo_contracts(session) -> list[dict]:
    result, _ = await ondo_request(session, "GET", "/v1/perps/contracts", label="contracts")
    return list(result or [])


async def fetch_ondo_markets(session) -> list[dict]:
    result, _ = await ondo_request(session, "GET", "/v1/markets", label="markets")
    return list(((result or {}).get("perps") or {}).get("tradingPairs") or [])


async def list_symbols(session) -> set[str]:
    return {ondo_symbol(row["market"]) for row in await fetch_ondo_contracts(session)
            if not row.get("disabled") and str(row.get("market", "")).endswith(ONDO_MARKET_SUFFIX)}


async def fetch_ondo_funding(session, symbol: str) -> list[FundingPoint]:
    return await fetch_ondo_funding_since(session, symbol, start_time_ms=ms_days_ago(LOOKBACK_DAYS))


async def fetch_ondo_funding_since(session, symbol: str, start_time_ms: int) -> list[FundingPoint]:
    """Newest-first pages linked by cursor, bounded by ``startTime``."""
    rows: dict[int, float] = {}
    cursor = None
    for _ in range(100):
        params = {"market": ondo_market_name(symbol), "limit": FUNDING_PAGE_LIMIT, "startTime": int(start_time_ms)}
        if cursor:
            params["cursor"] = cursor
        result, envelope = await ondo_request(session, "GET", "/v1/perps/funding_rate_history", params=params,
                                              label="funding")
        for row in result or []:
            rows[parse_iso_ms(row["time"])] = float(row["fundingRate"])
        cursor = (envelope.get("pageInfo") or {}).get("nextCursor")
        if not cursor or not result:
            break
    timestamps = sorted(ts for ts in rows if ts >= start_time_ms)
    inferred = infer_interval_hours_from_timestamps(timestamps)
    interval_hours = inferred if inferred is not None else ONDO_FUNDING_INTERVAL_HOURS
    return [FundingPoint(ONDO_VENUE, symbol, ts, rows[ts], interval_hours) for ts in timestamps]


async def fetch_ondo_orderbook(session, symbol: str) -> dict[str, float | int]:
    result, _ = await ondo_request(session, "GET", "/v1/perps/depth",
                                   params={"market": ondo_market_name(symbol), "depth": 1}, label="depth")
    if not result.get("bids") or not result.get("asks"):
        raise RuntimeError(f"missing ondo orderbook for {symbol}")
    return {"bid": float(result["bids"][0][0]), "ask": float(result["asks"][0][0]),
            "ts_ms": parse_iso_ms(result["time"])}


async def fetch_ondo_minute_closes(session, symbol: str, start_ms: int, end_ms: int) -> dict[int, float] | None:
    """1-minute candles need an API key; without one the caller falls back to recorded prices."""
    if ondo_credentials() is None:
        return None
    result, _ = await ondo_request(session, "GET", "/v1/perps/candles", auth=True, label="candles", params={
        "market": ondo_market_name(symbol), "resolution": "1", "from": start_ms // 1000, "to": end_ms // 1000})
    return {parse_iso_ms(row["startTime"]): float(row["close"]) for row in result or []}
