from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from hydra_basis.adapters.aster import fetch_aster_symbol_metadata
from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_universe
from hydra_basis.adapters.lighter import fetch_lighter_market_map
from hydra_basis.adapters.mexc import mexc_contract_symbol
from hydra_basis.adapters.variational import VARIATIONAL_BASE_URL
from hydra_basis.symbol_mapping import canonicalize_symbol


async def fetch_orderbook_snapshot(
    session: aiohttp.ClientSession,
    *,
    venue: str,
    symbol: str,
    clip_usd: float,
) -> dict[str, float | int]:
    normalized = venue.lower()
    if normalized == "hyperliquid":
        return await fetch_hyperliquid_orderbook(session, symbol)
    if normalized == "mexc":
        return await fetch_mexc_orderbook(session, symbol)
    if normalized == "aster":
        return await fetch_aster_orderbook(session, symbol)
    if normalized == "lighter":
        return await fetch_lighter_orderbook(session, symbol)
    if normalized == "variational":
        return await fetch_variational_quote(session, canonicalize_symbol(symbol, venue="variational"), clip_usd=clip_usd)
    raise RuntimeError(f"unsupported preview venue={venue}")


async def fetch_mexc_spot_orderbook(session: aiohttp.ClientSession, symbol: str) -> dict[str, float | int]:
    spot_symbol = symbol.strip().upper()
    if not spot_symbol.endswith("USDT"):
        spot_symbol = f"{spot_symbol}USDT"
    data = await fetch_json(
        session,
        "GET",
        "https://api.mexc.com/api/v3/depth",
        params={"symbol": spot_symbol, "limit": 5},
    )
    bids = data.get("bids") or []
    asks = data.get("asks") or []
    if not bids or not asks:
        raise RuntimeError(f"missing mexc spot orderbook for {symbol}")
    return {
        "bid": float(bids[0][0]),
        "ask": float(asks[0][0]),
        "ts_ms": 0,
    }


async def fetch_hyperliquid_orderbook(session: aiohttp.ClientSession, symbol: str) -> dict[str, float | int]:
    universe = await fetch_hyperliquid_universe(session)
    if symbol.upper() not in universe:
        raise RuntimeError(f"symbol not found on hyperliquid: {symbol}")
    payload = {"type": "l2Book", "coin": symbol.upper()}
    data = await fetch_json(session, "POST", "https://api.hyperliquid.xyz/info", json=payload)
    levels = data.get("levels") or []
    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []
    if not bids or not asks:
        raise RuntimeError(f"missing hyperliquid orderbook for {symbol}")
    return {
        "bid": float(bids[0]["px"]),
        "ask": float(asks[0]["px"]),
        "ts_ms": int(data.get("time") or 0),
    }


async def fetch_mexc_orderbook(session: aiohttp.ClientSession, symbol: str) -> dict[str, float | int]:
    contract_symbol = mexc_contract_symbol(symbol)
    url = f"https://contract.mexc.com/api/v1/contract/depth/{contract_symbol}"
    data = await fetch_json(session, "GET", url, params={"limit": 5})
    payload = data.get("data") or {}
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    if not bids or not asks:
        raise RuntimeError(f"missing mexc orderbook for {symbol}")
    bid = bids[0][0] if isinstance(bids[0], list) else bids[0].get("price")
    ask = asks[0][0] if isinstance(asks[0], list) else asks[0].get("price")
    return {
        "bid": float(bid),
        "ask": float(ask),
        "ts_ms": int(payload.get("timestamp") or payload.get("ts") or 0),
    }


async def fetch_aster_orderbook(session: aiohttp.ClientSession, symbol: str) -> dict[str, float | int]:
    symbol_metadata = await fetch_aster_symbol_metadata(session)
    metadata = symbol_metadata.get(symbol.upper())
    if metadata is None:
        raise RuntimeError(f"symbol not found on aster: {symbol}")

    raw = metadata["raw_symbol"]
    candidates = [raw]
    if raw.endswith("USD") and not raw.endswith("USDT"):
        candidates.append(raw + "T")

    last_exc: Exception = RuntimeError(f"missing aster orderbook for {symbol}")
    for contract_symbol in candidates:
        try:
            data = await fetch_json(
                session,
                "GET",
                "https://fapi.asterdex.com/fapi/v1/depth",
                params={"symbol": contract_symbol, "limit": 5},
            )
        except Exception as exc:
            last_exc = exc
            continue
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        if not bids or not asks:
            continue
        return {
            "bid": float(bids[0][0]),
            "ask": float(asks[0][0]),
            "ts_ms": int(data.get("T") or data.get("E") or data.get("lastUpdateId") or 0),
        }
    raise last_exc


async def fetch_lighter_orderbook(session: aiohttp.ClientSession, symbol: str) -> dict[str, float | int]:
    market_map = await fetch_lighter_market_map(session)
    market_id = market_map.get(symbol.upper())
    if market_id is None:
        raise RuntimeError(f"symbol not found on lighter: {symbol}")
    async with session.ws_connect("wss://mainnet.zklighter.elliot.ai/stream", heartbeat=30) as ws:
        await ws.send_json({"type": "subscribe", "channel": f"order_book/{market_id}"})
        while True:
            msg = await ws.receive(timeout=10)
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            payload = msg.json()
            if payload.get("type") == "ping":
                await ws.send_json({"type": "pong"})
                continue
            if payload.get("type") != "subscribed/order_book":
                continue
            order_book = payload.get("order_book") or {}
            bids = order_book.get("bids") or []
            asks = order_book.get("asks") or []
            if not bids or not asks:
                raise RuntimeError(f"missing lighter orderbook for {symbol}")
            bid = bids[0][0] if isinstance(bids[0], list) else bids[0].get("price")
            ask = asks[0][0] if isinstance(asks[0], list) else asks[0].get("price")
            return {
                "bid": float(bid),
                "ask": float(ask),
                "ts_ms": int(payload.get("timestamp") or payload.get("offset") or 0),
            }


def select_variational_quote_fields(listing: dict[str, Any], clip_usd: float) -> tuple[float, float]:
    quote = listing.get("quotes") or listing.get("quote") or {}
    if not isinstance(quote, dict):
        raise RuntimeError(f"missing variational quote for {listing.get('ticker')}")

    tiers = [
        ("base", 0),
        ("size_1k", 1_000),
        ("size_10k", 10_000),
        ("size_100k", 100_000),
        ("size_1m", 1_000_000),
    ]
    selected: dict[str, Any] | None = None
    for key, threshold in tiers:
        tier = quote.get(key)
        if not tier:
            continue
        if key == "base":
            selected = tier
            continue
        if clip_usd <= threshold:
            selected = tier
            break
    if selected is None:
        for key, _ in reversed(tiers):
            tier = quote.get(key)
            if tier:
                selected = tier
                break
    if selected is None:
        selected = quote

    bid = selected.get("bid") or selected.get("aggregated_bid") or listing.get("bid")
    ask = selected.get("ask") or selected.get("aggregated_ask") or listing.get("ask")
    if bid is None or ask is None:
        raise RuntimeError(f"missing variational bid/ask for {listing.get('ticker')}")
    return float(bid), float(ask)


async def fetch_variational_quote(session: aiohttp.ClientSession, symbol: str, *, clip_usd: float) -> dict[str, float | int]:
    data = await fetch_json(session, "GET", f"{VARIATIONAL_BASE_URL}/metadata/stats")
    listings = data.get("listings") or []
    for listing in listings:
        if str(listing.get("ticker") or "").upper() != symbol.upper():
            continue
        bid, ask = select_variational_quote_fields(listing, clip_usd)
        return {
            "bid": bid,
            "ask": ask,
            "ts_ms": 0,
        }
    raise RuntimeError(f"symbol not found on variational: {symbol}")
