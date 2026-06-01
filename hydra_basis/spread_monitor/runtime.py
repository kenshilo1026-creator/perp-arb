from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import aiohttp

from hydra_basis.adapters.aster import fetch_aster_symbol_metadata, normalize_aster_symbol
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_universe
from hydra_basis.adapters.lighter import fetch_lighter_market_map
from hydra_basis.adapters.mexc import list_symbols as list_mexc_symbols
from hydra_basis.streams.manager import (
    AsterStreamRunner,
    HyperliquidStreamRunner,
    LighterStreamRunner,
    MarketStateStore,
    MexcStreamRunner,
)


@dataclass(frozen=True)
class StreamBootstrap:
    enabled_venues: list[str]
    hyperliquid_ordered_symbols: list[str]
    lighter_market_map: dict[str, int]
    mexc_subscription_symbols: list[str]


def build_stream_bootstrap(
    *,
    venues: list[str],
    hyperliquid_ordered_symbols: list[str],
    lighter_market_map: dict[str, int],
    mexc_symbols: set[str],
) -> StreamBootstrap:
    return StreamBootstrap(
        enabled_venues=list(venues),
        hyperliquid_ordered_symbols=list(hyperliquid_ordered_symbols),
        lighter_market_map=dict(lighter_market_map),
        mexc_subscription_symbols=sorted(mexc_symbols),
    )


class HyperliquidQuoteRunner:
    def __init__(self, session: aiohttp.ClientSession, state_store: MarketStateStore, ordered_symbols: list[str]) -> None:
        self.session = session
        self.state_store = state_store
        self.ordered_symbols = ordered_symbols
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def initialize(self) -> None:
        self.ws = await self.session.ws_connect("wss://api.hyperliquid.xyz/ws", heartbeat=20)
        for symbol in self.ordered_symbols:
            await self.ws.send_json(
                {"method": "subscribe", "subscription": {"type": "l2Book", "coin": symbol}}
            )

    async def pump_once(self) -> dict[str, Any]:
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        message = await self.ws.receive()
        if message.type != aiohttp.WSMsgType.TEXT:
            return {"type": str(message.type)}
        payload = message.json()
        if isinstance(payload, dict) and payload.get("channel") == "l2Book":
            parsed = parse_hyperliquid_l2_book(payload)
            if parsed:
                self.state_store.update_quotes("hyperliquid", parsed)
        return payload

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()


class LighterQuoteRunner:
    def __init__(self, session: aiohttp.ClientSession, state_store: MarketStateStore, market_map: dict[str, int]) -> None:
        self.session = session
        self.state_store = state_store
        self.market_map = market_map
        self.market_by_channel = {f"ticker:{market_id}": symbol for symbol, market_id in market_map.items()}
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def initialize(self) -> None:
        self.ws = await self.session.ws_connect("wss://mainnet.zklighter.elliot.ai/stream?readonly=true", heartbeat=20)
        for market_id in self.market_map.values():
            await self.ws.send_json({"type": "subscribe", "channel": f"ticker/{market_id}"})

    async def pump_once(self) -> dict[str, Any]:
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        message = await self.ws.receive()
        if message.type != aiohttp.WSMsgType.TEXT:
            return {"type": str(message.type)}
        payload = message.json()
        if payload.get("type") == "ping":
            await self.ws.send_json({"type": "pong"})
            return payload
        parsed = parse_lighter_ticker_message(payload, market_by_channel=self.market_by_channel)
        if parsed:
            self.state_store.update_quotes("lighter", parsed)
        return payload

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()


class AsterQuoteRunner:
    def __init__(self, session: aiohttp.ClientSession, state_store: MarketStateStore) -> None:
        self.session = session
        self.state_store = state_store
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def initialize(self) -> None:
        self.ws = await self.session.ws_connect(
            "wss://fstream.asterdex.com/stream?streams=!bookTicker",
            heartbeat=20,
        )

    async def pump_once(self) -> dict[str, Any]:
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        message = await self.ws.receive()
        if message.type != aiohttp.WSMsgType.TEXT:
            return {"type": str(message.type)}
        payload = message.json()
        parsed = parse_aster_book_ticker_message(payload)
        if parsed:
            self.state_store.update_quotes("aster", parsed)
        return payload

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()


def parse_hyperliquid_l2_book(message: dict[str, Any]) -> dict[str, dict[str, float | int]]:
    data = message.get("data") or {}
    symbol = str(data.get("coin") or "").upper()
    levels = data.get("levels") or []
    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []
    if not symbol or not bids or not asks:
        return {}
    best_bid = bids[0]
    best_ask = asks[0]
    bid = best_bid.get("px") if isinstance(best_bid, dict) else None
    ask = best_ask.get("px") if isinstance(best_ask, dict) else None
    if bid is None or ask is None:
        return {}
    return {
        symbol: {
            "bid": float(bid),
            "ask": float(ask),
            "ts_ms": int(data.get("time") or 0),
        }
    }


def parse_lighter_ticker_message(
    message: dict[str, Any],
    *,
    market_by_channel: dict[str, str],
) -> dict[str, dict[str, float | int]]:
    channel = str(message.get("channel") or "")
    ticker = message.get("ticker") or {}
    symbol = str(ticker.get("s") or market_by_channel.get(channel) or "").upper()
    bid = ((ticker.get("b") or {}).get("price"))
    ask = ((ticker.get("a") or {}).get("price"))
    if not symbol or bid is None or ask is None:
        return {}
    try:
        bid_value = float(bid)
        ask_value = float(ask)
    except (TypeError, ValueError):
        return {}
    return {
        symbol: {
            "bid": bid_value,
            "ask": ask_value,
            "ts_ms": int(message.get("timestamp") or 0),
        }
    }


def parse_aster_book_ticker_message(message: dict[str, Any]) -> dict[str, dict[str, float | int]]:
    payload = message.get("data", message)
    if not isinstance(payload, dict):
        return {}
    raw_symbol = str(payload.get("s") or "").upper()
    symbol = normalize_aster_symbol(raw_symbol)
    bid = payload.get("b")
    ask = payload.get("a")
    if not symbol or bid is None or ask is None:
        return {}
    return {
        symbol: {
            "bid": float(bid),
            "ask": float(ask),
            "ts_ms": int(payload.get("E") or payload.get("T") or 0),
        }
    }


async def start_spread_monitor_runners(
    *,
    session: aiohttp.ClientSession,
    state_store: MarketStateStore,
    venues: list[str],
) -> dict[str, object]:
    hyperliquid_symbols: list[str] = []
    lighter_market_map: dict[str, int] = {}
    mexc_symbols: set[str] = set()

    if "hyperliquid" in venues:
        hyperliquid_symbols = await fetch_hyperliquid_universe(session)
    if "lighter" in venues:
        lighter_market_map = await fetch_lighter_market_map(session)
    if "mexc" in venues:
        mexc_symbols = await list_mexc_symbols(session)

    bootstrap = build_stream_bootstrap(
        venues=venues,
        hyperliquid_ordered_symbols=hyperliquid_symbols,
        lighter_market_map=lighter_market_map,
        mexc_symbols=mexc_symbols,
    )

    runners: dict[str, object] = {}
    for venue in bootstrap.enabled_venues:
        if venue == "hyperliquid":
            runner = HyperliquidStreamRunner(session, state_store, bootstrap.hyperliquid_ordered_symbols)
            await runner.initialize()
            quote_runner = HyperliquidQuoteRunner(session, state_store, bootstrap.hyperliquid_ordered_symbols)
            await quote_runner.initialize()
            runners[f"{venue}:funding"] = runner
            runners[f"{venue}:quote"] = quote_runner
            continue
        if venue == "lighter":
            runner = LighterStreamRunner(session, state_store)
            await runner.initialize()
            quote_runner = LighterQuoteRunner(session, state_store, bootstrap.lighter_market_map)
            await quote_runner.initialize()
            runners[f"{venue}:funding"] = runner
            runners[f"{venue}:quote"] = quote_runner
            continue
        if venue == "aster":
            runner = AsterStreamRunner(session, state_store)
            await runner.initialize()
            quote_runner = AsterQuoteRunner(session, state_store)
            await quote_runner.initialize()
            runners[f"{venue}:funding"] = runner
            runners[f"{venue}:quote"] = quote_runner
            continue
        if venue == "mexc":
            runner = MexcStreamRunner(session, state_store)
            await runner.initialize()
            await runner.subscribe_symbols(bootstrap.mexc_subscription_symbols)
            runners[venue] = runner
    return runners


async def pump_runner_forever(venue: str, runner: object) -> None:
    while True:
        try:
            await runner.pump_once()
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"spread monitor websocket error venue={venue}: {exc!r}")
            raise


async def close_runners(runners: dict[str, object]) -> None:
    for runner in runners.values():
        await runner.close()
