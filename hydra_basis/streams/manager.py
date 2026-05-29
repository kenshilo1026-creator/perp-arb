from __future__ import annotations

import datetime as dt
from typing import Any

import aiohttp

from hydra_basis.streams.aster import AsterStreamClient, parse_mark_price_array_message
from hydra_basis.streams.lighter import LighterStreamClient, parse_market_stats_all_message
from hydra_basis.streams.hyperliquid import HyperliquidStreamClient, parse_all_mids_message
from hydra_basis.streams.hyperliquid import parse_all_dexs_asset_ctxs_message
from hydra_basis.streams.mexc import MexcStreamClient, parse_push_ticker_message, parse_push_tickers_message


def now_ms() -> int:
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


class MarketStateStore:
    def __init__(self) -> None:
        self._mid_prices: dict[str, dict[str, dict[str, float | int]]] = {}
        self._asset_ctxs: dict[str, dict[str, dict[str, float | int]]] = {}

    def update_mids(self, venue: str, mids: dict[str, float], *, timestamp_ms: int | None = None) -> None:
        ts_ms = timestamp_ms if timestamp_ms is not None else now_ms()
        venue_state = self._mid_prices.setdefault(venue, {})
        for symbol, price in mids.items():
            venue_state[symbol] = {"price": price, "ts_ms": ts_ms}

    def get_mid_snapshot(self, venue: str) -> dict[str, dict[str, float | int]]:
        return dict(self._mid_prices.get(venue, {}))

    def update_asset_ctxs(self, venue: str, asset_ctxs: dict[str, dict[str, float]], *, timestamp_ms: int | None = None) -> None:
        venue_state = self._asset_ctxs.setdefault(venue, {})
        for symbol, ctx in asset_ctxs.items():
            ts_ms = int(ctx.get("ts_ms") or timestamp_ms or now_ms())
            venue_state[symbol] = {
                "funding": ctx.get("funding", 0.0),
                "markPx": ctx.get("markPx", 0.0),
                "midPx": ctx.get("midPx", 0.0),
                "oraclePx": ctx.get("oraclePx", 0.0),
                "ts_ms": ts_ms,
            }

    def get_asset_ctx_snapshot(self, venue: str) -> dict[str, dict[str, float | int]]:
        return dict(self._asset_ctxs.get(venue, {}))

    def get_symbols(self, venue: str) -> set[str]:
        return set(self._asset_ctxs.get(venue, {})) | set(self._mid_prices.get(venue, {}))


class HyperliquidStreamRunner:
    def __init__(self, session: aiohttp.ClientSession, state_store: MarketStateStore, ordered_symbols: list[str]) -> None:
        self.client = HyperliquidStreamClient(session)
        self.state_store = state_store
        self.ordered_symbols = ordered_symbols

    async def initialize(self) -> None:
        await self.client.connect()
        await self.client.subscribe()

    async def pump_once(self) -> dict[str, Any]:
        message = await self.client.receive()
        if isinstance(message, dict) and message.get("channel") == "allMids":
            self.state_store.update_mids("hyperliquid", parse_all_mids_message(message))
        if isinstance(message, dict) and message.get("channel") == "allDexsAssetCtxs":
            self.state_store.update_asset_ctxs(
                "hyperliquid",
                parse_all_dexs_asset_ctxs_message(message, self.ordered_symbols),
            )
        return message

    async def close(self) -> None:
        await self.client.close()


class LighterStreamRunner:
    def __init__(self, session: aiohttp.ClientSession, state_store: MarketStateStore) -> None:
        self.client = LighterStreamClient(session)
        self.state_store = state_store

    async def initialize(self) -> None:
        await self.client.connect()
        await self.client.subscribe()

    async def pump_once(self) -> dict[str, Any]:
        message = await self.client.receive()
        if isinstance(message, dict) and str(message.get("channel") or "").startswith("market_stats:"):
            parsed = parse_market_stats_all_message(message)
            self.state_store.update_asset_ctxs("lighter", parsed)
        return message

    async def close(self) -> None:
        await self.client.close()


class AsterStreamRunner:
    def __init__(self, session: aiohttp.ClientSession, state_store: MarketStateStore) -> None:
        self.client = AsterStreamClient(session)
        self.state_store = state_store

    async def initialize(self) -> None:
        await self.client.connect()
        await self.client.subscribe()

    async def pump_once(self) -> dict[str, Any]:
        message = await self.client.receive()
        if isinstance(message, dict):
            parsed = parse_mark_price_array_message(message)
            if parsed:
                self.state_store.update_asset_ctxs("aster", parsed)
        return message

    async def close(self) -> None:
        await self.client.close()


class MexcStreamRunner:
    def __init__(self, session: aiohttp.ClientSession, state_store: MarketStateStore) -> None:
        self.client = MexcStreamClient(session)
        self.state_store = state_store

    async def initialize(self) -> None:
        await self.client.connect()
        await self.client.subscribe()

    async def subscribe_symbols(self, symbols: list[str]) -> None:
        if not symbols:
            return
        await self.client.subscribe_symbols(symbols)

    async def pump_once(self) -> dict[str, Any]:
        message = await self.client.receive()
        if not isinstance(message, dict):
            return message
        channel = str(message.get("channel") or "")
        if channel == "push.tickers":
            parsed = parse_push_tickers_message(message)
            self.state_store.update_asset_ctxs("mexc", parsed)
        if channel == "push.ticker":
            parsed = parse_push_ticker_message(message)
            self.state_store.update_asset_ctxs("mexc", parsed)
        return message

    async def close(self) -> None:
        await self.client.close()
