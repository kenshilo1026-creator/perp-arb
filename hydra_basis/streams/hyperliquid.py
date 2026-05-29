from __future__ import annotations

from typing import Any

import aiohttp

from hydra_basis.streams.base import BaseStreamClient


def parse_all_mids_message(message: dict[str, Any]) -> dict[str, float]:
    mids = message.get("data", {}).get("mids", {})
    return {
        symbol: float(price)
        for symbol, price in mids.items()
        if isinstance(symbol, str) and isinstance(price, str)
    }


def parse_all_dexs_asset_ctxs_message(message: dict[str, Any], ordered_symbols: list[str]) -> dict[str, dict[str, float]]:
    ctx_groups = message.get("data", {}).get("ctxs", [])
    if not ctx_groups:
        return {}

    first_group = ctx_groups[0]
    if not isinstance(first_group, list) or len(first_group) < 2:
        return {}

    ctxs = first_group[1]
    parsed: dict[str, dict[str, float]] = {}
    for symbol, ctx in zip(ordered_symbols, ctxs):
        if not isinstance(ctx, dict):
            continue
        funding = ctx.get("funding")
        mark_px = ctx.get("markPx")
        mid_px = ctx.get("midPx")
        oracle_px = ctx.get("oraclePx")
        parsed[symbol] = {
            "funding": float(funding) if funding is not None else 0.0,
            "markPx": float(mark_px) if mark_px is not None else 0.0,
            "midPx": float(mid_px) if mid_px is not None else 0.0,
            "oraclePx": float(oracle_px) if oracle_px is not None else 0.0,
        }
    return parsed


class HyperliquidStreamClient(BaseStreamClient):
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def connect(self) -> None:
        self.ws = await self.session.ws_connect("wss://api.hyperliquid.xyz/ws", heartbeat=20)

    async def subscribe(self) -> None:
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        await self.ws.send_json({"method": "subscribe", "subscription": {"type": "allMids"}})
        await self.ws.send_json({"method": "subscribe", "subscription": {"type": "allDexsAssetCtxs"}})

    async def receive(self) -> Any:
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        message = await self.ws.receive()
        if message.type == aiohttp.WSMsgType.TEXT:
            return message.json()
        return message.data

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()
