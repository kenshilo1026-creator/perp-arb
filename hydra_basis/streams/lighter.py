from __future__ import annotations

from typing import Any

import aiohttp

from hydra_basis.streams.base import BaseStreamClient


def _lighter_percent_to_decimal(value: Any) -> float:
    return float(value or 0.0) / 100


def parse_market_stats_all_message(message: dict[str, Any]) -> dict[str, dict[str, float]]:
    market_stats = message.get("market_stats", {})
    parsed: dict[str, dict[str, float]] = {}
    for market in market_stats.values():
        symbol = str(market.get("symbol") or "").upper()
        if not symbol:
            continue
        parsed[symbol] = {
            # Lighter websocket market_stats funding values are reported in percent units.
            "funding": _lighter_percent_to_decimal(market.get("funding_rate")),
            "current_funding": _lighter_percent_to_decimal(market.get("current_funding_rate")),
            "markPx": float(market.get("mark_price") or 0.0),
            "indexPx": float(market.get("index_price") or 0.0),
            "midPx": float(market.get("mark_price") or 0.0),
        }
    return parsed


class LighterStreamClient(BaseStreamClient):
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def connect(self) -> None:
        self.ws = await self.session.ws_connect("wss://mainnet.zklighter.elliot.ai/stream?readonly=true", heartbeat=20)

    async def subscribe(self) -> None:
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        await self.ws.send_json({"type": "subscribe", "channel": "market_stats/all"})

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
