from __future__ import annotations

from typing import Any

import aiohttp

from hydra_basis.adapters.aster import normalize_aster_symbol
from hydra_basis.streams.base import BaseStreamClient


def parse_mark_price_array_message(message: dict[str, Any]) -> dict[str, dict[str, float | int]]:
    payload = message.get("data", message)
    if not isinstance(payload, list):
        return {}

    parsed: dict[str, dict[str, float | int]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        raw_symbol = str(row.get("s") or "").upper()
        symbol = normalize_aster_symbol(raw_symbol)
        if not symbol:
            continue
        timestamp = row.get("E") or row.get("T") or 0
        parsed[symbol] = {
            "funding": float(row.get("r") or 0.0),
            "markPx": float(row.get("p") or 0.0),
            "midPx": float(row.get("p") or 0.0),
            "oraclePx": float(row.get("i") or 0.0),
            "ts_ms": int(timestamp),
            "raw_symbol": raw_symbol,
        }
    return parsed


class AsterStreamClient(BaseStreamClient):
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def connect(self) -> None:
        self.ws = await self.session.ws_connect(
            "wss://fstream.asterdex.com/stream?streams=!markPrice@arr@1s",
            heartbeat=20,
        )

    async def subscribe(self) -> None:
        return None

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
