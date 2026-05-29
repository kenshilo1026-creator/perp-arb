from __future__ import annotations

from typing import Any

import aiohttp

from hydra_basis.adapters.mexc import mexc_contract_symbol
from hydra_basis.streams.base import BaseStreamClient


def normalize_mexc_symbol(raw_symbol: str) -> str:
    upper = raw_symbol.upper()
    if upper.endswith("_USDT"):
        return upper[:-5]
    return upper


def parse_push_tickers_message(message: dict[str, Any]) -> dict[str, dict[str, float | int | str]]:
    rows = message.get("data")
    if not isinstance(rows, list):
        return {}

    parsed: dict[str, dict[str, float | int | str]] = {}
    timestamp = int(message.get("ts") or 0)
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_symbol = str(row.get("symbol") or "").upper()
        symbol = normalize_mexc_symbol(raw_symbol)
        if not symbol:
            continue
        parsed[symbol] = {
            "symbol": symbol,
            "raw_symbol": raw_symbol,
            "markPx": float(row.get("fairPrice") or 0.0),
            "midPx": float(row.get("lastPrice") or row.get("fairPrice") or 0.0),
            "ts_ms": timestamp,
        }
    return parsed


def parse_push_ticker_message(message: dict[str, Any]) -> dict[str, dict[str, float | int]]:
    row = message.get("data")
    if not isinstance(row, dict):
        return {}

    raw_symbol = str(row.get("symbol") or message.get("symbol") or "").upper()
    symbol = normalize_mexc_symbol(raw_symbol)
    if not symbol:
        return {}

    timestamp = int(row.get("timestamp") or message.get("ts") or 0)
    return {
        symbol: {
            "funding": float(row.get("fundingRate") or 0.0),
            "markPx": float(row.get("fairPrice") or 0.0),
            "midPx": float(row.get("lastPrice") or row.get("fairPrice") or 0.0),
            "oraclePx": float(row.get("indexPrice") or 0.0),
            "ts_ms": timestamp,
        }
    }


class MexcStreamClient(BaseStreamClient):
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def connect(self) -> None:
        self.ws = await self.session.ws_connect("wss://contract.mexc.com/edge", heartbeat=20)

    async def subscribe(self) -> None:
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        await self.ws.send_json({"method": "sub.tickers", "param": {}})

    async def subscribe_symbols(self, symbols: list[str]) -> None:
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        for symbol in symbols:
            await self.ws.send_json(
                {
                    "method": "sub.ticker",
                    "param": {"symbol": mexc_contract_symbol(symbol)},
                }
            )

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
