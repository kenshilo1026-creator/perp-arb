"""Full top-20 MEXC spot snapshots used before maker fills, never on the hedge path."""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal

import aiohttp

from hydra_basis.execution_engine.order_stream import _protobuf_fields, _protobuf_items, format_stream_error


def decode_depth(data: bytes, symbol: str) -> dict | None:
    wrapper = _protobuf_fields(data)
    channel = f"spot@public.limit.depth.v3.api.pb@{symbol}@20"
    if wrapper.get(1) != channel.encode() or wrapper.get(3) != symbol.encode() or 303 not in wrapper:
        return None
    fields = _protobuf_items(wrapper[303])
    sides = {1: [], 2: []}
    for number, value in fields:
        if number not in sides:
            continue
        item = _protobuf_fields(value)
        price, size = Decimal(item[1].decode()), Decimal(item[2].decode())
        if not price.is_finite() or not size.is_finite() or price <= 0 or size < 0:
            raise ValueError("invalid MEXC depth level")
        if size > 0:
            sides[number].append((price, size))
    asks, bids = sorted(sides[1]), sorted(sides[2], reverse=True)
    if not asks or not bids or bids[0][0] >= asks[0][0]:
        raise ValueError("empty or crossed MEXC depth")
    if any(len({price for price, _ in levels}) != len(levels) for levels in (asks, bids)):
        raise ValueError("duplicate MEXC depth levels")
    return {"bid": bids[0][0], "ask": asks[0][0], "bids": bids, "asks": asks,
            "version": int(dict(fields)[4]), "source_ms": wrapper.get(6)}


class MexcSpotDepthStream:
    def __init__(self, *, connect, symbol: str, max_age_seconds: float = 1.0):
        self.connect, self.symbol = connect, symbol
        self.max_age_seconds = max_age_seconds
        self._ws = None
        self._tasks = []
        self._book = None
        self._received = 0.0
        self._changed = asyncio.Event()
        self._revision = 0
        self._waited_revision = -1
        self.healthy = False

    def _notify(self):
        self._revision += 1
        self._changed.set()
        self._changed = asyncio.Event()

    def publish(self, book):
        if self._book is not None and book["version"] <= self._book["version"]:
            return
        self._book, self._received = book, time.monotonic()
        self._notify()

    async def start(self):
        try:
            async with asyncio.timeout(10):
                self._ws = await self.connect()
                await self._ws.send_json({"method": "SUBSCRIPTION", "params": [
                    f"spot@public.limit.depth.v3.api.pb@{self.symbol}@20"], "id": 1})
                self.healthy = True
                self._tasks = [asyncio.create_task(self._read()), asyncio.create_task(self._ping())]
                while self._book is None:
                    if not self.healthy:
                        raise RuntimeError("MEXC depth subscription unavailable")
                    await self._changed.wait()
                self.snapshot()
        except BaseException:
            await self.close()
            raise

    async def _read(self):
        try:
            while True:
                message = await self._ws.receive()
                if message.type == aiohttp.WSMsgType.BINARY:
                    book = decode_depth(message.data, self.symbol)
                    if book is not None:
                        self.publish(book)
                elif message.type == aiohttp.WSMsgType.TEXT:
                    payload = json.loads(message.data)
                    if payload.get("code", 0) not in (0, "0"):
                        raise RuntimeError("MEXC depth subscription rejected")
                elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                    raise RuntimeError("MEXC depth disconnected")
        except Exception as exc:
            print(f"[hedge-depth] mexc_spot: {format_stream_error(exc)}; maker quote invalidated", flush=True)
            self.healthy = False
            self._notify()

    async def _ping(self):
        try:
            while True:
                await asyncio.sleep(20)
                await self._ws.send_json({"method": "PING"})
        except Exception:
            self.healthy = False
            self._notify()

    def snapshot(self) -> dict:
        age = time.monotonic() - self._received
        if not self.healthy or self._book is None or age > self.max_age_seconds:
            raise RuntimeError("MEXC hedge depth disconnected, missing or stale")
        source_ms = self._book.get("source_ms")
        return {
            **self._book,
            "age_ms": age * 1000,
            "ts_ms": int(source_ms) if source_ms not in (None, b"") else 0,
        }

    async def wait_for_update(self):
        # The timeout only checks freshness; no network request is made here.
        if self._waited_revision == self._revision:
            try:
                await asyncio.wait_for(self._changed.wait(), 0.05)
            except TimeoutError:
                pass
        self._waited_revision = self._revision

    async def close(self):
        self.healthy = False
        self._notify()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
