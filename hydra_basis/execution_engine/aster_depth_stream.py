"""Aster top-20 snapshots plus an independent, faster best-price warning."""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import aiohttp

from hydra_basis.execution_engine.mexc_depth_stream import MexcSpotDepthStream
from hydra_basis.execution_engine.order_stream import format_stream_error


def decode_aster_book_ticker(payload: dict, symbol: str) -> dict | None:
    data = payload.get("data", payload)
    if data.get("e") != "bookTicker" or data.get("s") != symbol:
        return None
    bid, ask, bid_size, ask_size = (Decimal(data[key]) for key in ("b", "a", "B", "A"))
    if any(not value.is_finite() or value <= 0 for value in (bid, ask, bid_size, ask_size)) or bid >= ask:
        raise ValueError("invalid Aster book ticker")
    return {"bid": bid, "ask": ask, "bid_size": bid_size, "ask_size": ask_size,
            "version": int(data["u"]), "source_ms": int(data.get("T") or data["E"])}


def decode_aster_depth(payload: dict, symbol: str) -> dict | None:
    data = payload.get("data", payload)
    if data.get("e") != "depthUpdate" or data.get("s") != symbol:
        return None
    sides = []
    for key in ("b", "a"):
        levels = [(Decimal(p), Decimal(q)) for p, q in data[key]]
        if any(not p.is_finite() or not q.is_finite() or p <= 0 or q < 0 for p, q in levels):
            raise ValueError("invalid Aster depth")
        levels = sorted(((p, q) for p, q in levels if q > 0), reverse=key == "b")
        if not levels or len({p for p, _ in levels}) != len(levels):
            raise ValueError("empty or duplicate Aster depth")
        sides.append(levels)
    bids, asks = sides
    if bids[0][0] >= asks[0][0]:
        raise ValueError("crossed Aster depth")
    return {"bids": bids, "asks": asks, "bid": bids[0][0], "ask": asks[0][0],
            "version": int(data["u"]), "source_ms": int(data.get("T") or data["E"])}


class AsterDepthStream(MexcSpotDepthStream):
    """Share freshness/version/lifecycle handling with the MEXC snapshot cache."""
    venue = "aster"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ticker = None
        self._ticker_received = 0.0

    def _reset_quotes(self):
        super()._reset_quotes()
        self._ticker = None
        self._ticker_received = 0.0

    def publish_ticker(self, ticker):
        if self._ticker is not None and ticker["version"] <= self._ticker["version"]:
            return
        if self._book is not None and ticker["version"] <= self._book["version"]:
            return
        self._ticker = ticker
        self._ticker_received = time.monotonic()
        # Wake the same guard immediately, without making a REST request or
        # pretending that a best-price event refreshes the full depth snapshot.
        self._notify()

    def snapshot(self) -> dict:
        book = super().snapshot()
        if self._ticker is not None and self._ticker["version"] > book["version"]:
            age = time.monotonic() - self._ticker_received
            if age > self.max_age_seconds:
                raise self.unavailable(f"ticker_stale ticker_age_ms={age * 1000:.0f}")
            source = self._ticker["source_ms"]
            if source > 946684800000 and abs(time.time()*1000 - source) > self.max_age_seconds * 1000:
                raise self.unavailable("ticker_source_timestamp_stale")
            book["fast_quote"] = {**self._ticker, "age_ms": age * 1000,
                                  "ts_ms": self._ticker["source_ms"]}
        return book

    async def start(self):
        try:
            async with asyncio.timeout(10):
                self._ws = await self.connect()
                self.healthy = True
                self._tasks = [asyncio.create_task(self._read())]
                while self._book is None:
                    if not self.healthy:
                        raise RuntimeError("Aster depth subscription unavailable")
                    await self._changed.wait()
                self.snapshot()
        except BaseException:
            await self.close()
            raise

    async def _read(self):
        try:
            while True:
                message = await self._ws.receive()
                if message.type == aiohttp.WSMsgType.TEXT:
                    payload = message.json()
                    if payload.get("code") is not None:
                        raise RuntimeError(f"Aster market stream error: {payload['code']}")
                    book = decode_aster_depth(payload, self.symbol)
                    if book is not None:
                        self.publish(book)
                    else:
                        ticker = decode_aster_book_ticker(payload, self.symbol)
                        if ticker is not None:
                            self.publish_ticker(ticker)
                elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                    raise RuntimeError("Aster depth disconnected")
        except Exception as exc:
            self._last_error = f"reader: {format_stream_error(exc)}"
            print(f"[hedge-depth] aster: {format_stream_error(exc)}; maker quote invalidated", flush=True)
            self.healthy = False
            self._notify()

