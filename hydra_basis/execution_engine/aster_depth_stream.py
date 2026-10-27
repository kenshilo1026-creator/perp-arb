"""Aster top-20 full snapshots, not incremental depth deltas."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import aiohttp

from hydra_basis.execution_engine.mexc_depth_stream import MexcSpotDepthStream


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
                    book = decode_aster_depth(message.json(), self.symbol)
                    if book is not None:
                        self.publish(book)
                elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                    raise RuntimeError("Aster depth disconnected")
        except Exception:
            self.healthy = False
            self._notify()

