"""Binance Spot JSON depth and signed WebSocket-API account subscriptions."""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal

import aiohttp

from .aster_depth_stream import AsterDepthStream
from .mexc_depth_stream import decode_rest_depth
from .order_stream import OrderEventStream, OrderStreamError, format_stream_error


def decode_binance_order(payload: dict) -> dict | None:
    event = payload.get("event", {})
    if event.get("e") in {"eventStreamTerminated", "serverShutdown"}:
        raise OrderStreamError("binance_spot user stream", reason="subscription terminated")
    if event.get("e") != "executionReport":
        return None
    status = event["X"]
    if status == "EXPIRED_IN_MATCH":
        status = "EXPIRED"
    return {
        "symbol": event["s"], "orderId": str(event["i"]),
        "clientOrderId": event["c"], "side": event["S"],
        "status": status, "origQty": event["q"], "executedQty": event["z"],
        "cummulativeQuoteQty": event["Z"], "eventTime": event["E"],
        "tradeTime": event.get("T"),
    }


class BinanceOrderEventStream(OrderEventStream):
    def __init__(self, *, connect, subscription):
        super().__init__(venue="binance_spot", connect=connect, renew=None,
                         decode=decode_binance_order)
        self.subscription = subscription
        self.subscription_id = None

    def _handle(self, message):
        if message.type == aiohttp.WSMsgType.TEXT:
            payload = json.loads(message.data)
            if payload.get("event", {}).get("e") == "serverShutdown":
                raise OrderStreamError("binance_spot user stream", reason="server shutdown")
            if "event" in payload and payload.get("subscriptionId") != self.subscription_id:
                raise OrderStreamError("binance_spot user stream", reason="wrong subscription identity")
        return super()._handle(message)

    async def start(self):
        try:
            async with asyncio.timeout(10):
                self._ws = await self.connect()
                request = self.subscription()
                await self._ws.send_json(request)
                pending = []
                while True:
                    message = await self._ws.receive()
                    if message.type != aiohttp.WSMsgType.TEXT:
                        self._handle(message)
                        continue
                    payload = json.loads(message.data)
                    if payload.get("id") == request["id"]:
                        if payload.get("status") != 200:
                            # Do not retain server text that might echo signed parameters.
                            raise OrderStreamError("binance_spot subscription", http_status=payload.get("status"),
                                                   reason="signed subscription rejected")
                        sid = (payload.get("result") or {}).get("subscriptionId")
                        if type(sid) is not int or sid < 0:
                            raise OrderStreamError("binance_spot subscription", reason="missing subscription ID")
                        self.subscription_id = sid
                        break
                    if len(pending) >= 128:
                        raise OrderStreamError("binance_spot subscription", reason="pre-ACK event overflow")
                    pending.append(message)
                for message in pending:
                    self._handle(message)
            self.healthy = True
            self._tasks = [asyncio.create_task(self._reader())]
        except BaseException as exc:
            await self.close()
            if isinstance(exc, Exception) and not isinstance(exc, OrderStreamError):
                raise OrderStreamError("binance_spot subscription", reason=format_stream_error(exc)) from None
            raise


class BinanceSpotDepthStream(AsterDepthStream):
    """Full top-20 snapshots plus BBO warnings; never stitch partial books."""
    venue = "binance_spot"

    def publish(self, book):
        # Binance partial-depth frames have no exchange timestamp. A newly
        # received full, identical snapshot confirms the unchanged book locally.
        if self._book is not None and book["version"] == self._book["version"]:
            if book["bids"] != self._book["bids"] or book["asks"] != self._book["asks"]:
                raise ValueError("conflicting Binance full snapshot")
            self._received = self._last_depth_received = time.monotonic()
            self._notify()
            return
        super().publish(book)

    async def _read(self):
        try:
            while True:
                message = await self._ws.receive()
                if message.type == aiohttp.WSMsgType.TEXT:
                    payload = message.json()
                    name, data = payload.get("stream"), payload.get("data", {})
                    if name == f"{self.symbol.lower()}@depth20@100ms":
                        book = decode_rest_depth(data)
                        book["quote_source"] = "binance_partial_depth_ws"
                        self.publish(book)
                    elif name == f"{self.symbol.lower()}@bookTicker" and data.get("s") == self.symbol:
                        bid, ask, bq, aq = (Decimal(data[k]) for k in ("b", "a", "B", "A"))
                        if any(not x.is_finite() or x <= 0 for x in (bid, ask, bq, aq)) or bid >= ask:
                            raise ValueError("invalid Binance book ticker")
                        self.publish_ticker({"bid": bid, "ask": ask, "bid_size": bq,
                                             "ask_size": aq, "version": int(data["u"]), "source_ms": 0})
                    elif "code" in payload:
                        raise RuntimeError("Binance market stream rejected")
                elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    raise RuntimeError("Binance depth disconnected")
        except Exception as exc:
            self._last_error = format_stream_error(exc)
            self.healthy = False
            self._notify()
