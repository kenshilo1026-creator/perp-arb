"""Private order events wake fill waiters; REST remains an independent fallback."""
from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict
from decimal import Decimal

import aiohttp

from hydra_basis.execution_engine.order_fill import poll_until_filled, status_dict_looks_filled


def decode_aster_order(message: dict) -> dict | None:
    if message.get("e") == "listenKeyExpired":
        raise RuntimeError("Aster order stream expired")
    if message.get("e") != "ORDER_TRADE_UPDATE":
        return None
    order = message["o"]
    return {
        "symbol": order["s"], "orderId": str(order["i"]),
        "status": order["X"], "executedQty": order["z"],
        "origQty": order["q"], "avgPrice": order.get("ap", "0"),
        "eventTime": message["E"], "tradeTime": order.get("T", message.get("T")),
    }


def _protobuf_fields(data: bytes) -> dict[int, bytes | int]:
    """Read the scalar fields used by MEXC's private-order protobuf schema.

    Unknown scalar fields are skipped; malformed/truncated frames fail closed.
    Schema: https://github.com/mexcdevelop/websocket-proto
    """
    position = 0
    fields = {}

    def varint():
        nonlocal position
        value = 0
        for shift in range(0, 70, 7):
            if position >= len(data):
                raise ValueError("truncated protobuf varint")
            byte = data[position]
            position += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError("invalid protobuf varint")

    while position < len(data):
        tag = varint()
        number, wire = tag >> 3, tag & 7
        if not number:
            raise ValueError("invalid protobuf field")
        if wire == 0:
            fields[number] = varint()
        elif wire in (1, 2, 5):
            length = varint() if wire == 2 else (8 if wire == 1 else 4)
            end = position + length
            if end > len(data):
                raise ValueError("truncated protobuf field")
            if wire == 2:
                fields[number] = data[position:end]
            position = end
        else:
            raise ValueError("unsupported protobuf wire type")
    return fields


MEXC_ORDER_CHANNEL = "spot@private.orders.v3.api.pb"


def decode_mexc_order(data: bytes) -> dict | None:
    # PushDataV3ApiWrapper: channel=1, symbol=3, sendTime=6, privateOrders=304.
    wrapper = _protobuf_fields(data)
    if wrapper.get(1) != MEXC_ORDER_CHANNEL.encode() or 304 not in wrapper:
        return None
    order = _protobuf_fields(wrapper[304])
    status = {1: "NEW", 2: "FILLED", 3: "PARTIALLY_FILLED",
              4: "CANCELED", 5: "CANCELED"}.get(order.get(15))
    if status is None:
        raise ValueError("unknown MEXC private order status")
    return {
        "symbol": wrapper[3].decode(), "orderId": order[1].decode(),
        "status": status, "executedQty": order.get(13, b"0").decode(),
        "origQty": order[4].decode(), "avgPrice": order.get(6, b"0").decode(),
        "cummulativeQuoteQty": order.get(14, b"0").decode(),
        "eventTime": wrapper.get(6, 0),
    }


class OrderEventStream:
    """One account connection, bounded cache, and order/symbol-specific wakeups.

    Start before maker submission so a fill preceding the HTTP ACK is cached.
    A disconnected stream falls back to REST for this batch; the next batch
    creates a new connection. Never log websocket URLs (they contain a token).
    """

    def __init__(self, *, venue, connect, renew, decode, channel=None):
        self.venue, self.connect, self.renew = venue, connect, renew
        self.decode, self.channel = decode, channel
        self.healthy = False
        self._ws = None
        self._tasks = []
        self._cache = OrderedDict()
        self._delivered = {}
        self._changed = asyncio.Event()
        self._sequence = 0

    def _notify(self):
        self._changed.set()
        self._changed = asyncio.Event()

    def publish(self, status):
        quantity = Decimal(str(status["executedQty"]))
        original = Decimal(str(status["origQty"]))
        if (not quantity.is_finite() or not original.is_finite()
                or quantity < 0 or original <= 0 or quantity > original):
            raise ValueError("invalid cumulative order fill")
        if status_dict_looks_filled(status) and quantity != original:
            raise ValueError("terminal fill does not match order quantity")
        key = (str(status["symbol"]), str(status["orderId"]))
        previous = self._cache.get(key)
        if previous:
            old = previous[1]
            if (int(status.get("eventTime", 0)) < int(old.get("eventTime", 0))
                    or quantity < Decimal(str(old["executedQty"]))
                    or status_dict_looks_filled(old)):
                return
            if quantity == Decimal(str(old["executedQty"])) and status["status"] == old["status"]:
                return
        self._sequence += 1
        self._cache[key] = (self._sequence, dict(status), time.monotonic_ns())
        self._cache.move_to_end(key)
        if len(self._cache) > 1024:
            removed, _ = self._cache.popitem(last=False)
            self._delivered.pop(removed, None)
        self._notify()

    def _handle(self, message):
        if message.type == aiohttp.WSMsgType.TEXT:
            payload = json.loads(message.data)
            if self.channel:
                if "code" in payload and payload["code"] != 0:
                    raise RuntimeError("private order subscription rejected")
                return payload.get("code") == 0 and payload.get("msg") == self.channel
            status = self.decode(payload)
        elif message.type == aiohttp.WSMsgType.BINARY and self.channel:
            status = self.decode(message.data)
        elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                               aiohttp.WSMsgType.ERROR):
            raise RuntimeError("private order stream disconnected")
        else:
            return False
        if status is not None:
            self.publish(status)
        return False

    async def start(self):
        try:
            async with asyncio.timeout(10):
                self._ws = await self.connect()
                if self.channel:
                    await self._ws.send_json({"method": "SUBSCRIPTION", "params": [self.channel], "id": 1})
                    while not self._handle(await self._ws.receive()):
                        pass
            self.healthy = True
            self._tasks = [asyncio.create_task(self._reader()), asyncio.create_task(self._maintain())]
        except BaseException:
            await self.close()
            raise

    def _failed(self, exc):
        if self.healthy:
            print(f"[fill-stream] {self.venue}: {type(exc).__name__}; using REST fallback", flush=True)
        self.healthy = False
        self._notify()

    async def _reader(self):
        try:
            while True:
                self._handle(await self._ws.receive())
        except Exception as exc:
            self._failed(exc)

    async def _maintain(self):
        next_renew = time.monotonic() + 30 * 60
        try:
            while True:
                await asyncio.sleep(20)
                if not self.healthy:
                    return
                if self.channel:
                    await self._ws.send_json({"method": "PING"})
                if time.monotonic() >= next_renew:
                    async with asyncio.timeout(10):
                        await self.renew()
                    next_renew = time.monotonic() + 30 * 60
        except Exception as exc:
            self._failed(exc)

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

    def _cached_fill(self, key, partial):
        record = self._cache.get(key)
        if record is None:
            return None
        sequence, status, received = record
        complete = status_dict_looks_filled(status)
        if not complete and (not partial or sequence <= self._delivered.get(key, 0)):
            return None
        quantity = Decimal(str(status["executedQty"]))
        if quantity <= 0:
            return None
        self._delivered[key] = sequence
        result = {"ok": True, "raw": status, "filled_quantity": str(quantity),
                  "source": "user_stream", "observed_monotonic_ns": received}
        if not complete:
            result["partial"] = True
        return result

    async def _wait_event(self, key, partial):
        while True:
            result = self._cached_fill(key, partial)
            if result is not None:
                return result
            if not self.healthy:
                raise RuntimeError("order stream unavailable")
            await self._changed.wait()

    async def wait_for_fill(self, *, symbol, order_id, fetch_status,
                            timeout_seconds, poll_interval_seconds,
                            timeout_message, return_on_partial_fill=False):
        key = (symbol, str(order_id))
        cached = self._cached_fill(key, return_on_partial_fill)
        if cached is not None:
            return cached
        tasks = [asyncio.create_task(poll_until_filled(
            fetch_status=fetch_status, timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds, timeout_message=timeout_message,
            return_on_partial_fill=return_on_partial_fill))]
        event = None
        if self.healthy:
            event = asyncio.create_task(self._wait_event(key, return_on_partial_fill))
            tasks.append(event)
        pending = set(tasks)
        try:
            async with asyncio.timeout(timeout_seconds):
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    # Prefer the push when a REST reply completes at the same time.
                    ordered = sorted(done, key=lambda task: task is not event)
                    for task in ordered:
                        try:
                            result = task.result()
                        except Exception:
                            if not pending and all(t.exception() is not None for t in done):
                                raise
                            continue
                        result.setdefault("source", "rest_poll")
                        result.setdefault("observed_monotonic_ns", time.monotonic_ns())
                        return result
        except TimeoutError as exc:
            raise RuntimeError(timeout_message) from exc
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
