"""Private order events wake fill waiters; REST remains an independent fallback."""
from __future__ import annotations

import asyncio
import json
import re
import time
from collections import OrderedDict
from decimal import Decimal

import aiohttp

from hydra_basis.execution_engine.order_fill import poll_until_filled, status_dict_looks_filled
from hydra_basis.execution_engine.hedge_safety import terminal_fill_quantity


def redact_stream_message(message, secrets=()) -> str:
    text = str(message or "")
    for secret in secrets:
        if secret:
            text = text.replace(str(secret), "<redacted>")
    text = re.sub(r"(?:https?|wss?)://\S+", "<url redacted>", text, flags=re.I)
    text = re.sub(
        r"(?i)((?:listen[_-]?key|api[_-]?key|secret|signature|authorization|token)"
        r"[\"']?\s*[:=]\s*[\"']?)[^\s,\"'&}]+", r"\1<redacted>", text,
    )
    text = re.sub(r"[A-Za-z0-9_+/=-]{24,}", "<redacted>", text)
    return " ".join(text.split())[:240]


class OrderStreamError(RuntimeError):
    """A public diagnostic; never retain raw response bodies or request URLs."""

    def __init__(self, stage, *, http_status=None, payload=None, reason=None, secrets=()):
        parts = [stage]
        if isinstance(http_status, int):
            parts.append(f"HTTP {http_status}")
        if isinstance(payload, dict):
            code = payload.get("code")
            if re.fullmatch(r"-?\d{1,10}", str(code)):
                parts.append(f"code={code}")
            message = redact_stream_message(payload.get("msg") or payload.get("message"), secrets)
            if message:
                parts.append(f"message={message}")
        if reason:
            parts.append(redact_stream_message(reason, secrets))
        super().__init__("; ".join(parts))


def format_stream_error(exc: Exception) -> str:
    if isinstance(exc, OrderStreamError):
        return str(exc)
    # aiohttp exception strings can contain authenticated URLs. Whitelist only
    # the numeric HTTP status; unknown errors continue to show their type.
    status = getattr(exc, "status", None)
    return type(exc).__name__ + (f" (HTTP {status})" if isinstance(status, int) else "")


async def prepare_private_order_streams(legs):
    """Subscribe both legs before submitting a maker; failures retain REST."""
    for venue, adapter in legs:
        prepare = getattr(adapter, "prepare_maker_fill_stream", None)
        if callable(prepare):
            try:
                await prepare()
                print(f"[fill-stream] {venue}: private order stream ready", flush=True)
            except Exception as exc:
                print(f"[fill-stream] {venue}: {format_stream_error(exc)}; using REST fallback", flush=True)


def decode_aster_order(message: dict) -> dict | None:
    if message.get("e") == "listenKeyExpired":
        raise OrderStreamError("aster user stream", reason="listen key expired")
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
                    or (terminal_fill_quantity(old) is not None and terminal_fill_quantity(status) is None)
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
                    raise OrderStreamError(f"{self.venue} subscription rejected", payload=payload)
                return payload.get("code") == 0 and payload.get("msg") == self.channel
            status = self.decode(payload)
        elif message.type == aiohttp.WSMsgType.BINARY and self.channel:
            status = self.decode(message.data)
        elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                               aiohttp.WSMsgType.ERROR):
            raise OrderStreamError(f"{self.venue} websocket", reason="connection closed or failed")
        else:
            return False
        if status is not None:
            self.publish(status)
        return False

    async def start(self):
        stage = "connect"
        try:
            async with asyncio.timeout(10):
                self._ws = await self.connect()
                if self.channel:
                    stage = "subscription acknowledgement"
                    await self._ws.send_json({"method": "SUBSCRIPTION", "params": [self.channel], "id": 1})
                    while not self._handle(await self._ws.receive()):
                        pass
            self.healthy = True
            self._tasks = [asyncio.create_task(self._reader()), asyncio.create_task(self._maintain())]
        except BaseException as exc:
            await self.close()
            if isinstance(exc, Exception) and not isinstance(exc, OrderStreamError):
                raise OrderStreamError(f"{self.venue} {stage}", reason=format_stream_error(exc)) from None
            raise

    def _failed(self, exc):
        if self.healthy:
            print(f"[fill-stream] {self.venue}: {format_stream_error(exc)}; using REST fallback", flush=True)
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

    def cached_terminal(self, *, symbol, order_id):
        record = self._cache.get((symbol, str(order_id)))
        if record is None:
            return None
        _, status, received = record
        quantity = terminal_fill_quantity(status)
        if quantity is None:
            return None
        # Include zero-fill cancellations/rejections, unlike maker fill waiters.
        # Reading this cache must not consume maker partial-fill notifications.
        return {"ok": True, "terminal": True, "filled_quantity": str(quantity),
                "raw": dict(status), "source": "user_stream",
                "observed_monotonic_ns": received}

    async def wait_for_terminal(self, *, symbol, order_id):
        while True:
            result = self.cached_terminal(symbol=symbol, order_id=order_id)
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


def _protobuf_items(data: bytes) -> list[tuple[int, bytes | int]]:
    """Read the scalar fields used by MEXC's private-order protobuf schema.

    Unknown scalar fields are skipped; malformed/truncated frames fail closed.
    Schema: https://github.com/mexcdevelop/websocket-proto
    """
    position = 0
    fields = []

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
            fields.append((number, varint()))
        elif wire in (1, 2, 5):
            length = varint() if wire == 2 else (8 if wire == 1 else 4)
            end = position + length
            if end > len(data):
                raise ValueError("truncated protobuf field")
            if wire == 2:
                fields.append((number, data[position:end]))
            position = end
        else:
            raise ValueError("unsupported protobuf wire type")
    return fields
