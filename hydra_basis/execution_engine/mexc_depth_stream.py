"""Full top-20 MEXC spot snapshots used before maker fills, never on the hedge path."""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal

import aiohttp

from hydra_basis.execution_engine.order_stream import _protobuf_fields, _protobuf_items, format_stream_error


class DepthQuoteUnavailable(RuntimeError):
    """Recoverable market-data failure, distinct from invalid order pricing."""


def decode_rest_depth(data):
    """Full REST snapshots share the exchange's depth version sequence."""
    sides = []
    for key in ("bids", "asks"):
        levels = [(Decimal(p), Decimal(q)) for p, q in data[key]]
        if not levels or any(not p.is_finite() or not q.is_finite() or p <= 0 or q <= 0 for p, q in levels):
            raise ValueError("invalid REST depth levels")
        if len({p for p, _ in levels}) != len(levels):
            raise ValueError("duplicate REST depth levels")
        sides.append(sorted(levels, reverse=key == "bids"))
    bids, asks = sides
    version = int(data["lastUpdateId"])
    if version < 0 or bids[0][0] >= asks[0][0]:
        raise ValueError("invalid REST depth version or crossed book")
    # REST provides no exchange timestamp. Freshness is measured from request
    # START, not by inventing an exchange time or resetting age on a slow reply.
    return {"bid": bids[0][0], "ask": asks[0][0], "bids": bids, "asks": asks,
            "version": version, "source_ms": 0, "quote_source": "rest"}


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
    venue = "mexc_spot"

    def __init__(self, *, connect, symbol: str, max_age_seconds: float = 1.0, fetch_snapshot=None):
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
        self._last_depth_received = 0.0
        self._last_error = None
        self._recovery_lock = asyncio.Lock()
        self.fetch_snapshot = fetch_snapshot
        self._rest_lock = asyncio.Lock()
        self._rest_error = None

    def unavailable(self, reason):
        now = time.monotonic()
        age = f"{(now - self._received) * 1000:.0f}" if self._book is not None else "missing"
        message_age = f"{(now - self._last_depth_received) * 1000:.0f}" if self._last_depth_received else "missing"
        version = self._book.get("version") if self._book is not None else None
        source = self._book.get("source_ms") if self._book is not None else None
        return DepthQuoteUnavailable(
            f"hedge depth {reason}: venue={self.venue} symbol={self.symbol} "
            f"healthy={self.healthy} age_ms={age} last_depth_message_age_ms={message_age} "
            f"version={version} source_ms={source} last_error={self._last_error} rest_error={self._rest_error}")

    async def _refresh_rest(self):
        async with self._rest_lock:
            if self._book is not None and time.monotonic() - self._received < self.max_age_seconds / 2:
                try:
                    self.snapshot()
                    return
                except DepthQuoteUnavailable:
                    pass
            started = time.monotonic()
            async with asyncio.timeout(min(0.95, self.max_age_seconds)):
                data = await self.fetch_snapshot()
            book = decode_rest_depth(data)
            if not self.healthy or time.monotonic() - started > self.max_age_seconds:
                raise self.unavailable("REST_response_unusable")
            if self._book is not None:
                if book["version"] < self._book["version"]:
                    raise self.unavailable("REST_version_behind_WS")
                if book["version"] == self._book["version"]:
                    if book["bids"] != self._book["bids"] or book["asks"] != self._book["asks"]:
                        raise ValueError("conflicting REST/WS snapshots for the same version")
                    if self._received > started:
                        return  # A fresher WS snapshot arrived during this GET.
            self._book, self._received = book, started
            self._rest_error = None
            self._notify()

    async def _rest_fallback(self):
        # Public read-only requests run independently of maker-fill/hedge tasks.
        # With a quiet WS book, refresh at most twice a second (default age=1s).
        while self.healthy:
            failed = False
            if self._book is None or time.monotonic() - self._received >= self.max_age_seconds / 2:
                try:
                    await self._refresh_rest()
                except Exception as exc:
                    self._rest_error = format_stream_error(exc)
                    failed = True
            # Schedule from request START so network RTT does not accumulate
            # on top of the refresh interval. Back off on errors/rate limits.
            delay = self.max_age_seconds / 2
            if not failed and self._book is not None:
                delay = max(0.05, delay - (time.monotonic() - self._received))
            await asyncio.sleep(delay)

    def _reset_quotes(self):
        self._book = None
        self._received = self._last_depth_received = 0.0

    async def fresh_snapshot(self, *, timeout_seconds=2.5, wait_seconds=0.25):
        """Only called BEFORE a maker submission; resting-order checks never wait."""
        try:
            return self.snapshot()
        except DepthQuoteUnavailable as exc:
            last = exc
        if self._book is not None or self._last_error is not None:
            print(f"[hedge-depth] recovering before maker: {last}", flush=True)
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self._recovery_lock:
                    # A live connection may simply be between two updates.
                    deadline = time.monotonic() + wait_seconds
                    while self.healthy and time.monotonic() < deadline:
                        try:
                            return self.snapshot()
                        except DepthQuoteUnavailable as exc:
                            last = exc
                        await self.wait_for_update()
                    retry_delay = 0.1
                    while True:
                        try:
                            return self.snapshot()
                        except DepthQuoteUnavailable as exc:
                            last = exc
                        if self.healthy and self.fetch_snapshot is not None:
                            try:
                                await self._refresh_rest()
                                return self.snapshot()
                            except Exception as exc:
                                self._rest_error = format_stream_error(exc)
                                last = self.unavailable("REST_recovery_failed")
                                # WS can recover while the GET is in flight.
                                try:
                                    return self.snapshot()
                                except DepthQuoteUnavailable:
                                    pass
                        await self.close()
                        self._reset_quotes()
                        last = self.unavailable("reconnecting_waiting_for_snapshot")
                        try:
                            await self.start()
                            return self.snapshot()
                        except Exception as exc:
                            self._last_error = format_stream_error(exc)
                            last = self.unavailable("reconnect_failed")
                            await asyncio.sleep(retry_delay)
                            retry_delay = min(0.5, retry_delay * 2)
        except TimeoutError as exc:
            raise DepthQuoteUnavailable(f"market-data recovery timed out after {timeout_seconds}s; "
                                        f"phase={last}; current={self.unavailable('recovery_timeout')}") from exc

    def _notify(self):
        self._revision += 1
        self._changed.set()
        self._changed = asyncio.Event()

    def publish(self, book):
        self._last_depth_received = time.monotonic()
        if self._book is not None:
            if book["version"] < self._book["version"]:
                return
            if book["version"] == self._book["version"]:
                if book["bids"] != self._book["bids"] or book["asks"] != self._book["asks"]:
                    raise ValueError("conflicting depth snapshots for the same version")
                # A newly timestamped FULL snapshot can confirm an unchanged
                # book. Replayed timestamps and heartbeats cannot refresh it.
                source = int(book.get("source_ms") or 0)
                if source <= int(self._book.get("source_ms") or 0) or abs(time.time()*1000 - source) > self.max_age_seconds * 1000:
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
                if self.fetch_snapshot is not None:
                    self._tasks.append(asyncio.create_task(self._rest_fallback()))
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
            self._last_error = f"reader: {format_stream_error(exc)}"
            print(f"[hedge-depth] mexc_spot: {format_stream_error(exc)}; maker quote invalidated", flush=True)
            self.healthy = False
            self._notify()

    async def _ping(self):
        try:
            while True:
                await asyncio.sleep(20)
                await self._ws.send_json({"method": "PING"})
        except Exception as exc:
            self._last_error = f"ping: {format_stream_error(exc)}"
            self.healthy = False
            self._notify()

    def snapshot(self) -> dict:
        age = time.monotonic() - self._received
        if not self.healthy:
            raise self.unavailable("disconnected")
        if self._book is None:
            raise self.unavailable("missing")
        if age > self.max_age_seconds:
            raise self.unavailable("stale")
        source_ms = self._book.get("source_ms")
        if source_ms and int(source_ms) > 946684800000 and abs(time.time()*1000 - int(source_ms)) > self.max_age_seconds * 1000:
            raise self.unavailable("source_timestamp_stale")
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
