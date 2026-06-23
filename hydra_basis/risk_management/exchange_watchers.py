from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal
import inspect
import os
from typing import AsyncIterator
from collections.abc import Callable
from urllib.parse import urlencode

import aiohttp

from hydra_basis.adapters.base import fetch_json
from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.lighter_adapter import LIGHTER_BASE_URL
from hydra_basis.execution_engine.lighter_live import fetch_lighter_orderbook_live
from hydra_basis.streams.aster import parse_mark_price_array_message
from hydra_basis.risk_management.margin_topup import VenueMarginHealthSignal
from hydra_basis.risk_management.watchers import (
    VenueRiskSignal,
    parse_aster_risk_signal,
    parse_hyperliquid_risk_signal,
)


HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
ASTER_USER_STREAM_WS_URL = "wss://fstream.asterdex.com/ws"
ASTER_MARK_PRICE_WS_URL = "wss://fstream.asterdex.com/stream?streams=!markPrice@arr@1s"


class LiveMarkPriceCache:
    def __init__(self) -> None:
        self.prices: dict[str, float] = {}

    def update(self, symbol: str, price: float) -> None:
        if price > 0:
            self.prices[str(symbol).strip().upper()] = float(price)

    async def get(self, symbol: str) -> float | None:
        return self.prices.get(str(symbol).strip().upper())


async def run_aster_mark_price_cache(cache: LiveMarkPriceCache) -> None:
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ASTER_MARK_PRICE_WS_URL, heartbeat=20) as ws:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        for symbol, item in parse_mark_price_array_message(msg.json()).items():
                            cache.update(symbol, float(item.get("markPx") or 0))
        except Exception as exc:
            print(f"aster mark price websocket error: {exc!r}; reconnecting in 10s", flush=True)
            await asyncio.sleep(10)


async def run_hyperliquid_mids_cache(cache: LiveMarkPriceCache) -> None:
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(HYPERLIQUID_WS_URL, heartbeat=20) as ws:
                    await ws.send_json({"method": "subscribe", "subscription": {"type": "allMids"}})
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = msg.json()
                        if payload.get("channel") != "allMids":
                            continue
                        mids = payload.get("data", {}).get("mids", {})
                        for symbol, price in mids.items():
                            cache.update(symbol, float(price))
        except Exception as exc:
            print(f"hyperliquid mids websocket error: {exc!r}; reconnecting in 10s", flush=True)
            await asyncio.sleep(10)


class HyperliquidUserEventsWatcher:
    def __init__(
        self,
        *,
        user: str | None = None,
        ws_url: str = HYPERLIQUID_WS_URL,
    ) -> None:
        self.user = user or os.getenv("HYPERLIQUID_ACCOUNT_ADDRESS", "")
        self.ws_url = ws_url
        if not self.user:
            raise RuntimeError("HYPERLIQUID_ACCOUNT_ADDRESS is not set")

    async def watch(self) -> AsyncIterator[VenueRiskSignal]:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.ws_url, heartbeat=20) as ws:
                await ws.send_json(
                    {
                        "method": "subscribe",
                        "subscription": {"type": "userEvents", "user": self.user},
                    }
                )
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    signal = parse_hyperliquid_risk_signal(msg.json())
                    if signal is not None:
                        yield signal


class AsterForceOrdersPoller:
    def __init__(
        self,
        *,
        adapter: AsterExecutionAdapter | None = None,
        keepalive_seconds: float = 30 * 60,
        reconnect_delay_seconds: float = 10.0,
        rate_limit_backoff_seconds: float = 10.0,
        on_rate_limit: Callable[[Exception], object] | None = None,
        ws_url: str = ASTER_USER_STREAM_WS_URL,
    ) -> None:
        self.adapter = adapter or AsterExecutionAdapter()
        self.keepalive_seconds = keepalive_seconds
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.rate_limit_backoff_seconds = rate_limit_backoff_seconds
        self.on_rate_limit = on_rate_limit
        self.ws_url = ws_url.rstrip("/")

    async def _signed_user_stream_request(self, method: str) -> dict:
        params = self.adapter.build_signed_params({})
        query = urlencode(params)
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                f"{self.adapter.BASE_URL}/fapi/v3/listenKey?{query}",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "funding-arb-risk/0.1",
                },
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster listenKey {resp.status}: {data}")
                return data

    async def _start_user_stream(self) -> str:
        data = await self._signed_user_stream_request("POST")
        listen_key = data.get("listenKey") if isinstance(data, dict) else None
        if not listen_key:
            raise RuntimeError(f"unexpected aster listenKey payload: {data}")
        return str(listen_key)

    async def _keepalive_user_stream(self, listen_key: str) -> None:
        await self._signed_user_stream_request("PUT")

    def _connect_user_stream(self, session: aiohttp.ClientSession, listen_key: str):
        return session.ws_connect(f"{self.ws_url}/{listen_key}", heartbeat=20)

    async def _run_keepalive_loop(self, listen_key: str) -> None:
        while True:
            await asyncio.sleep(self.keepalive_seconds)
            await self._keepalive_user_stream(listen_key)

    async def _notify_rate_limit(self, exc: Exception) -> None:
        if self.on_rate_limit is not None:
            result = self.on_rate_limit(exc)
            if inspect.isawaitable(result):
                await result

    @staticmethod
    def _is_rate_limit_error(exc: Exception) -> bool:
        text = str(exc)
        return "429" in text or "Too many requests" in text

    async def watch(self) -> AsyncIterator[VenueRiskSignal]:
        while True:
            keepalive_task: asyncio.Task | None = None
            try:
                listen_key = await self._start_user_stream()
                keepalive_task = asyncio.create_task(self._run_keepalive_loop(listen_key))
                async with aiohttp.ClientSession() as session:
                    ws_context = self._connect_user_stream(session, listen_key)
                    if inspect.isawaitable(ws_context):
                        ws_context = await ws_context
                    async with ws_context as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                payload = msg.json()
                                if payload.get("e") == "listenKeyExpired":
                                    break
                                signal = parse_aster_risk_signal(payload)
                                if signal is not None:
                                    yield signal
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as exc:
                if self._is_rate_limit_error(exc):
                    await self._notify_rate_limit(exc)
                    print(f"aster user stream rate limited; sleeping {self.rate_limit_backoff_seconds}s", flush=True)
                    await asyncio.sleep(self.rate_limit_backoff_seconds)
                    continue
                raise
            finally:
                if keepalive_task is not None:
                    keepalive_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await keepalive_task
            await asyncio.sleep(self.reconnect_delay_seconds)


def _strip_stable_suffix(symbol: str) -> str:
    normalized = str(symbol).upper()
    for suffix in ("USDT", "USDC", "USD"):
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


def _aster_position_side(item: dict) -> str | None:
    position_side = str(item.get("positionSide", "BOTH")).upper()
    amount = float(item.get("positionAmt", "0") or 0)
    if position_side == "LONG" or (position_side == "BOTH" and amount > 0):
        return "LONG"
    if position_side == "SHORT" or (position_side == "BOTH" and amount < 0):
        return "SHORT"
    return None


class AsterMarginHealthPoller:
    def __init__(
        self,
        *,
        adapter: AsterExecutionAdapter | None = None,
        poll_seconds: float = 30.0,
        mark_price_provider=None,
    ) -> None:
        self.adapter = adapter or AsterExecutionAdapter()
        self.poll_seconds = poll_seconds
        self.mark_price_provider = mark_price_provider

    async def _fetch_positions(self) -> list[dict]:
        params = self.adapter.build_signed_params({})
        query = urlencode(params)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.adapter.BASE_URL}/fapi/v3/positionRisk?{query}",
                headers={"User-Agent": "funding-arb-risk/0.1"},
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster positionRisk {resp.status}: {data}")
                if not isinstance(data, list):
                    raise RuntimeError(f"unexpected aster positionRisk payload: {data}")
                return data

    async def _mark_price(self, symbol: str, fallback: object) -> float:
        if self.mark_price_provider is not None:
            price = self.mark_price_provider(symbol)
            if inspect.isawaitable(price):
                price = await price
            try:
                price_float = float(price)
            except (TypeError, ValueError):
                price_float = 0.0
            if price_float > 0:
                return price_float
        return float(fallback or 0)

    async def poll_once(self) -> list[VenueMarginHealthSignal]:
        signals: list[VenueMarginHealthSignal] = []
        for item in await self._fetch_positions():
            side = _aster_position_side(item)
            if side is None:
                continue
            liq = float(item.get("liquidationPrice", "0") or 0)
            symbol = _strip_stable_suffix(str(item.get("symbol", "")))
            mark = await self._mark_price(symbol, item.get("markPrice", "0"))
            if liq <= 0 or mark <= 0:
                continue
            signals.append(
                VenueMarginHealthSignal(
                    venue="aster",
                    symbol=symbol,
                    side=side,  # type: ignore[arg-type]
                    mark_price=mark,
                    liquidation_price=liq,
                )
            )
        return signals

    async def watch(self):
        while True:
            for signal in await self.poll_once():
                yield signal
            await asyncio.sleep(self.poll_seconds)


class HyperliquidMarginHealthPoller:
    def __init__(
        self,
        *,
        user: str | None = None,
        poll_seconds: float = 30.0,
        mark_price_provider=None,
    ) -> None:
        self.user = user or os.getenv("HYPERLIQUID_ACCOUNT_ADDRESS", "")
        self.poll_seconds = poll_seconds
        self.mark_price_provider = mark_price_provider
        if not self.user:
            raise RuntimeError("HYPERLIQUID_ACCOUNT_ADDRESS is not set")

    async def _fetch_state(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "clearinghouseState", "user": self.user},
                headers={"User-Agent": "funding-arb-risk/0.1"},
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"hyperliquid clearinghouseState {resp.status}: {data}")
                return data

    async def _fetch_mids(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "allMids"},
                headers={"User-Agent": "funding-arb-risk/0.1"},
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"hyperliquid allMids {resp.status}: {data}")
                return data

    async def _mark_price(self, symbol: str, fallback: object) -> float:
        if self.mark_price_provider is not None:
            price = self.mark_price_provider(symbol)
            if inspect.isawaitable(price):
                price = await price
            try:
                price_float = float(price)
            except (TypeError, ValueError):
                price_float = 0.0
            if price_float > 0:
                return price_float
        return float(fallback or 0)

    async def poll_once(self) -> list[VenueMarginHealthSignal]:
        state = await self._fetch_state()
        signals: list[VenueMarginHealthSignal] = []
        for item in state.get("assetPositions", []):
            position = item.get("position", {})
            symbol = str(position.get("coin", "")).upper()
            size = float(position.get("szi", "0") or 0)
            liq_raw = position.get("liquidationPx")
            if not symbol or not liq_raw or size == 0:
                continue
            mark = await self._mark_price(symbol, position.get("markPx") or 0)
            liq = float(liq_raw)
            if mark <= 0 or liq <= 0:
                continue
            signals.append(
                VenueMarginHealthSignal(
                    venue="hyperliquid",
                    symbol=symbol,
                    side="LONG" if size > 0 else "SHORT",
                    mark_price=mark,
                    liquidation_price=liq,
                )
            )
        return signals

    async def watch(self):
        while True:
            for signal in await self.poll_once():
                yield signal
            await asyncio.sleep(self.poll_seconds)


def _lighter_position_side(item: dict) -> str | None:
    try:
        size = Decimal(str(item.get("position", "0") or "0"))
    except Exception:
        return None
    if size == 0:
        return None
    try:
        sign = int(item.get("sign", 1))
    except (TypeError, ValueError):
        sign = 1
    signed_size = size * sign
    return "LONG" if signed_size > 0 else "SHORT"


class LighterMarginHealthPoller:
    def __init__(
        self,
        *,
        account_index: str | None = None,
        account_fetcher=None,
        orderbook_fetcher=None,
        poll_seconds: float = 30.0,
    ) -> None:
        self.account_index = account_index or os.getenv("LIGHTER_ACCOUNT_INDEX", "")
        self.account_fetcher = account_fetcher or self._fetch_account
        self.orderbook_fetcher = orderbook_fetcher or fetch_lighter_orderbook_live
        self.poll_seconds = poll_seconds
        if account_fetcher is None and not self.account_index:
            raise RuntimeError("LIGHTER_ACCOUNT_INDEX is not set")

    async def _fetch_account(self) -> dict:
        async with aiohttp.ClientSession() as session:
            return await fetch_json(
                session,
                "GET",
                f"{LIGHTER_BASE_URL}/api/v1/account",
                params={"by": "index", "value": str(self.account_index)},
                headers={"accept": "application/json"},
            )

    async def _call_fetcher(self, fetcher, *args):
        result = fetcher(*args)
        if inspect.isawaitable(result):
            return await result
        return result

    async def poll_once(self) -> list[VenueMarginHealthSignal]:
        data = await self._call_fetcher(self.account_fetcher)
        accounts = data.get("accounts") or []
        positions = accounts[0].get("positions") if accounts else []
        signals: list[VenueMarginHealthSignal] = []
        for item in positions or []:
            if not isinstance(item, dict):
                continue
            side = _lighter_position_side(item)
            if side is None:
                continue
            symbol = str(item.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            try:
                liquidation_price = float(item.get("liquidation_price", "0") or 0)
            except (TypeError, ValueError):
                liquidation_price = 0.0
            if liquidation_price <= 0:
                print(f"lighter margin health skipped: liquidation price unavailable for {symbol}", flush=True)
                continue
            try:
                orderbook = await self._call_fetcher(self.orderbook_fetcher, symbol)
                bid = float(orderbook["bid"])
                ask = float(orderbook["ask"])
                mark = (bid + ask) / 2
            except Exception as exc:
                print(f"lighter margin health skipped: mark price unavailable for {symbol}: {exc!r}", flush=True)
                continue
            if mark <= 0:
                continue
            signals.append(
                VenueMarginHealthSignal(
                    venue="lighter",
                    symbol=symbol,
                    side=side,  # type: ignore[arg-type]
                    mark_price=mark,
                    liquidation_price=liquidation_price,
                )
            )
        return signals

    async def watch(self):
        while True:
            for signal in await self.poll_once():
                yield signal
            await asyncio.sleep(self.poll_seconds)
