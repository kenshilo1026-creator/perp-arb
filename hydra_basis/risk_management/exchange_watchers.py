from __future__ import annotations

import asyncio
import os
from typing import AsyncIterator
from urllib.parse import urlencode

import aiohttp

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.risk_management.margin_topup import VenueMarginHealthSignal
from hydra_basis.risk_management.watchers import (
    VenueRiskSignal,
    parse_aster_risk_signal,
    parse_hyperliquid_risk_signal,
)


HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"


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
        poll_seconds: float = 2.0,
    ) -> None:
        self.adapter = adapter or AsterExecutionAdapter()
        self.poll_seconds = poll_seconds
        self._seen_order_ids: set[str] = set()

    async def _fetch_force_orders(self, auto_close_type: str) -> list[dict]:
        params = self.adapter.build_signed_params({
            "autoCloseType": auto_close_type,
        })
        query = urlencode(params)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.adapter.BASE_URL}/fapi/v3/forceOrders?{query}",
                headers={"User-Agent": "funding-arb-risk/0.1"},
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster forceOrders {resp.status}: {data}")
                if not isinstance(data, list):
                    raise RuntimeError(f"unexpected aster forceOrders payload: {data}")
                return data

    async def watch(self) -> AsyncIterator[VenueRiskSignal]:
        while True:
            for auto_close_type in ("LIQUIDATION", "ADL"):
                for item in await self._fetch_force_orders(auto_close_type):
                    order_id = str(item.get("orderId", ""))
                    if not order_id or order_id in self._seen_order_ids:
                        continue
                    self._seen_order_ids.add(order_id)
                    payload = {
                        "e": "ORDER_TRADE_UPDATE",
                        "o": {
                            "s": item.get("symbol"),
                            "c": "adl_autoclose" if auto_close_type == "ADL" else "autoclose-",
                            "X": item.get("status", "FILLED"),
                        },
                    }
                    signal = parse_aster_risk_signal(payload)
                    if signal is not None:
                        yield signal
            await asyncio.sleep(self.poll_seconds)


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
        poll_seconds: float = 5.0,
    ) -> None:
        self.adapter = adapter or AsterExecutionAdapter()
        self.poll_seconds = poll_seconds

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

    async def watch(self):
        while True:
            for item in await self._fetch_positions():
                side = _aster_position_side(item)
                if side is None:
                    continue
                liq = float(item.get("liquidationPrice", "0") or 0)
                mark = float(item.get("markPrice", "0") or 0)
                if liq <= 0 or mark <= 0:
                    continue
                yield VenueMarginHealthSignal(
                    venue="aster",
                    symbol=_strip_stable_suffix(str(item.get("symbol", ""))),
                    side=side,  # type: ignore[arg-type]
                    mark_price=mark,
                    liquidation_price=liq,
                )
            await asyncio.sleep(self.poll_seconds)


class HyperliquidMarginHealthPoller:
    def __init__(
        self,
        *,
        user: str | None = None,
        poll_seconds: float = 5.0,
    ) -> None:
        self.user = user or os.getenv("HYPERLIQUID_ACCOUNT_ADDRESS", "")
        self.poll_seconds = poll_seconds
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

    async def watch(self):
        while True:
            state = await self._fetch_state()
            mids = await self._fetch_mids()
            for item in state.get("assetPositions", []):
                position = item.get("position", {})
                symbol = str(position.get("coin", "")).upper()
                size = float(position.get("szi", "0") or 0)
                liq_raw = position.get("liquidationPx")
                if not symbol or not liq_raw or size == 0:
                    continue
                mark = float(position.get("markPx") or mids.get(symbol) or 0)
                liq = float(liq_raw)
                if mark <= 0 or liq <= 0:
                    continue
                yield VenueMarginHealthSignal(
                    venue="hyperliquid",
                    symbol=symbol,
                    side="LONG" if size > 0 else "SHORT",
                    mark_price=mark,
                    liquidation_price=liq,
                )
            await asyncio.sleep(self.poll_seconds)
