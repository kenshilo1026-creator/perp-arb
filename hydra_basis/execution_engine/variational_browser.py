from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Awaitable, Callable

from aiohttp import ClientSession, WSMsgType

from hydra_basis.execution_engine.order_fill import status_dict_looks_filled
from hydra_basis.symbol_mapping import canonicalize_symbol


def build_place_order_payload(
    *,
    request_id: str,
    symbol: str,
    side: str,
    amount: str,
    order_type: str = "market",
    price: str | None = None,
    market: str | None = None,
    account: str | None = None,
    timeout_ms: int | None = None,
    submit_only: bool = False,
) -> dict[str, str | int]:
    payload: dict[str, str | int] = {
        "type": "PLACE_ORDER",
        "requestId": request_id,
        "symbol": symbol,
        "side": side,
        "amount": amount,
        "orderType": order_type.upper(),
    }
    if price is not None:
        payload["price"] = price
    if market is not None:
        payload["market"] = market
    if account is not None:
        payload["account"] = account
    if timeout_ms is not None:
        payload["timeoutMs"] = timeout_ms
    if submit_only:
        payload["submitOnly"] = True
    return payload


class VariationalBrowserExecutionAdapter:
    _last_order_ts_by_broker: dict[str, float] = {}

    def __init__(
        self,
        *,
        broker_url: str = "http://127.0.0.1:8768/",
        client_role: str = "strategy",
        timeout_seconds: float = 10.0,
        fill_timeout_seconds: float | None = None,
        debug_payload_path: Path | None = Path("data/variational_order_debug.json"),
        min_seconds_between_orders: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.broker_url = broker_url
        self.client_role = client_role
        self.timeout_seconds = timeout_seconds
        self.fill_timeout_seconds = fill_timeout_seconds
        self.debug_payload_path = debug_payload_path
        self.min_seconds_between_orders = min_seconds_between_orders
        self._clock = clock
        self._sleep = sleep
        self._market_prepared = False

    @staticmethod
    def _extract_order_id(order_result: dict[str, object]) -> object | None:
        order_id = order_result.get("orderId") or order_result.get("order_id")
        if order_id not in (None, ""):
            return order_id
        raw = order_result.get("raw")
        if isinstance(raw, dict):
            raw_order_id = raw.get("orderId") or raw.get("order_id")
            if raw_order_id not in (None, ""):
                return raw_order_id
        details = order_result.get("details")
        if isinstance(details, dict):
            submitted = details.get("submitted")
            if isinstance(submitted, dict):
                submitted_order_id = submitted.get("orderId") or submitted.get("order_id")
                if submitted_order_id not in (None, ""):
                    return submitted_order_id
        return None

    @staticmethod
    def _order_error(message: str, *, order_result: dict[str, object] | None = None) -> RuntimeError:
        error = RuntimeError(message)
        if order_result is not None:
            setattr(error, "order_result", order_result)
        return error

    def _map_symbol(self, symbol: str) -> str:
        return canonicalize_symbol(symbol, venue="variational")

    async def _wait_for_order_cooldown(self) -> None:
        if self.min_seconds_between_orders <= 0:
            return
        last_order_ts = self._last_order_ts_by_broker.get(self.broker_url)
        now = self._clock()
        if last_order_ts is not None:
            wait_seconds = self.min_seconds_between_orders - (now - last_order_ts)
            if wait_seconds > 0:
                print(f"[variational] order cooldown sleeping {wait_seconds:.2f}s", flush=True)
                await self._sleep(wait_seconds)
        self._last_order_ts_by_broker[self.broker_url] = self._clock()

    async def _place_order(
        self,
        *,
        symbol: str,
        side: str,
        amount: str,
        clip_usd: float | None = None,
        market: str | None = None,
        account: str | None = None,
        order_type: str = "market",
        price: str | None = None,
        timeout_ms: int | None = None,
        fill_timeout_seconds: float | None = None,
        submit_only: bool = False,
    ) -> dict[str, object]:
        symbol = self._map_symbol(symbol)
        request_id = str(uuid.uuid4())
        await self._wait_for_order_cooldown()
        async with ClientSession() as session:
            async with session.ws_connect(self.broker_url, heartbeat=20) as ws:
                await ws.send_json({"type": "REGISTER", "role": self.client_role})
                await self._await_register_ack(ws)
                await ws.send_json(
                    build_place_order_payload(
                        request_id=request_id,
                        symbol=symbol,
                        side=side,
                        amount=amount,
                        order_type=order_type,
                        price=price,
                        market=market,
                        account=account,
                        timeout_ms=timeout_ms,
                        submit_only=submit_only,
                    )
                )
                return await self._await_order_result(
                    ws,
                    request_id=request_id,
                    symbol=symbol,
                    fill_timeout_seconds=fill_timeout_seconds,
                )

    async def place_limit_order(
        self,
        *,
        symbol: str,
        side: str,
        amount: str,
        clip_usd: float | None = None,
        market: str | None = None,
        account: str | None = None,
        price: str | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, object]:
        return await self._place_order(
            symbol=symbol, side=side, amount=amount, clip_usd=clip_usd,
            market=market, account=account, order_type="limit", price=price,
            timeout_ms=timeout_ms,
            fill_timeout_seconds=self.fill_timeout_seconds,
        )

    async def prepare_market_order(
        self,
        *,
        symbol: str,
        side: str,
        amount: str,
        clip_usd: float | None = None,
    ) -> dict[str, object]:
        symbol = self._map_symbol(symbol)
        request_id = str(uuid.uuid4())
        async with ClientSession() as session:
            async with session.ws_connect(self.broker_url, heartbeat=20) as ws:
                await ws.send_json({"type": "REGISTER", "role": self.client_role})
                await self._await_register_ack(ws)
                await ws.send_json({
                    "type": "PREPARE_MARKET_ORDER",
                    "requestId": request_id,
                    "symbol": symbol,
                    "side": side,
                    "amount": amount,
                })
                msg = await asyncio.wait_for(ws.receive(), timeout=self.timeout_seconds)
        if msg.type != WSMsgType.TEXT:
            raise RuntimeError(f"variational prepare_market_order unexpected message type: {msg.type}")
        result = msg.json()
        if not result.get("ok"):
            raise RuntimeError(f"variational prepare_market_order failed for {symbol}: {result.get('error')}")
        self._market_prepared = True
        return {"ok": True, "raw": result}

    async def place_market_order(
        self,
        *,
        symbol: str,
        side: str,
        amount: str,
        clip_usd: float | None = None,
        market: str | None = None,
        account: str | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, object]:
        submit_only = self._market_prepared
        self._market_prepared = False
        return await self._place_order(
            symbol=symbol, side=side, amount=amount, clip_usd=clip_usd,
            market=market, account=account, order_type="market",
            timeout_ms=timeout_ms,
            submit_only=submit_only,
        )

    async def cancel_order(
        self,
        *,
        order_result: dict[str, object],
        symbol: str,
        side: str,
        amount: str,
    ) -> dict[str, object]:
        symbol = self._map_symbol(symbol)
        request_id = str(uuid.uuid4())
        order_id = self._extract_order_id(order_result)
        if order_id in (None, ""):
            print(
                f"[variational] cancel_order falling back to symbol+side match symbol={symbol} side={side}",
                flush=True,
            )
        else:
            print(f"[variational] cancel_order symbol={symbol} side={side} orderId={order_id}", flush=True)
        async with ClientSession() as session:
            async with session.ws_connect(self.broker_url, heartbeat=20) as ws:
                await ws.send_json({"type": "REGISTER", "role": self.client_role})
                await self._await_register_ack(ws)
                await ws.send_json({
                    "type": "CANCEL_ORDER",
                    "requestId": request_id,
                    "orderId": order_id,
                    "symbol": symbol,
                    "side": side,
                    "amount": amount,
                })
                msg = await asyncio.wait_for(ws.receive(), timeout=self.timeout_seconds)
        if msg.type != WSMsgType.TEXT:
            raise RuntimeError(f"variational cancel_order unexpected message type: {msg.type}")
        result = msg.json()
        if not result.get("ok"):
            detail_text = ""
            details = result.get("details")
            if details:
                detail_text = f" details={json.dumps(details, ensure_ascii=False)[:4000]}"
            raise RuntimeError(
                f"variational cancel_order failed for {symbol}: {result.get('error')}{detail_text}"
            )
        return {"ok": True, "raw": result}

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict | None:
        symbol = self._map_symbol(symbol)
        request_id = str(uuid.uuid4())
        async with ClientSession() as session:
            async with session.ws_connect(self.broker_url, heartbeat=20) as ws:
                await ws.send_json({"type": "REGISTER", "role": self.client_role})
                await self._await_register_ack(ws)
                await ws.send_json({
                    "type": "GET_OPEN_POSITION",
                    "requestId": request_id,
                    "symbol": symbol,
                    "marketType": market_type,
                })
                msg = await asyncio.wait_for(ws.receive(), timeout=self.timeout_seconds)
        if msg.type != WSMsgType.TEXT:
            raise RuntimeError(f"variational get_open_position unexpected message type: {msg.type}")
        result = msg.json()
        if not result.get("ok"):
            raise RuntimeError(f"variational get_open_position failed for {symbol}: {result.get('error')}")
        return result.get("position")

    async def wait_for_order_fill(
        self,
        *,
        order_result: dict[str, object],
        symbol: str,
        side: str,
        amount: str,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.5,
        allow_partial_fill: bool = False,
    ) -> dict[str, object]:
        if bool(order_result.get("filled")) or status_dict_looks_filled(order_result):
            return {"ok": True, "raw": order_result}
        details = order_result.get("details")
        if isinstance(details, dict) and status_dict_looks_filled(details):
            return {"ok": True, "raw": order_result}
        raise RuntimeError(
            "variational limit order fill confirmation unavailable: "
            "browser ORDER_RESULT must include filled=true or status=FILLED"
        )

    async def _await_register_ack(self, ws) -> None:
        while True:
            msg = await asyncio.wait_for(ws.receive(), timeout=self.timeout_seconds)
            if msg.type != WSMsgType.TEXT:
                continue
            payload = msg.json()
            if payload.get("type") != "REGISTER_ACK":
                continue
            if not payload.get("ok", False):
                raise RuntimeError("variational browser register failed")
            return

    async def _await_order_result(
        self,
        ws,
        *,
        request_id: str,
        symbol: str,
        fill_timeout_seconds: float | None = None,
    ) -> dict[str, object]:
        order_dispatched = False
        import time as _time
        fill_deadline: float | None = None
        submitted_order_result: dict[str, object] | None = None
        while True:
            if order_dispatched:
                if fill_deadline is not None:
                    remaining = fill_deadline - _time.monotonic()
                    if remaining <= 0:
                        raise self._order_error(
                            f"variational limit order fill timeout after {fill_timeout_seconds:.0f}s",
                            order_result=submitted_order_result,
                        )
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=remaining)
                    except (TimeoutError, asyncio.TimeoutError):
                        raise self._order_error(
                            f"variational limit order fill timeout after {fill_timeout_seconds:.0f}s",
                            order_result=submitted_order_result,
                        )
                else:
                    msg = await ws.receive()
            else:
                msg = await asyncio.wait_for(ws.receive(), timeout=self.timeout_seconds)
            if msg.type != WSMsgType.TEXT:
                continue
            payload = msg.json()
            if payload.get("requestId") != request_id:
                continue
            if payload.get("type") == "ORDER_DISPATCHED":
                order_dispatched = True
                if fill_timeout_seconds is not None:
                    fill_deadline = _time.monotonic() + fill_timeout_seconds
                continue
            if payload.get("type") == "ORDER_ACCEPTED":
                order_dispatched = True
                submitted_order_result = payload
                if fill_timeout_seconds is not None and fill_deadline is None:
                    fill_deadline = _time.monotonic() + fill_timeout_seconds
                continue
            if payload.get("type") != "ORDER_RESULT":
                continue
            submitted_order_result = payload if payload.get("ok", False) else submitted_order_result
            if not payload.get("ok", False):
                debug_path_text = ""
                if self.debug_payload_path is not None:
                    self.debug_payload_path.parent.mkdir(parents=True, exist_ok=True)
                    self.debug_payload_path.write_text(
                        json.dumps(payload, indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    debug_path_text = f" debug_payload={self.debug_payload_path}"
                details = payload.get("details")
                detail_text = ""
                if details:
                    detail_text = f" details={json.dumps(details, ensure_ascii=False)[:2000]}"
                raise self._order_error(
                    f"variational browser order failed for {symbol}: "
                    f"{payload.get('error', 'unknown error')}{debug_path_text}{detail_text}",
                    order_result=payload,
                )
            return payload
