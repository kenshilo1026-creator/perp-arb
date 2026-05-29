from __future__ import annotations

import asyncio
import uuid

from aiohttp import ClientSession, WSMsgType


def build_place_order_payload(
    *,
    request_id: str,
    side: str,
    amount: str,
    market: str | None = None,
    account: str | None = None,
    timeout_ms: int | None = None,
) -> dict[str, str | int]:
    payload: dict[str, str | int] = {
        "type": "PLACE_ORDER",
        "requestId": request_id,
        "side": side,
        "amount": amount,
    }
    if market is not None:
        payload["market"] = market
    if account is not None:
        payload["account"] = account
    if timeout_ms is not None:
        payload["timeoutMs"] = timeout_ms
    return payload


class VariationalBrowserExecutionAdapter:
    def __init__(
        self,
        *,
        broker_url: str = "http://127.0.0.1:8768/",
        client_role: str = "strategy",
        timeout_seconds: float = 10.0,
    ) -> None:
        self.broker_url = broker_url
        self.client_role = client_role
        self.timeout_seconds = timeout_seconds

    async def place_limit_order(
        self,
        *,
        symbol: str,
        side: str,
        amount: str,
        market: str | None = None,
        account: str | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, object]:
        request_id = str(uuid.uuid4())
        async with ClientSession() as session:
            async with session.ws_connect(self.broker_url, heartbeat=20) as ws:
                await ws.send_json({"type": "REGISTER", "role": self.client_role})
                await self._await_register_ack(ws)
                await ws.send_json(
                    build_place_order_payload(
                        request_id=request_id,
                        side=side,
                        amount=amount,
                        market=market,
                        account=account,
                        timeout_ms=timeout_ms,
                    )
                )
                return await self._await_order_result(ws, request_id=request_id, symbol=symbol)

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

    async def _await_order_result(self, ws, *, request_id: str, symbol: str) -> dict[str, object]:
        while True:
            msg = await asyncio.wait_for(ws.receive(), timeout=self.timeout_seconds)
            if msg.type != WSMsgType.TEXT:
                continue
            payload = msg.json()
            if payload.get("requestId") != request_id:
                continue
            if payload.get("type") == "ORDER_DISPATCHED":
                continue
            if payload.get("type") != "ORDER_RESULT":
                continue
            if not payload.get("ok", False):
                raise RuntimeError(
                    f"variational browser order failed for {symbol}: {payload.get('error', 'unknown error')}"
                )
            return payload
