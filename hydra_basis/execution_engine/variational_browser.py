from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

from aiohttp import ClientSession, WSMsgType

from hydra_basis.execution_engine.order_fill import status_dict_looks_filled


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
    return payload


class VariationalBrowserExecutionAdapter:
    def __init__(
        self,
        *,
        broker_url: str = "http://127.0.0.1:8768/",
        client_role: str = "strategy",
        timeout_seconds: float = 10.0,
        debug_payload_path: Path | None = Path("data/variational_order_debug.json"),
    ) -> None:
        self.broker_url = broker_url
        self.client_role = client_role
        self.timeout_seconds = timeout_seconds
        self.debug_payload_path = debug_payload_path

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
    ) -> dict[str, object]:
        request_id = str(uuid.uuid4())
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
                    )
                )
                return await self._await_order_result(ws, request_id=request_id, symbol=symbol)

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
        )

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
        return await self._place_order(
            symbol=symbol, side=side, amount=amount, clip_usd=clip_usd,
            market=market, account=account, order_type="market",
            timeout_ms=timeout_ms,
        )

    async def wait_for_order_fill(
        self,
        *,
        order_result: dict[str, object],
        symbol: str,
        side: str,
        amount: str,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.5,
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

    async def _await_order_result(self, ws, *, request_id: str, symbol: str) -> dict[str, object]:
        order_dispatched = False
        while True:
            if order_dispatched:
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
                continue
            if payload.get("type") != "ORDER_RESULT":
                continue
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
                raise RuntimeError(
                    f"variational browser order failed for {symbol}: "
                    f"{payload.get('error', 'unknown error')}{debug_path_text}{detail_text}"
                )
            return payload
