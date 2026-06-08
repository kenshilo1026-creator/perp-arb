from __future__ import annotations

import hashlib
import hmac
import json
import os
import time

import aiohttp

from hydra_basis.adapters.mexc import mexc_contract_symbol
from hydra_basis.execution_engine.order_fill import poll_until_filled


# MEXC futures order sides
_SIDE_OPEN_LONG = 1   # BUY
_SIDE_OPEN_SHORT = 3  # SELL
_SIDE_CLOSE_SHORT = 2  # BUY to close short
_SIDE_CLOSE_LONG = 4   # SELL to close long

# MEXC futures order types
_TYPE_LIMIT = 1
_TYPE_MARKET = 5


def mexc_close_side(side: str) -> int:
    normalized = side.strip().upper()
    if normalized == "BUY":
        return _SIDE_CLOSE_SHORT
    if normalized == "SELL":
        return _SIDE_CLOSE_LONG
    raise RuntimeError(f"unsupported mexc close side: {side}")


class MexcExecutionAdapter:
    BASE_URL = "https://contract.mexc.com"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        leverage: int = 1,
        open_type: int = 1,  # 1=isolated, 2=cross
        slippage_bps: float = 20.0,
    ) -> None:
        self.api_key = api_key or os.getenv("MEXC_API_KEY", "")
        self.api_secret = api_secret or os.getenv("MEXC_API_SECRET", "")
        self.leverage = leverage
        self.open_type = open_type
        self.slippage_bps = slippage_bps

    def _timestamp_ms(self) -> str:
        return str(int(time.time() * 1000))

    def _sign(self, api_key: str, timestamp: str, body: str) -> str:
        message = api_key + timestamp + body
        return hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _signed_headers(self, body: str) -> dict[str, str]:
        timestamp = self._timestamp_ms()
        return {
            "ApiKey": self.api_key,
            "Request-Time": timestamp,
            "Signature": self._sign(self.api_key, timestamp, body),
            "Content-Type": "application/json",
        }

    def _side(self, side: str) -> int:
        return _SIDE_OPEN_LONG if side.strip().upper() == "BUY" else _SIDE_OPEN_SHORT

    async def _post_order(self, body: dict) -> dict:
        body_str = json.dumps(body, separators=(",", ":"))
        headers = self._signed_headers(body_str)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.BASE_URL}/api/v1/private/order/submit",
                data=body_str,
                headers=headers,
            ) as resp:
                data = await resp.json()
                if resp.status != 200 or not data.get("success"):
                    raise RuntimeError(f"mexc order {resp.status}: {data}")
                return data

    async def _get_order_status(self, order_id: object) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.BASE_URL}/api/v1/private/order/get/{order_id}",
                headers=self._signed_headers(""),
            ) as resp:
                data = await resp.json()
                if resp.status != 200 or not data.get("success"):
                    raise RuntimeError(f"mexc order status {resp.status}: {data}")
                return data.get("data", data)

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str
    ) -> dict:
        contract_sym = mexc_contract_symbol(symbol)
        data = await self._post_order({
            "symbol": contract_sym,
            "price": float(price),
            "vol": float(amount),
            "leverage": self.leverage,
            "side": self._side(side),
            "type": _TYPE_LIMIT,
            "openType": self.open_type,
            "positionId": 0,
            "externalOid": "",
        })
        return {"ok": True, "order_id": data.get("data"), "raw": data}

    async def wait_for_order_fill(
        self,
        *,
        order_result: dict,
        symbol: str,
        side: str,
        amount: str,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.5,
    ) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if order_id is None:
            raise RuntimeError("mexc limit order fill wait requires order_id")
        return await poll_until_filled(
            fetch_status=lambda: self._get_order_status(order_id),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            timeout_message="mexc limit order fill timeout",
        )

    async def place_market_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float
    ) -> dict:
        contract_sym = mexc_contract_symbol(symbol)
        data = await self._post_order({
            "symbol": contract_sym,
            "price": 0,
            "vol": float(amount),
            "leverage": self.leverage,
            "side": self._side(side),
            "type": _TYPE_MARKET,
            "openType": self.open_type,
            "positionId": 0,
            "externalOid": "",
        })
        return {"ok": True, "order_id": data.get("data"), "raw": data}

    async def close_position(
        self,
        *,
        venue: str,
        symbol: str,
        side: str,
        quantity: str,
        market_type: str,
        **kwargs,
    ) -> dict:
        if market_type != "perp":
            raise RuntimeError("mexc spot emergency close is not implemented")
        contract_sym = mexc_contract_symbol(symbol)
        data = await self._post_order({
            "symbol": contract_sym,
            "price": 0,
            "vol": float(quantity),
            "leverage": self.leverage,
            "side": mexc_close_side(side),
            "type": _TYPE_MARKET,
            "openType": self.open_type,
            "positionId": 0,
            "externalOid": "",
        })
        return {"ok": True, "order_id": data.get("data"), "raw": data}
