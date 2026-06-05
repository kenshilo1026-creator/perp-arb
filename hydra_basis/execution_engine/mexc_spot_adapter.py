from __future__ import annotations

import hashlib
import hmac
import os
import time
from urllib.parse import urlencode

import aiohttp


class MexcSpotExecutionAdapter:
    BASE_URL = "https://api.mexc.com"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        self.api_key = (
            api_key
            if api_key is not None
            else os.getenv("MEXC_SPOT_API_KEY", "") or os.getenv("MEXC_API_KEY", "")
        )
        self.api_secret = (
            api_secret
            if api_secret is not None
            else os.getenv("MEXC_SPOT_API_SECRET", "") or os.getenv("MEXC_API_SECRET", "")
        )

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000)

    def _sign(self, query_string: str) -> str:
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _ensure_credentials(self) -> None:
        if self.api_key and self.api_secret:
            return
        raise RuntimeError(
            "MEXC spot API credentials missing: set MEXC_SPOT_API_KEY/"
            "MEXC_SPOT_API_SECRET or MEXC_API_KEY/MEXC_API_SECRET in .env"
        )

    def _signed_order_params(self, params: dict) -> dict:
        self._ensure_credentials()
        signed_params = dict(params)
        signed_params["timestamp"] = self._timestamp_ms()
        query = urlencode(signed_params)
        signature = self._sign(query)
        signed_params["signature"] = signature
        return signed_params

    def _order_headers(self) -> dict[str, str]:
        self._ensure_credentials()
        return {
            "X-MEXC-APIKEY": self.api_key,
            "Content-Type": "application/json",
        }

    async def _post_order(self, params: dict) -> dict:
        signed_params = self._signed_order_params(params)
        headers = {
            **self._order_headers(),
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.BASE_URL}/api/v3/order",
                params=signed_params,
                headers=headers,
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"mexc spot order {resp.status}: {data}")
                return data

    def _spot_symbol(self, symbol: str) -> str:
        normalized = symbol.strip().upper()
        if normalized.endswith("USDT"):
            return normalized
        return f"{normalized}USDT"

    async def place_market_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float
    ) -> dict:
        data = await self._post_order(
            {
                "symbol": self._spot_symbol(symbol),
                "side": side.strip().upper(),
                "type": "MARKET",
                "quantity": amount,
            }
        )
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str
    ) -> dict:
        data = await self._post_order(
            {
                "symbol": self._spot_symbol(symbol),
                "side": side.strip().upper(),
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": amount,
                "price": price,
            }
        )
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}

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
        if market_type != "spot":
            raise RuntimeError("mexc spot adapter only supports spot emergency close")
        if side.strip().upper() != "SELL":
            raise RuntimeError("mexc spot emergency close only supports SELL for long spot positions")
        data = await self._post_order(
            {
                "symbol": self._spot_symbol(symbol),
                "side": "SELL",
                "type": "MARKET",
                "quantity": quantity,
            }
        )
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}
