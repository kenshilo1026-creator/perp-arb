from __future__ import annotations

import hashlib
import hmac
import os
import time

import aiohttp

from hydra_basis.adapters.aster import fetch_aster_symbol_metadata


class AsterExecutionAdapter:
    BASE_URL = "https://fapi.asterdex.com"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        slippage_bps: float = 20.0,
    ) -> None:
        self.api_key = api_key or os.getenv("ASTER_API_KEY", "")
        self.api_secret = api_secret or os.getenv("ASTER_API_SECRET", "")
        self.slippage_bps = slippage_bps
        self._symbol_metadata: dict | None = None

    def _sign(self, query_string: str) -> str:
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000)

    async def _resolve_raw_symbol(self, symbol: str) -> str:
        if self._symbol_metadata is None:
            async with aiohttp.ClientSession() as session:
                self._symbol_metadata = await fetch_aster_symbol_metadata(session)
        metadata = self._symbol_metadata.get(symbol.upper())
        if metadata is None:
            raise RuntimeError(f"aster symbol not found: {symbol}")
        return metadata["raw_symbol"]

    async def _post_order(self, params: dict) -> dict:
        params["timestamp"] = self._timestamp_ms()
        query = "&".join(f"{k}={v}" for k, v in params.items())
        sig = self._sign(query)
        headers = {
            "X-MBX-APIKEY": self.api_key,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.BASE_URL}/fapi/v1/order",
                data=f"{query}&signature={sig}",
                headers=headers,
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster order {resp.status}: {data}")
                return data

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str
    ) -> dict:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        data = await self._post_order({
            "symbol": raw_symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": amount,
            "price": price,
        })
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}

    async def place_market_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float
    ) -> dict:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        data = await self._post_order({
            "symbol": raw_symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": amount,
        })
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}
