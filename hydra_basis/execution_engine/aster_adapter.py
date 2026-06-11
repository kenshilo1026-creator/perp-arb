from __future__ import annotations

import threading
import os
import time
from decimal import Decimal, ROUND_FLOOR
from urllib.parse import urlencode

import aiohttp
from eth_account import Account
try:
    from eth_account.messages import encode_structured_data
except ImportError:  # eth-account >= 0.12
    encode_structured_data = None
from eth_account.messages import encode_typed_data

from hydra_basis.execution_engine.order_fill import poll_until_filled

ASTER_EXECUTION_SUFFIXES = ("USDT", "USDC", "USD")


ASTER_V3_TYPED_DATA = {
    "types": {
        "EIP712Domain": [
            {"name": "name", "type": "string"},
            {"name": "version", "type": "string"},
            {"name": "chainId", "type": "uint256"},
            {"name": "verifyingContract", "type": "address"},
        ],
        "Message": [
            {"name": "msg", "type": "string"},
        ],
    },
    "primaryType": "Message",
    "domain": {
        "name": "AsterSignTransaction",
        "version": "1",
        "chainId": 1666,
        "verifyingContract": "0x0000000000000000000000000000000000000000",
    },
    "message": {"msg": ""},
}


def encode_aster_v3_typed_data(encoded_params: str):
    typed_data = dict(ASTER_V3_TYPED_DATA)
    typed_data["message"] = {"msg": encoded_params}
    if encode_structured_data is not None:
        return encode_structured_data(typed_data)
    return encode_typed_data(full_message=typed_data)


def format_aster_step_quantity(quantity: str, step_size: str) -> str:
    step = Decimal(str(step_size))
    value = Decimal(str(quantity))
    if step <= 0:
        return format(value.normalize(), "f")
    rounded = (value / step).to_integral_value(rounding=ROUND_FLOOR) * step
    if rounded <= 0:
        raise RuntimeError(f"aster quantity {quantity} is below minimum stepSize={step_size}")
    return format(rounded.normalize(), "f")


def strip_aster_stable_suffix(symbol: str) -> str:
    normalized = str(symbol).strip().upper()
    for suffix in ASTER_EXECUTION_SUFFIXES:
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


class AsterExecutionAdapter:
    BASE_URL = "https://fapi.asterdex.com"

    def __init__(
        self,
        *,
        signer_address: str | None = None,
        private_key: str | None = None,
        user_address: str | None = None,
        leverage: int = 1,
        slippage_bps: float = 20.0,
        skip_margin_setup: bool = False,
    ) -> None:
        self.signer_address = (
            signer_address
            or os.getenv("ASTER_API_WALLET_ADDRESS", "")
            or os.getenv("ASTER_SIGNER_ADDRESS", "")
        )
        self.private_key = private_key or os.getenv("ASTER_API_WALLET_PRIVATE_KEY", "")
        self.user_address = user_address or os.getenv("ASTER_USER_ADDRESS", "")
        self.leverage = leverage
        self.slippage_bps = slippage_bps
        self.skip_margin_setup = skip_margin_setup
        self._symbol_metadata: dict | None = None
        self._exchange_info_by_symbol: dict[str, dict] | None = None
        self._last_nonce_ms = 0
        self._nonce_i = 0
        self._nonce_lock = threading.Lock()
        self._isolated_symbols: set[str] = set()
        self._leveraged_symbols: set[str] = set()

    def _sign(self, encoded_params: str) -> str:
        if not self.private_key:
            raise RuntimeError("ASTER_API_WALLET_PRIVATE_KEY is not set")
        message = encode_aster_v3_typed_data(encoded_params)
        signed = Account.sign_message(message, private_key=self.private_key)
        return signed.signature.hex()

    def _nonce_us(self) -> int:
        with self._nonce_lock:
            now_ms = int(time.time())
            if now_ms == self._last_nonce_ms:
                self._nonce_i += 1
            else:
                self._last_nonce_ms = now_ms
                self._nonce_i = 0
            return now_ms * 1_000_000 + self._nonce_i

    async def _resolve_raw_symbol(self, symbol: str) -> str:
        exchange_info = await self._load_exchange_info_by_symbol()
        normalized = symbol.strip().upper()
        if normalized in exchange_info:
            return normalized
        for suffix in ASTER_EXECUTION_SUFFIXES:
            raw_symbol = f"{normalized}{suffix}"
            metadata = exchange_info.get(raw_symbol)
            if metadata is not None and metadata.get("status") == "TRADING":
                return raw_symbol
        for suffix in ASTER_EXECUTION_SUFFIXES:
            raw_symbol = f"{normalized}{suffix}"
            if raw_symbol in exchange_info:
                return raw_symbol
        raise RuntimeError(f"aster symbol not found: {symbol}")

    async def warm_up(self) -> None:
        await self._load_exchange_info_by_symbol()

    async def _load_exchange_info_by_symbol(self) -> dict[str, dict]:
        if self._exchange_info_by_symbol is not None:
            return self._exchange_info_by_symbol
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.BASE_URL}/fapi/v3/exchangeInfo") as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster exchangeInfo {resp.status}: {data}")
        self._exchange_info_by_symbol = {
            str(item.get("symbol", "")).upper(): item
            for item in data.get("symbols", [])
            if item.get("symbol")
        }
        return self._exchange_info_by_symbol

    async def _quantity_step_size(self, symbol: str, *, market: bool) -> str:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        exchange_info = await self._load_exchange_info_by_symbol()
        raw = exchange_info.get(raw_symbol, {})
        preferred = "MARKET_LOT_SIZE" if market else "LOT_SIZE"
        fallback = "LOT_SIZE"
        for filter_type in (preferred, fallback):
            for item in raw.get("filters", []):
                if item.get("filterType") == filter_type and item.get("stepSize"):
                    return str(item["stepSize"])
        quantity_precision = raw.get("quantityPrecision")
        if quantity_precision is not None:
            return "1" if int(quantity_precision) == 0 else "0." + ("0" * (int(quantity_precision) - 1)) + "1"
        return "0.00000001"

    async def _format_quantity(self, symbol: str, quantity: str, *, market: bool) -> str:
        step_size = await self._quantity_step_size(symbol, market=market)
        return format_aster_step_quantity(quantity, step_size)

    async def _post_form(self, url: str, data: dict) -> dict:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "funding-arb-execution/0.1",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, headers=headers) as resp:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" not in content_type.lower():
                    text = await resp.text()
                    raise RuntimeError(
                        f"aster order {resp.status}: unexpected content-type={content_type!r} "
                        f"body={text[:300]!r}"
                    )
                result = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster order {resp.status}: {result}")
                return result

    async def _post_signed_query(self, url: str, params: dict) -> dict:
        query = urlencode(params)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "funding-arb-execution/0.1",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{url}?{query}", headers=headers) as resp:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" not in content_type.lower():
                    text = await resp.text()
                    raise RuntimeError(
                        f"aster order {resp.status}: unexpected content-type={content_type!r} "
                        f"body={text[:300]!r}"
                    )
                result = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster order {resp.status}: {result}")
                return result

    async def _get_signed_query(self, url: str, params: dict) -> dict:
        query = urlencode(params)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "funding-arb-execution/0.1",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{url}?{query}", headers=headers) as resp:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" not in content_type.lower():
                    text = await resp.text()
                    raise RuntimeError(
                        f"aster order {resp.status}: unexpected content-type={content_type!r} "
                        f"body={text[:300]!r}"
                    )
                result = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster order {resp.status}: {result}")
                return result

    async def _delete_signed_query(self, url: str, params: dict) -> dict:
        query = urlencode(params)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "funding-arb-execution/0.1",
        }
        async with aiohttp.ClientSession() as session:
            async with session.delete(f"{url}?{query}", headers=headers) as resp:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" not in content_type.lower():
                    text = await resp.text()
                    raise RuntimeError(
                        f"aster cancel {resp.status}: unexpected content-type={content_type!r} "
                        f"body={text[:300]!r}"
                    )
                result = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"aster cancel {resp.status}: {result}")
                return result

    def build_signed_params(self, params: dict) -> dict:
        if not self.signer_address:
            raise RuntimeError("ASTER_API_WALLET_ADDRESS is not set")
        if not self.user_address:
            raise RuntimeError("ASTER_USER_ADDRESS is not set")
        params = dict(params)
        params["nonce"] = str(self._nonce_us())
        params["signer"] = self.signer_address
        params["user"] = self.user_address
        encoded_params = urlencode(params)
        params["signature"] = self._sign(encoded_params)
        return params

    async def _post_order(self, params: dict) -> dict:
        params = self.build_signed_params(params)
        return await self._post_signed_query(f"{self.BASE_URL}/fapi/v3/order", params)

    async def _get_order_status(self, *, symbol: str, order_id: object) -> dict:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        params = self.build_signed_params({
            "symbol": raw_symbol,
            "orderId": str(order_id),
        })
        return await self._get_signed_query(f"{self.BASE_URL}/fapi/v3/order", params)

    async def _fetch_position_risk(self) -> list[dict]:
        params = self.build_signed_params({})
        data = await self._get_signed_query(f"{self.BASE_URL}/fapi/v3/positionRisk", params)
        if not isinstance(data, list):
            raise RuntimeError(f"unexpected aster positionRisk payload: {data}")
        return data

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict | None:
        if market_type != "perp":
            raise RuntimeError("aster live position query only supports perp")
        normalized_symbol = symbol.strip().upper()
        for item in await self._fetch_position_risk():
            if strip_aster_stable_suffix(str(item.get("symbol", ""))) != normalized_symbol:
                continue
            amount = Decimal(str(item.get("positionAmt", "0") or "0"))
            if amount == 0:
                continue
            side = "LONG" if amount > 0 else "SHORT"
            return {
                "symbol": normalized_symbol,
                "market_type": "perp",
                "side": side,
                "quantity": format(abs(amount).normalize(), "f"),
                "raw": item,
            }
        return None

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
            raise RuntimeError("aster limit order fill wait requires order_id")
        return await poll_until_filled(
            fetch_status=lambda: self._get_order_status(symbol=symbol, order_id=order_id),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            timeout_message="aster limit order fill timeout",
        )

    async def cancel_order(
        self,
        *,
        order_result: dict,
        symbol: str,
        side: str,
        amount: str,
    ) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if order_id is None:
            raise RuntimeError("aster cancel_order requires order_id")
        raw_symbol = await self._resolve_raw_symbol(symbol)
        params = self.build_signed_params({
            "symbol": raw_symbol,
            "orderId": str(order_id),
        })
        data = await self._delete_signed_query(f"{self.BASE_URL}/fapi/v3/order", params)
        return {"ok": True, "raw": data}

    async def ensure_isolated_margin(self, symbol: str) -> None:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        if self.skip_margin_setup or raw_symbol in self._isolated_symbols:
            return
        params = self.build_signed_params({
            "symbol": raw_symbol,
            "marginType": "ISOLATED",
        })
        try:
            await self._post_signed_query(f"{self.BASE_URL}/fapi/v3/marginType", params)
        except RuntimeError as exc:
            message = str(exc)
            if "-4046" not in message and "No need to change margin type" not in message:
                raise
        self._isolated_symbols.add(raw_symbol)

    async def ensure_leverage(self, symbol: str) -> None:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        if self.skip_margin_setup or raw_symbol in self._leveraged_symbols:
            return
        params = self.build_signed_params({
            "symbol": raw_symbol,
            "leverage": self.leverage,
        })
        await self._post_signed_query(f"{self.BASE_URL}/fapi/v3/leverage", params)
        self._leveraged_symbols.add(raw_symbol)

    async def add_isolated_margin(
        self,
        *,
        venue: str,
        symbol: str,
        side: str,
        amount_usd: float,
        **kwargs,
    ) -> dict:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        params = self.build_signed_params({
            "symbol": raw_symbol,
            "positionSide": "BOTH",
            "amount": str(amount_usd),
            "type": 1,
        })
        data = await self._post_signed_query(f"{self.BASE_URL}/fapi/v3/positionMargin", params)
        return {"ok": True, "raw": data}

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str
    ) -> dict:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        quantity = await self._format_quantity(symbol, amount, market=False)
        await self.ensure_isolated_margin(symbol)
        await self.ensure_leverage(symbol)
        data = await self._post_order({
            "symbol": raw_symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": quantity,
            "price": price,
        })
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}

    async def place_market_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float
    ) -> dict:
        raw_symbol = await self._resolve_raw_symbol(symbol)
        quantity = await self._format_quantity(symbol, amount, market=True)
        await self.ensure_isolated_margin(symbol)
        await self.ensure_leverage(symbol)
        data = await self._post_order({
            "symbol": raw_symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": quantity,
        })
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
        if market_type != "perp":
            raise RuntimeError("aster spot emergency close is not supported")
        raw_symbol = await self._resolve_raw_symbol(symbol)
        formatted_quantity = await self._format_quantity(symbol, quantity, market=True)
        data = await self._post_order({
            "symbol": raw_symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": formatted_quantity,
            "reduceOnly": "true",
        })
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}
