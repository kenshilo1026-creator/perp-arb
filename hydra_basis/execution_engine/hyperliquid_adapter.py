from __future__ import annotations

import os
import time
from decimal import Decimal

import aiohttp
import msgpack
from eth_account import Account
try:
    from eth_account.messages import encode_structured_data
except ImportError:  # eth-account >= 0.12
    encode_structured_data = None
from eth_account.messages import encode_typed_data
from eth_hash.auto import keccak

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_universe
from hydra_basis.execution_engine.order_fill import poll_until_filled


HYPERLIQUID_EXCHANGE_URL = "https://api.hyperliquid.xyz/exchange"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"


def _action_hash(action: dict, vault_address: str | None, nonce: int) -> bytes:
    data = msgpack.packb(action, use_bin_type=True)
    data += nonce.to_bytes(8, "big")
    if vault_address is None:
        data += b"\x00"
    else:
        data += b"\x01"
        data += bytes.fromhex(vault_address[2:])
    return keccak(data)


def encode_hyperliquid_typed_data(structured: dict):
    if encode_structured_data is not None:
        return encode_structured_data(structured)
    return encode_typed_data(full_message=structured)


def _sign_l1_action(
    private_key: str,
    action: dict,
    vault_address: str | None,
    nonce: int,
    is_mainnet: bool = True,
) -> dict:
    connection_id = _action_hash(action, vault_address, nonce)
    phantom_agent = {
        "source": "a" if is_mainnet else "b",
        "connectionId": connection_id,
    }
    structured = {
        "domain": {
            "chainId": 1337,
            "name": "Exchange",
            "verifyingContract": "0x0000000000000000000000000000000000000000",
            "version": "1",
        },
        "types": {
            "Agent": [
                {"name": "source", "type": "string"},
                {"name": "connectionId", "type": "bytes32"},
            ],
        },
        "primaryType": "Agent",
        "message": phantom_agent,
    }
    wallet = Account.from_key(private_key)
    signed = wallet.sign_message(encode_hyperliquid_typed_data(structured))
    return {"r": hex(signed.r), "s": hex(signed.s), "v": signed.v}


def hyperliquid_float_to_wire(x: float) -> str:
    return f"{x:.5g}"


def extract_hyperliquid_order_id(data: dict, *, fill_type: str) -> int | None:
    statuses = data.get("response", {}).get("data", {}).get("statuses", [])
    if not statuses:
        return None
    status = statuses[0]
    if "error" in status:
        raise RuntimeError(f"hyperliquid order error: {status['error']}")
    return (status.get(fill_type) or {}).get("oid")


class HyperliquidExecutionAdapter:
    def __init__(
        self,
        *,
        private_key: str | None = None,
        account_address: str | None = None,
        leverage: int | None = None,
        slippage_bps: float = 50.0,
        skip_margin_setup: bool = False,
    ) -> None:
        self.private_key = private_key or os.getenv("HYPERLIQUID_PRIVATE_KEY", "")
        if not self.private_key:
            raise RuntimeError("HYPERLIQUID_PRIVATE_KEY is not set")
        self._wallet = Account.from_key(self.private_key)
        self.account_address = (
            account_address
            or os.getenv("HYPERLIQUID_ACCOUNT_ADDRESS", "")
            or self._wallet.address
        )
        self.slippage_bps = slippage_bps
        self.default_leverage = leverage if leverage is not None else int(os.getenv("HYPERLIQUID_LEVERAGE", "1"))
        self.skip_margin_setup = skip_margin_setup
        self._universe: list[str] | None = None
        self._isolated_asset_indices: set[int] = set()

    async def _get_asset_index(self, symbol: str) -> int:
        if self._universe is None:
            async with aiohttp.ClientSession() as session:
                self._universe = await fetch_hyperliquid_universe(session)
        sym = symbol.upper()
        try:
            return self._universe.index(sym)
        except ValueError:
            raise RuntimeError(f"hyperliquid symbol not found: {symbol}")

    async def _get_mid_price(self, symbol: str) -> float:
        async with aiohttp.ClientSession() as session:
            data = await fetch_json(session, "POST", HYPERLIQUID_INFO_URL, json={"type": "allMids"})
        mid = data.get(symbol.upper())
        if mid is None:
            raise RuntimeError(f"hyperliquid mid price not found: {symbol}")
        return float(mid)

    async def _post_order(self, action: dict) -> dict:
        nonce = int(time.time() * 1000)
        signature = _sign_l1_action(self.private_key, action, None, nonce)
        body = {"action": action, "nonce": nonce, "signature": signature, "vaultAddress": None}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                HYPERLIQUID_EXCHANGE_URL,
                json=body,
                headers={"Content-Type": "application/json"},
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"hyperliquid exchange {resp.status}: {data}")
                if data.get("status") != "ok":
                    raise RuntimeError(f"hyperliquid order rejected: {data}")
                return data

    async def _get_order_status(self, order_id: object) -> dict:
        async with aiohttp.ClientSession() as session:
            return await fetch_json(
                session,
                "POST",
                HYPERLIQUID_INFO_URL,
                json={
                    "type": "orderStatus",
                    "user": self.account_address,
                    "oid": order_id,
                },
            )

    async def _fetch_clearinghouse_state(self) -> dict:
        async with aiohttp.ClientSession() as session:
            return await fetch_json(
                session,
                "POST",
                HYPERLIQUID_INFO_URL,
                json={
                    "type": "clearinghouseState",
                    "user": self.account_address,
                },
            )

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict | None:
        if market_type != "perp":
            raise RuntimeError("hyperliquid live position query only supports perp")
        normalized_symbol = symbol.strip().upper()
        state = await self._fetch_clearinghouse_state()
        raw_positions = state.get("assetPositions", [])
        print(f"[hyperliquid] get_open_position symbol={normalized_symbol} querying_account={self.account_address} total_positions={len(raw_positions)}")
        for item in state.get("assetPositions", []):
            position = item.get("position", {})
            if str(position.get("coin", "")).strip().upper() != normalized_symbol:
                continue
            size = Decimal(str(position.get("szi", "0") or "0"))
            if size == 0:
                continue
            return {
                "symbol": normalized_symbol,
                "market_type": "perp",
                "side": "LONG" if size > 0 else "SHORT",
                "quantity": format(abs(size).normalize(), "f"),
                "raw": item,
            }
        return None

    async def ensure_isolated_margin(self, symbol: str) -> int:
        asset_index = await self._get_asset_index(symbol)
        if self.skip_margin_setup or asset_index in self._isolated_asset_indices:
            return asset_index
        action = {
            "type": "updateLeverage",
            "asset": asset_index,
            "isCross": False,
            "leverage": self.default_leverage,
        }
        await self._post_order(action)
        self._isolated_asset_indices.add(asset_index)
        return asset_index

    async def add_isolated_margin(
        self,
        *,
        venue: str,
        symbol: str,
        side: str,
        amount_usd: float,
        **kwargs,
    ) -> dict:
        asset_index = await self._get_asset_index(symbol)
        side_normalized = side.strip().upper()
        if side_normalized not in {"LONG", "SHORT"}:
            raise RuntimeError(f"unsupported hyperliquid position side: {side}")
        action = {
            "type": "updateIsolatedMargin",
            "asset": asset_index,
            "isBuy": side_normalized == "LONG",
            "ntli": int(Decimal(str(amount_usd)) * Decimal("1000000")),
        }
        data = await self._post_order(action)
        return {"ok": True, "raw": data}

    def _build_action(
        self,
        *,
        asset_index: int,
        is_buy: bool,
        price: float,
        size: float,
        tif: str,
        reduce_only: bool = False,
    ) -> dict:
        return {
            "type": "order",
            "orders": [{
                "a": asset_index,
                "b": is_buy,
                "p": hyperliquid_float_to_wire(price),
                "s": hyperliquid_float_to_wire(size),
                "r": reduce_only,
                "t": {"limit": {"tif": tif}},
            }],
            "grouping": "na",
        }

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str
    ) -> dict:
        asset_index = await self.ensure_isolated_margin(symbol)
        is_buy = side.strip().upper() == "BUY"
        action = self._build_action(
            asset_index=asset_index,
            is_buy=is_buy,
            price=float(price),
            size=float(amount),
            tif="Gtc",
        )
        data = await self._post_order(action)
        order_id = extract_hyperliquid_order_id(data, fill_type="resting")
        return {"ok": True, "order_id": order_id, "raw": data}

    async def wait_for_order_fill(
        self,
        *,
        order_result: dict,
        symbol: str,
        side: str,
        amount: str,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.5,
        allow_partial_fill: bool = False,
    ) -> dict:
        order_id = order_result.get("order_id") or order_result.get("oid")
        if order_id is None:
            raise RuntimeError("hyperliquid limit order fill wait requires order_id")
        return await poll_until_filled(
            fetch_status=lambda: self._get_order_status(order_id),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            timeout_message="hyperliquid limit order fill timeout",
            return_on_partial_fill=allow_partial_fill,
        )

    async def place_market_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float
    ) -> dict:
        asset_index = await self.ensure_isolated_margin(symbol)
        is_buy = side.strip().upper() == "BUY"
        mid = await self._get_mid_price(symbol)
        slippage = self.slippage_bps / 10000
        price = mid * (1 + slippage) if is_buy else mid * (1 - slippage)
        size = float(amount)
        action = self._build_action(
            asset_index=asset_index,
            is_buy=is_buy,
            price=price,
            size=size,
            tif="Ioc",
        )
        data = await self._post_order(action)
        order_id = extract_hyperliquid_order_id(data, fill_type="filled")
        return {"ok": True, "order_id": order_id, "raw": data}

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
            raise RuntimeError("hyperliquid spot emergency close is not supported")
        asset_index = await self._get_asset_index(symbol)
        is_buy = side.strip().upper() == "BUY"
        mid = await self._get_mid_price(symbol)
        slippage = self.slippage_bps / 10000
        price = mid * (1 + slippage) if is_buy else mid * (1 - slippage)
        action = self._build_action(
            asset_index=asset_index,
            is_buy=is_buy,
            price=price,
            size=float(quantity),
            tif="Ioc",
            reduce_only=True,
        )
        data = await self._post_order(action)
        order_id = extract_hyperliquid_order_id(data, fill_type="filled")
        return {"ok": True, "order_id": order_id, "raw": data}
