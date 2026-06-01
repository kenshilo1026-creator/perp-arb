from __future__ import annotations

import os
import time

import aiohttp
import msgpack
from eth_account import Account
from eth_account.messages import encode_structured_data
from eth_hash.auto import keccak

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_universe


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
    signed = wallet.sign_message(encode_structured_data(structured))
    return {"r": hex(signed.r), "s": hex(signed.s), "v": signed.v}


def _float_to_wire(x: float) -> str:
    return f"{x:.8f}".rstrip("0").rstrip(".")


class HyperliquidExecutionAdapter:
    def __init__(
        self,
        *,
        private_key: str | None = None,
        account_address: str | None = None,
        slippage_bps: float = 50.0,
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
        self._universe: list[str] | None = None

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
                "p": _float_to_wire(price),
                "s": _float_to_wire(size),
                "r": reduce_only,
                "t": {"limit": {"tif": tif}},
            }],
            "grouping": "na",
        }

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str
    ) -> dict:
        asset_index = await self._get_asset_index(symbol)
        is_buy = side.strip().upper() == "BUY"
        action = self._build_action(
            asset_index=asset_index,
            is_buy=is_buy,
            price=float(price),
            size=float(amount),
            tif="Gtc",
        )
        data = await self._post_order(action)
        statuses = data.get("response", {}).get("data", {}).get("statuses", [{}])
        order_id = (statuses[0].get("resting") or {}).get("oid") if statuses else None
        return {"ok": True, "order_id": order_id, "raw": data}

    async def place_market_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float
    ) -> dict:
        asset_index = await self._get_asset_index(symbol)
        is_buy = side.strip().upper() == "BUY"
        mid = await self._get_mid_price(symbol)
        slippage = self.slippage_bps / 10000
        price = mid * (1 + slippage) if is_buy else mid * (1 - slippage)
        action = self._build_action(
            asset_index=asset_index,
            is_buy=is_buy,
            price=price,
            size=float(amount),
            tif="Ioc",
        )
        data = await self._post_order(action)
        statuses = data.get("response", {}).get("data", {}).get("statuses", [{}])
        order_id = (statuses[0].get("filled") or {}).get("oid") if statuses else None
        return {"ok": True, "order_id": order_id, "raw": data}
