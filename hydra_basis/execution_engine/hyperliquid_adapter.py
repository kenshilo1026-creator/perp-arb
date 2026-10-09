from __future__ import annotations

import os
import threading
import time
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_EVEN, Decimal

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
from hydra_basis.adapters.hyperliquid import (
    fetch_hyperliquid_meta, fetch_hyperliquid_perp_dex_index, hyperliquid_asset_id,
)
from hydra_basis.execution_engine.order_fill import poll_until_filled
from hydra_basis.execution_engine.hedge_safety import wait_for_terminal_order


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


HYPERLIQUID_MAX_PERP_DECIMALS = 6
HYPERLIQUID_MAX_SIGNIFICANT_FIGURES = 5


def _decimal_to_wire(value: Decimal) -> str:
    # Fixed-point only: the exchange does not accept scientific notation.
    text = format(value.normalize(), "f")
    return "0" if text == "-0" else text


def hyperliquid_price_to_wire(price, *, sz_decimals: int | None = None,
                              rounding: str = ROUND_HALF_EVEN) -> str:
    """Perp price rule: integers are always valid; otherwise at most 5 significant
    figures and at most (6 - szDecimals) decimals."""
    value = Decimal(str(price))
    if not value.is_finite() or value <= 0:
        raise RuntimeError(f"invalid hyperliquid price: {price}")
    if value == value.to_integral_value():
        return _decimal_to_wire(value)
    integer_digits = value.adjusted() + 1
    if integer_digits >= HYPERLIQUID_MAX_SIGNIFICANT_FIGURES:
        return _decimal_to_wire(value.quantize(Decimal(1), rounding=rounding))
    places = HYPERLIQUID_MAX_SIGNIFICANT_FIGURES - integer_digits
    if sz_decimals is not None:
        places = min(places, max(0, HYPERLIQUID_MAX_PERP_DECIMALS - sz_decimals))
    return _decimal_to_wire(value.quantize(Decimal(1).scaleb(-places), rounding=rounding))


def hyperliquid_size_to_wire(size, *, sz_decimals: int | None = None) -> str:
    """Sizes are rounded down to the asset's szDecimals; never up past the request."""
    value = Decimal(str(size))
    if not value.is_finite() or value <= 0:
        raise RuntimeError(f"invalid hyperliquid size: {size}")
    if sz_decimals is not None:
        value = value.quantize(Decimal(1).scaleb(-sz_decimals), rounding=ROUND_DOWN)
        if value <= 0:
            raise RuntimeError(f"hyperliquid size {size} is below the {sz_decimals}-decimal lot size")
    return _decimal_to_wire(value)


def hyperliquid_float_to_wire(x: float) -> str:
    return hyperliquid_price_to_wire(x)


def extract_hyperliquid_order_id(data: dict, *, fill_type: str) -> int | None:
    statuses = data.get("response", {}).get("data", {}).get("statuses", [])
    if not statuses:
        return None
    status = statuses[0]
    if "error" in status:
        raise RuntimeError(f"hyperliquid order error: {status['error']}")
    return (status.get(fill_type) or {}).get("oid")


class HyperliquidExecutionAdapter:
    supports_limit_ioc = True
    # Every adapter instance signing for one key (main dex and HIP-3 dexes alike)
    # shares this nonce sequence: two orders in the same millisecond must not collide.
    _nonce_lock = threading.Lock()
    _last_nonce_by_signer: dict[str, int] = {}

    def __init__(
        self,
        *,
        private_key: str | None = None,
        account_address: str | None = None,
        leverage: int | None = None,
        slippage_bps: float = 50.0,
        skip_margin_setup: bool = False,
        dex: str | None = None,
        venue_name: str = "hyperliquid",
    ) -> None:
        # ``dex`` trades a HIP-3 builder-deployed dex on the same account, e.g. "io" (Entropy).
        self.dex = dex.lower() if dex else None
        self.venue_name = venue_name
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
        self._sz_decimals: dict[str, int] | None = None
        self._dex_index = 0
        self._isolated_asset_indices: set[int] = set()

    def _coin(self, symbol: str) -> str:
        """Exchange coin name: "ETH" on the main dex, "IO:OAI" (any case) on a HIP-3 dex."""
        normalized = symbol.strip().upper()
        dex = getattr(self, "dex", None)
        if not dex or normalized.startswith(f"{dex.upper()}:"):
            return normalized
        return f"{dex.upper()}:{normalized}"

    def _symbol(self, coin: str) -> str:
        """Strategy symbol for an exchange coin name (the HIP-3 prefix removed)."""
        return coin.strip().upper().partition(":")[2] if getattr(self, "dex", None) else coin.strip().upper()

    async def _load_meta(self) -> None:
        dex = getattr(self, "dex", None)
        async with aiohttp.ClientSession() as session:
            rows = await fetch_hyperliquid_meta(session, dex)
            self._dex_index = await fetch_hyperliquid_perp_dex_index(session, dex) if dex else 0
        self._universe = [str(row.get("name") or "").upper() for row in rows]
        self._sz_decimals = {str(row.get("name") or "").upper(): int(row["szDecimals"])
                             for row in rows if row.get("szDecimals") is not None}

    async def _get_asset_index(self, symbol: str) -> int:
        if getattr(self, "_universe", None) is None:
            await self._load_meta()
        try:
            index = self._universe.index(self._coin(symbol))
        except ValueError:
            raise RuntimeError(f"{getattr(self, 'venue_name', 'hyperliquid')} symbol not found: {symbol}")
        return hyperliquid_asset_id(index, getattr(self, "_dex_index", 0))

    async def _get_sz_decimals(self, symbol: str) -> int:
        if getattr(self, "_sz_decimals", None) is None:
            await self._load_meta()
        sz_decimals = self._sz_decimals.get(self._coin(symbol))
        if sz_decimals is None:
            raise RuntimeError(f"hyperliquid szDecimals not found: {symbol}")
        return sz_decimals

    async def _get_mid_price(self, symbol: str) -> float:
        payload = {"type": "allMids"}
        if getattr(self, "dex", None):
            payload["dex"] = self.dex
        async with aiohttp.ClientSession() as session:
            data = await fetch_json(session, "POST", HYPERLIQUID_INFO_URL, json=payload)
        coin = self._coin(symbol)
        mid = next((value for key, value in data.items() if str(key).upper() == coin), None)
        if mid is None:
            raise RuntimeError(f"hyperliquid mid price not found: {symbol}")
        return float(mid)

    def _next_nonce(self) -> int:
        signer = self._wallet.address.lower() if hasattr(self, "_wallet") else ""
        now = int(time.time() * 1000)
        with HyperliquidExecutionAdapter._nonce_lock:
            nonce = max(now, HyperliquidExecutionAdapter._last_nonce_by_signer.get(signer, 0) + 1)
            HyperliquidExecutionAdapter._last_nonce_by_signer[signer] = nonce
        return nonce

    async def _post_order(self, action: dict) -> dict:
        nonce = self._next_nonce()
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
                    **({"dex": self.dex} if getattr(self, "dex", None) else {}),
                },
            )

    async def list_open_orders(self, *, symbol: str) -> list[dict]:
        async with aiohttp.ClientSession() as session:
            orders = await fetch_json(session, "POST", HYPERLIQUID_INFO_URL,
                                      json={"type": "openOrders", "user": self.account_address,
                                            **({"dex": self.dex} if getattr(self, "dex", None) else {})})
        if not isinstance(orders, list):
            raise RuntimeError("hyperliquid open-order list unavailable")
        if any(not isinstance(item, dict) or "coin" not in item for item in orders):
            raise RuntimeError("malformed hyperliquid open orders")
        return [item for item in orders if str(item["coin"]).upper() == self._coin(symbol)]

    async def get_fill_average_price(self, *, symbol: str, order_result: dict,
                                     quantity: Decimal) -> Decimal | None:
        order_id = order_result.get("order_id") or order_result.get("oid")
        if order_id is None:
            return None
        async with aiohttp.ClientSession() as session:
            fills = await fetch_json(session, "POST", HYPERLIQUID_INFO_URL,
                                     json={"type": "userFills", "user": self.account_address})
        if not isinstance(fills, list):
            raise RuntimeError("hyperliquid fills unavailable")
        selected = [item for item in fills if str(item.get("oid")) == str(order_id)
                    and str(item.get("coin", "")).upper() == self._coin(symbol)]
        filled = sum((Decimal(str(item["sz"])) for item in selected), Decimal("0"))
        if filled != quantity:
            return None
        return sum((Decimal(str(item["sz"])) * Decimal(str(item["px"]))
                    for item in selected), Decimal("0")) / quantity

    async def get_order_execution(self, *, order_result: dict, symbol: str) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId") or order_result.get("oid")
        if order_id is None:
            raise RuntimeError("hyperliquid order query requires order_id")
        data = await self._get_order_status(order_id)
        wrapper = data.get("order") if data.get("status") == "order" else None
        order = wrapper.get("order") if isinstance(wrapper, dict) else None
        if not isinstance(order, dict):
            return {"status": "UNKNOWN", "terminal": False}
        if str(order.get("oid")) != str(order_id) or str(order.get("coin", "")).upper() != self._coin(symbol):
            raise RuntimeError("hyperliquid order query identity mismatch")
        status = str(wrapper.get("status", "")).upper()
        if status.endswith("CANCELED") or status == "SCHEDULEDCANCEL":
            status = "CANCELED"
        elif status.endswith("REJECTED"):
            status = "REJECTED"
        try:
            original = Decimal(str(order["origSz"]))
            remaining = Decimal(str(order["sz"]))
            if not original.is_finite() or not remaining.is_finite() or not 0 <= remaining <= original:
                raise ValueError("invalid sizes")
        except (KeyError, ValueError, ArithmeticError) as exc:
            raise RuntimeError("hyperliquid cumulative fill unavailable") from exc
        return {"status": status, "terminal": status in {"FILLED", "CANCELED", "REJECTED"},
                "filled_quantity": str(original - remaining), "order_id": order_id, "raw": data}

    async def get_available_margin(self) -> Decimal:
        state = await self._fetch_clearinghouse_state()
        if not isinstance(state, dict) or state.get("withdrawable") is None:
            raise RuntimeError(f"hyperliquid withdrawable balance unavailable: {state}")
        return Decimal(str(state["withdrawable"]))

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict | None:
        if market_type != "perp":
            raise RuntimeError("hyperliquid live position query only supports perp")
        normalized_symbol = symbol.strip().upper()
        coin = self._coin(symbol)
        state = await self._fetch_clearinghouse_state()
        raw_positions = state.get("assetPositions", [])
        print(f"[hyperliquid] get_open_position symbol={coin} querying_account={self.account_address} total_positions={len(raw_positions)}")
        for item in state.get("assetPositions", []):
            position = item.get("position", {})
            if str(position.get("coin", "")).strip().upper() != coin:
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

    async def list_open_positions(self) -> list[dict]:
        state = await self._fetch_clearinghouse_state()
        positions: list[dict] = []
        for item in state.get("assetPositions", []):
            position = item.get("position", {})
            symbol = self._symbol(str(position.get("coin", "")))
            if not symbol:
                continue
            size = Decimal(str(position.get("szi", "0") or "0"))
            if size == 0:
                continue
            positions.append(
                {
                    "venue": getattr(self, "venue_name", "hyperliquid"),
                    "symbol": symbol,
                    "market_type": "perp",
                    "side": "LONG" if size > 0 else "SHORT",
                    "quantity": format(abs(size).normalize(), "f"),
                    "raw": item,
                }
            )
        return positions

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
        price,
        size,
        tif: str,
        reduce_only: bool = False,
        sz_decimals: int | None = None,
        price_rounding: str = ROUND_HALF_EVEN,
    ) -> dict:
        return {
            "type": "order",
            "orders": [{
                "a": asset_index,
                "b": is_buy,
                "p": hyperliquid_price_to_wire(price, sz_decimals=sz_decimals, rounding=price_rounding),
                "s": hyperliquid_size_to_wire(size, sz_decimals=sz_decimals),
                "r": reduce_only,
                "t": {"limit": {"tif": tif}},
            }],
            "grouping": "na",
        }

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str,
        reduce_only: bool = False, post_only: bool = False,
    ) -> dict:
        asset_index = await self.ensure_isolated_margin(symbol)
        is_buy = side.strip().upper() == "BUY"
        action = self._build_action(
            asset_index=asset_index,
            is_buy=is_buy,
            price=price,
            size=amount,
            sz_decimals=await self._get_sz_decimals(symbol),
            # Alo (add liquidity only) is post-only: a crossing order is rejected.
            tif="Alo" if post_only else "Gtc",
            reduce_only=reduce_only,
        )
        data = await self._post_order(action)
        order_id = extract_hyperliquid_order_id(data, fill_type="resting")
        if order_id is None:
            order_id = extract_hyperliquid_order_id(data, fill_type="filled")
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
            fetch_status=lambda: self.get_order_execution(order_result=order_result, symbol=symbol),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            timeout_message="hyperliquid limit order fill timeout",
            return_on_partial_fill=allow_partial_fill,
        )

    async def cancel_order(
        self,
        *,
        order_result: dict,
        symbol: str,
        side: str,
        amount: str,
    ) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId") or order_result.get("oid")
        if order_id is None:
            raise RuntimeError("hyperliquid cancel_order requires order_id")
        asset_index = await self._get_asset_index(symbol)
        action = {
            "type": "cancel",
            "cancels": [{"a": asset_index, "o": int(order_id)}],
        }
        # An already-filled order can reject cancellation. In either case the
        # terminal lookup, not the cancel acknowledgement, decides the fill.
        data = None
        cancel_error = None
        try:
            data = await self._post_order(action)
        except Exception as exc:
            cancel_error = str(exc)
        final = await wait_for_terminal_order(
            lambda: self.get_order_execution(order_result=order_result, symbol=symbol),
        )
        return {"ok": True, "order_id": order_id, "raw": final,
                "cancel_response": data, "cancel_error": cancel_error}

    async def place_market_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float,
        reduce_only: bool = False, limit_price: str | None = None,
    ) -> dict:
        asset_index = await self.ensure_isolated_margin(symbol)
        is_buy = side.strip().upper() == "BUY"
        if limit_price is not None:
            # IOC at the caller's worst acceptable price; rounding only makes it stricter.
            price, rounding = Decimal(str(limit_price)), ROUND_FLOOR if is_buy else ROUND_CEILING
        else:
            mid = await self._get_mid_price(symbol)
            slippage = self.slippage_bps / 10000
            price = mid * (1 + slippage) if is_buy else mid * (1 - slippage)
            # Round the IOC limit away from the market so slippage protection is kept.
            rounding = ROUND_CEILING if is_buy else ROUND_FLOOR
        action = self._build_action(
            asset_index=asset_index,
            is_buy=is_buy,
            price=price,
            size=amount,
            tif="Ioc",
            reduce_only=reduce_only,
            sz_decimals=await self._get_sz_decimals(symbol),
            price_rounding=rounding,
        )
        data = await self._post_order(action)
        statuses = data.get("response", {}).get("data", {}).get("statuses", [])
        status = statuses[0] if len(statuses) == 1 else None
        if isinstance(status, dict) and "error" in status:
            error = RuntimeError(f"hyperliquid order error: {status['error']}")
            error.order_result = {
                "ok": False, "terminal": True, "filled_quantity": "0", "raw": data,
            }
            raise error
        fill = status.get("filled") if isinstance(status, dict) else None
        if not isinstance(fill, dict) or fill.get("totalSz") is None:
            raise RuntimeError(f"hyperliquid IOC fill quantity unavailable: {data}")
        filled_quantity = Decimal(str(fill["totalSz"]))
        if not filled_quantity.is_finite() or filled_quantity < 0:
            raise RuntimeError(f"hyperliquid invalid IOC fill quantity: {data}")
        return {
            "ok": True, "terminal": True, "order_id": fill.get("oid"),
            "filled_quantity": str(filled_quantity), "avg_price": fill.get("avgPx"),
            "raw": data,
        }

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
            size=quantity,
            tif="Ioc",
            reduce_only=True,
            sz_decimals=await self._get_sz_decimals(symbol),
            price_rounding=ROUND_CEILING if is_buy else ROUND_FLOOR,
        )
        data = await self._post_order(action)
        order_id = extract_hyperliquid_order_id(data, fill_type="filled")
        return {"ok": True, "order_id": order_id, "raw": data}
