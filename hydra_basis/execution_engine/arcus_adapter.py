"""Arcus perpetuals execution adapter (REST).

Auth: an Ed25519 API key registered to the master Ethereum address (create it on
https://app.arcus.xyz/api-keys). Environment:

* ``ARCUS_API_SIGNING_KEY`` - Ed25519 private key, 64 hex chars (shown once by the app)
* ``ARCUS_ADDRESS``         - master wallet address the key is registered to
* ``ARCUS_ACCOUNT_INDEX``   - subaccount index (default 0)
* ``ARCUS_BASE_URL``        - optional, e.g. https://api.testnet.arcus.xyz

placeOrder / cancelOrder sign a compact typed payload with integer ticks and
quantums; setLeverage signs ``timestamp + action + canonical_json(body)``.
Order placement is asynchronous: a 202 ``ACK`` carries only the order id, so
fills are read back through ``GET /v1/order/{id}``.
"""
from __future__ import annotations

import json
import os
import threading
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

import aiohttp
from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from hydra_basis.adapters.arcus import arcus_base_url, arcus_market_name
from hydra_basis.execution_engine.hedge_safety import wait_for_terminal_order
from hydra_basis.execution_engine.order_fill import poll_until_filled

SIDE = {"BUY": 0, "SELL": 1}
TIF = {"GTT": 0, "FOK": 1, "IOC": 2, "ALO": 3}
TERMINAL = {"FILLED", "CANCELED", "MARGIN_CANCELED", "REJECTED", "LIQUIDATED", "ADL"}
GOOD_TIL_DAYS = 40  # goodTilTime must be at least one month ahead, even for IOC


def canonical(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def ed25519_signer(private_key_hex: str):
    key = ECC.construct(curve="Ed25519", seed=bytes.fromhex(private_key_hex.removeprefix("0x")))
    return key, eddsa.new(key, "rfc8032")


def to_units(value: Decimal, unit: Decimal) -> int:
    units = value / unit
    if units != units.to_integral_value():
        raise RuntimeError(f"arcus value {value} is not a multiple of {unit}")
    return int(units)


def tick_for_price(market: dict, price: Decimal) -> Decimal:
    """Valid price increment at this price level (tickTiers widen it for large prices)."""
    for tier in market.get("tickTiers") or []:
        if tier.get("upToPrice") is None or price <= Decimal(str(tier["upToPrice"])):
            return Decimal(str(tier["tick"]))
    return Decimal(str(market["tickSize"]))


class ArcusExecutionAdapter:
    # One timestamp sequence per API key: X-Timestamp doubles as the payload's ``ct``.
    _ts_lock = threading.Lock()
    _last_ts_by_key: dict[str, int] = {}

    def __init__(self, *, private_key: str | None = None, address: str | None = None,
                 account_index: int | None = None, leverage: int = 1, slippage_bps: float = 50.0,
                 skip_margin_setup: bool = False, base_url: str | None = None) -> None:
        private_key = private_key or os.getenv("ARCUS_API_SIGNING_KEY", "")
        if not private_key:
            raise RuntimeError("ARCUS_API_SIGNING_KEY is not set")
        self.address = address or os.getenv("ARCUS_ADDRESS", "")
        if not self.address:
            raise RuntimeError("ARCUS_ADDRESS is not set")
        self.account_index = int(account_index if account_index is not None
                                 else os.getenv("ARCUS_ACCOUNT_INDEX", "0"))
        self._key, self._signer = ed25519_signer(private_key)
        self.api_key = self._key.public_key().export_key(format="raw").hex()
        self.leverage = leverage
        self.slippage_bps = slippage_bps
        self.skip_margin_setup = skip_margin_setup
        self.base_url = (base_url or arcus_base_url()).rstrip("/")
        self._markets: dict[str, dict] | None = None
        self._leverage_set: set[int] = set()

    # ------------------------------------------------------------------ plumbing

    def _sign(self, message: str) -> str:
        return self._signer.sign(message.encode()).hex()

    def _timestamp_ns(self) -> int:
        now = time.time_ns()
        with ArcusExecutionAdapter._ts_lock:
            ts = max(now, ArcusExecutionAdapter._last_ts_by_key.get(self.api_key, 0) + 1)
            ArcusExecutionAdapter._last_ts_by_key[self.api_key] = ts
        return ts

    def _headers(self, ts: int, signature: str) -> dict[str, str]:
        return {"Content-Type": "application/json", "X-API-Key": self.api_key,
                "X-Timestamp": str(ts), "X-Signature": signature}

    async def _request(self, method: str, path: str, *, params=None, body=None, headers=None,
                       label: str = "request") -> tuple[int, dict]:
        data = canonical(body).encode() if body is not None else None
        async with aiohttp.ClientSession() as session:
            async with session.request(method, f"{self.base_url}{path}", params=params, data=data,
                                       headers=headers or {}, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                text = await resp.text()
                try:
                    payload = json.loads(text) if text else {}
                except ValueError:
                    payload = {"raw": text[:300]}
                if resp.status >= 400:
                    raise RuntimeError(f"arcus {label} {resp.status}: {payload}")
                return resp.status, payload

    def _account_params(self, **extra) -> dict:
        return {"address": self.address, "accountIndex": self.account_index, **extra}

    async def _load_markets(self) -> dict[str, dict]:
        if self._markets is None:
            _, data = await self._request("GET", "/v1/markets", label="markets")
            self._markets = {str(m["baseAsset"]).upper(): m for m in data.get("markets") or []}
        return self._markets

    async def _market(self, symbol: str) -> dict:
        market = (await self._load_markets()).get(symbol.strip().upper().removesuffix("-USD"))
        if market is None:
            raise RuntimeError(f"arcus symbol not found: {symbol}")
        if market.get("status") != "ONLINE":
            raise RuntimeError(f"arcus market {market['marketDisplayName']} is {market.get('status')}")
        return market

    async def warm_up(self) -> None:
        await self._load_markets()

    # ------------------------------------------------------------------ orders

    async def _place(self, *, symbol: str, side: str, amount, price: Decimal, tif: str,
                     reduce_only: bool) -> dict:
        market = await self._market(symbol)
        side = side.strip().upper()
        step = Decimal(str(market["stepSize"]))
        quantity = (Decimal(str(amount)) / step).to_integral_value(rounding=ROUND_FLOOR) * step
        if quantity <= 0:
            raise RuntimeError(f"arcus quantity {amount} is below stepSize={step}")
        tick = tick_for_price(market, price)
        rounding = ROUND_FLOOR if side == "BUY" else ROUND_CEILING
        price = (price / tick).to_integral_value(rounding=rounding) * tick
        top_tick = Decimal(str(market["tickSize"]))
        good_til_us = int(time.time() * 1_000_000) + GOOD_TIL_DAYS * 86_400 * 1_000_000
        ts = self._timestamp_ns()
        payload = canonical({
            "ad": self.address.lower(), "ai": self.account_index, "ct": ts, "g": good_til_us * 1000,
            "m": int(market["marketId"]), "op": 1, "p": to_units(price, top_tick), "q": to_units(quantity, step),
            "r": 1 if reduce_only else 0, "s": SIDE[side], "t": TIF[tif], "v": 1,
        })
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": int(market["marketId"]),
                "orderSide": side, "orderType": "LIMIT", "quantity": format(quantity.normalize(), "f"),
                "price": format(price.normalize(), "f"), "timeInForce": tif, "goodTilTime": str(good_til_us),
                "reduceOnly": reduce_only, "timestamp": ts}
        status_code, data = await self._request("POST", "/v1/placeOrder", body=body,
                                                 headers=self._headers(ts, self._sign(payload)), label="order")
        status = str(data.get("status") or "").upper()
        result = {"ok": True, "order_id": data.get("orderId"), "status": status or "ACK",
                  "http_status": status_code, "raw": data}
        if status == "REJECTED":
            reason = data.get("rejectionReason") or "REJECTED"
            if tif == "IOC" and reason in {"IOC_CANCELED", "COULD_NOT_FILL"}:
                # Zero-fill IOC is a definitive terminal outcome, not an error.
                return {**result, "terminal": True, "filled_quantity": "0", "status": "CANCELED"}
            error = RuntimeError(f"arcus order rejected: {reason}")
            error.order_result = {**result, "ok": False, "terminal": True, "filled_quantity": "0"}
            raise error
        if status in TERMINAL and data.get("filledSize") is not None:
            result |= {"terminal": True, "filled_quantity": str(data["filledSize"])}
        return result

    async def place_limit_order(self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str,
                                reduce_only: bool = False, post_only: bool = False) -> dict:
        if not reduce_only:
            await self.ensure_leverage(symbol)
        return await self._place(symbol=symbol, side=side, amount=amount, price=Decimal(str(price)),
                                 tif="ALO" if post_only else "GTT", reduce_only=reduce_only)

    async def place_market_order(self, *, symbol: str, side: str, amount: str, clip_usd: float,
                                 reduce_only: bool = False) -> dict:
        # A marketable IOC limit bounded by slippage from the live top of book.
        if not reduce_only:
            await self.ensure_leverage(symbol)
        _, book = await self._request("GET", f"/v1/bbo/{arcus_market_name(symbol)}", label="bbo")
        is_buy = side.strip().upper() == "BUY"
        level = book.get("bestAsk" if is_buy else "bestBid")
        if not level:
            raise RuntimeError(f"arcus empty book for {symbol}")
        slip = Decimal(str(self.slippage_bps)) / Decimal(10_000)
        price = Decimal(str(level["price"])) * (1 + slip if is_buy else 1 - slip)
        # Round away from the market so the slippage bound is kept.
        market = await self._market(symbol)
        tick = tick_for_price(market, price)
        price = (price / tick).to_integral_value(rounding=ROUND_CEILING if is_buy else ROUND_FLOOR) * tick
        return await self._place(symbol=symbol, side=side, amount=amount, price=price, tif="IOC",
                                 reduce_only=reduce_only)

    async def get_order_execution(self, *, order_result: dict, symbol: str) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if not order_id:
            raise RuntimeError("arcus order query requires order_id")
        try:
            _, order = await self._request("GET", f"/v1/order/{order_id}", params=self._account_params(),
                                           label="order status")
        except RuntimeError as exc:
            if " 404:" in str(exc):
                return {"status": "UNKNOWN", "terminal": False}  # accepted but not indexed yet
            raise
        if str(order.get("orderId")) != str(order_id):
            raise RuntimeError("arcus order query identity mismatch")
        status = str(order.get("status") or "UNKNOWN").upper()
        terminal = status in TERMINAL
        result = {"status": "CANCELED" if status in {"MARGIN_CANCELED", "LIQUIDATED", "ADL"} else status,
                  "terminal": terminal, "order_id": order_id, "raw": order}
        if order.get("filledSize") is not None:
            result["filled_quantity"] = str(order["filledSize"])
        if order.get("avgFillPrice") not in (None, "", "0"):
            result["avg_price"] = str(order["avgFillPrice"])
        return result

    async def wait_for_order_fill(self, *, order_result: dict, symbol: str, side: str, amount: str,
                                  timeout_seconds: float, poll_interval_seconds: float = 0.5,
                                  allow_partial_fill: bool = False) -> dict:
        return await poll_until_filled(
            fetch_status=lambda: self.get_order_execution(order_result=order_result, symbol=symbol),
            timeout_seconds=timeout_seconds, poll_interval_seconds=poll_interval_seconds,
            timeout_message="arcus limit order fill timeout", return_on_partial_fill=allow_partial_fill)

    async def cancel_order(self, *, order_result: dict, symbol: str, side: str, amount: str) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if not order_id:
            raise RuntimeError("arcus cancel_order requires order_id")
        market = await self._market(symbol)
        ts = self._timestamp_ns()
        payload = canonical({"ad": self.address.lower(), "ai": self.account_index, "ct": ts, "id": str(order_id),
                             "m": int(market["marketId"]), "op": 2, "v": 1})
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": int(market["marketId"]),
                "kind": "orderId", "orderId": str(order_id), "timestamp": ts}
        cancel_error = None
        try:
            await self._request("POST", "/v1/cancelOrder", body=body,
                                headers=self._headers(ts, self._sign(payload)), label="cancel")
        except RuntimeError as exc:
            cancel_error = str(exc)  # e.g. already filled; the terminal lookup decides
        final = await wait_for_terminal_order(
            lambda: self.get_order_execution(order_result=order_result, symbol=symbol))
        return {"ok": True, "order_id": order_id, "raw": final, "cancel_error": cancel_error}

    async def list_open_orders(self, *, symbol: str) -> list[dict]:
        _, data = await self._request("GET", "/v1/openOrders",
                                      params=self._account_params(market=arcus_market_name(symbol)),
                                      label="open orders")
        orders = data.get("orders", data) if isinstance(data, dict) else data
        if not isinstance(orders, list):
            raise RuntimeError("arcus open-order list unavailable")
        return orders

    # ------------------------------------------------------------------ account

    async def _positions(self, symbol: str | None = None) -> list[dict]:
        params = self._account_params(**({"market": arcus_market_name(symbol)} if symbol else {}))
        _, data = await self._request("GET", "/v1/positions", params=params, label="positions")
        positions = data.get("positions") or []
        # Live responses key positions by market id; the spec documents a list.
        return list(positions.values()) if isinstance(positions, dict) else list(positions)

    @staticmethod
    def _normalize_position(item: dict) -> dict | None:
        size = Decimal(str(item.get("size") or "0"))
        if size == 0:
            return None
        return {"venue": "arcus", "symbol": str(item.get("marketDisplayName", "")).upper().removesuffix("-USD"),
                "market_type": "perp", "side": "LONG" if size > 0 else "SHORT",
                "quantity": format(abs(size).normalize(), "f"), "raw": item}

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict | None:
        if market_type != "perp":
            raise RuntimeError("arcus live position query only supports perp")
        name = arcus_market_name(symbol)
        for item in await self._positions(symbol):
            if str(item.get("marketDisplayName", "")).upper() == name:
                return self._normalize_position(item)
        return None

    async def list_open_positions(self) -> list[dict]:
        return [p for p in map(self._normalize_position, await self._positions()) if p is not None]

    async def get_available_margin(self) -> Decimal:
        try:
            _, account = await self._request("GET", "/v1/account", params=self._account_params(), label="account")
        except RuntimeError as exc:
            if " 404:" in str(exc):
                return Decimal("0")  # no deposit yet
            raise
        return Decimal(str(account.get("freeCollateral") or "0"))

    async def ensure_leverage(self, symbol: str) -> None:
        """Isolated margin at the configured leverage, once per market per process."""
        market = await self._market(symbol)
        market_id = int(market["marketId"])
        if self.skip_margin_setup or market_id in self._leverage_set:
            return
        body = {"accountIndex": self.account_index, "address": self.address, "isolated": True,
                "leverage": int(self.leverage), "marketId": market_id}
        ts = self._timestamp_ns()
        _, data = await self._request("POST", "/v1/setLeverage", body=body,
                                      headers=self._headers(ts, self._sign(f"{ts}setLeverage{canonical(body)}")),
                                      label="setLeverage")
        status = str(data.get("status") or "").upper()
        if status == "REJECTED":
            reason = data.get("rejectReason")
            # A cross position already exists: the mode cannot change; keep trading at the current mode.
            if reason != "HAS_OPEN_POSITION":
                raise RuntimeError(f"arcus setLeverage rejected: {reason}")
            print(f"[arcus] {market['marketDisplayName']} keeps its current margin mode: open position exists", flush=True)
        self._leverage_set.add(market_id)

    ensure_isolated_margin = ensure_leverage

    async def close_position(self, *, venue: str, symbol: str, side: str, quantity: str, market_type: str,
                             **kwargs) -> dict:
        if market_type != "perp":
            raise RuntimeError("arcus spot emergency close is not supported")
        return await self.place_market_order(symbol=symbol, side=side, amount=quantity, clip_usd=0,
                                             reduce_only=True)

    async def add_isolated_margin(self, *, venue: str, symbol: str, side: str, amount_usd: float, **kwargs) -> dict:
        raise RuntimeError("arcus isolated margin top-up is not supported by this adapter")
