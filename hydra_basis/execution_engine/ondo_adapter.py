"""Ondo Perps execution adapter (REST, HMAC API key).

Environment: ``ONDO_API_KEY_ID`` ("ondoKeyId_..."), ``ONDO_API_SECRET``
("ondoApiSecret_..."), optional ``ONDO_BASE_URL``. Create the key on the Ondo web app
(address menu -> API Keys) with trading permission; whitelisting your IP is recommended.

Ondo margin is cross only: every position shares the account's collateral.
"""
from __future__ import annotations

import uuid
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

import aiohttp

from hydra_basis.adapters.ondo import (
    fetch_ondo_markets, ondo_credentials, ondo_market_name, ondo_request, ondo_symbol,
)
from hydra_basis.execution_engine.hedge_safety import wait_for_terminal_order
from hydra_basis.execution_engine.order_fill import poll_until_filled

STATUS = {"open": "OPEN", "pending": "OPEN", "untriggered": "OPEN", "fullyfilled": "FILLED", "canceled": "CANCELED"}
TERMINAL = {"FILLED", "CANCELED"}


def _snap(value: Decimal, step: Decimal, rounding) -> Decimal:
    return (value / step).to_integral_value(rounding=rounding) * step


class OndoExecutionAdapter:
    supports_limit_ioc = True

    def __init__(self, *, leverage: int = 1, skip_margin_setup: bool = False) -> None:
        if ondo_credentials() is None:
            raise RuntimeError("ONDO_API_KEY_ID / ONDO_API_SECRET are not set")
        self.leverage = leverage
        self.skip_margin_setup = skip_margin_setup
        self._markets: dict[str, dict] | None = None
        self._leverage_set: set[str] = set()

    async def _call(self, method: str, path: str, *, params=None, body=None, label: str = "request"):
        async with aiohttp.ClientSession() as session:
            result, _ = await ondo_request(session, method, path, params=params, body=body, auth=True, label=label)
            return result

    async def _market(self, symbol: str) -> dict:
        if self._markets is None:
            async with aiohttp.ClientSession() as session:
                self._markets = {row["market"]: row for row in await fetch_ondo_markets(session)}
        market = self._markets.get(ondo_market_name(symbol))
        if market is None:
            raise RuntimeError(f"ondo symbol not found: {symbol}")
        return market

    async def warm_up(self) -> None:
        await self._market("BTC")

    # ------------------------------------------------------------------ orders

    async def _order(self, *, symbol: str, side: str, amount, order_type: str, price=None,
                     time_in_force: str | None = None, post_only: bool = False, reduce_only: bool = False) -> dict:
        market = await self._market(symbol)
        side = side.strip().upper()
        lot = Decimal(str(market["baseIncrement"]))
        size = _snap(Decimal(str(amount)), lot, ROUND_FLOOR)
        if size <= 0:
            raise RuntimeError(f"ondo quantity {amount} is below baseIncrement={lot}")
        body = {"market": market["market"], "side": side.lower(), "type": order_type,
                "size": format(size.normalize(), "f"), "clientOrderId": uuid.uuid4().hex,
                "reduceOnly": bool(reduce_only)}
        if order_type == "limit":
            tick = Decimal(str(market["quoteIncrement"]))
            # BUY rounds down, SELL up: rounding never loosens the caller's price.
            limit = _snap(Decimal(str(price)), tick, ROUND_FLOOR if side == "BUY" else ROUND_CEILING)
            body |= {"price": format(limit.normalize(), "f"), "timeInForce": time_in_force or "GTC",
                     "postOnly": bool(post_only)}
        order = await self._call("POST", "/v1/perps/orders", body=body, label="order")
        return {"ok": True, "order_id": order.get("orderId"), "client_order_id": body["clientOrderId"],
                **self._status(order), "raw": order}

    @staticmethod
    def _status(order: dict) -> dict:
        status = STATUS.get(str(order.get("status") or "").lower(), "UNKNOWN")
        filled = Decimal(str(order.get("filledSize") or "0"))
        result = {"status": status, "terminal": status in TERMINAL, "filled_quantity": str(filled)}
        cost = Decimal(str(order.get("filledCost") or "0"))
        if filled > 0 and cost > 0:
            result["avg_price"] = str(cost / filled)
        return result

    async def place_limit_order(self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str,
                                reduce_only: bool = False, post_only: bool = False) -> dict:
        if not reduce_only:
            await self.ensure_leverage(symbol)
        return await self._order(symbol=symbol, side=side, amount=amount, order_type="limit", price=price,
                                 time_in_force="GTC", post_only=post_only, reduce_only=reduce_only)

    async def place_market_order(self, *, symbol: str, side: str, amount: str, clip_usd: float,
                                 reduce_only: bool = False, limit_price: str | None = None) -> dict:
        if not reduce_only:
            await self.ensure_leverage(symbol)
        if limit_price is not None:
            # IOC limit at the caller's worst acceptable price; the exchange also applies mark-price protection.
            return await self._order(symbol=symbol, side=side, amount=amount, order_type="limit", price=limit_price,
                                     time_in_force="IOC", reduce_only=reduce_only)
        return await self._order(symbol=symbol, side=side, amount=amount, order_type="market",
                                 reduce_only=reduce_only)

    async def get_order_execution(self, *, order_result: dict, symbol: str) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if not order_id:
            client_id = order_result.get("client_order_id")
            if not client_id:
                raise RuntimeError("ondo order query requires order_id or client_order_id")
            order_id = f"client:{client_id}"
        try:
            order = await self._call("GET", f"/v1/perps/orders/{order_id}", label="order status")
        except RuntimeError as exc:
            if " 404:" in str(exc):
                return {"status": "UNKNOWN", "terminal": False}
            raise
        if order.get("market") and order["market"] != ondo_market_name(symbol):
            raise RuntimeError("ondo order query market mismatch")
        return {**self._status(order), "order_id": order.get("orderId"), "raw": order}

    async def wait_for_order_fill(self, *, order_result: dict, symbol: str, side: str, amount: str,
                                  timeout_seconds: float, poll_interval_seconds: float = 0.5,
                                  allow_partial_fill: bool = False) -> dict:
        return await poll_until_filled(
            fetch_status=lambda: self.get_order_execution(order_result=order_result, symbol=symbol),
            timeout_seconds=timeout_seconds, poll_interval_seconds=poll_interval_seconds,
            timeout_message="ondo limit order fill timeout", return_on_partial_fill=allow_partial_fill)

    async def cancel_order(self, *, order_result: dict, symbol: str, side: str, amount: str) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if not order_id:
            raise RuntimeError("ondo cancel_order requires order_id")
        cancel_error = None
        try:
            await self._call("DELETE", f"/v1/perps/orders/{order_id}", label="cancel")
        except RuntimeError as exc:
            cancel_error = str(exc)  # already filled/cancelled: the terminal lookup decides
        final = await wait_for_terminal_order(
            lambda: self.get_order_execution(order_result=order_result, symbol=symbol))
        return {"ok": True, "order_id": order_id, "raw": final, "cancel_error": cancel_error}

    async def list_open_orders(self, *, symbol: str) -> list[dict]:
        orders = await self._call("GET", "/v1/perps/orders", label="open orders",
                                  params={"market": ondo_market_name(symbol), "status": "open", "limit": 100})
        if not isinstance(orders, list):
            raise RuntimeError("ondo open-order list unavailable")
        return orders

    # ------------------------------------------------------------------ account

    @staticmethod
    def _normalize_position(item: dict) -> dict | None:
        direction = str(item.get("direction") or "").lower()
        quantity = abs(Decimal(str(item.get("netQuantity") or "0")))
        if direction not in {"long", "short"} or quantity == 0:
            return None
        return {"venue": "ondo", "symbol": ondo_symbol(item.get("market", "")), "market_type": "perp",
                "side": direction.upper(), "quantity": format(quantity.normalize(), "f"), "raw": item}

    async def list_open_positions(self) -> list[dict]:
        rows = await self._call("GET", "/v1/perps/positions", label="positions")
        return [p for p in map(self._normalize_position, rows or []) if p is not None]

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict | None:
        if market_type != "perp":
            raise RuntimeError("ondo live position query only supports perp")
        wanted = ondo_symbol(ondo_market_name(symbol))
        return next((p for p in await self.list_open_positions() if p["symbol"] == wanted), None)

    async def get_available_margin(self) -> Decimal:
        balance = await self._call("GET", "/v1/perps/balance", label="balance")
        return Decimal(str((balance or {}).get("availableMargin") or "0"))

    async def ensure_leverage(self, symbol: str) -> None:
        """Ondo is cross margin only; leverage is set per market once per process."""
        market = await self._market(symbol)
        if self.skip_margin_setup or market["market"] in self._leverage_set:
            return
        await self._call("POST", "/v1/perps/leverage", label="setLeverage",
                         body={"market": market["market"], "leverage": str(int(self.leverage))})
        self._leverage_set.add(market["market"])

    ensure_isolated_margin = ensure_leverage

    async def close_position(self, *, venue: str, symbol: str, side: str, quantity: str, market_type: str,
                             **kwargs) -> dict:
        if market_type != "perp":
            raise RuntimeError("ondo spot emergency close is not supported")
        return await self.place_market_order(symbol=symbol, side=side, amount=quantity, clip_usd=0,
                                             reduce_only=True)

    async def add_isolated_margin(self, *, venue: str, symbol: str, side: str, amount_usd: float, **kwargs) -> dict:
        raise RuntimeError("ondo is cross margin only; isolated margin top-ups do not apply")
