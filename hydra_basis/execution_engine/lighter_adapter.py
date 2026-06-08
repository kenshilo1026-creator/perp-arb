from __future__ import annotations

import inspect
import time
from decimal import Decimal, ROUND_FLOOR

from hydra_basis.execution_engine.order_fill import poll_until_filled


def compute_base_quantity_from_clip_usd(
    *,
    clip_usd: float,
    orderbook: dict[str, float | int],
) -> Decimal:
    bid = Decimal(str(orderbook["bid"]))
    ask = Decimal(str(orderbook["ask"]))
    mid = (bid + ask) / Decimal("2")
    if mid <= 0:
        raise RuntimeError("invalid lighter mid price")
    return (Decimal(str(clip_usd)) / mid).quantize(Decimal("0.00000001"))


def build_lighter_market_order_request(
    *,
    side: str,
    quantity: Decimal,
    orderbook: dict[str, float | int],
    base_amount_multiplier: int,
    price_multiplier: int,
    slippage_bps: float,
    market_index: int,
    client_order_index: int | None = None,
    min_base_amount: Decimal | None = None,
    min_quote_amount: Decimal | None = None,
) -> dict[str, int | bool]:
    side_normalized = side.strip().lower()
    if side_normalized not in {"buy", "sell"}:
        raise RuntimeError(f"unsupported lighter side: {side}")

    bid = Decimal(str(orderbook["bid"]))
    ask = Decimal(str(orderbook["ask"]))
    slippage = Decimal(str(slippage_bps)) / Decimal("10000")
    if side_normalized == "buy":
        price = ask * (Decimal("1") + slippage)
        is_ask = False
    else:
        price = bid * (Decimal("1") - slippage)
        is_ask = True

    notional = quantity * ((bid + ask) / Decimal("2"))
    if min_base_amount is not None and quantity < min_base_amount:
        raise RuntimeError(
            f"lighter order quantity={quantity} below min_base_amount={min_base_amount}"
        )
    if min_quote_amount is not None and notional < min_quote_amount:
        raise RuntimeError(
            f"lighter order notional={notional:.6f} below min_quote_amount={min_quote_amount}"
        )

    base_amount = int(
        (quantity * Decimal(str(base_amount_multiplier))).to_integral_value(rounding=ROUND_FLOOR)
    )
    price_i = int((price * Decimal(str(price_multiplier))).to_integral_value(rounding=ROUND_FLOOR))
    if base_amount <= 0:
        raise RuntimeError("lighter base amount rounds to zero")

    return {
        "market_index": market_index,
        "client_order_index": client_order_index if client_order_index is not None else int(time.time() * 1000),
        "base_amount": base_amount,
        "price": price_i,
        "is_ask": is_ask,
    }


def build_lighter_limit_order_request(
    *,
    side: str,
    quantity: Decimal,
    price: Decimal,
    base_amount_multiplier: int,
    price_multiplier: int,
    market_index: int,
    client_order_index: int | None = None,
    min_base_amount: Decimal | None = None,
    min_quote_amount: Decimal | None = None,
) -> dict[str, int | bool]:
    side_normalized = side.strip().lower()
    if side_normalized not in {"buy", "sell"}:
        raise RuntimeError(f"unsupported lighter side: {side}")
    if price <= 0:
        raise RuntimeError("lighter limit price must be positive")

    notional = quantity * price
    if min_base_amount is not None and quantity < min_base_amount:
        raise RuntimeError(
            f"lighter order quantity={quantity} below min_base_amount={min_base_amount}"
        )
    if min_quote_amount is not None and notional < min_quote_amount:
        raise RuntimeError(
            f"lighter order notional={notional:.6f} below min_quote_amount={min_quote_amount}"
        )

    base_amount = int(
        (quantity * Decimal(str(base_amount_multiplier))).to_integral_value(rounding=ROUND_FLOOR)
    )
    price_i = int((price * Decimal(str(price_multiplier))).to_integral_value(rounding=ROUND_FLOOR))
    if base_amount <= 0:
        raise RuntimeError("lighter base amount rounds to zero")
    if price_i <= 0:
        raise RuntimeError("lighter price rounds to zero")

    return {
        "market_index": market_index,
        "client_order_index": client_order_index if client_order_index is not None else int(time.time() * 1000),
        "base_amount": base_amount,
        "price": price_i,
        "is_ask": side_normalized == "sell",
    }


class LighterExecutionAdapter:
    def __init__(
        self,
        *,
        signer_client_factory,
        market_config_loader,
        orderbook_loader,
        slippage_bps: float = 100.0,
    ) -> None:
        self._signer_client_factory = signer_client_factory
        self._market_config_loader = market_config_loader
        self._orderbook_loader = orderbook_loader
        self.slippage_bps = slippage_bps
        self.client = None

    def _get_client(self):
        if self.client is None:
            self.client = self._signer_client_factory()
        return self.client

    async def close(self) -> None:
        if self.client is None:
            return
        for method_name in ("close", "close_client", "api_client.close"):
            target = self.client
            for part in method_name.split("."):
                target = getattr(target, part, None)
                if target is None:
                    break
            if target is None:
                continue
            result = target()
            if inspect.isawaitable(result):
                await result
            return

    async def _load_market_config(self, symbol: str):
        result = self._market_config_loader(symbol)
        if inspect.isawaitable(result):
            return await result
        return result

    def _normalize_market_config(self, config):
        if isinstance(config, dict):
            return config
        market_index, base_amount_multiplier, price_multiplier = config
        return {
            "market_index": market_index,
            "base_amount_multiplier": base_amount_multiplier,
            "price_multiplier": price_multiplier,
            "min_base_amount": None,
            "min_quote_amount": None,
        }

    async def _load_orderbook(self, symbol: str):
        result = self._orderbook_loader(symbol)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _get_order_status(self, client_order_index: object) -> dict[str, object]:
        client = self._get_client()
        for method_name in (
            "get_order",
            "get_order_by_client_order_index",
            "get_order_by_client_order_id",
        ):
            method = getattr(client, method_name, None)
            if method is None:
                continue
            result = method(client_order_index)
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, tuple):
                _response, payload, error = result if len(result) == 3 else (None, result[-1], None)
                if error is not None:
                    raise RuntimeError(f"lighter order status failed: {error}")
                result = payload
            if isinstance(result, dict):
                return result
            return {"status": str(result), "raw": result}
        raise RuntimeError("lighter client has no order status method")

    async def _submit_market_order(
        self,
        *,
        symbol: str,
        side: str,
        amount: str,
        reduce_only: bool,
    ) -> dict[str, object]:
        quantity = Decimal(str(amount))
        market_config = self._normalize_market_config(await self._load_market_config(symbol))
        orderbook = await self._load_orderbook(symbol)
        request = build_lighter_market_order_request(
            side=side,
            quantity=quantity,
            orderbook=orderbook,
            base_amount_multiplier=market_config["base_amount_multiplier"],
            price_multiplier=market_config["price_multiplier"],
            slippage_bps=self.slippage_bps,
            market_index=market_config["market_index"],
            min_base_amount=market_config.get("min_base_amount"),
            min_quote_amount=market_config.get("min_quote_amount"),
        )
        client = self._get_client()
        _tx, tx_hash, error = await client.create_order(
            market_index=request["market_index"],
            client_order_index=request["client_order_index"],
            base_amount=request["base_amount"],
            price=request["price"],
            is_ask=request["is_ask"],
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            reduce_only=reduce_only,
            trigger_price=0,
        )
        if error is not None:
            raise RuntimeError(f"lighter create_order failed: {error}")
        return {
            "ok": True,
            "tx_hash": tx_hash,
            "client_order_index": request["client_order_index"],
        }

    async def _submit_limit_order(
        self,
        *,
        symbol: str,
        side: str,
        amount: str,
        price: str,
        reduce_only: bool,
    ) -> dict[str, object]:
        quantity = Decimal(str(amount))
        limit_price = Decimal(str(price))
        market_config = self._normalize_market_config(await self._load_market_config(symbol))
        request = build_lighter_limit_order_request(
            side=side,
            quantity=quantity,
            price=limit_price,
            base_amount_multiplier=market_config["base_amount_multiplier"],
            price_multiplier=market_config["price_multiplier"],
            market_index=market_config["market_index"],
            min_base_amount=market_config.get("min_base_amount"),
            min_quote_amount=market_config.get("min_quote_amount"),
        )
        client = self._get_client()
        _tx, tx_hash, error = await client.create_order(
            market_index=request["market_index"],
            client_order_index=request["client_order_index"],
            base_amount=request["base_amount"],
            price=request["price"],
            is_ask=request["is_ask"],
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            reduce_only=reduce_only,
            trigger_price=0,
        )
        if error is not None:
            raise RuntimeError(f"lighter create_order failed: {error}")
        return {
            "ok": True,
            "tx_hash": tx_hash,
            "client_order_index": request["client_order_index"],
        }

    async def place_market_order(self, *, symbol: str, side: str, amount: str, clip_usd: float) -> dict[str, object]:
        return await self._submit_market_order(
            symbol=symbol,
            side=side,
            amount=amount,
            reduce_only=False,
        )

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str
    ) -> dict[str, object]:
        return await self._submit_limit_order(
            symbol=symbol,
            side=side,
            amount=amount,
            price=price,
            reduce_only=False,
        )

    async def wait_for_order_fill(
        self,
        *,
        order_result: dict,
        symbol: str,
        side: str,
        amount: str,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.5,
    ) -> dict[str, object]:
        client_order_index = order_result.get("client_order_index")
        if client_order_index is None:
            raise RuntimeError("lighter limit order fill wait requires client_order_index")
        return await poll_until_filled(
            fetch_status=lambda: self._get_order_status(client_order_index),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            timeout_message="lighter limit order fill timeout",
        )

    async def close_position(
        self,
        *,
        venue: str,
        symbol: str,
        side: str,
        quantity: str,
        market_type: str,
        **kwargs,
    ) -> dict[str, object]:
        if market_type != "perp":
            raise RuntimeError("lighter spot emergency close is not supported")
        return await self._submit_market_order(
            symbol=symbol,
            side=side,
            amount=quantity,
            reduce_only=True,
        )

    async def add_isolated_margin(
        self,
        *,
        venue: str,
        symbol: str,
        side: str,
        amount_usd: float,
        **kwargs,
    ) -> dict[str, object]:
        raise RuntimeError("lighter isolated margin top-up is not supported by this adapter")
