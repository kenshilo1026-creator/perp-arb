from __future__ import annotations

import inspect
import time
from decimal import Decimal, ROUND_FLOOR


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

    async def _load_market_config(self, symbol: str):
        result = self._market_config_loader(symbol)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _load_orderbook(self, symbol: str):
        result = self._orderbook_loader(symbol)
        if inspect.isawaitable(result):
            return await result
        return result

    async def place_market_order(self, *, symbol: str, side: str, amount: str, clip_usd: float) -> dict[str, object]:
        quantity = Decimal(str(amount))
        market_index, base_amount_multiplier, price_multiplier = await self._load_market_config(symbol)
        orderbook = await self._load_orderbook(symbol)
        request = build_lighter_market_order_request(
            side=side,
            quantity=quantity,
            orderbook=orderbook,
            base_amount_multiplier=base_amount_multiplier,
            price_multiplier=price_multiplier,
            slippage_bps=self.slippage_bps,
            market_index=market_index,
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
            reduce_only=False,
            trigger_price=0,
        )
        if error is not None:
            raise RuntimeError(f"lighter create_order failed: {error}")
        return {
            "ok": True,
            "tx_hash": tx_hash,
            "client_order_index": request["client_order_index"],
        }
