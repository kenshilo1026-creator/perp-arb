from __future__ import annotations

import asyncio
import inspect
import os
import time
from decimal import Decimal, ROUND_FLOOR

import aiohttp

from hydra_basis.adapters.base import fetch_json
from hydra_basis.execution_engine.order_fill import poll_until_filled
from hydra_basis.execution_engine.hedge_safety import terminal_fill_quantity, wait_for_terminal_order


LIGHTER_BASE_URL = "https://mainnet.zklighter.elliot.ai"


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


def _lighter_payload_items(payload) -> list[dict]:
    if isinstance(payload, tuple):
        _response, payload, error = payload if len(payload) == 3 else (None, payload[-1], None)
        if error is not None:
            raise RuntimeError(f"lighter positions failed: {error}")
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("positions", "data", "accounts"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]
    return []


def _lighter_position_symbol(item: dict) -> str:
    for key in ("symbol", "ticker", "market", "name"):
        value = item.get(key)
        if value:
            return str(value).strip().upper()
    return ""


def _normalize_lighter_symbol_label(symbol: str) -> str:
    normalized = symbol.strip().upper()
    for suffix in ("-PERP", "_PERP", "/USDC", "/USDT", "-USDC", "-USDT"):
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


def _lighter_position_market_id(item: dict) -> int | None:
    for key in ("market_id", "market_index", "marketIndex", "marketId"):
        value = item.get(key)
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _lighter_position_size(item: dict) -> Decimal:
    for key in (
        "position",
        "szi",
        "size",
        "position_size",
        "positionSize",
        "base_amount",
        "baseAmount",
        "base_asset_amount",
        "baseAssetAmount",
        "quantity",
    ):
        value = item.get(key)
        if value not in (None, ""):
            return Decimal(str(value))
    return Decimal("0")


def _normalize_lighter_live_position(
    *,
    symbol: str,
    item: dict,
    market_index: int | None = None,
) -> dict[str, object] | None:
    normalized_symbol = symbol.strip().upper()
    item_symbol = _lighter_position_symbol(item)
    if item_symbol:
        if _normalize_lighter_symbol_label(item_symbol) != _normalize_lighter_symbol_label(normalized_symbol):
            return None
    elif market_index is not None:
        item_market_id = _lighter_position_market_id(item)
        if item_market_id != market_index:
            return None
    size = _lighter_position_size(item)
    if size == 0:
        return None
    # Lighter returns position as absolute value; sign field: 1=long, -1=short
    try:
        sign = int(item.get("sign", 1))
    except (TypeError, ValueError):
        sign = 1
    signed_size = size * sign
    return {
        "venue": "lighter",
        "symbol": normalized_symbol,
        "market_type": "perp",
        "side": "LONG" if signed_size > 0 else "SHORT",
        "quantity": format(abs(size).normalize(), "f"),
        "raw": item,
    }


def _lighter_snapshot_position_items(snapshot: dict[str, object]) -> list[dict]:
    positions: list[dict] = []
    direct_positions = snapshot.get("positions")
    if isinstance(direct_positions, list):
        positions.extend(item for item in direct_positions if isinstance(item, dict))

    data = snapshot.get("data")
    if isinstance(data, dict):
        positions.extend(_lighter_snapshot_position_items(data))
    elif isinstance(data, list):
        positions.extend(item for item in data if isinstance(item, dict))

    accounts = snapshot.get("accounts")
    if isinstance(accounts, list):
        for account in accounts:
            if not isinstance(account, dict):
                continue
            account_positions = account.get("positions")
            if isinstance(account_positions, list):
                positions.extend(item for item in account_positions if isinstance(item, dict))
    return positions


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

    def _create_auth_token(self) -> str:
        client = self._get_client()
        token, error = client.create_auth_token_with_expiry(-1)
        if error is not None:
            raise RuntimeError(f"lighter auth token error: {error}")
        return token

    async def _fetch_account_snapshot(self) -> dict[str, object]:
        account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        async with aiohttp.ClientSession() as session:
            return await fetch_json(
                session,
                "GET",
                f"{LIGHTER_BASE_URL}/api/v1/account",
                params={"by": "index", "value": str(account_index)},
                headers={"accept": "application/json"},
            )

    async def _get_order_status(
        self, client_order_index: object, market_index: int,
        original_base_amount: int | None = None, base_amount_multiplier: int | None = None,
    ) -> dict[str, object]:
        try:
            async with aiohttp.ClientSession() as session:
                data = await fetch_json(
                    session, "GET", f"{LIGHTER_BASE_URL}/api/v1/accountOrders",
                    params={"account_index": int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0")),
                            "client_order_indexes": str(client_order_index)},
                    headers={"accept": "application/json", "Authorization": self._create_auth_token()},
                )
            for order in data.get("orders") or []:
                coi = order.get("client_order_index", order.get("client_order_id"))
                if (coi is None or int(coi) != int(client_order_index)
                        or _lighter_position_market_id(order) != market_index):
                    continue
                status = str(order.get("status", "UNKNOWN")).upper()
                if status.startswith(("CANCELED", "CANCELLED")):
                    status = "CANCELED"
                raw_filled = order.get("filled_base_amount", order.get("filledBaseAmount"))
                if raw_filled is None and order.get("remaining_base_amount") is not None:
                    initial = order.get("initial_base_amount")
                    if initial is None and original_base_amount is not None and base_amount_multiplier:
                        initial = Decimal(str(original_base_amount)) / Decimal(str(base_amount_multiplier))
                    if initial is not None:
                        raw_filled = Decimal(str(initial)) - Decimal(str(order["remaining_base_amount"]))
                if raw_filled is None:
                    return {"status": "UNKNOWN", "terminal": False}
                # REST amounts are token units; only submission integers are scaled.
                filled = Decimal(str(raw_filled))
                if not filled.is_finite() or filled < 0:
                    raise RuntimeError("invalid lighter cumulative fill")
                return {"status": status, "filled_quantity": str(filled),
                        "terminal": status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"},
                        "raw": order}
            # Missing orders / indexing delays are never evidence of a fill.
            return {"status": "UNKNOWN", "terminal": False}
        except Exception as exc:
            print(f"[lighter] order status unresolved: {exc}", flush=True)
            return {"status": "UNKNOWN", "terminal": False}

    async def get_order_execution(self, *, order_result: dict, symbol: str) -> dict:
        return await self._get_order_status(
            order_result["client_order_index"], int(order_result["market_index"]),
            original_base_amount=order_result.get("base_amount"),
            base_amount_multiplier=order_result.get("base_amount_multiplier"),
        )

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict[str, object] | None:
        if market_type != "perp":
            raise RuntimeError("lighter live position query only supports perp")
        market_index = None
        try:
            market_config = self._normalize_market_config(await self._load_market_config(symbol))
            market_index = int(market_config["market_index"])
        except Exception:
            market_index = None
        client = self._get_client()
        for method_name in (
            "get_positions",
            "get_account_positions",
            "get_positions_by_account",
            "get_account",
        ):
            method = getattr(client, method_name, None)
            if method is None:
                continue
            result = method()
            if inspect.isawaitable(result):
                result = await result
            for item in _lighter_payload_items(result):
                normalized = _normalize_lighter_live_position(
                    symbol=symbol,
                    item=item,
                    market_index=market_index,
                )
                if normalized is not None:
                    return normalized

        snapshot = await self._fetch_account_snapshot()
        snapshot_positions = _lighter_snapshot_position_items(snapshot)
        for item in snapshot_positions:
            normalized = _normalize_lighter_live_position(
                symbol=symbol,
                item=item,
                market_index=market_index,
            )
            if normalized is not None:
                return normalized
        if snapshot_positions or snapshot.get("accounts"):
            return None

        raise RuntimeError("lighter account snapshot returned no accounts")

    async def list_open_positions(self) -> list[dict[str, object]]:
        client = self._get_client()
        for method_name in (
            "get_positions",
            "get_account_positions",
            "get_positions_by_account",
            "get_account",
        ):
            method = getattr(client, method_name, None)
            if method is None:
                continue
            result = method()
            if inspect.isawaitable(result):
                result = await result
            positions = []
            for item in _lighter_payload_items(result):
                item_symbol = _lighter_position_symbol(item)
                if not item_symbol:
                    continue
                normalized = _normalize_lighter_live_position(symbol=item_symbol, item=item)
                if normalized is not None:
                    positions.append(normalized)
            if positions:
                return positions

        snapshot = await self._fetch_account_snapshot()
        positions: list[dict[str, object]] = []
        snapshot_positions = _lighter_snapshot_position_items(snapshot)
        for item in snapshot_positions:
            item_symbol = _lighter_position_symbol(item)
            if not item_symbol:
                continue
            normalized = _normalize_lighter_live_position(symbol=item_symbol, item=item)
            if normalized is not None:
                positions.append(normalized)
        if snapshot_positions or snapshot.get("accounts"):
            return positions

        raise RuntimeError("lighter account snapshot returned no accounts")

    async def _send_order(self, request: dict, market_config: dict, *, reduce_only: bool, immediate: bool) -> dict:
        client = self._get_client()
        identity = {"client_order_index": request["client_order_index"],
                    "market_index": request["market_index"], "base_amount": request["base_amount"],
                    "base_amount_multiplier": market_config["base_amount_multiplier"]}
        options = {"time_in_force": client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME}
        if immediate:
            options = {"time_in_force": client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                       "order_expiry": client.DEFAULT_IOC_EXPIRY}
        try:
            _tx, tx_hash, error = await client.create_order(
                **request, order_type=client.ORDER_TYPE_LIMIT, reduce_only=reduce_only,
                trigger_price=0, **options,
            )
            if error is not None:
                raise RuntimeError(f"lighter create_order failed: {error}")
        except Exception as exc:
            # Preserve the identity even if the response is lost. Query before retrying.
            exc.order_result = {"ok": False, **identity}
            raise
        return {"ok": True, "tx_hash": tx_hash, **identity}

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
        return await self._send_order(request, market_config, reduce_only=reduce_only, immediate=True)

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
        return await self._send_order(request, market_config, reduce_only=reduce_only, immediate=False)

    async def place_market_order(self, *, symbol: str, side: str, amount: str, clip_usd: float, reduce_only: bool = False) -> dict[str, object]:
        return await self._submit_market_order(
            symbol=symbol,
            side=side,
            amount=amount,
            reduce_only=reduce_only,
        )

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str, reduce_only: bool = False
    ) -> dict[str, object]:
        return await self._submit_limit_order(
            symbol=symbol,
            side=side,
            amount=amount,
            price=price,
            reduce_only=reduce_only,
        )

    async def wait_for_order_fill(
        self,
        *,
        order_result: dict,
        symbol: str,
        side: str,
        amount: str,
        timeout_seconds: float,
        poll_interval_seconds: float = 2.0,
        initial_delay_seconds: float = 3.0,
        allow_partial_fill: bool = False,
    ) -> dict[str, object]:
        client_order_index = order_result.get("client_order_index")
        if client_order_index is None:
            raise RuntimeError("lighter limit order fill wait requires client_order_index")
        market_index = order_result.get("market_index")
        if market_index is None:
            market_config = self._normalize_market_config(await self._load_market_config(symbol))
            market_index = market_config["market_index"]
        original_base_amount = order_result.get("base_amount")
        base_amount_multiplier = order_result.get("base_amount_multiplier")
        # wait for blockchain to index the order before first poll
        await asyncio.sleep(initial_delay_seconds)
        return await poll_until_filled(
            fetch_status=lambda: self._get_order_status(
                client_order_index,
                int(market_index),
                original_base_amount=original_base_amount,
                base_amount_multiplier=base_amount_multiplier,
            ),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            timeout_message="lighter limit order fill timeout",
            return_on_partial_fill=allow_partial_fill,
        )

    async def cancel_order(
        self,
        *,
        order_result: dict,
        symbol: str,
        side: str,
        amount: str,
    ) -> dict[str, object]:
        client_order_index = order_result.get("client_order_index")
        market_index = order_result.get("market_index")
        if client_order_index is None or market_index is None:
            raise RuntimeError("lighter cancel_order requires client_order_index and market_index in order_result")

        pre_cancel_status = await self.get_order_execution(order_result=order_result, symbol=symbol)
        if terminal_fill_quantity(pre_cancel_status) is not None:
            return {"ok": True, "already_gone": True, "raw": pre_cancel_status}
        client = self._get_client()
        # The signer accepts the client order index for cancellation.
        result = client.cancel_order(market_index=int(market_index), order_index=int(client_order_index))
        if inspect.isawaitable(result):
            result = await result
        _tx, tx_hash, error = result
        # An accepted cancel is not final. Include fills racing the cancellation.
        final = await wait_for_terminal_order(
            lambda: self.get_order_execution(order_result=order_result, symbol=symbol),
        )
        return {"ok": True, "tx_hash": tx_hash, "cancel_error": error, "raw": final}

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
