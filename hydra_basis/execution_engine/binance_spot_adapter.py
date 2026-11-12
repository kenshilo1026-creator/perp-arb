from __future__ import annotations

import asyncio
import uuid
import hashlib
import hmac
import os
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from urllib.parse import urlencode

import aiohttp

from hydra_basis.execution_engine.binance_spot_stream import BinanceSpotDepthStream, BinanceOrderEventStream

from hydra_basis.symbol_mapping import canonicalize_symbol, venue_symbol
from hydra_basis.execution_engine.order_fill import poll_until_filled
from hydra_basis.execution_engine.hedge_safety import wait_for_terminal_order, terminal_fill_quantity, race_order_confirmation
from hydra_basis.execution_engine.order_stream import OrderEventStream, OrderStreamError


def market_fill_has_average(data: dict) -> bool:
    if str(data.get("status", "")).upper() != "FILLED":
        return False
    try:
        quantity = Decimal(str(data.get("executedQty") or "0"))
        if not quantity.is_finite() or quantity <= 0:
            return False
        values = (Decimal(str(data.get(key) or "0"))
                  for key in ("avgPrice", "cummulativeQuoteQty", "cumulativeQuoteQty"))
        return any(value.is_finite() and value > 0 for value in values)
    except (ArithmeticError, ValueError, TypeError):
        # Optional fill metadata must not turn an accepted order into a retry.
        return False


class BinanceSpotAPIError(OrderStreamError):
    def __init__(self, method, path, status, payload):
        self.http_status = status
        self.code = payload.get("code") if isinstance(payload, dict) else None
        super().__init__(f"binance_spot {method} {path}", http_status=status,
                         payload={"code": self.code}, reason="request rejected; see Binance error code")


class BinanceSpotExecutionAdapter:
    BASE_URL = "https://api.binance.com"
    DEFAULT_RECV_WINDOW_MS = 5000

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        recv_window_ms: int = DEFAULT_RECV_WINDOW_MS,
    ) -> None:
        self.api_key = (
            api_key
            if api_key is not None
            else os.getenv("BINANCE_SPOT_API_KEY", "") or os.getenv("BINANCE_API_KEY", "")
        )
        self.api_secret = (
            api_secret
            if api_secret is not None
            else os.getenv("BINANCE_SPOT_API_SECRET", "") or os.getenv("BINANCE_API_SECRET", "")
        )
        self.recv_window_ms = recv_window_ms
        self._session: aiohttp.ClientSession | None = None
        self._hedge_depth = None
        self._fill_stream: OrderEventStream | None = None
        self._symbol_rules = {}
        self._clock_offset_ms = 0
        self._blocked_until = 0.0
        self._submitted_orders = {}
        self._fee_cache = {}
        self.last_base_asset_fee = Decimal(0)
        if not 1 <= recv_window_ms <= 60000:
            raise ValueError("Binance recvWindow must be 1..60000 ms")

    @asynccontextmanager
    async def _request_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(keepalive_timeout=120))
        yield self._session

    async def refresh_hedge_quote(self) -> dict:
        if self._hedge_depth is None:
            raise RuntimeError("BINANCE hedge depth not started")
        return await self._hedge_depth.fresh_snapshot()

    async def hedge_quote(self) -> dict:
        if self._hedge_depth is None:
            raise RuntimeError("BINANCE hedge depth not started")
        return self._hedge_depth.snapshot()

    async def wait_for_hedge_quote(self) -> None:
        if self._hedge_depth is None:
            raise RuntimeError("BINANCE hedge depth not started")
        await self._hedge_depth.wait_for_update()


    async def get_price_tick_size(self, symbol: str) -> str:
        return str((await self._load_symbol_rules(symbol))["tick"])

    async def get_order_execution(self, *, order_result: dict, symbol: str) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if order_id is None:
            raw = order_result.get("raw")
            if isinstance(raw, dict):
                order_id = raw.get("orderId")
        if order_id is None:
            raise RuntimeError("binance spot order status requires order_id")
        data = await self._get_order({"symbol": self._spot_symbol(symbol), "orderId": order_id})
        if (not str(order_id).startswith("pa-") and data.get("orderId") is not None and str(data["orderId"]) != str(order_id)):
            raise RuntimeError("binance spot order status identity mismatch")
        if data.get("symbol") is not None and data["symbol"] != self._spot_symbol(symbol):
            raise RuntimeError("binance spot order status symbol mismatch")
        try:
            filled = Decimal(str(data["executedQty"]))
            if not filled.is_finite() or filled < 0:
                raise ValueError("invalid cumulative fill")
        except (KeyError, ValueError, ArithmeticError) as exc:
            raise RuntimeError("binance spot cumulative fill unavailable") from exc
        status = str(data.get("status") or "").upper()
        if status == "EXPIRED_IN_MATCH":
            status = "EXPIRED"
        result = {
            "status": status,
            "terminal": status in {"FILLED", "CANCELED", "CANCELLED", "PARTIALLY_CANCELED", "REJECTED", "EXPIRED"},
            "filled_quantity": str(filled),
            "order_id": data.get("orderId", order_id),
            "raw": data,
        }
        if market_fill_has_average(data):
            average = Decimal(str(data.get("avgPrice") or "0"))
            if average <= 0:
                average = Decimal(str(
                    data.get("cummulativeQuoteQty") or data.get("cumulativeQuoteQty")
                )) / Decimal(str(data.get("executedQty")))
            result["avg_price"] = str(average)
        return result

    async def wait_for_hedge_terminal(self, *, order_result: dict, symbol: str) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId") or (order_result.get("raw") or {}).get("orderId")
        if self._fill_stream is None or order_id is None:
            raise RuntimeError("BINANCE hedge stream or order identity unavailable")
        return await self._fill_stream.wait_for_terminal(symbol=self._spot_symbol(symbol), order_id=order_id)

    def _sign(self, query_string: str) -> str:
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _ensure_credentials(self) -> None:
        if self.api_key and self.api_secret:
            return
        raise RuntimeError(
            "BINANCE spot API credentials missing: set BINANCE_SPOT_API_KEY/"
            "BINANCE_SPOT_API_SECRET or BINANCE_API_KEY/BINANCE_API_SECRET in .env"
        )

    def _signed_order_params(self, params: dict) -> dict:
        self._ensure_credentials()
        signed_params = dict(params)
        signed_params.setdefault("recvWindow", self.recv_window_ms)
        signed_params["timestamp"] = self._timestamp_ms()
        query = urlencode(signed_params)
        signature = self._sign(query)
        signed_params["signature"] = signature
        return signed_params

    def _spot_symbol(self, symbol: str) -> str:
        normalized = venue_symbol(symbol, venue="binance_spot")
        if normalized.endswith("USDT"):
            return normalized
        return f"{normalized}USDT"

    async def _enrich_order_with_status(self, *, symbol: str, order_data: dict) -> dict:
        order_id = order_data.get("orderId")
        if order_id is None:
            return order_data
        try:
            status = await self._get_order(
                {
                    "symbol": self._spot_symbol(symbol),
                    "orderId": order_id,
                }
            )
        except Exception:
            return order_data
        return {**order_data, **status}

    async def place_market_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float
    ) -> dict:
        self.validate_order_inputs(symbol=symbol, amount=amount, market=True)
        started = time.monotonic_ns()
        data = await self._post_order({
            "symbol": self._spot_symbol(symbol), "side": side.strip().upper(),
            "type": "MARKET", "quantity": amount,
        })
        submitted = time.monotonic_ns()
        needs_status = not market_fill_has_average(data)
        queried = False
        source, observed = "rest", None
        async def rest_status():
            nonlocal queried
            queried = True
            return await self._enrich_order_with_status(symbol=symbol, order_data=data), "rest", None

        def from_stream(status):
            return {**data, **status["raw"]}, "user_stream", status["observed_monotonic_ns"]

        async def stream_status():
            return from_stream(await self.wait_for_hedge_terminal(order_result=data, symbol=symbol))

        if needs_status:
            if self._fill_stream is not None and data.get("orderId") is not None:
                cached = self._fill_stream.cached_terminal(symbol=self._spot_symbol(symbol), order_id=data["orderId"])
                data, source, observed = from_stream(cached) if cached is not None else await race_order_confirmation(
                    rest_status, stream_status())
            else:
                data, source, observed = await rest_status()
        return {"ok": True, "order_id": data.get("orderId"), "raw": data, "source": source,
                **({"observed_monotonic_ns": observed} if observed is not None else {}),
                "request_timing": {"order_submit_started_ns": started,
                    "order_submit_ms": (submitted - started) / 1_000_000,
                    "post_order_status_ms": (time.monotonic_ns() - submitted) / 1_000_000 if needs_status else 0,
                    "status_query_performed": queried}}

    async def place_limit_order(
        self, *, symbol: str, side: str, amount: str, clip_usd: float, price: str, post_only: bool = False
    ) -> dict:
        self.validate_order_inputs(symbol=symbol, amount=amount, price=price, market=False)
        data = await self._post_order(
            {
                "symbol": self._spot_symbol(symbol),
                "side": side.strip().upper(),
                "type": "LIMIT_MAKER" if post_only else "LIMIT",
                **({} if post_only else {"timeInForce": "GTC"}),
                "quantity": amount,
                "price": price,
            }
        )
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}

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
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if order_id is None:
            raise RuntimeError("binance spot limit order fill wait requires order_id")
        if self._fill_stream is not None:
            return await self._fill_stream.wait_for_fill(
                symbol=self._spot_symbol(symbol), order_id=order_id,
                fetch_status=lambda: self._get_order({"symbol": self._spot_symbol(symbol), "orderId": order_id}),
                timeout_seconds=timeout_seconds, poll_interval_seconds=poll_interval_seconds,
                timeout_message="binance spot limit order fill timeout",
                return_on_partial_fill=allow_partial_fill,
            )
        return await poll_until_filled(
            fetch_status=lambda: self._get_order(
                {
                    "symbol": self._spot_symbol(symbol),
                    "orderId": order_id,
                }
            ),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            timeout_message="binance spot limit order fill timeout",
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
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if order_id is None:
            raw = order_result.get("raw")
            if isinstance(raw, dict):
                order_id = raw.get("orderId")
        if order_id is None:
            raise RuntimeError("binance spot cancel_order requires order_id")
        data = await self._delete_order(
            {
                "symbol": self._spot_symbol(symbol),
                "orderId": order_id,
            }
        )
        if data.get("cancelRejectedAsFilled"):
            data = await self._enrich_order_with_status(symbol=symbol, order_data=data)
        final = await wait_for_terminal_order(
            lambda: self.get_order_execution(order_result={"order_id": order_id}, symbol=symbol), initial=data,
        )
        return {"ok": True, "order_id": order_id, "raw": final,
                "terminal": True, "filled_quantity": str(terminal_fill_quantity(final))}

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict | None:
        if market_type != "spot":
            raise RuntimeError("binance spot live position query only supports spot")
        asset = self._spot_symbol(symbol)[:-4]
        account = await self._get_account()
        for item in account.get("balances", []):
            if str(item.get("asset", "")).strip().upper() != asset:
                continue
            free = Decimal(str(item.get("free", "0") or "0"))
            locked = Decimal(str(item.get("locked", "0") or "0"))
            quantity = free + locked
            if quantity <= 0:
                continue
            return {
                "symbol": canonicalize_symbol(asset, venue="binance_spot"),
                "market_type": "spot",
                "side": "LONG",
                "quantity": format(quantity.normalize(), "f"),
                "raw": item,
            }
        return None

    async def list_open_positions(self) -> list[dict]:
        account = await self._get_account()
        positions: list[dict] = []
        for item in account.get("balances", []):
            asset = str(item.get("asset", "")).strip().upper()
            if not asset or asset in {"USDT", "USDC", "USD"}:
                continue
            free = Decimal(str(item.get("free", "0") or "0"))
            locked = Decimal(str(item.get("locked", "0") or "0"))
            quantity = free + locked
            if quantity <= 0:
                continue
            positions.append(
                {
                    "venue": "binance_spot",
                    "symbol": canonicalize_symbol(asset, venue="binance_spot"),
                    "market_type": "spot",
                    "side": "LONG",
                    "quantity": format(quantity.normalize(), "f"),
                    "raw": item,
                }
            )
        return positions

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
        if market_type != "spot":
            raise RuntimeError("binance spot adapter only supports spot emergency close")
        if side.strip().upper() != "SELL":
            raise RuntimeError("binance spot emergency close only supports SELL for long spot positions")
        await self.prepare_execution(symbol)
        return await self.place_market_order(symbol=symbol, side="SELL", amount=quantity, clip_usd=0)

    async def close(self) -> None:
        for stream in (self._hedge_depth, self._fill_stream):
            if stream is not None:
                await stream.close()
        self._hedge_depth = self._fill_stream = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000) + self._clock_offset_ms

    def _order_headers(self) -> dict[str, str]:
        self._ensure_credentials()
        return {"X-MBX-APIKEY": self.api_key}

    async def _request(self, method, path, params=None, *, signed=True):
        if time.monotonic() < self._blocked_until:
            raise BinanceSpotAPIError(method, path, 429, {})
        values = self._signed_order_params(params or {}) if signed else (params or {})
        headers = self._order_headers() if signed else {}
        try:
            async with self._request_session() as session:
                async with session.request(method, f"{self.BASE_URL}{path}", params=values,
                        headers=headers, timeout=aiohttp.ClientTimeout(total=10), allow_redirects=False) as response:
                    if response.status in (418, 429):
                        try:
                            delay = float(response.headers.get("Retry-After", "60"))
                            if not 1 <= delay <= 86400:
                                delay = 60
                        except ValueError:
                            delay = 60
                        self._blocked_until = time.monotonic() + delay
                    try:
                        data = await response.json(content_type=None)
                    except (ValueError, aiohttp.ContentTypeError):
                        raise BinanceSpotAPIError(method, path, response.status, {}) from None
                    if response.status == 200 and path == "/api/v3/myTrades" and isinstance(data, list):
                        return data
                    if response.status != 200 or not isinstance(data, dict) or data.get("code", 0) not in (0, "0"):
                        raise BinanceSpotAPIError(method, path, response.status, data)
                    if data.get("status") == "EXPIRED_IN_MATCH":
                        data = {**data, "status": "EXPIRED"}
                    return data
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise OrderStreamError(f"binance_spot {method} {path}", reason=type(exc).__name__) from None

    async def warm_up(self) -> None:
        started = int(time.time() * 1000)
        data = await self._request("GET", "/api/v3/time", signed=False)
        ended = int(time.time() * 1000)
        self._clock_offset_ms = int(data["serverTime"]) - (started + ended) // 2

    async def prepare_execution(self, symbol):
        self._ensure_credentials()
        await self.warm_up()
        await self._load_symbol_rules(symbol)

    @staticmethod
    def _identity_params(params):
        values = dict(params)
        if str(values.get("orderId", "")).startswith("pa-"):
            values["origClientOrderId"] = values.pop("orderId")
        return values

    async def _post_order(self, params: dict) -> dict:
        if params.get("side") not in {"BUY", "SELL"}:
            raise ValueError("Binance side must be BUY or SELL")
        client_id = f"pa-{uuid.uuid4().hex}"
        values = {**params, "newClientOrderId": client_id, "newOrderRespType": "FULL"}
        try:
            data = await self._request("POST", "/api/v3/order", values)
            if (data.get("symbol") != params["symbol"] or data.get("clientOrderId") != client_id
                    or data.get("orderId") is None):
                raise RuntimeError("Binance submission acknowledgement identity unavailable")
            self._submitted_orders[str(data["orderId"])] = data["symbol"]
            return data
        except BaseException as exc:
            # Never retry a POST whose outcome may be unknown. The existing
            # executor can reconcile/cancel using this client-ID identity.
            if not isinstance(exc, BinanceSpotAPIError) or exc.http_status == 200 or exc.http_status >= 500 or exc.code in {-1006, -1007}:
                exc.order_result = {"ok": False, "order_id": client_id,
                                    "client_order_id": client_id,
                                    "raw": {"clientOrderId": client_id, "symbol": params["symbol"]}}
            raise

    async def _get_order(self, params: dict) -> dict:
        values = self._identity_params(params)
        data = await self._request("GET", "/api/v3/order", values)
        if (data.get("symbol") != values["symbol"]
                or ("origClientOrderId" in values and data.get("clientOrderId") != values["origClientOrderId"])
                or ("orderId" in values and str(data.get("orderId")) != str(values["orderId"]))):
            raise RuntimeError("binance spot order identity mismatch")
        self._submitted_orders[str(data["orderId"])] = data["symbol"]
        return data

    async def _delete_order(self, params: dict) -> dict:
        values = self._identity_params(params)
        try:
            data = await self._request("DELETE", "/api/v3/order", values)
            if (data.get("symbol") != values["symbol"]
                    or ("orderId" in values and str(data.get("orderId")) != str(values["orderId"]))
                    or ("origClientOrderId" in values and data.get("origClientOrderId", data.get("clientOrderId")) != values["origClientOrderId"])):
                raise RuntimeError("Binance cancellation identity mismatch")
            return data
        except BinanceSpotAPIError as exc:
            if exc.code != -2011:
                raise
            # Unknown order during cancel is not proof of a fill or cancellation.
            # Query the exact identity; only a proven terminal state is usable.
            state = await self._get_order(params)
            if terminal_fill_quantity(state) is None:
                raise exc
            return state

    async def _get_account(self) -> dict:
        return await self._request("GET", "/api/v3/account")

    async def execution_position_adjustment(self, symbol):
        """After hedging only: reconcile actual base-asset fees, never estimate them.

        A BUY executedQty is gross; balances are net if commission uses the base
        asset. Match only this adapter's orders, and require complete trade sums.
        """
        native = self._spot_symbol(symbol)
        total = Decimal(0)
        for order_id, order_symbol in list(self._submitted_orders.items()):
            if order_symbol != native:
                continue
            if order_id not in self._fee_cache:
                state = await self._get_order({"symbol": native, "orderId": order_id})
                filled = terminal_fill_quantity(state)
                if filled is None:
                    raise RuntimeError("Binance fee reconciliation requires a terminal order")
                fee, quantity, seen = Decimal(0), Decimal(0), set()
                if filled:
                    params = {"symbol": native, "orderId": order_id, "limit": 1000}
                    for _ in range(20):
                        trades = await self._request("GET", "/api/v3/myTrades", params)
                        if not isinstance(trades, list):
                            raise RuntimeError("Binance trade history unavailable")
                        for trade in trades:
                            if trade.get("symbol") != native or str(trade.get("orderId")) != order_id:
                                raise RuntimeError("Binance fee trade identity mismatch")
                            tid = int(trade["id"])
                            if tid in seen:
                                raise RuntimeError("Binance fee pagination repeated a trade")
                            seen.add(tid)
                            q, commission = Decimal(trade["qty"]), Decimal(trade["commission"])
                            if not q.is_finite() or not commission.is_finite() or q <= 0 or commission < 0:
                                raise RuntimeError("invalid Binance trade quantity/commission")
                            quantity += q
                            if trade["commissionAsset"] == native[:-4]:
                                fee += commission
                        if len(trades) < 1000 or quantity >= filled:
                            break
                        params["fromId"] = max(seen) + 1
                    if quantity != filled:
                        raise RuntimeError("Binance fee trade history does not cover confirmed fill")
                self._fee_cache[order_id] = fee
            total += self._fee_cache[order_id]
        self.last_base_asset_fee = total
        return -total

    def minimum_order_notional(self, symbol, *, market):
        filters = self._symbol_rules[self._spot_symbol(symbol)]["filters"]
        minimum = Decimal(0)
        for name, flag in (("MIN_NOTIONAL", "applyToMarket"), ("NOTIONAL", "applyMinToMarket")):
            f = filters.get(name, {})
            if not market or f.get(flag, False):
                minimum = max(minimum, Decimal(f.get("minNotional", "0")))
        return minimum

    async def _load_symbol_rules(self, symbol):
        native = self._spot_symbol(symbol)
        if native in self._symbol_rules:
            return self._symbol_rules[native]
        data = await self._request("GET", "/api/v3/exchangeInfo", {"symbol": native}, signed=False)
        rows = data.get("symbols", [])
        if len(rows) != 1 or rows[0].get("symbol") != native:
            raise RuntimeError("Binance spot symbol unavailable")
        info = rows[0]
        if (info.get("status") != "TRADING" or info.get("quoteAsset") != "USDT"
                or info.get("isSpotTradingAllowed") is False
                or not {"MARKET", "LIMIT", "LIMIT_MAKER"}.issubset(info.get("orderTypes", []))):
            raise RuntimeError("Binance symbol is not an enabled USDT spot market")
        filters = {f["filterType"]: f for f in info.get("filters", [])}
        rules = {"filters": filters, "tick": Decimal(filters["PRICE_FILTER"]["tickSize"]),
                 "step": Decimal(filters["LOT_SIZE"]["stepSize"])}
        if any(not rules[k].is_finite() or rules[k] <= 0 for k in ("tick", "step")):
            raise ValueError("invalid Binance symbol precision")
        self._symbol_rules[native] = rules
        return rules

    def validate_order_inputs(self, *, symbol, amount, market, price=None):
        # Metadata is prepared before either leg fills; no HTTP on the hedge path.
        rules = self._symbol_rules.get(self._spot_symbol(symbol))
        if rules is None:
            raise RuntimeError("Binance symbol must be prepared before submitting orders")
        quantity = Decimal(str(amount))
        if not quantity.is_finite() or quantity <= 0:
            raise ValueError("invalid Binance order quantity")
        filters = rules["filters"]
        for name in ("LOT_SIZE", "MARKET_LOT_SIZE") if market else ("LOT_SIZE",):
            f = filters.get(name, {})
            for field, fails in (("minQty", lambda x: quantity < x),
                                 ("maxQty", lambda x: quantity > x),
                                 ("stepSize", lambda x: quantity % x != 0)):
                value = Decimal(f.get(field, "0"))
                if not value.is_finite() or value < 0 or (value > 0 and fails(value)):
                    raise ValueError(f"Binance {name}.{field}: quantity is not tradable; no rounding performed")
        if price is None:
            return
        p = Decimal(str(price))
        if not p.is_finite() or p <= 0:
            raise ValueError("invalid Binance price")
        if not market:
            f = filters["PRICE_FILTER"]
            for field, fails in (("minPrice", lambda x: p < x), ("maxPrice", lambda x: p > x),
                                 ("tickSize", lambda x: p % x != 0)):
                value = Decimal(f.get(field, "0"))
                if value > 0 and fails(value):
                    raise ValueError(f"Binance PRICE_FILTER.{field}: invalid price")
        for name in ("MIN_NOTIONAL", "NOTIONAL"):
            f = filters.get(name, {})
            for field, flag, fails in (("minNotional", "applyMinToMarket" if name == "NOTIONAL" else "applyToMarket", lambda x: p * quantity < x),
                                       ("maxNotional", "applyMaxToMarket", lambda x: p * quantity > x)):
                value = Decimal(f.get(field, "0"))
                if value > 0 and (not market or f.get(flag, False)) and fails(value):
                    raise ValueError(f"Binance {name}.{field}: order notional outside exchange limits")

    async def prepare_hedge_quotes(self, symbol):
        if self._hedge_depth is not None:
            return
        native = self._spot_symbol(symbol)
        async def connect():
            async with self._request_session() as session:
                streams = f"{native.lower()}@depth20@100ms/{native.lower()}@bookTicker"
                return await session.ws_connect(f"wss://stream.binance.com:9443/stream?streams={streams}", heartbeat=20)
        stream = BinanceSpotDepthStream(connect=connect, symbol=native)
        await stream.fresh_snapshot()
        self._hedge_depth = stream

    def _subscription_request(self):
        self._ensure_credentials()
        params = {"apiKey": self.api_key, "recvWindow": self.recv_window_ms, "timestamp": self._timestamp_ms()}
        params["signature"] = self._sign(urlencode(sorted(params.items())))
        return {"id": "spot-orders", "method": "userDataStream.subscribe.signature", "params": params}

    async def prepare_maker_fill_stream(self):
        if self._fill_stream is not None:
            return
        async def connect():
            async with self._request_session() as session:
                return await session.ws_connect("wss://ws-api.binance.com:443/ws-api/v3", heartbeat=20)
        stream = BinanceOrderEventStream(connect=connect, subscription=self._subscription_request)
        await stream.start()
        self._fill_stream = stream
