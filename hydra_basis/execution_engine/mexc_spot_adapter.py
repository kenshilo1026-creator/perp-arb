from __future__ import annotations

import hashlib
import hmac
import os
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from urllib.parse import urlencode

import aiohttp

from hydra_basis.execution_engine.mexc_depth_stream import MexcSpotDepthStream

from hydra_basis.symbol_mapping import canonicalize_symbol, venue_symbol
from hydra_basis.execution_engine.order_fill import poll_until_filled
from hydra_basis.execution_engine.hedge_safety import wait_for_terminal_order, terminal_fill_quantity, race_order_confirmation
from hydra_basis.execution_engine.order_stream import OrderEventStream, OrderStreamError, decode_mexc_order, MEXC_ORDER_CHANNEL


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


class MexcSpotExecutionAdapter:
    BASE_URL = "https://api.mexc.com"
    DEFAULT_RECV_WINDOW_MS = 60000

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
            else os.getenv("MEXC_SPOT_API_KEY", "") or os.getenv("MEXC_API_KEY", "")
        )
        self.api_secret = (
            api_secret
            if api_secret is not None
            else os.getenv("MEXC_SPOT_API_SECRET", "") or os.getenv("MEXC_API_SECRET", "")
        )
        self.recv_window_ms = recv_window_ms
        self._session: aiohttp.ClientSession | None = None
        self._hedge_depth = None
        self._fill_stream: OrderEventStream | None = None
        self._symbol_rules = {}
        self._listen_key: str | None = None

    @asynccontextmanager
    async def _request_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(keepalive_timeout=120))
        yield self._session

    async def close(self) -> None:
        if self._hedge_depth is not None:
            await self._hedge_depth.close()
            self._hedge_depth = None
        if self._fill_stream is not None:
            await self._fill_stream.close()
            self._fill_stream = None
        if self._listen_key is not None:
            try:
                await self._user_stream_request("DELETE")
            except Exception:
                pass
            self._listen_key = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def warm_up(self) -> None:
        """Open the public HTTPS connection before a maker order is submitted."""
        async with self._request_session() as session:
            async with session.get(f"{self.BASE_URL}/api/v3/ping") as response:
                response.raise_for_status()
                await response.read()

    async def prepare_hedge_quotes(self, symbol: str) -> None:
        if self._hedge_depth is not None:
            return
        async def connect():
            async with self._request_session() as session:
                return await session.ws_connect("wss://wbs-api.mexc.com/ws", heartbeat=20)

        async def fetch_snapshot():
            async with self._request_session() as session:
                async with session.get(f"{self.BASE_URL}/api/v3/depth",
                    params={"symbol": self._spot_symbol(symbol), "limit": 20},
                    timeout=aiohttp.ClientTimeout(total=0.95)) as response:
                    response.raise_for_status()
                    return await response.json()

        stream = MexcSpotDepthStream(connect=connect, symbol=self._spot_symbol(symbol), fetch_snapshot=fetch_snapshot)
        await stream.fresh_snapshot()
        self._hedge_depth = stream

    async def refresh_hedge_quote(self) -> dict:
        if self._hedge_depth is None:
            raise RuntimeError("MEXC hedge depth not started")
        return await self._hedge_depth.fresh_snapshot()

    async def hedge_quote(self) -> dict:
        if self._hedge_depth is None:
            raise RuntimeError("MEXC hedge depth not started")
        return self._hedge_depth.snapshot()

    async def wait_for_hedge_quote(self) -> None:
        if self._hedge_depth is None:
            raise RuntimeError("MEXC hedge depth not started")
        await self._hedge_depth.wait_for_update()

    async def _load_symbol_rules(self, symbol: str) -> dict[str, Decimal]:
        spot_symbol = self._spot_symbol(symbol)
        if spot_symbol in self._symbol_rules:
            return self._symbol_rules[spot_symbol]
        async with self._request_session() as session:
            async with session.get(
                f"{self.BASE_URL}/api/v3/exchangeInfo", params={"symbol": spot_symbol},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as response:
                data = await response.json(content_type=None)
                if response.status != 200:
                    raise RuntimeError(f"mexc spot exchangeInfo {response.status}: {data}")
        symbols = data.get("symbols") or []
        if len(symbols) != 1:
            raise RuntimeError(f"mexc spot symbol rules unavailable for {spot_symbol}")
        filters = {item.get("filterType"): item for item in symbols[0].get("filters") or []}
        price_filter = filters.get("PRICE_FILTER") or {}
        lot_filter = filters.get("LOT_SIZE") or {}
        info = symbols[0]
        def precision_step(key):
            value = int(info[key])
            if not 0 <= value <= 18:
                raise RuntimeError(f"invalid mexc spot precision: {key}")
            return Decimal(10) ** -value
        rules = {
            "tick": Decimal(str(price_filter["tickSize"])) if price_filter.get("tickSize") else precision_step("quotePrecision"),
            "step": Decimal(str(lot_filter["stepSize"])) if lot_filter.get("stepSize") else precision_step("baseAssetPrecision"),
            "min_qty": Decimal(str(lot_filter.get("minQty") or info.get("baseSizePrecision") or "0")),
            "min_notional": Decimal(str(info.get("quoteAmountPrecision") or "0")),
        }
        if any(not value.is_finite() or value <= 0 for value in (rules["tick"], rules["step"])):
            raise RuntimeError(f"invalid mexc spot symbol rules for {spot_symbol}")
        self._symbol_rules[spot_symbol] = rules
        return rules

    async def get_price_tick_size(self, symbol: str) -> str:
        return str((await self._load_symbol_rules(symbol))["tick"])

    async def get_order_execution(self, *, order_result: dict, symbol: str) -> dict:
        order_id = order_result.get("order_id") or order_result.get("orderId")
        if order_id is None:
            raw = order_result.get("raw")
            if isinstance(raw, dict):
                order_id = raw.get("orderId")
        if order_id is None:
            raise RuntimeError("mexc spot order status requires order_id")
        data = await self._get_order({"symbol": self._spot_symbol(symbol), "orderId": order_id})
        if data.get("orderId") is not None and str(data["orderId"]) != str(order_id):
            raise RuntimeError("mexc spot order status identity mismatch")
        if data.get("symbol") is not None and data["symbol"] != self._spot_symbol(symbol):
            raise RuntimeError("mexc spot order status symbol mismatch")
        try:
            filled = Decimal(str(data["executedQty"]))
            if not filled.is_finite() or filled < 0:
                raise ValueError("invalid cumulative fill")
        except (KeyError, ValueError, ArithmeticError) as exc:
            raise RuntimeError("mexc spot cumulative fill unavailable") from exc
        status = str(data.get("status") or "").upper()
        result = {
            "status": status,
            "terminal": status in {"FILLED", "CANCELED", "CANCELLED", "PARTIALLY_CANCELED", "REJECTED", "EXPIRED"},
            "filled_quantity": str(filled),
            "order_id": order_id,
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
            raise RuntimeError("MEXC hedge stream or order identity unavailable")
        return await self._fill_stream.wait_for_terminal(symbol=self._spot_symbol(symbol), order_id=order_id)

    async def _user_stream_request(self, method: str) -> dict:
        params = {"listenKey": self._listen_key} if method != "POST" else {}
        # Listen-key lifecycle endpoints require the same timestamp/HMAC
        # authentication as other private spot REST requests.
        signed_params = self._signed_order_params(params)
        async with self._request_session() as session:
            async with session.request(method, f"{self.BASE_URL}/api/v3/userDataStream",
                                       params=signed_params, headers=self._order_headers(),
                                       timeout=aiohttp.ClientTimeout(total=5)) as response:
                try:
                    data = await response.json(content_type=None)
                except ValueError:
                    data = None
                if response.status != 200 or (isinstance(data, dict) and data.get("code", 0) not in (0, "0")):
                    raise OrderStreamError(f"mexc_spot listen-key {method}", http_status=response.status,
                                           payload=data, secrets=(self.api_key, self.api_secret, self._listen_key))
                if not isinstance(data, dict) or (method == "POST" and not data.get("listenKey")):
                    raise OrderStreamError(f"mexc_spot listen-key {method}", reason="invalid response / missing listenKey")
                return data

    async def prepare_maker_fill_stream(self) -> None:
        if self._fill_stream is not None:
            return

        async def connect():
            data = await self._user_stream_request("POST")
            self._listen_key = str(data["listenKey"])
            async with self._request_session() as session:
                return await session.ws_connect("wss://wbs-api.mexc.com/ws",
                                                params={"listenKey": self._listen_key}, heartbeat=20)

        stream = OrderEventStream(venue="mexc_spot", connect=connect,
                                  renew=lambda: self._user_stream_request("PUT"),
                                  decode=decode_mexc_order, channel=MEXC_ORDER_CHANNEL)
        await stream.start()
        self._fill_stream = stream

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000)

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
            "MEXC spot API credentials missing: set MEXC_SPOT_API_KEY/"
            "MEXC_SPOT_API_SECRET or MEXC_API_KEY/MEXC_API_SECRET in .env"
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

    def _order_headers(self) -> dict[str, str]:
        self._ensure_credentials()
        return {
            "X-MEXC-APIKEY": self.api_key,
            "Content-Type": "application/json",
        }

    async def _post_order(self, params: dict) -> dict:
        signed_params = self._signed_order_params(params)
        headers = {
            **self._order_headers(),
        }
        async with self._request_session() as session:
            async with session.post(
                f"{self.BASE_URL}/api/v3/order",
                params=signed_params,
                headers=headers,
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"mexc spot order {resp.status}: {data}")
                return data

    async def _get_order(self, params: dict) -> dict:
        signed_params = self._signed_order_params(params)
        async with self._request_session() as session:
            async with session.get(
                f"{self.BASE_URL}/api/v3/order",
                params=signed_params,
                headers=self._order_headers(),
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"mexc spot order status {resp.status}: {data}")
                return data

    async def _delete_order(self, params: dict) -> dict:
        signed_params = self._signed_order_params(params)
        async with self._request_session() as session:
            async with session.delete(
                f"{self.BASE_URL}/api/v3/order",
                params=signed_params,
                headers=self._order_headers(),
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    if (
                        data.get("code") in {-2011, "-2011"}
                        and "filled" in str(data.get("msg", "")).lower()
                    ):
                        return {
                            "orderId": params.get("orderId"),
                            "status": "FILLED",
                            "cancelRejectedAsFilled": True,
                            "rawCancelError": data,
                        }
                    raise RuntimeError(f"mexc spot cancel {resp.status}: {data}")
                return data

    async def _get_account(self) -> dict:
        signed_params = self._signed_order_params({})
        async with self._request_session() as session:
            async with session.get(
                f"{self.BASE_URL}/api/v3/account",
                params=signed_params,
                headers=self._order_headers(),
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"mexc spot account {resp.status}: {data}")
                return data

    def _spot_symbol(self, symbol: str) -> str:
        normalized = venue_symbol(symbol, venue="mexc_spot")
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
            raise RuntimeError("mexc spot limit order fill wait requires order_id")
        if self._fill_stream is not None:
            return await self._fill_stream.wait_for_fill(
                symbol=self._spot_symbol(symbol), order_id=order_id,
                fetch_status=lambda: self._get_order({"symbol": self._spot_symbol(symbol), "orderId": order_id}),
                timeout_seconds=timeout_seconds, poll_interval_seconds=poll_interval_seconds,
                timeout_message="mexc spot limit order fill timeout",
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
            timeout_message="mexc spot limit order fill timeout",
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
            raise RuntimeError("mexc spot cancel_order requires order_id")
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
            raise RuntimeError("mexc spot live position query only supports spot")
        asset = venue_symbol(symbol, venue="mexc_spot")
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
                "symbol": canonicalize_symbol(asset, venue="mexc_spot"),
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
                    "venue": "mexc_spot",
                    "symbol": canonicalize_symbol(asset, venue="mexc_spot"),
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
            raise RuntimeError("mexc spot adapter only supports spot emergency close")
        if side.strip().upper() != "SELL":
            raise RuntimeError("mexc spot emergency close only supports SELL for long spot positions")
        data = await self._post_order(
            {
                "symbol": self._spot_symbol(symbol),
                "side": "SELL",
                "type": "MARKET",
                "quantity": quantity,
            }
        )
        return {"ok": True, "order_id": data.get("orderId"), "raw": data}
