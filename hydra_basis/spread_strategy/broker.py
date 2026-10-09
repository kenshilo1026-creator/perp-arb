"""Venue-facing helpers: order status parsing, market/quote submission, paper venue."""
from __future__ import annotations

import asyncio
import itertools
import json
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from hydra_basis.execution_engine.hedge_safety import position_quantity
from hydra_basis.risk_management.models import PositionLeg
from hydra_basis.risk_management.registry import PositionRegistry
from hydra_basis.spread_strategy.core import ONE, ZERO, Config, State, number

STATUS_ALIASES = {"CANCELLED": "CANCELED"}
TERMINAL = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
FILL_KEYS = ("filled_quantity", "executedQty", "executed_qty", "filledQty", "filled_qty",
             "filledBaseAmount", "cumQty", "totalSz")
AVERAGE_KEYS = ("avg_price", "avgPrice", "averagePrice", "avgPx", "dealAvgPrice", "fill_price", "fillPrice")
MARGIN_ERROR = re.compile(r"margin|balance|insufficient|undercollateral", re.IGNORECASE)
RATE_LIMIT_ERROR = re.compile(r"\b429\b|-1003|too many|rate limit", re.IGNORECASE)
POST_ONLY_CROSS = re.compile(r"post only|post-only|post_only|would have immediately matched|would_cross|-5022",
                             re.IGNORECASE)


@dataclass(frozen=True)
class OrderStatus:
    filled: Decimal | None
    terminal: bool
    average: Decimal | None
    state: str


def _decimal(value) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


def _average(payload: dict) -> Decimal | None:
    average = next((value for key in AVERAGE_KEYS
                    if (value := _decimal(payload.get(key))) is not None and value > 0), None)
    if average is None:
        # Lighter orders report cumulative base and quote amounts instead of an average.
        base, quote = _decimal(payload.get("filled_base_amount")), _decimal(payload.get("filled_quote_amount"))
        if base and quote:
            average = quote / base
    return average


def parse_status(payload) -> OrderStatus:
    """Read an adapter order payload; only top-level fields, ``raw`` and a Variational fill."""
    if not isinstance(payload, dict):
        return OrderStatus(None, False, None, "UNKNOWN")
    status = str(payload.get("status") or payload.get("orderStatus") or "").upper()
    status = STATUS_ALIASES.get(status, status)
    filled = next((value for key in FILL_KEYS if (value := _decimal(payload.get(key))) is not None), None)
    average = _average(payload)
    if average is None and isinstance(payload.get("raw"), dict):
        average = _average(payload["raw"])
    fill = (payload.get("details") or {}).get("fill") if isinstance(payload.get("details"), dict) else None
    if isinstance(fill, dict) and filled is None:
        filled = _decimal(fill.get("filledBaseAmount"))
        quote_amount = _decimal(fill.get("filledQuoteAmount"))
        if average is None and quote_amount and filled:
            average = quote_amount / filled
    # Submission/cancel wrappers carry the venue's order payload under ``raw``.
    if not status and filled is None and payload.get("terminal") is None and isinstance(payload.get("raw"), dict):
        return parse_status(payload["raw"])
    terminal = payload.get("terminal") is True or status in TERMINAL
    if status in TERMINAL:
        state = status
    elif terminal:
        state = "FILLED" if filled else "CANCELED"
    else:
        state = "OPEN" if status or filled is not None else "UNKNOWN"
    return OrderStatus(filled, terminal, average, state)


def definitive_rejection(exc: BaseException, result) -> bool:
    """True only when the venue certainly did not accept the order."""
    if getattr(exc, "definitive", False):
        return True
    message = str(exc).lower()
    if re.search(r"aster order 4\d\d", message) or re.search(r"hyperliquid exchange 4\d\d", message):
        return True
    if "hyperliquid order error" in message or "hyperliquid order rejected" in message:
        return True
    # MEXC answers HTTP 200 with success=false for a rejected order; Lighter's signer
    # returns an error (instead of raising) when the transaction was not accepted.
    if re.search(r"mexc order (200|4\d\d):", message) or "lighter create_order failed" in message:
        return True
    # Arcus: an HTTP 4xx or an engine REJECTED status means no order exists.
    if re.search(r"arcus order 4\d\d", message) or "arcus order rejected" in message:
        return True
    if "no extension command client connected" in message:
        return True
    if isinstance(result, dict) and result.get("type") == "ORDER_RESULT" and result.get("ok") is False:
        details = result.get("details") if isinstance(result.get("details"), dict) else {}
        return not details.get("postSubmitAmbiguous")
    return False


@dataclass
class Execution:
    filled: Decimal
    average: Decimal | None
    state: str              # FILLED (possibly partial) | REJECTED | CANCELED | UNKNOWN
    remote: dict | None
    error: str | None = None


async def submit_market(adapter, *, symbol: str, side: str, quantity: Decimal, reduce_only: bool,
                        reference_price: Decimal, timeout_seconds: float,
                        poll_seconds: float = 0.5) -> Execution:
    """Submit one IOC/market order and settle it; never resubmit an unknown outcome."""
    direction = ONE if side == "BUY" else -ONE
    query = getattr(adapter, "get_order_execution", None)
    # Venues without an order query (Variational) settle from the position delta.
    baseline = None if callable(query) else await position_quantity(adapter, symbol)
    kwargs = dict(symbol=symbol, side=side, amount=format(quantity.normalize(), "f"),
                  clip_usd=float(quantity * reference_price))
    if reduce_only:
        kwargs["reduce_only"] = True
    error = None
    try:
        result = await adapter.place_market_order(**kwargs)
    except Exception as exc:
        result, error = getattr(exc, "order_result", None), str(exc)
        if definitive_rejection(exc, result):
            return Execution(ZERO, None, "REJECTED", result, error)
    deadline = time.monotonic() + timeout_seconds
    status = parse_status(result)
    while True:
        if status.terminal and status.filled is not None:
            filled, average = status.filled, status.average
            if baseline is not None:
                delta = (await position_quantity(adapter, symbol) - baseline) * direction
                if filled < delta <= quantity:
                    filled, average = delta, average
            if filled > quantity:
                return Execution(filled, average, "UNKNOWN", result, "fill exceeds order quantity")
            if filled > 0 and average is None:
                average = await _fill_average(adapter, symbol, result, filled)
            return Execution(filled, average, "FILLED" if filled > 0 else status.state, result, error)
        if baseline is not None:
            try:
                delta = (await position_quantity(adapter, symbol) - baseline) * direction
                if delta == quantity:
                    return Execution(delta, status.average, "FILLED", result, error)
            except Exception as exc:
                error = str(exc)
        if time.monotonic() >= deadline:
            return Execution(status.filled or ZERO, status.average, "UNKNOWN", result,
                             error or "order outcome unresolved before timeout")
        await asyncio.sleep(poll_seconds)
        if callable(query) and isinstance(result, dict) and any(
                result.get(key) is not None for key in ("order_id", "orderId", "client_order_index")):
            try:
                status = parse_status(await query(order_result=result, symbol=symbol))
            except Exception as exc:
                error = str(exc)


async def _fill_average(adapter, symbol: str, result, quantity: Decimal) -> Decimal | None:
    query = getattr(adapter, "get_fill_average_price", None)
    if not callable(query) or not isinstance(result, dict):
        return None
    try:
        return await query(symbol=symbol, order_result=result, quantity=quantity)
    except Exception:
        return None


async def place_quote(adapter, *, symbol: str, side: str, quantity: Decimal, price: Decimal,
                      reduce_only: bool) -> dict:
    return await adapter.place_limit_order(
        symbol=symbol, side=side, amount=format(quantity.normalize(), "f"),
        clip_usd=float(quantity * price), price=format(price.normalize(), "f"),
        reduce_only=reduce_only, post_only=True)


# ---------------------------------------------------------------------------
# Paper venue
# ---------------------------------------------------------------------------

class OrderRejected(RuntimeError):
    """Raised before anything reaches a venue: the order certainly does not exist."""
    definitive = True


class PaperRejection(OrderRejected):
    pass


class PaperVenue:
    """Simulated venue on live public quotes.

    Market orders fill in full at the current top of book. A resting post-only
    quote fills at its limit price once the opposite best price reaches it.
    Neither models depth, queue position or latency: paper fills are not
    evidence of realizable profit.
    """
    _ids = itertools.count(1)

    def __init__(self, venue: str, feed):
        self.venue, self.feed = venue, feed
        self.position = ZERO
        self.orders: dict[str, dict] = {}

    def _book(self):
        book = self.feed.fresh(self.venue)
        if book is None:
            raise PaperRejection(f"paper {self.venue}: no fresh quote")
        return book

    def _fill(self, side: str, quantity: Decimal):
        self.position += quantity if side == "BUY" else -quantity

    def _check_reduce_only(self, side: str, quantity: Decimal, reduce_only: bool):
        if not reduce_only:
            return
        reduces = (self.position > 0 and side == "SELL") or (self.position < 0 and side == "BUY")
        if not reduces or quantity > abs(self.position):
            raise PaperRejection(f"paper {self.venue}: reduce-only order would increase position")

    def _match(self):
        book = self.feed.fresh(self.venue)
        if book is None:
            return
        for order in self.orders.values():
            if order["status"] != "NEW":
                continue
            price = order["price"]
            if (order["side"] == "BUY" and book.ask <= price) or (order["side"] == "SELL" and book.bid >= price):
                self._fill(order["side"], order["quantity"])
                order.update(status="FILLED", executedQty=order["quantity"])

    def _status(self, order_id: str) -> dict:
        order = self.orders[order_id]
        return {"order_id": order_id, "status": order["status"],
                "executedQty": str(order["executedQty"]), "avgPrice": str(order["price"])}

    async def place_limit_order(self, *, symbol, side, amount, clip_usd, price, reduce_only=False,
                                post_only=False):
        book, quantity, price = self._book(), number(amount), number(price)
        self._check_reduce_only(side, quantity, reduce_only)
        order_id = f"paper-{next(self._ids)}"
        crosses = (side == "BUY" and price >= book.ask) or (side == "SELL" and price <= book.bid)
        self.orders[order_id] = {"side": side, "quantity": quantity, "price": price, "executedQty": ZERO,
                                 "status": "EXPIRED" if post_only and crosses else "NEW"}
        return {"ok": True, "order_id": order_id, "raw": self._status(order_id)}

    async def place_market_order(self, *, symbol, side, amount, clip_usd, reduce_only=False):
        book, quantity = self._book(), number(amount)
        self._check_reduce_only(side, quantity, reduce_only)
        self._fill(side, quantity)
        price = book.ask if side == "BUY" else book.bid
        return {"ok": True, "terminal": True, "order_id": f"paper-{next(self._ids)}",
                "filled_quantity": str(quantity), "avg_price": str(price)}

    async def get_order_execution(self, *, order_result, symbol):
        self._match()
        return self._status(order_result["order_id"])

    async def cancel_order(self, *, order_result, symbol, side, amount):
        self._match()
        order = self.orders[order_result["order_id"]]
        if order["status"] == "NEW":
            order["status"] = "CANCELED"
        return {"ok": True, "raw": self._status(order_result["order_id"])}

    async def get_open_position(self, *, symbol, market_type):
        if self.position == 0:
            return None
        return {"side": "LONG" if self.position > 0 else "SHORT", "quantity": str(abs(self.position))}


# ---------------------------------------------------------------------------
# Live adapters and position registry
# ---------------------------------------------------------------------------

class MexcUnits:
    """Strategy quantities are base units; MEXC orders and positions count contracts.

    Kept local to this strategy: other project flows call the MEXC adapter directly.
    """

    def __init__(self, adapter, contract_size: Decimal):
        if contract_size <= 0:
            raise ValueError("mexc contract size must be positive")
        self.adapter, self.contract_size = adapter, contract_size

    def _contracts(self, amount) -> str:
        contracts = number(amount) / self.contract_size
        if contracts != contracts.to_integral_value() or contracts <= 0:
            raise OrderRejected(f"mexc quantity {amount} is not a whole number of "
                                 f"{self.contract_size}-unit contracts")
        return str(int(contracts))

    def _base(self, payload):
        if not isinstance(payload, dict):
            return payload
        payload = dict(payload)
        for key in ("filled_quantity", "dealVol"):
            if payload.get(key) is not None:
                payload[key] = str(number(payload[key]) * self.contract_size)
        if isinstance(payload.get("raw"), dict):
            payload["raw"] = self._base(payload["raw"])
        return payload

    async def place_market_order(self, *, symbol, side, amount, clip_usd, reduce_only=False):
        return await self.adapter.place_market_order(symbol=symbol, side=side, amount=self._contracts(amount),
                                                     clip_usd=clip_usd, reduce_only=reduce_only)

    async def place_limit_order(self, *, symbol, side, amount, clip_usd, price, reduce_only=False,
                                post_only=False):
        return await self.adapter.place_limit_order(symbol=symbol, side=side, amount=self._contracts(amount),
                                                    clip_usd=clip_usd, price=price, reduce_only=reduce_only,
                                                    post_only=post_only)

    async def get_order_execution(self, *, order_result, symbol):
        return self._base(await self.adapter.get_order_execution(order_result=order_result, symbol=symbol))

    async def cancel_order(self, *, order_result, symbol, side, amount):
        return self._base(await self.adapter.cancel_order(order_result=order_result, symbol=symbol, side=side,
                                                          amount=self._contracts(amount)))

    async def get_open_position(self, *, symbol, market_type):
        position = await self.adapter.get_open_position(symbol=symbol, market_type=market_type)
        if position is None:
            return None
        return {**position, "quantity": str(number(position["quantity"]) * self.contract_size)}

    async def get_available_margin(self):
        return await self.adapter.get_available_margin()


def build_venue_adapter(venue: str, *, leverage: int, order_timeout_seconds: float,
                        broker_url: str | None = None, orderbook_loader=None):
    """One authenticated adapter per venue. Lazy imports: paper runs never construct one."""
    if venue == "aster":
        from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
        return AsterExecutionAdapter(leverage=leverage)
    if venue == "hyperliquid":
        from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
        return HyperliquidExecutionAdapter(leverage=leverage)
    if venue == "arcus":
        from hydra_basis.execution_engine.arcus_adapter import ArcusExecutionAdapter
        return ArcusExecutionAdapter(leverage=leverage)
    if venue == "entropy":
        # HIP-3 dex "io" on the same Hyperliquid account and key.
        from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
        return HyperliquidExecutionAdapter(leverage=leverage, dex="io", venue_name="entropy")
    if venue == "lighter":
        from hydra_basis.execution_engine.lighter_adapter import LighterExecutionAdapter
        from hydra_basis.execution_engine.lighter_live import (
            build_lighter_client_factory_from_env, fetch_lighter_market_config, fetch_lighter_orderbook_live,
        )
        configs: dict[str, dict] = {}

        async def market_config(symbol):
            if symbol not in configs:
                configs[symbol] = await fetch_lighter_market_config(symbol)
            return configs[symbol]
        return LighterExecutionAdapter(
            signer_client_factory=build_lighter_client_factory_from_env(),
            market_config_loader=market_config,
            orderbook_loader=orderbook_loader or (lambda symbol: fetch_lighter_orderbook_live(symbol)),
            leverage=leverage)
    if venue == "mexc":
        from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter
        return MexcExecutionAdapter(leverage=leverage)
    if venue == "variational":
        from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter
        return VariationalBrowserExecutionAdapter(broker_url=broker_url or "http://127.0.0.1:8768/",
                                                  fill_timeout_seconds=order_timeout_seconds)
    raise ValueError(f"unsupported venue {venue}")


def feed_orderbook_loader(feed):
    """Lighter sizes its IOC limit from the book; use the strategy's live quote when fresh."""
    async def load(symbol):
        from hydra_basis.execution_engine.lighter_live import fetch_lighter_orderbook_live
        book = feed.fresh("lighter")
        if book is None:
            return await fetch_lighter_orderbook_live(symbol)
        return {"bid": float(book.bid), "ask": float(book.ask), "ts_ms": book.received_ms}
    return load


def strategy_adapter(venue: str, adapter, instrument):
    """Per-symbol view of a venue adapter in base units."""
    if venue == "mexc":
        return MexcUnits(adapter, instrument.contract_size)
    return adapter


def build_adapters(config: Config, instruments: dict, *, feed, broker_url: str | None = None):
    adapters = {}
    for venue in config.venues:
        adapter = build_venue_adapter(venue, leverage=config.leverage_of(venue),
                                      order_timeout_seconds=config.order_timeout_seconds,
                                      broker_url=broker_url, orderbook_loader=feed_orderbook_loader(feed))
        adapters[venue] = strategy_adapter(venue, adapter, instruments[venue])
    return adapters


def load_registry(path: Path) -> PositionRegistry:
    # A live strategy cannot treat a corrupt registry as empty and overwrite other strategies.
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or not isinstance(payload.get("legs"), list):
            raise RuntimeError("invalid position registry; manual recovery required")
    return PositionRegistry.load(path)


def assert_registry_owner(path: Path, config: Config, state: State):
    registry = load_registry(path)
    for venue in config.venues:
        if any(leg.strategy_id != state.strategy_id for leg in
               registry.open_legs_for_venue_symbol(venue=venue, symbol=config.symbol)):
            raise RuntimeError(f"another registered strategy owns {venue} {config.symbol}")


def registry_units(instruments: dict) -> dict[str, Decimal]:
    """Venues whose adapters report positions in contracts rather than base units."""
    return {venue: inst.contract_size for venue, inst in instruments.items() if inst.contract_size}


def sync_registry(path: Path, config: Config, state: State, contract_sizes: dict[str, Decimal] | None = None):
    """Publish the strategy's per-leg exposure for the existing risk supervisor.

    Quantities are written in each adapter's native unit (MEXC: contracts), because the
    supervisor reconciles them against ``get_open_position`` and closes with them.
    """
    registry = load_registry(path)
    changed = False
    for name in ("short", "long"):
        venue, held = config.venue_of(name), number(state.leg(name).quantity)
        side = "SHORT" if held < 0 or (held == 0 and name == "short") else "LONG"
        native = abs(held) / (contract_sizes or {}).get(venue, ONE)
        leg = PositionLeg(state.strategy_id, f"{state.strategy_id}:{venue}:{name}", venue, config.symbol,
                          "perp", side, format(native.normalize(), "f"),
                          status="open" if held != 0 else "closed")
        existing = next((item for item in registry.legs_for_strategy(state.strategy_id)
                         if item.leg_id == leg.leg_id), None)
        if existing is not None:
            # Preserve margin-topup history managed by the risk supervisor.
            leg.margin_topups = existing.margin_topups
            leg.last_margin_topup_ts_ms = existing.last_margin_topup_ts_ms
        if existing != leg and (existing is not None or held != 0):
            registry.add_leg(leg)
            changed = True
    if changed:
        registry.save(path)
