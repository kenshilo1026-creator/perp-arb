from __future__ import annotations

import asyncio
import inspect
import json
import time
from decimal import Decimal
from pathlib import Path

import aiohttp

from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.hedge_safety import (
    position_quantity, execute_confirmed_market_order, terminal_fill_quantity,
)
from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.risk_management.models import PositionLeg
from hydra_basis.risk_management.registry import PositionRegistry
from hydra_basis.spread_strategy.core import (
    Config, Quote, State, entry_allowed, executable_prices, exit_allowed, number,
)


class QuoteSource:
    def __init__(self, config: Config, session: aiohttp.ClientSession):
        self.config, self.session = config, session

    async def pair(self, quantity: Decimal) -> dict[str, Quote]:
        # Variational quotes are size-tiered. Start with a small reference, then
        # fetch both sides again for the requested clip notional (not account size).
        references = await self._pair(1.0)
        ref_price = max((q.bid + q.ask) / 2 for q in references.values())
        return await self._pair(float(quantity * ref_price))

    async def _pair(self, clip_usd: float) -> dict[str, Quote]:
        async def fetch(venue):
            start = time.monotonic()
            book = await fetch_orderbook_snapshot(self.session, venue=venue,
                                                 symbol=self.config.symbol, clip_usd=clip_usd)
            now = int(time.time() * 1000)
            # Aster's legacy fetcher may put lastUpdateId in ts_ms: it is NOT a
            # timestamp. Variational public stats have no source timestamp.
            source = int(book.get("ts_ms") or 0) if venue == "hyperliquid" else None
            quote = Quote(number(book["bid"]), number(book["ask"]), now,
                          source or None, time.monotonic() - start)
            quote.validate(now_ms=now, config=self.config)
            return venue, quote
        return dict(await asyncio.gather(*(fetch(v) for v in self.config.venues)))


class PaperBroker:
    """Read live public quotes but simulate fills; never construct a live adapter."""
    def __init__(self, config: Config, source: QuoteSource):
        self.config, self.source = config, source

    async def quotes(self, quantity):
        return await self.source.pair(quantity)

    async def reconcile(self, state):
        pass

    async def sync_registry(self, state):
        pass

    async def execute(self, intent, quantity, state, force=False):
        books = await self.quotes(quantity)
        for quote in books.values():
            quote.validate(now_ms=int(time.time() * 1000), config=self.config)
        short, long = executable_prices(self.config, books, intent, maker=(
            not force and self.config.execution_method == "maker_taker"))
        allowed = entry_allowed(self.config, short, long) if intent == "entry" else (
            force or exit_allowed(self.config, state, short, long, quantity))
        if not allowed:
            return {"ok": True, "skipped": True, "reason": "spread_changed"}
        return {"ok": True, "hedge_verified": True, "quantity": str(quantity),
                "short_price": str(short), "long_price": str(long), "paper": True}


class GuardedMaker:
    """Recheck both quotes immediately before each limit submission.

    Once a maker has filled, the hedge must execute regardless of price; a late
    economic veto on that hedge would leave an unhedged position.
    """
    def __init__(self, adapter, broker, intent, quantity, state, force):
        self.adapter, self.broker = adapter, broker
        self.intent, self.quantity, self.state, self.force = intent, quantity, state, force
        self.last_result = None
        self.vetoed = False

    def __getattr__(self, name):
        return getattr(self.adapter, name)

    async def place_limit_order(self, **kwargs):
        books = await self.broker.quotes(self.quantity)
        for quote in books.values():
            quote.validate(now_ms=int(time.time() * 1000), config=self.broker.config)
        short, long = executable_prices(self.broker.config, books, self.intent, maker=True)
        permitted = entry_allowed(self.broker.config, short, long) if self.intent == "entry" else (
            self.force or exit_allowed(self.broker.config, self.state, short, long, self.quantity))
        if not permitted:
            self.vetoed = True
            raise RuntimeError("spread changed before maker dispatch; no new order permitted")
        kwargs["price"] = str(short if self.broker.config.maker_venue == self.broker.config.short_venue else long)
        self.last_result = await self.adapter.place_limit_order(**kwargs)
        return self.last_result


class LiveBroker(PaperBroker):
    def __init__(self, config: Config, source: QuoteSource, adapters: dict,
                 registry_path: Path):
        super().__init__(config, source)
        self.adapters, self.registry_path = adapters, registry_path

    def registry(self):
        # Unlike the general recovery helper, a live strategy cannot treat a
        # corrupt registry as an empty account and overwrite other strategies.
        if self.registry_path.exists():
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not isinstance(payload.get("legs"), list):
                raise RuntimeError("invalid position registry; manual recovery required")
        return PositionRegistry.load(self.registry_path)

    async def reconcile(self, state: State):
        positions = await asyncio.gather(*(position_quantity(self.adapters[v], self.config.symbol)
                                            for v in self.config.venues))
        expected = (-number(state.quantity), number(state.quantity))
        # Exclusive ownership of this venue-symbol pair. Do not adopt unrelated
        # positions, and do not hide dust/partial mismatches behind a % tolerance.
        if tuple(positions) != expected:
            raise RuntimeError(f"live position mismatch: expected {expected}, got {tuple(positions)}")
        registry = self.registry()
        for venue in self.config.venues:
            if any(leg.strategy_id != state.strategy_id for leg in
                   registry.open_legs_for_venue_symbol(venue=venue, symbol=self.config.symbol)):
                raise RuntimeError("another registered strategy owns this venue/symbol")
        # Require a known empty remote order set on startup and before each clip.
        # Existing adapters expose list_open_orders for all three venues.
        for venue in self.config.venues:
            adapter = self.adapters[venue]
            list_orders = getattr(adapter, "list_open_orders", None)
            if not callable(list_orders):
                raise RuntimeError(f"{venue} cannot reconcile open orders")
            orders = await list_orders(symbol=self.config.symbol)
            if not isinstance(orders, list) or orders:
                raise RuntimeError(f"{venue} has unresolved open orders for {self.config.symbol}")

    async def sync_registry(self, state):
        registry = self.registry()
        changed = False
        for venue, side in ((self.config.short_venue, "SHORT"), (self.config.long_venue, "LONG")):
            leg = PositionLeg(state.strategy_id, f"{state.strategy_id}:{venue}:{side.lower()}",
                              venue, self.config.symbol, "perp", side, state.quantity,
                              status="open" if number(state.quantity) > 0 else "closed")
            existing = next((item for item in registry.legs_for_strategy(state.strategy_id)
                             if item.leg_id == leg.leg_id), None)
            if existing is not None:
                # Preserve margin-topup history managed by the risk supervisor.
                leg.margin_topups = existing.margin_topups
                leg.last_margin_topup_ts_ms = existing.last_margin_topup_ts_ms
            if existing != leg and (existing is not None or number(state.quantity) > 0):
                registry.add_leg(leg)
                changed = True
        if changed:
            registry.save(self.registry_path)

    async def execute(self, intent, quantity, state, force=False):
        await self.reconcile(state)
        c = self.config
        # Prepare leverage/margin before checking the final economic quote.
        # Never change margin settings when reducing a position.
        if intent == "entry":
            for adapter in self.adapters.values():
                for name in ("ensure_isolated_margin", "ensure_leverage"):
                    setup = getattr(adapter, name, None)
                    if callable(setup):
                        await setup(c.symbol)
        books = await self.quotes(quantity)
        market = force or c.execution_method == "taker_taker"
        short, long = executable_prices(c, books, intent, maker=not market)
        if not (entry_allowed(c, short, long) if intent == "entry" else
                force or exit_allowed(c, state, short, long, quantity)):
            return {"ok": True, "skipped": True, "reason": "spread_changed"}
        side_by_venue = {c.short_venue: "SELL" if intent == "entry" else "BUY",
                         c.long_venue: "BUY" if intent == "entry" else "SELL"}
        maker = GuardedMaker(self.adapters[c.maker_venue], self, intent, quantity, state, force)
        taker = self.adapters[c.taker_venue]
        reduce_only = intent == "exit"
        # Close adapters must not repeat leverage/margin setup on reduce-only.
        previous_skip = {}
        if reduce_only:
            for venue, adapter in self.adapters.items():
                if hasattr(adapter, "skip_margin_setup"):
                    previous_skip[venue] = adapter.skip_margin_setup
                    adapter.skip_margin_setup = True
        clip_usd = float(quantity * max(short, long))
        # Do not prepare a Variational submit-only ticket with the requested
        # size: partial maker fills may require a smaller actual hedge size.
        try:
            if market:
                # Submit both legs only with captured baselines, then verify the
                # resulting positions. Unknown outcomes pause, never blind retry.
                baselines = await asyncio.gather(*(position_quantity(self.adapters[v], c.symbol)
                                                   for v in c.venues))
                results = await asyncio.gather(*(
                    execute_confirmed_market_order(self.adapters[v], symbol=c.symbol,
                        side=side_by_venue[v], quantity=quantity, clip_usd=clip_usd,
                        baseline=baseline, reduce_only=reduce_only, max_attempts=3)
                    for v, baseline in zip(c.venues, baselines)), return_exceptions=True)
                if any(isinstance(item, BaseException) for item in results):
                    raise RuntimeError("market pair incomplete or unresolved; manual reconciliation required")
                positions = await asyncio.gather(*(position_quantity(self.adapters[v], c.symbol) for v in c.venues))
                for current, baseline, venue in zip(positions, baselines, c.venues):
                    direction = 1 if side_by_venue[venue] == "BUY" else -1
                    if current != baseline + quantity * direction:
                        raise RuntimeError("market pair live position delta mismatch")
                averages = await asyncio.gather(*(confirmed_average(self.adapters[v], c.symbol, result, quantity)
                                                  for v, result in zip(c.venues, results)))
                if any(price is None for price in averages):
                    raise RuntimeError("market fills lack actual averages; manual reconciliation required")
                return {"ok": True, "hedge_verified": True, "quantity": str(quantity),
                        "short_price": str(averages[0]), "long_price": str(averages[1])}
            try:
                result = await execute_single_clip_with_sides(
                    symbol=c.symbol, quantity=quantity, clip_usd=clip_usd,
                    maker_venue=c.maker_venue, taker_venue=c.taker_venue,
                    maker_side=side_by_venue[c.maker_venue], taker_side=side_by_venue[c.taker_venue],
                    maker_adapter=maker, taker_adapter=taker,
                    maker_price=str(short if c.maker_venue == c.short_venue else long),
                    max_hedge_retries=0, max_maker_reprice_attempts=0,
                    state_machine=ExecutionStateMachine(), require_maker_fill_confirmation=True,
                    maker_fill_timeout_seconds=c.maker_timeout_seconds,
                    verify_hedge_fill=True, maker_reduce_only=reduce_only,
                    taker_reduce_only=reduce_only,
                )
            except Exception:
                # A known, terminal zero-fill maker can safely return to quote
                # monitoring. Unknown dispatch/cancellation outcomes stay paused.
                if maker.vetoed and maker.last_result is None:
                    await self.reconcile(state)
                    return {"ok": True, "skipped": True, "reason": "spread_changed"}
                query = getattr(self.adapters[c.maker_venue], "get_order_execution", None)
                if maker.last_result is not None and callable(query):
                    status = await query(order_result=maker.last_result, symbol=c.symbol)
                    if terminal_fill_quantity(status) == 0:
                        await self.reconcile(state)
                        return {"ok": True, "skipped": True, "reason": "maker_unfilled"}
                raise
        finally:
            for venue, skip in previous_skip.items():
                self.adapters[venue].skip_margin_setup = skip
        filled = number(result["executed_quantity"])
        positions = await asyncio.gather(*(position_quantity(self.adapters[v], c.symbol) for v in c.venues))
        target = number(state.quantity) + (filled if intent == "entry" else -filled)
        if tuple(positions) != (-target, target):
            raise RuntimeError("confirmed maker/hedge quantities differ from strategy live positions")
        # Never account using submitted prices or the executor's maker fallback.
        maker_result = result.get("maker_result") or {}
        maker_payload = {"order_id": maker_result.get("order_id") or maker_result.get("oid"),
                         "raw": result.get("maker_fill_result"), "fill_result": maker_result}
        maker_price = await confirmed_average(self.adapters[c.maker_venue], c.symbol, maker_payload, filled)
        taker_price = await confirmed_average(taker, c.symbol, result.get("hedge_result"), filled)
        if maker_price is None or taker_price is None:
            raise RuntimeError("fills confirmed but actual average prices unavailable; manual reconciliation required")
        prices = {c.maker_venue: maker_price, c.taker_venue: taker_price}
        return {"ok": result.get("ok", False), "hedge_verified": result.get("hedge_verified", False),
                "quantity": str(result["executed_quantity"]),
                "short_price": str(prices[c.short_venue]), "long_price": str(prices[c.long_venue])}

    async def close(self):
        for adapter in self.adapters.values():
            close = getattr(adapter, "close", None)
            if callable(close):
                result = close()
                if inspect.isawaitable(result):
                    await result


def build_adapters(config: Config, broker_url: str | None = None):
    # Lazy imports: a paper run never instantiates authenticated adapters.
    from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
    from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
    from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter
    factories = {
        "aster": lambda: AsterExecutionAdapter(leverage=config.leverage),
        "hyperliquid": lambda: HyperliquidExecutionAdapter(leverage=config.leverage),
        "variational": lambda: VariationalBrowserExecutionAdapter(
            broker_url=broker_url or "http://127.0.0.1:8768/",
            fill_timeout_seconds=config.maker_timeout_seconds),
    }
    return {venue: factories[venue]() for venue in config.venues}


def actual_average(payload) -> Decimal | None:
    """Recognize fill averages only; never use a submitted limit 'price'."""
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key.lower().replace("_", "") in {"avgprice", "averageprice", "avgpx", "fillprice", "executedprice"}:
                if value not in (None, "") and number(value) > 0:
                    return number(value)
        for key in ("raw", "details", "response", "data", "statuses", "filled", "fill_result"):
            price = actual_average(payload.get(key))
            if price is not None:
                return price
    if isinstance(payload, list):
        for item in payload:
            price = actual_average(item)
            if price is not None:
                return price
    return None


async def confirmed_average(adapter, symbol: str, payload: dict, quantity: Decimal) -> Decimal | None:
    attempts = payload.get("attempts") if isinstance(payload, dict) else None
    if attempts:
        total, quote = Decimal("0"), Decimal("0")
        for attempt in attempts:
            status = attempt.get("order_status") or attempt.get("result")
            qty = terminal_fill_quantity(status)
            if qty == 0:
                continue
            if qty is None:
                return None
            original = attempt.get("result") or {}
            price = await confirmed_average(adapter, symbol,
                {"order_id": original.get("order_id") or original.get("oid"),
                 "raw": status, "fill_result": original}, qty)
            if price is None:
                return None
            total += qty
            quote += qty * price
        return quote / total if total == quantity and total > 0 else None
    price = actual_average(payload)
    if price is not None:
        return price
    query = getattr(adapter, "get_fill_average_price", None)
    if callable(query):
        return await query(symbol=symbol, order_result=payload or {}, quantity=quantity)
    return None
