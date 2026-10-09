"""Strategy engine, modelled on Gate CrossEx's ``auto`` strategy loop.

Each tick: settle order updates, repair any hedge imbalance, then either
maintain resting post-only quotes (maker_taker) or fire paired market clips
(taker_taker). Entry and take-profit both stay armed: the strategy re-enters
whenever the entry spread reopens and runs until it is stopped.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from hydra_basis.spread_strategy.broker import (
    MARGIN_ERROR, POST_ONLY_CROSS, RATE_LIMIT_ERROR, Execution, definitive_rejection,
    parse_status, place_quote, submit_market,
)
from hydra_basis.spread_strategy.core import (
    EPSILON, ZERO, Config, Order, State, StateStore, entry_allowed, exit_allowed,
    imbalance, maker_boundary, matched_quantity, now_ms, number, spread_bps,
)
from hydra_basis.spread_strategy.feeds import MarketFeed
from hydra_basis.spread_strategy.instruments import Instrument, common_lot

MAX_FAILURES = 5
MAX_REPAIR_ATTEMPTS = 3
MARGIN_RESERVE = Decimal("1.10")
ACTIVE = {"RUNNING", "STOPPING"}
ENTRY_SIDES = {"short": "SELL", "long": "BUY"}


def opposite(side: str) -> str:
    return "BUY" if side == "SELL" else "SELL"


def leg_side(leg: str, intent: str) -> str:
    side = ENTRY_SIDES[leg]
    return side if intent == "entry" else opposite(side)


def fit_clip(config: Config, instruments: dict, books: dict, intent: str, cap: Decimal, short_leg, long_leg
             ) -> tuple[Decimal, tuple[Decimal, Decimal]] | None:
    """Largest quantity up to ``cap`` that BOTH books fill with every single fill passing the
    entry/exit gate (fees and reserves included), within every venue's order-size rules.

    Sized on the deepest level the clip touches (not the average), so the IOC limits at the profit
    boundary admit the whole clip: no partial fill by design, and no fill outside the gate.

    Entries never go below ``min_clip_notional_usd``; exits may, so a small remainder can still
    close. Returns (quantity, (short price, long price)) or None when nothing profitable fits.
    """
    lot = common_lot([instruments[venue] for venue in config.venues]) or Decimal("0.00000001")

    def snap(quantity: Decimal, rounding) -> Decimal:
        return (quantity / lot).to_integral_value(rounding=rounding) * lot

    def priced(quantity: Decimal):
        prices = tuple(books[config.venue_of(leg)].marginal(leg_side(leg, intent), quantity)
                       for leg in ("short", "long"))
        if None in prices:
            return None
        for leg, price in zip(("short", "long"), prices):
            if instruments[config.venue_of(leg)].size_error(quantity, price):
                return None
        passes = (entry_allowed(config, *prices) if intent == "entry"
                  else exit_allowed(config, short_leg, long_leg, *prices))
        return prices if passes else None

    top = snap(cap, ROUND_FLOOR)
    if top <= 0:
        return None
    prices = priced(top)
    if prices is not None:
        return top, prices
    reference = books[config.short_venue].bid
    floor_quantity = max([inst.min_size or ZERO for inst in instruments.values()]
                         + [(inst.min_notional or ZERO) / reference for inst in instruments.values()]
                         + ([config.min_clip_notional_usd / reference] if intent == "entry" else []))
    low = max(snap(floor_quantity, ROUND_CEILING), lot)
    if low >= top or priced(low) is None:
        return None
    high = top  # known to fail; the gate only gets harder as size grows
    while high - low > lot:
        middle = snap((low + high) / 2, ROUND_FLOOR)
        if middle <= low:
            break
        if priced(middle) is not None:
            low = middle
        else:
            high = middle
    return low, priced(low)


class Engine:
    def __init__(self, config: Config, state: State, store: StateStore, feed: MarketFeed,
                 adapters: dict, instruments: dict[str, Instrument], *, live: bool,
                 on_exposure=None, clock=now_ms, log=None):
        self.config, self.state, self.store, self.feed = config, state, store, feed
        self.adapters, self.instruments, self.live = adapters, instruments, live
        self.on_exposure = on_exposure
        self.clock = clock
        self.emit = log or (lambda payload: print(json.dumps(payload, default=str), flush=True))
        self.failure_count = 0
        self.cooldown_until = 0
        self.repair_attempts = 0
        self.last_repair_at = 0
        self.last_quote_at = 0
        self._dust_logged: Decimal | None = None
        self._thin_logged: tuple | None = None
        self.last_clip_at = 0
        self._polled_at: dict[str, int] = {}

    # ------------------------------------------------------------------ helpers

    def log(self, event: str, **fields):
        self.emit({"event": event, "ts_ms": self.clock(), **fields})

    def save(self):
        self.store.save(self.state)

    def quotes(self) -> dict[str, Order]:
        return {order.purpose.removeprefix("quote_"): order for order in self.state.open_orders()
                if order.purpose.startswith("quote_")}

    def non_quote_open_orders(self) -> list[Order]:
        return [order for order in self.state.open_orders() if not order.purpose.startswith("quote_")]

    def matched(self) -> Decimal:
        return matched_quantity(self.state.short, self.state.long)

    def imbalance(self) -> Decimal:
        return imbalance(self.state.short, self.state.long)

    def remaining_entry_capacity(self) -> Decimal:
        in_flight = sum((number(o.quantity) - number(o.executed_quantity)
                         for o in self.non_quote_open_orders()), ZERO)
        return max(ZERO, self.config.total_quantity - self.matched() - in_flight - abs(self.imbalance()))

    def executable_price(self, venue: str, side: str, quantity: Decimal | None = None) -> Decimal | None:
        """Top of book, or the average fill price for ``quantity`` across visible depth."""
        book = self.feed.fresh(venue)
        if book is None:
            return None
        return book.executable(side, quantity)

    def new_order(self, *, leg: str, side: str, purpose: str, quantity: Decimal, reduce_only: bool,
                  price: Decimal | None = None, reference: Decimal | None = None,
                  clip: str | None = None) -> Order:
        order = Order(id=uuid.uuid4().hex[:16], venue=self.config.venue_of(leg), leg=leg, side=side,
                      purpose=purpose, quantity=str(quantity), reduce_only=reduce_only,
                      price=None if price is None else str(price),
                      reference_price=None if reference is None else str(reference), clip=clip,
                      created_ms=self.clock(), updated_ms=self.clock())
        # Durable before dispatch: a crash leaves PENDING_SUBMIT, which refuses auto-restart.
        self.state.orders.append(order)
        self.save()
        return order

    def apply(self, order: Order, filled: Decimal | None, average: Decimal | None, state: str | None):
        """Record a cumulative fill update; exposure changes only by the new increment."""
        old = number(order.executed_quantity)
        changed = state is not None and state != order.state
        if filled is not None and filled > old:
            changed = True
            delta = filled - old
            old_average = number(order.average_price) if order.average_price else None
            if average is not None and (old == 0 or old_average is not None):
                price = (average * filled - (old_average or ZERO) * old) / delta
                order.average_price = str(average)
            else:
                # Post-only quotes fill at their limit; taker orders fall back to the decision quote.
                price = number(order.price or order.reference_price)
                order.average_price = str(((old_average or price) * old + price * delta) / filled)
                order.average_estimated = order.price is None
            self.state.leg(order.leg).apply(delta * order.signed_direction, price)
            role = "maker" if order.purpose.startswith("quote_") else "taker"
            fee = delta * price * number(self.config.fees[order.venue][role])
            self.state.fees_usd = str(number(self.state.fees_usd) + fee)
            order.executed_quantity = str(filled)
            self.log("fill", order=order.id, purpose=order.purpose, venue=order.venue, side=order.side,
                     quantity=str(delta), price=str(price), short=self.state.short.quantity,
                     long=self.state.long.quantity)
            if self.on_exposure is not None:
                self.on_exposure(self.state)
        if not changed:
            return
        if state is not None:
            order.state = state
        order.updated_ms = self.clock()
        self.save()

    def apply_execution(self, order: Order, result: Execution):
        order.remote, order.error = result.remote, result.error
        self.apply(order, result.filled, result.average, result.state)

    # ------------------------------------------------------------------ lifecycle

    async def start(self, *, resume: bool = False):
        s = self.state
        if s.status == "PAUSED" and not resume:
            raise RuntimeError(f"strategy is PAUSED ({s.reason}); review positions/orders, then rerun with --resume")
        if s.status == "STOPPED":
            return
        await self.resolve_open_orders()
        await self.preflight()
        if s.status == "PAUSED":
            s.status, s.reason = "RUNNING", None
            self.log("resumed")
        self.save()

    def settle_order(self, order_id: str, filled: Decimal, price: Decimal | None):
        """Record a manually verified outcome for an order the bot could not resolve."""
        order = next((item for item in self.state.orders if item.id == order_id), None)
        if order is None or order.terminal:
            raise ValueError(f"no unresolved order {order_id}")
        if not ZERO <= filled <= number(order.quantity):
            raise ValueError("settled quantity must be between 0 and the order quantity")
        if filled > 0 and price is None and not (order.price or order.reference_price):
            raise ValueError("a fill price is required for this order")
        order.error = f"manually settled: {filled}"
        self.apply(order, filled, price, "FILLED" if filled > 0 else "CANCELED")
        self.log("order_settled", order=order_id, filled=str(filled))

    async def resolve_open_orders(self):
        """Original prepareForLiveActivation: every remembered order must be proven terminal."""
        for order in self.state.open_orders():
            if order.state == "PENDING_SUBMIT" or not order.remote:
                raise RuntimeError(f"order {order.id} on {order.venue} may have been submitted without a "
                                   "recorded outcome; reconcile it manually before restarting")
            adapter = self.adapters[order.venue]
            if order.purpose.startswith("quote_"):
                if not await self.cancel_quote(order):
                    raise RuntimeError(f"quote {order.id} on {order.venue} could not be confirmed cancelled")
                continue
            query = getattr(adapter, "get_order_execution", None)
            status = parse_status(await query(order_result=order.remote, symbol=self.config.symbol)) \
                if callable(query) else None
            if status is None or not status.terminal:
                raise RuntimeError(f"order {order.id} on {order.venue} is unresolved; reconcile it manually")
            self.apply(order, status.filled, status.average, status.state)

    async def preflight(self):
        """Original validateStrategyOrderSizes + prepareStrategyMargin."""
        c = self.config
        books = self.feed.fresh_pair()
        if books is None:
            raise RuntimeError("market data unavailable for preflight")
        first = min(c.clip_quantity, c.total_quantity)
        remainder = c.total_quantity % c.clip_quantity
        clips = [first] + ([remainder] if remainder > EPSILON and remainder != first else [])
        for venue in c.venues:
            instrument = self.instruments[venue]
            if not instrument.validated:
                self.log("warning", message=f"{venue} publishes no lot/minimum rules; sizes not validated")
                continue
            for leg in ("short", "long"):
                if c.venue_of(leg) != venue:
                    continue
                price = self.executable_price(venue, leg_side(leg, "entry"))
                for quantity in clips:
                    error = instrument.size_error(quantity, price)
                    if error:
                        raise RuntimeError(f"order-size compliance check failed on {venue}: {error}")
        if not self.live:
            return
        capacity = max(ZERO, c.total_quantity - self.matched())
        for leg in ("short", "long"):
            venue = c.venue_of(leg)
            available_margin = getattr(self.adapters[venue], "get_available_margin", None)
            if not callable(available_margin):
                self.log("warning", message=f"{venue} margin preflight unavailable")
                continue
            price = self.executable_price(venue, leg_side(leg, "entry"))
            required = capacity * price / c.leverage_of(venue) * MARGIN_RESERVE
            available = number(await available_margin())
            if available < required:
                raise RuntimeError(f"insufficient {venue} margin: required {required:.2f}, available {available:.2f}")
            self.log("margin_preflight", venue=venue, required=f"{required:.2f}", available=f"{available:.2f}")

    async def step(self):
        s = self.state
        if s.status not in ACTIVE:
            return
        await self.refresh_orders()
        await self.ensure_hedged()
        if s.status == "RUNNING":
            self.check_stop_loss()
        if s.status == "STOPPING":
            await self.evaluate_stopping()
            return
        if s.status != "RUNNING" or self.clock() < self.cooldown_until:
            return
        if self.config.maker_taker:
            await self.evaluate_maker_quotes()
        else:
            await self.evaluate_taker_clip()

    async def shutdown(self):
        """Ctrl+C: cancel resting quotes and hedge any residual; positions stay open."""
        for order in self.quotes().values():
            await self.cancel_quote(order)
        for _ in range(MAX_REPAIR_ATTEMPTS):
            if abs(self.imbalance()) <= EPSILON or self.state.status not in ACTIVE:
                break
            self.last_repair_at = 0
            await self.ensure_hedged()
        self.save()

    async def pause(self, reason: str):
        """Original pause/finishQuiesce: stop trading and cancel resting quotes."""
        for order in self.quotes().values():
            await self.cancel_quote(order)
        self.state.status, self.state.reason = "PAUSED", reason
        residual = self.imbalance()
        unresolved = [order.id for order in self.state.open_orders()]
        self.log("paused", reason=reason, residual=str(residual), unresolved_orders=unresolved)
        self.save()

    # ------------------------------------------------------------------ order updates

    async def refresh_orders(self):
        now = self.clock()
        for order in self.state.open_orders():
            if order.state != "OPEN" or not order.remote:
                continue
            if now - self._polled_at.get(order.id, 0) < self.config.poll_seconds(order.venue) * 1000:
                continue
            self._polled_at[order.id] = now
            query = getattr(self.adapters[order.venue], "get_order_execution", None)
            if not callable(query):
                continue
            try:
                status = parse_status(await query(order_result=order.remote, symbol=self.config.symbol))
            except Exception as exc:
                self.log("warning", message=f"order status query failed for {order.id}: {str(exc)[:160]}")
                continue
            self.apply(order, status.filled, status.average, status.state if status.terminal else None)

    async def cancel_quote(self, order: Order) -> bool:
        try:
            result = await self.adapters[order.venue].cancel_order(
                order_result=order.remote, symbol=self.config.symbol, side=order.side, amount=order.quantity)
        except Exception as exc:
            self.log("warning", message=f"quote cancel unconfirmed for {order.id}: {str(exc)[:160]}")
            return False
        status = parse_status(result)
        self.apply(order, status.filled, status.average, status.state if status.terminal else None)
        return order.terminal

    # ------------------------------------------------------------------ taker_taker

    async def evaluate_taker_clip(self):
        """One clip per tick, re-priced from scratch every time: each clip is the largest size the
        books can fill profitably right now, so a thin book means smaller clips, not a bad fill."""
        c = self.config
        if abs(self.imbalance()) > EPSILON:
            return
        if self.clock() - self.last_clip_at < c.clip_interval_seconds * 1000:
            return  # let thin books refill between clips
        books = self.feed.fresh_pair()
        if books is None:
            return
        capacity = min(c.clip_quantity, self.remaining_entry_capacity())
        if capacity > EPSILON and await self.try_clip(books, "entry", capacity):
            return
        matched = self.matched()
        if matched > EPSILON:
            await self.try_clip(books, "exit", min(c.clip_quantity, matched))

    async def try_clip(self, books, intent: str, cap: Decimal) -> bool:
        c, s = self.config, self.state
        fit = fit_clip(c, self.instruments, books, intent, cap, s.short, s.long)
        top_prices = tuple(books[c.venue_of(leg)].executable(leg_side(leg, intent)) for leg in ("short", "long"))
        top_passes = (entry_allowed(c, *top_prices) if intent == "entry"
                      else exit_allowed(c, s.short, s.long, *top_prices))
        if fit is None:
            if top_passes:
                # The spread exists at the top of book but no size the venues accept clears it.
                minute = self.clock() // 60_000
                if self._thin_logged != (intent, minute):
                    self._thin_logged = (intent, minute)
                    self.log("depth_insufficient", intent=intent, cap=str(cap),
                             message="no order size the books can fill profitably; waiting")
            return False
        quantity, prices = fit
        full = self.clip_size(cap)
        if quantity < full:
            self.log("clip_resized", intent=intent, requested=str(full), quantity=str(quantity),
                     message="shrunk to the size the books fill profitably")
        await self.execute_taker_clip(intent, quantity, f"{intent} {prices[0]}/{prices[1]}", prices=prices)
        return True

    def clip_size(self, quantity: Decimal) -> Decimal:
        lot = common_lot([self.instruments[venue] for venue in self.config.venues])
        if not lot:
            return quantity
        return (quantity / lot).to_integral_value(rounding=ROUND_FLOOR) * lot

    def boundary_limits(self, intent: str, short_price: Decimal, long_price: Decimal) -> tuple[Decimal, Decimal]:
        """Worst acceptable IOC limit per leg: both legs filling at their limits still passes the
        entry/exit gate (fees and reserves included). The slack is split evenly between the legs."""
        c, s = self.config, self.state

        def allowed(k: Decimal) -> bool:
            if intent == "entry":
                return entry_allowed(c, short_price * (1 - k), long_price * (1 + k))
            return exit_allowed(c, s.short, s.long, short_price * (1 + k), long_price * (1 - k))

        low, high = Decimal(0), Decimal("0.05")
        if allowed(high):
            low = high
        elif allowed(low):
            for _ in range(40):
                middle = (low + high) / 2
                if allowed(middle):
                    low = middle
                else:
                    high = middle
        short_inst, long_inst = self.instruments[c.short_venue], self.instruments[c.long_venue]
        if intent == "entry":   # short SELLs (minimum price, round up), long BUYs (maximum price, round down)
            return (short_inst.round_price(short_price * (1 - low), "up"),
                    long_inst.round_price(long_price * (1 + low), "down"))
        return (short_inst.round_price(short_price * (1 + low), "down"),   # short buys back
                long_inst.round_price(long_price * (1 - low), "up"))        # long sells

    async def execute_taker_clip(self, intent: str, quantity: Decimal, condition: str, *, purpose: str | None = None,
                                 prices: tuple[Decimal, Decimal] | None = None):
        """Both legs at once. With ``prices`` (normal entry/exit) each leg is an IOC limit at the
        profit boundary, so it fills profitably or not at all; without (stop loss) they are market."""
        c = self.config
        quantity = self.clip_size(quantity)
        self.last_clip_at = self.clock()
        limits = self.boundary_limits(intent, *prices) if prices is not None else (None, None)
        legs = []
        for index, leg in enumerate(("short", "long")):
            venue, side = c.venue_of(leg), leg_side(leg, intent)
            price = prices[index] if prices is not None else self.executable_price(venue, side)
            error = None if price is not None else "market data unavailable"
            error = error or self.instruments[venue].size_error(quantity, price)
            if error:
                await self.pause(f"Order-size compliance check failed on {venue}: {error}")
                return
            legs.append((leg, venue, side, price, limits[index]))
        clip = f"clip-{uuid.uuid4().hex[:12]}"
        reduce_only = intent == "exit"
        orders = [self.new_order(leg=leg, side=side, purpose=purpose or f"taker_{intent}", quantity=quantity,
                                 reduce_only=reduce_only, reference=price, clip=clip)
                  for leg, venue, side, price, limit in legs]
        self.log("clip_triggered", intent=intent, condition=condition, quantity=str(quantity),
                 limits=[None if limit is None else str(limit) for *_, limit in legs])
        results = await asyncio.gather(*(
            submit_market(self.adapters[venue], symbol=c.symbol, side=side, quantity=quantity,
                          reduce_only=reduce_only, reference_price=price, limit_price=limit,
                          timeout_seconds=c.order_timeout_seconds, poll_seconds=c.poll_seconds(venue))
            for leg, venue, side, price, limit in legs))
        for order, result in zip(orders, results):
            self.apply_execution(order, result)
        rejected = [result for result in results if result.state == "REJECTED"]
        if any(result.state == "UNKNOWN" for result in results):
            await self.pause("market order outcome unresolved; reconcile the venue order manually")
            return
        if rejected:
            await self.record_failure([result.error or "rejected" for result in rejected],
                                      margin_pause=intent == "entry" and len(rejected) == len(results))
        else:
            self.failure_count = 0
        event = "clip_unfilled" if all(result.filled == 0 for result in results) else "clip_settled"
        self.log(event, intent=intent, fills=[str(r.filled) for r in results],
                 matched=str(self.matched()), imbalance=str(self.imbalance()))
        if abs(self.imbalance()) > EPSILON:
            # The spread moved mid-order and the legs filled unevenly: settle it now, not next tick,
            # so no unhedged position is left open (an entry is unwound, an exit completed).
            await self.ensure_hedged()

    async def record_failure(self, errors: list[str], *, margin_pause: bool = False):
        """Original backoff: 2s * 2^min(4, n) capped at 60s; 429 waits >= 30s; pause on margin / 5 failures."""
        self.failure_count += 1
        wait = min(60_000, 2_000 * 2 ** min(4, self.failure_count))
        if any(RATE_LIMIT_ERROR.search(error) for error in errors):
            wait = max(wait, 30_000)
        self.cooldown_until = self.clock() + wait
        self.log("submission_failed", errors=[error[:160] for error in errors],
                 failures=self.failure_count, cooldown_ms=wait)
        if margin_pause and any(MARGIN_ERROR.search(error) for error in errors):
            await self.pause("Order rejected for insufficient margin; review before resuming")
        elif self.failure_count >= MAX_FAILURES:
            await self.pause("Five consecutive order submissions failed; check credentials, balances and venue status")

    # ------------------------------------------------------------------ maker_taker

    def desired_maker_price(self, intent: str, quantity: Decimal | None = None) -> tuple[Decimal, str] | None:
        c, s = self.config, self.state
        books = self.feed.fresh_pair()
        if books is None:
            return None
        maker_leg = c.maker_leg
        taker_leg = "long" if maker_leg == "short" else "short"
        maker_side, taker_side = leg_side(maker_leg, intent), leg_side(taker_leg, intent)
        taker_book, maker_book = books[c.venue_of(taker_leg)], books[c.maker_venue]
        # The hedge will take the quote's whole size: price it at the taker book's average.
        taker_price = taker_book.executable(taker_side, quantity)
        if taker_price is None:
            return None
        boundary = maker_boundary(c, intent, taker_price, s.short, s.long)
        instrument = self.instruments[c.maker_venue]
        if maker_side == "SELL":
            # Join the best ask when it already satisfies the spread; otherwise rest at the boundary.
            price = max(instrument.round_price(boundary, "up"), maker_book.ask)
        else:
            price = min(instrument.round_price(boundary, "down"), maker_book.bid)
        return (price, maker_side) if price > 0 else None

    async def evaluate_maker_quotes(self):
        c = self.config
        matched = self.matched()
        intents: dict[str, Decimal] = {}
        capacity = self.remaining_entry_capacity()
        if capacity > EPSILON:
            intents["entry"] = self.hedgeable_size("entry", min(c.clip_quantity, capacity))
        if matched > EPSILON:
            intents["exit"] = self.hedgeable_size("exit", min(c.clip_quantity, matched))
        intents = {intent: quantity for intent, quantity in intents.items() if quantity > EPSILON}
        for intent, order in self.quotes().items():
            if intent not in intents:
                await self.cancel_quote(order)
        for intent, quantity in intents.items():
            if self.state.status != "RUNNING":
                return
            await self.maintain_quote(intent, quantity)

    def hedgeable_size(self, intent: str, cap: Decimal) -> Decimal:
        """Quote size the taker book can hedge in full: the cap, or the largest lot multiple whose
        hedge the visible depth covers (zero when not even the minimum fits)."""
        c = self.config
        books = self.feed.fresh_pair()
        if books is None:
            return ZERO
        taker_leg = "long" if c.maker_leg == "short" else "short"
        taker_book = books[c.venue_of(taker_leg)]
        side = leg_side(taker_leg, intent)
        lot = common_lot([self.instruments[venue] for venue in c.venues]) or Decimal("0.00000001")
        top = self.clip_size(cap)
        if top <= 0 or taker_book.executable(side, top) is not None:
            return top
        low, high = ZERO, top
        while high - low > lot:
            middle = ((low + high) / 2 / lot).to_integral_value(rounding=ROUND_FLOOR) * lot
            if middle <= low:
                break
            if taker_book.executable(side, middle) is not None:
                low = middle
            else:
                high = middle
        minimum = c.min_clip_notional_usd / taker_book.bid if intent == "entry" else ZERO
        return low if low >= minimum else ZERO

    async def maintain_quote(self, intent: str, quantity: Decimal):
        c = self.config
        desired = self.desired_maker_price(intent, quantity)
        if desired is None:
            # No data, or the hedge book is too thin for this size: do not leave a quote resting.
            existing = self.quotes().get(intent)
            if existing is not None:
                await self.cancel_quote(existing)
            return
        price, side = desired
        instrument = self.instruments[c.maker_venue]
        existing = self.quotes().get(intent)
        now = self.clock()
        if existing is not None:
            drifted = abs(price - number(existing.price)) >= instrument.price_tolerance(number(existing.price))
            if not drifted or now - existing.created_ms < c.requote_interval_seconds * 1000:
                return
            if not await self.cancel_quote(existing):
                return
        if now - self.last_quote_at < c.requote_interval_seconds * 500:
            return
        quantity = instrument.round_quantity(quantity)
        error = instrument.size_error(quantity, price)
        if error:
            await self.pause(f"Order-size compliance check failed on {c.maker_venue}: {error}")
            return
        order = self.new_order(leg=c.maker_leg, side=side, purpose=f"quote_{intent}", quantity=quantity,
                               reduce_only=intent == "exit", price=price)
        self.last_quote_at = now
        try:
            result = await place_quote(self.adapters[c.maker_venue], symbol=c.symbol, side=side,
                                       quantity=quantity, price=price, reduce_only=intent == "exit")
        except Exception as exc:
            result, message = getattr(exc, "order_result", None), str(exc)
            order.error = message[:300]
            if POST_ONLY_CROSS.search(message):
                # The book moved through the quote: harmless, requote on a later tick.
                self.apply(order, ZERO, None, "EXPIRED")
            elif definitive_rejection(exc, result):
                self.apply(order, ZERO, None, "REJECTED")
                await self.record_failure([message])
            else:
                order.state = "UNKNOWN"
                self.save()
                await self.pause(f"quote submission outcome unknown: {message[:160]}")
            return
        status = parse_status(result)
        order.remote = result
        self.apply(order, status.filled, status.average, status.state if status.terminal else "OPEN")
        self.failure_count = 0
        self.log("quote_placed", intent=intent, side=side, price=str(price), quantity=str(quantity),
                 state=order.state)

    # ------------------------------------------------------------------ hedge repair

    async def ensure_hedged(self):
        """Original ensureHedged: market-repair the lagging leg; pause after 3 failed attempts."""
        c, s = self.config, self.state
        if s.status not in ACTIVE:
            return
        residual = self.imbalance()
        if abs(residual) <= EPSILON:
            self.repair_attempts, self._dust_logged = 0, None
            return
        if self.non_quote_open_orders():
            return
        now = self.clock()
        # The cooldown spaces out retries; a fresh fill after a successful hedge repairs at once.
        if self.repair_attempts and now - self.last_repair_at < c.repair_cooldown_seconds * 1000:
            return
        if self.repair_attempts >= MAX_REPAIR_ATTEMPTS:
            await self.pause(f"Unable to hedge residual exposure of {residual} {c.symbol} after "
                             f"{MAX_REPAIR_ATTEMPTS} attempts; manual review required")
            return
        short_q, long_q = number(s.short.quantity), number(s.long.quantity)
        lagging = "short" if abs(short_q) < abs(long_q) else "long"
        excess = "long" if lagging == "short" else "short"
        held = short_q if lagging == "short" else long_q
        other = long_q if lagging == "short" else short_q
        delta = -other - held
        trim_side = "SELL" if other > 0 else "BUY"
        if self.last_fill_trims():
            # A take-profit fill reduced one leg: close the other to match (topping up the smaller
            # leg, the original's rule, would reopen what just closed). A bounded taker entry that
            # filled unevenly is unwound the same way rather than chased at market.
            leg, side, trim = excess, trim_side, True
        else:
            leg, side, trim = lagging, "BUY" if delta > 0 else "SELL", False
        quantity = self.instruments[c.venue_of(leg)].round_quantity(abs(delta))
        error = self.repair_size_error(leg, side, quantity)
        if error and not trim:
            # Cannot top up the lagging leg: trim the excess leg instead when that is executable.
            if self.quotes():
                return
            leg, side, trim = excess, trim_side, True
            quantity = self.instruments[c.venue_of(leg)].round_quantity(abs(delta))
            error = self.repair_size_error(leg, side, quantity)
        if error:
            if self._dust_logged != residual:
                self._dust_logged = residual
                self.log("warning", message=f"residual {residual} is below venue minimums: {error}")
            return
        exposure = number(s.leg(leg).quantity)
        signed = quantity if side == "BUY" else -quantity
        reduce_only = trim or abs(exposure + signed) < abs(exposure)
        self.last_repair_at = now
        self.repair_attempts += 1
        price = self.executable_price(c.venue_of(leg), side)
        order = self.new_order(leg=leg, side=side, purpose="repair", quantity=quantity,
                               reduce_only=reduce_only, reference=price)
        self.log("hedge_repair", leg=leg, side=side, quantity=str(quantity), residual=str(residual), trim=trim)
        result = await submit_market(self.adapters[order.venue], symbol=c.symbol, side=side,
                                     quantity=quantity, reduce_only=reduce_only, reference_price=price,
                                     timeout_seconds=c.order_timeout_seconds,
                                     poll_seconds=c.poll_seconds(order.venue))
        self.apply_execution(order, result)
        if result.state == "UNKNOWN":
            await self.pause("hedge repair outcome unresolved; reconcile the venue order manually")
            return
        if result.state == "FILLED" and result.filled == quantity:
            self.repair_attempts = 0
        elif result.state == "REJECTED":
            self.log("warning", message=f"hedge repair rejected: {(result.error or '')[:160]}")
        if abs(self.imbalance()) <= EPSILON:
            self.repair_attempts = 0

    def last_fill_trims(self) -> bool:
        for order in reversed(self.state.orders):
            if order.purpose != "repair" and number(order.executed_quantity) > 0:
                return order.purpose in {"quote_exit", "taker_exit", "stop", "taker_entry"}
        return False

    def repair_size_error(self, leg: str, side: str, quantity: Decimal) -> str | None:
        venue = self.config.venue_of(leg)
        price = self.executable_price(venue, side)
        if price is None:
            return "market data unavailable"
        return self.instruments[venue].size_error(quantity, price)

    # ------------------------------------------------------------------ optional stop loss

    def check_stop_loss(self):
        c, s = self.config, self.state
        if c.stop_loss_usd is None or self.matched() <= EPSILON:
            return
        books = self.feed.fresh_pair()
        if books is None:
            return
        short_ask, long_bid = books[c.short_venue].ask, books[c.long_venue].bid
        pnl = self.matched() * (number(s.short.average) - short_ask + long_bid - number(s.long.average))
        if pnl <= -c.stop_loss_usd:
            s.status, s.reason = "STOPPING", f"stop loss: unrealized {pnl:.2f} USD"
            self.log("stop_loss", unrealized=f"{pnl:.2f}")
            self.save()

    async def evaluate_stopping(self):
        s = self.state
        for order in self.quotes().values():
            if not await self.cancel_quote(order):
                return
        if abs(self.imbalance()) > EPSILON or self.non_quote_open_orders():
            return
        matched = self.matched()
        if matched <= EPSILON:
            s.status = "STOPPED"
            self.log("stopped", reason=s.reason)
            self.save()
            return
        await self.execute_taker_clip("exit", min(self.config.clip_quantity, matched), s.reason or "stop",
                                      purpose="stop")

    # ------------------------------------------------------------------ status

    def snapshot(self) -> dict:
        c, s = self.config, self.state
        books = self.feed.fresh_pair()
        payload = {"status": s.status, "matched": str(self.matched()), "imbalance": str(self.imbalance()),
                   "short": s.short.quantity, "long": s.long.quantity,
                   "realized": str(number(s.short.realized) + number(s.long.realized)),
                   "fees": s.fees_usd, "quotes": {k: v.price for k, v in self.quotes().items()}}
        if books:
            payload["entry_bps"] = f"{spread_bps(books[c.short_venue].bid, books[c.long_venue].ask):.2f}"
            payload["exit_bps"] = f"{spread_bps(books[c.short_venue].ask, books[c.long_venue].bid):.2f}"
        return payload
