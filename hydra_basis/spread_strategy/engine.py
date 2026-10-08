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
from decimal import Decimal

from hydra_basis.spread_strategy.broker import (
    MARGIN_ERROR, POST_ONLY_CROSS, RATE_LIMIT_ERROR, Execution, definitive_rejection,
    parse_status, place_quote, submit_market,
)
from hydra_basis.spread_strategy.core import (
    EPSILON, ZERO, Config, Order, State, StateStore, entry_allowed, exit_allowed,
    imbalance, maker_boundary, matched_quantity, now_ms, number, spread_bps,
)
from hydra_basis.spread_strategy.feeds import MarketFeed
from hydra_basis.spread_strategy.instruments import Instrument

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

    def executable_price(self, venue: str, side: str) -> Decimal | None:
        book = self.feed.fresh(venue)
        if book is None:
            return None
        return book.ask if side == "BUY" else book.bid

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
        c, s = self.config, self.state
        if abs(self.imbalance()) > EPSILON:
            return
        books = self.feed.fresh_pair()
        if books is None:
            return
        matched = self.matched()
        short_bid, long_ask = books[c.short_venue].bid, books[c.long_venue].ask
        if entry_allowed(c, short_bid, long_ask):
            quantity = min(c.clip_quantity, self.remaining_entry_capacity())
            if quantity > EPSILON:
                await self.execute_taker_clip("entry", quantity, f"entry {short_bid}/{long_ask}")
                return
        if matched > EPSILON:
            short_ask, long_bid = books[c.short_venue].ask, books[c.long_venue].bid
            if exit_allowed(c, s.short, s.long, short_ask, long_bid):
                await self.execute_taker_clip("exit", min(c.clip_quantity, matched),
                                              f"exit {short_ask}/{long_bid}")

    async def execute_taker_clip(self, intent: str, quantity: Decimal, condition: str, *, purpose: str | None = None):
        c = self.config
        for venue in c.venues:
            quantity = self.instruments[venue].round_quantity(quantity)
        legs = []
        for leg in ("short", "long"):
            venue, side = c.venue_of(leg), leg_side(leg, intent)
            price = self.executable_price(venue, side)
            error = None if price is not None else "market data unavailable"
            error = error or self.instruments[venue].size_error(quantity, price)
            if error:
                await self.pause(f"Order-size compliance check failed on {venue}: {error}")
                return
            legs.append((leg, venue, side, price))
        clip = f"clip-{uuid.uuid4().hex[:12]}"
        reduce_only = intent == "exit"
        orders = [self.new_order(leg=leg, side=side, purpose=purpose or f"taker_{intent}", quantity=quantity,
                                 reduce_only=reduce_only, reference=price, clip=clip)
                  for leg, venue, side, price in legs]
        self.log("clip_triggered", intent=intent, condition=condition, quantity=str(quantity))
        results = await asyncio.gather(*(
            submit_market(self.adapters[venue], symbol=c.symbol, side=side, quantity=quantity,
                          reduce_only=reduce_only, reference_price=price,
                          timeout_seconds=c.order_timeout_seconds, poll_seconds=c.poll_seconds(venue))
            for leg, venue, side, price in legs))
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
        self.log("clip_settled", intent=intent, fills=[str(r.filled) for r in results],
                 matched=str(self.matched()), imbalance=str(self.imbalance()))

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

    def desired_maker_price(self, intent: str) -> tuple[Decimal, str] | None:
        c, s = self.config, self.state
        books = self.feed.fresh_pair()
        if books is None:
            return None
        maker_leg = c.maker_leg
        taker_leg = "long" if maker_leg == "short" else "short"
        maker_side, taker_side = leg_side(maker_leg, intent), leg_side(taker_leg, intent)
        taker_book, maker_book = books[c.venue_of(taker_leg)], books[c.maker_venue]
        taker_price = taker_book.ask if taker_side == "BUY" else taker_book.bid
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
            intents["entry"] = min(c.clip_quantity, capacity)
        if matched > EPSILON:
            intents["exit"] = min(c.clip_quantity, matched)
        for intent, order in self.quotes().items():
            if intent not in intents:
                await self.cancel_quote(order)
        for intent, quantity in intents.items():
            if self.state.status != "RUNNING":
                return
            await self.maintain_quote(intent, quantity)

    async def maintain_quote(self, intent: str, quantity: Decimal):
        c = self.config
        desired = self.desired_maker_price(intent)
        if desired is None:
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
        if self.last_fill_closed():
            # A take-profit fill reduced one leg. Close the other leg to match; topping up
            # the smaller leg (the original's lagging-leg rule) would reopen what just closed.
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

    def last_fill_closed(self) -> bool:
        for order in reversed(self.state.orders):
            if order.purpose != "repair" and number(order.executed_quantity) > 0:
                return order.purpose in {"quote_exit", "taker_exit", "stop"}
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
