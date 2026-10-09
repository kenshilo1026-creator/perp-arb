import json
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path

from hydra_basis.risk_management.registry import PositionRegistry
from hydra_basis.spread_strategy.broker import (
    PaperRejection, PaperVenue, definitive_rejection, parse_status, submit_market, sync_registry,
)
from hydra_basis.spread_strategy.core import (
    Config, Leg, Order, State, StateStore, entry_allowed, entry_ratio_required, exit_allowed,
    maker_boundary, spread_bps,
)
from hydra_basis.spread_strategy.engine import Engine
from hydra_basis.spread_strategy.feeds import MarketFeed
from hydra_basis.spread_strategy.instruments import Instrument

ZERO_FEES = {v: {"maker": D("0"), "taker": D("0")} for v in ("aster", "hyperliquid", "lighter", "mexc", "variational")}
INSTRUMENTS = {
    "aster": Instrument("aster", tick_size=D("0.01"), lot_size=D("0.001"), min_size=D("0.001"),
                        min_notional=D("5")),
    "hyperliquid": Instrument("hyperliquid", lot_size=D("0.0001"), min_size=D("0.0001"),
                              min_notional=D("10"), sz_decimals=4),
    "variational": Instrument("variational"),
}


def config(**kwargs):
    defaults = dict(symbol="ETH", short_venue="aster", long_venue="hyperliquid",
                    execution_method="taker_taker", total_quantity=D("0.02"), clip_quantity=D("0.01"),
                    entry_bps=D("40"), take_profit_bps=D("10"), fees=ZERO_FEES,
                    min_profit_bps=D("0"), slippage_buffer_bps=D("0"), funding_budget_bps=D("0"),
                    clip_interval_seconds=0)
    return Config(**(defaults | kwargs))


class Clock:
    def __init__(self):
        self.t = 1_700_000_000_000

    def __call__(self):
        return self.t

    def advance(self, ms):
        self.t += ms


class Harness:
    def __init__(self, tmp, venues=None, live=False, **kwargs):
        self.config = config(**kwargs)
        self.clock = Clock()
        self.feed = MarketFeed(self.config, clock=self.clock)
        self.store = StateStore(Path(tmp) / "state.json")
        self.state = self.store.load(self.config, live=live)
        self.adapters = venues or {v: PaperVenue(v, self.feed) for v in self.config.venues}
        self.events = []
        self.exposures = []
        self.engine = Engine(self.config, self.state, self.store, self.feed, self.adapters, INSTRUMENTS,
                             live=live, on_exposure=lambda s: self.exposures.append(s.short.quantity),
                             clock=self.clock, log=self.events.append)

    def books(self, aster, hyperliquid):
        for venue, (bid, ask) in (("aster", aster), ("hyperliquid", hyperliquid)):
            self.feed.update(venue, D(bid), D(ask), source_ms=self.clock(), received_ms=self.clock())

    def wide(self):
        self.books(("2010", "2010.5"), ("1999.5", "2000"))

    def converged(self):
        self.books(("2000", "2000.5"), ("2000", "2000.5"))

    def legs(self):
        return D(self.state.short.quantity), D(self.state.long.quantity)

    def events_named(self, name):
        return [event for event in self.events if event["event"] == name]


class CoreTests(unittest.TestCase):
    def test_spread_direction_and_units(self):
        self.assertEqual(spread_bps(D("101"), D("100")), D("100"))
        c = config()
        self.assertTrue(entry_allowed(c, D("100.4"), D("100")))
        self.assertFalse(entry_allowed(c, D("100.39"), D("100")))

    def test_net_gate_raises_required_entry_ratio(self):
        c = config(fees={v: {"maker": D("0"), "taker": D("0.001")} for v in ("aster", "hyperliquid")},
                   funding_budget_bps=D("5"))
        self.assertGreater(entry_ratio_required(c), D("1.004"))
        self.assertFalse(entry_allowed(c, D("100.4"), D("100")))

    def test_maker_boundary_for_both_maker_legs(self):
        flat = Leg()
        short_maker = config(execution_method="maker_taker", maker_venue="aster")
        self.assertEqual(maker_boundary(short_maker, "entry", D("2000"), flat, flat), D("2008"))
        long_maker = config(execution_method="maker_taker", maker_venue="hyperliquid")
        self.assertEqual(maker_boundary(long_maker, "entry", D("2008"), flat, flat), D("2000"))

    def test_exit_boundary_uses_take_profit_and_entry_costs(self):
        c = config(execution_method="maker_taker", maker_venue="aster")
        short, long = Leg("-1", "2008"), Leg("1", "2000")
        # Take profit (10 bps over the long bid) is stricter than break-even (8 USD of edge).
        self.assertEqual(maker_boundary(c, "exit", D("1999.5"), short, long), D("1999.5") * D("1.001"))
        self.assertTrue(exit_allowed(c, short, long, D("2001"), D("1999.5")))
        losing = Leg("-1", "1990")
        self.assertFalse(exit_allowed(c, losing, long, D("2001"), D("1999.5")))

    def test_leg_average_cost_and_realized(self):
        leg = Leg()
        leg.apply(D("-1"), D("100"))
        leg.apply(D("-1"), D("102"))
        self.assertEqual(D(leg.average), D("101"))
        leg.apply(D("1"), D("99"))
        self.assertEqual(D(leg.realized), D("2"))
        self.assertEqual(D(leg.quantity), D("-1"))

    def test_config_validation(self):
        for kwargs in ({"total_quantity": D("NaN")}, {"long_venue": "aster"}, {"take_profit_bps": D("40")},
                       {"execution_method": "maker_taker", "maker_venue": None},
                       {"execution_method": "maker_taker", "maker_venue": "variational",
                        "long_venue": "variational"},
                       {"short_leverage": 0}, {"stop_loss_usd": D("0")}):
            with self.assertRaises(ValueError):
                config(**kwargs)

    def test_load_rejects_removed_keys_and_maps_legacy_leverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "c.json"
            payload = json.loads(Path("configs/spread_strategy.example.json").read_text())
            path.write_text(json.dumps(payload | {"leverage": 3}))
            loaded = Config.load(path)
            self.assertEqual(loaded.short_leverage, 1)  # explicit per-leg values win
            path.write_text(json.dumps(payload | {"max_hold_seconds": 10}))
            with self.assertRaises(ValueError):
                Config.load(path)


class InstrumentTests(unittest.TestCase):
    def test_hyperliquid_price_rules(self):
        hl = INSTRUMENTS["hyperliquid"]
        self.assertEqual(hl.round_price(D("2345.678"), "down"), D("2345.6"))
        self.assertEqual(hl.round_price(D("2345.61"), "up"), D("2345.7"))
        self.assertEqual(hl.round_price(D("123456.7"), "down"), D("123456"))

    def test_size_errors(self):
        aster = INSTRUMENTS["aster"]
        self.assertIsNone(aster.size_error(D("0.01"), D("2000")))
        self.assertIn("minimum_size", aster.size_error(D("0.0001"), D("2000")))
        self.assertIn("lot", aster.size_error(D("0.0105"), D("2000")))
        self.assertIn("notional", aster.size_error(D("0.002"), D("2000")))


class StatusTests(unittest.TestCase):
    def test_parse_venue_payloads(self):
        hl = parse_status({"status": "FILLED", "terminal": True, "filled_quantity": "0.5"})
        self.assertEqual((hl.filled, hl.terminal, hl.state), (D("0.5"), True, "FILLED"))
        aster_place = parse_status({"ok": True, "order_id": 1, "raw": {"status": "NEW", "executedQty": "0"}})
        self.assertEqual((aster_place.terminal, aster_place.state), (False, "OPEN"))
        aster_cancel = parse_status({"ok": True, "raw": {"status": "CANCELED", "executedQty": "0.2",
                                                         "avgPrice": "10"}})
        self.assertEqual((aster_cancel.filled, aster_cancel.average, aster_cancel.state),
                         (D("0.2"), D("10"), "CANCELED"))
        variational = parse_status({"type": "ORDER_RESULT", "ok": True, "filled": True, "terminal": False,
                                    "status": "FILLED", "details": {"fill": {"filledBaseAmount": "0.3"}}})
        self.assertEqual((variational.filled, variational.terminal), (D("0.3"), True))
        hl_alo = parse_status({"ok": True, "order_id": 5, "raw": {"status": "ok", "response": {}}})
        self.assertEqual(hl_alo.state, "OPEN")

    def test_definitive_rejection(self):
        self.assertTrue(definitive_rejection(RuntimeError("aster order 400: {'code': -2019}"), None))
        self.assertTrue(definitive_rejection(RuntimeError("hyperliquid order error: Insufficient margin"), None))
        self.assertFalse(definitive_rejection(RuntimeError("aster order 503: busy"), None))
        self.assertFalse(definitive_rejection(RuntimeError("x"), {"type": "ORDER_RESULT", "ok": False,
                                                                   "details": {"postSubmitAmbiguous": True}}))


class FeedTests(unittest.TestCase):
    def test_freshness_rules(self):
        c, clock = config(), Clock()
        feed = MarketFeed(c, clock=clock)
        self.assertIsNone(feed.fresh("aster"))
        feed.update("aster", D("1"), D("2"), source_ms=clock(), received_ms=clock())
        self.assertIsNotNone(feed.fresh("aster"))
        clock.advance(15_001)
        self.assertIsNone(feed.fresh("aster"), "stale")
        feed.update("aster", D("1"), D("2"), source_ms=clock() + 2_001, received_ms=clock())
        self.assertIsNone(feed.fresh("aster"), "future")
        feed.update("aster", D("1"), D("2"), source_ms=clock() - 3_001, received_ms=clock())
        self.assertIsNone(feed.fresh("aster"), "transport lag")
        feed.update("aster", D("1"), D("2"), source_ms=clock(), received_ms=clock())
        feed.set_health("aster", False)
        self.assertIsNone(feed.fresh("aster"), "disconnected")
        feed.update("aster", D("2"), D("1"), source_ms=clock(), received_ms=clock())
        self.assertIsNone(feed.fresh("aster"), "crossed")


class RejectingVenue(PaperVenue):
    def __init__(self, venue, feed, message="insufficient margin"):
        super().__init__(venue, feed)
        self.message = message
        self.market_calls = 0

    async def place_market_order(self, **kwargs):
        self.market_calls += 1
        raise PaperRejection(self.message)


class TakerTakerTests(unittest.IsolatedAsyncioTestCase):
    async def test_cycles_entry_exit_and_reenters(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.wide()
            await h.engine.start()
            await h.engine.step()
            h.wide()  # a replenished book, rather than consuming the same cached depth
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.02"), D("0.02")))
            await h.engine.step()  # at capacity: no further entry
            self.assertEqual(h.legs(), (D("-0.02"), D("0.02")))
            h.converged()
            await h.engine.step()
            h.converged()
            await h.engine.step()
            self.assertEqual(h.legs(), (D("0"), D("0")))
            self.assertGreater(D(h.state.short.realized) + D(h.state.long.realized), 0)
            h.wide()
            await h.engine.step()  # no exit latch: the strategy re-enters
            self.assertEqual(h.legs(), (D("-0.01"), D("0.01")))
            self.assertEqual(h.state.status, "RUNNING")

    async def test_stale_market_does_not_trade(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.wide()
            await h.engine.start()
            h.clock.advance(16_000)
            await h.engine.step()
            self.assertEqual(h.legs(), (D("0"), D("0")))

    async def test_one_leg_rejection_backs_off_then_unwinds_the_filled_leg(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.adapters["hyperliquid"] = RejectingVenue("hyperliquid", h.feed)
            h.wide()
            await h.engine.start()
            await h.engine.step()
            self.assertEqual(h.engine.failure_count, 1)
            self.assertEqual(h.engine.cooldown_until, h.clock() + 4_000)
            self.assertEqual(h.state.status, "RUNNING", "one accepted leg: margin rejection does not pause")
            # In the same step, the leg that filled is unwound rather than the other chased at market.
            self.assertEqual(h.legs(), (D("0"), D("0")))
            repair = [o for o in h.state.orders if o.purpose == "repair"][-1]
            self.assertEqual((repair.venue, repair.side, repair.reduce_only), ("aster", "BUY", True))

    async def test_maker_hedge_failures_pause_after_three_attempts(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, execution_method="maker_taker", maker_venue="aster")
            h.adapters["hyperliquid"] = RejectingVenue("hyperliquid", h.feed)
            h.books(("2000", "2000.5"), ("1999.5", "2000"))
            await h.engine.start()
            await h.engine.step()
            h.books(("2008.5", "2009"), ("1999.5", "2000"))  # the quote fills; the hedge must follow
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.01"), D("0")))
            for _ in range(3):
                h.clock.advance(3_001)
                h.books(("2008.5", "2009"), ("1999.5", "2000"))
                await h.engine.step()
            self.assertEqual(h.state.status, "PAUSED")
            self.assertIn("after 3 attempts", h.state.reason)

    async def test_one_leg_exit_rejection_closes_the_other_leg(self):
        class ExitRejecting(PaperVenue):
            reject = False

            async def place_market_order(self, **kwargs):
                if self.reject and kwargs.get("reduce_only"):
                    self.reject = False
                    raise PaperRejection("venue busy")
                return await super().place_market_order(**kwargs)
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, total_quantity=D("0.01"))
            hl = ExitRejecting("hyperliquid", h.feed)
            h.adapters["hyperliquid"] = hl
            h.wide()
            await h.engine.start()
            await h.engine.step()
            hl.reject = True
            h.converged()
            await h.engine.step()  # short closes, long exit rejected, then completed at once
            self.assertEqual(h.legs(), (D("0"), D("0")))
            self.assertEqual((h.state.orders[-1].purpose, h.state.orders[-1].side), ("repair", "SELL"))

    async def test_entry_margin_rejection_on_both_legs_pauses(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.wide()
            for venue in h.config.venues:
                h.adapters[venue] = RejectingVenue(venue, h.feed)
            await h.engine.start()
            await h.engine.step()
            self.assertEqual(h.state.status, "PAUSED")
            self.assertIn("insufficient margin", h.state.reason)

    async def test_five_consecutive_failures_pause_with_exponential_backoff(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.wide()
            for venue in h.config.venues:
                h.adapters[venue] = RejectingVenue(venue, h.feed, message="venue busy")
            await h.engine.start()
            waits = []
            for _ in range(5):
                before = h.clock()
                await h.engine.step()
                waits.append(h.engine.cooldown_until - before)
                h.clock.advance(61_000)
                h.wide()
            self.assertEqual(waits, [4_000, 8_000, 16_000, 32_000, 32_000])
            self.assertEqual(h.state.status, "PAUSED")

    async def test_unknown_outcome_pauses(self):
        class Silent(PaperVenue):
            async def place_market_order(self, **kwargs):
                raise RuntimeError("connection reset")
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, order_timeout_seconds=0.01)
            h.adapters["aster"] = Silent("aster", h.feed)
            h.wide()
            await h.engine.start()
            await h.engine.step()
            self.assertEqual(h.state.status, "PAUSED")
            self.assertTrue(any(o.state == "UNKNOWN" for o in h.state.orders))
            with self.assertRaises(RuntimeError):
                await Harness(tmp).engine.start(resume=True)  # unknown order blocks restart
            restarted = Harness(tmp)
            restarted.wide()
            unknown = next(o for o in restarted.state.orders if o.state == "UNKNOWN")
            restarted.engine.settle_order(unknown.id, D("0.01"), D("2010"))
            await restarted.engine.start(resume=True)
            self.assertEqual(restarted.state.status, "RUNNING")
            self.assertEqual(restarted.legs(), (D("-0.01"), D("0.01")))

    async def test_optional_stop_loss_closes_and_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, stop_loss_usd=D("1"))
            h.wide()
            await h.engine.start()
            await h.engine.step()
            h.wide()
            await h.engine.step()
            h.books(("2100", "2100.5"), ("1999.5", "2000"))  # spread blows out: -1.8 USD
            await h.engine.step()
            self.assertEqual(h.state.status, "STOPPING")
            for _ in range(3):
                await h.engine.step()
            self.assertEqual(h.state.status, "STOPPED")
            self.assertEqual(h.legs(), (D("0"), D("0")))


class MakerTakerTests(unittest.IsolatedAsyncioTestCase):
    def harness(self, tmp, **kwargs):
        return Harness(tmp, execution_method="maker_taker", maker_venue="aster", **kwargs)

    async def test_quote_rests_at_boundary_then_fills_and_hedges(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = self.harness(tmp)
            h.books(("2000", "2000.5"), ("1999.5", "2000"))
            await h.engine.start()
            await h.engine.step()
            quote = h.engine.quotes()["entry"]
            # Spread not reached at the top of book: rest at long ask * 1.004.
            self.assertEqual((quote.side, D(quote.price)), ("SELL", D("2008")))
            self.assertEqual(h.legs(), (D("0"), D("0")))
            h.books(("2008.5", "2009"), ("1999.5", "2000"))  # buyers reach the quote
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.01"), D("0.01")))
            repair = [o for o in h.state.orders if o.purpose == "repair"]
            self.assertEqual((repair[0].venue, repair[0].side), ("hyperliquid", "BUY"))

    async def test_joins_top_of_book_when_spread_already_satisfied(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = self.harness(tmp)
            h.books(("2010", "2010.5"), ("1999.5", "2000"))
            await h.engine.start()
            await h.engine.step()
            self.assertEqual(D(h.engine.quotes()["entry"].price), D("2010.5"))

    async def test_requotes_on_drift_only_after_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = self.harness(tmp)
            h.books(("2000", "2000.5"), ("1999.5", "2000"))
            await h.engine.start()
            await h.engine.step()
            first = h.engine.quotes()["entry"]
            h.clock.advance(1_000)
            h.books(("2000", "2000.5"), ("2000.5", "2001"))
            await h.engine.step()
            self.assertIs(h.engine.quotes()["entry"], first, "inside the requote interval")
            h.clock.advance(1_001)
            h.books(("2000", "2000.5"), ("2000.5", "2001"))
            await h.engine.step()
            second = h.engine.quotes()["entry"]
            self.assertEqual(first.state, "CANCELED")
            self.assertEqual(D(second.price), D("2009.01"))  # 2001 * 1.004 rounded up to tick

    async def test_exit_quote_after_entry_and_reentry(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = self.harness(tmp, total_quantity=D("0.01"))
            h.books(("2000", "2000.5"), ("1999.5", "2000"))
            await h.engine.start()
            await h.engine.step()
            h.books(("2008.5", "2009"), ("1999.5", "2000"))
            await h.engine.step()
            h.clock.advance(1_001)
            await h.engine.step()
            exit_quote = h.engine.quotes()["exit"]
            self.assertTrue(exit_quote.reduce_only)
            self.assertEqual(D(exit_quote.price), D("2001.49"))  # 1999.5 * 1.001 rounded down
            self.assertNotIn("entry", h.engine.quotes(), "max position reached")
            h.books(("2000.5", "2001"), ("2000", "2000.5"))  # sellers reach the exit bid
            await h.engine.step()
            self.assertEqual(h.legs(), (D("0"), D("0")))
            hedge = [o for o in h.state.orders if o.purpose == "repair"][-1]
            # The exit hedge closes the long; it must not reopen the short that just closed.
            self.assertEqual((hedge.venue, hedge.side, hedge.reduce_only), ("hyperliquid", "SELL", True))
            h.clock.advance(1_001)
            h.books(("2000", "2000.5"), ("1999.5", "2000"))
            await h.engine.step()
            self.assertIn("entry", h.engine.quotes(), "re-armed after take profit")

    async def test_partial_fill_hedges_only_filled_quantity(self):
        class Partial(PaperVenue):
            def _match(self):
                book = self.feed.fresh(self.venue)
                for order in self.orders.values():
                    if order["status"] == "NEW" and order["side"] == "SELL" and book.bid >= order["price"]:
                        half = order["quantity"] / 2
                        if order["executedQty"] < half:
                            self._fill("SELL", half)
                            order["executedQty"] = half
        with tempfile.TemporaryDirectory() as tmp:
            h = self.harness(tmp)
            h.adapters["aster"] = Partial("aster", h.feed)
            h.books(("2000", "2000.5"), ("1999.5", "2000"))
            await h.engine.start()
            await h.engine.step()
            h.books(("2008.5", "2009"), ("1999.5", "2000"))
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.005"), D("0.005")))
            self.assertEqual(h.engine.quotes()["entry"].state, "OPEN", "remainder keeps resting")

    async def test_post_only_cross_is_not_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = self.harness(tmp)
            h.books(("2000", "2000.5"), ("1999.5", "2000"))
            await h.engine.start()
            h.feed.update("aster", D("2009"), D("2010"), source_ms=h.clock(), received_ms=h.clock())
            original = h.engine.desired_maker_price
            h.engine.desired_maker_price = lambda intent, quantity=None: (D("2008"), "SELL")  # stale decision
            await h.engine.step()
            h.engine.desired_maker_price = original
            expired = [o for o in h.state.orders if o.state == "EXPIRED"]
            self.assertEqual(len(expired), 1)
            self.assertEqual(h.engine.failure_count, 0)

    async def test_restart_cancels_remembered_quote_and_refuses_pending_submit(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = self.harness(tmp)
            h.books(("2000", "2000.5"), ("1999.5", "2000"))
            await h.engine.start()
            await h.engine.step()
            quote = h.engine.quotes()["entry"]
            restarted = Engine(h.config, h.store.load(h.config, live=False), h.store, h.feed, h.adapters,
                               INSTRUMENTS, live=False, clock=h.clock, log=lambda e: None)
            await restarted.start()
            self.assertEqual(restarted.state.orders[-1].id, quote.id)
            self.assertEqual(restarted.state.orders[-1].state, "CANCELED")
            restarted.state.orders.append(Order(id="x", venue="aster", leg="short", side="SELL",
                                                purpose="taker_entry", quantity="0.01", reduce_only=False))
            restarted.save()
            again = Engine(h.config, h.store.load(h.config, live=False), h.store, h.feed, h.adapters,
                           INSTRUMENTS, live=False, clock=h.clock, log=lambda e: None)
            with self.assertRaises(RuntimeError):
                await again.start()


class PreflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_minimum_notional_blocks_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, total_quantity=D("0.004"), clip_quantity=D("0.004"))
            h.wide()
            with self.assertRaises(RuntimeError):
                await h.engine.start()  # 0.004 * 2000 = 8 USD < Hyperliquid's 10 USD

    async def test_live_margin_preflight(self):
        class Poor(PaperVenue):
            async def get_available_margin(self):
                return D("10")
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, live=True)
            h.adapters["aster"] = Poor("aster", h.feed)
            h.wide()
            with self.assertRaises(RuntimeError) as raised:
                await h.engine.start()
            self.assertIn("insufficient aster margin", str(raised.exception))


class VariationalSettlementTests(unittest.IsolatedAsyncioTestCase):
    async def test_market_fill_settles_from_position_delta(self):
        class Browser:
            def __init__(self):
                self.position = D("0")

            async def get_open_position(self, *, symbol, market_type):
                return None if self.position == 0 else {"side": "LONG", "quantity": str(self.position)}

            async def place_market_order(self, **kwargs):
                self.position += D(kwargs["amount"])
                return {"type": "ORDER_RESULT", "ok": True, "filled": True, "terminal": False,
                        "status": "FILLED", "details": {"fill": {"filledBaseAmount": kwargs["amount"]}}}
        result = await submit_market(Browser(), symbol="ETH", side="BUY", quantity=D("0.01"),
                                     reduce_only=False, reference_price=D("2000"), timeout_seconds=1)
        self.assertEqual((result.state, result.filled, result.average), ("FILLED", D("0.01"), None))


class RegistryTests(unittest.TestCase):
    def test_sync_publishes_both_legs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "registry.json"
            state = State("id", "live", "spread-1", short=Leg("-0.02", "2010"), long=Leg("0.02", "2000"))
            sync_registry(path, config(), state)
            legs = PositionRegistry.load(path).legs_for_strategy("spread-1")
            self.assertEqual({(leg.venue, leg.side, leg.quantity) for leg in legs},
                             {("aster", "SHORT", "0.02"), ("hyperliquid", "LONG", "0.02")})

    def test_mexc_registry_quantity_is_in_contracts(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "registry.json"
            state = State("id", "live", "spread-2", short=Leg("-0.12", "771"), long=Leg("0.12", "769"))
            sync_registry(path, config(short_venue="mexc", long_venue="lighter"), state, {"mexc": D("0.01")})
            legs = {leg.venue: leg.quantity for leg in PositionRegistry.load(path).legs_for_strategy("spread-2")}
            self.assertEqual(legs, {"mexc": "12", "lighter": "0.12"})


if __name__ == "__main__":
    unittest.main()

