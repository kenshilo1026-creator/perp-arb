"""VWAP net-dollar sizing, venue minimums, IOC depth limits and event-driven evaluation."""
import tempfile
import unittest
import random
import asyncio
from decimal import Decimal as D

from hydra_basis.spread_strategy.broker import PaperVenue
from hydra_basis.spread_strategy.core import Leg
from hydra_basis.spread_strategy.engine import fit_clip, clip_net_profit
from hydra_basis.spread_strategy.core import entry_allowed, exit_allowed
from hydra_basis.spread_strategy.feeds import Ticker, StoreFeed
from hydra_basis.spread_strategy.instruments import Instrument
from hydra_basis.spread_strategy.dispatcher import QuoteStore
from test_spread_strategy import INSTRUMENTS, Harness, config


def levels(*rows):
    return tuple((D(p), D(s)) for p, s in rows)


def books(short_bids, long_asks):
    return {"aster": Ticker(D(short_bids[0][0]), D(short_bids[0][0]) + D("0.5"), 0, None, levels(*short_bids),
                            levels((str(D(short_bids[0][0]) + D("0.5")), "100"))),
            "hyperliquid": Ticker(D(long_asks[0][0]) - D("0.5"), D(long_asks[0][0]), 0, None,
                                  levels((str(D(long_asks[0][0]) - D("0.5")), "100")), levels(*long_asks))}


class FitTests(unittest.TestCase):
    def test_full_clip_cannot_bypass_configured_minimum(self):
        b = books([("2010", "1")], [("2000", "1")])
        self.assertIsNone(fit_clip(config(min_clip_notional_usd=D("30")), INSTRUMENTS,
                                   b, "entry", D("0.01"), Leg(), Leg()))

    def test_different_leg_prices_do_not_hide_a_legal_size(self):
        b = books([("2010", "0.006"), ("1981", "1")], [("1980", "1")])
        fit = fit_clip(config(), INSTRUMENTS, b, "entry", D("0.01"), Leg(), Leg())
        self.assertIsNotNone(fit)
        self.assertEqual(fit[0], D("0.006"))

    def test_vwap_accepts_full_clip_even_when_tail_fails_individually(self):
        b = books([("2010", "0.009"), ("2005", "1")], [("2000", "1")])
        fit = fit_clip(config(), INSTRUMENTS, b, "entry", D("0.01"), Leg(), Leg())
        self.assertEqual(fit, (D("0.01"), (D("2009.5"), D("2000"))))

    def test_net_dollars_win_over_largest_passing_size(self):
        b = books([("2020", "0.006"), ("2001", "1")], [("2000", "1")])
        c = config()
        full = (b["aster"].executable("SELL", D("0.01")), D("2000"))
        self.assertTrue(entry_allowed(c, *full))
        self.assertEqual(fit_clip(c, INSTRUMENTS, b, "entry", D("0.01"), Leg(), Leg())[0], D("0.006"))

    def test_depth_boundary_optimizer_matches_exhaustive_legal_lots(self):
        rng = random.Random(91)
        inst = {v: Instrument(v, lot_size=D("0.1"), min_size=D("0.1"), min_notional=D("10"))
                for v in ("aster", "hyperliquid")}
        for intent in ("entry", "exit"):
            for _ in range(70):
                bids = sorted([rng.randint(99, 105) for _ in range(4)], reverse=True)
                asks = sorted([rng.randint(100, 106) for _ in range(4)])
                bid_levels = [(str(p), str(D(rng.randint(1, 9)) / 10 + D("0.05"))) for p in bids]
                ask_levels = [(str(p), str(D(rng.randint(1, 9)) / 10 + D("0.05"))) for p in asks]
                b = books(bid_levels, ask_levels)
                if intent == "exit":
                    b = {"aster": Ticker(D("99"), D(asks[0]), 0, None, levels(("99", "10")), levels(*ask_levels)),
                         "hyperliquid": Ticker(D(bids[0]), D("106"), 0, None, levels(*bid_levels), levels(("106", "10")))}
                c = config(total_quantity=D("3"), clip_quantity=D("2"),
                           min_clip_notional_usd=D(rng.choice([0, 15, 30])),
                           fees={v: {"maker": D("0"), "taker": D(rng.choice([0, 1, 5])) / 1000}
                                 for v in ("aster", "hyperliquid")},
                           funding_budget_bps=D(rng.choice([0, 5])),
                           slippage_buffer_bps=D(rng.choice([0, 5])))
                short, long = Leg("-3", "110"), Leg("3", "100")
                oracle = []
                for count in range(1, 21):
                    q = D(count) / 10
                    prices = tuple(b[v].executable(side, q) for v, side in (
                        ("aster", "SELL" if intent == "entry" else "BUY"),
                        ("hyperliquid", "BUY" if intent == "entry" else "SELL")))
                    if None in prices:
                        continue
                    if any(inst[v].size_error(q, p) for v, p in zip(c.venues, prices)):
                        continue
                    if intent == "entry" and any(q * p < c.min_clip_notional_usd for p in prices):
                        continue
                    if not (entry_allowed(c, *prices) if intent == "entry" else exit_allowed(c, short, long, *prices)):
                        continue
                    oracle.append((clip_net_profit(c, intent, q, prices, short, long), q, prices))
                expected = max(oracle, key=lambda item: item[:2]) if oracle else None
                actual = fit_clip(c, inst, b, intent, D("2"), short, long)
                self.assertEqual(actual, (expected[1], expected[2]) if expected else None)

    def test_full_clip_when_depth_allows(self):
        fit = fit_clip(config(), INSTRUMENTS, books([("2010", "1")], [("2000", "1")]), "entry", D("0.01"), Leg(), Leg())
        self.assertEqual(fit, (D("0.01"), (D("2010"), D("2000"))))

    def test_shrinks_to_the_profitable_depth(self):
        # 0.006 at 2010 clears 40 bps; the next bid (2001) does not, so the clip stops at 0.006.
        b = books([("2010", "0.006"), ("2001", "1")], [("2000", "1")])
        quantity, prices = fit_clip(config(), INSTRUMENTS, b, "entry", D("0.01"), Leg(), Leg())
        self.assertEqual((quantity, prices), (D("0.006"), (D("2010"), D("2000"))))

    def test_minimum_notional_and_venue_minimums(self):
        b = books([("2010", "0.006"), ("2001", "1")], [("2000", "1")])
        # 0.006 * 2010 = 12 USD is below a 20 USD minimum clip: wait instead of trading dust.
        self.assertIsNone(fit_clip(config(min_clip_notional_usd=D("20")), INSTRUMENTS, b, "entry", D("0.01"),
                                   Leg(), Leg()))
        # VWAP may include a worse tail: 0.005 is legal and its average still passes.
        thin = books([("2010", "0.004"), ("2001", "1")], [("2000", "1")])
        self.assertEqual(fit_clip(config(), INSTRUMENTS, thin, "entry", D("0.01"), Leg(), Leg()),
                         (D("0.005"), (D("2008.2"), D("2000"))))

    def test_exits_may_go_below_the_minimum_clip(self):
        short, long = Leg("-0.01", "2010"), Leg("0.01", "2000")
        exit_books = {"aster": Ticker(D("2000"), D("2000.5"), 0, None, levels(("2000", "1")),
                                      levels(("2000.5", "0.006"), ("2030", "1"))),
                      "hyperliquid": Ticker(D("2000"), D("2000.5"), 0, None, levels(("2000", "1")),
                                            levels(("2000.5", "1")))}
        quantity, _ = fit_clip(config(min_clip_notional_usd=D("100")), INSTRUMENTS, exit_books, "exit", D("0.01"),
                               short, long)
        self.assertEqual(quantity, D("0.006"))


class EngineSizingTests(unittest.IsolatedAsyncioTestCase):
    async def test_maker_full_size_also_obeys_minimum_clip(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, execution_method="maker_taker", maker_venue="aster",
                        min_clip_notional_usd=D("30"))
            h.wide()
            await h.engine.start()
            await h.engine.step()
            self.assertFalse(h.engine.quotes())

    async def test_same_cached_book_not_reused_but_new_quote_bypasses_fixed_wait(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, clip_interval_seconds=0)
            h.wide()
            await h.engine.start()
            await h.engine.step()
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.01"), D("0.01")))
            h.clock.advance(1)  # less than the former 1-second interval
            h.wide()
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.02"), D("0.02")))

    async def test_ioc_marginal_limits_allow_profitable_vwap_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.feed.update("aster", D("2010"), D("2010.5"), source_ms=h.clock(), received_ms=h.clock(),
                          bids=levels(("2010", "0.009"), ("2005", "1")), asks=levels(("2010.5", "1")))
            h.feed.update("hyperliquid", D("1999.5"), D("2000"), source_ms=h.clock(), received_ms=h.clock(),
                          bids=levels(("1999.5", "1")), asks=levels(("2000", "1")))
            await h.engine.start()
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.01"), D("0.01")))
            self.assertEqual(D(h.state.short.average), D("2009.5"))
            event = next(e for e in h.events if e["event"] == "clip_triggered")
            self.assertEqual(event["limits"], ["2005", "2000"])

    def thin(self, h, short_depth="0.006"):
        h.feed.update("aster", D("2010"), D("2010.5"), source_ms=h.clock(), received_ms=h.clock(),
                      bids=levels(("2010", short_depth), ("2001", "1")), asks=levels(("2010.5", "1")))
        h.feed.update("hyperliquid", D("1999.5"), D("2000"), source_ms=h.clock(), received_ms=h.clock(),
                      bids=levels(("1999.5", "1")), asks=levels(("2000", "1")))

    async def test_clip_resized_then_rechecked_each_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            self.thin(h)
            await h.engine.start()
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.006"), D("0.006")))
            self.assertTrue(any(e["event"] == "clip_resized" for e in h.events))
            # The fills consumed the profitable level; the next clip is priced again from the book,
            # which now shows only the unprofitable 2001 bid: nothing more is opened.
            h.feed.update("aster", D("2001"), D("2010.5"), source_ms=h.clock(), received_ms=h.clock(),
                          bids=levels(("2001", "1")), asks=levels(("2010.5", "1")))
            h.feed.update("hyperliquid", D("1999.5"), D("2000"), source_ms=h.clock(), received_ms=h.clock(),
                          bids=levels(("1999.5", "1")), asks=levels(("2000", "1")))
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.006"), D("0.006")))

    async def test_clip_interval_paces_clips(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, clip_interval_seconds=1)
            h.wide()
            await h.engine.start()
            await h.engine.step()
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.01"), D("0.01")), "second clip waits for the interval")
            h.clock.advance(1_000)
            h.wide()
            await h.engine.step()
            self.assertEqual(h.legs(), (D("-0.02"), D("0.02")))

    async def test_spread_moving_mid_order_leaves_no_open_leg(self):
        class Gone(PaperVenue):
            """The book moves away between the decision and the order: the IOC limit finds nothing."""
            async def place_market_order(self, **kwargs):
                return {"ok": True, "terminal": True, "order_id": "x", "filled_quantity": "0"}
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.adapters["hyperliquid"] = Gone("hyperliquid", h.feed)
            h.wide()
            await h.engine.start()
            await h.engine.step()
            # The short filled, the long did not: the short is unwound in the same step.
            self.assertEqual(h.legs(), (D("0"), D("0")))
            self.assertEqual(h.engine.failure_count, 0, "an unfilled IOC is not a failure")
            self.assertEqual(h.state.status, "RUNNING")

    async def test_maker_quote_shrinks_to_hedgeable_depth(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, execution_method="maker_taker", maker_venue="aster")
            h.feed.update("aster", D("2000"), D("2000.5"), source_ms=h.clock(), received_ms=h.clock(),
                          bids=levels(("2000", "1")), asks=levels(("2000.5", "1")))
            h.feed.update("hyperliquid", D("1999.5"), D("2000"), source_ms=h.clock(), received_ms=h.clock(),
                          bids=levels(("1999.5", "1")), asks=levels(("2000", "0.007")))
            await h.engine.start()
            await h.engine.step()
            self.assertEqual(D(h.engine.quotes()["entry"].quantity), D("0.007"))


class MarketWakeTests(unittest.IsolatedAsyncioTestCase):
    async def test_feed_update_wakes_before_housekeeping_timer(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            revision = h.feed.revision
            waiting = asyncio.create_task(h.feed.wait_for_update(revision, 60))
            await asyncio.sleep(0)
            h.wide()
            result = await asyncio.wait_for(waiting, 0.2)
            self.assertNotEqual(result, revision)

    async def test_update_during_execution_is_not_lost_before_wait(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            revision = h.feed.revision
            h.wide()
            self.assertNotEqual(await asyncio.wait_for(h.feed.wait_for_update(revision, 60), 0.2), revision)

    async def test_store_feed_depth_update_wakes_group(self):
        store = QuoteStore()
        feed = StoreFeed(config(), store, {"aster": True, "hyperliquid": True})
        revision = feed.revision
        waiting = asyncio.create_task(feed.wait_for_update(revision, 60))
        await asyncio.sleep(0)
        store.update_depth("aster", "ETH", levels(("2010", "1")), levels(("2011", "1")), source_ms=None)
        self.assertNotEqual(await asyncio.wait_for(waiting, 0.2), revision)

    async def test_no_update_timer_and_cancellation_do_not_leak_waiters(self):
        store = QuoteStore()
        feed = StoreFeed(config(), store, {})
        self.assertEqual(await feed.wait_for_update(feed.revision, 0.001), feed.revision)
        waiting = asyncio.create_task(feed.wait_for_update(feed.revision, 60))
        await asyncio.sleep(0)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting

    async def test_new_quote_wakes_dispatcher_scan(self):
        store = QuoteStore()
        waiting = asyncio.create_task(store.wait_for_scan_update(store.revision, 60))
        await asyncio.sleep(0)
        store.update_quotes("aster", {"ETH": {"bid": 2010, "ask": 2011}})
        await asyncio.wait_for(waiting, 0.2)

    async def test_other_symbol_venue_and_scan_only_do_not_wake_active_pair(self):
        store = QuoteStore()
        feed = StoreFeed(config(), store, {})
        revision = feed.revision
        waiting = asyncio.create_task(feed.wait_for_update(revision, 60))
        await asyncio.sleep(0)
        store.update_quotes("aster", {"BTC": {"bid": 100, "ask": 101}})
        store.update_quotes("mexc", {"ETH": {"bid": 100, "ask": 101}})
        store.update_quotes("aster", {"ETH": {"bid": 100, "ask": 101, "scan_only": True}})
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        self.assertEqual(feed.revision, revision)
        store.update_quotes("hyperliquid", {"ETH": {"bid": 100, "ask": 101}})
        self.assertNotEqual(await asyncio.wait_for(waiting, 0.2), revision)


if __name__ == "__main__":
    unittest.main()
