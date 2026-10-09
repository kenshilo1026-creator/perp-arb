"""Clips shrink to the size the books fill profitably; every clip is re-priced; no leftover legs."""
import tempfile
import unittest
from decimal import Decimal as D

from hydra_basis.spread_strategy.broker import PaperVenue
from hydra_basis.spread_strategy.core import Leg
from hydra_basis.spread_strategy.engine import fit_clip
from hydra_basis.spread_strategy.feeds import Ticker
from test_spread_strategy import INSTRUMENTS, Harness, config


def levels(*rows):
    return tuple((D(p), D(s)) for p, s in rows)


def books(short_bids, long_asks):
    return {"aster": Ticker(D(short_bids[0][0]), D(short_bids[0][0]) + D("0.5"), 0, None, levels(*short_bids),
                            levels((str(D(short_bids[0][0]) + D("0.5")), "100"))),
            "hyperliquid": Ticker(D(long_asks[0][0]) - D("0.5"), D(long_asks[0][0]), 0, None,
                                  levels((str(D(long_asks[0][0]) - D("0.5")), "100")), levels(*long_asks))}


class FitTests(unittest.TestCase):
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
        # Hyperliquid's 10 USD minimum: 0.004 ETH (8 USD) can never be an order.
        thin = books([("2010", "0.004"), ("2001", "1")], [("2000", "1")])
        self.assertIsNone(fit_clip(config(), INSTRUMENTS, thin, "entry", D("0.01"), Leg(), Leg()))

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


if __name__ == "__main__":
    unittest.main()
