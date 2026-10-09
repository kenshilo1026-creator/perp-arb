"""Depth-aware pricing and IOC limits at the profit boundary."""
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hydra_basis.spread_strategy import locks
from hydra_basis.spread_strategy.broker import PaperVenue, submit_market
from hydra_basis.spread_strategy.core import entry_allowed, exit_allowed, Leg
from hydra_basis.spread_strategy.dispatcher import Dispatcher, QuoteStore, Settings
from hydra_basis.spread_strategy.feeds import (
    LighterBook, StoreFeed, Ticker, parse_aster_depth, parse_mexc_depth, to_levels, vwap,
)
from test_spread_strategy import INSTRUMENTS, Harness


def levels(*rows):
    return tuple((D(p), D(s)) for p, s in rows)


class DepthMathTests(unittest.TestCase):
    def test_vwap_and_executable(self):
        asks = levels(("100", "1"), ("101", "1"), ("103", "2"))
        self.assertEqual(vwap(asks, D("1")), D("100"))
        self.assertEqual(vwap(asks, D("2")), D("100.5"))
        self.assertEqual(vwap(asks, D("3")), D("304") / 3)
        self.assertIsNone(vwap(asks, D("5")), "deeper than the visible book")
        ticker = Ticker(D("99"), D("100"), 0, None, levels(("99", "0.5")), asks)
        self.assertEqual(ticker.executable("BUY", D("2")), D("100.5"))
        self.assertIsNone(ticker.executable("SELL", D("1")))
        self.assertEqual(Ticker(D("99"), D("100"), 0).executable("SELL", D("9")), D("99"), "no sizes published")

    def test_parsers(self):
        aster = parse_aster_depth({"e": "depthUpdate", "T": 5, "s": "ETHUSDT",
                                   "b": [["2495.57", "5.126"], ["2495.41", "0"]], "a": [["2495.6", "1"]]})
        self.assertEqual((aster["bids"], aster["ts_ms"]), (levels(("2495.57", "5.126")), 5))
        mexc = parse_mexc_depth({"channel": "push.depth.full", "symbol": "ETH_USDT", "ts": 7,
                                 "data": {"bids": [[2561.52, 29344, 1]], "asks": [[2561.53, 7390, 1]]}}, D("0.01"))
        self.assertEqual(mexc["bids"], levels(("2561.52", "293.44")), "contracts converted to ETH")
        self.assertEqual(parse_mexc_depth({"channel": "push.depth.full", "ts": 1, "data": {
            "bids": [[1, 2, 1]], "asks": [[2, 2, 1]]}})["bids"], (), "unknown contract size: no sizes")

    def test_lighter_book_deltas(self):
        book = LighterBook()
        book.apply({"type": "subscribed/order_book", "timestamp": 1, "order_book": {
            "bids": [{"price": "100", "size": "1"}, {"price": "99", "size": "2"}],
            "asks": [{"price": "101", "size": "1"}]}})
        book.apply({"type": "update/order_book", "timestamp": 2, "order_book": {
            "bids": [{"price": "100", "size": "0"}, {"price": "100.5", "size": "3"}], "asks": []}})
        bids, asks = book.levels()
        self.assertEqual(bids, levels(("100.5", "3"), ("99", "2")))
        self.assertEqual((asks, book.ts_ms), (levels(("101", "1")), 2))
        book.apply({"type": "update/order_book", "order_book": {"bids": [{"price": "102", "size": "1"}]}})
        self.assertEqual(book.levels(), ((), ()), "crossed book is unusable")

    def test_store_feed_prefers_fresh_depth(self):
        clock = [10_000]
        store = QuoteStore(clock=lambda: clock[0])
        store.update_quotes("aster", {"ETH": {"bid": 100, "ask": 101, "ts_ms": 10_000,
                                              "bids": levels(("100", "0.1")), "asks": levels(("101", "0.1"))}})
        store.update_depth("aster", "ETH", levels(("100", "0.1"), ("99", "5")), levels(("101", "0.1"), ("102", "5")),
                           source_ms=10_000)
        from test_spread_strategy import config
        feed = StoreFeed(config(), store, {"aster": True}, clock=lambda: clock[0])
        self.assertEqual(feed.fresh("aster").executable("BUY", D("1")), (D("10.1") + D("91.8")) / 1)
        clock[0] += 6_000
        self.assertIsNone(feed.ticker("aster").executable("BUY", D("1")), "stale depth falls back to top of book")


class BoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_entry_limits_sit_on_the_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            short_limit, long_limit = h.engine.boundary_limits("entry", D("2010"), D("2000"))
            self.assertTrue(entry_allowed(h.config, short_limit, long_limit))
            self.assertLess(short_limit, D("2010"))
            self.assertGreater(long_limit, D("2000"))
            # Limits round to the stricter tick (Hyperliquid's 5-significant-figure rule makes its tick 0.1
            # here), so one tick worse on each leg breaks the gate: they are the worst acceptable prices.
            self.assertEqual(long_limit, D("2000.9"))
            self.assertFalse(entry_allowed(h.config, short_limit - D("0.01"), long_limit + D("0.1")))
            self.assertEqual(short_limit, INSTRUMENTS["aster"].round_price(short_limit, "up"))

    def test_exit_limits_sit_on_the_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.state.short, h.state.long = Leg("-0.02", "2010"), Leg("0.02", "2000")
            short_limit, long_limit = h.engine.boundary_limits("exit", D("2000.5"), D("2000"))
            self.assertTrue(exit_allowed(h.config, h.state.short, h.state.long, short_limit, long_limit))
            self.assertGreater(short_limit, D("2000.5"))
            self.assertLess(long_limit, D("2000"))

    async def test_clip_sends_limits_and_thin_books_do_not_trade(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            calls = []
            for venue, adapter in h.adapters.items():
                original = adapter.place_market_order

                async def spy(original=original, **kwargs):
                    calls.append(kwargs)
                    return await original(**kwargs)
                adapter.place_market_order = spy
            h.wide()
            await h.engine.start()
            await h.engine.step()
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(call["limit_price"] for call in calls))
            # Thin books: the visible depth cannot fill the clip, so nothing is sent.
            calls.clear()
            for venue, (bid, ask) in (("aster", ("2010", "2010.5")), ("hyperliquid", ("1999.5", "2000"))):
                h.feed.update(venue, D(bid), D(ask), source_ms=h.clock(), received_ms=h.clock(),
                              bids=levels((bid, "0.001")), asks=levels((ask, "0.001")))
            await h.engine.step()
            self.assertEqual(calls, [])
            self.assertTrue(any(e["event"] == "depth_insufficient" for e in h.events))

    async def test_paper_ioc_limit_fills_only_inside_the_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            h.feed.update("hyperliquid", D("1999"), D("2000"), source_ms=h.clock(), received_ms=h.clock(),
                          bids=levels(("1999", "1")), asks=levels(("2000", "0.004"), ("2001", "1")))
            venue = PaperVenue("hyperliquid", h.feed)
            result = await venue.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=20,
                                                    limit_price="2000.5")
            self.assertEqual((result["filled_quantity"], result["avg_price"]), ("0.004", "2000"))
            none = await venue.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=20,
                                                  limit_price="1999.5")
            self.assertEqual(none["filled_quantity"], "0")


class SubmitTests(unittest.IsolatedAsyncioTestCase):
    async def test_ioc_no_match_is_a_clean_zero_fill(self):
        class Venue:
            supports_limit_ioc = True

            async def get_order_execution(self, **kwargs):
                raise AssertionError("not needed")

            async def place_market_order(self, **kwargs):
                self.kwargs = kwargs
                error = RuntimeError("hyperliquid order error: Order could not immediately match against any resting orders.")
                error.order_result = {"ok": False, "terminal": True, "filled_quantity": "0"}
                raise error
        venue = Venue()
        result = await submit_market(venue, symbol="ETH", side="BUY", quantity=D("0.01"), reduce_only=False,
                                     reference_price=D("2000"), timeout_seconds=1, limit_price=D("2000.50"))
        self.assertEqual((result.state, result.filled), ("CANCELED", D("0")))
        self.assertEqual(venue.kwargs["limit_price"], "2000.5")

    async def test_limit_is_not_sent_to_venues_without_ioc_limits(self):
        class Browser:
            position = D("0")

            async def get_open_position(self, **kwargs):
                return None if self.position == 0 else {"side": "LONG", "quantity": str(self.position)}

            async def place_market_order(self, **kwargs):
                assert "limit_price" not in kwargs
                self.position += D(kwargs["amount"])
                return {"filled": True, "status": "FILLED", "details": {"fill": {"filledBaseAmount": kwargs["amount"]}}}
        result = await submit_market(Browser(), symbol="ETH", side="BUY", quantity=D("0.01"), reduce_only=False,
                                     reference_price=D("2000"), timeout_seconds=1, limit_price=D("2000"))
        self.assertEqual(result.filled, D("0.01"))


class AdapterLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_hyperliquid_limit_rounds_strictly(self):
        from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
        adapter = object.__new__(HyperliquidExecutionAdapter)
        adapter.ensure_isolated_margin = AsyncMock(return_value=1)
        adapter._get_sz_decimals = AsyncMock(return_value=4)
        adapter._get_mid_price = AsyncMock(side_effect=AssertionError("limit given: no mid lookup"))
        adapter._post_order = AsyncMock(return_value={"status": "ok", "response": {"data": {"statuses": [
            {"filled": {"totalSz": "0.01", "avgPx": "2000", "oid": 1}}]}}})
        await adapter.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=20, limit_price="2000.56")
        order = adapter._post_order.await_args.args[0]["orders"][0]
        self.assertEqual((order["p"], order["t"]), ("2000.5", {"limit": {"tif": "Ioc"}}))

    async def test_aster_and_mexc_ioc_payloads(self):
        from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
        from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter
        aster = AsterExecutionAdapter(signer_address="0x1", private_key="0x" + "1" * 64, user_address="0x2")
        aster._resolve_raw_symbol = AsyncMock(return_value="ETHUSDT")
        aster._format_quantity = AsyncMock(return_value="0.01")
        aster.ensure_isolated_margin = AsyncMock()
        aster.ensure_leverage = AsyncMock()
        aster._post_order = AsyncMock(return_value={"orderId": 9})
        await aster.place_market_order(symbol="ETH", side="SELL", amount="0.01", clip_usd=20, limit_price="2010.1")
        body = aster._post_order.await_args.args[0]
        self.assertEqual((body["type"], body["timeInForce"], body["price"]), ("LIMIT", "IOC", "2010.1"))
        mexc = MexcExecutionAdapter(api_key="k", api_secret="s")
        mexc._post_order = AsyncMock(return_value={"data": 5})
        await mexc.place_market_order(symbol="ETH", side="BUY", amount="1", clip_usd=20, limit_price="2000")
        body = mexc._post_order.await_args.args[0]
        self.assertEqual((body["type"], body["price"]), (3, 2000.0))


class LaunchDepthTests(unittest.IsolatedAsyncioTestCase):
    async def test_launch_needs_depth_for_the_clip(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(locks, "LOCK_DIR", Path(tmp) / "locks"):
            venues = ("aster", "hyperliquid", "lighter", "mexc")
            settings = Settings(venues=venues, fees={v: {"maker": D("0"), "taker": D("0")} for v in venues},
                                min_profit_bps=D("0"), slippage_buffer_bps=D("0"), funding_budget_bps=D("0"),
                                confirm_seconds=0, history={"enabled": False}, strategy={"tick_seconds": 0.01})
            store = QuoteStore(clock=lambda: 1_000)
            events = []

            async def loader(venue, symbol):
                from hydra_basis.spread_strategy.instruments import Instrument
                return Instrument(venue, tick_size=D("0.01"), lot_size=D("0.001"), min_size=D("0.001"))
            d = Dispatcher(settings, live=False, data_dir=Path(tmp), registry_path=Path("x"), store=store,
                           clock=lambda: 1_000, emit=events.append, instrument_loader=loader)
            d.health.update({v: True for v in venues})
            d.trade_quote_timeout_seconds = 0.2
            for venue, (bid, ask) in (("aster", ("100.5", "100.55")), ("hyperliquid", ("99.95", "100"))):
                store.update_quotes(venue, {"AAA": {"bid": float(bid), "ask": float(ask), "ts_ms": 1_000,
                                                    "bids": levels((bid, "0.01")), "asks": levels((ask, "0.01"))}})
            await d.scan_once()
            self.assertEqual(d.groups, {})
            self.assertIn("no order size the books fill profitably", events[-1]["error"])


if __name__ == "__main__":
    unittest.main()
