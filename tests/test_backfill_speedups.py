"""Backfill: stale-history classification, deferred rate-limit errors, Loris running alongside the
direct fetches, fewer history rewrites, and bulk/WebSocket top of book for the spread refresh."""
from __future__ import annotations

import asyncio
import os
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, Mock, patch

from hydra_basis.backfill import classify_backfill_key
from hydra_basis.backfill_ws_spreads import collect_ws_top_of_book, fetch_aster_top_of_book
from hydra_basis.funding_engine.models import FundingConfig, FundingPoint
from hydra_basis.spread_strategy.dispatcher import LighterRunner, QuoteStore, lighter_market_shards
from scripts import backfill_funding_history as runner

HOUR = 3_600_000
NOW = 1_800_000_000_000


def hourly(venue, symbol, *, newest_ms, hours):
    return [FundingPoint(venue, symbol, newest_ms - i * HOUR, 0.0001, 1.0) for i in range(hours)]


class RateLimited(Exception):
    status = 429

    def __str__(self):
        return "429, message='Too Many Requests'"


class ClassificationTests(unittest.TestCase):
    def test_stale_but_covered_history_is_a_top_up_not_a_full_backfill(self):
        # Ten days of hourly data whose newest point is three days old (the backfill was offline).
        points = hourly("hyperliquid", "BTC", newest_ms=NOW - 72 * HOUR, hours=240)
        self.assertEqual(classify_backfill_key(points, now_ms=NOW), "top_up")

    def test_fresh_history_is_skipped_and_missing_window_start_is_full(self):
        self.assertEqual(classify_backfill_key(hourly("hl", "BTC", newest_ms=NOW - HOUR // 2, hours=200),
                                               now_ms=NOW), "skip")
        self.assertEqual(classify_backfill_key(hourly("hl", "BTC", newest_ms=NOW - 2 * HOUR, hours=48),
                                               now_ms=NOW), "full")
        self.assertEqual(classify_backfill_key([], now_ms=NOW), "full")


class RunBackfillTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.history = Mock()
        self.history.load.return_value = {}
        self.spreads = Mock()
        self.spreads.load.return_value = {}
        self.telegram = AsyncMock()
        self.direct = AsyncMock()
        self.loris = AsyncMock()
        for name, value in {
            "FundingHistoryStore": Mock(return_value=self.history),
            "OrderbookSpreadStore": Mock(return_value=self.spreads),
            "VENUE_CONFIG": {"aster": FundingConfig("aster"), "variational": FundingConfig("variational")},
            "SYMBOL_DISCOVERERS": {"aster": AsyncMock(return_value={"BTC", "ETH", "SOL"}),
                                   "variational": AsyncMock(return_value={"BTC", "ETH"})},
            "FETCHERS": {"aster": self.direct, "variational": self.loris},
            "FETCHERS_SINCE": {},
            "VARIATIONAL_LORIS_INVALID_SYMBOLS": set(),
            "send_telegram": self.telegram,
            "send_spread_error_alert": AsyncMock(),
            "PERSIST_EVERY_N": 2,
        }.items():
            self.stack.enter_context(patch.object(runner, name, value))

    @staticmethod
    def point(venue, symbol):
        return [FundingPoint(venue, symbol, runner.now_ms(), 0.0001, 8.0)]

    def returning(self, venue):
        async def fetch(session, symbol):
            return self.point(venue, symbol) if venue else []
        return fetch

    async def test_rate_limit_is_reported_after_the_run_instead_of_aborting_it(self):
        async def direct(session, symbol):
            if symbol == "ETH":
                raise RateLimited()
            return self.point("aster", symbol)
        self.direct.side_effect = direct
        self.loris.side_effect = self.returning("variational")
        await runner.run_backfill(skip_spread_refresh=True)
        saved = self.history.save.call_args.args[0]
        self.assertEqual(set(saved), {("aster", "BTC"), ("aster", "SOL"), ("variational", "BTC"),
                                      ("variational", "ETH")})
        self.telegram.assert_awaited_once()
        message = self.telegram.await_args.args[0]
        self.assertIn("aster=1", message)
        self.assertIn("aster ETH", message)

    async def test_no_report_without_deferred_errors(self):
        self.direct.side_effect = self.returning("aster")
        self.loris.side_effect = self.returning(None)
        await runner.run_backfill(skip_spread_refresh=True)
        self.telegram.assert_not_awaited()

    async def test_loris_runs_while_direct_fetches_are_still_in_flight(self):
        loris_started = asyncio.Event()

        async def direct(session, symbol):
            # Completes only once a Loris fetch has started: a sequential run would never finish.
            await asyncio.wait_for(loris_started.wait(), timeout=2)
            return self.point("aster", symbol)

        async def loris(session, symbol):
            loris_started.set()
            return self.point("variational", symbol)
        self.direct.side_effect = direct
        self.loris.side_effect = loris
        await runner.run_backfill(skip_spread_refresh=True)
        self.assertEqual(len(self.history.save.call_args.args[0]), 5)

    async def test_history_is_saved_every_n_updated_keys_and_once_at_the_end(self):
        self.direct.side_effect = self.returning("aster")
        self.loris.side_effect = self.returning("variational")
        await runner.run_backfill(skip_spread_refresh=True)
        self.assertEqual(self.history.save.call_count, 3)  # after 2, after 4, final flush for the 5th

    async def test_stale_history_only_fetches_the_gap(self):
        stale = hourly("aster", "BTC", newest_ms=runner.now_ms() - 72 * HOUR, hours=240)
        self.history.load.return_value = {("aster", "BTC"): stale}
        since = AsyncMock(return_value=[])
        with patch.object(runner, "FETCHERS_SINCE", {"aster": since}), \
                patch.object(runner, "SYMBOL_DISCOVERERS", {"aster": AsyncMock(return_value={"BTC"})}), \
                patch("builtins.print") as printed:
            await runner.run_backfill(skip_spread_refresh=True)
        self.assertEqual(since.await_args.args[2], max(p.ts_ms for p in stale) + 1)
        summaries = [c.args[0] for c in printed.call_args_list if c.args and "backfill summary" in str(c.args[0])]
        self.assertIn("top_up=1 full=0", summaries[0])


class SpreadRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_bulk_quotes_first_rest_only_for_the_rest(self):
        spreads = {("aster", "BAD"): {"status": "invalid_symbol"}}
        store = Mock()
        bulk = AsyncMock(return_value=({("aster", "BTC"): {"bid": 100.0, "ask": 100.1, "ts_ms": 5}}, {}))
        rest = AsyncMock(return_value={"stored": True, "error": None})
        with patch.object(runner, "collect_top_of_book", bulk), \
                patch.object(runner, "capture_spread_snapshot_with_venue_delay", rest), \
                patch.object(runner, "send_spread_error_alert", AsyncMock()):
            await runner.refresh_spreads(session=None, keys=[("aster", "BTC"), ("aster", "ETH"), ("aster", "BAD")],
                                         spreads=spreads, spread_store=store)
        self.assertEqual(bulk.await_args.args[1], {"aster": {"BTC", "ETH"}})  # cached invalid symbols excluded
        self.assertEqual(spreads[("aster", "BTC")]["bid"], 100.0)
        self.assertAlmostEqual(spreads[("aster", "BTC")]["spread_pct"], 0.1 / 100.05, places=9)
        self.assertEqual(sorted(call.kwargs["symbol"] for call in rest.await_args_list), ["BAD", "ETH"])
        self.assertGreaterEqual(store.save.call_count, 2)


class FakeRunner:
    def __init__(self, store, venue, quotes, fail=False):
        self.store, self.venue, self.quotes, self.fail, self.closed = store, venue, list(quotes), fail, False

    async def pump_once(self):
        if self.fail:
            raise RuntimeError("socket refused")
        if self.quotes:
            self.store.update_quotes(self.venue, dict([self.quotes.pop(0)]))
        await asyncio.sleep(0.01)

    async def close(self):
        self.closed = True


class TopOfBookTests(unittest.IsolatedAsyncioTestCase):
    async def test_collects_until_every_symbol_is_quoted_and_reports_failed_venues(self):
        built = []

        async def build(venue, session, store):
            if venue == "ondo":
                runners = [FakeRunner(store, venue, [], fail=True)]
            else:
                runners = [FakeRunner(store, venue, [("XYZ:NVDA", {"bid": "235.3", "ask": "235.4", "ts_ms": 7}),
                                                     ("XYZ:TSLA", {"bid": "0", "ask": "1", "ts_ms": 7})])]
            built.extend(runners)
            return runners
        quotes, errors = await collect_ws_top_of_book(
            None, {"trade_xyz": {"XYZ:NVDA"}, "ondo": {"ETH"}, "aster": {"BTC"}},
            build_runners=build, timeout_seconds=2, settle_seconds=0.5, min_seconds=0)
        self.assertEqual(quotes, {("trade_xyz", "XYZ:NVDA"): {"bid": 235.3, "ask": 235.4, "ts_ms": 7}})
        self.assertIn("socket refused", errors["ondo"])
        self.assertTrue(all(r.closed for r in built))

    async def test_aster_bulk_book_ticker_uses_the_listed_contract_then_its_usdt_twin(self):
        metadata = {"BTC": {"raw_symbol": "BTCUSDT"}, "ETH": {"raw_symbol": "ETHUSD"}, "OLD": {"raw_symbol": "OLDUSDT"}}
        rows = [{"symbol": "BTCUSDT", "bidPrice": "82639.4", "askPrice": "82639.5", "time": 1},
                {"symbol": "ETHUSDT", "bidPrice": "2500", "askPrice": "2500.1", "time": 2}]
        with patch("hydra_basis.adapters.aster.fetch_aster_symbol_metadata", AsyncMock(return_value=metadata)), \
                patch("hydra_basis.backfill_ws_spreads.fetch_json", AsyncMock(return_value=rows)):
            quotes = await fetch_aster_top_of_book(None, {"BTC", "ETH", "OLD"})
        self.assertEqual(quotes, {"BTC": {"bid": 82639.4, "ask": 82639.5, "ts_ms": 1},
                                  "ETH": {"bid": 2500.0, "ask": 2500.1, "ts_ms": 2}})


class LighterTests(unittest.TestCase):
    def test_markets_are_sharded_under_the_per_socket_message_limit(self):
        shards = lighter_market_shards({f"S{i}": i for i in range(213)})
        self.assertEqual([len(s) for s in shards], [150, 63])
        self.assertEqual(sum(len(s) for s in shards), 213)
        self.assertEqual(lighter_market_shards({}), [{}])

    def test_empty_book_ticker_is_ignored(self):
        store = QuoteStore(clock=lambda: 1)
        runner_ = LighterRunner(None, store, {"KIOXIA": 225, "ETH": 0})
        runner_.handle({"channel": "ticker:225", "type": "subscribed/ticker", "timestamp": 1,
                        "ticker": {"a": {"price": "", "size": ""}, "b": {"price": "", "size": ""}}})
        self.assertIsNone(store.get_quote("lighter", "KIOXIA"))
        runner_.handle({"channel": "ticker:0", "type": "update/ticker", "timestamp": 2,
                        "ticker": {"a": {"price": "2500.1", "size": "1"}, "b": {"price": "2500", "size": "2"}}})
        self.assertEqual(str(store.get_quote("lighter", "ETH")["bid"]), "2500")


class LorisTests(unittest.IsolatedAsyncioTestCase):
    async def test_requests_only_the_variational_series(self):
        from hydra_basis.adapters import variational
        stats = {"listings": [{"ticker": "BTC", "funding_rate": "0.1", "funding_interval_s": 28800}]}
        loris = AsyncMock(return_value={"series": {"variational": [{"t": "2026-10-09T00:00:00Z", "y": 1.0}]}})
        variational._VARIATIONAL_STATS_CACHE.clear()
        with patch.dict(os.environ, {"LORIS_USE_NODRIVER": "true"}), \
                patch.object(variational, "fetch_json", AsyncMock(return_value=stats)), \
                patch.object(variational, "fetch_loris_historical_with_nodriver", loris):
            points = await variational.fetch_variational_funding_since(object(), "BTC", start_time_ms=1)
        variational._VARIATIONAL_STATS_CACHE.clear()
        self.assertEqual(loris.await_args.kwargs["exchanges"], "variational")
        self.assertEqual(points[0].raw_rate, 0.0001)

    def test_rate_limit_backoff_is_exponential_with_bounded_jitter(self):
        from hydra_basis.adapters.variational import loris_rate_limit_backoff_seconds
        for attempt, base in ((1, 2), (2, 4), (3, 8), (5, 32), (9, 60)):
            value = loris_rate_limit_backoff_seconds(attempt)
            self.assertGreaterEqual(value, base)
            self.assertLessEqual(value, base * 1.1)


class ArcusRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_funding_retries_rate_limits(self):
        from hydra_basis.adapters import arcus
        page = {"fundingRates": [{"time": 1_790_000_000_000_000, "fundingRate": "0.00001"}]}
        fetch = AsyncMock(side_effect=[RateLimited(), page])
        sleep = AsyncMock()
        with patch.object(arcus, "fetch_json", fetch), patch.object(arcus.asyncio, "sleep", sleep):
            points = await arcus.fetch_arcus_funding_since(None, "BTC", start_time_ms=0)
        self.assertEqual(len(points), 1)
        sleep.assert_awaited_once_with(1.0)

    async def test_persistent_rate_limit_still_raises(self):
        from hydra_basis.adapters import arcus
        with patch.object(arcus, "fetch_json", AsyncMock(side_effect=RateLimited())), \
                patch.object(arcus.asyncio, "sleep", AsyncMock()):
            with self.assertRaises(RateLimited):
                await arcus.fetch_arcus_funding_since(None, "BTC", start_time_ms=0)


if __name__ == "__main__":
    unittest.main()
