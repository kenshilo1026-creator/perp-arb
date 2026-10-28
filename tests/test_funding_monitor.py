import asyncio
import io
import time
import unittest
from contextlib import ExitStack, redirect_stdout, redirect_stderr
from unittest.mock import AsyncMock, patch

from scripts import run_funding_monitor as monitor
from hydra_basis.funding_engine.models import FundingConfig


CONFIG = {name: FundingConfig(name) for name in ("aster", "hyperliquid", "lighter")}


class MonitorArgumentsTests(unittest.TestCase):
    def test_required_venue_and_legacy_days(self):
        with patch.object(monitor, "VENUE_CONFIG", CONFIG):
            self.assertEqual(monitor.parse_args([]).analysis_days, 7)
            self.assertIsNone(monitor.parse_args(["14"]).required_venue)
            args = monitor.parse_args(["14", "--required-venue", "ASTER"])
            self.assertEqual((args.analysis_days, args.required_venue), (14, "aster"))
            self.assertEqual(monitor.parse_args(["--required-venue", "aster"]).analysis_days, 7)

    def test_invalid_or_disabled_venue_and_nonpositive_days_are_rejected(self):
        config = {**CONFIG, "mexc": FundingConfig("mexc", enabled=False)}
        with patch.object(monitor, "VENUE_CONFIG", config), redirect_stderr(io.StringIO()):
            for argv in (["--required-venue", "unknown"], ["--required-venue", "mexc"], ["0"]):
                with self.subTest(argv=argv), self.assertRaises(SystemExit) as raised:
                    monitor.parse_args(argv)
                self.assertEqual(raised.exception.code, 2)


class MonitorFilterTests(unittest.IsolatedAsyncioTestCase):
    async def run_monitor(self, required=None, aster_history=True, *, history=None, listings=None, config=None, days=7, entry_books=None):
        config = config or CONFIG
        if history is None:
            history = {(venue, "BTC"): [venue] for venue in CONFIG if venue != "aster" or aster_history}
            history.update({(venue, "OTHER"): [venue] for venue in ("hyperliquid", "lighter")})
        if listings is None:
            listings = {venue: {symbol for v, symbol in history if v == venue} for venue in config}
        discoverers = {venue: AsyncMock(side_effect=value) if isinstance(value, Exception)
                      else AsyncMock(return_value=value) for venue, value in listings.items()}
        def stats(*args, **kwargs):
            self.assertEqual(kwargs["analysis_days"], days)
            if any(not points for points in args):
                return None
            # Excluded pairs deliberately rank higher than Aster pairs.
            return {"avg_hourly": 0.001, "annualized_avg": 1 if any("aster" in points for points in args) else 3,
                    "positive_ratio": 1, "score": 1, "signal": True}
        stdout = io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(patch.object(monitor, "VENUE_CONFIG", config))
            stack.enter_context(patch.object(monitor, "SYMBOL_DISCOVERERS", discoverers))
            history_store = stack.enter_context(patch.object(monitor, "FundingHistoryStore"))
            history_store.return_value.load.return_value = history
            stack.enter_context(patch.object(monitor, "fetch_monitor_entry_books", new_callable=AsyncMock, return_value=entry_books or {}))
            stack.enter_context(patch.object(monitor, "merge_points_by_interval_bucket", side_effect=lambda points: points))
            stack.enter_context(patch.object(monitor, "trim_points_to_analysis_days", side_effect=lambda points, **kw: points))
            stack.enter_context(patch.object(monitor, "funding_history_is_complete", side_effect=lambda points, **kw: bool(points)))
            analyze = stack.enter_context(patch.object(monitor, "analyze_spread", side_effect=stats))
            stack.enter_context(patch.object(monitor, "explain_spread_skip", return_value="missing_points"))
            stack.enter_context(patch.object(monitor, "analyze_positive_funding", side_effect=stats))
            save = stack.enter_context(patch.object(monitor, "save_monitor_signals"))
            send = stack.enter_context(patch.object(monitor, "send_telegram", new_callable=AsyncMock))
            stack.enter_context(redirect_stdout(stdout))
            await monitor.run_once(analysis_days=days, required_venue=required)
        return save.call_args.kwargs["cross_exchange_signals"], send, stdout.getvalue(), analyze

    async def test_required_venue_filters_before_ranking_for_all_outputs(self):
        signals, send, output, analyze = await self.run_monitor("aster")
        self.assertEqual({(s["short_venue"], s["long_venue"]) for s in signals}, {
            ("aster", "hyperliquid"), ("hyperliquid", "aster"), ("aster", "lighter"), ("lighter", "aster")})
        self.assertEqual(analyze.call_count, 4)
        self.assertEqual({s["symbol"] for s in signals}, {"BTC"})
        self.assertNotIn("SHORT hyperliquid / LONG lighter", output)
        self.assertNotIn("SHORT lighter / LONG hyperliquid", output)
        self.assertNotIn("SPOT-PERP hyperliquid", output)
        self.assertNotIn("OTHER", output)
        send.assert_awaited_once()
        digest = send.call_args.args[0]
        self.assertIn("aster", digest)
        for line in digest.splitlines():
            if "跨所 |" in line or "SPOT-PERP |" in line:
                self.assertIn("aster", line)

    async def test_omitted_option_retains_all_venue_pairs(self):
        signals, send, output, analyze = await self.run_monitor()
        self.assertEqual(len(signals), 8)
        self.assertIn(("hyperliquid", "lighter"), {(s["short_venue"], s["long_venue"]) for s in signals})
        self.assertEqual(analyze.call_count, 8)
        self.assertIn("SPOT-PERP hyperliquid", output)
        send.assert_awaited_once()

    async def test_requested_days_reach_both_cross_and_spot_analysis(self):
        signals, _, _, _ = await self.run_monitor(days=14)
        self.assertEqual(len(signals), 8)

    async def test_final_notification_uses_fresh_directional_entry_filter(self):
        config = {v: FundingConfig(v) for v in ("aster", "variational")}
        history = {(v, "IOTX"): [v] for v in config}
        timestamp = int(time.time() * 1000)
        books = {("aster", "IOTX"): {"bid": 110, "ask": 111, "ts_ms": timestamp},
                 ("variational", "IOTX"): {"bid": 100, "ask": 101, "ts_ms": timestamp}}
        signals, send, output, _ = await self.run_monitor("aster", config=config, history=history, entry_books=books)
        self.assertEqual(len(signals), 2)  # The request changes notification filtering, not funding history.
        digest = send.call_args.args[0]
        self.assertIn("做空 aster / 做多 variational", digest)
        self.assertNotIn("做空 variational / 做多 aster", digest)
        self.assertIn("預估有利價差", digest)
        self.assertIn("reason=maker_taker_gap", output)
        self.assertIn("funding_candidates=2 cross_after_gap_filter_and_ranking=1 entry_unconfirmed=0", output)

    async def test_missing_required_venue_history_returns_empty_not_other_pairs(self):
        signals, send, output, analyze = await self.run_monitor("aster", aster_history=False)
        self.assertEqual(signals, [])
        analyze.assert_not_called()
        send.assert_not_awaited()

    async def test_direct_call_rejects_unknown_venue_before_io(self):
        with patch.object(monitor, "FundingHistoryStore") as store:
            with self.assertRaises(ValueError):
                await monitor.run_once(required_venue="not-a-venue")
            store.assert_not_called()

    async def test_absent_syn_is_excluded_from_pairs_diagnostics_and_saved_signals(self):
        config = {venue: FundingConfig(venue) for venue in ("aster", "variational")}
        history = {(venue, "BTC"): [venue] for venue in config}
        history.update({("aster", "SYN"): [], ("variational", "SYN"): ["variational"]})
        signals, send, output, analyze = await self.run_monitor(
            "aster", history=history, config=config,
            listings={"aster": {"BTC", "SYN"}, "variational": {"BTC"}},
        )
        self.assertIn("listing_skip venue=variational symbol=SYN reason=not_in_current_listing", output)
        self.assertNotIn("SYN SHORT", output)
        self.assertNotIn("missing_points", output)
        self.assertNotIn("history_incomplete", output)
        self.assertNotIn("SYN SPOT-PERP variational", output)
        self.assertEqual({s["symbol"] for s in signals}, {"BTC"})
        self.assertEqual(analyze.call_count, 2)
        self.assertNotIn("SYN", send.call_args.args[0])

    async def test_current_listing_with_missing_history_still_reports_missing_points(self):
        history = {("aster", "SYN"): ["aster"], ("hyperliquid", "SYN"): []}
        signals, _, output, analyze = await self.run_monitor(history=history)
        self.assertEqual(signals, [])
        self.assertEqual(analyze.call_count, 2)
        self.assertEqual(output.count("reason=missing_points"), 2)
        self.assertNotIn("listing_skip", output)

    async def test_discovery_failure_excludes_venue_but_preserves_other_pairs(self):
        signals, _, output, _ = await self.run_monitor(listings={
            "aster": TimeoutError(), "hyperliquid": {"BTC", "OTHER"}, "lighter": {"BTC", "OTHER"},
        })
        self.assertEqual(len(signals), 4)
        self.assertTrue(all("aster" not in (s["short_venue"], s["long_venue"]) for s in signals))
        self.assertIn("listing_unavailable venue=aster error=TimeoutError", output)
        self.assertNotIn("not_in_current_listing", output)
        self.assertNotIn("SPOT-PERP aster", output)

    async def test_required_venue_discovery_failure_does_not_fall_back_to_old_history(self):
        signals, send, output, analyze = await self.run_monitor("aster", listings={
            "aster": RuntimeError("offline"), "hyperliquid": {"BTC", "OTHER"}, "lighter": {"BTC", "OTHER"},
        })
        self.assertEqual(signals, [])
        analyze.assert_not_called()
        send.assert_not_awaited()
        self.assertIn("listing_unavailable venue=aster", output)


class CurrentListingTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_malformed_or_missing_discoverer_is_not_reported_as_delisting(self):
        for discoverers in ({}, {"aster": AsyncMock(return_value=set())},
                            {"aster": AsyncMock(return_value={"bad": "payload"})}):
            with self.subTest(discoverers=discoverers):
                output = io.StringIO()
                with patch.object(monitor, "SYMBOL_DISCOVERERS", discoverers), redirect_stdout(output):
                    result = await monitor.filter_current_listings({"aster": {"SYN"}})
                self.assertEqual(result, {"aster": set()})
                self.assertIn("listing_unavailable", output.getvalue())
                self.assertNotIn("not_in_current_listing", output.getvalue())

    async def test_timeout_is_bounded_and_cancels_discovery(self):
        cancelled = asyncio.Event()
        async def slow(session):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        output = io.StringIO()
        with patch.object(monitor, "SYMBOL_DISCOVERERS", {"aster": slow}), \
                patch.object(monitor, "LISTING_TIMEOUT_SECONDS", 0.01), redirect_stdout(output):
            result = await monitor.filter_current_listings({"aster": {"SYN"}})
        self.assertEqual(result, {"aster": set()})
        self.assertTrue(cancelled.is_set())
        self.assertIn("TimeoutError", output.getvalue())

    async def test_disabled_venues_are_not_queried_and_aliases_match_history(self):
        disabled = AsyncMock()
        with patch.object(monitor, "VENUE_CONFIG", {
            "aster": FundingConfig("aster"), "disabled": FundingConfig("disabled", enabled=False),
        }), patch.object(monitor, "SYMBOL_DISCOVERERS", {
            "aster": AsyncMock(return_value={"NATIVE"}), "disabled": disabled,
        }), patch.object(monitor, "canonicalize_symbol", return_value="CANONICAL"):
            result = await monitor.filter_current_listings({"aster": {"CANONICAL"}, "disabled": {"SYN"}})
        self.assertEqual(result, {"aster": {"CANONICAL"}})
        disabled.assert_not_awaited()
