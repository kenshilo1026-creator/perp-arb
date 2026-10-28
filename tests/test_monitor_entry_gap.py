import asyncio
import io
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from hydra_basis.alerts import build_ranked_alert_digest, select_best_alerts_by_symbol
from hydra_basis import monitor_quotes


NOW = 1_800_000_000_000


def opportunity(symbol="IOTX", short="variational", long="aster", annualized=0.8):
    return {"symbol": symbol, "short_venue": short, "long_venue": long,
            "stats": {"signal": True, "annualized_avg": annualized, "positive_ratio": .8}}


def book(bid, ask, ts=NOW):
    return {"bid": bid, "ask": ask, "ts_ms": ts}


class EntryGapTests(unittest.TestCase):
    def select(self, quotes, rows=None):
        with redirect_stdout(io.StringIO()):
            return select_best_alerts_by_symbol(rows or [opportunity()], min_annualized_avg=.4,
                                               spreads_by_venue_symbol=quotes, now_ms=NOW)

    def test_iotx_long_maker_under_point_three_passes_even_if_individual_spreads_are_wide(self):
        quotes = {("aster", "IOTX"): book(.00365, .00367),
                  ("hyperliquid", "IOTX"): book(.003642, .00365)}
        result = self.select(quotes, [opportunity(short="hyperliquid")])
        self.assertEqual(len(result), 1)
        entry = result[0]["entry_estimate"]
        self.assertEqual(entry["maker_venue"], "aster")
        self.assertEqual(entry["maker_side"], "BUY")
        self.assertAlmostEqual(entry["adverse_gap_pct"], .000008 / .00365)
        self.assertEqual(entry["maker_price"], .00365)
        self.assertEqual(entry["taker_price"], .003642)
        digest = build_ranked_alert_digest(cross_exchange_alerts=result, spot_perp_alerts=[])
        self.assertIn("Maker aster", digest)
        self.assertIn("0.2192%", digest)

    def test_short_maker_uses_ask_and_taker_buy_uses_ask_with_maker_denominator(self):
        result = self.select({("aster", "IOTX"): book(101, 101.1),
                              ("variational", "IOTX"): book(100, 101)})
        entry = result[0]["entry_estimate"]
        self.assertEqual(entry["maker_venue"], "variational")
        self.assertEqual(entry["maker_side"], "SELL")
        self.assertAlmostEqual(entry["adverse_gap_pct"], .1 / 101)

    def test_tight_individual_spreads_cannot_hide_bad_cross_venue_basis(self):
        self.assertEqual(self.select({("aster", "IOTX"): book(101, 101.01),
                                      ("variational", "IOTX"): book(100, 100.01)}), [])

    def test_exact_point_three_passes_and_over_limit_fails(self):
        for short_bid, expected_count in ((99.7, 1), (99.6999, 0)):
            with self.subTest(short_bid=short_bid):
                result = self.select({("aster", "IOTX"): book(100, 101),
                                      ("hyperliquid", "IOTX"): book(short_bid, 99.8)},
                                     [opportunity(short="hyperliquid")])
                self.assertEqual(len(result), expected_count)

    def test_favorable_entry_and_reversed_trade_direction_are_distinguished(self):
        quotes = {("aster", "IOTX"): book(99, 99.1), ("variational", "IOTX"): book(100, 100.1)}
        result = self.select(quotes)
        self.assertLess(result[0]["entry_estimate"]["adverse_gap_pct"], 0)
        self.assertIn("預估有利價差", build_ranked_alert_digest(cross_exchange_alerts=result, spot_perp_alerts=[]))
        self.assertEqual(self.select(quotes, [opportunity(short="aster", long="variational")]), [])

    def test_variational_taker_configuration_is_not_used_to_pass_notification_filter(self):
        quotes = {("aster", "IOTX"): book(.00365, .00367),
                  ("variational", "IOTX"): book(.003642, .00365)}
        # Aster maker would pass at 0.2192%, but Variational maker is over 0.3%.
        self.assertEqual(self.select(quotes), [])

    def test_variational_long_is_also_always_notification_maker(self):
        result = self.select({("aster", "IOTX"): book(100, 101),
                              ("variational", "IOTX"): book(100, 100.1)},
                             [opportunity(short="aster", long="variational")])
        self.assertEqual(result[0]["entry_estimate"]["maker_venue"], "variational")
        self.assertEqual(result[0]["entry_estimate"]["maker_side"], "BUY")

    def test_missing_stale_undated_and_invalid_quotes_are_unconfirmed_not_zero(self):
        for invalid in ({}, book(100, 101, 0), book(100, 101, NOW - 120001),
                        book(100, 101, NOW + 6000), book(101, 100), book(float("nan"), 100), book(0, 100)):
            with self.subTest(invalid=invalid):
                result = self.select({("aster", "IOTX"): invalid, ("variational", "IOTX"): book(1, 2)})
                self.assertEqual(result[0]["entry_estimate"]["status"], "unconfirmed")
                self.assertNotIn("adverse_gap_pct", result[0]["entry_estimate"])
                self.assertIn("入場價差未確認", build_ranked_alert_digest(cross_exchange_alerts=result, spot_perp_alerts=[]))

    def test_filter_precedes_best_symbol_ranking_and_does_not_mutate_candidates(self):
        rows = [opportunity(long="aster", annualized=1), opportunity(long="hyperliquid", annualized=.8)]
        quotes = {("aster", "IOTX"): book(110, 111), ("hyperliquid", "IOTX"): book(100, 101),
                  ("variational", "IOTX"): book(100, 101)}
        result = self.select(quotes, rows)
        self.assertEqual(result[0]["long_venue"], "hyperliquid")
        self.assertTrue(all("entry_estimate" not in row for row in rows))


class QuoteRefreshTests(unittest.IsolatedAsyncioTestCase):
    def public_stats(self, updated=True):
        quote = {"base": {"bid": "101", "ask": "102"}, "size_1k": {"bid": "100", "ask": "103"}}
        if updated:
            quote["updated_at"] = datetime.fromtimestamp((NOW - 55000) / 1000, timezone.utc).isoformat()
        return {"listings": [{"ticker": "IOTX", "quotes": quote}]}

    async def test_refresh_deduplicates_and_uses_actual_variational_timestamp_and_size(self):
        public = AsyncMock(return_value=self.public_stats())
        fetch = AsyncMock(return_value=book(100, 101, 0))
        rows = [opportunity(), opportunity(short="aster", long="variational"), opportunity("LOW", annualized=.2)]
        with patch.object(monitor_quotes, "fetch_json", public), \
                patch.object(monitor_quotes, "fetch_orderbook_snapshot", fetch), \
                patch.object(monitor_quotes.time, "time", return_value=NOW / 1000):
            result = await monitor_quotes.fetch_monitor_entry_books(rows)
        public.assert_awaited_once()
        fetch.assert_awaited_once()
        self.assertEqual(fetch.call_args.kwargs["clip_usd"], 1000)
        self.assertEqual(set(result), {("aster", "IOTX"), ("variational", "IOTX")})
        self.assertEqual(result[("aster", "IOTX")]["ts_ms"], NOW)
        self.assertEqual(result[("variational", "IOTX")], book(100, 103, NOW - 55000))

    async def test_no_eligible_candidates_does_not_query(self):
        with patch.object(monitor_quotes.aiohttp, "ClientSession") as session:
            self.assertEqual(await monitor_quotes.fetch_monitor_entry_books([opportunity(annualized=.2)]), {})
        session.assert_not_called()

    async def test_failure_and_undated_public_quote_do_not_use_stored_quotes(self):
        with patch.object(monitor_quotes, "fetch_json", AsyncMock(return_value=self.public_stats(updated=False))), \
                patch.object(monitor_quotes, "fetch_orderbook_snapshot", AsyncMock(side_effect=TimeoutError())), \
                redirect_stdout(io.StringIO()):
            result = await monitor_quotes.fetch_monitor_entry_books([opportunity()])
        self.assertEqual(result, {})

    async def test_public_failure_preserves_other_venue_and_timeout_cancels_task(self):
        cancelled = asyncio.Event()
        async def slow(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        with patch.object(monitor_quotes, "fetch_json", slow), \
                patch.object(monitor_quotes, "fetch_orderbook_snapshot", AsyncMock(return_value=book(100, 101))), \
                patch.object(monitor_quotes, "QUOTE_TIMEOUT_SECONDS", .01), redirect_stdout(io.StringIO()):
            result = await monitor_quotes.fetch_monitor_entry_books([opportunity()])
        self.assertTrue(cancelled.is_set())
        self.assertEqual(set(result), {("aster", "IOTX")})
