from __future__ import annotations

import copy
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, Mock, patch

from hydra_basis.backfill import capture_backfill_spread_snapshot_with_error
from hydra_basis.execution_engine.market_data import fetch_variational_quote, parse_variational_quote
from hydra_basis.funding_engine.models import FundingConfig, FundingPoint
from scripts import backfill_funding_history as runner


def stats(symbols, bid="100"):
    return {"listings": [
        {"ticker": symbol, "quotes": {"size_1k": {"bid": bid, "ask": "102"}}}
        for symbol in symbols
    ]}


class BackfillPerformanceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.symbols = {"BTC", "ETH", "SOL"}
        self.history = Mock()
        self.history.load.return_value = {}
        self.spreads = Mock()
        self.spreads.load.return_value = {}
        self.saved_spreads = []
        self.spreads.save.side_effect = lambda values: self.saved_spreads.append(copy.deepcopy(values))
        self.fetch_history = AsyncMock(return_value=[])
        self.bulk = AsyncMock(return_value=stats(self.symbols))
        self.individual = AsyncMock(return_value={"bid": 100.0, "ask": 102.0, "ts_ms": 123})
        self.sleep = AsyncMock()
        for name, value in {
            "FundingHistoryStore": Mock(return_value=self.history),
            "OrderbookSpreadStore": Mock(return_value=self.spreads),
            "VENUE_CONFIG": {"variational": FundingConfig("variational")},
            "SYMBOL_DISCOVERERS": {"variational": AsyncMock(return_value=self.symbols)},
            "FETCHERS": {"variational": self.fetch_history},
            "FETCHERS_SINCE": {},
            "VARIATIONAL_LORIS_INVALID_SYMBOLS": set(),
            "fetch_json": self.bulk,
            "send_spread_error_alert": AsyncMock(),
            "send_telegram": AsyncMock(),
            "PERSIST_EVERY_N": 2,
        }.items():
            self.stack.enter_context(patch.object(runner, name, value))
        self.stack.enter_context(patch("hydra_basis.backfill.fetch_orderbook_snapshot", self.individual))
        self.stack.enter_context(patch("asyncio.sleep", self.sleep))

    async def test_bulk_quotes_reduce_requests_and_do_not_rewrite_history(self):
        self.bulk.side_effect = [stats(self.symbols, "100"), stats(self.symbols, "101")]
        await runner.run_backfill()
        self.assertEqual(self.bulk.await_count, 2)  # one request per 2-symbol batch
        self.individual.assert_not_called()
        self.history.save.assert_not_called()
        self.assertEqual(len(self.saved_spreads), 2)
        self.assertEqual(self.saved_spreads[-1][("variational", "BTC")]["bid"], 100)
        self.assertEqual(self.saved_spreads[-1][("variational", "SOL")]["bid"], 101)
        self.assertEqual(self.sleep.await_count, 2)  # pace requests, not local parsing

    async def test_funding_updates_keep_original_checkpoints_and_data(self):
        async def fetch(session, symbol):
            return [FundingPoint("variational", symbol, runner.now_ms(), 0.0001, 8.0)]
        self.fetch_history.side_effect = fetch
        await runner.run_backfill()
        self.assertEqual(self.history.save.call_count, 2)  # at 2 dirty keys and batch tail
        saved = self.history.save.call_args.args[0]
        self.assertEqual(set(saved), {("variational", symbol) for symbol in self.symbols})
        self.assertTrue(all(points[0].raw_rate == 0.0001 for points in saved.values()))

    async def test_bulk_failure_falls_back_to_original_per_symbol_path(self):
        self.bulk.side_effect = TimeoutError("stats unavailable")
        await runner.run_backfill()
        self.assertEqual(self.individual.await_count, 3)
        self.assertEqual(set(self.saved_spreads[-1]), {("variational", s) for s in self.symbols})
        self.assertEqual(self.sleep.await_count, 5)  # 2 bulk attempts + 3 paced fallbacks

    async def test_empty_bulk_response_also_uses_fallback(self):
        self.bulk.return_value = {"listings": []}
        await runner.run_backfill()
        self.assertEqual(self.individual.await_count, 3)
        self.assertTrue(all("bid" in value for value in self.saved_spreads[-1].values()))

    async def test_symbols_filter_applies_to_history_and_spreads(self):
        await runner.run_backfill(symbols_filter={"ETH"})
        self.assertEqual(self.fetch_history.await_count, 1)
        self.assertEqual(self.fetch_history.await_args.args[1], "ETH")
        self.assertEqual(set(self.saved_spreads[-1]), {("variational", "ETH")})

    async def test_skip_spreads_does_not_fetch_bulk_or_rewrite_stores(self):
        await runner.run_backfill(skip_spread_refresh=True)
        self.bulk.assert_not_called()
        self.individual.assert_not_called()
        self.spreads.save.assert_not_called()
        self.history.save.assert_not_called()

    async def test_fallback_transient_error_preserves_existing_spread(self):
        existing = {"bid": 98.0, "ask": 99.0, "spread_pct": 0.01, "ts_ms": 1}
        self.spreads.load.return_value = {("variational", "ETH"): existing.copy()}
        self.bulk.side_effect = TimeoutError("bulk unavailable")
        self.individual.side_effect = TimeoutError("timeout")
        await runner.run_backfill(symbols_filter={"ETH"})
        self.assertEqual(self.saved_spreads[-1][("variational", "ETH")], existing)

    async def test_one_bad_quote_does_not_discard_other_symbols(self):
        data = stats(self.symbols)
        next(item for item in data["listings"] if item["ticker"] == "ETH")["quotes"] = {}
        self.bulk.return_value = data
        await runner.run_backfill()
        self.assertIn("bid", self.saved_spreads[-1][("variational", "BTC")])
        self.assertIn("bid", self.saved_spreads[-1][("variational", "SOL")])
        self.assertEqual(self.saved_spreads[-1][("variational", "ETH")], {"status": "no_orderbook"})

    async def test_live_quote_fetch_remains_fresh_and_equivalent(self):
        data = stats(["BTC"])
        with patch("hydra_basis.execution_engine.market_data.fetch_json", new=AsyncMock(return_value=data)) as fetch:
            first = await fetch_variational_quote(None, "BTC", clip_usd=1000)
            second = await fetch_variational_quote(None, "BTC", clip_usd=1000)
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(first, second)
        self.assertEqual(first, parse_variational_quote(data, "BTC", clip_usd=1000))
        spread = {}
        result = await capture_backfill_spread_snapshot_with_error(
            session=None, spreads=spread, venue="variational", symbol="BTC", clip_usd=1000,
            variational_stats=data,
        )
        self.assertTrue(result["stored"])
        self.assertEqual(spread[("variational", "BTC")]["bid"], first["bid"])


if __name__ == "__main__":
    unittest.main()
