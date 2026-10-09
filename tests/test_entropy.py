"""Entropy: Hyperliquid HIP-3 dex "io" (coins like io:OAI)."""
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

from hydra_basis.adapters.entropy import (
    build_entropy_funding_history_payload, entropy_api_coin, fetch_entropy_funding_since,
    list_symbols as list_entropy_symbols,
)
from hydra_basis.adapters.hyperliquid import hyperliquid_asset_id
from hydra_basis.adapters.registry import FETCHERS, FETCHERS_SINCE, SYMBOL_DISCOVERERS
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
from hydra_basis.spread_strategy.dispatcher import EntropyBooksRunner, QuoteStore

KEY = "0x" + "1" * 64


def entropy_adapter():
    adapter = HyperliquidExecutionAdapter(private_key=KEY, account_address="0xabc", dex="io", venue_name="entropy")
    adapter._universe = ["IO:OAI", "IO:ANTH", "IO:SNDK"]
    adapter._sz_decimals = {"IO:OAI": 3, "IO:ANTH": 3, "IO:SNDK": 4}
    adapter._dex_index = 10
    return adapter


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def test_coin_names_and_asset_ids(self):
        adapter = entropy_adapter()
        self.assertEqual(adapter._coin("oai"), "IO:OAI")
        self.assertEqual(adapter._coin("io:OAI"), "IO:OAI")
        self.assertEqual(adapter._symbol("io:SNDK"), "SNDK")
        self.assertEqual(hyperliquid_asset_id(2, 10), 100000 + 10 * 10000 + 2)
        self.assertEqual(hyperliquid_asset_id(5), 5)
        main = HyperliquidExecutionAdapter(private_key=KEY, account_address="0xabc")
        self.assertEqual((main._coin("eth"), main._symbol("ETH")), ("ETH", "ETH"))

    async def test_asset_index_and_size_decimals(self):
        adapter = entropy_adapter()
        self.assertEqual(await adapter._get_asset_index("SNDK"), 100000 + 10 * 10000 + 2)
        self.assertEqual(await adapter._get_sz_decimals("SNDK"), 4)
        with self.assertRaisesRegex(RuntimeError, "entropy symbol not found"):
            await adapter._get_asset_index("BTC")

    async def test_positions_and_state_queries_are_dex_scoped(self):
        adapter = entropy_adapter()
        state = {"assetPositions": [{"position": {"coin": "io:OAI", "szi": "-0.5"}},
                                    {"position": {"coin": "io:SNDK", "szi": "2"}}], "withdrawable": "123.4"}
        with patch("hydra_basis.execution_engine.hyperliquid_adapter.fetch_json",
                   new=AsyncMock(return_value=state)) as fetch:
            position = await adapter.get_open_position(symbol="OAI", market_type="perp")
            self.assertEqual((position["side"], position["quantity"], position["symbol"]), ("SHORT", "0.5", "OAI"))
            self.assertEqual(fetch.await_args.kwargs["json"], {"type": "clearinghouseState", "user": "0xabc",
                                                               "dex": "io"})
            listed = await adapter.list_open_positions()
            self.assertEqual({(p["venue"], p["symbol"]) for p in listed}, {("entropy", "OAI"), ("entropy", "SNDK")})
            self.assertEqual(await adapter.get_available_margin(), D("123.4"))

    async def test_mid_price_uses_dex_and_order_targets_hip3_asset(self):
        adapter = entropy_adapter()
        adapter._isolated_asset_indices = {100000 + 10 * 10000}
        adapter._post_order = AsyncMock(return_value={"status": "ok", "response": {"data": {"statuses": [
            {"filled": {"totalSz": "0.01", "avgPx": "1690", "oid": 7}}]}}})
        with patch("hydra_basis.execution_engine.hyperliquid_adapter.fetch_json",
                   new=AsyncMock(return_value={"io:OAI": "1689.65"})) as fetch:
            await adapter.place_market_order(symbol="OAI", side="BUY", amount="0.0123", clip_usd=20)
        self.assertEqual(fetch.await_args.kwargs["json"], {"type": "allMids", "dex": "io"})
        order = adapter._post_order.await_args.args[0]["orders"][0]
        self.assertEqual((order["a"], order["s"]), (200000, "0.012"))

    def test_nonce_is_unique_across_instances_of_one_signer(self):
        first, second = entropy_adapter(), HyperliquidExecutionAdapter(private_key=KEY, account_address="0xabc")
        nonces = [adapter._next_nonce() for adapter in (first, second) * 50]
        self.assertEqual(len(set(nonces)), len(nonces))
        self.assertEqual(nonces, sorted(nonces))


class FundingTests(unittest.IsolatedAsyncioTestCase):
    def test_registered_and_payloads(self):
        for registry in (FETCHERS, FETCHERS_SINCE, SYMBOL_DISCOVERERS):
            self.assertIn("entropy", registry)
        self.assertEqual(entropy_api_coin("IO:OAI"), "io:OAI")
        self.assertEqual(entropy_api_coin("oai"), "io:OAI")
        self.assertEqual(build_entropy_funding_history_payload("IO:OAI", 123),
                         {"type": "fundingHistory", "coin": "io:OAI", "startTime": 123, "dex": "io"})

    async def test_funding_history_points(self):
        rows = [{"coin": "io:OAI", "fundingRate": "0.0000015625", "time": 3_600_000 * n} for n in range(1, 4)]
        with patch("hydra_basis.adapters.entropy._post_hyperliquid_info", new=AsyncMock(return_value=rows)):
            points = await fetch_entropy_funding_since(object(), "IO:OAI", start_time_ms=0)
        self.assertEqual([(p.venue, p.symbol, p.interval_hours) for p in points], [("entropy", "IO:OAI", 1.0)] * 3)
        meta = {"universe": [{"name": "io:OAI"}, {"name": "io:OLD", "isDelisted": True}]}
        with patch("hydra_basis.adapters.entropy._post_hyperliquid_info", new=AsyncMock(return_value=meta)):
            self.assertEqual(await list_entropy_symbols(object()), {"IO:OAI"})


class ScannerTests(unittest.TestCase):
    def test_books_runner_strips_dex_prefix(self):
        store = QuoteStore(clock=lambda: 1000)
        runner = EntropyBooksRunner(None, store, ["io:OAI"])
        runner.handle({"channel": "l2Book", "data": {"coin": "io:OAI", "time": 990, "levels": [
            [{"px": "1689.5", "sz": "1", "n": 1}], [{"px": "1689.8", "sz": "1", "n": 1}]]}})
        quote = store.get_quote("entropy", "OAI")
        self.assertEqual((quote["bid"], quote["ask"], quote["source_ms"]), (D("1689.5"), D("1689.8"), 990))
        self.assertEqual(quote["bids"], ((D("1689.5"), D("1")),))


if __name__ == "__main__":
    unittest.main()
