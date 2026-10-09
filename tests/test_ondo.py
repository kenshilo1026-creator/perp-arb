"""Ondo Perps: HMAC-signed REST, -USD.P markets, ISO-nanosecond timestamps, cursor pagination."""
import hashlib
import hmac
import json
import os
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

from hydra_basis.adapters.ondo import (
    fetch_ondo_funding_since, ondo_market_name, ondo_request, ondo_symbol, parse_iso_ms, sign_request,
)
from hydra_basis.adapters.registry import FETCHERS, FETCHERS_SINCE, SYMBOL_DISCOVERERS
from hydra_basis.spread_strategy.broker import POST_ONLY_CROSS, definitive_rejection, parse_status
from hydra_basis.spread_strategy.dispatcher import OndoRunner, QuoteStore
from hydra_basis.spread_strategy.feeds import parse_ondo_books

CREDS = {"ONDO_API_KEY_ID": "ondoKeyId_TEST", "ONDO_API_SECRET": "ondoApiSecret_SECRET"}
MARKET = {"market": "ETH-USD.P", "baseIncrement": "0.001", "quoteIncrement": "0.1"}


class FakeResponse:
    def __init__(self, status, payload):
        self.status, self.payload = status, payload

    async def text(self):
        return json.dumps(self.payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return FakeResponse(*self.responses.pop(0))


class SigningTests(unittest.IsolatedAsyncioTestCase):
    def test_signature_matches_documented_recipe(self):
        expected = hmac.new(b"ondoApiSecret_SECRET", b"1700000000000GET/v1/perps/orders?market=AAPL-USD.P&limit=1000",
                            hashlib.sha256).hexdigest()
        self.assertEqual(sign_request("get", "/v1/perps/orders?market=AAPL-USD.P&limit=1000", "",
                                      "ondoApiSecret_SECRET", 1700000000000), expected)

    async def test_signed_request_signs_exactly_what_is_sent(self):
        session = FakeSession((200, {"success": True, "result": {"orderId": "o1"}}))
        with patch.dict(os.environ, CREDS):
            await ondo_request(session, "POST", "/v1/perps/orders", params={"x": "1"},
                               body={"market": "ETH-USD.P", "size": "0.01"}, auth=True)
        method, url, kwargs = session.calls[0]
        headers, body = kwargs["headers"], kwargs["data"]
        self.assertTrue(url.endswith("/v1/perps/orders?x=1"))
        self.assertEqual(body, '{"market":"ETH-USD.P","size":"0.01"}')
        expected = sign_request("POST", "/v1/perps/orders?x=1", body, CREDS["ONDO_API_SECRET"],
                                int(headers["ONDO-TIMESTAMP"]))
        self.assertEqual((headers["ONDO-KEY-ID"], headers["ONDO-SIGN"]), ("ondoKeyId_TEST", expected))
        self.assertIsNotNone(kwargs["ssl"], "verified against certifi, never disabled")

    async def test_errors_surface_with_codes(self):
        session = FakeSession((400, {"success": False, "error_code": "post_only_has_match", "error": "would match"}))
        with self.assertRaises(RuntimeError) as raised:
            await ondo_request(session, "POST", "/v1/perps/orders", body={}, label="order")
        self.assertTrue(POST_ONLY_CROSS.search(str(raised.exception)))
        self.assertTrue(definitive_rejection(raised.exception, None))
        self.assertFalse(definitive_rejection(RuntimeError("ondo order 500: internal"), None))


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def adapter(self, *results):
        from hydra_basis.execution_engine.ondo_adapter import OndoExecutionAdapter
        with patch.dict(os.environ, CREDS):
            a = OndoExecutionAdapter(leverage=2)
        a._markets = {"ETH-USD.P": MARKET}
        a._leverage_set.add("ETH-USD.P")
        a._call = AsyncMock(side_effect=list(results))
        return a

    async def test_post_only_limit_and_ioc_limit_payloads(self):
        a = self.adapter({"orderId": "o1", "status": "open", "filledSize": "0"},
                         {"orderId": "o2", "status": "canceled", "filledSize": "0", "cancelReason": "immediateOrCancel"})
        placed = await a.place_limit_order(symbol="ETH", side="SELL", amount="0.0105", clip_usd=25, price="2500.04",
                                           post_only=True)
        body = a._call.await_args_list[0].kwargs["body"]
        self.assertEqual((body["side"], body["type"], body["size"], body["price"], body["timeInForce"], body["postOnly"]),
                         ("sell", "limit", "0.01", "2500.1", "GTC", True))  # SELL rounds up, size down to the lot
        self.assertEqual(parse_status(placed).state, "OPEN")
        ioc = await a.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=25, limit_price="2500.09")
        body = a._call.await_args_list[1].kwargs["body"]
        self.assertEqual((body["type"], body["timeInForce"], body["price"]), ("limit", "IOC", "2500"))  # BUY rounds down
        status = parse_status(ioc)
        self.assertEqual((status.terminal, status.filled, status.state), (True, D("0"), "CANCELED"))

    async def test_market_order_and_fill_average(self):
        a = self.adapter({"orderId": "m1", "status": "fullyfilled", "filledSize": "0.02", "filledCost": "50.004"})
        result = await a.place_market_order(symbol="ETH", side="BUY", amount="0.02", clip_usd=50)
        body = a._call.await_args.kwargs["body"]
        self.assertEqual(body["type"], "market")
        self.assertNotIn("price", body)
        status = parse_status(result)
        self.assertEqual((status.filled, status.average, status.state), (D("0.02"), D("2500.2"), "FILLED"))

    async def test_positions_and_balance(self):
        a = self.adapter([{"market": "ETH-USD.P", "direction": "short", "netQuantity": "-0.5"},
                          {"market": "BTC-USD.P", "direction": "neutral", "netQuantity": "0"}],
                         {"availableMargin": "321.5"})
        position = await a.get_open_position(symbol="ETH", market_type="perp")
        self.assertEqual((position["side"], position["quantity"], position["symbol"]), ("SHORT", "0.5", "ETH"))
        self.assertEqual(await a.get_available_margin(), D("321.5"))


class DataTests(unittest.IsolatedAsyncioTestCase):
    def test_names_and_timestamps(self):
        self.assertEqual((ondo_market_name("eth"), ondo_symbol("NVDA-USD.P")), ("ETH-USD.P", "NVDA"))
        self.assertEqual(parse_iso_ms("2026-10-09T08:28:00.486875829Z"), 1791534480486)
        self.assertEqual(parse_iso_ms("1791534480"), 1791534480000)
        for registry in (FETCHERS, FETCHERS_SINCE, SYMBOL_DISCOVERERS):
            self.assertIn("ondo", registry)

    async def test_funding_follows_cursors(self):
        first = {"success": True, "result": [{"time": "2026-10-09T08:00:00Z", "fundingRate": "0.0000125"},
                                             {"time": "2026-10-09T07:00:00Z", "fundingRate": "0.00001"}],
                 "pageInfo": {"nextCursor": "C2"}}
        second = {"success": True, "result": [{"time": "2026-10-09T06:00:00Z", "fundingRate": "-0.00002"}],
                  "pageInfo": {"nextCursor": ""}}
        session = FakeSession((200, first), (200, second))
        points = await fetch_ondo_funding_since(session, "ETH", start_time_ms=0)
        self.assertEqual([p.raw_rate for p in points], [-0.00002, 0.00001, 0.0000125])
        self.assertEqual(points[0].interval_hours, 1.0)
        self.assertIn("cursor=C2", session.calls[1][1])

    def test_book_frames_and_runner(self):
        top = {"type": "update", "channel": "topOfBooksPerps", "data": [
            {"market": "ETH-USD.P", "time": "2026-10-09T08:28:00.4828758Z", "asks": [["2497.2", "0.4"]],
             "bids": [["2497", "0.004"]]}]}
        depth = {"type": "update", "channel": "depthBooksPerps", "data": [
            {"market": "ETH-USD.P", "time": "2026-10-09T08:28:00.9Z", "asks": [["2497.2", "0.4"], ["2497.4", "6"]],
             "bids": [["2497", "0.004"], ["2496.9", "2.8"]]}]}
        self.assertEqual(parse_ondo_books({"type": "subscribed", "data": {"op": "subscribe"}}), [])
        store = QuoteStore(clock=lambda: 1_791_534_481_000)
        runner = OndoRunner(None, store)
        runner.handle(top)
        runner.handle(depth)
        quote = store.get_quote("ondo", "ETH")
        self.assertEqual((quote["bid"], quote["ask"], quote["bids"]), (D("2497"), D("2497.2"), ((D("2497"), D("0.004")),)))
        self.assertEqual(len(store.get_depth("ondo", "ETH")["asks"]), 2)


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_without_api_key_history_uses_recorded_mids(self):
        from hydra_basis.spread_strategy.history import MinuteRecorder, SpreadHistory, fetch_minute_closes
        with patch.dict(os.environ, {"ONDO_API_KEY_ID": "", "ONDO_API_SECRET": ""}):
            self.assertIsNone(await fetch_minute_closes(object(), "ondo", "ETH", 0, 60_000))
            recorder = MinuteRecorder(None)
            recorder.record("ondo", "ETH", 99, 101, 120_000)
            history = SpreadHistory(recorder=recorder, fetcher=fetch_minute_closes, clock=lambda: 180_000)
            series, source = await history._series("ondo", "ETH", 180_000)
            await history.close()
        self.assertEqual((series, source), ({120_000: 100.0}, "recorded"))


if __name__ == "__main__":
    unittest.main()
