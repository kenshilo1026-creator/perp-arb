import asyncio
import hashlib
import hmac
import json
import time
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

import aiohttp

from hydra_basis.execution_engine.binance_spot_adapter import BinanceSpotExecutionAdapter, BinanceSpotAPIError
from hydra_basis.execution_engine.binance_spot_stream import BinanceOrderEventStream, BinanceSpotDepthStream, decode_binance_order
from hydra_basis.execution_engine.mexc_depth_stream import decode_rest_depth, DepthQuoteUnavailable
from hydra_basis.execution_engine.order_stream import OrderStreamError


def metadata():
    return {"symbols": [{"symbol": "ETHUSDT", "status": "TRADING", "baseAsset": "ETH",
        "quoteAsset": "USDT", "isSpotTradingAllowed": True,
        "orderTypes": ["MARKET", "LIMIT", "LIMIT_MAKER"], "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01", "minPrice": "0.01", "maxPrice": "100000"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "100"},
            {"filterType": "MARKET_LOT_SIZE", "stepSize": "0", "minQty": "0", "maxQty": "10"},
            {"filterType": "NOTIONAL", "minNotional": "5", "maxNotional": "10000",
             "applyMinToMarket": True, "applyMaxToMarket": True},
        ]}]}


def adapter():
    a = BinanceSpotExecutionAdapter(api_key="test-key", api_secret="test-secret")
    filters = {f["filterType"]: f for f in metadata()["symbols"][0]["filters"]}
    a._symbol_rules["ETHUSDT"] = {"tick": D("0.01"), "step": D("0.001"), "filters": filters}
    return a


def order(**overrides):
    return {"orderId": 7, "clientOrderId": "pa-test", "symbol": "ETHUSDT", "side": "BUY",
            "status": "FILLED", "origQty": "0.01", "executedQty": "0.01", "cummulativeQuoteQty": "20", **overrides}


def event(**overrides):
    return {"subscriptionId": 0, "event": {"e": "executionReport", "s": "ETHUSDT", "i": 7,
        "c": "pa-test", "S": "BUY", "X": "FILLED", "q": "0.01", "z": "0.01", "Z": "20", "E": 100, **overrides}}


def message(payload):
    return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(payload), json=lambda: payload)


class FakeWS:
    def __init__(self, payloads):
        self.queue = asyncio.Queue()
        for payload in payloads:
            self.queue.put_nowait(message(payload))
        self.sent = []
        self.closed = False

    async def send_json(self, payload):
        self.sent.append(payload)

    async def receive(self):
        return await self.queue.get()

    async def close(self):
        self.closed = True


class BinanceSpotTests(unittest.TestCase):
    def test_binance_credentials_headers_and_sorted_ws_signature(self):
        with patch.dict("os.environ", {"MEXC_API_KEY": "wrong", "MEXC_API_SECRET": "wrong"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "BINANCE_SPOT_API_KEY"):
                BinanceSpotExecutionAdapter()._ensure_credentials()
        a = adapter()
        with patch.object(a, "_timestamp_ms", return_value=1234):
            signed = a._signed_order_params({"symbol": "ETHUSDT", "side": "BUY"})
            signature = signed.pop("signature")
            self.assertEqual(signature, hmac.new(b"test-secret", urlencode(signed).encode(), hashlib.sha256).hexdigest())
            request = a._subscription_request()
        self.assertEqual(a._order_headers(), {"X-MBX-APIKEY": "test-key"})
        self.assertEqual(request["method"], "userDataStream.subscribe.signature")
        params = request["params"]
        signature = params.pop("signature")
        self.assertEqual(signature, hmac.new(b"test-secret", urlencode(sorted(params.items())).encode(), hashlib.sha256).hexdigest())

    def test_filters_reject_instead_of_rounding_one_leg(self):
        a = adapter()
        for amount, price, market in [("0.0009", "2000", False), ("0.0105", "2000", True),
                                      ("11", None, True), ("0.001", "2000", True),
                                      ("0.01", "2000.001", False), ("NaN", "2000", False)]:
            with self.subTest(amount=amount, price=price, market=market), self.assertRaises(ValueError):
                a.validate_order_inputs(symbol="ETH", amount=amount, price=price, market=market)
        a.validate_order_inputs(symbol="ETH", amount="0.01", price="2000", market=True)
        a.validate_order_inputs(symbol="ETHUSDT", amount="0.01", price="2000", market=False)

    def test_decoder_preserves_cumulative_and_quote_and_normalizes_stp_expiry(self):
        self.assertEqual(decode_binance_order(event())["cummulativeQuoteQty"], "20")
        self.assertEqual(decode_binance_order(event(X="EXPIRED_IN_MATCH", z="0.005"))["status"], "EXPIRED")
        with self.assertRaises(OrderStreamError):
            decode_binance_order(event(e="eventStreamTerminated"))

    def test_factory_uses_binance_and_risk_closer_supports_recorded_venue(self):
        from scripts.run_spot_perp_arbitrage import build_spot_perp_adapter
        from scripts.run_risk_manager import _funding_close_adapter_for_leg
        self.assertIsInstance(build_spot_perp_adapter("binance_spot", leverage=1, mode="open"), BinanceSpotExecutionAdapter)
        self.assertIsInstance(_funding_close_adapter_for_leg(SimpleNamespace(venue="binance_spot", market_type="spot")), BinanceSpotExecutionAdapter)


class BinanceSpotAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_wire_signs_binance_header_and_honors_rate_limit_without_retry(self):
        a = adapter()
        calls = []
        class Response:
            status = 429
            headers = {"Retry-After": "60"}
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            async def json(self, **kwargs): return {"code": -1003, "msg": "test-secret"}
        class Session:
            closed = False
            def request(self, method, url, **kwargs):
                calls.append((method, url, kwargs))
                return Response()
        a._session = Session()
        for _ in range(2):
            with self.assertRaises(BinanceSpotAPIError) as caught:
                await a._get_account()
            self.assertNotIn("test-secret", str(caught.exception))
        self.assertEqual(len(calls), 1)
        method, url, options = calls[0]
        self.assertEqual((method, url), ("GET", "https://api.binance.com/api/v3/account"))
        self.assertEqual(options["headers"], {"X-MBX-APIKEY": "test-key"})
        params = dict(options["params"])
        signature = params.pop("signature")
        self.assertEqual(signature, a._sign(urlencode(params)))
        self.assertFalse(options["allow_redirects"])

    async def test_base_asset_fee_reconciles_net_inventory_without_changing_gross_fill(self):
        from hydra_basis.execution_engine.hedge_safety import confirm_pair_execution
        a = adapter()
        a._submitted_orders["7"] = "ETHUSDT"
        trades = [{"symbol": "ETHUSDT", "id": 1, "orderId": 7,
                   "qty": "0.01", "commission": "0.00001", "commissionAsset": "ETH"}]
        a._request = AsyncMock(side_effect=[order(), trades])
        a.get_open_position = AsyncMock(return_value={"side": "LONG", "quantity": "0.00999"})
        perp = SimpleNamespace(get_open_position=AsyncMock(return_value={"side": "SHORT", "quantity": "0.01"}))
        await confirm_pair_execution(a, perp, symbol="ETH", maker_baseline=D(0), taker_baseline=D(0),
                                     maker_side="BUY", quantity=D("0.01"), maker_market_type="spot", attempts=1)
        self.assertEqual(a.last_base_asset_fee, D("0.00001"))
        self.assertEqual(await a.execution_position_adjustment("ETH"), D("-0.00001"))
        self.assertEqual(a._request.await_count, 2)
        a.get_open_position.return_value["quantity"] = "0.009"
        with self.assertRaisesRegex(RuntimeError, "confirmation failed"):
            await confirm_pair_execution(a, perp, symbol="ETH", maker_baseline=D(0), taker_baseline=D(0),
                                         maker_side="BUY", quantity=D("0.01"), maker_market_type="spot", attempts=1)

    async def test_fee_history_must_cover_exact_order_fill(self):
        a = adapter()
        a._submitted_orders["7"] = "ETHUSDT"
        a._request = AsyncMock(side_effect=[order(), []])
        with self.assertRaisesRegex(RuntimeError, "does not cover"):
            await a.execution_position_adjustment("ETH")
        self.assertFalse(a._fee_cache)

    async def test_spot_preflight_filters_run_before_either_leg_submits(self):
        from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, execute_spot_perp_plan
        a = adapter()
        a.prepare_execution = AsyncMock()
        a.place_market_order = AsyncMock()
        maker = SimpleNamespace(prepare_execution=AsyncMock(), place_limit_order=AsyncMock())
        plan = build_spot_perp_plan(symbol="ETH", mode="close", short_venue="aster", quantity=D("0.0105"),
            clip_usd=21, spot_book={"bid": 2000, "ask": 2000.1}, perp_book={"bid": 1999, "ask": 2001})
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[maker, a]):
            with self.assertRaisesRegex(ValueError, "LOT_SIZE.stepSize"):
                await execute_spot_perp_plan(plan=plan, leverage=1)
        maker.place_limit_order.assert_not_awaited()
        a.place_market_order.assert_not_awaited()

    async def test_market_full_reply_needs_no_followup_query_and_uses_base_quantity(self):
        a = adapter()
        async def request(method, path, params):
            self.assertEqual((method, path), ("POST", "/api/v3/order"))
            self.assertEqual(params["quantity"], "0.01")
            self.assertNotIn("quoteOrderQty", params)
            self.assertEqual(params["newOrderRespType"], "FULL")
            return order(clientOrderId=params["newClientOrderId"])
        a._request = AsyncMock(side_effect=request)
        a._get_order = AsyncMock(side_effect=AssertionError("unnecessary status query"))
        result = await a.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=20)
        self.assertEqual(result["raw"]["executedQty"], "0.01")
        self.assertFalse(result["request_timing"]["status_query_performed"])
        self.assertEqual(a._request.await_count, 1)

    async def test_limit_maker_omits_time_in_force_and_sell_direction_is_preserved(self):
        a = adapter()
        async def request(method, path, params):
            self.assertEqual(params["type"], "LIMIT_MAKER")
            self.assertNotIn("timeInForce", params)
            self.assertEqual(params["side"], "SELL")
            return order(clientOrderId=params["newClientOrderId"], status="NEW", executedQty="0")
        a._request = AsyncMock(side_effect=request)
        await a.place_limit_order(symbol="ETH", side="SELL", amount="0.01", price="2000", clip_usd=20, post_only=True)

    async def test_unknown_post_preserves_client_id_without_resubmission(self):
        a = adapter()
        a._request = AsyncMock(side_effect=OrderStreamError("network timeout"))
        with self.assertRaises(OrderStreamError) as caught:
            await a.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=20)
        result = caught.exception.order_result
        self.assertTrue(result["order_id"].startswith("pa-"))
        self.assertEqual(a._request.await_count, 1)
        a._request = AsyncMock(return_value=order(clientOrderId=result["order_id"]))
        status = await a.get_order_execution(order_result=result, symbol="ETH")
        self.assertTrue(status["terminal"])
        self.assertEqual(status["filled_quantity"], "0.01")
        self.assertEqual(a._request.call_args.args[2]["origClientOrderId"], result["order_id"])
        self.assertNotIn("orderId", a._request.call_args.args[2])

    async def test_cancel_unknown_requires_terminal_query(self):
        a = adapter()
        error = BinanceSpotAPIError("DELETE", "/api/v3/order", 400, {"code": -2011})
        a._request = AsyncMock(side_effect=[error, order(status="CANCELED", executedQty="0.005")])
        result = await a.cancel_order(order_result={"order_id": 7}, symbol="ETH", side="BUY", amount="0.01")
        self.assertEqual(result["filled_quantity"], "0.005")
        a._request = AsyncMock(side_effect=[error, order(status="NEW", executedQty="0")])
        with self.assertRaises(BinanceSpotAPIError):
            await a.cancel_order(order_result={"order_id": 7}, symbol="ETH", side="BUY", amount="0.01")

    async def test_wrong_order_identity_and_malformed_ack_fail_closed(self):
        a = adapter()
        a._request = AsyncMock(return_value=order(symbol="BTCUSDT"))
        with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
            await a._get_order({"symbol": "ETHUSDT", "orderId": 7})
        a._request = AsyncMock(return_value={})
        with self.assertRaises(RuntimeError) as caught:
            await a.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=20)
        self.assertIn("order_result", caught.exception.__dict__)

    async def test_preparation_loads_binance_rules_and_time_before_dispatch(self):
        a = BinanceSpotExecutionAdapter(api_key="k", api_secret="s")
        a._request = AsyncMock(side_effect=[{"serverTime": int(time.time()*1000)}, metadata()])
        await a.prepare_execution("ETH")
        self.assertEqual(await a.get_price_tick_size("ETH"), "0.01")
        self.assertEqual([c.args[1] for c in a._request.call_args_list], ["/api/v3/time", "/api/v3/exchangeInfo"])

    async def test_user_ws_accepts_pre_ack_fill_and_never_double_counts(self):
        a = adapter()
        ws = FakeWS([event(), {"id": "spot-orders", "status": 200, "result": {"subscriptionId": 0}}])
        stream = BinanceOrderEventStream(connect=AsyncMock(return_value=ws), subscription=a._subscription_request)
        await stream.start()
        try:
            first = stream.cached_terminal(symbol="ETHUSDT", order_id=7)
            self.assertEqual(first["filled_quantity"], "0.01")
            stream._handle(message(event()))
            self.assertEqual(stream._sequence, 1)
            wrong = event(); wrong["subscriptionId"] = 1
            with self.assertRaises(OrderStreamError):
                stream._handle(message(wrong))
        finally:
            await stream.close()
        self.assertTrue(ws.closed)

    async def test_user_ws_auth_failure_closes_socket_and_does_not_leak_server_echo(self):
        a = adapter()
        ws = FakeWS([{"id": "spot-orders", "status": 401, "error": {"msg": "test-secret"}}])
        stream = BinanceOrderEventStream(connect=AsyncMock(return_value=ws), subscription=a._subscription_request)
        with self.assertRaises(OrderStreamError) as caught:
            await stream.start()
        self.assertNotIn("test-secret", str(caught.exception))
        self.assertTrue(ws.closed)

    async def test_depth_ws_and_bbo_wake_guard_without_refreshing_old_depth(self):
        payload = {"lastUpdateId": 1, "bids": [["2000", "1"]], "asks": [["2001", "1"]]}
        ws = FakeWS([{"stream": "ethusdt@depth20@100ms", "data": payload}])
        stream = BinanceSpotDepthStream(connect=AsyncMock(return_value=ws), symbol="ETHUSDT")
        await stream.start()
        try:
            self.assertEqual(stream.snapshot()["bid"], D("2000"))
            received = stream._received
            stream.publish_ticker({"bid": D("1999"), "ask": D("2000"), "bid_size": D(1), "ask_size": D(1), "version": 2, "source_ms": 0})
            self.assertEqual(stream._received, received)
            self.assertEqual(stream.snapshot()["fast_quote"]["bid"], D("1999"))
            stream._received = time.monotonic() - 5
            with self.assertRaises(DepthQuoteUnavailable):
                stream.snapshot()
            stream.publish(decode_rest_depth(payload))
            self.assertLess(stream.snapshot()["age_ms"], 1000)
        finally:
            await stream.close()


if __name__ == "__main__":
    unittest.main()
