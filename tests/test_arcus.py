"""Arcus: Ed25519-signed REST orders, integer ticks/quantums, microsecond timestamps."""
import json
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from hydra_basis.adapters.arcus import arcus_market_name, fetch_arcus_funding_since
from hydra_basis.adapters.registry import FETCHERS, FETCHERS_SINCE, SYMBOL_DISCOVERERS
from hydra_basis.execution_engine.arcus_adapter import ArcusExecutionAdapter, ed25519_signer, tick_for_price
from hydra_basis.spread_strategy.broker import (
    MARGIN_ERROR, POST_ONLY_CROSS, definitive_rejection, parse_status,
)
from hydra_basis.spread_strategy.feeds import parse_arcus_bbo

# RFC 8032 section 7.1, TEST 1 (empty message).
RFC_SECRET = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
RFC_PUBLIC = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
RFC_SIGNATURE = ("e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065"
                 "224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b")
ADDRESS = "0xAbC0000000000000000000000000000000000001"
MARKET = {"marketDisplayName": "ETH-USD", "marketId": 2, "status": "ONLINE", "baseAsset": "ETH",
          "tickSize": "0.01", "stepSize": "0.0001",
          "tickTiers": [{"upToPrice": "50000", "tick": "0.01"}, {"tick": "0.1"}]}


def adapter():
    a = ArcusExecutionAdapter(private_key=RFC_SECRET, address=ADDRESS, account_index=0, leverage=2)
    a._markets = {"ETH": MARKET}
    return a


def verify(message: str, signature_hex: str):
    public = ECC.construct(curve="Ed25519", seed=bytes.fromhex(RFC_SECRET)).public_key()
    eddsa.new(public, "rfc8032").verify(message.encode(), bytes.fromhex(signature_hex))


class SigningTests(unittest.TestCase):
    def test_rfc8032_vector(self):
        key, signer = ed25519_signer(RFC_SECRET)
        self.assertEqual(key.public_key().export_key(format="raw").hex(), RFC_PUBLIC)
        self.assertEqual(signer.sign(b"").hex(), RFC_SIGNATURE)
        self.assertEqual(adapter().api_key, RFC_PUBLIC)

    def test_timestamps_strictly_increase(self):
        a, b = adapter(), adapter()
        stamps = [x._timestamp_ns() for x in (a, b) * 50]
        self.assertEqual(stamps, sorted(set(stamps)))


class OrderTests(unittest.IsolatedAsyncioTestCase):
    async def place(self, response, **kwargs):
        a = adapter()
        a._leverage_set.add(2)
        a._request = AsyncMock(return_value=response)
        result = await a.place_limit_order(symbol="ETH", side="SELL", amount="0.05", clip_usd=125,
                                           price="2500.123", **kwargs)
        _, kwargs_ = a._request.await_args.args, a._request.await_args.kwargs
        return a, result, kwargs_

    async def test_place_order_payload_and_signature(self):
        a, result, call = await self.place((202, {"orderId": "abc123", "status": "ACK"}), post_only=True)
        body, headers = call["body"], call["headers"]
        ts = int(headers["X-Timestamp"])
        # SELL rounds the price up to the tick; integers use the top-level tickSize / stepSize.
        self.assertEqual((body["price"], body["quantity"], body["timeInForce"]), ("2500.13", "0.05", "ALO"))
        payload = json.loads(json.dumps({
            "ad": ADDRESS.lower(), "ai": 0, "ct": ts, "g": int(body["goodTilTime"]) * 1000, "m": 2, "op": 1,
            "p": 250013, "q": 500, "r": 0, "s": 1, "t": 3, "v": 1}))
        message = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        verify(message, headers["X-Signature"])
        self.assertEqual((headers["X-API-Key"], body["timestamp"]), (RFC_PUBLIC, ts))
        self.assertEqual((result["order_id"], result["status"]), ("abc123", "ACK"))

    async def test_post_only_cross_is_a_definitive_benign_rejection(self):
        with self.assertRaises(RuntimeError) as raised:
            await self.place((200, {"orderId": "x", "status": "REJECTED",
                                    "rejectionReason": "POST_ONLY_WOULD_CROSS"}), post_only=True)
        self.assertTrue(POST_ONLY_CROSS.search(str(raised.exception)))
        self.assertTrue(definitive_rejection(raised.exception, raised.exception.order_result))

    async def test_market_order_ioc_and_zero_fill(self):
        a = adapter()
        a._leverage_set.add(2)
        a._request = AsyncMock(side_effect=[
            (200, {"bestBid": {"price": "2499.9", "size": "1"}, "bestAsk": {"price": "2500", "size": "1"}}),
            (200, {"orderId": "m1", "status": "REJECTED", "rejectionReason": "IOC_CANCELED"}),
        ])
        result = await a.place_market_order(symbol="ETH", side="BUY", amount="0.01", clip_usd=25)
        body = a._request.await_args_list[1].kwargs["body"]
        self.assertEqual((body["timeInForce"], body["price"]), ("IOC", "2512.5"))  # +50 bps, rounded up
        self.assertEqual(parse_status(result).filled, D("0"))
        self.assertTrue(parse_status(result).terminal)

    async def test_cancel_payload(self):
        a = adapter()
        a._request = AsyncMock(side_effect=[
            (200, {"status": "CANCEL_ACKNOWLEDGED"}),
            (200, {"orderId": "abc", "status": "CANCELED", "filledSize": "0.01"}),
        ])
        result = await a.cancel_order(order_result={"order_id": "abc"}, symbol="ETH", side="BUY", amount="0.05")
        call = a._request.await_args_list[0].kwargs
        ts = int(call["headers"]["X-Timestamp"])
        verify(f'{{"ad":"{ADDRESS.lower()}","ai":0,"ct":{ts},"id":"abc","m":2,"op":2,"v":1}}',
               call["headers"]["X-Signature"])
        status = parse_status(result)
        self.assertEqual((status.filled, status.state), (D("0.01"), "CANCELED"))

    async def test_set_leverage_uses_legacy_message(self):
        a = adapter()
        a._request = AsyncMock(return_value=(200, {"status": "APPLIED"}))
        await a.ensure_leverage("ETH")
        await a.ensure_leverage("ETH")  # cached: setLeverage weighs 125 on the IP budget
        self.assertEqual(a._request.await_count, 1)
        call = a._request.await_args.kwargs
        ts = call["headers"]["X-Timestamp"]
        body = '{"accountIndex":0,"address":"' + ADDRESS + '","isolated":true,"leverage":2,"marketId":2}'
        verify(f"{ts}setLeverage{body}", call["headers"]["X-Signature"])

    async def test_order_status_mapping(self):
        a = adapter()
        a._request = AsyncMock(return_value=(200, {"orderId": "o", "status": "PARTIALLY_FILLED",
                                                   "filledSize": "0.02", "avgFillPrice": "2501"}))
        status = parse_status(await a.get_order_execution(order_result={"order_id": "o"}, symbol="ETH"))
        self.assertEqual((status.filled, status.average, status.terminal), (D("0.02"), D("2501"), False))
        a._request = AsyncMock(return_value=(200, {"orderId": "o", "status": "MARGIN_CANCELED", "filledSize": "0"}))
        status = parse_status(await a.get_order_execution(order_result={"order_id": "o"}, symbol="ETH"))
        self.assertEqual((status.terminal, status.state), (True, "CANCELED"))
        a._request = AsyncMock(side_effect=RuntimeError("arcus order status 404: {'error': 'not found'}"))
        self.assertFalse((await a.get_order_execution(order_result={"order_id": "o"}, symbol="ETH"))["terminal"])

    async def test_positions_keyed_by_market_or_listed(self):
        a = adapter()
        row = {"marketDisplayName": "ETH-USD", "size": "-0.5", "side": "SHORT"}
        for payload in ({"positions": {"2": row}}, {"positions": [row]}):
            a._request = AsyncMock(return_value=(200, payload))
            position = await a.get_open_position(symbol="ETH", market_type="perp")
            self.assertEqual((position["side"], position["quantity"], position["symbol"]), ("SHORT", "0.5", "ETH"))
        a._request = AsyncMock(return_value=(200, {"freeCollateral": "321.5"}))
        self.assertEqual(await a.get_available_margin(), D("321.5"))

    def test_error_classification(self):
        self.assertTrue(definitive_rejection(RuntimeError("arcus order 400: {'errorType': 'Tick'}"), None))
        self.assertFalse(definitive_rejection(RuntimeError("arcus order 503: unavailable"), None))
        self.assertTrue(MARGIN_ERROR.search("arcus order rejected: UNDERCOLLATERALIZED"))

    def test_price_tiers(self):
        self.assertEqual(tick_for_price(MARKET, D("2500")), D("0.01"))
        self.assertEqual(tick_for_price(MARKET, D("60000")), D("0.1"))


class DataTests(unittest.IsolatedAsyncioTestCase):
    def test_registered(self):
        for registry in (FETCHERS, FETCHERS_SINCE, SYMBOL_DISCOVERERS):
            self.assertIn("arcus", registry)
        self.assertEqual((arcus_market_name("eth"), arcus_market_name("BTC-USD")), ("ETH-USD", "BTC-USD"))

    async def test_funding_history_pages_backwards(self):
        hour, base = 3_600_000_000, 1_791_000_000_000_000  # API times are epoch microseconds
        first = [{"fundingRate": "0.0000125", "time": base + (2000 - n) * hour} for n in range(1000)]
        second = [{"fundingRate": "0.00001", "time": base + (1000 - n) * hour} for n in range(5)]
        fetch = AsyncMock(side_effect=[{"fundingRates": first}, {"fundingRates": second}])
        with patch("hydra_basis.adapters.arcus.fetch_json", new=fetch):
            points = await fetch_arcus_funding_since(object(), "ETH", start_time_ms=(base + 995 * hour) // 1000)
        self.assertEqual(len(points), 1005)
        self.assertEqual(points[0].ts_ms, (base + 996 * hour) // 1000)
        self.assertEqual(points[-1].interval_hours, 1.0)
        second_call = fetch.await_args_list[1].kwargs["params"]
        self.assertEqual((second_call["market"], second_call["to"]), ("ETH-USD", base + 1001 * hour - 1000))

    def test_bbo_frame(self):
        frame = {"type": "channel_data", "channel": "bbo", "id": "ETH-USD", "contents": {
            "bestBid": {"price": "2495.65", "size": "1"}, "bestAsk": {"price": "2495.66", "size": "2"},
            "timestamp": 1791526601788875}}
        book = parse_arcus_bbo(frame)
        self.assertEqual((book["symbol"], book["bid"], book["ask"], book["ts_ms"]),
                         ("ETH", D("2495.65"), D("2495.66"), 1791526601788))
        self.assertEqual((book["bids"], book["asks"]), (((D("2495.65"), D("1")),), ((D("2495.66"), D("2")),)))
        self.assertIsNone(parse_arcus_bbo({"type": "channel_data", "channel": "trades"}))


if __name__ == "__main__":
    unittest.main()
