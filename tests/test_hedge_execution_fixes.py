import asyncio
import time
import unittest
from contextlib import asynccontextmanager
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp

from hydra_basis.execution_engine.aster_depth_stream import AsterDepthStream, decode_aster_depth
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides, extract_actual_average_price
from hydra_basis.execution_engine.hedge_safety import execute_confirmed_market_order, terminal_fill_quantity
from hydra_basis.execution_engine.maker_price_guard import MakerPriceGuard, MakerPriceRecheck, create_maker_guard
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.quote_safety import cap_hedge_to_maker, stamp_quote, validate_hedge_limits
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine


def book(bid="100", ask="100.1"):
    return stamp_quote(dict(bid=bid, ask=ask, bids=[(bid, "10")], asks=[(ask, "10")]))


class HedgeExecutionFixTests(unittest.IsolatedAsyncioTestCase):
    async def test_mexc_documented_precision_fields_and_cache(self):
        # Public exchangeInfo shape, deliberately without Binance LOT_SIZE/PRICE_FILTER.
        info = {"symbols": [{"symbol": "BTCUSDT", "quotePrecision": 2,
            "baseAssetPrecision": 8, "baseSizePrecision": "0.000001",
            "quoteAmountPrecision": "1", "filters": [{"filterType": "PERCENT_PRICE_BY_SIDE"}]}]}
        class Response:
            status = 200
            async def json(self, **kw): return info
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
        session = SimpleNamespace(get=lambda *a, **kw: Response())
        class Adapter(MexcSpotExecutionAdapter):
            @asynccontextmanager
            async def _request_session(self): yield session
        adapter = Adapter(api_key="unused", api_secret="unused")
        rules = await adapter._load_symbol_rules("BTC")
        self.assertEqual(rules["tick"], D("0.01"))
        self.assertEqual(rules["step"], D("0.00000001"))
        self.assertEqual(rules["min_qty"], D("0.000001"))
        self.assertIs(rules, await adapter._load_symbol_rules("BTC"))
        with self.assertRaisesRegex(RuntimeError, "notional"):
            await adapter._bounded_order_values("BTC", "BUY", "0.000001", "100")

    async def test_mexc_ioc_partial_cancel_retries_only_remainder_without_delay(self):
        orders = []
        adapter = MexcSpotExecutionAdapter(api_key="unused", api_secret="unused")
        adapter._load_symbol_rules = AsyncMock(return_value={"tick": D("0.1"), "step": D("0.1"), "min_qty": D("0.1")})
        async def post(params):
            orders.append(params)
            qty, quote = ("0.4", "40") if len(orders) == 1 else ("0.6", "61.2")
            return {"orderId": len(orders), "status": "PARTIALLY_CANCELED" if len(orders)==1 else "FILLED",
                    "executedQty": qty, "cummulativeQuoteQty": quote}
        adapter._post_order = post
        adapter._get_order = AsyncMock(return_value={"status": "PARTIALLY_CANCELED", "executedQty": "0.4", "cummulativeQuoteQty": "40"})
        with patch("hydra_basis.execution_engine.hedge_safety.asyncio.sleep", new=AsyncMock()) as sleep:
            result = await execute_confirmed_market_order(adapter, symbol="BTC", side="BUY", quantity=D(1),
                clip_usd=100, baseline=D(0), limit_price="103", market_type="spot")
        self.assertEqual([o["quantity"] for o in orders], ["1", "0.6"])
        self.assertTrue(all(o["type"] == "IMMEDIATE_OR_CANCEL" and "timeInForce" not in o for o in orders))
        self.assertTrue(all(c.args[0] == 0 for c in sleep.await_args_list))
        self.assertEqual(extract_actual_average_price(result), D("101.2"))
        self.assertEqual(len(result["request_timings"]), 2)
        self.assertGreaterEqual(result["first_submit_ns"], result["first_dispatch_ns"])

    async def test_missing_one_fill_price_never_uses_last_order_average(self):
        adapter = SimpleNamespace(place_market_order=AsyncMock(side_effect=[
            {"terminal": True, "filled_quantity": "0.4"},
            {"terminal": True, "filled_quantity": "0.6", "avg_price": "102"},
        ]))
        result = await execute_confirmed_market_order(adapter, symbol="BTC", side="BUY", quantity=D(1),
            clip_usd=100, baseline=D(0), limit_price="103")
        self.assertIsNone(extract_actual_average_price(result))

    async def test_status_query_fill_price_is_used_in_aggregate(self):
        adapter = SimpleNamespace(place_market_order=AsyncMock(return_value={"order_id": 1}),
            get_order_execution=AsyncMock(return_value={"status": "FILLED", "executedQty": "1", "avgPrice": "100"}))
        result = await execute_confirmed_market_order(adapter, symbol="BTC", side="BUY", quantity=D(1),
            clip_usd=100, baseline=D(0), limit_price="100.2")
        self.assertEqual(extract_actual_average_price(result), D(100))

    async def test_post_fill_hedge_does_not_call_quote_refresher_and_caps_pair_gap(self):
        class Maker:
            async def place_limit_order(self, *, post_only=False, **kw):
                return {"ok": True, "terminal": True, "filled_quantity": "1", "status": "FILLED", "avg_price": "100"}
            get_open_position = AsyncMock(side_effect=[None, {"side": "SHORT", "quantity": "1"}])
        calls = []
        class Taker:
            async def place_market_order(self, *, limit_price=None, **kw):
                calls.append(limit_price)
                return {"terminal": True, "filled_quantity": "1", "avg_price": limit_price}
            get_open_position = AsyncMock(side_effect=[None, {"side": "LONG", "quantity": "1"}])
        refresh = AsyncMock(side_effect=AssertionError("network on critical hedge path"))
        result = await execute_single_clip_with_sides(symbol="BTC", quantity=D(1), clip_usd=100,
            maker_venue="mexc_spot", taker_venue="aster", maker_side="SELL", taker_side="BUY",
            maker_adapter=Maker(), taker_adapter=Taker(), max_hedge_retries=0,
            state_machine=ExecutionStateMachine(), maker_price="100", maker_orderbook=book(),
            taker_orderbook=book(), require_maker_fill_confirmation=True, verify_hedge_fill=True,
            require_bounded_hedge=True, hedge_limit_price="100.34028", max_quote_age_ms=1000,
            hedge_quote_refresher=refresh, maker_market_type="spot")
        refresh.assert_not_awaited()
        self.assertEqual([D(x) for x in calls], [D("100.2")])
        self.assertEqual(D(result["execution_price_summary"]["realized_adverse_gap_bps"]), D(20))

    async def test_cancel_late_fills_use_final_average_or_conservative_limit(self):
        for final_average in ("100.5", None):
            with self.subTest(final_average=final_average):
                terminal = {"ok": True, "terminal": True, "filled_quantity": "1"}
                if final_average is not None:
                    terminal["avg_price"] = final_average
                class Maker:
                    async def place_limit_order(self, *, post_only=False, **kw):
                        return {"ok": True, "order_id": 1}
                    wait_for_order_fill = AsyncMock(return_value={
                        "ok": True, "filled_quantity": "0.4", "avg_price": "102"})
                    cancel_order = AsyncMock(return_value=terminal)
                    get_open_position = AsyncMock(side_effect=[None, {"side": "SHORT", "quantity": "1"}])
                class Taker:
                    async def place_market_order(self, *, limit_price=None, **kw):
                        return {"terminal": True, "filled_quantity": "1", "avg_price": limit_price}
                    get_open_position = AsyncMock(side_effect=[None, {"side": "LONG", "quantity": "1"}])
                result = await execute_single_clip_with_sides(symbol="BTC", quantity=D(1), clip_usd=100,
                    maker_venue="mexc_spot", taker_venue="aster", maker_side="SELL", taker_side="BUY",
                    maker_adapter=Maker(), taker_adapter=Taker(), max_hedge_retries=0,
                    state_machine=ExecutionStateMachine(), maker_price="100", maker_orderbook=book(),
                    taker_orderbook=book(), require_maker_fill_confirmation=True, verify_hedge_fill=True,
                    require_bounded_hedge=True, hedge_limit_price="103", max_quote_age_ms=1000,
                    maker_market_type="spot")
                prices = result["execution_price_summary"]
                self.assertEqual(D(prices["hedge_limit_price"]), D(final_average or "100") * D("1.002"))
                self.assertEqual(prices["maker_avg_price"], final_average)
                self.assertEqual(prices["maker_avg_price_confirmed"], final_average is not None)

    async def test_resting_guard_checks_cap_even_when_gap_is_under_point_two_percent(self):
        current = book("99.9", "100")
        guard = MakerPriceGuard(fetch_books=AsyncMock(return_value=(current, current)), tick_size=None,
            maker_side="BUY", taker_side="SELL", quantity=D(1), hedge_limit_price="99.95")
        with self.assertRaises(MakerPriceRecheck): await guard.check("100")

    async def test_guard_requires_all_depth_to_fit_cap_not_just_vwap(self):
        current = book()
        current["asks"] = [("100", "0.9"), ("101", "0.1")]
        guard = MakerPriceGuard(fetch_books=AsyncMock(return_value=(current, current)), tick_size=None,
            maker_side="SELL", taker_side="BUY", quantity=D(1), hedge_limit_price="100.2")
        with self.assertRaises(MakerPriceRecheck): await guard.check("100")

    async def test_reverse_direction_uses_stream_and_stale_cache_cancels(self):
        taker = SimpleNamespace(prepare_hedge_quotes=AsyncMock(),
            hedge_quote=AsyncMock(return_value=book()), wait_for_hedge_quote=AsyncMock())
        guard = await create_maker_guard(maker_adapter=SimpleNamespace(), taker_adapter=taker,
            symbol="BTC", fetch_books=AsyncMock(return_value=(book(), book())), maker_side="SELL",
            taker_side="BUY", quantity=D(1), hedge_limit_price="100.2", max_quote_age_ms=1000)
        taker.prepare_hedge_quotes.assert_awaited_once_with("BTC")
        await guard.check("100")
        taker.hedge_quote.side_effect = RuntimeError("stream disconnected")
        with self.assertRaises(MakerPriceRecheck): await guard.check("100")

    async def test_mexc_cancel_ack_reconciles_terminal_partial(self):
        adapter = MexcSpotExecutionAdapter(api_key="unused", api_secret="unused")
        adapter._delete_order = AsyncMock(return_value={"orderId": 1})
        adapter._get_order = AsyncMock(return_value={"status": "PARTIALLY_CANCELED", "executedQty": "0.4"})
        result = await adapter.cancel_order(order_result={"order_id": 1}, symbol="BTC", side="BUY", amount="1")
        self.assertEqual(terminal_fill_quantity(result), D("0.4"))

    async def test_mexc_incomplete_or_mismatched_status_cannot_authorize_retry(self):
        adapter = MexcSpotExecutionAdapter(api_key="unused", api_secret="unused")
        for status in ({"status": "CANCELED"},
                       {"status": "CANCELED", "executedQty": "NaN"},
                       {"status": "CANCELED", "executedQty": "0", "orderId": 2}):
            with self.subTest(status=status):
                adapter._get_order = AsyncMock(return_value=status)
                with self.assertRaises(RuntimeError):
                    await adapter.get_order_execution(order_result={"order_id": 1}, symbol="BTC")

    def test_pair_bound_is_directional_and_never_widens_original_cap(self):
        self.assertEqual(cap_hedge_to_maker("100.34028", maker_price=100, maker_side="SELL"), D("100.2"))
        self.assertEqual(cap_hedge_to_maker("99.6", maker_price=100, maker_side="BUY"), D("99.8"))
        self.assertEqual(cap_hedge_to_maker("100.1", maker_price=100, maker_side="SELL"), D("100.1"))

    def test_nonfinite_and_invalid_limits_are_rejected(self):
        for bps, age, retries in [("NaN",1000,3),(20,"Infinity",3),(10000,1000,3),(20,1000,1.5)]:
            with self.subTest(bps=bps, age=age, retries=retries), self.assertRaises(ValueError):
                validate_hedge_limits(max_hedge_slippage_bps=bps, max_quote_age_ms=age, max_maker_reprices=retries)


class AsterDepthTests(unittest.IsolatedAsyncioTestCase):
    def payload(self, version=1):
        return {"e":"depthUpdate", "s":"BTCUSDT", "u":version, "T":int(time.time()*1000),
                "b":[["99","1"],["98","2"]], "a":[["100","1"],["101","2"]]}

    def test_full_snapshot_replaces_old_levels_and_ignores_old_versions(self):
        stream = AsterDepthStream(connect=None, symbol="BTCUSDT")
        stream.healthy = True
        stream.publish(decode_aster_depth(self.payload(), "BTCUSDT"))
        newer = self.payload(2); newer["b"] = [["98","1"]]
        stream.publish(decode_aster_depth(newer, "BTCUSDT"))
        stream.publish(decode_aster_depth(self.payload(), "BTCUSDT"))
        self.assertEqual(stream.snapshot()["bids"], [(D(98), D(1))])

    async def test_stream_start_and_disconnect_invalidate_cache(self):
        queue = asyncio.Queue()
        queue.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.TEXT, json=self.payload))
        ws = SimpleNamespace(receive=queue.get, close=AsyncMock())
        stream = AsterDepthStream(connect=AsyncMock(return_value=ws), symbol="BTCUSDT")
        try:
            await stream.start()
            self.assertEqual(stream.snapshot()["ask"], D(100))
            queue.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.CLOSED))
            await asyncio.sleep(0)
            with self.assertRaises(RuntimeError): stream.snapshot()
        finally:
            await stream.close()
        ws.close.assert_awaited_once()

    def test_invalid_levels_fail_closed(self):
        for bad in ("NaN", "-1"):
            payload = self.payload(); payload["a"][0][0] = bad
            with self.assertRaises(ValueError): decode_aster_depth(payload, "BTCUSDT")


class SafetyWiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_live_service_limits_fail_before_market_data(self):
        from hydra_basis.execution_engine import order_service
        params = order_service.OpenParams("perp_perp", "BTC", "aster", "hyperliquid",
            "maker_taker", 1, D(1), D(1), 0, 0)
        with patch.object(order_service, "build_open_preview", new=AsyncMock()) as preview:
            with self.assertRaisesRegex(ValueError, "requires explicit"):
                await order_service.execute_open(params, live=True, on_progress=lambda event: None)
        preview.assert_not_awaited()

    async def test_funding_auto_close_passes_configured_limits(self):
        from hydra_basis.risk_management.models import PositionLeg
        from scripts import run_risk_manager as risk
        legs = [PositionLeg(strategy_id="s", leg_id=v, venue=v, symbol="BTC", market_type="perp",
                            side=side, quantity="1", status="open")
                for v, side in (("aster", "SHORT"), ("hyperliquid", "LONG"))]
        registry = SimpleNamespace(legs_for_strategy=lambda _: legs, mark_status=lambda *a: None)
        event = SimpleNamespace(strategy_id="s", event_type="funding", leg_id="aster", venue="aster", symbol="BTC")
        def adapter(leg):
            return SimpleNamespace(get_open_position=AsyncMock(return_value={"side": leg.side, "quantity": "1"}), close=AsyncMock())
        plan = SimpleNamespace(symbol="BTC", maker_venue="aster", taker_venue="hyperliquid")
        with patch.object(risk, "_funding_close_adapter_for_leg", side_effect=adapter), patch(
                "scripts.place_order.fetch_close_orderbooks", new=AsyncMock(return_value={})), patch(
                "scripts.place_order.build_close_position_plan", return_value=plan), patch(
                "hydra_basis.execution_engine.priority.load_execution_priorities", return_value={}), patch(
                "scripts.place_order.execute_close_position_plan", new=AsyncMock(return_value={"ok": True})) as execute:
            result = await risk.execute_funding_auto_close_with_maker_taker(registry=registry, event=event,
                dry_run=False, max_hedge_slippage_bps=D(12), max_quote_age_ms=D(250), max_maker_reprices=2)
        self.assertTrue(result["ok"])
        self.assertEqual(execute.await_args.kwargs["max_hedge_slippage_bps"], D(12))
        self.assertEqual(execute.await_args.kwargs["max_quote_age_ms"], D(250))
        self.assertEqual(execute.await_args.kwargs["max_maker_reprices"], 2)

    def test_web_live_open_and_close_forward_limits(self):
        from fastapi.testclient import TestClient
        from web.app import create_app
        for mode in ("open", "close"):
            captured = []
            async def execute(params, **kwargs):
                captured.append(params)
                return {"ok": True}
            with self.subTest(mode=mode), patch(
                    f"hydra_basis.execution_engine.order_service.execute_{mode}", new=execute):
                with TestClient(create_app()) as client, client.websocket_connect("/ws/execute") as ws:
                    ws.send_json({"live": True, "params": {"kind": "perp_perp", "mode": mode,
                        "exec_mode": "maker_taker", "symbol": "BTC", "short_venue": "aster",
                        "long_venue": "hyperliquid", "leverage": 1, "total_size": "1", "clip_size": "1",
                        "interval_ms": 0, "debounce_ms": 0, "max_hedge_slippage_bps": "12",
                        "max_quote_age_ms": "250", "max_maker_reprices": 2}})
                    self.assertTrue(ws.receive_json()["ok"])
                self.assertEqual(captured[0].max_hedge_slippage_bps, D(12))
                self.assertEqual(captured[0].max_quote_age_ms, D(250))
                self.assertEqual(captured[0].max_maker_reprices, 2)


if __name__ == "__main__": unittest.main()
