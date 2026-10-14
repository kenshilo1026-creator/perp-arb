import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.executor import extract_actual_average_price, wait_for_maker_fill
from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, execute_spot_perp_plan, format_spot_perp_execution_summary


def plan():
    return build_spot_perp_plan(symbol="GRVT", mode="close", short_venue="aster",
        quantity=D("47"), clip_usd=10, spot_book={"bid": 0.2092, "ask": 0.2093},
        perp_book={"bid": 0.2101, "ask": 0.2105})


class SummaryTests(unittest.TestCase):
    def test_reports_close_buy_and_actual_04284_percent(self):
        lines = format_spot_perp_execution_summary(plan=plan(), result={
            "execution_price_summary": {"maker_avg_price": "0.2101", "taker_avg_price": "0.2092"}})
        self.assertIn("aster買入平空成交價: 0.2101", lines)
        self.assertTrue(any("0.4284%" in line for line in lines))
        self.assertTrue(any("警告" in line for line in lines))

    def test_missing_average_cannot_be_reported_as_verified_gap(self):
        lines = format_spot_perp_execution_summary(plan=plan(), result={
            "execution_price_summary": {"maker_avg_price": "0.2101", "taker_avg_price": None}})
        self.assertTrue(any("無法驗證" in line for line in lines))
        self.assertFalse(any("實際不利成交價差" in line for line in lines))

    def test_submitted_order_price_is_not_an_actual_average(self):
        self.assertIsNone(extract_actual_average_price({"raw": {"status": "NEW", "price": "0.2092"}}))
        self.assertEqual(extract_actual_average_price({"raw": {
            "executedQty": "2", "cummulativeQuoteQty": "0.4184"}}), D("0.2092"))

    def test_unconfirmed_maker_limit_price_is_not_a_fill_average(self):
        lines = format_spot_perp_execution_summary(plan=plan(), result={
            "execution_price_summary": {"maker_avg_price": "0.2101",
                "maker_avg_price_confirmed": False, "taker_avg_price": "0.2092"}})
        self.assertIn("aster買入平空成交價: N/A", lines)
        self.assertTrue(any("無法驗證" in line for line in lines))
        self.assertFalse(any("實際不利成交價差" in line for line in lines))

    def test_latency_is_labeled_as_local_processing_not_exchange_fill_gap(self):
        lines = format_spot_perp_execution_summary(plan=plan(), result={
            "execution_timing": {"maker_confirmation_source": "user_stream",
                "maker_observed_to_hedge_call_ms": 1.25, "hedge_call_duration_ms": 52.5}})
        self.assertTrue(any("1.25 ms" in line and "52.50 ms" in line
                            and "非兩所實際成交時間差" in line for line in lines))


class LatencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_setup_finishes_before_dispatch_and_poll_interval_is_explicit(self):
        events = []
        async def prepare(symbol):
            events.append("prepared")
        async def warm():
            events.append("connected")
        async def stream():
            events.append("subscribed")
        async def execute(**kwargs):
            self.assertEqual(events, ["prepared", "connected", "subscribed"])
            self.assertEqual(kwargs["maker_fill_poll_interval_seconds"], 0.2)
            return {"ok": True}
        maker = SimpleNamespace(prepare_execution=prepare, place_limit_order=AsyncMock(),
                                prepare_maker_fill_stream=stream)
        taker = SimpleNamespace(warm_up=warm)
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[maker, taker]), \
             patch("scripts.run_spot_perp_arbitrage.refresh_spot_perp_maker_price", new=AsyncMock(return_value="0.2101")), \
             patch("scripts.run_spot_perp_arbitrage.execute_single_clip", new=execute):
            await execute_spot_perp_plan(plan=plan(), leverage=1)

    async def test_stream_start_failure_keeps_rest_execution_available(self):
        maker = SimpleNamespace(place_limit_order=AsyncMock(),
            prepare_maker_fill_stream=AsyncMock(side_effect=RuntimeError("subscription unavailable")))
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[maker, SimpleNamespace()]), \
             patch("scripts.run_spot_perp_arbitrage.refresh_spot_perp_maker_price", new=AsyncMock(return_value="0.2101")), \
             patch("scripts.run_spot_perp_arbitrage.execute_single_clip", new=AsyncMock(return_value={"ok": True})) as execute:
            self.assertTrue((await execute_spot_perp_plan(plan=plan(), leverage=1))["ok"])
            execute.assert_awaited_once()

    async def test_setup_failure_does_not_dispatch_maker(self):
        maker = SimpleNamespace(prepare_execution=AsyncMock(side_effect=RuntimeError("setup failed")),
                                place_limit_order=AsyncMock())
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[maker, SimpleNamespace()]), \
             patch("scripts.run_spot_perp_arbitrage.execute_single_clip", new=AsyncMock()) as execute:
            with self.assertRaisesRegex(RuntimeError, "setup failed"):
                await execute_spot_perp_plan(plan=plan(), leverage=1)
            execute.assert_not_awaited()
            maker.place_limit_order.assert_not_awaited()

    async def test_pooled_sessions_reuse_connection_and_close(self):
        for cls, module in ((AsterExecutionAdapter, "aster_adapter"), (MexcSpotExecutionAdapter, "mexc_spot_adapter")):
            adapter = cls()
            session = SimpleNamespace(closed=False, close=AsyncMock())
            with patch(f"hydra_basis.execution_engine.{module}.aiohttp.ClientSession", return_value=session) as factory:
                async with adapter._request_session() as first:
                    pass
                async with adapter._request_session() as second:
                    pass
                self.assertIs(first, second)
                factory.assert_called_once()
                session.close.assert_not_awaited()
                await adapter.close()
                session.close.assert_awaited_once()
                self.assertIsNone(adapter._session)

    async def test_aster_preparation_completes_all_setup(self):
        adapter = AsterExecutionAdapter()
        adapter.warm_up = AsyncMock()
        adapter.ensure_isolated_margin = AsyncMock()
        adapter.ensure_leverage = AsyncMock()
        await adapter.prepare_execution("GRVT")
        adapter.warm_up.assert_awaited_once()
        adapter.ensure_isolated_margin.assert_awaited_once_with("GRVT")
        adapter.ensure_leverage.assert_awaited_once_with("GRVT")

    async def test_poll_interval_forwarded_without_breaking_old_adapter_signature(self):
        waiter = AsyncMock(return_value={"ok": True})
        await wait_for_maker_fill(SimpleNamespace(wait_for_order_fill=waiter), maker_result={"ok": True},
            symbol="GRVT", side="BUY", amount="47", timeout_seconds=10, poll_interval_seconds=0.2)
        self.assertEqual(waiter.call_args.kwargs["poll_interval_seconds"], 0.2)
        async def legacy(*, order_result, symbol, side, amount, timeout_seconds, allow_partial_fill):
            return {"ok": True}
        self.assertTrue((await wait_for_maker_fill(SimpleNamespace(wait_for_order_fill=legacy),
            maker_result={"ok": True}, symbol="GRVT", side="BUY", amount="47",
            timeout_seconds=10, poll_interval_seconds=0.2))["ok"])


if __name__ == "__main__":
    unittest.main()
