import tempfile
import unittest
import json
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.hedge_safety import HedgeFailure, execute_confirmed_market_order
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.lighter_adapter import build_lighter_limit_order_request
from hydra_basis.execution_engine.maker_price_guard import MakerPriceGuard, MakerPriceRecheck
from hydra_basis.execution_engine.quote_safety import (
    assert_hedge_limit_still_marketable,
    bounded_hedge_limit,
    executable_price,
    stamp_quote,
    validate_quote_pair_age,
)
from hydra_basis.execution_engine.order_service import Deps, run_batched_execution
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from hydra_basis.execution_engine.residual_report import append_residual_report


class QuoteSafetyTests(unittest.TestCase):
    def test_stale_quote_fails_closed(self):
        maker = stamp_quote({"bid": 99, "ask": 100}, received_ns=1_000_000_000)
        taker = stamp_quote({"bid": 99, "ask": 100}, received_ns=1_050_000_000)
        with self.assertRaisesRegex(RuntimeError, "stale executable quote"):
            validate_quote_pair_age(maker, taker, max_age_ms=100, now_ns=1_200_000_000)

    def test_fresh_receipt_cannot_hide_stale_venue_timestamp(self):
        maker = stamp_quote({"bid": 99, "ask": 100, "ts_ms": 1_999_999_999_950}, received_ns=1_000)
        taker = stamp_quote({"bid": 99, "ask": 100, "ts_ms": 1_999_999_998_000}, received_ns=1_000)
        with self.assertRaisesRegex(RuntimeError, "stale executable quote"):
            validate_quote_pair_age(
                maker, taker, max_age_ms=100, now_ns=1_000,
                now_epoch_ms=2_000_000_000_000,
            )

    def test_disappearing_depth_cannot_use_top_of_book_fallback(self):
        book = {"bid": 100, "ask": 101, "asks": [(101, 1)]}
        with self.assertRaisesRegex(RuntimeError, "insufficient executable hedge depth"):
            executable_price(book, side="BUY", quantity=D("2"))
        with self.assertRaisesRegex(RuntimeError, "full-quantity hedge depth"):
            executable_price({"bid": 100, "ask": 101}, side="BUY", quantity=D("1"))

    def test_directional_bps_bound_is_not_symmetric_absolute_gap(self):
        self.assertEqual(bounded_hedge_limit(D("100"), side="BUY", max_slippage_bps=25), D("100.25"))
        self.assertEqual(bounded_hedge_limit(D("100"), side="SELL", max_slippage_bps=25), D("99.75"))

    def test_reprice_refuses_to_chase_after_fixed_cap_is_no_longer_marketable(self):
        with self.assertRaisesRegex(RuntimeError, "no longer marketable"):
            assert_hedge_limit_still_marketable(D("101"), side="BUY", limit_price="100.25")
        with self.assertRaisesRegex(RuntimeError, "no longer marketable"):
            assert_hedge_limit_still_marketable(D("99"), side="SELL", limit_price="99.75")


class ConfirmedHedgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_fill_retries_only_terminal_remainder_with_same_cap(self):
        calls = []

        class Adapter:
            async def place_market_order(self, **kwargs):
                calls.append(kwargs)
                fill = D("0.4") if len(calls) == 1 else D("0.6")
                return {"terminal": True, "filled_quantity": str(fill), "ok": True}

            async def get_open_position(self, *, symbol, market_type):
                return None

        result = await execute_confirmed_market_order(
            Adapter(), symbol="BTC", side="BUY", quantity=D("1"), clip_usd=100,
            baseline=D("0"), max_attempts=2, confirmation_attempts=1,
            limit_price="100.25", market_type="spot",
        )
        self.assertEqual(D(result["filled_quantity"]), D("1"))
        self.assertEqual([call["amount"] for call in calls], ["1", "0.6"])
        self.assertEqual({call["limit_price"] for call in calls}, {"100.25"})

    async def test_ack_only_unknown_never_submits_duplicate(self):
        adapter = SimpleNamespace(
            place_market_order=AsyncMock(return_value={"ok": True, "order_id": "ack-only"}),
            get_open_position=AsyncMock(return_value=None),
        )
        with self.assertRaises(HedgeFailure) as caught:
            await execute_confirmed_market_order(
                adapter, symbol="BTC", side="SELL", quantity=D("1"), clip_usd=100,
                baseline=D("0"), max_attempts=3, confirmation_attempts=2,
                poll_delay=0, limit_price="99.75",
            )
        self.assertTrue(caught.exception.uncertain)
        adapter.place_market_order.assert_awaited_once()

    async def test_duplicate_terminal_event_does_not_create_another_order(self):
        adapter = SimpleNamespace(
            place_market_order=AsyncMock(return_value={
                "ok": True, "terminal": True, "filled_quantity": "1",
                "raw": {"terminal": True, "filled_quantity": "1"},
            }),
            get_open_position=AsyncMock(return_value=None),
        )
        result = await execute_confirmed_market_order(
            adapter, symbol="BTC", side="BUY", quantity=D("1"), clip_usd=100,
            baseline=D("0"), limit_price="100.25",
        )
        self.assertEqual(result["filled_quantity"], "1")
        adapter.place_market_order.assert_awaited_once()


class MexcBoundedIocTests(unittest.IsolatedAsyncioTestCase):
    async def test_tick_and_quantity_rounding_never_loosens_cap(self):
        calls = []

        class Adapter(MexcSpotExecutionAdapter):
            async def _load_symbol_rules(self, symbol):
                return {"tick": D("0.1"), "step": D("0.01"), "min_qty": D("0.01")}

            async def _post_order(self, params):
                calls.append(params)
                return {"orderId": 1, "status": "FILLED", "executedQty": params["quantity"],
                        "cummulativeQuoteQty": "100"}

        adapter = Adapter(api_key="unused", api_secret="unused")
        await adapter.place_market_order(symbol="BTC", side="BUY", amount="1.009",
                                         clip_usd=100, limit_price="100.29")
        self.assertEqual(calls[0]["type"], "IMMEDIATE_OR_CANCEL")
        self.assertNotIn("timeInForce", calls[0])
        self.assertEqual(calls[0]["quantity"], "1")
        self.assertEqual(calls[0]["price"], "100.2")

    async def test_uncapped_market_is_disabled(self):
        adapter = MexcSpotExecutionAdapter(api_key="unused", api_secret="unused")
        with self.assertRaisesRegex(RuntimeError, "uncapped MARKET is disabled"):
            await adapter.place_market_order(symbol="BTC", side="BUY", amount="1", clip_usd=100)

    async def test_aster_ioc_tick_rounding_never_loosens_cap(self):
        adapter = AsterExecutionAdapter(
            signer_address="0x0000000000000000000000000000000000000001",
            private_key="unused", user_address="0x0000000000000000000000000000000000000001",
        )
        adapter._resolve_raw_symbol = AsyncMock(return_value="BTCUSDT")
        adapter._format_quantity = AsyncMock(return_value="1")
        adapter.get_price_tick_size = AsyncMock(return_value="0.1")
        adapter.ensure_isolated_margin = AsyncMock()
        adapter.ensure_leverage = AsyncMock()
        adapter._post_order = AsyncMock(return_value={"orderId": 1})
        await adapter.place_market_order(
            symbol="BTC", side="BUY", amount="1", clip_usd=100, limit_price="100.29",
        )
        self.assertEqual(adapter._post_order.await_args.args[0]["timeInForce"], "IOC")
        self.assertEqual(adapter._post_order.await_args.args[0]["price"], "100.2")

    def test_lighter_price_rounding_never_loosens_buy_or_sell_cap(self):
        common = dict(
            quantity=D("1"), price=D("100.21"), base_amount_multiplier=100,
            price_multiplier=10, market_index=1,
        )
        buy = build_lighter_limit_order_request(side="BUY", **common)
        sell = build_lighter_limit_order_request(side="SELL", **common)
        self.assertEqual(buy["price"], 1002)
        self.assertEqual(sell["price"], 1003)


class RestingMakerFreshnessTests(unittest.IsolatedAsyncioTestCase):
    async def test_guard_rejects_stale_source_timestamp_while_maker_rests(self):
        now_ms = 2_000_000_000_000
        stale = stamp_quote({
            "bid": 99, "ask": 100, "bids": [(99, 10)], "asks": [(100, 10)],
            "ts_ms": now_ms - 5_000,
        })
        guard = MakerPriceGuard(
            fetch_books=AsyncMock(return_value=(stale, stale)),
            fetch_taker_book=AsyncMock(return_value=stale),
            tick_size=AsyncMock(return_value="0.1"), maker_side="BUY", taker_side="SELL",
            quantity=D("1"), max_quote_age_ms=100,
        )
        with patch("hydra_basis.execution_engine.quote_safety.time.time_ns", return_value=now_ms * 1_000_000):
            with self.assertRaises(MakerPriceRecheck):
                await guard.check("99")


class BatchStopTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_or_residual_batch_stops_future_batches(self):
        calls = []

        async def run_batch(quantity):
            calls.append(quantity)
            return {"ok": False, "error": "UNRESOLVED_HEDGE_EXPOSURE"}

        deps = Deps(fetch_orderbook=AsyncMock(return_value={"bid": 99, "ask": 100}), sleep=AsyncMock())
        result = await run_batched_execution(
            symbol="BTC", total_size=D("3"), clip_size=D("1"), batch_count=3,
            clip_usd=100, venues=["maker", "taker"], interval_ms=0, debounce_ms=0,
            run_batch=run_batch, on_progress=lambda event: None, deps=deps,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(calls, [D("1")])

    def test_residual_report_is_durable_and_non_secret_by_construction(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "residuals.jsonl"
            append_residual_report(path, {
                "symbol": "BTC", "maker_venue": "aster", "taker_venue": "mexc_spot",
                "maker_filled_quantity": "1", "hedge_confirmed_quantity": "0.4",
                "hedge_remaining_quantity": "0.6", "outcome_uncertain": True,
            })
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["event"], "UNRESOLVED_HEDGE_EXPOSURE")
            self.assertEqual(payload["hedge_remaining_quantity"], "0.6")
            self.assertNotIn("account", payload)


class ExecutorIntegrationTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _book():
        return stamp_quote({
            "bid": 99, "ask": 100,
            "bids": [(99, 10)], "asks": [(100, 10)],
        })

    async def test_cap_and_post_only_are_propagated_and_fill_is_reconciled(self):
        maker_positions = [None, {"side": "LONG", "quantity": "1"}]
        taker_positions = [None, {"side": "SHORT", "quantity": "1"}]

        class Maker:
            async def place_limit_order(self, *, symbol, side, amount, clip_usd, price,
                                        post_only=False):
                self.place_kwargs = locals()
                return {"ok": True, "terminal": True, "status": "FILLED",
                        "filled_quantity": "1", "avg_price": "99"}

            async def get_open_position(self, *, symbol, market_type):
                return maker_positions.pop(0) if len(maker_positions) > 1 else maker_positions[0]

        class Taker:
            async def place_market_order(self, *, symbol, side, amount, clip_usd,
                                         limit_price=None):
                self.market_kwargs = locals()
                return {"ok": True, "terminal": True, "status": "FILLED",
                        "filled_quantity": "1", "avg_price": "100"}

            async def get_open_position(self, *, symbol, market_type):
                return taker_positions.pop(0) if len(taker_positions) > 1 else taker_positions[0]

        maker, taker = Maker(), Taker()
        result = await execute_single_clip_with_sides(
            symbol="BTC", clip_usd=100, quantity=D("1"), maker_venue="aster",
            taker_venue="hyperliquid", maker_side="BUY", taker_side="SELL",
            maker_adapter=maker, taker_adapter=taker, max_hedge_retries=0,
            state_machine=ExecutionStateMachine(), maker_price="99",
            maker_orderbook=self._book(), taker_orderbook=self._book(),
            require_maker_fill_confirmation=True, verify_hedge_fill=True,
            require_bounded_hedge=True, hedge_limit_price="98.75", max_quote_age_ms=1000,
        )
        self.assertTrue(result["hedge_verified"])
        self.assertTrue(maker.place_kwargs["post_only"])
        self.assertEqual(D(taker.market_kwargs["limit_price"]), D("98.802"))
        self.assertEqual(D(result["execution_price_summary"]["hedge_limit_price"]), D("98.802"))

    async def test_unsupported_bounded_adapter_fails_before_maker(self):
        maker = SimpleNamespace(place_limit_order=AsyncMock())

        async def uncapped_market(*, symbol, side, amount, clip_usd):
            return {"ok": True}

        taker = SimpleNamespace(place_market_order=uncapped_market)
        with self.assertRaisesRegex(RuntimeError, "bounded hedge unsupported"):
            await execute_single_clip_with_sides(
                symbol="BTC", clip_usd=100, quantity=D("1"), maker_venue="maker",
                taker_venue="legacy", maker_side="BUY", taker_side="SELL",
                maker_adapter=maker, taker_adapter=taker, max_hedge_retries=0,
                state_machine=ExecutionStateMachine(), maker_price="99",
                maker_orderbook=self._book(), taker_orderbook=self._book(),
                verify_hedge_fill=True, require_bounded_hedge=True,
                hedge_limit_price="98.75", max_quote_age_ms=1000,
            )
        maker.place_limit_order.assert_not_awaited()

    async def test_fill_during_cancel_is_hedged_exactly_once(self):
        maker_positions = [None, {"side": "LONG", "quantity": "1"}]
        taker_positions = [None, {"side": "SHORT", "quantity": "1"}]

        class Maker:
            async def place_limit_order(self, *, symbol, side, amount, clip_usd, price,
                                        post_only=False):
                return {"ok": True, "order_id": "m1"}
            wait_for_order_fill = AsyncMock(side_effect=RuntimeError("maker fill timeout"))
            cancel_order = AsyncMock(return_value={
                "ok": True, "terminal": True, "status": "FILLED", "filled_quantity": "1",
            })

            async def get_open_position(self, *, symbol, market_type):
                return maker_positions.pop(0) if len(maker_positions) > 1 else maker_positions[0]

        class Taker:
            def __init__(self):
                self.calls = 0

            async def place_market_order(self, *, symbol, side, amount, clip_usd,
                                         limit_price=None):
                self.calls += 1
                return {"ok": True, "terminal": True, "status": "FILLED",
                        "filled_quantity": "1"}

            async def get_open_position(self, *, symbol, market_type):
                return taker_positions.pop(0) if len(taker_positions) > 1 else taker_positions[0]

        maker, taker = Maker(), Taker()
        result = await execute_single_clip_with_sides(
            symbol="BTC", clip_usd=100, quantity=D("1"), maker_venue="aster",
            taker_venue="hyperliquid", maker_side="BUY", taker_side="SELL",
            maker_adapter=maker, taker_adapter=taker, max_hedge_retries=0,
            state_machine=ExecutionStateMachine(), maker_price="99",
            maker_orderbook=self._book(), taker_orderbook=self._book(),
            require_maker_fill_confirmation=True, max_maker_reprice_attempts=0,
            verify_hedge_fill=True, require_bounded_hedge=True,
            hedge_limit_price="98.75", max_quote_age_ms=1000,
        )
        self.assertTrue(result["ok"])
        maker.cancel_order.assert_awaited_once()
        self.assertEqual(taker.calls, 1)

    async def test_lagging_position_endpoint_preserves_proven_hedge_fill_in_report(self):
        class Maker:
            async def place_limit_order(self, *, symbol, side, amount, clip_usd, price,
                                        post_only=False):
                return {"ok": True, "terminal": True, "status": "FILLED",
                        "filled_quantity": "1"}

            async def get_open_position(self, *, symbol, market_type):
                return None

        class Taker:
            async def place_market_order(self, *, symbol, side, amount, clip_usd,
                                         limit_price=None):
                return {"ok": True, "terminal": True, "status": "FILLED",
                        "filled_quantity": "1"}

            async def get_open_position(self, *, symbol, market_type):
                return None

        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "residuals.jsonl"
            with patch("hydra_basis.execution_engine.hedge_safety.asyncio.sleep", new=AsyncMock()):
                with self.assertRaisesRegex(RuntimeError, "fill confirmed"):
                    await execute_single_clip_with_sides(
                        symbol="BTC", clip_usd=100, quantity=D("1"), maker_venue="aster",
                        taker_venue="hyperliquid", maker_side="BUY", taker_side="SELL",
                        maker_adapter=Maker(), taker_adapter=Taker(), max_hedge_retries=0,
                        state_machine=ExecutionStateMachine(), maker_price="99",
                        maker_orderbook=self._book(), taker_orderbook=self._book(),
                        require_maker_fill_confirmation=True, verify_hedge_fill=True,
                        require_bounded_hedge=True, hedge_limit_price="98.75",
                        max_quote_age_ms=1000, residual_report_path=report,
                    )
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["hedge_confirmed_quantity"], "1")
            self.assertEqual(payload["hedge_remaining_quantity"], "0")
            self.assertTrue(payload["outcome_uncertain"])

    async def test_post_fill_bounded_ioc_attempts_then_reports_unfilled_residual(self):
        class Maker:
            async def place_limit_order(self, *, symbol, side, amount, clip_usd, price,
                                        post_only=False):
                return {"ok": True, "terminal": True, "status": "FILLED",
                        "filled_quantity": "1"}

            async def get_open_position(self, *, symbol, market_type):
                return None

        class Taker:
            def __init__(self):
                self.submissions = AsyncMock(return_value={"terminal": True, "filled_quantity": "0", "status": "EXPIRED"})
                self.get_open_position = AsyncMock(return_value=None)

            async def place_market_order(self, *, symbol, side, amount, clip_usd, limit_price=None):
                return await self.submissions(
                    symbol=symbol, side=side, amount=amount, clip_usd=clip_usd,
                    limit_price=limit_price,
                )

        taker = Taker()
        stale = stamp_quote({
            "bid": 99, "ask": 100, "bids": [(99, 10)], "asks": [(100, 10)],
        }, received_ns=1)
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "residuals.jsonl"
            with self.assertRaisesRegex(RuntimeError, "hedge incomplete"):
                await execute_single_clip_with_sides(
                    symbol="BTC", clip_usd=100, quantity=D("1"), maker_venue="aster",
                    taker_venue="hyperliquid", maker_side="BUY", taker_side="SELL",
                    maker_adapter=Maker(), taker_adapter=taker, max_hedge_retries=0,
                    state_machine=ExecutionStateMachine(), maker_price="99",
                    maker_orderbook=self._book(), taker_orderbook=self._book(),
                    require_maker_fill_confirmation=True, verify_hedge_fill=True,
                    require_bounded_hedge=True, hedge_limit_price="98.75",
                    max_quote_age_ms=1000, hedge_quote_refresher=AsyncMock(return_value=stale),
                    residual_report_path=report,
                )
            self.assertEqual(taker.submissions.await_count, 3)
            self.assertTrue(all(D(c.kwargs["limit_price"]) == D("98.802") for c in taker.submissions.await_args_list))
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["hedge_confirmed_quantity"], "0")
            self.assertEqual(payload["hedge_remaining_quantity"], "1")
            self.assertFalse(payload["outcome_uncertain"])


if __name__ == "__main__":
    unittest.main()
