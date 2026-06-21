import asyncio
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from scripts.run_spot_perp_arbitrage import (
    build_spot_perp_plan,
    compute_token_batch_count,
    compute_base_quantity,
    execute_spot_perp_plan,
    format_spot_perp_execution_summary,
    maker_limit_price,
    normalize_mode,
    run_spot_perp_arbitrage,
    spot_perp_sides,
)
from hydra_basis.execution_engine.executor import execute_single_clip, extract_average_price
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter
from hydra_basis.risk_management.registry import PositionRegistry
from scripts.place_order import record_open_execution_from_live_positions


class SpotPerpArbitrageScriptTests(unittest.TestCase):
    def test_open_mode_buys_spot_and_sells_perp(self) -> None:
        self.assertEqual(
            spot_perp_sides(mode="open"),
            {"mexc_spot": "BUY", "perp": "SELL"},
        )

    def test_close_mode_sells_spot_and_buys_perp(self) -> None:
        self.assertEqual(
            spot_perp_sides(mode="close"),
            {"mexc_spot": "SELL", "perp": "BUY"},
        )

    def test_spread_lower_side_becomes_taker(self) -> None:
        plan = build_spot_perp_plan(
            symbol="ETH",
            mode="open",
            short_venue="aster",
            quantity=Decimal("0.1"),
            clip_usd=300.0,
            spot_book={"bid": 2999.0, "ask": 3001.0, "ts_ms": 1},
            perp_book={"bid": 2990.0, "ask": 3010.0, "ts_ms": 1},
        )

        self.assertEqual(plan.taker_venue, "mexc_spot")
        self.assertEqual(plan.maker_venue, "aster")
        self.assertEqual(plan.maker_side, "SELL")
        self.assertEqual(plan.taker_side, "BUY")
        self.assertEqual(plan.maker_price, "3010")

    def test_close_mode_reverses_sides_but_keeps_spread_taker_rule(self) -> None:
        plan = build_spot_perp_plan(
            symbol="ETH",
            mode="close",
            short_venue="hyperliquid",
            quantity=Decimal("0.1"),
            clip_usd=300.0,
            spot_book={"bid": 2990.0, "ask": 3010.0, "ts_ms": 1},
            perp_book={"bid": 2999.0, "ask": 3001.0, "ts_ms": 1},
        )

        self.assertEqual(plan.taker_venue, "hyperliquid")
        self.assertEqual(plan.maker_venue, "mexc_spot")
        self.assertEqual(plan.maker_side, "SELL")
        self.assertEqual(plan.taker_side, "BUY")
        self.assertEqual(plan.maker_price, "3010")

    def test_limit_price_uses_passive_side_of_book(self) -> None:
        book = {"bid": 100.0, "ask": 100.5, "ts_ms": 1}

        self.assertEqual(maker_limit_price(book, "BUY"), "100")
        self.assertEqual(maker_limit_price(book, "SELL"), "100.5")

    def test_compute_base_quantity_from_clip_usd_uses_mid(self) -> None:
        quantity = compute_base_quantity(
            quantity=None,
            clip_usd=1000.0,
            taker_book={"bid": 99.0, "ask": 101.0, "ts_ms": 1},
        )

        self.assertEqual(quantity, Decimal("10"))

    def test_compute_token_batch_count_rounds_up(self) -> None:
        self.assertEqual(compute_token_batch_count(Decimal("21"), Decimal("10")), 3)

    def test_normalize_mode_rejects_invalid_value(self) -> None:
        with self.assertRaises(RuntimeError):
            normalize_mode("hold")

    def test_formats_open_summary_when_spot_is_taker(self) -> None:
        plan = build_spot_perp_plan(
            symbol="ETH",
            mode="open",
            short_venue="aster",
            quantity=Decimal("0.1"),
            clip_usd=300.0,
            spot_book={"bid": 2999.0, "ask": 3001.0, "ts_ms": 1},
            perp_book={"bid": 2990.0, "ask": 3010.0, "ts_ms": 1},
        )

        lines = format_spot_perp_execution_summary(
            plan=plan,
            result={
                "execution_price_summary": {
                    "maker_avg_price": "3010",
                    "taker_avg_price": "3001",
                },
            },
        )

        self.assertEqual(
            lines,
            [
                "下單成功 mexc現貨成交價: 3001",
                "aster做空成交價: 3010",
            ],
        )

    def test_formats_open_summary_when_spot_is_maker(self) -> None:
        plan = build_spot_perp_plan(
            symbol="ETH",
            mode="open",
            short_venue="aster",
            quantity=Decimal("0.1"),
            clip_usd=300.0,
            spot_book={"bid": 2990.0, "ask": 3010.0, "ts_ms": 1},
            perp_book={"bid": 2999.0, "ask": 3001.0, "ts_ms": 1},
        )

        lines = format_spot_perp_execution_summary(
            plan=plan,
            result={
                "execution_price_summary": {
                    "maker_avg_price": "2990",
                    "taker_avg_price": "2999",
                },
            },
        )

        self.assertEqual(
            lines,
            [
                "下單成功 mexc現貨成交價: 2990",
                "aster做空成交價: 2999",
            ],
        )

    def test_extract_average_price_prefers_mexc_filled_average_over_order_price(self) -> None:
        price = extract_average_price(
            {
                "price": "8.6",
                "executedQty": "10",
                "cummulativeQuoteQty": "84.2",
            }
        )

        self.assertEqual(price, Decimal("8.42"))


class MexcSpotAdapterForSpotPerpTests(unittest.IsolatedAsyncioTestCase):
    async def test_place_market_order_enriches_raw_with_filled_order_status(self) -> None:
        get_calls: list[dict] = []

        class Adapter(MexcSpotExecutionAdapter):
            async def _post_order(self, params: dict) -> dict:
                return {"orderId": "spot-1", "price": "8.6"}

            async def _get_order(self, params: dict) -> dict:
                get_calls.append(dict(params))
                return {
                    "orderId": params["orderId"],
                    "status": "FILLED",
                    "executedQty": "10",
                    "cummulativeQuoteQty": "84.2",
                }

        result = await Adapter(api_key="k", api_secret="s").place_market_order(
            symbol="eth",
            side="BUY",
            amount="10",
            clip_usd=0.0,
        )

        self.assertEqual(get_calls, [{"symbol": "ETHUSDT", "orderId": "spot-1"}])
        self.assertEqual(result["raw"]["status"], "FILLED")
        self.assertEqual(result["raw"]["executedQty"], "10")

    async def test_cancel_order_treats_order_filled_error_as_filled_status(self) -> None:
        get_calls: list[dict] = []

        class Adapter(MexcSpotExecutionAdapter):
            async def _delete_order(self, params: dict) -> dict:
                return {
                    "orderId": params["orderId"],
                    "status": "FILLED",
                    "cancelRejectedAsFilled": True,
                    "rawCancelError": {"code": -2011, "msg": "Order filled."},
                }

            async def _get_order(self, params: dict) -> dict:
                get_calls.append(dict(params))
                return {
                    "orderId": params["orderId"],
                    "status": "FILLED",
                    "executedQty": "10",
                    "origQty": "10",
                }

        result = await Adapter(api_key="k", api_secret="s").cancel_order(
            order_result={"order_id": "spot-1"},
            symbol="eth",
            side="BUY",
            amount="10",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(get_calls, [{"symbol": "ETHUSDT", "orderId": "spot-1"}])
        self.assertEqual(result["raw"]["status"], "FILLED")
        self.assertEqual(result["raw"]["executedQty"], "10")


class MissingMakerCancelAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_hyperliquid_cancel_order_posts_cancel_action_for_order_id(self) -> None:
        actions: list[dict] = []

        class Adapter(HyperliquidExecutionAdapter):
            def __init__(self) -> None:
                pass

            async def _get_asset_index(self, symbol: str) -> int:
                self.assert_symbol = symbol
                return 7

            async def _post_order(self, action: dict) -> dict:
                actions.append(action)
                return {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}

        result = await Adapter().cancel_order(
            order_result={"order_id": 12345},
            symbol="BTC",
            side="SELL",
            amount="0.01",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(
            actions,
            [{"type": "cancel", "cancels": [{"a": 7, "o": 12345}]}],
        )

    async def test_mexc_perp_cancel_order_posts_order_id_list(self) -> None:
        payloads: list[list[int]] = []

        class Adapter(MexcExecutionAdapter):
            def __init__(self) -> None:
                pass

            async def _post_cancel(self, order_ids: list[int]) -> dict:
                payloads.append(order_ids)
                return {
                    "success": True,
                    "code": 0,
                    "data": [{"orderId": order_ids[0], "errorCode": 0, "errorMsg": "success"}],
                }

        result = await Adapter().cancel_order(
            order_result={"order_id": 98765},
            symbol="BTC",
            side="SELL",
            amount="1",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(payloads, [[98765]])


class SpotPerpArbitrageRecordingTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_mode_does_not_prompt_for_leverage(self) -> None:
        printed: list[str] = []
        prompts: list[str] = []
        executed: list[int] = []
        args = SimpleNamespace(
            mode="close",
            symbol="BEAT",
            short_venue="aster",
            total_size="10",
            clip_size="10",
            leverage=None,
            variational_broker_host="127.0.0.1",
            variational_broker_port=8768,
            variational_extension_timeout=30.0,
            live=True,
        )

        def fake_input(prompt: str = "") -> str:
            prompts.append(prompt)
            return "PLACE LIVE SPOT PERP ORDER"

        async def fake_execute(*, plan, leverage, **kwargs):
            executed.append(leverage)
            return {
                "ok": True,
                "executed_quantity": str(plan.quantity),
                "execution_price_summary": {
                    "maker_avg_price": "10.1",
                    "taker_avg_price": "10.01",
                },
            }

        with patch("scripts.run_spot_perp_arbitrage.parse_args", return_value=args), \
            patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 9.9, "ask": 10.1, "ts_ms": 1},
                )),
            ), \
            patch("scripts.run_spot_perp_arbitrage.execute_spot_perp_plan", new=AsyncMock(side_effect=fake_execute)), \
            patch("builtins.input", side_effect=fake_input), \
            patch("builtins.print", side_effect=lambda *parts, **kwargs: printed.append(" ".join(str(p) for p in parts))):
            await run_spot_perp_arbitrage()

        self.assertEqual(executed, [1])
        self.assertEqual(prompts, ["> "])
        output = "\n".join(printed)
        self.assertNotIn("leverage_x:", output)

    async def test_live_run_large_gap_stops_when_user_confirms_stop(self) -> None:
        printed: list[str] = []
        prompts: list[str] = []
        args = SimpleNamespace(
            mode="open",
            symbol="BEAT",
            short_venue="aster",
            total_size="10",
            clip_size="10",
            leverage=1,
            variational_broker_host="127.0.0.1",
            variational_broker_port=8768,
            variational_extension_timeout=30.0,
            live=True,
        )

        with patch("scripts.run_spot_perp_arbitrage.parse_args", return_value=args), \
            patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 11.9, "ask": 12.1, "ts_ms": 1},
                )),
            ), \
            patch(
                "scripts.run_spot_perp_arbitrage.execute_spot_perp_plan",
                new=AsyncMock(side_effect=AssertionError("should not execute")),
            ), \
            patch("builtins.input", side_effect=lambda prompt="": prompts.append(prompt) or "Y"), \
            patch("builtins.print", side_effect=lambda *parts, **kwargs: printed.append(" ".join(str(p) for p in parts))):
            await run_spot_perp_arbitrage()

        output = "\n".join(printed)
        self.assertIn("pre_trade_price_gap", output)
        self.assertIn("maker=12.1", output)
        self.assertIn("taker=10.01", output)
        self.assertIn("價差過大", output)
        self.assertIn("是否停止下單", output)
        self.assertEqual(prompts, ["> "])

    async def test_live_run_large_gap_continues_when_user_declines_stop(self) -> None:
        printed: list[str] = []
        prompts: list[str] = []
        execute = AsyncMock(return_value={
            "ok": True,
            "execution_price_summary": {
                "maker_avg_price": "12.1",
                "taker_avg_price": "10.01",
            },
        })
        args = SimpleNamespace(
            mode="open",
            symbol="BEAT",
            short_venue="aster",
            total_size="10",
            clip_size="10",
            leverage=1,
            variational_broker_host="127.0.0.1",
            variational_broker_port=8768,
            variational_extension_timeout=30.0,
            live=True,
        )

        def fake_input(prompt: str = "") -> str:
            prompts.append(prompt)
            return "N" if len(prompts) == 1 else "PLACE LIVE SPOT PERP ORDER"

        with patch("scripts.run_spot_perp_arbitrage.parse_args", return_value=args), \
            patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 11.9, "ask": 12.1, "ts_ms": 1},
                )),
            ), \
            patch("scripts.run_spot_perp_arbitrage.execute_spot_perp_plan", new=execute), \
            patch("builtins.input", side_effect=fake_input), \
            patch("builtins.print", side_effect=lambda *parts, **kwargs: printed.append(" ".join(str(p) for p in parts))):
            await run_spot_perp_arbitrage()

        self.assertEqual(prompts, ["> ", "> "])
        execute.assert_awaited_once()
        output = "\n".join(printed)
        self.assertIn("Type exactly 'PLACE LIVE SPOT PERP ORDER' to continue:", output)
        self.assertIn("下單成功 mexc現貨成交價", output)

    async def test_live_run_splits_total_size_into_multiple_clips(self) -> None:
        execute_plans: list[tuple[Decimal, float]] = []
        args = SimpleNamespace(
            mode="open",
            symbol="BEAT",
            short_venue="aster",
            total_size="25",
            clip_size="10",
            leverage=1,
            variational_broker_host="127.0.0.1",
            variational_broker_port=8768,
            variational_extension_timeout=30.0,
            live=True,
        )

        async def fake_execute(*, plan, **kwargs):
            execute_plans.append((plan.quantity, plan.clip_usd))
            return {
                "ok": True,
                "executed_quantity": str(plan.quantity),
                "execution_price_summary": {
                    "maker_avg_price": "10.1",
                    "taker_avg_price": "10.01",
                },
            }

        with patch("scripts.run_spot_perp_arbitrage.parse_args", return_value=args), \
            patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 9.9, "ask": 10.1, "ts_ms": 1},
                )),
            ), \
            patch("scripts.run_spot_perp_arbitrage.execute_spot_perp_plan", new=AsyncMock(side_effect=fake_execute)), \
            patch("builtins.input", return_value="PLACE LIVE SPOT PERP ORDER"), \
            patch("builtins.print"):
            await run_spot_perp_arbitrage()

        self.assertEqual([qty for qty, _ in execute_plans], [Decimal("10"), Decimal("10"), Decimal("5")])

    async def test_live_run_rejects_final_batch_below_min_notional_before_ordering(self) -> None:
        args = SimpleNamespace(
            mode="open",
            symbol="BEAT",
            short_venue="aster",
            total_size="10.2",
            clip_size="10",
            leverage=1,
            variational_broker_host="127.0.0.1",
            variational_broker_port=8768,
            variational_extension_timeout=30.0,
            live=True,
        )

        with patch("scripts.run_spot_perp_arbitrage.parse_args", return_value=args), \
            patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 9.9, "ask": 10.1, "ts_ms": 1},
                )),
            ), \
            patch(
                "scripts.run_spot_perp_arbitrage.execute_spot_perp_plan",
                new=AsyncMock(side_effect=AssertionError("should not execute")),
            ), \
            patch("builtins.input", side_effect=AssertionError("should not ask for live confirmation")), \
            patch("builtins.print"):
            with self.assertRaisesRegex(RuntimeError, "spot-perp clip notional below exchange minimum"):
                await run_spot_perp_arbitrage()

    async def test_waits_for_maker_fill_before_taker_market_order(self) -> None:
        events: list[str] = []

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def place_limit_order(self, **kwargs):
                events.append(f"{self.venue}:place_limit")
                return {"ok": True, "order_id": f"{self.venue}-maker"}

            async def wait_for_order_fill(self, **kwargs):
                events.append(f"{self.venue}:wait_fill")
                return {"ok": True, "filled": True, "status": "FILLED"}

            async def place_market_order(self, **kwargs):
                events.append(f"{self.venue}:place_market")
                return {"ok": True, "order_id": f"{self.venue}-taker"}

            async def get_open_position(self, *, symbol: str, market_type: str):
                if self.venue == "mexc_spot":
                    return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "10"}
                return {"symbol": symbol, "market_type": market_type, "side": "SHORT", "quantity": "10"}

        plan = build_spot_perp_plan(
            symbol="BEAT",
            mode="open",
            short_venue="aster",
            quantity=Decimal("10"),
            clip_usd=100.0,
            spot_book={"bid": 9.99, "ask": 10.01, "ts_ms": 1},
            perp_book={"bid": 9.9, "ask": 10.1, "ts_ms": 1},
        )

        with TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            with patch(
                "scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                side_effect=lambda venue, **kwargs: Adapter(venue),
            ), patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 9.9, "ask": 10.1, "ts_ms": 1},
                )),
            ):
                result = await execute_spot_perp_plan(
                    plan=plan,
                    leverage=1,
                    registry_path=registry_path,
                )

        self.assertTrue(result["ok"])
        self.assertEqual(
            events[:3],
            ["aster:place_limit", "aster:wait_fill", "mexc_spot:place_market"],
        )

    async def test_rejects_large_pre_trade_price_gap_before_maker_order(self) -> None:
        events: list[str] = []

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def place_limit_order(self, **kwargs):
                events.append(f"{self.venue}:place_limit")
                return {"ok": True, "order_id": f"{self.venue}-maker"}

            async def place_market_order(self, **kwargs):
                events.append(f"{self.venue}:place_market")
                return {"ok": True}

        plan = build_spot_perp_plan(
            symbol="BEAT",
            mode="open",
            short_venue="aster",
            quantity=Decimal("10"),
            clip_usd=100.0,
            spot_book={"bid": 9.99, "ask": 10.01, "ts_ms": 1},
            perp_book={"bid": 11.9, "ask": 12.1, "ts_ms": 1},
        )

        with TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            with patch(
                "scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                side_effect=lambda venue, **kwargs: Adapter(venue),
            ), patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 11.9, "ask": 12.1, "ts_ms": 1},
                )),
            ):
                with self.assertRaisesRegex(RuntimeError, "pre-trade maker/taker price gap"):
                    await execute_spot_perp_plan(
                        plan=plan,
                        leverage=1,
                        registry_path=registry_path,
                    )

        self.assertEqual(events, [])

    async def test_allows_large_pre_trade_price_gap_when_user_approved(self) -> None:
        events: list[str] = []

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def place_limit_order(self, **kwargs):
                events.append(f"{self.venue}:place_limit")
                return {"ok": True, "order_id": f"{self.venue}-maker"}

            async def wait_for_order_fill(self, **kwargs):
                events.append(f"{self.venue}:wait_fill")
                return {"ok": True, "filled": True, "status": "FILLED"}

            async def place_market_order(self, **kwargs):
                events.append(f"{self.venue}:place_market")
                return {"ok": True}

            async def get_open_position(self, *, symbol: str, market_type: str):
                if self.venue == "mexc_spot":
                    return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "10"}
                return {"symbol": symbol, "market_type": market_type, "side": "SHORT", "quantity": "10"}

        plan = build_spot_perp_plan(
            symbol="BEAT",
            mode="open",
            short_venue="aster",
            quantity=Decimal("10"),
            clip_usd=100.0,
            spot_book={"bid": 9.99, "ask": 10.01, "ts_ms": 1},
            perp_book={"bid": 11.9, "ask": 12.1, "ts_ms": 1},
        )

        with TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            with patch(
                "scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                side_effect=lambda venue, **kwargs: Adapter(venue),
            ), patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 9.9, "ask": 10.1, "ts_ms": 1},
                )),
            ):
                result = await execute_spot_perp_plan(
                    plan=plan,
                    leverage=1,
                    registry_path=registry_path,
                    allow_large_price_gap=True,
                )

        self.assertTrue(result["ok"])
        self.assertEqual(
            events[:3],
            ["aster:place_limit", "aster:wait_fill", "mexc_spot:place_market"],
        )

    async def test_maker_timeout_cancels_refreshes_price_and_replaces_order(self) -> None:
        events: list[str] = []
        prices: list[str | None] = []
        fetch_calls = 0

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue
                self.order_id = 0

            async def place_limit_order(self, **kwargs):
                events.append(f"{self.venue}:place_limit")
                prices.append(kwargs.get("price"))
                self.order_id += 1
                return {"ok": True, "order_id": f"{self.venue}-{self.order_id}"}

            async def wait_for_order_fill(self, **kwargs):
                events.append(f"{self.venue}:wait_fill")
                if len([event for event in events if event.endswith(":wait_fill")]) == 1:
                    raise RuntimeError("maker fill timeout")
                return {"ok": True, "filled": True, "status": "FILLED"}

            async def cancel_order(self, **kwargs):
                events.append(f"{self.venue}:cancel")
                return {"ok": True}

            async def place_market_order(self, **kwargs):
                events.append(f"{self.venue}:place_market")
                return {"ok": True}

            async def get_open_position(self, *, symbol: str, market_type: str):
                if self.venue == "mexc_spot":
                    return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "10"}
                return {"symbol": symbol, "market_type": market_type, "side": "SHORT", "quantity": "10"}

        async def fake_fetch_plan_books(*, symbol: str, short_venue: str, clip_usd: float):
            nonlocal fetch_calls
            fetch_calls += 1
            return (
                {"bid": 9.99, "ask": 10.01, "ts_ms": fetch_calls},
                {"bid": 9.9, "ask": 10.1 + max(0, fetch_calls - 1), "ts_ms": fetch_calls},
            )

        plan = build_spot_perp_plan(
            symbol="BEAT",
            mode="open",
            short_venue="aster",
            quantity=Decimal("10"),
            clip_usd=100.0,
            spot_book={"bid": 9.99, "ask": 10.01, "ts_ms": 1},
            perp_book={"bid": 9.9, "ask": 10.1, "ts_ms": 1},
        )

        with TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            with patch(
                "scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                side_effect=lambda venue, **kwargs: Adapter(venue),
            ), patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(side_effect=fake_fetch_plan_books),
            ), patch("asyncio.sleep", new=AsyncMock()):
                result = await execute_spot_perp_plan(
                    plan=plan,
                    leverage=1,
                    registry_path=registry_path,
                    allow_large_price_gap=True,
                )

        self.assertTrue(result["ok"])
        self.assertEqual(
            events[:6],
            [
                "aster:place_limit",
                "aster:wait_fill",
                "aster:cancel",
                "aster:place_limit",
                "aster:wait_fill",
                "mexc_spot:place_market",
            ],
        )
        self.assertEqual(prices, ["10.1", "11.1"])

    async def test_maker_timeout_retries_transient_cancel_before_replacing_order(self) -> None:
        events: list[str] = []

        class MakerAdapter:
            def __init__(self) -> None:
                self.order_id = 0
                self.cancel_attempts = 0

            async def place_limit_order(self, **kwargs):
                self.order_id += 1
                events.append(f"place_{self.order_id}")
                return {"ok": True, "order_id": f"maker-{self.order_id}"}

            async def wait_for_order_fill(self, **kwargs):
                events.append(f"wait_{kwargs['order_result']['order_id']}")
                if kwargs["order_result"]["order_id"] == "maker-1":
                    raise RuntimeError("maker fill timeout")
                return {"ok": True, "filled": True, "status": "FILLED"}

            async def cancel_order(self, **kwargs):
                self.cancel_attempts += 1
                events.append(f"cancel_{self.cancel_attempts}")
                if self.cancel_attempts < 3:
                    raise RuntimeError("Frame with ID 0 was removed.")
                return {"ok": True, "order_id": kwargs["order_result"]["order_id"]}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                events.append("place_market")
                return {"ok": True}

        async def refreshed_price() -> str:
            return "10.2"

        with patch("asyncio.sleep", new=AsyncMock()):
            result = await execute_single_clip(
                symbol="COPPER",
                clip_usd=100.0,
                quantity=Decimal("10"),
                maker_venue="variational",
                taker_venue="aster",
                short_venue="variational",
                long_venue="aster",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=0,
                state_machine=SimpleNamespace(
                    to_preview_ready=lambda: None,
                    to_awaiting_confirm=lambda: None,
                    to_placing_maker_leg=lambda: None,
                    to_hedging_taker_leg=lambda: None,
                    to_completed=lambda: None,
                    to_retrying_hedge=lambda: None,
                    to_emergency_exit=lambda: None,
                ),
                maker_price="10.1",
                require_maker_fill_confirmation=True,
                max_maker_reprice_attempts=1,
                maker_price_refresher=refreshed_price,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            events,
            [
                "place_1",
                "wait_maker-1",
                "cancel_1",
                "cancel_2",
                "cancel_3",
                "place_2",
                "wait_maker-2",
                "place_market",
            ],
        )

    async def test_variational_place_call_timeout_cancels_before_replacing_order(self) -> None:
        events: list[str] = []

        class MakerAdapter:
            def __init__(self) -> None:
                self.place_attempts = 0

            async def place_limit_order(self, **kwargs):
                self.place_attempts += 1
                events.append(f"place_{self.place_attempts}")
                if self.place_attempts == 1:
                    raise RuntimeError("variational limit order fill timeout after 60s")
                return {"ok": True, "filled": True, "status": "FILLED", "order_id": "maker-2"}

            async def cancel_order(self, **kwargs):
                events.append("cancel")
                self.assert_empty_order_result = kwargs["order_result"]
                return {"ok": True}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                events.append("place_market")
                return {"ok": True}

        adapter = MakerAdapter()
        with patch("asyncio.sleep", new=AsyncMock()):
            result = await execute_single_clip(
                symbol="COPPER",
                clip_usd=100.0,
                quantity=Decimal("10"),
                maker_venue="variational",
                taker_venue="aster",
                short_venue="variational",
                long_venue="aster",
                maker_adapter=adapter,
                taker_adapter=TakerAdapter(),
                max_hedge_retries=0,
                state_machine=SimpleNamespace(
                    to_preview_ready=lambda: None,
                    to_awaiting_confirm=lambda: None,
                    to_placing_maker_leg=lambda: None,
                    to_hedging_taker_leg=lambda: None,
                    to_completed=lambda: None,
                    to_retrying_hedge=lambda: None,
                    to_emergency_exit=lambda: None,
                ),
                maker_price="10.1",
                require_maker_fill_confirmation=True,
                max_maker_reprice_attempts=1,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(events, ["place_1", "cancel", "place_2", "place_market"])
        self.assertEqual(adapter.assert_empty_order_result, {})

    async def test_second_reprice_timeout_does_not_reuse_cancelled_order_id(self) -> None:
        cancelled_results: list[dict] = []

        class MakerAdapter:
            def __init__(self) -> None:
                self.place_attempts = 0

            async def place_limit_order(self, **kwargs):
                self.place_attempts += 1
                if self.place_attempts == 1:
                    return {"ok": True, "order_id": "maker-1"}
                if self.place_attempts == 2:
                    raise RuntimeError("variational limit order fill timeout after 60s")
                return {"ok": True, "filled": True, "status": "FILLED", "order_id": "maker-3"}

            async def wait_for_order_fill(self, **kwargs):
                raise RuntimeError("maker fill timeout")

            async def cancel_order(self, **kwargs):
                cancelled_results.append(kwargs["order_result"])
                return {"ok": True}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                return {"ok": True}

        with patch("asyncio.sleep", new=AsyncMock()):
            result = await execute_single_clip(
                symbol="COPPER",
                clip_usd=100.0,
                quantity=Decimal("10"),
                maker_venue="variational",
                taker_venue="aster",
                short_venue="variational",
                long_venue="aster",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=0,
                state_machine=SimpleNamespace(
                    to_preview_ready=lambda: None,
                    to_awaiting_confirm=lambda: None,
                    to_placing_maker_leg=lambda: None,
                    to_hedging_taker_leg=lambda: None,
                    to_completed=lambda: None,
                    to_retrying_hedge=lambda: None,
                    to_emergency_exit=lambda: None,
                ),
                maker_price="10.1",
                require_maker_fill_confirmation=True,
                max_maker_reprice_attempts=2,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(cancelled_results, [{"ok": True, "order_id": "maker-1"}, {}])

    async def test_maker_timeout_keeps_existing_order_when_refreshed_price_barely_moves(self) -> None:
        events: list[str] = []
        prices: list[str | None] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                events.append("maker:place_limit")
                prices.append(kwargs.get("price"))
                return {"ok": True, "order_id": "maker-1"}

            async def wait_for_order_fill(self, **kwargs):
                events.append("maker:wait_fill")
                if events.count("maker:wait_fill") == 1:
                    raise RuntimeError("maker fill timeout")
                return {"ok": True, "filled": True, "status": "FILLED"}

            async def cancel_order(self, **kwargs):
                events.append("maker:cancel")
                return {"ok": True}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                events.append("taker:place_market")
                return {"ok": True}

        async def barely_changed_price() -> str:
            events.append("maker:refresh_price")
            return "10.1001"

        result = await execute_single_clip(
            symbol="BEAT",
            clip_usd=100.0,
            quantity=Decimal("10"),
            maker_venue="variational",
            taker_venue="aster",
            short_venue="aster",
            long_venue="variational",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=0,
            state_machine=SimpleNamespace(
                to_preview_ready=lambda: None,
                to_awaiting_confirm=lambda: None,
                to_placing_maker_leg=lambda: None,
                to_hedging_taker_leg=lambda: None,
                to_completed=lambda: None,
                to_retrying_hedge=lambda: None,
                to_emergency_exit=lambda: None,
            ),
            maker_price="10.1",
            require_maker_fill_confirmation=True,
            max_maker_reprice_attempts=-1,
            maker_price_refresher=barely_changed_price,
            maker_reprice_min_change_pct=0.0005,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(
            events,
            [
                "maker:place_limit",
                "maker:wait_fill",
                "maker:refresh_price",
                "maker:wait_fill",
                "taker:place_market",
            ],
        )
        self.assertEqual(prices, ["10.1"])

    async def test_cancel_after_partial_fill_updates_hedge_quantity_when_order_finished(self) -> None:
        market_amounts: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                return {"ok": True, "order_id": "maker-1"}

            async def wait_for_order_fill(self, **kwargs):
                return {
                    "ok": True,
                    "partial": True,
                    "filled_quantity": "6",
                    "raw": {"status": "PARTIALLY_FILLED", "executedQty": "6"},
                }

            async def cancel_order(self, **kwargs):
                return {
                    "ok": True,
                    "order_id": "maker-1",
                    "raw": {"status": "FILLED", "executedQty": "10", "origQty": "10"},
                }

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                market_amounts.append(kwargs["amount"])
                return {"ok": True, "avgPrice": "10"}

        result = await execute_single_clip(
            symbol="BEAT",
            clip_usd=100.0,
            quantity=Decimal("10"),
            maker_venue="mexc_spot",
            taker_venue="aster",
            short_venue="aster",
            long_venue="mexc_spot",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=0,
            state_machine=SimpleNamespace(
                to_preview_ready=lambda: None,
                to_awaiting_confirm=lambda: None,
                to_placing_maker_leg=lambda: None,
                to_hedging_taker_leg=lambda: None,
                to_completed=lambda: None,
                to_retrying_hedge=lambda: None,
                to_emergency_exit=lambda: None,
            ),
            maker_price="10",
            require_maker_fill_confirmation=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(market_amounts, ["10"])
        self.assertEqual(result["executed_quantity"], "10")
        self.assertFalse(result["partial_fill"])

    async def test_partial_fill_below_min_hedge_notional_waits_without_cancel(self) -> None:
        events: list[str] = []
        market_amounts: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                events.append("place_limit")
                return {"ok": True, "order_id": "maker-1"}

            async def wait_for_order_fill(self, **kwargs):
                events.append("wait_fill")
                if events.count("wait_fill") == 1:
                    return {
                        "ok": True,
                        "partial": True,
                        "filled_quantity": "0.2",
                        "raw": {"status": "PARTIALLY_FILLED", "executedQty": "0.2"},
                    }
                return {
                    "ok": True,
                    "partial": True,
                    "filled_quantity": "0.6",
                    "raw": {"status": "PARTIALLY_FILLED", "executedQty": "0.6"},
                }

            async def cancel_order(self, **kwargs):
                events.append("cancel")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "CANCELED", "executedQty": "0.6"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                events.append("place_market")
                market_amounts.append(kwargs["amount"])
                return {"ok": True, "avgPrice": "10"}

        result = await execute_single_clip(
            symbol="BEAT",
            clip_usd=10.0,
            quantity=Decimal("1"),
            maker_venue="mexc_spot",
            taker_venue="aster",
            short_venue="aster",
            long_venue="mexc_spot",
            maker_adapter=MakerAdapter(),
            taker_adapter=TakerAdapter(),
            max_hedge_retries=0,
            state_machine=SimpleNamespace(
                to_preview_ready=lambda: None,
                to_awaiting_confirm=lambda: None,
                to_placing_maker_leg=lambda: None,
                to_hedging_taker_leg=lambda: None,
                to_completed=lambda: None,
                to_retrying_hedge=lambda: None,
                to_emergency_exit=lambda: None,
            ),
            maker_price="10",
            require_maker_fill_confirmation=True,
            min_hedge_notional_usd=5.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(events, ["place_limit", "wait_fill", "wait_fill", "cancel", "place_market"])
        self.assertEqual(market_amounts, ["0.6"])
        self.assertEqual(result["executed_quantity"], "0.6")

    async def test_partial_fill_below_min_hedge_notional_keeps_waiting_without_progress(self) -> None:
        events: list[str] = []
        market_amounts: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                events.append("place_limit")
                return {"ok": True, "order_id": "maker-1"}

            async def wait_for_order_fill(self, **kwargs):
                events.append("wait_fill")
                filled = "0.17" if events.count("wait_fill") < 3 else "0.6"
                return {
                    "ok": True,
                    "partial": True,
                    "filled_quantity": filled,
                    "raw": {"status": "PARTIALLY_FILLED", "executedQty": filled},
                }

            async def cancel_order(self, **kwargs):
                events.append("cancel")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "CANCELED", "executedQty": "0.6"}}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                events.append("place_market")
                market_amounts.append(kwargs["amount"])
                return {"ok": True, "avgPrice": "10"}

        with patch("asyncio.sleep", new=AsyncMock()):
            result = await execute_single_clip(
                symbol="BEAT",
                clip_usd=19.0,
                quantity=Decimal("1"),
                maker_venue="mexc_spot",
                taker_venue="aster",
                short_venue="aster",
                long_venue="mexc_spot",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=0,
                state_machine=SimpleNamespace(
                    to_preview_ready=lambda: None,
                    to_awaiting_confirm=lambda: None,
                    to_placing_maker_leg=lambda: None,
                    to_hedging_taker_leg=lambda: None,
                    to_completed=lambda: None,
                    to_retrying_hedge=lambda: None,
                    to_emergency_exit=lambda: None,
                ),
                maker_price="19",
                require_maker_fill_confirmation=True,
                min_hedge_notional_usd=5.0,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(events, ["place_limit", "wait_fill", "wait_fill", "wait_fill", "cancel", "place_market"])
        self.assertEqual(market_amounts, ["0.6"])
        self.assertEqual(result["executed_quantity"], "0.6")

    async def test_fill_error_cancels_active_maker_before_exit(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append("maker_wait")
                raise RuntimeError("maker fill websocket disconnected")

            async def cancel_order(self, **kwargs):
                calls.append("maker_cancel")
                return {"ok": True, "order_id": "maker-1"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True}

        with self.assertRaisesRegex(RuntimeError, "websocket disconnected"):
            await execute_single_clip(
                symbol="BTC",
                clip_usd=1000.0,
                quantity=Decimal("10"),
                maker_venue="aster",
                taker_venue="lighter",
                short_venue="aster",
                long_venue="lighter",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=1,
                state_machine=SimpleNamespace(
                    to_preview_ready=lambda: None,
                    to_awaiting_confirm=lambda: None,
                    to_placing_maker_leg=lambda: None,
                    to_hedging_taker_leg=lambda: None,
                    to_completed=lambda: None,
                    to_retrying_hedge=lambda: None,
                    to_emergency_exit=lambda: None,
                ),
                require_maker_fill_confirmation=True,
                maker_fill_timeout_seconds=5.0,
                max_maker_reprice_attempts=1,
            )

        self.assertEqual(calls, ["maker_submit", "maker_wait", "maker_cancel"])

    async def test_error_cleanup_retries_cancel_three_times(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append("maker_wait")
                raise RuntimeError("maker fill websocket disconnected")

            async def cancel_order(self, **kwargs):
                calls.append("maker_cancel")
                if calls.count("maker_cancel") < 3:
                    raise RuntimeError("temporary cancel failure")
                return {"ok": True, "order_id": "maker-1"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True}

        with patch("asyncio.sleep", new=AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, "websocket disconnected"):
                await execute_single_clip(
                    symbol="BTC",
                    clip_usd=1000.0,
                    quantity=Decimal("10"),
                    maker_venue="hyperliquid",
                    taker_venue="lighter",
                    short_venue="hyperliquid",
                    long_venue="lighter",
                    maker_adapter=MakerAdapter(),
                    taker_adapter=TakerAdapter(),
                    max_hedge_retries=1,
                    state_machine=SimpleNamespace(
                        to_preview_ready=lambda: None,
                        to_awaiting_confirm=lambda: None,
                        to_placing_maker_leg=lambda: None,
                        to_hedging_taker_leg=lambda: None,
                        to_completed=lambda: None,
                        to_retrying_hedge=lambda: None,
                        to_emergency_exit=lambda: None,
                    ),
                    require_maker_fill_confirmation=True,
                    maker_fill_timeout_seconds=5.0,
                    max_maker_reprice_attempts=1,
                )

        self.assertEqual(
            calls,
            ["maker_submit", "maker_wait", "maker_cancel", "maker_cancel", "maker_cancel"],
        )

    async def test_task_cancellation_cleans_up_active_maker(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def wait_for_order_fill(self, **kwargs):
                calls.append("maker_wait")
                raise asyncio.CancelledError()

            async def cancel_order(self, **kwargs):
                calls.append("maker_cancel")
                return {"ok": True, "order_id": "maker-1"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                return {"ok": True}

        with self.assertRaises(asyncio.CancelledError):
            await execute_single_clip(
                symbol="BTC",
                clip_usd=1000.0,
                quantity=Decimal("10"),
                maker_venue="mexc_spot",
                taker_venue="aster",
                short_venue="mexc_spot",
                long_venue="aster",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=1,
                state_machine=SimpleNamespace(
                    to_preview_ready=lambda: None,
                    to_awaiting_confirm=lambda: None,
                    to_placing_maker_leg=lambda: None,
                    to_hedging_taker_leg=lambda: None,
                    to_completed=lambda: None,
                    to_retrying_hedge=lambda: None,
                    to_emergency_exit=lambda: None,
                ),
                require_maker_fill_confirmation=True,
                maker_fill_timeout_seconds=5.0,
                max_maker_reprice_attempts=1,
            )

        self.assertEqual(calls, ["maker_submit", "maker_wait", "maker_cancel"])

    async def test_hedge_failure_cleans_up_unconfirmed_maker(self) -> None:
        calls: list[str] = []

        class MakerAdapter:
            async def place_limit_order(self, **kwargs):
                calls.append("maker_submit")
                return {"ok": True, "order_id": "maker-1", "raw": {"status": "NEW"}}

            async def cancel_order(self, **kwargs):
                calls.append("maker_cancel")
                return {"ok": True, "order_id": "maker-1"}

        class TakerAdapter:
            async def place_market_order(self, **kwargs):
                calls.append("taker_market")
                raise RuntimeError("taker unavailable")

        with self.assertRaisesRegex(RuntimeError, "hedge failed"):
            await execute_single_clip(
                symbol="BTC",
                clip_usd=1000.0,
                quantity=Decimal("10"),
                maker_venue="lighter",
                taker_venue="aster",
                short_venue="lighter",
                long_venue="aster",
                maker_adapter=MakerAdapter(),
                taker_adapter=TakerAdapter(),
                max_hedge_retries=0,
                state_machine=SimpleNamespace(
                    to_preview_ready=lambda: None,
                    to_awaiting_confirm=lambda: None,
                    to_placing_maker_leg=lambda: None,
                    to_hedging_taker_leg=lambda: None,
                    to_completed=lambda: None,
                    to_retrying_hedge=lambda: None,
                    to_emergency_exit=lambda: None,
                ),
                require_maker_fill_confirmation=False,
            )

        self.assertEqual(calls, ["maker_submit", "taker_market", "maker_cancel"])

    async def test_execution_fetches_fresh_maker_price_before_first_order(self) -> None:
        prices: list[str | None] = []

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def place_limit_order(self, **kwargs):
                prices.append(kwargs.get("price"))
                return {"ok": True, "order_id": f"{self.venue}-maker"}

            async def wait_for_order_fill(self, **kwargs):
                return {"ok": True, "filled": True, "status": "FILLED"}

            async def place_market_order(self, **kwargs):
                return {"ok": True}

            async def get_open_position(self, *, symbol: str, market_type: str):
                if self.venue == "mexc_spot":
                    return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "10"}
                return {"symbol": symbol, "market_type": market_type, "side": "SHORT", "quantity": "10"}

        async def fake_fetch_plan_books(*, symbol: str, short_venue: str, clip_usd: float):
            return (
                {"bid": 9.99, "ask": 10.01, "ts_ms": 2},
                {"bid": 9.9, "ask": 10.09, "ts_ms": 2},
            )

        plan = build_spot_perp_plan(
            symbol="BEAT",
            mode="open",
            short_venue="aster",
            quantity=Decimal("10"),
            clip_usd=100.0,
            spot_book={"bid": 9.99, "ask": 10.01, "ts_ms": 1},
            perp_book={"bid": 9.9, "ask": 10.1, "ts_ms": 1},
        )

        with TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            with patch(
                "scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                side_effect=lambda venue, **kwargs: Adapter(venue),
            ), patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(side_effect=fake_fetch_plan_books),
            ):
                result = await execute_spot_perp_plan(
                    plan=plan,
                    leverage=1,
                    registry_path=registry_path,
                    allow_large_price_gap=True,
                )

        self.assertTrue(result["ok"])
        self.assertEqual(prices, ["10.09"])

    async def test_open_execution_records_live_spot_and_perp_legs(self) -> None:
        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            async def place_limit_order(self, **kwargs):
                return {"ok": True, "filled": True, "status": "FILLED", "order_id": f"{self.venue}-maker"}

            async def wait_for_order_fill(self, **kwargs):
                return {"ok": True, "raw": {"filled": True, "status": "FILLED"}}

            async def place_market_order(self, **kwargs):
                return {"ok": True, "order_id": f"{self.venue}-taker"}

            async def get_open_position(self, *, symbol: str, market_type: str):
                if self.venue == "mexc_spot":
                    return {"symbol": symbol, "market_type": market_type, "side": "LONG", "quantity": "10.1"}
                return {"symbol": symbol, "market_type": market_type, "side": "SHORT", "quantity": "9.8"}

        plan = build_spot_perp_plan(
            symbol="BEAT",
            mode="open",
            short_venue="aster",
            quantity=Decimal("10"),
            clip_usd=100.0,
            spot_book={"bid": 9.99, "ask": 10.01, "ts_ms": 1},
            perp_book={"bid": 9.9, "ask": 10.1, "ts_ms": 1},
        )

        with TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            with patch(
                "scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                side_effect=lambda venue, **kwargs: Adapter(venue),
            ), patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 9.9, "ask": 10.1, "ts_ms": 1},
                )),
            ):
                result = await execute_spot_perp_plan(
                    plan=plan,
                    leverage=1,
                    registry_path=registry_path,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["recorded_strategy_id"], result["strategy_id"])
            registry = PositionRegistry.load(registry_path)
            short_leg = registry.get_leg(f"{result['strategy_id']}:aster:perp:short")
            spot_leg = registry.get_leg(f"{result['strategy_id']}:mexc_spot:spot:long")
            self.assertEqual(short_leg.quantity, "9.8")
            self.assertEqual(spot_leg.quantity, "10.1")

    async def test_close_execution_does_not_record_new_open_legs(self) -> None:
        class Adapter:
            async def place_limit_order(self, **kwargs):
                return {"ok": True, "filled": True, "status": "FILLED"}

            async def wait_for_order_fill(self, **kwargs):
                return {"ok": True, "raw": {"filled": True, "status": "FILLED"}}

            async def place_market_order(self, **kwargs):
                return {"ok": True}

        plan = build_spot_perp_plan(
            symbol="BEAT",
            mode="close",
            short_venue="aster",
            quantity=Decimal("10"),
            clip_usd=100.0,
            spot_book={"bid": 9.99, "ask": 10.01, "ts_ms": 1},
            perp_book={"bid": 9.9, "ask": 10.1, "ts_ms": 1},
        )

        with TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            with patch(
                "scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                side_effect=lambda venue, **kwargs: Adapter(),
            ), patch(
                "scripts.run_spot_perp_arbitrage.fetch_plan_books",
                new=AsyncMock(return_value=(
                    {"bid": 9.99, "ask": 10.01, "ts_ms": 1},
                    {"bid": 9.9, "ask": 10.1, "ts_ms": 1},
                )),
            ):
                result = await execute_spot_perp_plan(
                    plan=plan,
                    leverage=1,
                    registry_path=registry_path,
                )

            self.assertTrue(result["ok"])
            self.assertFalse(registry_path.exists())

    async def test_place_order_records_live_perp_pair_after_successful_open(self) -> None:
        class Adapter:
            def __init__(self, side: str, quantity: str) -> None:
                self.side = side
                self.quantity = quantity

            async def get_open_position(self, *, symbol: str, market_type: str):
                return {"symbol": symbol, "market_type": market_type, "side": self.side, "quantity": self.quantity}

        with TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "position_registry.json"
            strategy_id = await record_open_execution_from_live_positions(
                execution_result={
                    "ok": True,
                    "maker_result": {"ok": True},
                    "hedge_result": {"ok": True},
                },
                adapters_by_venue={
                    "aster": Adapter("SHORT", "99"),
                    "lighter": Adapter("LONG", "100"),
                },
                symbol="LAB",
                short_venue="aster",
                long_venue="lighter",
                registry_path=registry_path,
            )

            registry = PositionRegistry.load(registry_path)
            self.assertEqual(registry.get_leg(f"{strategy_id}:aster:perp:short").quantity, "99")
            self.assertEqual(registry.get_leg(f"{strategy_id}:lighter:perp:long").quantity, "100")


class VariationalBrowserAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_order_cooldown_waits_until_ten_seconds_after_previous_order(self) -> None:
        now = [100.0]
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            now[0] += seconds

        adapter = VariationalBrowserExecutionAdapter(
            broker_url="ws://127.0.0.1:8768",
            min_seconds_between_orders=10.0,
            clock=lambda: now[0],
            sleep=fake_sleep,
        )
        adapter._last_order_ts_by_broker.clear()

        await adapter._wait_for_order_cooldown()
        now[0] = 105.0
        await adapter._wait_for_order_cooldown()

        self.assertEqual(sleeps, [5.0])


if __name__ == "__main__":
    unittest.main()
