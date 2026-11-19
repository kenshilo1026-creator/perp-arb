import unittest
import asyncio
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from hydra_basis.execution_engine.depth_sizing import SizeRules, combined_size_rules, depth_clip
from hydra_basis.execution_engine.maker_price_guard import MakerPriceGuard, adverse_price_gap
from scripts.run_spot_perp_arbitrage import execute_depth_sized_batches, run_spot_perp_arbitrage


def filters(step="1", minimum="1", maximum="10000"):
    return {"LOT_SIZE": {"filterType": "LOT_SIZE", "stepSize": step, "minQty": minimum, "maxQty": maximum},
            "MIN_NOTIONAL": {"filterType": "MIN_NOTIONAL", "minNotional": "5", "notional": "5"}}


class DepthSizingTests(unittest.TestCase):
    def test_cli_gap_timeout_prints_reason_and_exits(self):
        from scripts.run_spot_perp_arbitrage import main, GAP_WAIT_TIMEOUT_SECONDS
        from hydra_basis.execution_engine.maker_price_guard import MakerGapTimeout
        self.assertEqual(GAP_WAIT_TIMEOUT_SECONDS, 60)
        with patch("scripts.run_spot_perp_arbitrage.run_spot_perp_arbitrage", new=AsyncMock(side_effect=MakerGapTimeout("超時"))), \
             patch("builtins.print") as output:
            with self.assertRaises(SystemExit) as exc:
                main()
        self.assertEqual(exc.exception.code, 1)
        output.assert_called_once_with("[gap-timeout] 超時", flush=True)

    def test_wait_mode_does_not_retreat_price_to_bypass_adverse_best_quote(self):
        kwargs = dict(remaining=D(1000), maker_book={"bid": ".1154", "ask": ".1158"},
            taker_book={"bid": ".115", "ask": ".1151", "bids": [(".115", 10000)]},
            maker_side="BUY", tick=D(".0001"), rules=combined_size_rules(filters()), wait_for_gap=True)
        q, p, v = depth_clip(max_gap=D("0.002"), **kwargs)
        self.assertEqual(q, 0)
        self.assertEqual(p, D(".1154"))

    def test_exchange_steps_use_common_multiple_and_limits(self):
        a, b = filters("0.02", "0.02", "20"), filters("0.03", "0.03", "30")
        b["MARKET_LOT_SIZE"] = {"stepSize": "0", "minQty": "0", "maxQty": "10"}
        r = combined_size_rules(a, b)
        self.assertEqual(r.step, D("0.06"))
        self.assertEqual(r.maximum, D(10))

    def test_real_syn_depth_chooses_smaller_tradable_clip(self):
        r = combined_size_rules(filters(), filters("0.1", "0.1"))
        book = {"bid": "0.11808", "ask": "0.1182", "age_ms": 0,
                "bids": [("0.11808", "356.3"), ("0.11806", "43.9"),
                         ("0.11804", "328.5"), ("0.11802", "359.3")]}
        q, p, v = depth_clip(max_gap=D("0.002"), remaining=D(1000), maker_book={"bid": ".1182", "ask": ".1186"},
            taker_book=book, maker_side="BUY", tick=D(".0001"), rules=r, improve=True)
        self.assertGreater(q, D("356.3"))
        self.assertLess(q, 1000)
        self.assertEqual(q % r.step, 0)
        self.assertLess(adverse_price_gap(p, v, "BUY"), D(".002"))
        guard = MakerPriceGuard(fetch_books=None, tick_size=None, maker_side="BUY", taker_side="SELL", quantity=q+1)
        self.assertGreaterEqual(adverse_price_gap(p, guard.taker_price(book), "BUY"), D(".002"))

    def test_reserves_minimum_tail_and_never_exceeds_depth_or_remaining(self):
        rules = SizeRules(D(1), D(1), D(1000), D(5), D("Infinity"))
        for side in ("BUY", "SELL"):
            q, _, _ = depth_clip(max_gap=D("0.002"), remaining=D(12), maker_book={"bid": 1, "ask": 1.01},
                taker_book={"bid": 1, "ask": 1.01, "bids": [(1, 10)], "asks": [(1.01, 10)]},
                maker_side=side, tick=D(".01"), rules=rules)
            self.assertEqual(q, 6)  # reserve six units to meet conservative $5 minimum

    def test_no_depth_capacity_waits_and_untradable_remainder_is_explicit(self):
        rules = combined_size_rules(filters(), filters())
        kwargs = dict(maker_book={"bid": 100, "ask": 101},
            taker_book={"bid": 100, "ask": 101, "bids": [(100, ".5")]},
            maker_side="BUY", tick=D(".1"), rules=rules)
        self.assertEqual(depth_clip(max_gap=D("0.002"), remaining=D(10), **kwargs)[0], 0)
        with self.assertRaisesRegex(ValueError, "multiple"):
            depth_clip(max_gap=D("0.002"), remaining=D("1.1"), **kwargs)
        with self.assertRaisesRegex(ValueError, "below current exchange minimum"):
            depth_clip(max_gap=D("0.002"), remaining=D(1), **{**kwargs, "maker_book": {"bid": 1, "ask": 2}})

    def test_exact_boundary_preserves_tradable_tail(self):
        q, p, v = depth_clip(max_gap=D("0.002"), remaining=D(20), maker_book={"bid": 10, "ask": 11},
            taker_book={"bid": 10, "ask": 11, "bids": [(10, 10), ("9.96", 10)]},
            maker_side="BUY", tick=D(".01"),
            rules=SizeRules(D(1), D(5), D(100), D(5), D("Infinity")))
        self.assertEqual(q, 15)
        self.assertLess(adverse_price_gap(p, v, "BUY"), D(".002"))

    def test_market_quantity_and_notional_caps_bound_size(self):
        a, b = filters(), filters()
        b["MARKET_LOT_SIZE"] = {"maxQty": "7"}
        kwargs = dict(remaining=D(100), maker_book={"bid": 100, "ask": 101},
            taker_book={"bid": 100, "ask": 101, "bids": [(100, 100)]},
            maker_side="BUY", tick=D(".1"))
        self.assertEqual(depth_clip(max_gap=D("0.002"), rules=combined_size_rules(a, b), **kwargs)[0], 7)
        b["NOTIONAL"] = {"maxNotional": "500"}
        self.assertEqual(depth_clip(max_gap=D("0.002"), rules=combined_size_rules(a, b), **kwargs)[0], 4)

    def test_stale_and_missing_depth_cannot_authorize_order(self):
        kwargs = dict(remaining=D(10), maker_book={"bid": 100, "ask": 101}, maker_side="BUY",
                      tick=D(".1"), rules=combined_size_rules(filters(), filters()))
        with self.assertRaisesRegex(RuntimeError, "stale"):
            depth_clip(max_gap=D("0.002"), taker_book={"bid": 100, "ask": 101, "age_ms": 1001, "bids": [(100, 10)]}, **kwargs)
        with self.assertRaisesRegex(ValueError, "full hedge depth"):
            depth_clip(max_gap=D("0.002"), taker_book={"bid": 100, "ask": 101}, **kwargs)


class AutoBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_silent_depth_stream_times_out_without_any_order(self):
        from hydra_basis.execution_engine.maker_price_guard import MakerGapTimeout
        spot, perp, book = self.adapters(depth="0.1")
        perp.refresh_hedge_quote.return_value["bids"] = [(100, ".1")]
        spot.wait_for_hedge_quote = AsyncMock(side_effect=asyncio.Event().wait)
        execute = AsyncMock()
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[spot, perp]), \
             patch("scripts.run_spot_perp_arbitrage.execute_spot_perp_plan", new=execute), \
             patch("scripts.run_spot_perp_arbitrage.GAP_WAIT_TIMEOUT_SECONDS", 0.02):
            with self.assertRaises(MakerGapTimeout):
                await execute_depth_sized_batches(symbol="SYN", mode="open", total_size=D(10), leverage=1)
        execute.assert_not_awaited()
        spot.close.assert_awaited_once()
        perp.close.assert_awaited_once()

    def adapters(self, depth=7):
        book = {"bid": 100, "ask": 100.1, "bids": [(100, depth)], "asks": [(100.1, depth)]}
        common = dict(prepare_execution=AsyncMock(), prepare_hedge_quotes=AsyncMock(), close=AsyncMock(),
                      get_price_tick_size=AsyncMock(return_value="0.1"), wait_for_hedge_quote=AsyncMock())
        spot = SimpleNamespace(**common, _load_symbol_rules=AsyncMock(return_value={"filters": filters()}),
                               refresh_hedge_quote=AsyncMock(side_effect=lambda: book),
                               reset_completed_batch_accounting=Mock())
        perp = SimpleNamespace(prepare_execution=AsyncMock(), prepare_hedge_quotes=AsyncMock(), close=AsyncMock(),
            get_price_tick_size=AsyncMock(return_value="0.1"), _resolve_raw_symbol=AsyncMock(return_value="SYNUSDT"),
            _load_exchange_info_by_symbol=AsyncMock(return_value={"SYNUSDT": {"filters": list(filters().values())}}),
            refresh_hedge_quote=AsyncMock(return_value={"bid": 100, "ask": 100.3,
                "bids": [(100, 100)], "asks": [(100.3, 100)]}))
        return spot, perp, book

    async def test_dynamic_batches_count_actual_partial_fills_and_share_connections(self):
        spot, perp, _ = self.adapters()
        requested = []
        async def execute(*, plan, adapters, **kwargs):
            self.assertIs(adapters["binance_spot"], spot)
            requested.append(plan.quantity)
            return {"ok": True, "executed_quantity": "2" if len(requested) == 1 else str(plan.quantity)}
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[spot, perp]), \
             patch("scripts.run_spot_perp_arbitrage.execute_spot_perp_plan", new=execute), patch("builtins.print"):
            result = await execute_depth_sized_batches(symbol="SYN", mode="open", total_size=D(20), leverage=1)
        self.assertEqual(requested, [D(7), D(7), D(7), D(4)])
        self.assertEqual(result["executed_qty"], "20")
        self.assertEqual(spot.reset_completed_batch_accounting.call_count, 4)
        spot.prepare_hedge_quotes.assert_awaited_once()
        spot.close.assert_awaited_once()
        perp.close.assert_awaited_once()

    async def test_waits_for_depth_then_completes_without_zero_size_submission(self):
        spot, perp, book = self.adapters(depth="0.1")
        perp.refresh_hedge_quote.return_value["bids"] = [(100, ".1")]
        async def update():
            book["asks"] = [(100.1, 10)]
        spot.wait_for_hedge_quote = AsyncMock(side_effect=update)
        execute = AsyncMock(return_value={"ok": True, "executed_quantity": "10"})
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[spot, perp]), \
             patch("scripts.run_spot_perp_arbitrage.execute_spot_perp_plan", new=execute), \
             patch("asyncio.sleep", new=AsyncMock()), patch("builtins.print"):
            await execute_depth_sized_batches(symbol="SYN", mode="open", total_size=D(10), leverage=1)
        spot.wait_for_hedge_quote.assert_awaited_once()
        execute.assert_awaited_once()
        self.assertEqual(execute.call_args.kwargs["plan"].quantity, 10)

    async def test_invalid_fill_ack_stops_without_submitting_next_batch(self):
        spot, perp, _ = self.adapters()
        execute = AsyncMock(return_value={"ok": True})
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[spot, perp]), \
             patch("scripts.run_spot_perp_arbitrage.execute_spot_perp_plan", new=execute), patch("builtins.print"):
            with self.assertRaisesRegex(RuntimeError, "invalid confirmed"):
                await execute_depth_sized_batches(symbol="SYN", mode="open", total_size=D(20), leverage=1)
        execute.assert_awaited_once()
        spot.close.assert_awaited_once()
        spot.reset_completed_batch_accounting.assert_not_called()

    async def test_role_changes_only_between_completed_batches_and_keeps_total(self):
        spot, perp, spot_book = self.adapters()
        perp_book = perp.refresh_hedge_quote.return_value
        selected = []
        async def execute(*, plan, **kwargs):
            selected.append((plan.maker_venue, plan.taker_venue, plan.quantity))
            spot_book.update(bid=99, ask=101, bids=[(99, 9)], asks=[(101, 9)])
            perp_book.update(bid=100, ask=100.1, bids=[(100, 4)], asks=[(100.1, 4)])
            return {"ok": True, "executed_quantity": str(plan.quantity)}
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter", side_effect=[spot, perp]), \
             patch("scripts.run_spot_perp_arbitrage.execute_spot_perp_plan", new=execute), patch("builtins.print"):
            result = await execute_depth_sized_batches(symbol="SYN", mode="open", total_size=D(20), leverage=1)
        self.assertEqual(selected[0], ("aster", "binance_spot", D(7)))
        self.assertEqual(selected[1:], [("binance_spot", "aster", D(q)) for q in (4, 4, 4, 1)])
        self.assertEqual(result["executed_qty"], "20")

    async def test_cli_without_clip_never_prompts_for_clip(self):
        args = SimpleNamespace(mode="close", symbol="SYN", short_venue="aster", total_size="1000",
                               clip_size=None, leverage=None, live=True)
        execute = AsyncMock(return_value={"ok": True})
        with patch("scripts.run_spot_perp_arbitrage.parse_args", return_value=args), \
             patch("scripts.run_spot_perp_arbitrage.fetch_plan_books", new=AsyncMock(return_value=(
                 {"bid": 100, "ask": 100.1}, {"bid": 100, "ask": 100.3}))), \
             patch("scripts.run_spot_perp_arbitrage.execute_depth_sized_batches", new=execute), \
             patch("builtins.input", return_value="PLACE LIVE SPOT PERP ORDER") as prompt, patch("builtins.print"):
            await run_spot_perp_arbitrage()
        prompt.assert_called_once_with("> ")
        execute.assert_awaited_once_with(symbol="SYN", mode="close", total_size=D(1000), leverage=1)
