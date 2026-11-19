import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.depth_sizing import SizeRules, depth_clip
from hydra_basis.execution_engine.executor import execute_single_clip_with_sides
from hydra_basis.execution_engine.maker_price_guard import (
    MakerPriceGuard, MakerPriceRecheck, MakerGapTimeout, taker_best_level,
)
from hydra_basis.execution_engine.state_machine import ExecutionStateMachine
from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, load_spot_perp_size_rules, wait_for_depth_sized_plan, execute_spot_perp_plan, wait_for_pair_quotes


RULES = SizeRules(D(1), D(1), D(10000), D(5), D("Infinity"))


def books(capacity=7):
    return ({"bid": "100", "ask": "100.1"},
            {"bid": "100", "ask": "100.1", "age_ms": 0,
             "bids": [("100", capacity), ("99", 1000)],
             "asks": [("100.1", capacity), ("101", 1000)]})


class BestLevelTests(unittest.TestCase):
    def test_latest_bbo_price_and_size_replace_old_depth_together(self):
        _, book = books(100)
        book["fast_quote"] = {"bid": "100.2", "ask": "100.3", "bid_size": "3.9", "ask_size": "4.8"}
        self.assertEqual(taker_best_level(book, "SELL"), (D("100.2"), D("3.9")))
        self.assertEqual(taker_best_level(book, "BUY"), (D("100.3"), D("4.8")))
        del book["fast_quote"]["bid_size"]
        with self.assertRaisesRegex(RuntimeError, "missing quantity"):
            taker_best_level(book, "SELL")  # Never borrow old depth size.

    def test_only_one_tick_is_used_even_if_deeper_liquidity_is_profitable(self):
        for side in ("BUY", "SELL"):
            maker, hedge = books("7.9")
            qty, price, taker = depth_clip(remaining=D(20), maker_book=maker, taker_book=hedge,
                maker_side=side, tick=D(".1"), rules=RULES, top_level_only=True, wait_for_gap=True)
            self.assertEqual(qty, 7)
            self.assertEqual(price, D("100" if side == "BUY" else "100.1"))
            self.assertEqual(taker, D("100" if side == "BUY" else "100.1"))

    def test_best_level_sizing_uses_newer_ticker_capacity_and_explicit_clip_cap(self):
        maker, hedge = books(100)
        hedge["fast_quote"] = {"bid": "100", "ask": "100.1", "bid_size": "3.9", "ask_size": "8"}
        args = dict(remaining=D(20), maker_book=maker, taker_book=hedge, maker_side="BUY",
                    tick=D(".1"), rules=RULES, top_level_only=True, wait_for_gap=True)
        self.assertEqual(depth_clip(**args)[0], 3)
        self.assertEqual(depth_clip(**args, clip_limit=D(2))[0], 2)
        hedge["fast_quote"]["bid_size"] = "0.9"
        self.assertEqual(depth_clip(**args)[0], 0)

    def test_reserves_tradable_tail_and_rejects_stale_or_mismatched_quote(self):
        maker = {"bid": "1", "ask": "1.01"}
        hedge = {"bid": "1", "ask": "1.01", "bids": [("1", 10)]}
        args = dict(remaining=D(12), maker_book=maker, taker_book=hedge, maker_side="BUY",
                    tick=D(".01"), rules=RULES, top_level_only=True, wait_for_gap=True)
        self.assertEqual(depth_clip(**args)[0], 6)
        hedge["age_ms"] = 1001
        with self.assertRaisesRegex(RuntimeError, "stale"):
            depth_clip(**args)
        hedge["age_ms"] = 0
        hedge["bids"] = [(".99", 100)]
        with self.assertRaisesRegex(RuntimeError, "mismatch"):
            depth_clip(**args)

    def test_aster_is_taker_when_its_tighter_spread_gives_lower_adverse_gap(self):
        for mode in ("open", "close"):
            plan = build_spot_perp_plan(symbol="SYN", mode=mode, short_venue="aster", quantity=D(20),
                clip_usd=None, spot_book={"bid": "99", "ask": "101"},
                perp_book={"bid": "100", "ask": "100.1"})
            self.assertEqual((plan.maker_venue, plan.taker_venue), ("binance_spot", "aster"))

    def test_binance_is_taker_when_its_spread_and_adverse_gap_are_lower(self):
        for mode in ("open", "close"):
            plan = build_spot_perp_plan(symbol="SYN", mode=mode, short_venue="aster", quantity=D(20),
                clip_usd=None, spot_book={"bid": "100", "ask": "100.1"},
                perp_book={"bid": "99", "ask": "101"})
            self.assertEqual((plan.maker_venue, plan.taker_venue), ("aster", "binance_spot"))

    def test_latest_bbo_can_change_selected_roles(self):
        spot, perp = {"bid": "100", "ask": "100.1"}, {"bid": "99", "ask": "101"}
        perp["fast_quote"] = {"bid": "100", "ask": "100.01"}
        for mode in ("open", "close"):
            plan = build_spot_perp_plan(symbol="SYN", mode=mode, short_venue="aster", quantity=D(20),
                clip_usd=None, spot_book=spot, perp_book=perp)
            self.assertEqual(plan.taker_venue, "aster")
            self.assertEqual(plan.taker_execution_price, D("100" if mode == "open" else "100.01"))

    def test_equal_gap_prefers_tighter_taker_spread(self):
        plan = build_spot_perp_plan(symbol="SYN", mode="open", short_venue="aster", quantity=D(20),
            clip_usd=None, spot_book={"bid": "100", "ask": "102.01"},
            perp_book={"bid": "99", "ask": "101"})
        # Both configurations have a 1% adverse gap; Binance's spread is narrower.
        self.assertEqual(plan.taker_venue, "binance_spot")
        plan = build_spot_perp_plan(symbol="SYN", mode="close", short_venue="aster", quantity=D(20),
            clip_usd=None, spot_book={"bid": "99", "ask": "100"},
            perp_book={"bid": "100", "ask": "101"})
        self.assertEqual(plan.taker_venue, "aster")


class BestLevelExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_reverse_role_script_wires_rules_guard_and_reduce_only_to_correct_venues(self):
        spot, perp = (SimpleNamespace(place_limit_order=AsyncMock()) for _ in range(2))
        plan = build_spot_perp_plan(symbol="SYN", mode="close", short_venue="aster", quantity=D(10),
            clip_usd=None, spot_book={"bid": "99", "ask": "101"}, perp_book={"bid": "100", "ask": "100.1"})
        directory = self.enterContext(TemporaryDirectory())
        self.enterContext(patch("hydra_basis.execution_engine.execution_trace.TRACE_PATH", Path(directory) / "timing.jsonl"))
        with patch("scripts.run_spot_perp_arbitrage.load_spot_perp_size_rules", new=AsyncMock(return_value=RULES)) as rules, \
             patch("scripts.run_spot_perp_arbitrage.create_maker_guard", new=AsyncMock()) as guard, \
             patch("scripts.run_spot_perp_arbitrage.refresh_spot_perp_maker_price", new=AsyncMock(return_value="101")), \
             patch("scripts.run_spot_perp_arbitrage.execute_single_clip", new=AsyncMock(return_value={"ok": True})) as execute:
            await execute_spot_perp_plan(plan=plan, leverage=1, adapters={"aster": perp, "binance_spot": spot})
        rules.assert_awaited_once_with(spot, perp, "SYN")
        self.assertTrue(guard.call_args.kwargs["top_level_only"])
        self.assertIs(guard.call_args.kwargs["maker_adapter"], spot)
        self.assertIs(guard.call_args.kwargs["taker_adapter"], perp)
        self.assertEqual(execute.call_args.kwargs["maker_venue"], "binance_spot")
        self.assertEqual(execute.call_args.kwargs["taker_venue"], "aster")
        self.assertFalse(execute.call_args.kwargs["maker_reduce_only"])
        self.assertTrue(execute.call_args.kwargs["taker_reduce_only"])

    async def test_aster_update_wakes_selection_and_cleans_silent_spot_waiter(self):
        cancelled = asyncio.Event()
        async def silent():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        adapters = {"aster": SimpleNamespace(wait_for_hedge_quote=AsyncMock()),
                    "binance_spot": SimpleNamespace(wait_for_hedge_quote=silent)}
        await asyncio.wait_for(wait_for_pair_quotes(adapters), 1)
        self.assertTrue(cancelled.is_set())

    async def test_auto_selects_eligible_role_and_limits_actual_taker_capacity(self):
        for mode in ("open", "close"):
            for preferred_depth in ("0.1", "3.9"):
                with self.subTest(mode=mode, depth=preferred_depth):
                    spot = {"bid": "99.9", "ask": "100.1", "bids": [("99.9", 9)], "asks": [("100.1", 9)]}
                    perp = {"bid": "100", "ask": "100.1", "bids": [("100", preferred_depth)], "asks": [("100.1", preferred_depth)]}
                    adapters = {venue: SimpleNamespace(refresh_hedge_quote=AsyncMock(return_value=book))
                                for venue, book in (("aster", perp), ("binance_spot", spot))}
                    plan, _, _ = await wait_for_depth_sized_plan(adapters=adapters, symbol="SYN", mode=mode,
                        remaining=D(20), ticks={"aster": D(".1"), "binance_spot": D(".1")}, rules=RULES)
                    self.assertEqual(plan.taker_venue, "binance_spot" if preferred_depth == "0.1" else "aster")
                    self.assertEqual(plan.quantity, 9 if preferred_depth == "0.1" else 3)

    def guard(self, maker, hedge, side="BUY", total=20):
        return MakerPriceGuard(fetch_books=AsyncMock(side_effect=lambda: (maker, hedge)),
            tick_size=AsyncMock(return_value=".1"), maker_side=side,
            taker_side="SELL" if side == "BUY" else "BUY", quantity=D(10),
            max_gap=D(".0025"), follow_maker_quote=True, wait_for_gap_seconds=.05,
            check_seconds=.001, top_level_only=True, size_rules=RULES, remaining_quantity=D(total))

    async def test_quantity_is_fixed_until_maker_price_changes(self):
        for side in ("BUY", "SELL"):
            maker, hedge = books(7)
            guard = self.guard(maker, hedge, side)
            price = await guard.prepare("100")
            self.assertEqual(guard.prepared_quantity, 7)
            self.assertEqual(guard.taker_price(hedge), D("100" if side == "BUY" else "100.1"))
            await guard.check(price)
            for capacity in (3, 12, 0, 3):
                hedge["bids" if side == "BUY" else "asks"][0] = ("100" if side == "BUY" else "100.1", capacity)
                await guard.check(price)
                self.assertEqual(guard.quantity, 7)
                self.assertEqual(guard.prepared_quantity, 7)
            maker.update(bid="100.1", ask="100.2") if side == "BUY" else maker.update(bid="99.9", ask="100")
            with self.assertRaisesRegex(MakerPriceRecheck, "best quote changed"):
                await guard.check(price)
            await guard.prepare(price)
            self.assertEqual(guard.prepared_quantity, 3)

    async def test_below_minimum_capacity_waits_without_oversizing(self):
        maker, hedge = books(".1")
        guard = self.guard(maker, hedge)
        with self.assertRaises(MakerGapTimeout):
            await guard.prepare("100")
        self.assertIsNone(guard.prepared_quantity)

    async def test_safe_order_is_kept_when_chase_is_unsafe_even_if_depth_shrinks(self):
        maker, hedge = books(10)
        guard = self.guard(maker, hedge)
        price = await guard.prepare("100")
        maker.update(bid="100.3", ask="100.4")
        await guard.check(price)
        hedge["bids"][0] = ("100", 9)
        await guard.check(price)
        self.assertEqual(guard.quantity, 10)

    async def test_quantity_lock_does_not_bypass_gap_or_stale_quote_protection(self):
        for side in ("BUY", "SELL"):
            for stale in (False, True):
                with self.subTest(side=side, stale=stale):
                    maker, hedge = books(7)
                    guard = self.guard(maker, hedge, side)
                    price = await guard.prepare("100")
                    key = "bids" if side == "BUY" else "asks"
                    hedge[key][0] = ("100" if side == "BUY" else "100.1", 1)
                    await guard.check(price)
                    if stale:
                        hedge["age_ms"] = 1001
                    else:
                        bad_price = "99" if side == "BUY" else "101"
                        hedge["bid" if side == "BUY" else "ask"] = bad_price
                        hedge[key][0] = (bad_price, 1)
                    with self.assertRaisesRegex(MakerPriceRecheck, "stale" if stale else "adverse"):
                        await guard.check(price)

    async def test_actual_syn_prices_use_best_quote_instead_of_multi_level_vwap(self):
        maker = {"bid": ".1147", "ask": ".1148"}
        hedge = {"bid": ".11469", "ask": ".11470", "bids": [(".11469", 100), (".11400", 1000)]}
        guard = self.guard(maker, hedge, total=1000)
        guard.quantity = guard.quantity_cap = D(1000)
        guard.tick_size = AsyncMock(return_value=".0001")
        price = await guard.prepare(".1147")
        self.assertEqual(guard.prepared_quantity, 100)
        self.assertEqual(guard.taker_price(hedge), D(".11469"))
        await guard.check(price)

    async def execute_with_shrink(self, side, cancel_qty="0", cancel_unknown=False, spot_maker=False,
                                  reprice=True):
        maker_book, hedge_book = books(7)
        guard = self.guard(maker_book, hedge_book, side)
        orders, events, waits = [], [], []
        async def place(**kwargs):
            orders.append(kwargs)
            events.append("place")
            return {"ok": True, "order_id": str(len(orders))}
        async def fill(**kwargs):
            waits.append(kwargs["order_result"]["order_id"])
            if len(waits) == 1:
                hedge_book["bids" if side == "BUY" else "asks"][0] = ("100" if side == "BUY" else "100.1", 3)
                if not reprice:
                    # Even an in-band taker price change must not resize an
                    # unchanged maker. Exercise the fill-timeout keep path too.
                    price = "99.9" if side == "BUY" else "100.2"
                    hedge_book["bid" if side == "BUY" else "ask"] = price
                    hedge_book["bids" if side == "BUY" else "asks"][0] = (price, 3)
                    raise RuntimeError("maker fill timeout")
                if side == "BUY":
                    maker_book.update(bid="100.1", ask="100.2")
                else:
                    maker_book.update(bid="99.9", ask="100")
                await asyncio.Event().wait()
            return {"ok": True, "terminal": True, "filled_quantity": kwargs["amount"]}
        async def cancel(**kwargs):
            events.append("cancel")
            self.assertEqual(kwargs["amount"], "7")
            return {"ok": True} if cancel_unknown else {"ok": True, "terminal": True, "filled_quantity": cancel_qty}
        async def hedge(**kwargs):
            events.append("hedge")
            return {"ok": True}
        taker = SimpleNamespace(place_market_order=AsyncMock(side_effect=hedge))
        try:
            result = await asyncio.wait_for(execute_single_clip_with_sides(
                symbol="SYN", clip_usd=1000, quantity=D(10),
                maker_venue="binance_spot" if spot_maker else "aster", taker_venue="aster" if spot_maker else "binance_spot",
                maker_side=side, taker_side="SELL" if side == "BUY" else "BUY",
                maker_adapter=SimpleNamespace(place_limit_order=place, wait_for_order_fill=fill, cancel_order=cancel),
                taker_adapter=taker, max_hedge_retries=0, state_machine=ExecutionStateMachine(),
                maker_price="100" if side == "BUY" else "100.1", maker_price_guard=guard,
                require_maker_fill_confirmation=True, max_maker_reprice_attempts=-1), 2)
        except RuntimeError:
            self.assertEqual(events, ["place", "cancel"])
            taker.place_market_order.assert_not_awaited()
            raise
        return result, orders, events, taker

    async def test_price_replacement_samples_smaller_depth_after_confirmed_zero_fill_cancel(self):
        for side in ("BUY", "SELL"):
            result, orders, events, taker = await self.execute_with_shrink(side)
            self.assertEqual([o["amount"] for o in orders], ["7", "3"])
            self.assertEqual([o["clip_usd"] for o in orders], [700, 300])
            self.assertEqual(events, ["place", "cancel", "place", "hedge"])
            self.assertEqual(result["executed_quantity"], "3")
            self.assertEqual(taker.place_market_order.call_args.kwargs["amount"], "3")

    async def test_fill_during_price_cancel_is_hedged_before_any_new_order(self):
        for side in ("BUY", "SELL"):
            result, orders, events, taker = await self.execute_with_shrink(side, cancel_qty="4")
            self.assertEqual(len(orders), 1)
            self.assertEqual(events, ["place", "cancel", "hedge"])
            self.assertEqual(result["executed_quantity"], "4")
            self.assertEqual(taker.place_market_order.call_args.kwargs["amount"], "4")

    async def test_unknown_cancel_fill_never_resizes_or_duplicates(self):
        with self.assertRaisesRegex(RuntimeError, "no terminal fill quantity"):
            await self.execute_with_shrink("BUY", cancel_unknown=True)

    async def test_depth_change_and_fill_timeout_keep_order_and_hedge_original_quantity(self):
        for side in ("BUY", "SELL"):
            for spot_maker in (False, True):
                with self.subTest(side=side, spot_maker=spot_maker):
                    result, orders, events, taker = await self.execute_with_shrink(side, spot_maker=spot_maker, reprice=False)
                    self.assertEqual([o["amount"] for o in orders], ["7"])
                    self.assertEqual(events, ["place", "hedge"])
                    self.assertEqual(result["executed_quantity"], "7")
                    self.assertEqual(taker.place_market_order.call_args.kwargs["amount"], "7")

    async def test_binance_maker_reduces_to_aster_depth_and_hedges_cancel_fill(self):
        for side in ("BUY", "SELL"):
            for cancel_qty in ("0", "4"):
                result, orders, events, taker = await self.execute_with_shrink(side, cancel_qty, spot_maker=True)
                self.assertEqual([o["amount"] for o in orders], ["7", "3"] if cancel_qty == "0" else ["7"])
                expected = "3" if cancel_qty == "0" else "4"
                self.assertEqual(result["executed_quantity"], expected)
                self.assertEqual(taker.place_market_order.call_args.kwargs["amount"], expected)

    async def test_combines_actual_exchange_metadata_for_common_step(self):
        spot = SimpleNamespace(_load_symbol_rules=AsyncMock(return_value={"filters": {
            "LOT_SIZE": {"stepSize": ".02", "minQty": ".02", "maxQty": "100"}}}))
        perp = SimpleNamespace(_resolve_raw_symbol=AsyncMock(return_value="SYNUSDT"),
            _load_exchange_info_by_symbol=AsyncMock(return_value={"SYNUSDT": {"filters": [
                {"filterType": "LOT_SIZE", "stepSize": ".03", "minQty": ".03", "maxQty": "50"}]}}))
        rules = await load_spot_perp_size_rules(spot, perp, "SYN")
        self.assertEqual(rules.step, D(".06"))
        self.assertEqual(rules.maximum, 50)
