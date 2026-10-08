import asyncio
from dataclasses import replace
from decimal import Decimal as D
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from hydra_basis.spread_strategy.core import (
    Config, Quote, State, StateStore, Strategy, entry_allowed, entry_net_bps,
    executable_prices, projected_exit_net, spread_bps,
)
from hydra_basis.spread_strategy.broker import (
    LiveBroker, PaperBroker, GuardedMaker, QuoteSource, actual_average, confirmed_average,
)
from hydra_basis.risk_management.registry import PositionRegistry


def config(**kwargs):
    defaults = dict(symbol="ETH", short_venue="aster", long_venue="hyperliquid",
                    maker_venue="aster", total_quantity=D("2"), clip_quantity=D("1"),
                    entry_bps=D("40"), take_profit_bps=D("10"),
                    fees={v: {"maker": D("0"), "taker": D("0")} for v in ("aster", "hyperliquid")},
                    slippage_buffer_bps=D("0"), funding_budget_bps=D("0"))
    return Config(**(defaults | kwargs))


class Source:
    def __init__(self):
        self.values = {"aster": ("101", "101.1"), "hyperliquid": ("99.9", "100")}
        self.quantities = []

    async def pair(self, quantity):
        self.quantities.append(quantity)
        return {venue: Quote(D(bid), D(ask), int(time.time() * 1000))
                for venue, (bid, ask) in self.values.items()}

    def converged(self):
        self.values = {"aster": ("100", "100.05"), "hyperliquid": ("100", "100.01")}


class Broker(PaperBroker):
    def __init__(self, config, source):
        super().__init__(config, source)
        self.calls = []
        self.fraction = D("1")
        self.fail = False
        self.skip = False

    async def execute(self, intent, quantity, state, force=False):
        self.calls.append((intent, quantity, force))
        if self.fail:
            raise RuntimeError("unknown hedge outcome")
        if self.skip:
            return {"ok": True, "skipped": True}
        return await super().execute(intent, quantity * self.fraction, state, force)


class CoreTests(unittest.TestCase):
    def test_bid_ask_direction_and_percent_units(self):
        c = config()
        quotes = {"aster": Quote(D("101"), D("102"), 1),
                  "hyperliquid": Quote(D("99"), D("100"), 1)}
        self.assertEqual(executable_prices(c, quotes, "entry"), (D("101"), D("100")))
        self.assertEqual(executable_prices(c, quotes, "exit"), (D("102"), D("99")))
        self.assertEqual(spread_bps(D("101"), D("100")), D("100"))

    def test_fees_and_funding_prevent_gross_only_entry(self):
        c = config(fees={v: {"maker": D("0"), "taker": D("0.001")} for v in config().venues},
                   funding_budget_bps=D("5"))
        self.assertFalse(entry_allowed(c, D("100.4"), D("100")))
        self.assertLess(entry_net_bps(c, D("100.4"), D("100")), 0)

    def test_validate_nonfinite_same_venue_and_fee_units(self):
        for kwargs in ({"total_quantity": D("NaN")}, {"long_venue": "aster"},
                       {"take_profit_bps": D("40")}, {"execution_method": "bad"},
                       {"fees": {v: {"maker": D("1"), "taker": D("0")} for v in config().venues}}):
            with self.assertRaises(ValueError):
                config(**kwargs)

    def test_reject_stale_future_invalid_or_slow_quotes(self):
        c = config()
        for quote in (Quote(D("2"), D("1"), 10000), Quote(D("1"), D("2"), 0),
                      Quote(D("1"), D("2"), 20000), Quote(D("1"), D("2"), 10000, 0),
                      Quote(D("1"), D("2"), 10000, None, 4)):
            with self.assertRaises(ValueError):
                quote.validate(now_ms=10000, config=c)

    def test_accounting_uses_entry_prices_and_allocates_opening_fees(self):
        c = config(funding_budget_bps=D("10"))
        s = State(c.fingerprint(), "paper", "test", quantity="2", short_average="101",
                  long_average="100", entry_fees_remaining="0.4")
        self.assertEqual(projected_exit_net(c, s, D("100"), D("100"), D("1")), D("0.7"))

    def test_actual_average_never_uses_limit_price(self):
        self.assertIsNone(actual_average({"raw": {"price": "123"}}))
        self.assertEqual(actual_average({"raw": {"avgPx": "100"}}), D("100"))


class StrategyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = StateStore(Path(self.tmp.name) / "state.json")
        self.c = config()
        self.source = Source()
        self.broker = Broker(self.c, self.source)
        self.engine = Strategy(self.c, self.broker, self.store, live=False)

    async def test_complete_single_cycle_and_no_reentry(self):
        await self.engine.step()
        await self.engine.step()
        self.assertEqual(self.engine.state.quantity, "2")
        self.source.converged()
        await self.engine.step()
        self.assertEqual(self.engine.state.phase, "EXITING")
        await self.engine.step()
        self.assertEqual(self.engine.state.phase, "DONE")
        self.source.values["aster"] = ("102", "103")
        await self.engine.step()
        self.assertEqual([call[0] for call in self.broker.calls], ["entry", "entry", "exit", "exit"])

    async def test_partial_fill_reduces_remaining_capacity(self):
        self.broker.fraction = D("0.4")
        await self.engine.step()
        self.assertEqual(D(self.engine.state.quantity), D("0.4"))
        self.broker.fraction = D("1")
        await self.engine.step()
        await self.engine.step()
        self.assertEqual(self.broker.calls[-1][1], D("0.6"))
        self.assertEqual(D(self.engine.state.quantity), D("2"))

    async def test_restart_preserves_entries_and_exit_latch(self):
        await self.engine.step()
        self.source.converged()
        await self.engine.step()
        resumed = Strategy(self.c, self.broker, self.store, live=False)
        self.assertEqual(resumed.state.phase, "DONE")
        await resumed.step()
        self.assertEqual(len(self.broker.calls), 2)

    async def test_restart_halfway_through_exit_never_reopens(self):
        await self.engine.step()
        await self.engine.step()
        self.source.converged()
        await self.engine.step()
        resumed = Strategy(self.c, self.broker, self.store, live=False)
        self.assertEqual(resumed.state.phase, "EXITING")
        self.source.values = {"aster": ("102", "103"), "hyperliquid": ("99", "100")}
        await resumed.step()
        self.assertEqual(len(self.broker.calls), 3)
        self.assertEqual(resumed.state.phase, "EXITING")

    async def test_unknown_hedge_pauses_and_pending_cannot_replay(self):
        self.broker.fail = True
        with self.assertRaisesRegex(RuntimeError, "unknown hedge"):
            await self.engine.step()
        self.assertEqual(self.engine.state.phase, "PAUSED")
        self.assertIsNotNone(self.engine.state.pending)
        with self.assertRaisesRegex(RuntimeError, "automatic replay refused"):
            Strategy(self.c, self.broker, self.store, live=False)

    async def test_persist_intent_before_dispatch(self):
        original = self.broker.execute
        async def inspect_intent(*args):
            payload = json.loads(self.store.path.read_text())
            self.assertEqual(payload["pending"]["intent"], "entry")
            self.assertEqual(payload["quantity"], "0")
            return await original(*args)
        self.broker.execute = inspect_intent
        await self.engine.step()
        self.assertIsNone(self.engine.state.pending)

    async def test_quote_changes_before_submission_skip_without_capacity_loss(self):
        self.broker.skip = True
        await self.engine.step()
        self.assertEqual(self.engine.state.quantity, "0")
        self.assertEqual(self.engine.state.phase, "WAITING")
        self.assertIsNone(self.engine.state.pending)

    async def test_stop_loss_closes_with_force_and_cannot_add(self):
        self.c = replace(self.c, stop_loss_usd=D("1"))
        self.broker.config = self.c
        self.engine = Strategy(self.c, self.broker, self.store, live=False)
        await self.engine.step()
        self.source.values = {"aster": ("103", "104"), "hyperliquid": ("99", "100")}
        await self.engine.step()
        self.assertEqual(self.broker.calls[-1], ("exit", D("1"), True))
        self.assertEqual(self.engine.state.exit_reason, "stop_loss")

    async def test_max_hold_forces_exit(self):
        await self.engine.step()
        self.engine.state.first_entry_ms = int(time.time() * 1000) - 3600001
        await self.engine.step()
        self.assertEqual(self.engine.state.exit_reason, "max_hold")
        self.assertTrue(self.broker.calls[-1][2])

    async def test_concurrent_steps_do_not_double_dispatch(self):
        c = replace(self.c, total_quantity=D("1"))
        self.broker.config = c
        engine = Strategy(c, self.broker, self.store, live=False)
        await asyncio.gather(engine.step(), engine.step())
        self.assertEqual(len(self.broker.calls), 1)

    async def test_corrupt_state_and_mode_change_are_rejected(self):
        await self.engine.step()
        with self.assertRaisesRegex(ValueError, "mode mismatch"):
            Strategy(self.c, self.broker, self.store, live=True)
        self.store.path.write_text("{")
        with self.assertRaises(json.JSONDecodeError):
            Strategy(self.c, self.broker, self.store, live=False)

    async def test_cost_negative_exit_waits_despite_price_threshold(self):
        await self.engine.step()
        # Both venues move; the spread meets the threshold but the actual paired
        # PnL is negative. Entry/exit bps difference alone is insufficient.
        self.source.values = {"aster": ("2000", "2001.5"), "hyperliquid": ("2000", "2002")}
        await self.engine.step()
        self.assertEqual(self.engine.state.phase, "HOLDING")
        self.assertEqual(len(self.broker.calls), 1)


class Adapter:
    """Deterministic exchange simulator used with the REAL clip executor."""
    def __init__(self, source, venue):
        self.source, self.venue = source, venue
        self.pos = D("0")
        self.orders = []
        self.skip_margin_setup = False
        self.dispatches = []
        self.last_price = None
        self.last_qty = None
        self.maker_fraction = D("1")

    async def get_open_position(self, **kwargs):
        return None if not self.pos else {"side": "LONG" if self.pos > 0 else "SHORT",
                                         "quantity": str(abs(self.pos))}

    async def list_open_orders(self, **kwargs):
        return self.orders

    async def place_limit_order(self, **kwargs):
        return self.fill({**kwargs, "amount": str(D(kwargs["amount"]) * self.maker_fraction)}, D(kwargs["price"]))

    async def place_market_order(self, **kwargs):
        bid, ask = self.source.values[self.venue]
        return self.fill(kwargs, D(ask if kwargs["side"] == "BUY" else bid))

    def fill(self, kwargs, price):
        qty = D(kwargs["amount"])
        self.pos += qty * (1 if kwargs["side"] == "BUY" else -1)
        self.last_price, self.last_qty = price, qty
        self.dispatches.append(kwargs)
        return {"ok": True, "order_id": 1, "terminal": True, "status": "FILLED",
                "filled_quantity": str(qty), "avg_price": str(price)}

    async def wait_for_order_fill(self, **kwargs):
        if self.last_qty == 0:
            raise RuntimeError("maker fill timeout")
        return {"ok": True, "filled_quantity": str(self.last_qty), "avg_price": str(self.last_price)}

    async def cancel_order(self, **kwargs):
        return await self.get_order_execution()

    async def get_order_execution(self, **kwargs):
        return {"ok": True, "terminal": True, "status": "CANCELED",
                "filled_quantity": str(self.last_qty), "avg_price": str(self.last_price)}


class LiveIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.c = config(total_quantity=D("1"))
        self.source = Source()
        self.adapters = {v: Adapter(self.source, v) for v in self.c.venues}
        self.registry_path = Path(self.tmp.name) / "registry.json"
        self.broker = LiveBroker(self.c, self.source, self.adapters, self.registry_path)
        self.store = StateStore(Path(self.tmp.name) / "state.json")
        self.engine = Strategy(self.c, self.broker, self.store, live=True)

    async def test_real_executor_opens_hedges_and_reduce_only_closes(self):
        await self.engine.step()
        self.assertEqual(self.adapters["aster"].pos, D("-1"))
        self.assertEqual(self.adapters["hyperliquid"].pos, D("1"))
        legs = PositionRegistry.load(self.registry_path).legs_for_strategy(self.engine.state.strategy_id)
        self.assertEqual(len(legs), 2)
        self.assertTrue(all(leg.status == "open" for leg in legs))
        self.source.converged()
        await self.engine.step()
        self.assertEqual(self.engine.state.phase, "DONE")
        for adapter in self.adapters.values():
            self.assertEqual(adapter.pos, D("0"))
            self.assertTrue(adapter.dispatches[-1]["reduce_only"])
        self.assertTrue(all(leg.status == "closed" for leg in
                            PositionRegistry.load(self.registry_path).legs_for_strategy(self.engine.state.strategy_id)))

    async def test_foreign_positions_or_open_orders_block_dispatch(self):
        for mismatch in ("position", "orders"):
            self.adapters["aster"].pos = D("1") if mismatch == "position" else D("0")
            self.adapters["aster"].orders = [{"order_id": 99}] if mismatch == "orders" else []
            with self.assertRaises(RuntimeError):
                await self.broker.reconcile(self.engine.state)
        self.assertFalse(self.adapters["aster"].dispatches)

    async def test_guard_refuses_maker_if_spread_disappears(self):
        self.source.converged()
        maker = GuardedMaker(self.adapters["aster"], self.broker, "entry", D("1"), self.engine.state, False)
        with self.assertRaisesRegex(RuntimeError, "spread changed"):
            await maker.place_limit_order(symbol="ETH", side="SELL", amount="1", price="101", clip_usd=100)
        self.assertFalse(self.adapters["aster"].dispatches)

    async def test_taker_taker_uses_confirmed_market_fills(self):
        c = replace(self.c, execution_method="taker_taker")
        self.broker.config = c
        engine = Strategy(c, self.broker, self.store, live=True)
        await engine.step()
        self.assertEqual(engine.state.quantity, "1")
        self.source.converged()
        await engine.step()
        self.assertEqual(engine.state.phase, "DONE")
        self.assertEqual(engine.state.short_average, "101")

    async def test_forced_exit_uses_reduce_only_market_orders(self):
        await self.engine.step()
        self.engine.state.first_entry_ms = int(time.time() * 1000) - 3600001
        await self.engine.step()
        self.assertEqual(self.engine.state.phase, "DONE")
        self.assertEqual(self.engine.state.exit_reason, "max_hold")
        self.assertTrue(all(adapter.dispatches[-1]["reduce_only"] for adapter in self.adapters.values()))

    async def test_partial_hedge_prices_are_weighted(self):
        payload = {"attempts": [
            {"result": {"terminal": True, "filled_quantity": "0.4", "avg_price": "100"}},
            {"result": {"terminal": True, "filled_quantity": "0.6", "avg_price": "102"}}]}
        self.assertEqual(await confirmed_average(object(), "ETH", payload, D("1")), D("101.2"))

    async def test_real_executor_partial_maker_hedges_only_actual_fill(self):
        self.adapters["aster"].maker_fraction = D("0.4")
        await self.engine.step()
        self.assertEqual(D(self.engine.state.quantity), D("0.4"))
        self.assertEqual(self.adapters["hyperliquid"].dispatches[0]["amount"], "0.4")
        self.assertEqual(self.adapters["aster"].pos + self.adapters["hyperliquid"].pos, 0)

    async def test_terminal_unfilled_maker_returns_to_monitoring(self):
        self.adapters["aster"].maker_fraction = D("0")
        await self.engine.step()
        self.assertEqual(self.engine.state.phase, "WAITING")
        self.assertIsNone(self.engine.state.pending)
        self.assertFalse(self.adapters["hyperliquid"].dispatches)

    async def test_variational_pair_routing_uses_same_executor(self):
        for short, long, maker in (("variational", "aster", "variational"),
                                   ("aster", "variational", "aster"),
                                   ("hyperliquid", "variational", "variational")):
            source = Source()
            source.values = {short: ("101", "101.1"), long: ("99.9", "100")}
            c = config(short_venue=short, long_venue=long, maker_venue=maker,
                       total_quantity=D("1"),
                       fees={v: {"maker": D("0"), "taker": D("0")} for v in (short, long)})
            adapters = {v: Adapter(source, v) for v in c.venues}
            broker = LiveBroker(c, source, adapters, Path(self.tmp.name) / f"{short}-{long}.registry.json")
            engine = Strategy(c, broker, StateStore(Path(self.tmp.name) / f"{short}-{long}.json"), live=True)
            await engine.step()
            self.assertEqual(adapters[short].pos, D("-1"))
            self.assertEqual(adapters[long].pos, D("1"))

    async def test_missing_actual_fill_prices_preserve_pending_and_pause(self):
        with patch("hydra_basis.spread_strategy.broker.execute_single_clip_with_sides", new=AsyncMock(
            return_value={"ok": True, "hedge_verified": True, "executed_quantity": "1",
                          "maker_result": {"price": "101"}, "hedge_result": {"price": "100"}})):
            # Acknowledged results without position movement are insufficient.
            with self.assertRaises(RuntimeError):
                await self.engine.step()
        self.assertEqual(self.engine.state.phase, "PAUSED")
        self.assertIsNotNone(self.engine.state.pending)

    async def test_corrupt_registry_never_adopts_or_overwrites(self):
        self.registry_path.write_text("{")
        with self.assertRaises(json.JSONDecodeError):
            await self.broker.reconcile(self.engine.state)
        self.assertEqual(self.registry_path.read_text(), "{")


class AdapterReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_aster_open_order_query_is_symbol_scoped(self):
        from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
        adapter = AsterExecutionAdapter.__new__(AsterExecutionAdapter)
        adapter._resolve_raw_symbol = AsyncMock(return_value="ETHUSDT")
        adapter.build_signed_params = lambda payload: payload
        adapter._get_signed_query = AsyncMock(return_value=[])
        self.assertEqual(await adapter.list_open_orders(symbol="ETH"), [])
        adapter._get_signed_query.assert_awaited_once_with(
            f"{adapter.BASE_URL}/fapi/v3/openOrders", {"symbol": "ETHUSDT"})

    async def test_hyperliquid_filters_open_orders_and_weights_same_oid_fills(self):
        from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter
        adapter = HyperliquidExecutionAdapter.__new__(HyperliquidExecutionAdapter)
        adapter.account_address = "test-account"
        with patch("hydra_basis.execution_engine.hyperliquid_adapter.fetch_json", new=AsyncMock(
            return_value=[{"coin": "ETH", "oid": 7}, {"coin": "BTC", "oid": 8}])):
            self.assertEqual(await adapter.list_open_orders(symbol="ETH"), [{"coin": "ETH", "oid": 7}])
        with patch("hydra_basis.execution_engine.hyperliquid_adapter.fetch_json", new=AsyncMock(
            return_value=[{"coin": "ETH", "oid": 7, "sz": "0.4", "px": "100"},
                          {"coin": "ETH", "oid": 7, "sz": "0.6", "px": "102"},
                          {"coin": "ETH", "oid": 8, "sz": "5", "px": "10"}])):
            self.assertEqual(await adapter.get_fill_average_price(symbol="ETH", order_result={"order_id": 7},
                                                                  quantity=D("1")), D("101.2"))

    async def test_variational_open_order_check_is_read_only_symbol_scope(self):
        from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter
        adapter = VariationalBrowserExecutionAdapter.__new__(VariationalBrowserExecutionAdapter)
        adapter.has_open_order = AsyncMock(return_value=True)
        orders = await adapter.list_open_orders(symbol="ETH")
        self.assertTrue(orders)
        adapter.has_open_order.assert_awaited_once_with(order_result={}, symbol="ETH", side="", amount="")

    async def test_size_tier_quote_and_aster_update_id_not_mistaken_for_timestamp(self):
        c = config()
        async def fetch(_session, *, venue, symbol, clip_usd):
            return {"bid": 100, "ask": 100, "ts_ms": 9999999999 if venue == "aster" else int(time.time() * 1000)}
        with patch("hydra_basis.spread_strategy.broker.fetch_orderbook_snapshot", new=AsyncMock(side_effect=fetch)) as mocked:
            quotes = await QuoteSource(c, None).pair(D("2"))
            self.assertIsNone(quotes["aster"].source_ms)
            self.assertIsNotNone(quotes["hyperliquid"].source_ms)
            self.assertEqual([call.kwargs["clip_usd"] for call in mocked.await_args_list], [1.0, 1.0, 200.0, 200.0])


if __name__ == "__main__":
    unittest.main()
