import asyncio
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

from hydra_basis.spread_strategy import locks
from hydra_basis.spread_strategy.broker import MexcUnits
from hydra_basis.spread_strategy.dispatcher import (
    Dispatcher, QuoteStore, Settings, common_lot, find_opportunities, size_group,
)
from hydra_basis.spread_strategy.core import Leg, State
from hydra_basis.spread_strategy.dispatcher import Opportunity
from hydra_basis.spread_strategy.estimates import estimate_group, estimate_opportunity
from hydra_basis.spread_strategy.instruments import Instrument

VENUES = ("aster", "hyperliquid", "lighter", "mexc")
ZERO_FEES = {v: {"maker": D("0"), "taker": D("0")} for v in VENUES}


def settings(**kwargs):
    defaults = dict(venues=VENUES, fees=ZERO_FEES, min_profit_bps=D("0"), slippage_buffer_bps=D("0"),
                    funding_budget_bps=D("0"), confirm_seconds=0, scan_seconds=0.01,
                    strategy={"tick_seconds": 0.01, "clip_interval_seconds": 0}, history={"enabled": False})
    return Settings(**(defaults | kwargs))


class Clock:
    def __init__(self):
        self.t = 1_700_000_000_000

    def __call__(self):
        return self.t


def quote(store, venue, symbol, bid, ask, clock):
    store.update_quotes(venue, {symbol: {"bid": float(bid), "ask": float(ask), "ts_ms": clock()}})


def instrument(venue, symbol):
    return Instrument(venue, tick_size=D("0.01"), lot_size=D("0.001"), min_size=D("0.001"), min_notional=D("5"))


async def until(condition, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


class ScanTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.store = QuoteStore(clock=self.clock)
        self.health = {venue: True for venue in VENUES}

    def scan(self, s=None, **kwargs):
        return find_opportunities(s or settings(), self.store, self.health, self.clock(), **kwargs)

    def test_direction_and_ranking(self):
        quote(self.store, "aster", "ETH", "100.5", "100.55", self.clock)
        quote(self.store, "hyperliquid", "ETH", "99.95", "100", self.clock)
        quote(self.store, "lighter", "SOL", "10.07", "10.08", self.clock)
        quote(self.store, "mexc", "SOL", "9.99", "10", self.clock)
        found = self.scan()
        self.assertEqual([(o.symbol, o.short_venue, o.long_venue) for o in found],
                         [("SOL", "lighter", "mexc"), ("ETH", "aster", "hyperliquid")])
        self.assertEqual(found[1].entry_bps, D("50"))

    def test_filters(self):
        for symbol in ("KPEPE", "1000PEPE"):  # aliases differ in units: never traded
            quote(self.store, "aster", symbol, "100.5", "100.55", self.clock)
            quote(self.store, "hyperliquid", symbol, "99.95", "100", self.clock)
        quote(self.store, "aster", "WIDE", "100.5", "101.5", self.clock)  # own book too wide
        quote(self.store, "hyperliquid", "WIDE", "99.95", "100", self.clock)
        quote(self.store, "aster", "UNIT", "105", "105.01", self.clock)  # 5% apart: different contracts
        quote(self.store, "hyperliquid", "UNIT", "99.99", "100", self.clock)
        self.assertEqual(self.scan(), [])

    def test_stale_unhealthy_excluded_funding_and_lists(self):
        quote(self.store, "aster", "ETH", "100.5", "100.55", self.clock)
        quote(self.store, "hyperliquid", "ETH", "99.95", "100", self.clock)
        self.assertEqual(len(self.scan()), 1)
        self.assertEqual(self.scan(exclude={"ETH"}), [])
        self.assertEqual(self.scan(settings(symbol_blocklist=("ETH",))), [])
        self.assertEqual(self.scan(settings(symbol_allowlist=("BTC",))), [])
        self.health["aster"] = False
        self.assertEqual(self.scan(), [])
        self.health["aster"] = True
        self.store.update_asset_ctxs("aster", {"ETH": {"funding": 0.002}})
        self.assertEqual(self.scan(), [], "funding above 0.1%")
        self.store.update_asset_ctxs("aster", {"ETH": {"funding": 0.0001}})
        self.clock.t += 16_000
        self.assertEqual(self.scan(), [], "stale")

    def test_fees_raise_the_entry_bar(self):
        quote(self.store, "aster", "ETH", "100.5", "100.55", self.clock)
        quote(self.store, "hyperliquid", "ETH", "99.95", "100", self.clock)
        fees = {v: {"maker": D("0"), "taker": D("0.001")} for v in VENUES}
        self.assertEqual(self.scan(settings(fees=fees)), [])


class SizingTests(unittest.TestCase):
    def test_common_lot(self):
        lot = lambda *lots: common_lot([Instrument("x", lot_size=D(l)) for l in lots])
        self.assertEqual(lot("0.001", "0.0001"), D("0.001"))
        self.assertEqual(lot("0.01", "0.0001"), D("0.01"))
        self.assertEqual(lot("0.3", "0.2"), D("0.6"))
        self.assertIsNone(common_lot([Instrument("variational")]))

    def test_size_group_uses_notional_and_lot(self):
        s = settings()
        insts = [Instrument("a", lot_size=D("0.01")), Instrument("b", lot_size=D("0.001"))]
        self.assertEqual(size_group(s, D("2500"), insts), (D("0.04"), D("0.02")))
        self.assertEqual(size_group(s, D("100.25"), [Instrument("a", lot_size=D("0.001"))]),
                         (D("0.996"), D("0.498")), "two equal clips, no sliver")
        with self.assertRaises(RuntimeError):
            size_group(s, D("2500"), [Instrument("a", lot_size=D("1"))])

    def test_settings_validation(self):
        with self.assertRaises(ValueError):
            settings(venues=("aster",))
        with self.assertRaises(ValueError):
            settings(clip_notional_usd=D("200"))
        with self.assertRaises(ValueError):
            settings(strategy={"bogus": 1})
        self.assertEqual(settings(execution_method="maker_taker").maker_for("mexc", "lighter"), "lighter")
        loaded = Settings.load(Path("configs/spread_dispatcher.json"))
        self.assertEqual((loaded.max_groups, loaded.group_notional_usd), (5, D("100")))


class MexcUnitsTests(unittest.IsolatedAsyncioTestCase):
    async def test_contract_conversion(self):
        class Fake:
            async def place_market_order(self, **kwargs):
                self.sent = kwargs
                return {"order_id": 1}

            async def get_order_execution(self, **kwargs):
                return {"status": "FILLED", "terminal": True, "filled_quantity": "3", "raw": {"dealVol": "3"}}

            async def get_open_position(self, **kwargs):
                return {"side": "SHORT", "quantity": "5"}
        fake = Fake()
        units = MexcUnits(fake, D("0.01"))
        await units.place_market_order(symbol="ETH", side="BUY", amount="0.03", clip_usd=75)
        self.assertEqual(fake.sent["amount"], "3")
        status = await units.get_order_execution(order_result={"order_id": 1}, symbol="ETH")
        self.assertEqual(D(status["filled_quantity"]), D("0.03"))
        self.assertEqual(D((await units.get_open_position(symbol="ETH", market_type="perp"))["quantity"]), D("0.05"))
        with self.assertRaises(RuntimeError):
            await units.place_market_order(symbol="ETH", side="BUY", amount="0.015", clip_usd=1)


class EstimateTests(unittest.TestCase):
    def opportunity(self):
        return Opportunity("ETH", "aster", "hyperliquid", D("50"), D("100.25"),
                           short_bid=D("100.5"), short_ask=D("100.55"), long_bid=D("99.95"), long_ask=D("100"))

    def test_opportunity_profit_at_take_profit(self):
        config = settings().group_config("ETH", "aster", "hyperliquid", D("1"), D("1"))
        estimate = estimate_opportunity(config, self.opportunity(), D("100"))
        # 1 unit; exit when the short converges to 100 * 1.001: (100.5 - 100.1) + 0
        self.assertEqual((estimate.quantity, estimate.gross_at_tp, estimate.net_at_tp), (D("1"), D("0.4"), D("0.4")))
        self.assertEqual(estimate.required_bps, D("40"))
        fees = {v: {"maker": D("0"), "taker": D("0.001")} for v in VENUES}
        config = settings(fees=fees).group_config("ETH", "aster", "hyperliquid", D("1"), D("1"))
        estimate = estimate_opportunity(config, self.opportunity(), D("100"))
        self.assertEqual(estimate.fees, D("0.4006"))
        self.assertEqual(estimate.net_at_tp, D("-0.0006"))

    def test_group_close_now_and_at_take_profit(self):
        config = settings().group_config("ETH", "aster", "hyperliquid", D("1"), D("1"))
        state = State("id", "paper", "g", short=Leg("-1", "100.5"), long=Leg("1", "100"))
        books = {"aster": (D("100.2"), D("100.3")), "hyperliquid": (D("100"), D("100.05"))}
        estimate = estimate_group("g", config, state, books)
        self.assertEqual((estimate.quantity, estimate.entry_spread_bps), (D("1"), D("50")))
        self.assertEqual((estimate.close_now, estimate.at_tp), (D("0.2"), D("0.4")))
        self.assertIsNone(estimate_group("g", config, state, None).close_now)


class DispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        lock_dir = patch.object(locks, "LOCK_DIR", Path(self.tmp.name) / "locks")
        lock_dir.start()
        self.addCleanup(lock_dir.stop)
        self.clock = Clock()
        self.store = QuoteStore(clock=self.clock)
        self.events = []

    def dispatcher(self, s=None, loader=None):
        d = Dispatcher(s or settings(), live=False, data_dir=Path(self.tmp.name), registry_path=Path("unused"),
                       store=self.store, clock=self.clock, emit=self.events.append,
                       instrument_loader=loader or self.loader)
        d.health.update({venue: True for venue in VENUES})
        return d

    async def loader(self, venue, symbol):
        return instrument(venue, symbol)

    def wide(self, symbol, short="aster", long="hyperliquid"):
        quote(self.store, short, symbol, "100.5", "100.55", self.clock)
        quote(self.store, long, symbol, "99.95", "100", self.clock)

    async def test_group_new_quote_trades_before_long_housekeeping_timer(self):
        self.wide("AAA")
        d = self.dispatcher(settings(strategy={"tick_seconds": 60, "clip_interval_seconds": 0}))
        try:
            await d.scan_once()
            group = next(iter(d.groups.values()))
            await until(lambda: group.engine.matched() > 0)
            self.wide("AAA")
            await until(lambda: group.engine.matched() == group.config.total_quantity, timeout=0.5)
        finally:
            await d.shutdown()

    async def test_launches_one_group_per_symbol_up_to_the_slot_limit(self):
        for symbol in ("AAA", "BBB", "CCC"):
            self.wide(symbol)
            quote(self.store, "lighter", symbol, "99.9", "100.1", self.clock)  # second pair, same symbol
        d = self.dispatcher(settings(max_groups=2))
        await d.scan_once()
        self.assertEqual(len(d.groups), 2)
        self.assertEqual(len({g.config.symbol for g in d.groups.values()}), 2)
        group = next(iter(d.groups.values()))
        total, clip = group.config.total_quantity, group.config.clip_quantity
        self.assertEqual(total, clip * 2)
        await until(lambda: all(g.engine.matched() > 0 for g in d.groups.values()))
        for current in d.groups.values():
            self.wide(current.config.symbol)  # the next clip needs a fresh snapshot
        await until(lambda: all(g.engine.matched() == g.config.total_quantity for g in d.groups.values()))
        await d.scan_once()
        self.assertEqual(len(d.groups), 2, "no free slot")
        await d.shutdown()

    async def test_scan_only_quotes_discover_but_never_trade(self):
        quote(self.store, "aster", "AAA", "100.5", "100.55", self.clock)
        self.store.update_quotes("mexc", {"AAA": {"bid": 99.95, "ask": 100.0, "ts_ms": self.clock(),
                                                  "scan_only": True}})
        d = self.dispatcher()
        d.trade_quote_timeout_seconds = 0.2
        found = await d.scan_once()
        self.assertEqual([(o.short_venue, o.long_venue) for o in found], [("aster", "mexc")])
        self.assertEqual(d.groups, {})
        self.assertIn("no fresh tradeable quotes", self.events[-1]["error"])
        d.rejected_until.clear()

        async def depth_arrives():
            await asyncio.sleep(0.05)
            self.assertIn("AAA", d.active_symbols(), "pending launch requests depth")
            quote(self.store, "mexc", "AAA", "99.95", "100", self.clock)
        d.trade_quote_timeout_seconds = 2
        asyncio.create_task(depth_arrives())
        await d.scan_once()
        self.assertEqual(len(d.groups), 1)
        await d.shutdown()

    async def test_dry_run_reports_without_opening(self):
        self.wide("AAA")
        quote(self.store, "aster", "BBB", "100.2", "100.25", self.clock)  # 20 bps: below the 40 bps gate
        quote(self.store, "hyperliquid", "BBB", "99.95", "100", self.clock)
        d = self.dispatcher()
        report = await d.dry_run_report()
        self.assertEqual(d.groups, {})
        self.assertFalse(d.index_path.exists())
        qualifying, near = report.split("[接近門檻")
        self.assertIn("AAA", qualifying)
        self.assertIn("BBB", near)
        self.assertIn("未達門檻", near)
        # Existing groups are read from the index without being started.
        live = self.dispatcher()
        await live.scan_once()
        group = next(iter(live.groups.values()))
        await until(lambda: group.engine.matched() > 0)
        await live.shutdown()
        report = await self.dispatcher().dry_run_report()
        self.assertIn(group.id, report)
        self.assertIn("[現有倉位] 1 組", report)

    async def test_confirmation_delay(self):
        self.wide("AAA")
        d = self.dispatcher(settings(confirm_seconds=3))
        await d.scan_once()
        self.assertEqual(d.groups, {})
        self.clock.t += 3_000
        self.wide("AAA")
        await d.scan_once()
        self.assertEqual(len(d.groups), 1)
        await d.shutdown()

    async def test_rejected_launch_cools_down(self):
        self.wide("AAA")

        async def strict(venue, symbol):
            return Instrument(venue, lot_size=D("0.001"), min_size=D("0.001"), min_notional=D("1000"))
        d = self.dispatcher(loader=strict)
        await d.scan_once()
        self.assertEqual(d.groups, {})
        self.assertEqual(self.events[-1]["event"], "launch_rejected")
        rejected = len(self.events)
        await d.scan_once()
        self.assertEqual(len(self.events), rejected, "symbol is cooling down")

    async def test_idle_flat_group_retires_and_frees_its_slot(self):
        self.wide("AAA")
        d = self.dispatcher(settings(idle_retire_seconds=60))
        await d.scan_once()
        group = next(iter(d.groups.values()))
        await until(lambda: group.engine.matched() > 0)
        self.store.update_quotes("aster", {"AAA": {"bid": 100.0, "ask": 100.05, "ts_ms": self.clock()}})
        self.store.update_quotes("hyperliquid", {"AAA": {"bid": 100.0, "ask": 100.05, "ts_ms": self.clock()}})
        await until(lambda: group.engine.matched() == 0)
        self.assertEqual(group.status, "RUNNING", "flat but not yet idle")
        self.clock.t += 61_000
        await until(lambda: group.id not in d.groups)
        self.assertEqual(group.engine.state.status, "STOPPED")
        locks.SymbolLock(locks.lock_path("AAA", "paper")).acquire()  # released

    async def test_symbol_lock_blocks_standalone_runner(self):
        self.wide("AAA")
        d = self.dispatcher()
        await d.scan_once()
        with self.assertRaises(RuntimeError):
            locks.SymbolLock(locks.lock_path("AAA", "paper")).acquire()
        await d.shutdown()

    async def test_restart_restores_groups(self):
        self.wide("AAA")
        d = self.dispatcher()
        await d.scan_once()
        group = next(iter(d.groups.values()))
        await until(lambda: group.engine.matched() > 0)
        await d.shutdown()
        restored = self.dispatcher()
        self.wide("AAA")
        await restored.restore()
        again = restored.groups[group.id]
        self.assertEqual(again.engine.state.short.quantity, group.engine.state.short.quantity)
        self.assertEqual(again.status, "RUNNING")
        await restored.shutdown()

    async def test_paused_group_keeps_slot_until_resumed(self):
        self.wide("AAA")
        d = self.dispatcher()
        await d.scan_once()
        group = next(iter(d.groups.values()))
        await group.engine.pause("test pause")
        await until(lambda: group.task.done())
        self.assertIn(group.id, d.groups)
        await d.shutdown()
        restored = self.dispatcher()
        self.wide("AAA")
        await restored.restore()
        self.assertEqual(restored.groups[group.id].status, "PAUSED")
        await restored.resume(group.id)
        self.assertEqual(restored.groups[group.id].status, "RUNNING")
        await restored.shutdown()


if __name__ == "__main__":
    unittest.main()
