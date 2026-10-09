import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

from hydra_basis.spread_strategy import locks
from hydra_basis.spread_strategy.dispatcher import Dispatcher, Opportunity, QuoteStore, Settings
from hydra_basis.spread_strategy.history import MINUTE_MS, MinuteRecorder, SpreadHistory, build_profile

NOW = 1_791_000_000_000 - 1_791_000_000_000 % MINUTE_MS
VENUES = ("aster", "hyperliquid", "lighter", "mexc")


def series(spreads_bps, base=100.0):
    """Short/long minute closes giving the requested spread per minute, oldest first, ending now."""
    n = len(spreads_bps)
    minutes = [NOW - (n - i) * MINUTE_MS for i in range(n)]
    long = {m: base for m in minutes}
    short = {m: base * (1 + s / 10_000) for m, s in zip(minutes, spreads_bps)}
    return short, long


def profile(spreads, **kwargs):
    short, long = series(spreads)
    defaults = dict(now_ms=NOW, lookback_minutes=len(spreads), threshold_bps=30, take_profit_bps=10,
                    persistent_pct=70, min_coverage_pct=50)
    return build_profile(short, long, **(defaults | kwargs))


class ProfileTests(unittest.TestCase):
    def test_persistent_structural_spread(self):
        p = profile([35] * 1440)
        self.assertEqual((p.label, p.above_pct, p.converged_episodes), ("persistent", 100.0, 0))
        self.assertEqual(p.current_episode_minutes, 1440)
        self.assertIsNone(p.minutes_since_converged)

    def test_reverting_spread(self):
        day = ([5] * 100 + [35] * 20 + [8] * 100) * 6 + [5] * 120
        p = profile(day)
        self.assertEqual(p.label, "reverting")
        self.assertEqual((p.episodes, p.converged_episodes), (6, 6))
        self.assertEqual(p.median_minutes_to_converge, 20)
        self.assertEqual(p.minutes_since_converged, 0)

    def test_new_spike(self):
        p = profile([5] * 1437 + [40] * 3)
        self.assertEqual((p.label, p.current_episode_minutes, p.episodes), ("new_spike", 3, 1))
        self.assertEqual(p.minutes_since_converged, 3)

    def test_insufficient_coverage_and_forward_fill(self):
        short, long = series([35] * 1440)
        sparse = {m: v for i, (m, v) in enumerate(sorted(short.items())) if i > 1000}
        self.assertEqual(build_profile(sparse, long, now_ms=NOW, lookback_minutes=1440, threshold_bps=30,
                                       take_profit_bps=10, persistent_pct=70, min_coverage_pct=50).label,
                         "insufficient")
        every_third = {m: v for i, (m, v) in enumerate(sorted(short.items())) if i % 3 == 0}
        filled = build_profile(every_third, long, now_ms=NOW, lookback_minutes=1440, threshold_bps=30,
                               take_profit_bps=10, persistent_pct=70, min_coverage_pct=50)
        self.assertGreater(filled.coverage_pct, 99)  # gaps up to 5 minutes carry the last price


class RecorderTests(unittest.TestCase):
    def test_record_prune_save_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "mids.json.gz"
            recorder = MinuteRecorder(path, keep_minutes=60)
            recorder.record("lighter", "eth", 99, 101, NOW - 120 * MINUTE_MS)
            recorder.record("lighter", "ETH", 100, 102, NOW + 5_000)
            recorder.save(NOW, force=True)
            loaded = MinuteRecorder(path)
            self.assertEqual(loaded.get("lighter", "ETH"), {NOW: 101.0})


class DispatcherHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        lock_dir = patch.object(locks, "LOCK_DIR", Path(self.tmp.name) / "locks")
        lock_dir.start()
        self.addCleanup(lock_dir.stop)

    def dispatcher(self, spreads, **history):
        short, long = series(spreads)

        async def fetcher(session, venue, symbol, start, end):
            return short if venue == "aster" else long
        settings = Settings(venues=VENUES, fees={v: {"maker": D("0"), "taker": D("0")} for v in VENUES},
                            history={"check_bps": 30, **history})
        spread_history = SpreadHistory(recorder=MinuteRecorder(None), fetcher=fetcher, clock=lambda: NOW)
        events = []
        d = Dispatcher(settings, live=False, data_dir=Path(self.tmp.name), registry_path=Path("x"),
                       store=QuoteStore(clock=lambda: NOW), clock=lambda: NOW, emit=events.append,
                       history=spread_history)
        return d, events

    async def test_launch_blocks_structural_spread(self):
        d, _ = self.dispatcher([35] * 1440)
        opportunity = Opportunity("AAA", "aster", "hyperliquid", D("35"), D("100"))
        with self.assertRaisesRegex(RuntimeError, "likely structural"):
            await d.check_history(opportunity)
        allowed, _ = self.dispatcher([35] * 1440, block_persistent=False)
        self.assertEqual((await allowed.check_history(opportunity)).label, "persistent")
        await d.shutdown()
        await allowed.shutdown()

    async def test_reverting_spread_passes_and_small_spreads_skip_the_check(self):
        d, _ = self.dispatcher(([5] * 100 + [35] * 20 + [8] * 100) * 6 + [5] * 120)
        self.assertEqual((await d.check_history(Opportunity("AAA", "aster", "hyperliquid", D("35"), D("100")))).label,
                         "reverting")
        self.assertIsNone(await d.check_history(Opportunity("AAA", "aster", "hyperliquid", D("20"), D("100"))))
        await d.shutdown()

    async def test_dry_run_report_lists_history(self):
        d, _ = self.dispatcher([35] * 1440)
        d.health.update({venue: True for venue in VENUES})
        d.store.update_quotes("aster", {"AAA": {"bid": 100.5, "ask": 100.55, "ts_ms": NOW}})
        d.store.update_quotes("hyperliquid", {"AAA": {"bid": 99.95, "ask": 100.0, "ts_ms": NOW}})
        report = await d.dry_run_report()
        section = report.split("[價差歷史")[1].split("[現有倉位]")[0]
        self.assertIn("AAA", section)
        self.assertIn("持續型", section)
        await d.shutdown()


if __name__ == "__main__":
    unittest.main()
