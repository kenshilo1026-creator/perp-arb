import unittest
from decimal import Decimal
from unittest import mock

from hydra_basis.execution_engine.models import ExecutionPreview, ExecutionSignal
from hydra_basis.execution_engine.order_service import (
    Deps,
    DebounceTimeout,
    OpenParams,
    Preview,
    build_open_preview,
    execute_open,
    progress_printer,
    validate_open_params,
    wait_for_stable_orderbook,
)


def _params(**over):
    base = dict(kind="perp_perp", symbol="BTC", short_venue="aster", long_venue="hyperliquid",
                exec_mode="maker_taker", leverage=3, total_size=Decimal("0.5"),
                clip_size=Decimal("0.1"), interval_ms=500, debounce_ms=300)
    base.update(over)
    return OpenParams(**base)


def _preview(**over):
    base = dict(symbol="BTC", kind="perp_perp", maker_venue="aster", taker_venue="hyperliquid",
                short_venue="aster", long_venue="hyperliquid", total_usd=5000.0, clip_usd=1000.0,
                batch_count=5, maker_spread_pct=0.0004, taker_spread_pct=0.0006,
                requires_confirm=False, exec_mode="maker_taker")
    base.update(over)
    return Preview(**base)


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    async def sleep(self, seconds):
        self.t += seconds


class ValidateOpenParamsTests(unittest.TestCase):
    def test_ok_params_pass(self):
        validate_open_params(_params())

    def test_clip_larger_than_total_raises(self):
        with self.assertRaises(ValueError):
            validate_open_params(_params(clip_size=Decimal("1.0")))

    def test_same_venue_raises_for_perp_perp(self):
        with self.assertRaises(ValueError):
            validate_open_params(_params(long_venue="aster"))

    def test_negative_debounce_raises(self):
        with self.assertRaises(ValueError):
            validate_open_params(_params(debounce_ms=-1))

    def test_unknown_exec_mode_raises(self):
        with self.assertRaises(ValueError):
            validate_open_params(_params(exec_mode="whatever"))


class DebounceTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_zero_when_debounce_disabled(self):
        clock = FakeClock()
        deps = Deps(fetch_orderbook=lambda **k: {"bid": 100, "ask": 100.1},
                    sleep=clock.sleep, now=clock.now)
        waited = await wait_for_stable_orderbook(
            venues=["aster"], symbol="BTC", clip_usd=1000, deps=deps,
            debounce_ms=0, timeout_ms=5000)
        self.assertEqual(waited, 0.0)

    async def test_waits_until_stable(self):
        clock = FakeClock()
        deps = Deps(fetch_orderbook=lambda **k: {"bid": 100.0, "ask": 100.01},
                    sleep=clock.sleep, now=clock.now)
        waited = await wait_for_stable_orderbook(
            venues=["aster"], symbol="BTC", clip_usd=1000, deps=deps,
            debounce_ms=300, timeout_ms=5000)
        self.assertGreaterEqual(waited, 300.0)

    async def test_timeout_when_never_stable(self):
        clock = FakeClock()
        jumpy = iter([100, 200] * 50)
        def fetch(**k):
            p = float(next(jumpy, 100))
            return {"bid": p, "ask": p + 0.01}
        deps = Deps(fetch_orderbook=fetch, sleep=clock.sleep, now=clock.now)
        with self.assertRaises(DebounceTimeout):
            await wait_for_stable_orderbook(
                venues=["aster"], symbol="BTC", clip_usd=1000, deps=deps,
                debounce_ms=300, timeout_ms=1000)


class BuildOpenPreviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_maps_execution_preview_and_keeps_params_venues(self):
        sig = ExecutionSignal("BTC", "aster", "hyperliquid", 0.42, 1.8, "2026-07-06T00:00:00+00:00")
        exec_preview = ExecutionPreview(
            symbol="BTC", maker_venue="aster", taker_venue="hyperliquid",
            total_usd=5000.0, clip_usd=1000.0, batch_count=5,
            maker_spread_pct=0.0004, taker_spread_pct=0.0006, requires_confirm=False)
        with mock.patch(
            "hydra_basis.execution_engine.order_service.prepare_execution_preview_for_size",
            new=mock.AsyncMock(return_value=(sig, exec_preview, {"bid": 1, "ask": 1}, {"bid": 1, "ask": 1})),
        ):
            preview = await build_open_preview(_params(), signal=sig)
        self.assertEqual(preview.maker_venue, "aster")
        self.assertEqual(preview.taker_venue, "hyperliquid")
        self.assertEqual(preview.batch_count, 5)
        self.assertEqual(preview.exec_mode, "maker_taker")
        self.assertEqual(preview.short_venue, "aster")


class ExecuteOpenTests(unittest.IsolatedAsyncioTestCase):
    def _deps_for(self, clock, run_results):
        it = iter(run_results)
        async def run_clip(**kwargs):
            return next(it)
        return Deps(fetch_orderbook=lambda **k: {"bid": 100.0, "ask": 100.01},
                    sleep=clock.sleep, now=clock.now, run_clip=run_clip,
                    record_open=lambda **k: "sid-1")

    async def _events(self):
        events = []
        async def on_progress(ev):
            events.append(ev)
        return events, on_progress

    async def test_taker_mode_returns_not_implemented(self):
        events, on_progress = await self._events()
        result = await execute_open(_params(exec_mode="taker"), live=False, on_progress=on_progress, deps=Deps())
        self.assertFalse(result["ok"])
        self.assertTrue(result["not_implemented"])

    async def test_runs_all_batches_and_emits_progress(self):
        clock = FakeClock()
        ok = {"ok": True, "executed_quantity": "0.1", "summary": {"maker_avg_price": "100.0", "taker_avg_price": "100.1"}}
        deps = self._deps_for(clock, [ok, ok, ok, ok, ok])
        events, on_progress = await self._events()
        with mock.patch("hydra_basis.execution_engine.order_service.build_open_preview",
                        new=mock.AsyncMock(return_value=_preview(batch_count=5))):
            result = await execute_open(_params(), live=True, on_progress=on_progress, deps=deps)
        self.assertTrue(result["ok"])
        self.assertEqual(sum(1 for e in events if e["type"] == "batch_filled"), 5)
        self.assertEqual(events[-1]["type"], "done")

    async def test_stops_on_batch_failure(self):
        clock = FakeClock()
        ok = {"ok": True, "executed_quantity": "0.1", "summary": {"maker_avg_price": "1", "taker_avg_price": "1"}}
        bad = {"ok": False, "error": "hedge failed"}
        deps = self._deps_for(clock, [ok, bad, ok, ok, ok])
        events, on_progress = await self._events()
        with mock.patch("hydra_basis.execution_engine.order_service.build_open_preview",
                        new=mock.AsyncMock(return_value=_preview(batch_count=5))):
            result = await execute_open(_params(), live=True, on_progress=on_progress, deps=deps)
        self.assertFalse(result["ok"])
        self.assertTrue(any(e["type"] == "batch_failed" for e in events))
        self.assertEqual(sum(1 for e in events if e["type"] == "batch_filled"), 1)

    async def test_zero_quantity_fill_stops_instead_of_looping(self):
        clock = FakeClock()
        zero = {"ok": True, "executed_quantity": "0", "summary": {"maker_avg_price": "1", "taker_avg_price": "1"}}
        deps = self._deps_for(clock, [zero, zero, zero, zero, zero])
        events, on_progress = await self._events()
        with mock.patch("hydra_basis.execution_engine.order_service.build_open_preview",
                        new=mock.AsyncMock(return_value=_preview(batch_count=5))):
            result = await execute_open(_params(), live=True, on_progress=on_progress, deps=deps)
        self.assertFalse(result["ok"])
        self.assertIn("zero quantity", result["error"])

    async def test_default_path_calls_execute_open_clip(self):
        # No run_clip injected -> execute_open should drive the real per-batch
        # executor (execute_open_clip), which we mock so no real order is placed.
        clock = FakeClock()
        events, on_progress = await self._events()
        calls = []
        async def fake_clip(**kwargs):
            calls.append(kwargs)
            return {"ok": True, "executed_quantity": "0.1",
                    "summary": {"maker_avg_price": "100", "taker_avg_price": "100.1"}}
        deps = Deps(fetch_orderbook=lambda **k: {"bid": 100.0, "ask": 100.01},
                    sleep=clock.sleep, now=clock.now)  # run_clip is None
        with mock.patch("hydra_basis.execution_engine.order_service.build_open_preview",
                        new=mock.AsyncMock(return_value=_preview(batch_count=1))), \
             mock.patch("scripts.place_order.execute_open_clip", new=fake_clip):
            result = await execute_open(
                _params(total_size=Decimal("0.1"), clip_size=Decimal("0.1")),
                live=True, on_progress=on_progress, deps=deps)
        self.assertTrue(result["ok"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["maker_venue"], "aster")
        self.assertEqual(calls[0]["batch_clip_size"], Decimal("0.1"))


class ProgressPrinterTests(unittest.IsolatedAsyncioTestCase):
    async def test_prints_batch_filled_prices(self):
        lines = []
        printer = progress_printer(emit=lines.append)
        await printer({"type": "batch_filled", "index": 1, "maker_price": "100.0",
                       "taker_price": "100.1", "qty": "0.1"})
        self.assertTrue(any("100.0" in ln and "100.1" in ln for ln in lines))


if __name__ == "__main__":
    unittest.main()
