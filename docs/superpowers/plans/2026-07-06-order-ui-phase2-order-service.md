# Order UI — Phase 2: order_service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the non-interactive open/close execution core (maker+taker) from `scripts/place_order.py` and `scripts/run_spot_perp_arbitrage.py` into a reusable `order_service.py` that adds a per-batch **interval** wait, a pre-submit **debounce** (orderbook stability) wait, and an `on_progress` callback — while keeping the CLIs working as thin wrappers.

**Architecture:** `order_service.py` owns dataclasses (`OpenParams`, `CloseParams`, `Preview`), a testable batch loop with injectable seams (`Deps`: clip executor, orderbook fetch, sleep, clock, position recorder), and preview builders that reuse `prepare_execution_preview_for_size`. The CLIs build the dataclasses from prompts and call the service with a print-based `on_progress`. taker-only execution returns a "not implemented" result (TODO).

**Tech Stack:** Python 3.11, asyncio, `unittest`.

## Global Constraints

- Test runner `unittest`: `python -m unittest tests.test_order_service`.
- Do not change the real order/hedge logic inside `execute_single_clip` / `execute_spot_perp_plan`; only wrap them.
- Existing CLI behavior and CLI args must not change; existing suites stay green: `python -m unittest tests.test_execution_engine tests.test_single_order`.
- `Decimal` for all sizes/quantities.
- `exec_mode` values: `"maker_taker"` (implemented) | `"taker"` (returns not-implemented result — TODO this phase).
- Reused signatures (verified):
  - `prepare_execution_preview_for_size(*, symbol, total_size: Decimal, clip_size: Decimal, signal: ExecutionSignal|None=None) -> (signal, ExecutionPreview, short_book, long_book)`.
  - `ExecutionPreview`: `symbol, maker_venue, taker_venue, total_usd, clip_usd, batch_count, maker_spread_pct, taker_spread_pct, requires_confirm`.
  - `execute_single_clip(*, symbol, clip_usd: float, quantity: Decimal, maker_venue, taker_venue, short_venue, long_venue, maker_adapter, taker_adapter, max_hedge_retries: int, state_machine, maker_orderbook=None, taker_orderbook=None, require_maker_fill_confirmation=False, maker_fill_timeout_seconds=60.0, max_maker_reprice_attempts=0, maker_reprice_min_change_pct=0.0, maker_price_refresher=None, taker_pre_hook=None, max_execution_price_gap_pct=0.01, ...) -> dict` (key `"ok"`, `"executed_quantity"`, price summary under `"summary"` with `maker_avg_price`/`taker_avg_price`).
  - `execute_spot_perp_plan(*, plan: SpotPerpPlan, leverage: int, broker_url=None, registry_path=POSITION_REGISTRY_PATH, allow_large_price_gap=False) -> dict`.
  - `fetch_orderbook_snapshot(session, *, venue, symbol, clip_usd) -> dict{bid, ask}` from `hydra_basis.execution_engine.market_data`.
  - `ExecutionSignal`: `symbol, short_venue, long_venue, annualized_avg, score, updated_at`.

---

## File Structure

- Create: `hydra_basis/execution_engine/order_service.py` — dataclasses, `Deps`, debounce, batch loop, preview + execute for perp-perp and spot-perp (open/close).
- Create: `tests/test_order_service.py` — unit tests using fakes (no network, no real orders).
- Modify: `scripts/place_order.py` — `run_open_execution_once` / `run_close_execution_once` build dataclasses and call the service (prompts unchanged).
- Modify: `scripts/run_spot_perp_arbitrage.py` — build dataclasses and call the service (prompts unchanged).

`order_service.py` public surface (final):
```python
@dataclass class OpenParams
@dataclass class CloseParams
@dataclass class Preview
@dataclass class Deps            # injectable seams (defaults = real implementations)
validate_open_params(params) -> None                       # raises ValueError
async def wait_for_stable_orderbook(*, venues, symbol, clip_usd, deps, debounce_ms, timeout_ms, threshold_pct=0.0005) -> float
async def build_open_preview(params, *, signal=None) -> Preview
async def execute_open(params, *, live: bool, on_progress, deps=Deps()) -> dict
async def build_close_preview(params) -> Preview
async def execute_close(params, *, live: bool, on_progress, deps=Deps()) -> dict
```

`on_progress` is `Callable[[dict], Awaitable[None] | None]`; events documented in the spec (`batch_start`, `debounce_wait`, `batch_filled`, `batch_failed`, `done`).

---

## Task 1: Dataclasses + validation

**Files:**
- Create: `hydra_basis/execution_engine/order_service.py`
- Test: `tests/test_order_service.py`

**Interfaces:**
- Produces:
  - `OpenParams(kind: str, symbol: str, short_venue: str, long_venue: str, exec_mode: str, leverage: int, total_size: Decimal, clip_size: Decimal, interval_ms: int, debounce_ms: int)`.
  - `Preview(symbol, kind, maker_venue, taker_venue, short_venue, long_venue, total_usd: float, clip_usd: float, batch_count: int, maker_spread_pct: float, taker_spread_pct: float, requires_confirm: bool, exec_mode: str)`.
  - `validate_open_params(params: OpenParams) -> None` raising `ValueError` on: non-positive sizes, `clip_size > total_size`, `short_venue == long_venue` (perp_perp only), negative `interval_ms`/`debounce_ms`, unknown `exec_mode`.

- [ ] **Step 1: Write the failing test**

```python
import unittest
from decimal import Decimal
from hydra_basis.execution_engine.order_service import OpenParams, validate_open_params


def _params(**over):
    base = dict(kind="perp_perp", symbol="BTC", short_venue="aster", long_venue="hyperliquid",
                exec_mode="maker_taker", leverage=3, total_size=Decimal("0.5"),
                clip_size=Decimal("0.1"), interval_ms=500, debounce_ms=300)
    base.update(over)
    return OpenParams(**base)


class ValidateOpenParamsTests(unittest.TestCase):
    def test_ok_params_pass(self):
        validate_open_params(_params())  # no raise

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_service -v`
Expected: FAIL — `ImportError: cannot import name 'OpenParams'`.

- [ ] **Step 3: Write minimal implementation**

```python
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

VALID_EXEC_MODES = {"maker_taker", "taker"}


@dataclass
class OpenParams:
    kind: str
    symbol: str
    short_venue: str
    long_venue: str
    exec_mode: str
    leverage: int
    total_size: Decimal
    clip_size: Decimal
    interval_ms: int
    debounce_ms: int


@dataclass
class Preview:
    symbol: str
    kind: str
    maker_venue: str
    taker_venue: str
    short_venue: str
    long_venue: str
    total_usd: float
    clip_usd: float
    batch_count: int
    maker_spread_pct: float
    taker_spread_pct: float
    requires_confirm: bool
    exec_mode: str


def validate_open_params(params: OpenParams) -> None:
    if params.exec_mode not in VALID_EXEC_MODES:
        raise ValueError(f"unknown exec_mode: {params.exec_mode}")
    if params.total_size <= 0 or params.clip_size <= 0:
        raise ValueError("total_size and clip_size must be positive")
    if params.clip_size > params.total_size:
        raise ValueError("clip_size cannot exceed total_size")
    if params.interval_ms < 0 or params.debounce_ms < 0:
        raise ValueError("interval_ms and debounce_ms must be >= 0")
    if params.kind == "perp_perp" and params.short_venue == params.long_venue:
        raise ValueError("short_venue and long_venue must differ")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_service -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/order_service.py tests/test_order_service.py
git commit -m "feat(order_service): OpenParams/Preview dataclasses + validation"
```

---

## Task 2: Deps seams + debounce (wait_for_stable_orderbook)

**Files:**
- Modify: `hydra_basis/execution_engine/order_service.py`
- Test: `tests/test_order_service.py`

**Interfaces:**
- Produces:
  - `Deps` dataclass with injectable async/sync callables (defaults = real): `fetch_orderbook(venue, symbol, clip_usd) -> dict{bid,ask}`, `sleep(seconds) -> None`, `now() -> float` (monotonic seconds), `run_clip(**kwargs) -> dict`, `record_open(**kwargs) -> str|None`.
  - `class DebounceTimeout(RuntimeError)`.
  - `async def wait_for_stable_orderbook(*, venues: list[str], symbol: str, clip_usd: float, deps: Deps, debounce_ms: int, timeout_ms: int, threshold_pct: float = 0.0005) -> float` — polls each venue's mid; returns elapsed ms once **all** venues held mid change `< threshold_pct` for a continuous `debounce_ms`; raises `DebounceTimeout` after `timeout_ms`. `debounce_ms == 0` returns `0.0` immediately.

- [ ] **Step 1: Write the failing test**

```python
import asyncio
from hydra_basis.execution_engine.order_service import Deps, wait_for_stable_orderbook, DebounceTimeout


class FakeClock:
    def __init__(self):
        self.t = 0.0
    def now(self):
        return self.t
    async def sleep(self, seconds):
        self.t += seconds


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
        prices = iter([100.0, 100.0, 100.0, 100.0, 100.0, 100.0])  # stable book
        def fetch(**k):
            p = next(prices, 100.0)
            return {"bid": p, "ask": p + 0.01}
        deps = Deps(fetch_orderbook=fetch, sleep=clock.sleep, now=clock.now)
        waited = await wait_for_stable_orderbook(
            venues=["aster"], symbol="BTC", clip_usd=1000, deps=deps,
            debounce_ms=300, timeout_ms=5000)
        self.assertGreaterEqual(waited, 300.0)

    async def test_timeout_when_never_stable(self):
        clock = FakeClock()
        jumpy = iter([100, 200, 100, 200, 100, 200, 100, 200, 100, 200, 100, 200, 100, 200])
        def fetch(**k):
            p = float(next(jumpy, 100))
            return {"bid": p, "ask": p + 0.01}
        deps = Deps(fetch_orderbook=fetch, sleep=clock.sleep, now=clock.now)
        with self.assertRaises(DebounceTimeout):
            await wait_for_stable_orderbook(
                venues=["aster"], symbol="BTC", clip_usd=1000, deps=deps,
                debounce_ms=300, timeout_ms=1000)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_service.DebounceTests -v`
Expected: FAIL — `ImportError: cannot import name 'Deps'`.

- [ ] **Step 3: Write minimal implementation**

```python
import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from typing import Any

from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
import aiohttp


class DebounceTimeout(RuntimeError):
    pass


async def _default_fetch_orderbook(venue: str, symbol: str, clip_usd: float) -> dict:
    async with aiohttp.ClientSession() as session:
        return await fetch_orderbook_snapshot(session, venue=venue, symbol=symbol, clip_usd=clip_usd)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@dataclass
class Deps:
    fetch_orderbook: Callable[..., Any] = None      # (venue, symbol, clip_usd) -> dict|awaitable
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    now: Callable[[], float] = time.monotonic
    run_clip: Callable[..., Awaitable[dict]] = None
    record_open: Callable[..., Any] = None

    def __post_init__(self):
        if self.fetch_orderbook is None:
            self.fetch_orderbook = lambda venue, symbol, clip_usd: _default_fetch_orderbook(venue, symbol, clip_usd)


def _mid(book: dict) -> float:
    return (float(book["bid"]) + float(book["ask"])) / 2


async def wait_for_stable_orderbook(
    *, venues: list[str], symbol: str, clip_usd: float, deps: Deps,
    debounce_ms: int, timeout_ms: int, threshold_pct: float = 0.0005,
) -> float:
    if debounce_ms <= 0:
        return 0.0
    start = deps.now()
    last_mid: dict[str, float] = {}
    stable_since: float | None = None
    poll_seconds = min(debounce_ms, 100) / 1000.0
    while True:
        moved = False
        for venue in venues:
            book = await _maybe_await(deps.fetch_orderbook(venue=venue, symbol=symbol, clip_usd=clip_usd))
            mid = _mid(book)
            prev = last_mid.get(venue)
            if prev is not None and prev > 0 and abs(mid - prev) / prev >= threshold_pct:
                moved = True
            last_mid[venue] = mid
        now = deps.now()
        if moved or stable_since is None:
            stable_since = now
        if (now - stable_since) * 1000.0 >= debounce_ms:
            return (now - start) * 1000.0
        if (now - start) * 1000.0 >= timeout_ms:
            raise DebounceTimeout(f"orderbook not stable within {timeout_ms}ms for {symbol}")
        await deps.sleep(poll_seconds)
```

Note: on the first poll every venue has no `prev`, so `stable_since` starts now; a genuinely still book then satisfies `debounce_ms` after enough `sleep` steps. The FakeClock advances time only via `sleep`, so the loop terminates deterministically.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_service.DebounceTests -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/order_service.py tests/test_order_service.py
git commit -m "feat(order_service): Deps seams + debounce wait_for_stable_orderbook"
```

---

## Task 3: build_open_preview (perp-perp)

**Files:**
- Modify: `hydra_basis/execution_engine/order_service.py`
- Test: `tests/test_order_service.py`

**Interfaces:**
- Consumes: `prepare_execution_preview_for_size`, `load_best_signal_for_symbol`, `ExecutionSignal`, `compute_token_batch_count` (copy the 3-line helper from `place_order.py:49` into `order_service.py` to avoid importing the script).
- Produces: `async def build_open_preview(params: OpenParams, *, signal: ExecutionSignal | None = None) -> Preview`. Loads the default signal when `signal is None`, overrides `signal.short_venue`/`long_venue` from `params`, calls `prepare_execution_preview_for_size`, and maps `ExecutionPreview` → `Preview` (carrying `params.exec_mode`, `kind`, chosen `short_venue`/`long_venue`).

- [ ] **Step 1: Write the failing test**

```python
from unittest import mock
from hydra_basis.execution_engine.order_service import build_open_preview
from hydra_basis.execution_engine.models import ExecutionPreview, ExecutionSignal


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_service.BuildOpenPreviewTests -v`
Expected: FAIL — `ImportError: cannot import name 'build_open_preview'`.

- [ ] **Step 3: Write minimal implementation**

```python
from hydra_basis.config import MONITOR_SIGNALS_PATH
from hydra_basis.execution_engine.models import ExecutionSignal
from hydra_basis.execution_engine.runtime import prepare_execution_preview_for_size
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol


def compute_token_batch_count(total_size: Decimal, clip_size: Decimal) -> int:
    from math import ceil
    return int(ceil(total_size / clip_size))


async def build_open_preview(params: OpenParams, *, signal: ExecutionSignal | None = None) -> Preview:
    validate_open_params(params)
    if signal is None:
        signal = load_best_signal_for_symbol(path=MONITOR_SIGNALS_PATH, symbol=params.symbol)
    signal.short_venue = params.short_venue
    signal.long_venue = params.long_venue
    _sig, exec_preview, _short_book, _long_book = await prepare_execution_preview_for_size(
        symbol=params.symbol,
        total_size=params.total_size,
        clip_size=params.clip_size,
        signal=signal,
    )
    return Preview(
        symbol=exec_preview.symbol,
        kind=params.kind,
        maker_venue=exec_preview.maker_venue,
        taker_venue=exec_preview.taker_venue,
        short_venue=signal.short_venue,
        long_venue=signal.long_venue,
        total_usd=exec_preview.total_usd,
        clip_usd=exec_preview.clip_usd,
        batch_count=exec_preview.batch_count,
        maker_spread_pct=exec_preview.maker_spread_pct,
        taker_spread_pct=exec_preview.taker_spread_pct,
        requires_confirm=exec_preview.requires_confirm,
        exec_mode=params.exec_mode,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_service.BuildOpenPreviewTests -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/order_service.py tests/test_order_service.py
git commit -m "feat(order_service): build_open_preview"
```

---

## Task 4: execute_open batch loop (interval / debounce / progress / stop-on-fail / taker TODO)

**Files:**
- Modify: `hydra_basis/execution_engine/order_service.py`
- Test: `tests/test_order_service.py`

**Interfaces:**
- Consumes: `build_open_preview`, `wait_for_stable_orderbook`, `Deps`.
- Produces: `async def execute_open(params: OpenParams, *, live: bool, on_progress, deps: Deps = None) -> dict`.
  - `exec_mode == "taker"` → immediately return `{"ok": False, "error": "taker-only 尚未實作 (TODO)", "not_implemented": True}` (no `deps.run_clip` call).
  - Otherwise: build preview, then loop `batch_count` batches. Each batch: emit `batch_start`; `wait_for_stable_orderbook` → emit `debounce_wait`; call `deps.run_clip(...)`; on `not ok` emit `batch_failed` and return failure (stop); on ok emit `batch_filled` with `maker_price`/`taker_price` (from `result["summary"]`) and `qty`; `deps.record_open(...)` when `live`; sleep `interval_ms/1000` between batches. After loop emit `done`. Returns `{"ok": True, "batches": n, "executed_qty": str}`.
  - `deps` default: real `Deps()` whose `run_clip` wraps `execute_single_clip` (see impl); tests pass a fake `run_clip`.
- Progress events (dicts): `{"type":"batch_start","index","total","clip_size"}`, `{"type":"debounce_wait","waited_ms"}`, `{"type":"batch_filled","index","maker_price","taker_price","qty"}`, `{"type":"batch_failed","index","error"}`, `{"type":"done","executed_qty","batches"}`.

- [ ] **Step 1: Write the failing test**

```python
from hydra_basis.execution_engine.order_service import execute_open


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
        deps = self._deps_for(clock, [ok, ok, ok, ok, ok])  # 0.5 / 0.1 = 5 batches
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
```

Add this helper near the top of the test module:
```python
from hydra_basis.execution_engine.order_service import Preview
def _preview(**over):
    base = dict(symbol="BTC", kind="perp_perp", maker_venue="aster", taker_venue="hyperliquid",
                short_venue="aster", long_venue="hyperliquid", total_usd=5000.0, clip_usd=1000.0,
                batch_count=5, maker_spread_pct=0.0004, taker_spread_pct=0.0006,
                requires_confirm=False, exec_mode="maker_taker")
    base.update(over)
    return Preview(**base)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_service.ExecuteOpenTests -v`
Expected: FAIL — `ImportError: cannot import name 'execute_open'`.

- [ ] **Step 3: Write minimal implementation**

```python
async def _emit(on_progress, event: dict) -> None:
    await _maybe_await(on_progress(event))


async def execute_open(params: OpenParams, *, live: bool, on_progress, deps: Deps | None = None) -> dict:
    validate_open_params(params)
    if params.exec_mode == "taker":
        return {"ok": False, "error": "taker-only 尚未實作 (TODO)", "not_implemented": True}
    deps = deps or Deps()
    preview = await build_open_preview(params)
    venues = [preview.maker_venue, preview.taker_venue]
    remaining = params.total_size
    executed_total = Decimal("0")
    index = 0
    debounce_timeout_ms = max(params.debounce_ms * 10, 5000)
    while remaining > 0:
        index += 1
        this_clip = min(params.clip_size, remaining)
        await _emit(on_progress, {"type": "batch_start", "index": index,
                                  "total": preview.batch_count, "clip_size": str(this_clip)})
        waited = await wait_for_stable_orderbook(
            venues=venues, symbol=params.symbol, clip_usd=preview.clip_usd, deps=deps,
            debounce_ms=params.debounce_ms, timeout_ms=debounce_timeout_ms)
        await _emit(on_progress, {"type": "debounce_wait", "waited_ms": waited})
        result = await deps.run_clip(params=params, preview=preview, this_clip=this_clip, live=live)
        if not result.get("ok", False):
            await _emit(on_progress, {"type": "batch_failed", "index": index,
                                      "error": str(result.get("error", "batch failed"))})
            return {"ok": False, "batches": index - 1, "error": result.get("error", "batch failed")}
        summary = result.get("summary", {}) or {}
        qty = Decimal(str(result.get("executed_quantity") or this_clip))
        await _emit(on_progress, {"type": "batch_filled", "index": index,
                                  "maker_price": summary.get("maker_avg_price"),
                                  "taker_price": summary.get("taker_avg_price"), "qty": str(qty)})
        if live and deps.record_open is not None:
            await _maybe_await(deps.record_open(result=result, params=params, preview=preview))
        executed_total += qty
        remaining = max(Decimal("0"), remaining - qty)
        if remaining > 0 and params.interval_ms > 0:
            await deps.sleep(params.interval_ms / 1000.0)
    await _emit(on_progress, {"type": "done", "executed_qty": str(executed_total), "batches": index})
    return {"ok": True, "batches": index, "executed_qty": str(executed_total)}
```

Also implement the default real `run_clip` / `record_open` on `Deps.__post_init__` by porting `execute_one_batch` (`scripts/place_order.py:581-671`) and `record_open_execution_from_live_positions` usage: build adapters via `build_adapter_for_venue`, fetch fresh books, call `execute_single_clip(...)` with the same kwargs shown at `place_order.py:628-653`, and record on success. Keep the Variational embedded-broker branch (`place_order.py:690-714`) inside the default `run_clip` when either venue is `variational`. Move `build_adapter_for_venue` into `order_service.py` (copy from `place_order.py:106-129`) so the service does not import the script.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_service.ExecuteOpenTests -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/order_service.py tests/test_order_service.py
git commit -m "feat(order_service): execute_open batch loop with interval/debounce/progress"
```

---

## Task 5: place_order.py CLI calls the service (regression)

**Files:**
- Modify: `scripts/place_order.py` — `run_open_execution_once` (`502-716`).
- Test: existing `tests/test_execution_engine.py`, `tests/test_single_order.py`.

**Interfaces:**
- Consumes: `order_service.OpenParams`, `order_service.execute_open`.
- Produces: no new public surface; `run_open_execution_once` keeps its prompts and CLI args, but after gathering inputs it builds `OpenParams` and calls `execute_open(params, live=True, on_progress=<print adapter>)`. The print adapter maps progress events to the existing `print(...)` lines (batch header, execution prices). `build_adapter_for_venue` now lives in `order_service`; re-export it from `place_order` (`from hydra_basis.execution_engine.order_service import build_adapter_for_venue`) so other call sites keep working.

- [ ] **Step 1: Add a characterization test for the print adapter**

```python
# tests/test_order_service.py
from hydra_basis.execution_engine.order_service import progress_printer


class ProgressPrinterTests(unittest.IsolatedAsyncioTestCase):
    async def test_prints_batch_filled_prices(self):
        lines = []
        printer = progress_printer(emit=lines.append)
        await printer({"type": "batch_filled", "index": 1, "maker_price": "100.0",
                       "taker_price": "100.1", "qty": "0.1"})
        self.assertTrue(any("100.0" in ln and "100.1" in ln for ln in lines))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_service.ProgressPrinterTests -v`
Expected: FAIL — `ImportError: cannot import name 'progress_printer'`.

- [ ] **Step 3: Implement progress_printer and wire the CLI**

In `order_service.py`:
```python
def progress_printer(*, emit=print):
    async def _printer(event: dict) -> None:
        t = event.get("type")
        if t == "batch_start":
            emit(f"\nbatch {event['index']}/{event['total']}  clip_size_token={event['clip_size']}")
        elif t == "debounce_wait":
            emit(f"防抖等待 {float(event['waited_ms']):.0f}ms")
        elif t == "batch_filled":
            emit(f"限價方成交: {event['maker_price']}  市價方成交: {event['taker_price']}  qty {event['qty']}")
        elif t == "batch_failed":
            emit(f"batch {event['index']} failed: {event['error']}")
        elif t == "done":
            emit(f"\nopen complete: {event['batches']} batch(es) executed")
    return _printer
```
In `scripts/place_order.py`, inside `run_open_execution_once`, after computing `total_size/clip_size/leverage/_sig` and applying the direction + maker/taker overrides, replace the local `execute_one_batch` + `run_batches` block with:
```python
from hydra_basis.execution_engine.order_service import OpenParams, execute_open, progress_printer
params = OpenParams(
    kind="perp_perp", symbol=symbol, short_venue=_sig.short_venue, long_venue=_sig.long_venue,
    exec_mode="maker_taker", leverage=leverage, total_size=total_size, clip_size=clip_size,
    interval_ms=0, debounce_ms=0,
)
# keep the existing print(...) preview block and the y/N confirmation prompts as-is
result = await execute_open(params, live=True, on_progress=progress_printer())
if not result.get("ok", False):
    raise RuntimeError(result.get("error", "execution failed"))
```

- [ ] **Step 4: Run the regression suites**

Run: `python -m unittest tests.test_order_service tests.test_execution_engine tests.test_single_order -v`
Expected: PASS (order_service tests green; existing execution/single-order tests unchanged and green).

- [ ] **Step 5: Manual smoke (dry, no orders)**

Run: `python scripts/place_order.py --ticker BTC --short_venue aster --long_venue hyperliquid`
At the `mode` prompt type `open`; confirm the preview prints as before, then answer `N` to cancel. Expected: identical preview output to pre-refactor; clean cancel.

- [ ] **Step 6: Commit**

```bash
git add scripts/place_order.py hydra_basis/execution_engine/order_service.py tests/test_order_service.py
git commit -m "refactor(place_order): open flow calls order_service (CLI unchanged)"
```

---

## Task 6: Spot-perp preview + execute via service

**Files:**
- Modify: `hydra_basis/execution_engine/order_service.py`
- Test: `tests/test_order_service.py`

**Interfaces:**
- Consumes: `execute_spot_perp_plan`, `SpotPerpPlan`, and the spot-perp plan/orderbook builders in `scripts/run_spot_perp_arbitrage.py` (`build_spot_perp_plan`, `fetch_plan_books` — confirm exact names at `run_spot_perp_arbitrage.py:389,408` before implementing; if they are script-local, move them into a new `hydra_basis/execution_engine/spot_perp_runtime.py` and import from both the script and the service).
- Produces: `async def execute_spot_perp_open(params: OpenParams, *, live, on_progress, deps=None) -> dict` that loops batches like `execute_open` but each batch calls `execute_spot_perp_plan` (spot leg fixed to `mexc`). Reuse the same debounce/interval/progress scaffolding by factoring the loop body of Task 4 into a shared `_run_batches(*, params, preview, venues, run_batch, live, on_progress, deps)` helper and passing a spot-perp `run_batch`.

- [ ] **Step 1: Write the failing test**

```python
from hydra_basis.execution_engine.order_service import execute_spot_perp_open


class ExecuteSpotPerpTests(unittest.IsolatedAsyncioTestCase):
    async def test_runs_batches_with_spot_perp_run_batch(self):
        clock = FakeClock()
        ok = {"ok": True, "executed_quantity": "0.1", "summary": {"maker_avg_price": "1", "taker_avg_price": "1"}}
        it = iter([ok, ok])
        async def run_clip(**kwargs):
            return next(it)
        deps = Deps(fetch_orderbook=lambda **k: {"bid": 100.0, "ask": 100.01},
                    sleep=clock.sleep, now=clock.now, run_clip=run_clip, record_open=lambda **k: None)
        events = []
        async def on_progress(ev): events.append(ev)
        p = _params(kind="spot_perp", long_venue="mexc", total_size=Decimal("0.2"), clip_size=Decimal("0.1"))
        with mock.patch("hydra_basis.execution_engine.order_service.build_spot_perp_preview",
                        new=mock.AsyncMock(return_value=_preview(kind="spot_perp", batch_count=2, taker_venue="mexc", long_venue="mexc"))):
            result = await execute_spot_perp_open(p, live=True, on_progress=on_progress, deps=deps)
        self.assertTrue(result["ok"])
        self.assertEqual(sum(1 for e in events if e["type"] == "batch_filled"), 2)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_service.ExecuteSpotPerpTests -v`
Expected: FAIL — `ImportError: cannot import name 'execute_spot_perp_open'`.

- [ ] **Step 3: Implement**

Factor the Task 4 loop into `_run_batches(...)`; implement `build_spot_perp_preview(params)` (wrap the script's plan/book builder into a `Preview`, spot leg = `mexc`) and `execute_spot_perp_open` that calls `_run_batches` with a `run_batch` invoking `execute_spot_perp_plan`. Default `deps.run_clip` for spot-perp builds a fresh `SpotPerpPlan` per batch (port `run_spot_perp_arbitrage.py` batch body) and calls `execute_spot_perp_plan(plan=..., leverage=params.leverage, ...)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_service.ExecuteSpotPerpTests -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/order_service.py tests/test_order_service.py
git commit -m "feat(order_service): spot-perp preview + execute via shared batch loop"
```

---

## Task 7: run_spot_perp_arbitrage.py CLI calls the service (regression) + close flows

**Files:**
- Modify: `scripts/run_spot_perp_arbitrage.py`, `scripts/place_order.py`.
- Modify: `hydra_basis/execution_engine/order_service.py` (add `build_close_preview` / `execute_close` mirroring open but using the existing `build_close_position_plan`/`execute_close_position_plan` in `place_order.py:321,386`; move those two into `order_service.py` and re-export from `place_order`).
- Test: `tests/test_order_service.py`, existing suites.

**Interfaces:**
- Produces:
  - `async def build_close_preview(params: CloseParams) -> Preview`, `async def execute_close(params: CloseParams, *, live, on_progress, deps=None) -> dict` (reuses `_run_batches`).
  - `CloseParams(kind, symbol, interval_ms, debounce_ms)` — close reads the live pair/legs (existing scan logic stays in the CLI/service).
- Consumes: existing close plan/execute functions.

- [ ] **Step 1: Write the failing test**

```python
from hydra_basis.execution_engine.order_service import CloseParams, execute_close


class ExecuteCloseTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_runs_batch_and_emits_done(self):
        clock = FakeClock()
        ok = {"ok": True, "executed_quantity": "0.1", "summary": {"maker_avg_price": "1", "taker_avg_price": "1"}}
        it = iter([ok])
        async def run_clip(**kwargs): return next(it)
        deps = Deps(fetch_orderbook=lambda **k: {"bid": 100.0, "ask": 100.01},
                    sleep=clock.sleep, now=clock.now, run_clip=run_clip, record_open=lambda **k: None)
        events = []
        async def on_progress(ev): events.append(ev)
        with mock.patch("hydra_basis.execution_engine.order_service.build_close_preview",
                        new=mock.AsyncMock(return_value=_preview(batch_count=1))):
            result = await execute_close(CloseParams(kind="perp_perp", symbol="BTC", interval_ms=0, debounce_ms=0),
                                         live=True, on_progress=on_progress, deps=deps)
        self.assertTrue(result["ok"])
        self.assertEqual(events[-1]["type"], "done")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_service.ExecuteCloseTests -v`
Expected: FAIL — `ImportError: cannot import name 'CloseParams'`.

- [ ] **Step 3: Implement + wire both CLIs**

Add `CloseParams`, `build_close_preview`, `execute_close`. In `scripts/run_spot_perp_arbitrage.py` `run_spot_perp_arbitrage()` and `scripts/place_order.py` `run_close_execution_once`, build the params dataclasses and call the service with `progress_printer()`, keeping all prompts.

- [ ] **Step 4: Run all suites**

Run: `python -m unittest tests.test_order_service tests.test_execution_engine tests.test_single_order tests.test_risk_management -v`
Expected: PASS.

- [ ] **Step 5: Manual smoke (dry)**

Run each CLI to the confirmation prompt and cancel with `N`; verify preview output matches pre-refactor.

- [ ] **Step 6: Commit**

```bash
git add scripts/run_spot_perp_arbitrage.py scripts/place_order.py hydra_basis/execution_engine/order_service.py tests/test_order_service.py
git commit -m "refactor(spot_perp/close): CLIs call order_service; add close flows"
```

---

## Phase 2 Done — Definition of Done

- `python -m unittest tests.test_order_service` green.
- `order_service.py` exposes: `OpenParams, CloseParams, Preview, Deps, validate_open_params, wait_for_stable_orderbook, build_open_preview, execute_open, build_spot_perp_preview, execute_spot_perp_open, build_close_preview, execute_close, progress_printer, build_adapter_for_venue`.
- Both CLIs still run with unchanged prompts/args; `test_execution_engine`, `test_single_order`, `test_risk_management` remain green.
- taker-only returns a clear not-implemented result.

**Next phase:** Phase 3 — Web UI (FastAPI backend consuming `apy_service` + `order_service`; 繁中 frontend; `scripts/run_order_ui.py`).
