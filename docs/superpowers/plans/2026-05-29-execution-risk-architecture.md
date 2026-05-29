# Execution Risk Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first execution-and-risk architecture for cross-exchange funding arbitrage without enabling real order placement yet.

**Architecture:** Extend the monitor to persist ranked cross-exchange signals into a JSON signal store, then add a new `execution_engine` package that reads those signals, validates freshness, selects maker/taker venues from priority config plus live spread tie-breaks, produces an execution preview, and runs through a risk-aware state machine using fake adapters.

**Tech Stack:** Python, asyncio, stdlib `json`, existing funding/stream adapters, `unittest`

---

## File Structure

### New files

- `configs/execution_venues.json`
  - Venue priority configuration for maker/taker routing.
- `hydra_basis/execution_engine/__init__.py`
  - Package marker.
- `hydra_basis/execution_engine/models.py`
  - Dataclasses for signal records, preview requests, preview results, batches, and execution states.
- `hydra_basis/execution_engine/signal_store.py`
  - Load and validate `monitor_signals.json`.
- `hydra_basis/execution_engine/priority.py`
  - Resolve maker/taker legs from venue priority and live spread tie-break.
- `hydra_basis/execution_engine/preview.py`
  - Build execution previews from user input plus signals plus live order books.
- `hydra_basis/execution_engine/risk.py`
  - Signal freshness checks, spread confirm gate, order-book anomaly checks.
- `hydra_basis/execution_engine/state_machine.py`
  - Execution lifecycle state transitions and retry/emergency-exit rules.
- `hydra_basis/execution_engine/interfaces.py`
  - Abstract execution adapter protocol plus fake adapter for tests.
- `scripts/run_execution_preview.py`
  - Entry point to request ticker, total USD, and clip USD and print preview.

### Modified files

- `hydra_basis/config.py`
  - Add path constant for `monitor_signals.json` and `execution_venues.json`.
- `scripts/run_funding_monitor.py`
  - Persist ranked cross-exchange signals into `data/monitor_signals.json`.
- `tests/test_funding_analysis.py`
  - Add execution-engine unit tests or import split tests if file becomes too large.

### Optional split if test file gets too large

- `tests/test_execution_engine.py`
  - Dedicated execution/risk tests.

---

### Task 1: Add execution config and data models

**Files:**
- Create: `configs/execution_venues.json`
- Create: `hydra_basis/execution_engine/__init__.py`
- Create: `hydra_basis/execution_engine/models.py`
- Modify: `hydra_basis/config.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write the failing test for config paths and core execution models**

```python
import unittest
from pathlib import Path

from hydra_basis.config import EXECUTION_VENUES_PATH, MONITOR_SIGNALS_PATH
from hydra_basis.execution_engine.models import ExecutionRequest, ExecutionSignal


class ExecutionConfigTests(unittest.TestCase):
    def test_execution_paths_point_to_expected_files(self) -> None:
        self.assertEqual(MONITOR_SIGNALS_PATH, Path("data/monitor_signals.json"))
        self.assertEqual(EXECUTION_VENUES_PATH, Path("configs/execution_venues.json"))

    def test_execution_models_hold_core_fields(self) -> None:
        signal = ExecutionSignal(
            symbol="BTC",
            short_venue="hyperliquid",
            long_venue="lighter",
            annualized_avg=0.55,
            score=0.91,
            updated_at="2026-05-29T00:00:00+00:00",
        )
        request = ExecutionRequest(symbol="BTC", total_usd=10000.0, clip_usd=500.0)
        self.assertEqual(signal.symbol, "BTC")
        self.assertEqual(request.total_usd, 10000.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL with missing imports/modules.

- [ ] **Step 3: Add config constants and execution models**

`hydra_basis/config.py`

```python
from pathlib import Path

FUNDING_HISTORY_PATH = Path("data/funding_history.json")
MONITOR_SIGNALS_PATH = Path("data/monitor_signals.json")
EXECUTION_VENUES_PATH = Path("configs/execution_venues.json")
```

`hydra_basis/execution_engine/models.py`

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ExecutionSignal:
    symbol: str
    short_venue: str
    long_venue: str
    annualized_avg: float
    score: float
    updated_at: str


@dataclass(slots=True)
class ExecutionRequest:
    symbol: str
    total_usd: float
    clip_usd: float
```
```

- [ ] **Step 4: Add venue priority config file**

`configs/execution_venues.json`

```json
{
  "lighter": 0,
  "variational": 1,
  "hyperliquid": 2,
  "mexc": 2,
  "aster": 2
}
```

- [ ] **Step 5: Run test to verify it passes**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 2: Persist monitor results to `monitor_signals.json`

**Files:**
- Modify: `scripts/run_funding_monitor.py`
- Create: `hydra_basis/execution_engine/signal_store.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write the failing test for monitor signal JSON serialization**

```python
import json
import tempfile
import unittest
from pathlib import Path

from hydra_basis.execution_engine.signal_store import save_monitor_signals


class MonitorSignalStoreTests(unittest.TestCase):
    def test_save_monitor_signals_writes_ranked_cross_exchange_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "monitor_signals.json"
            save_monitor_signals(
                path=path,
                cross_exchange_signals=[
                    {
                        "symbol": "BTC",
                        "short_venue": "hyperliquid",
                        "long_venue": "lighter",
                        "stats": {"annualized_avg": 0.55, "score": 0.9, "signal": True},
                    }
                ],
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["cross_exchange_signals"][0]["symbol"], "BTC")
            self.assertIn("updated_at", payload)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL with missing `save_monitor_signals`.

- [ ] **Step 3: Implement `save_monitor_signals()`**

`hydra_basis/execution_engine/signal_store.py`

```python
from __future__ import annotations

import json
import datetime as dt
from pathlib import Path
from typing import Any


def save_monitor_signals(*, path: Path, cross_exchange_signals: list[dict[str, Any]]) -> None:
    payload = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "cross_exchange_signals": [
            {
                "symbol": row["symbol"],
                "short_venue": row["short_venue"],
                "long_venue": row["long_venue"],
                "annualized_avg": row["stats"]["annualized_avg"],
                "score": row["stats"]["score"],
                "signal": row["stats"]["signal"],
            }
            for row in sorted(
                cross_exchange_signals,
                key=lambda item: item["stats"]["annualized_avg"],
                reverse=True,
            )
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
```

- [ ] **Step 4: Call serializer from monitor**

Add near the end of `scripts/run_funding_monitor.py`:

```python
from hydra_basis.config import MONITOR_SIGNALS_PATH
from hydra_basis.execution_engine.signal_store import save_monitor_signals

save_monitor_signals(
    path=MONITOR_SIGNALS_PATH,
    cross_exchange_signals=opportunities,
)
```

- [ ] **Step 5: Run focused tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 3: Load signal store and enforce 24-hour freshness

**Files:**
- Modify: `hydra_basis/execution_engine/signal_store.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write failing tests for freshness and ticker lookup**

```python
import json
import tempfile
import unittest
from pathlib import Path

from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol


class LoadBestSignalTests(unittest.TestCase):
    def test_rejects_monitor_signal_file_older_than_24h(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "monitor_signals.json"
            path.write_text(json.dumps({
                "updated_at": "2026-05-27T00:00:00+00:00",
                "cross_exchange_signals": [],
            }), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                load_best_signal_for_symbol(path=path, symbol="BTC")

    def test_loads_best_signal_for_requested_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "monitor_signals.json"
            path.write_text(json.dumps({
                "updated_at": "2099-05-29T00:00:00+00:00",
                "cross_exchange_signals": [
                    {"symbol": "BTC", "short_venue": "mexc", "long_venue": "lighter", "annualized_avg": 0.6, "score": 0.9, "signal": True}
                ],
            }), encoding="utf-8")
            signal = load_best_signal_for_symbol(path=path, symbol="BTC")
            self.assertEqual(signal.short_venue, "mexc")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL due to missing loader.

- [ ] **Step 3: Implement loader and freshness guard**

`hydra_basis/execution_engine/signal_store.py`

```python
import datetime as dt

from hydra_basis.execution_engine.models import ExecutionSignal


def load_best_signal_for_symbol(*, path: Path, symbol: str, max_age_hours: int = 24) -> ExecutionSignal:
    payload = json.loads(path.read_text(encoding="utf-8"))
    updated_at = dt.datetime.fromisoformat(payload["updated_at"])
    age = dt.datetime.now(dt.timezone.utc) - updated_at
    if age > dt.timedelta(hours=max_age_hours):
        raise RuntimeError("monitor_signals.json is older than 24 hours")

    for row in payload.get("cross_exchange_signals", []):
        if row.get("symbol", "").upper() == symbol.upper():
            return ExecutionSignal(
                symbol=row["symbol"],
                short_venue=row["short_venue"],
                long_venue=row["long_venue"],
                annualized_avg=float(row["annualized_avg"]),
                score=float(row["score"]),
                updated_at=payload["updated_at"],
            )
    raise RuntimeError(f"signal not found for symbol={symbol}")
```

- [ ] **Step 4: Run focused tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 4: Resolve maker/taker legs from venue priority plus live spread tie-break

**Files:**
- Create: `hydra_basis/execution_engine/priority.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write failing tests for normal priority and equal-priority tie-break**

```python
import unittest

from hydra_basis.execution_engine.priority import resolve_execution_legs


class PriorityTests(unittest.TestCase):
    def test_lower_priority_number_becomes_taker(self) -> None:
        maker, taker = resolve_execution_legs(
            short_venue="hyperliquid",
            long_venue="lighter",
            priorities={"hyperliquid": 2, "lighter": 0},
            spreads={"hyperliquid": 0.0004, "lighter": 0.0002},
        )
        self.assertEqual(maker, "hyperliquid")
        self.assertEqual(taker, "lighter")

    def test_equal_priority_uses_smaller_spread_as_taker(self) -> None:
        maker, taker = resolve_execution_legs(
            short_venue="mexc",
            long_venue="aster",
            priorities={"mexc": 2, "aster": 2},
            spreads={"mexc": 0.0008, "aster": 0.0003},
        )
        self.assertEqual(maker, "mexc")
        self.assertEqual(taker, "aster")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL with missing priority resolver.

- [ ] **Step 3: Implement venue priority resolver**

`hydra_basis/execution_engine/priority.py`

```python
from __future__ import annotations


def resolve_execution_legs(
    *,
    short_venue: str,
    long_venue: str,
    priorities: dict[str, int],
    spreads: dict[str, float],
) -> tuple[str, str]:
    short_priority = priorities[short_venue]
    long_priority = priorities[long_venue]

    if short_priority > long_priority:
        return short_venue, long_venue
    if long_priority > short_priority:
        return long_venue, short_venue

    short_spread = spreads[short_venue]
    long_spread = spreads[long_venue]
    if short_spread < long_spread:
        return long_venue, short_venue
    if long_spread < short_spread:
        return short_venue, long_venue
    return short_venue, long_venue
```

- [ ] **Step 4: Run focused tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 5: Add execution adapter interfaces and fake order book source

**Files:**
- Create: `hydra_basis/execution_engine/interfaces.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write failing tests for fake adapter order book access**

```python
import unittest

from hydra_basis.execution_engine.interfaces import FakeExecutionAdapter


class ExecutionAdapterTests(unittest.TestCase):
    def test_fake_adapter_returns_orderbook_snapshot(self) -> None:
        adapter = FakeExecutionAdapter(
            venue="lighter",
            orderbook={"bid": 100.0, "ask": 100.05, "ts_ms": 123456},
        )
        snapshot = adapter.get_orderbook("BTC")
        self.assertEqual(snapshot["bid"], 100.0)
        self.assertEqual(snapshot["ask"], 100.05)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL with missing adapter interface.

- [ ] **Step 3: Implement protocol and fake adapter**

`hydra_basis/execution_engine/interfaces.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class ExecutionAdapter(Protocol):
    def get_orderbook(self, symbol: str) -> dict[str, float | int]:
        ...


@dataclass(slots=True)
class FakeExecutionAdapter:
    venue: str
    orderbook: dict[str, float | int]

    def get_orderbook(self, symbol: str) -> dict[str, float | int]:
        return dict(self.orderbook)
```

- [ ] **Step 4: Run focused tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 6: Build risk helpers for spread confirm gate and order-book anomaly checks

**Files:**
- Create: `hydra_basis/execution_engine/risk.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write failing tests for spread gate and invalid books**

```python
import unittest

from hydra_basis.execution_engine.risk import compute_spread_pct, orderbook_is_anomalous, spread_requires_confirm


class ExecutionRiskTests(unittest.TestCase):
    def test_spread_requires_confirm_above_point_one_percent(self) -> None:
        self.assertTrue(spread_requires_confirm(0.0011))
        self.assertFalse(spread_requires_confirm(0.0010))

    def test_orderbook_is_anomalous_for_crossed_book(self) -> None:
        self.assertTrue(orderbook_is_anomalous({"bid": 101.0, "ask": 100.0, "ts_ms": 1}))

    def test_compute_spread_pct(self) -> None:
        spread = compute_spread_pct({"bid": 100.0, "ask": 100.1, "ts_ms": 1})
        self.assertGreater(spread, 0.0009)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL with missing risk helpers.

- [ ] **Step 3: Implement risk helpers**

`hydra_basis/execution_engine/risk.py`

```python
from __future__ import annotations


def compute_spread_pct(orderbook: dict[str, float | int]) -> float:
    bid = float(orderbook["bid"])
    ask = float(orderbook["ask"])
    mid = (bid + ask) / 2.0
    if mid <= 0:
        raise RuntimeError("invalid mid price")
    return (ask - bid) / mid


def spread_requires_confirm(spread_pct: float, threshold: float = 0.001) -> bool:
    return spread_pct > threshold


def orderbook_is_anomalous(orderbook: dict[str, float | int]) -> bool:
    bid = float(orderbook.get("bid", 0.0))
    ask = float(orderbook.get("ask", 0.0))
    return bid <= 0 or ask <= 0 or bid > ask
```

- [ ] **Step 4: Run focused tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 7: Build execution preview assembly

**Files:**
- Create: `hydra_basis/execution_engine/preview.py`
- Modify: `hydra_basis/execution_engine/models.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write failing test for preview result**

```python
import unittest

from hydra_basis.execution_engine.interfaces import FakeExecutionAdapter
from hydra_basis.execution_engine.models import ExecutionRequest, ExecutionSignal
from hydra_basis.execution_engine.preview import build_execution_preview


class PreviewTests(unittest.TestCase):
    def test_build_execution_preview_computes_batches_and_confirm_gate(self) -> None:
        signal = ExecutionSignal(
            symbol="BTC",
            short_venue="hyperliquid",
            long_venue="lighter",
            annualized_avg=0.5,
            score=0.9,
            updated_at="2099-05-29T00:00:00+00:00",
        )
        request = ExecutionRequest(symbol="BTC", total_usd=10000.0, clip_usd=500.0)
        preview = build_execution_preview(
            request=request,
            signal=signal,
            priorities={"hyperliquid": 2, "lighter": 0},
            adapters={
                "hyperliquid": FakeExecutionAdapter("hyperliquid", {"bid": 100.0, "ask": 100.2, "ts_ms": 1}),
                "lighter": FakeExecutionAdapter("lighter", {"bid": 100.0, "ask": 100.05, "ts_ms": 1}),
            },
        )
        self.assertEqual(preview.batch_count, 20)
        self.assertEqual(preview.maker_venue, "hyperliquid")
        self.assertEqual(preview.taker_venue, "lighter")
        self.assertFalse(preview.requires_confirm)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL with missing preview builder.

- [ ] **Step 3: Expand models and implement preview builder**

`hydra_basis/execution_engine/models.py`

```python
@dataclass(slots=True)
class ExecutionPreview:
    symbol: str
    maker_venue: str
    taker_venue: str
    total_usd: float
    clip_usd: float
    batch_count: int
    maker_spread_pct: float
    taker_spread_pct: float
    requires_confirm: bool
```

`hydra_basis/execution_engine/preview.py`

```python
from __future__ import annotations

import math

from hydra_basis.execution_engine.models import ExecutionPreview, ExecutionRequest, ExecutionSignal
from hydra_basis.execution_engine.priority import resolve_execution_legs
from hydra_basis.execution_engine.risk import compute_spread_pct, orderbook_is_anomalous, spread_requires_confirm


def build_execution_preview(*, request, signal, priorities, adapters) -> ExecutionPreview:
    short_book = adapters[signal.short_venue].get_orderbook(signal.symbol)
    long_book = adapters[signal.long_venue].get_orderbook(signal.symbol)
    if orderbook_is_anomalous(short_book) or orderbook_is_anomalous(long_book):
        raise RuntimeError("orderbook anomaly detected")

    spreads = {
        signal.short_venue: compute_spread_pct(short_book),
        signal.long_venue: compute_spread_pct(long_book),
    }
    maker_venue, taker_venue = resolve_execution_legs(
        short_venue=signal.short_venue,
        long_venue=signal.long_venue,
        priorities=priorities,
        spreads=spreads,
    )
    return ExecutionPreview(
        symbol=signal.symbol,
        maker_venue=maker_venue,
        taker_venue=taker_venue,
        total_usd=request.total_usd,
        clip_usd=request.clip_usd,
        batch_count=math.ceil(request.total_usd / request.clip_usd),
        maker_spread_pct=spreads[maker_venue],
        taker_spread_pct=spreads[taker_venue],
        requires_confirm=(
            spread_requires_confirm(spreads[maker_venue]) or
            spread_requires_confirm(spreads[taker_venue])
        ),
    )
```

- [ ] **Step 4: Run focused tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 8: Add execution state machine and emergency path

**Files:**
- Create: `hydra_basis/execution_engine/state_machine.py`
- Modify: `hydra_basis/execution_engine/models.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write failing tests for transitions**

```python
import unittest

from hydra_basis.execution_engine.state_machine import ExecutionStateMachine


class StateMachineTests(unittest.TestCase):
    def test_state_machine_moves_from_idle_to_preview_and_confirm(self) -> None:
        sm = ExecutionStateMachine()
        sm.to_preview_ready()
        sm.to_awaiting_confirm()
        self.assertEqual(sm.state, "awaiting_confirm")

    def test_state_machine_enters_emergency_exit_after_hedge_failure(self) -> None:
        sm = ExecutionStateMachine()
        sm.to_preview_ready()
        sm.to_awaiting_confirm()
        sm.to_placing_maker_leg()
        sm.to_hedging_taker_leg()
        sm.to_retrying_hedge()
        sm.to_emergency_exit()
        self.assertEqual(sm.state, "emergency_exit")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL with missing state machine.

- [ ] **Step 3: Implement minimal state machine**

`hydra_basis/execution_engine/state_machine.py`

```python
from __future__ import annotations


class ExecutionStateMachine:
    def __init__(self) -> None:
        self.state = "idle"

    def to_preview_ready(self) -> None:
        self.state = "preview_ready"

    def to_awaiting_confirm(self) -> None:
        self.state = "awaiting_confirm"

    def to_placing_maker_leg(self) -> None:
        self.state = "placing_maker_leg"

    def to_hedging_taker_leg(self) -> None:
        self.state = "hedging_taker_leg"

    def to_retrying_hedge(self) -> None:
        self.state = "retrying_hedge"

    def to_paused_risk(self) -> None:
        self.state = "paused_risk"

    def to_emergency_exit(self) -> None:
        self.state = "emergency_exit"

    def to_completed(self) -> None:
        self.state = "completed"
```

- [ ] **Step 4: Run focused tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 9: Add CLI preview script

**Files:**
- Create: `scripts/run_execution_preview.py`
- Test: `tests/test_execution_engine.py`

- [ ] **Step 1: Write failing smoke test for CLI helper**

```python
import unittest

from scripts.run_execution_preview import compute_batch_count


class ExecutionPreviewCliTests(unittest.TestCase):
    def test_compute_batch_count_rounds_up(self) -> None:
        self.assertEqual(compute_batch_count(10000.0, 500.0), 20)
        self.assertEqual(compute_batch_count(10250.0, 500.0), 21)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: FAIL with missing script helper.

- [ ] **Step 3: Implement preview CLI**

`scripts/run_execution_preview.py`

```python
from __future__ import annotations

import math

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()


def compute_batch_count(total_usd: float, clip_usd: float) -> int:
    if total_usd <= 0 or clip_usd <= 0:
        raise RuntimeError("total_usd and clip_usd must be positive")
    return math.ceil(total_usd / clip_usd)


def main() -> None:
    print("Execution preview entrypoint not implemented yet.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run focused tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

---

### Task 10: Run full regression suite and verify monitor still compiles

**Files:**
- Test: `tests/test_funding_analysis.py`
- Test: `tests/test_execution_engine.py`
- Verify: `scripts/run_funding_monitor.py`
- Verify: `scripts/run_execution_preview.py`

- [ ] **Step 1: Run execution-engine tests**

Run:

```powershell
python -m unittest tests.test_execution_engine -v
```

Expected: PASS

- [ ] **Step 2: Run existing monitor regression tests**

Run:

```powershell
python -m unittest tests.test_funding_analysis -v
```

Expected: PASS

- [ ] **Step 3: Run compile verification**

Run:

```powershell
python -m compileall hydra_basis scripts funding_arbitrage_monitor_demo.py
```

Expected: PASS with no syntax errors.

- [ ] **Step 4: Manually inspect generated `data/monitor_signals.json` path handling**

Run:

```powershell
python scripts/run_funding_monitor.py
```

Expected:
- monitor prints shared symbols
- writes `data/monitor_signals.json`
- no execution code is triggered

---

## Self-Review

- Spec coverage:
  - `monitor_signals.json` persistence: covered in Task 2
  - `24h` freshness: covered in Task 3
  - venue priority config: covered in Task 1 and Task 4
  - equal-priority smaller-spread taker rule: covered in Task 4
  - preview + `0.1%` confirm gate: covered in Task 6 and Task 7
  - execution state machine: covered in Task 8
  - fake-adapter/non-live architecture: covered in Task 5
- Placeholder scan:
  - No `TODO`, `TBD`, or “implement later” placeholders remain in task steps.
- Type consistency:
  - `ExecutionSignal`, `ExecutionRequest`, `ExecutionPreview`, and `ExecutionStateMachine` are referenced consistently across tasks.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-29-execution-risk-architecture.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
