# Order UI — Phase 1: apy_service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a read-only `apy_service` that the order UI uses to list venues/symbols, resolve default long/short venues from `monitor_signals.json`, and compute historical APY + funding-spread series for any venue pair (perp-perp) or single perp venue (spot-perp).

**Architecture:** Pure functions over `FundingPoint` lists for the analytics (so they unit-test without files), plus thin file-facing helpers that load `data/funding_history.json` / `data/monitor_signals.json`. Reuses the existing `analyze_spread`, `analyze_positive_funding`, `align_on_coarser_interval`, `merge_points_by_interval_bucket`, and `trim_points_to_analysis_days`.

**Tech Stack:** Python 3.11, `unittest` (project standard — no pytest installed).

## Global Constraints

- Test runner is `unittest`; run with `python -m unittest tests.test_apy_service`.
- Traditional Chinese is only for UI copy (Phase 3); this phase is pure Python, comments in English matching the codebase.
- Do not modify trading/analysis logic; only compose existing functions.
- `FundingPoint` (`hydra_basis/funding_engine/models.py`): fields `venue, symbol, ts_ms, raw_rate, interval_hours`; property `hourly_rate = raw_rate / interval_hours`.
- Config: `FUNDING_HISTORY_PATH = Path("data/funding_history.json")`, `MONITOR_SIGNALS_PATH = Path("data/monitor_signals.json")`, `VENUE_CONFIG: dict[str, FundingConfig]` where each config has `.enabled`.
- `analyze_spread(short_points, long_points, *, analysis_days=7, min_observations=..., ...) -> dict|None` returns keys `samples, avg_hourly, median_hourly, std_hourly, positive_ratio, last_24h_positive_ratio, annualized_avg, score, signal`.
- `analyze_positive_funding(venue_points, *, min_observations=..., ...) -> dict|None` returns the same key set.
- `align_on_coarser_interval(short_points, long_points, max_lag_minutes=90) -> list[tuple[FundingPoint, FundingPoint]]`.

---

## File Structure

- Create: `hydra_basis/execution_engine/apy_service.py` — all Phase 1 functions.
- Create: `tests/test_apy_service.py` — unit tests.

`apy_service.py` public surface (final):
```python
list_venues() -> list[str]
list_symbols(*, history_path: Path = FUNDING_HISTORY_PATH) -> list[str]
get_default_venues(symbol: str, kind: str, *, signals_path: Path = MONITOR_SIGNALS_PATH) -> dict
build_spread_series(short_points, long_points) -> list[dict]
compute_pair_apy(short_points, long_points, *, days: int) -> dict
compute_spot_perp_apy(venue_points, *, days: int) -> dict
load_points(*, history_path: Path = FUNDING_HISTORY_PATH) -> dict[tuple[str, str], list[FundingPoint]]
```

---

## Task 1: build_spread_series (pure)

**Files:**
- Create: `hydra_basis/execution_engine/apy_service.py`
- Test: `tests/test_apy_service.py`

**Interfaces:**
- Consumes: `align_on_coarser_interval` from `hydra_basis.funding_engine.analysis`; `FundingPoint`.
- Produces: `build_spread_series(short_points: list[FundingPoint], long_points: list[FundingPoint]) -> list[dict]` where each dict is `{"ts_ms": int, "spread_hourly": float}`, sorted ascending by `ts_ms`.

- [ ] **Step 1: Write the failing test**

```python
import unittest
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.execution_engine.apy_service import build_spread_series


def _pt(venue, ts_ms, raw_rate, interval_hours=8.0, symbol="BTC"):
    return FundingPoint(venue=venue, symbol=symbol, ts_ms=ts_ms, raw_rate=raw_rate, interval_hours=interval_hours)


class BuildSpreadSeriesTests(unittest.TestCase):
    def test_series_is_short_minus_long_hourly_sorted_by_ts(self) -> None:
        short = [_pt("aster", 2_000, 0.0016), _pt("aster", 1_000, 0.0008)]
        long = [_pt("hyperliquid", 2_000, 0.0008), _pt("hyperliquid", 1_000, 0.0008)]
        series = build_spread_series(short, long)
        self.assertEqual([row["ts_ms"] for row in series], [1_000, 2_000])
        # hourly = raw/interval; spread = short.hourly - long.hourly
        # t=1000: 0.0008/8 - 0.0008/8 = 0.0 ; t=2000: 0.0016/8 - 0.0008/8 = 0.0001
        self.assertAlmostEqual(series[0]["spread_hourly"], 0.0)
        self.assertAlmostEqual(series[1]["spread_hourly"], 0.0001)

    def test_empty_when_no_overlap(self) -> None:
        self.assertEqual(build_spread_series([], []), [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_apy_service -v`
Expected: FAIL — `ModuleNotFoundError`/`ImportError: cannot import name 'build_spread_series'`.

- [ ] **Step 3: Write minimal implementation**

```python
from __future__ import annotations

from hydra_basis.funding_engine.analysis import align_on_coarser_interval
from hydra_basis.funding_engine.models import FundingPoint


def build_spread_series(
    short_points: list[FundingPoint],
    long_points: list[FundingPoint],
) -> list[dict]:
    pairs = align_on_coarser_interval(short_points, long_points)
    series = [
        {
            "ts_ms": max(short_point.ts_ms, long_point.ts_ms),
            "spread_hourly": short_point.hourly_rate - long_point.hourly_rate,
        }
        for short_point, long_point in pairs
    ]
    series.sort(key=lambda row: row["ts_ms"])
    return series
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_apy_service -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/apy_service.py tests/test_apy_service.py
git commit -m "feat(apy_service): add build_spread_series"
```

---

## Task 2: compute_pair_apy (perp-perp stats + series)

**Files:**
- Modify: `hydra_basis/execution_engine/apy_service.py`
- Test: `tests/test_apy_service.py`

**Interfaces:**
- Consumes: `analyze_spread`, `merge_points_by_interval_bucket`, `trim_points_to_analysis_days`; `build_spread_series` from Task 1.
- Produces: `compute_pair_apy(short_points, long_points, *, days: int) -> dict` returning `{"days": int, "stats": dict | None, "spread_series": list[dict]}`. `stats` is the `analyze_spread` dict, or `None` when there is insufficient data.

- [ ] **Step 1: Write the failing test**

```python
from hydra_basis.execution_engine.apy_service import compute_pair_apy


class ComputePairApyTests(unittest.TestCase):
    def test_returns_stats_and_series_for_positive_spread(self) -> None:
        base = 1_000_000_000_000
        hour = 3_600_000
        short = [_pt("aster", base + i * hour, 0.0016) for i in range(48)]
        long = [_pt("hyperliquid", base + i * hour, 0.0008) for i in range(48)]
        result = compute_pair_apy(short, long, days=7)
        self.assertEqual(result["days"], 7)
        self.assertIsNotNone(result["stats"])
        self.assertGreater(result["stats"]["annualized_avg"], 0)
        self.assertEqual(len(result["spread_series"]), 48)

    def test_stats_none_when_insufficient_data(self) -> None:
        result = compute_pair_apy([], [], days=7)
        self.assertIsNone(result["stats"])
        self.assertEqual(result["spread_series"], [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_apy_service.ComputePairApyTests -v`
Expected: FAIL — `ImportError: cannot import name 'compute_pair_apy'`.

- [ ] **Step 3: Write minimal implementation**

Add imports at top of `apy_service.py`:
```python
from hydra_basis.funding_engine.analysis import align_on_coarser_interval, analyze_spread, analyze_positive_funding
from hydra_basis.history_store import merge_points_by_interval_bucket, trim_points_to_analysis_days
```
Add function:
```python
def compute_pair_apy(
    short_points: list[FundingPoint],
    long_points: list[FundingPoint],
    *,
    days: int,
) -> dict:
    trimmed_short = trim_points_to_analysis_days(
        merge_points_by_interval_bucket(short_points), analysis_days=days
    )
    trimmed_long = trim_points_to_analysis_days(
        merge_points_by_interval_bucket(long_points), analysis_days=days
    )
    stats = analyze_spread(
        trimmed_short,
        trimmed_long,
        analysis_days=days,
        min_observations=1,
    )
    return {
        "days": days,
        "stats": stats,
        "spread_series": build_spread_series(trimmed_short, trimmed_long),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_apy_service -v`
Expected: PASS (all tests so far).

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/apy_service.py tests/test_apy_service.py
git commit -m "feat(apy_service): add compute_pair_apy"
```

---

## Task 3: compute_spot_perp_apy (single perp venue)

**Files:**
- Modify: `hydra_basis/execution_engine/apy_service.py`
- Test: `tests/test_apy_service.py`

**Interfaces:**
- Consumes: `analyze_positive_funding`, `merge_points_by_interval_bucket`, `trim_points_to_analysis_days`.
- Produces: `compute_spot_perp_apy(venue_points, *, days: int) -> dict` returning `{"days": int, "stats": dict | None, "rate_series": list[dict]}` where each `rate_series` row is `{"ts_ms": int, "hourly": float}` sorted ascending.

- [ ] **Step 1: Write the failing test**

```python
from hydra_basis.execution_engine.apy_service import compute_spot_perp_apy


class ComputeSpotPerpApyTests(unittest.TestCase):
    def test_returns_stats_and_rate_series(self) -> None:
        base = 1_000_000_000_000
        hour = 3_600_000
        points = [_pt("aster", base + i * hour, 0.0008) for i in range(48)]
        result = compute_spot_perp_apy(points, days=7)
        self.assertEqual(result["days"], 7)
        self.assertIsNotNone(result["stats"])
        self.assertEqual(len(result["rate_series"]), 48)
        self.assertAlmostEqual(result["rate_series"][0]["hourly"], 0.0001)

    def test_stats_none_when_empty(self) -> None:
        result = compute_spot_perp_apy([], days=7)
        self.assertIsNone(result["stats"])
        self.assertEqual(result["rate_series"], [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_apy_service.ComputeSpotPerpApyTests -v`
Expected: FAIL — `ImportError: cannot import name 'compute_spot_perp_apy'`.

- [ ] **Step 3: Write minimal implementation**

```python
def compute_spot_perp_apy(venue_points: list[FundingPoint], *, days: int) -> dict:
    trimmed = trim_points_to_analysis_days(
        merge_points_by_interval_bucket(venue_points), analysis_days=days
    )
    stats = analyze_positive_funding(trimmed, min_observations=1)
    rate_series = sorted(
        ({"ts_ms": point.ts_ms, "hourly": point.hourly_rate} for point in trimmed),
        key=lambda row: row["ts_ms"],
    )
    return {"days": days, "stats": stats, "rate_series": rate_series}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_apy_service -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/apy_service.py tests/test_apy_service.py
git commit -m "feat(apy_service): add compute_spot_perp_apy"
```

---

## Task 4: load_points + list_symbols + list_venues (file-facing)

**Files:**
- Modify: `hydra_basis/execution_engine/apy_service.py`
- Test: `tests/test_apy_service.py`

**Interfaces:**
- Consumes: `FundingHistoryStore` from `hydra_basis.history_store`, `VENUE_CONFIG`, `FUNDING_HISTORY_PATH` from `hydra_basis.config`.
- Produces:
  - `load_points(*, history_path=FUNDING_HISTORY_PATH) -> dict[tuple[str, str], list[FundingPoint]]`
  - `list_symbols(*, history_path=FUNDING_HISTORY_PATH) -> list[str]` — sorted unique uppercase symbols present in history.
  - `list_venues() -> list[str]` — enabled venues from `VENUE_CONFIG`, sorted.

- [ ] **Step 1: Write the failing test**

```python
import tempfile
from pathlib import Path
from hydra_basis.history_store import FundingHistoryStore
from hydra_basis.execution_engine.apy_service import load_points, list_symbols, list_venues


class FileHelpersTests(unittest.TestCase):
    def test_list_venues_returns_enabled_only(self) -> None:
        venues = list_venues()
        self.assertIn("aster", venues)
        self.assertNotIn("mexc", venues)  # mexc is enabled=False in VENUE_CONFIG
        self.assertEqual(venues, sorted(venues))

    def test_load_points_and_list_symbols_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "funding_history.json"
            store = FundingHistoryStore(path)
            store.save({
                ("aster", "BTC"): [_pt("aster", 1_000, 0.0008)],
                ("hyperliquid", "ETH"): [_pt("hyperliquid", 1_000, 0.0008, symbol="ETH")],
            })
            points = load_points(history_path=path)
            self.assertIn(("aster", "BTC"), points)
            self.assertEqual(list_symbols(history_path=path), ["BTC", "ETH"])
```

(`FundingHistoryStore.save(funding_points: dict[tuple[str, str], list[FundingPoint]])` is the exact inverse of `.load()` — verified in `hydra_basis/history_store.py:130`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_apy_service.FileHelpersTests -v`
Expected: FAIL — `ImportError: cannot import name 'load_points'`.

- [ ] **Step 3: Write minimal implementation**

Add imports:
```python
from hydra_basis.config import FUNDING_HISTORY_PATH, MONITOR_SIGNALS_PATH, VENUE_CONFIG
from hydra_basis.history_store import (
    FundingHistoryStore,
    merge_points_by_interval_bucket,
    trim_points_to_analysis_days,
)
from pathlib import Path
```
Add functions:
```python
def load_points(*, history_path: Path = FUNDING_HISTORY_PATH) -> dict[tuple[str, str], list[FundingPoint]]:
    return FundingHistoryStore(history_path).load()


def list_symbols(*, history_path: Path = FUNDING_HISTORY_PATH) -> list[str]:
    points = load_points(history_path=history_path)
    return sorted({symbol.upper() for _venue, symbol in points})


def list_venues() -> list[str]:
    return sorted(venue for venue, config in VENUE_CONFIG.items() if config.enabled)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_apy_service -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/apy_service.py tests/test_apy_service.py
git commit -m "feat(apy_service): add load_points, list_symbols, list_venues"
```

---

## Task 5: get_default_venues (from monitor_signals)

**Files:**
- Modify: `hydra_basis/execution_engine/apy_service.py`
- Test: `tests/test_apy_service.py`

**Interfaces:**
- Consumes: `load_best_signal_for_symbol` from `hydra_basis.execution_engine.signal_store` (returns `ExecutionSignal` with `.short_venue, .long_venue, .annualized_avg`), `MONITOR_SIGNALS_PATH`.
- Produces: `get_default_venues(symbol: str, kind: str, *, signals_path=MONITOR_SIGNALS_PATH) -> dict`.
  - `kind == "perp_perp"` → `{"short_venue": str, "long_venue": str, "annualized": float}`.
  - `kind == "spot_perp"` → `{"perp_venue": str, "annualized": float}` (perp_venue = the signal's `short_venue`; spot leg is always mexc, handled by the UI).
  - On missing/stale signal, returns `{"error": "<reason>"}` instead of raising, so the UI can degrade gracefully.

- [ ] **Step 1: Write the failing test**

```python
import json
import datetime as dt
from hydra_basis.execution_engine.apy_service import get_default_venues


def _write_signals(path, rows):
    payload = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "cross_exchange_signals": rows,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


class GetDefaultVenuesTests(unittest.TestCase):
    def test_perp_perp_returns_short_long_and_annualized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor_signals.json"
            _write_signals(path, [
                {"symbol": "BTC", "short_venue": "aster", "long_venue": "hyperliquid",
                 "annualized_avg": 0.42, "score": 1.8, "signal": True},
            ])
            result = get_default_venues("BTC", "perp_perp", signals_path=path)
            self.assertEqual(result["short_venue"], "aster")
            self.assertEqual(result["long_venue"], "hyperliquid")
            self.assertAlmostEqual(result["annualized"], 0.42)

    def test_spot_perp_uses_short_venue_as_perp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor_signals.json"
            _write_signals(path, [
                {"symbol": "BTC", "short_venue": "aster", "long_venue": "hyperliquid",
                 "annualized_avg": 0.42, "score": 1.8, "signal": True},
            ])
            result = get_default_venues("BTC", "spot_perp", signals_path=path)
            self.assertEqual(result["perp_venue"], "aster")

    def test_missing_symbol_returns_error_dict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor_signals.json"
            _write_signals(path, [])
            result = get_default_venues("BTC", "perp_perp", signals_path=path)
            self.assertIn("error", result)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_apy_service.GetDefaultVenuesTests -v`
Expected: FAIL — `ImportError: cannot import name 'get_default_venues'`.

- [ ] **Step 3: Write minimal implementation**

Add import:
```python
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol
```
Add function:
```python
def get_default_venues(symbol: str, kind: str, *, signals_path: Path = MONITOR_SIGNALS_PATH) -> dict:
    try:
        signal = load_best_signal_for_symbol(path=signals_path, symbol=symbol)
    except Exception as exc:  # missing file, stale, or symbol not found
        return {"error": str(exc)}
    if kind == "spot_perp":
        return {"perp_venue": signal.short_venue, "annualized": signal.annualized_avg}
    return {
        "short_venue": signal.short_venue,
        "long_venue": signal.long_venue,
        "annualized": signal.annualized_avg,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_apy_service -v`
Expected: PASS (all Phase 1 tests).

- [ ] **Step 5: Commit**

```bash
git add hydra_basis/execution_engine/apy_service.py tests/test_apy_service.py
git commit -m "feat(apy_service): add get_default_venues"
```

---

## Phase 1 Done — Definition of Done

- `python -m unittest tests.test_apy_service` is green.
- `apy_service.py` exposes: `list_venues, list_symbols, get_default_venues, build_spread_series, compute_pair_apy, compute_spot_perp_apy, load_points`.
- No changes to trading/analysis logic; existing suites remain green (`python -m unittest tests.test_funding_analysis`).

**Next phases (separate plans):**
- **Phase 2 — order_service:** extract non-interactive open/close execution (maker+taker) from `place_order.py` and `run_spot_perp_arbitrage.py` into `order_service.py`; add `interval_ms` / `debounce_ms` wrapper and `on_progress` callback; keep CLIs as thin wrappers. (taker-only = TODO.)
- **Phase 3 — Web UI:** FastAPI backend (`web/app.py`) consuming apy_service + order_service, plus `web/static` frontend (繁中) and `scripts/run_order_ui.py`.
