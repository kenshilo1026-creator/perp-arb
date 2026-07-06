# Order UI — Phase 3: Web UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A local FastAPI web app (繁體中文) that consumes `apy_service` (Phase 1) and `order_service` (Phase 2): pick symbol → default long/short venue from monitor signals, recompute historical APY + spread chart on venue/day change, preview a plan, then confirm→live execute with live batch progress, including 批次間隔 (interval) and 防抖 (debounce) inputs.

**Architecture:** `web/app.py` exposes read-only GET endpoints + `POST /api/preview` + `WS /ws/execute`, all delegating to the two services. Static frontend (`web/static/`) is vanilla HTML/JS with uPlot for the chart. Bound to `127.0.0.1` only. `scripts/run_order_ui.py` launches uvicorn.

**Tech Stack:** FastAPI, uvicorn, Python 3.11, `unittest` + `fastapi.testclient.TestClient` (needs `httpx`). Frontend: vanilla JS + uPlot (CDN).

## Global Constraints

- New deps: `pip install fastapi uvicorn httpx` (httpx for TestClient).
- Test runner `unittest`: `python -m unittest tests.test_order_web`.
- Bind host fixed to `127.0.0.1`; non-GET endpoints reject non-local `Origin`.
- All user-facing copy is Traditional Chinese; code/identifiers English.
- Backend must not import the CLI scripts; it imports `apy_service` and `order_service` only.
- Reused (from earlier phases):
  - `apy_service.list_venues()`, `list_symbols()`, `get_default_venues(symbol, kind, *, signals_path)`, `load_points(*, history_path)`, `compute_pair_apy(short_points, long_points, *, days)`, `compute_spot_perp_apy(venue_points, *, days)`.
  - `order_service.OpenParams`, `CloseParams`, `build_open_preview(params)`, `execute_open(params, *, live, on_progress, deps)`, `build_spot_perp_preview`, `execute_spot_perp_open`, `build_close_preview`, `execute_close`, `progress_printer`.

---

## File Structure

- Create: `web/__init__.py` (empty), `web/app.py` (FastAPI app factory `create_app()` + routes).
- Create: `web/static/index.html`, `web/static/app.js`, `web/static/style.css`.
- Create: `scripts/run_order_ui.py` (uvicorn launcher, `127.0.0.1`).
- Create: `tests/test_order_web.py` (TestClient tests with mocked services).

`web/app.py` exposes `create_app() -> FastAPI` so tests build an app with injected/mocked service modules.

---

## Task 1: App factory + GET /api/venues, /api/symbols

**Files:**
- Create: `web/__init__.py`, `web/app.py`
- Test: `tests/test_order_web.py`

**Interfaces:**
- Produces: `create_app() -> FastAPI` serving `GET /api/venues -> {"venues": [...]}`, `GET /api/symbols -> {"symbols": [...]}`, static mount at `/` → `web/static`.

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock
from fastapi.testclient import TestClient


class VenuesSymbolsTests(unittest.TestCase):
    def test_venues_and_symbols(self):
        with mock.patch("hydra_basis.execution_engine.apy_service.list_venues", return_value=["aster", "hyperliquid"]), \
             mock.patch("hydra_basis.execution_engine.apy_service.list_symbols", return_value=["BTC", "ETH"]):
            from web.app import create_app
            client = TestClient(create_app())
            self.assertEqual(client.get("/api/venues").json(), {"venues": ["aster", "hyperliquid"]})
            self.assertEqual(client.get("/api/symbols").json(), {"symbols": ["BTC", "ETH"]})

    def test_rejects_non_localhost_host_header(self):
        from web.app import create_app
        client = TestClient(create_app())
        r = client.get("/api/venues", headers={"host": "evil.example.com"})
        self.assertEqual(r.status_code, 403)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_web -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'web.app'` (or fastapi import error → run `pip install fastapi uvicorn httpx` first).

- [ ] **Step 3: Write minimal implementation**

`web/__init__.py`: empty.
`web/app.py`:
```python
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from hydra_basis.execution_engine import apy_service

STATIC_DIR = Path(__file__).parent / "static"


ALLOWED_HOSTS = {"127.0.0.1", "localhost"}


def create_app() -> FastAPI:
    app = FastAPI(title="Order UI")

    @app.middleware("http")
    async def _localhost_only(request, call_next):
        # Defense-in-depth against DNS-rebinding: the app also binds 127.0.0.1,
        # but reject any request whose Host header is not localhost so a remote
        # page cannot drive order placement via a rebound hostname.
        host = (request.headers.get("host") or "").split(":")[0]
        if host and host not in ALLOWED_HOSTS:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=403, content={"error": "forbidden host"})
        return await call_next(request)

    @app.get("/api/venues")
    def venues() -> dict:
        return {"venues": apy_service.list_venues()}

    @app.get("/api/symbols")
    def symbols() -> dict:
        return {"symbols": apy_service.list_symbols()}

    if STATIC_DIR.exists():
        app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
    return app
```
Create an empty `web/static/` dir (add `.gitkeep`) so the mount succeeds.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_web -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/__init__.py web/app.py web/static/.gitkeep tests/test_order_web.py
git commit -m "feat(web): FastAPI app factory + venues/symbols endpoints"
```

---

## Task 2: GET /api/defaults + GET /api/apy

**Files:**
- Modify: `web/app.py`
- Test: `tests/test_order_web.py`

**Interfaces:**
- Produces:
  - `GET /api/defaults?symbol=&kind=` → `apy_service.get_default_venues(symbol, kind)` (returns its dict, incl. `{"error": ...}` passthrough with HTTP 200).
  - `GET /api/apy?symbol=&short_venue=&long_venue=&days=&kind=` → for `perp_perp`: load points, `compute_pair_apy`; for `spot_perp`: `compute_spot_perp_apy` on `short_venue`'s points. Returns `{"stats": ..., "series": [...], "days": n}`. Missing points → `{"stats": None, "series": [], "days": n}`.

- [ ] **Step 1: Write the failing test**

```python
class DefaultsApyTests(unittest.TestCase):
    def test_defaults_passthrough(self):
        with mock.patch("hydra_basis.execution_engine.apy_service.get_default_venues",
                        return_value={"short_venue": "aster", "long_venue": "hyperliquid", "annualized": 0.42}):
            from web.app import create_app
            client = TestClient(create_app())
            data = client.get("/api/defaults", params={"symbol": "BTC", "kind": "perp_perp"}).json()
            self.assertEqual(data["short_venue"], "aster")

    def test_apy_perp_perp(self):
        pts = {("aster", "BTC"): ["s"], ("hyperliquid", "BTC"): ["l"]}
        with mock.patch("hydra_basis.execution_engine.apy_service.load_points", return_value=pts), \
             mock.patch("hydra_basis.execution_engine.apy_service.compute_pair_apy",
                        return_value={"days": 7, "stats": {"annualized_avg": 0.4}, "spread_series": [{"ts_ms": 1, "spread_hourly": 0.001}]}):
            from web.app import create_app
            client = TestClient(create_app())
            data = client.get("/api/apy", params={"symbol": "BTC", "short_venue": "aster",
                                                  "long_venue": "hyperliquid", "days": 7, "kind": "perp_perp"}).json()
            self.assertEqual(data["days"], 7)
            self.assertEqual(len(data["series"]), 1)
```
(Delete the placeholder probe line if it errors; it is only illustrative — keep the real assertion block.)

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_web.DefaultsApyTests -v`
Expected: FAIL — 404 on `/api/defaults`.

- [ ] **Step 3: Write minimal implementation**

Add to `create_app()`:
```python
    @app.get("/api/defaults")
    def defaults(symbol: str, kind: str) -> dict:
        return apy_service.get_default_venues(symbol, kind)

    @app.get("/api/apy")
    def apy(symbol: str, days: int, kind: str, short_venue: str, long_venue: str = "") -> dict:
        points = apy_service.load_points()
        sym = symbol.upper()
        if kind == "spot_perp":
            venue_points = points.get((short_venue, sym), [])
            result = apy_service.compute_spot_perp_apy(venue_points, days=days)
            return {"stats": result["stats"], "series": result["rate_series"], "days": days}
        short_points = points.get((short_venue, sym), [])
        long_points = points.get((long_venue, sym), [])
        result = apy_service.compute_pair_apy(short_points, long_points, days=days)
        return {"stats": result["stats"], "series": result["spread_series"], "days": days}
```
Note: history keys use the stored symbol casing. If lookups miss because stored symbols are not uppercased, normalize once: build `{(v, s.upper()): pts for (v, s), pts in points.items()}` before lookup.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_web.DefaultsApyTests -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/app.py tests/test_order_web.py
git commit -m "feat(web): defaults + apy endpoints"
```

---

## Task 3: POST /api/preview

**Files:**
- Modify: `web/app.py`
- Test: `tests/test_order_web.py`

**Interfaces:**
- Produces: `POST /api/preview` accepting JSON `{kind, mode, exec_mode, symbol, short_venue, long_venue, leverage, total_size, clip_size, interval_ms, debounce_ms}`; builds `OpenParams`/`CloseParams`, calls `order_service.build_open_preview` (or `build_spot_perp_preview` / `build_close_preview`), returns the `Preview` as a dict. Validation errors → HTTP 400 with `{"error": msg}`.

- [ ] **Step 1: Write the failing test**

```python
from dataclasses import asdict


class PreviewTests(unittest.TestCase):
    def test_open_perp_perp_preview(self):
        from hydra_basis.execution_engine.order_service import Preview
        preview = Preview(symbol="BTC", kind="perp_perp", maker_venue="aster", taker_venue="hyperliquid",
                          short_venue="aster", long_venue="hyperliquid", total_usd=5000.0, clip_usd=1000.0,
                          batch_count=5, maker_spread_pct=0.0004, taker_spread_pct=0.0006,
                          requires_confirm=False, exec_mode="maker_taker")
        with mock.patch("hydra_basis.execution_engine.order_service.build_open_preview",
                        new=mock.AsyncMock(return_value=preview)):
            from web.app import create_app
            client = TestClient(create_app())
            body = {"kind": "perp_perp", "mode": "open", "exec_mode": "maker_taker", "symbol": "BTC",
                    "short_venue": "aster", "long_venue": "hyperliquid", "leverage": 3,
                    "total_size": "0.5", "clip_size": "0.1", "interval_ms": 500, "debounce_ms": 300}
            data = client.post("/api/preview", json=body).json()
            self.assertEqual(data["maker_venue"], "aster")
            self.assertEqual(data["batch_count"], 5)

    def test_validation_error_returns_400(self):
        from web.app import create_app
        client = TestClient(create_app())
        body = {"kind": "perp_perp", "mode": "open", "exec_mode": "maker_taker", "symbol": "BTC",
                "short_venue": "aster", "long_venue": "aster", "leverage": 3,
                "total_size": "0.5", "clip_size": "0.1", "interval_ms": 500, "debounce_ms": 300}
        r = client.post("/api/preview", json=body)
        self.assertEqual(r.status_code, 400)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_web.PreviewTests -v`
Expected: FAIL — 404 on `/api/preview`.

- [ ] **Step 3: Write minimal implementation**

Add to `web/app.py` (top imports + route):
```python
from dataclasses import asdict
from decimal import Decimal
from fastapi import Request
from fastapi.responses import JSONResponse
from hydra_basis.execution_engine import order_service


def _open_params_from_body(body: dict):
    return order_service.OpenParams(
        kind=body["kind"], symbol=body["symbol"], short_venue=body["short_venue"],
        long_venue=body["long_venue"], exec_mode=body["exec_mode"], leverage=int(body["leverage"]),
        total_size=Decimal(str(body["total_size"])), clip_size=Decimal(str(body["clip_size"])),
        interval_ms=int(body["interval_ms"]), debounce_ms=int(body["debounce_ms"]))
```
Route inside `create_app()`:
```python
    @app.post("/api/preview")
    async def preview(request: Request):
        body = await request.json()
        try:
            if body.get("mode") == "close":
                params = order_service.CloseParams(kind=body["kind"], symbol=body["symbol"],
                                                   interval_ms=int(body["interval_ms"]), debounce_ms=int(body["debounce_ms"]))
                result = await order_service.build_close_preview(params)
            elif body["kind"] == "spot_perp":
                result = await order_service.build_spot_perp_preview(_open_params_from_body(body))
            else:
                result = await order_service.build_open_preview(_open_params_from_body(body))
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        return asdict(result)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_web.PreviewTests -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/app.py tests/test_order_web.py
git commit -m "feat(web): preview endpoint"
```

---

## Task 4: WS /ws/execute (streaming progress)

**Files:**
- Modify: `web/app.py`
- Test: `tests/test_order_web.py`

**Interfaces:**
- Produces: `WS /ws/execute`. Client sends one JSON `{params:{...}, live: bool}`; server builds params, calls the matching `order_service.execute_*` with an `on_progress` that `await websocket.send_json(event)`; on completion sends `{"type":"result", ...}` and closes. On `ValueError`/exception sends `{"type":"error","error":msg}`.

- [ ] **Step 1: Write the failing test**

```python
class WsExecuteTests(unittest.TestCase):
    def test_streams_progress_then_result(self):
        async def fake_execute(params, *, live, on_progress, deps=None):
            await on_progress({"type": "batch_start", "index": 1, "total": 1, "clip_size": "0.1"})
            await on_progress({"type": "batch_filled", "index": 1, "maker_price": "100", "taker_price": "100.1", "qty": "0.1"})
            await on_progress({"type": "done", "executed_qty": "0.1", "batches": 1})
            return {"ok": True, "batches": 1, "executed_qty": "0.1"}
        with mock.patch("hydra_basis.execution_engine.order_service.execute_open", new=fake_execute):
            from web.app import create_app
            client = TestClient(create_app())
            with client.websocket_connect("/ws/execute") as ws:
                ws.send_json({"live": False, "params": {
                    "kind": "perp_perp", "mode": "open", "exec_mode": "maker_taker", "symbol": "BTC",
                    "short_venue": "aster", "long_venue": "hyperliquid", "leverage": 3,
                    "total_size": "0.1", "clip_size": "0.1", "interval_ms": 0, "debounce_ms": 0}})
                types = []
                while True:
                    msg = ws.receive_json()
                    types.append(msg["type"])
                    if msg["type"] in {"result", "error"}:
                        break
        self.assertIn("batch_filled", types)
        self.assertEqual(types[-1], "result")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_web.WsExecuteTests -v`
Expected: FAIL — websocket route missing (rejected/404).

- [ ] **Step 3: Write minimal implementation**

Add import `from fastapi import WebSocket` and route inside `create_app()`:
```python
    @app.websocket("/ws/execute")
    async def ws_execute(websocket: WebSocket):
        await websocket.accept()
        try:
            msg = await websocket.receive_json()
            body = msg["params"]
            live = bool(msg.get("live", False))

            async def on_progress(event: dict) -> None:
                await websocket.send_json(event)

            if body.get("mode") == "close":
                params = order_service.CloseParams(kind=body["kind"], symbol=body["symbol"],
                                                   interval_ms=int(body["interval_ms"]), debounce_ms=int(body["debounce_ms"]))
                result = await order_service.execute_close(params, live=live, on_progress=on_progress)
            elif body["kind"] == "spot_perp":
                result = await order_service.execute_spot_perp_open(_open_params_from_body(body), live=live, on_progress=on_progress)
            else:
                result = await order_service.execute_open(_open_params_from_body(body), live=live, on_progress=on_progress)
            await websocket.send_json({"type": "result", **result})
        except Exception as exc:  # noqa: BLE001 - surface to UI
            await websocket.send_json({"type": "error", "error": str(exc)})
        finally:
            await websocket.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_web.WsExecuteTests -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/app.py tests/test_order_web.py
git commit -m "feat(web): websocket execute with streamed progress"
```

---

## Task 5: Frontend — index.html + style.css

**Files:**
- Create: `web/static/index.html`, `web/static/style.css`

**Interfaces:**
- Produces: static page with the fields from the spec mockup (繁中), element IDs consumed by `app.js` (Task 6): `#kind-tabs`, `#mode`, `#exec-mode`, `#symbol`, `#short-venue`, `#long-venue`, `#leverage`, `#total-size`, `#clip-size`, `#interval-ms`, `#debounce-ms`, `#days`, `#apy-stats`, `#apy-chart`, `#btn-preview`, `#btn-submit`, `#preview-box`, `#progress-log`.

- [ ] **Step 0: Vendor uPlot locally (no CDN)**

Download uPlot 1.6.30 into `web/static/vendor/` so no third-party CDN is loaded at runtime (avoids supply-chain/SRI risk, works offline):

```bash
mkdir -p web/static/vendor
curl -Lo web/static/vendor/uPlot.iife.min.js https://cdn.jsdelivr.net/npm/uplot@1.6.30/dist/uPlot.iife.min.js
curl -Lo web/static/vendor/uPlot.min.css https://cdn.jsdelivr.net/npm/uplot@1.6.30/dist/uPlot.min.css
```

Commit these vendored files with the task. (If you prefer a CDN instead, you MUST add `integrity="sha384-..."` + `crossorigin="anonymous"` to the tags — vendoring is simpler and preferred here.)

- [ ] **Step 1: Create index.html**

```html
<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <title>下單面板</title>
  <link rel="stylesheet" href="/style.css" />
  <link rel="stylesheet" href="/vendor/uPlot.min.css" />
  <script src="/vendor/uPlot.iife.min.js"></script>
</head>
<body>
  <h1>下單面板</h1>
  <div id="kind-tabs">
    <button data-kind="perp_perp" class="active">perp-perp</button>
    <button data-kind="spot_perp">spot-perp</button>
  </div>
  <label>模式
    <select id="mode"><option value="open">開倉</option><option value="close">平倉</option></select>
  </label>
  <label>執行方式
    <select id="exec-mode">
      <option value="maker_taker">掛單+吃單</option>
      <option value="taker">純吃單 (尚未實作)</option>
    </select>
  </label>
  <div class="row">
    <label>幣種 <input id="symbol" list="symbol-list" /></label>
    <datalist id="symbol-list"></datalist>
    <label>做空交易所 <select id="short-venue"></select></label>
    <label id="long-venue-wrap">做多交易所 <select id="long-venue"></select></label>
  </div>
  <div class="row">
    <label>槓桿 <input id="leverage" type="number" value="3" /></label>
    <label>總數量 <input id="total-size" /></label>
    <label>每批數量 <input id="clip-size" /></label>
    <label>批次間隔(ms) <input id="interval-ms" type="number" value="500" /></label>
    <label>防抖(ms) <input id="debounce-ms" type="number" value="300" /></label>
  </div>
  <fieldset>
    <legend>歷史 APY</legend>
    <div id="days">
      <button data-days="1">1</button><button data-days="3">3</button>
      <button data-days="7" class="active">7</button><button data-days="14">14</button>
      <button data-days="30">30</button>
    </div>
    <div id="apy-stats">—</div>
    <div id="apy-chart"></div>
  </fieldset>
  <div class="row">
    <button id="btn-preview">預覽</button>
    <button id="btn-submit" disabled>確認送出</button>
  </div>
  <pre id="preview-box"></pre>
  <h3>執行進度</h3>
  <pre id="progress-log"></pre>
  <script src="/app.js"></script>
</body>
</html>
```

- [ ] **Step 2: Create style.css**

```css
body { font-family: system-ui, sans-serif; margin: 24px; max-width: 900px; }
.row { display: flex; gap: 12px; flex-wrap: wrap; align-items: end; margin: 8px 0; }
label { display: flex; flex-direction: column; font-size: 13px; gap: 4px; }
button.active { background: #2563eb; color: #fff; }
#apy-chart { width: 100%; height: 220px; }
#progress-log, #preview-box { background: #0b1020; color: #d6e2ff; padding: 12px; min-height: 60px; white-space: pre-wrap; }
```

- [ ] **Step 3: Manual check**

Run: `python scripts/run_order_ui.py` (implemented in Task 7 — if not yet, temporarily `uvicorn web.app:create_app --factory --host 127.0.0.1 --port 8765`), open `http://127.0.0.1:8765/`. Expected: the form renders (dropdowns empty until Task 6 wires data).

- [ ] **Step 4: Commit**

```bash
git add web/static/index.html web/static/style.css web/static/vendor/
git commit -m "feat(web): frontend markup + styles (vendored uPlot)"
```

---

## Task 6: Frontend — app.js (data flow, chart, preview, execute)

**Files:**
- Create: `web/static/app.js`

**Interfaces:**
- Consumes: the endpoints from Tasks 1–4 and the element IDs from Task 5.
- Produces: on load fills venues/symbols; symbol input → `/api/defaults` fills default venues then loads APY; venue/day change → `/api/apy` refresh + uPlot redraw; `預覽` → `/api/preview` fills `#preview-box` and enables submit (disabled when `exec_mode==="taker"`); `確認送出` → opens `/ws/execute`, appends each progress event to `#progress-log`.

- [ ] **Step 1: Create app.js**

```javascript
const $ = (id) => document.getElementById(id);
let state = { kind: "perp_perp", days: 7 };
let chart = null;

async function jget(url) { return (await fetch(url)).json(); }

async function init() {
  const [{venues}, {symbols}] = await Promise.all([jget("/api/venues"), jget("/api/symbols")]);
  for (const v of venues) {
    $("short-venue").add(new Option(v, v));
    $("long-venue").add(new Option(v, v));
  }
  $("symbol-list").innerHTML = symbols.map(s => `<option value="${s}">`).join("");

  document.querySelectorAll("#kind-tabs button").forEach(b =>
    b.onclick = () => { setActive("#kind-tabs", b); state.kind = b.dataset.kind; onKindChange(); });
  document.querySelectorAll("#days button").forEach(b =>
    b.onclick = () => { setActive("#days", b); state.days = +b.dataset.days; loadApy(); });
  $("symbol").onchange = loadDefaultsThenApy;
  $("short-venue").onchange = loadApy;
  $("long-venue").onchange = loadApy;
  $("exec-mode").onchange = () => { /* submit gating handled after preview */ };
  $("btn-preview").onclick = doPreview;
  $("btn-submit").onclick = doExecute;
  onKindChange();
}

function setActive(sel, btn) {
  document.querySelectorAll(sel + " button").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
}

function onKindChange() {
  $("long-venue-wrap").style.display = state.kind === "spot_perp" ? "none" : "";
  if ($("symbol").value) loadDefaultsThenApy();
}

async function loadDefaultsThenApy() {
  const sym = $("symbol").value.trim().toUpperCase();
  if (!sym) return;
  const d = await jget(`/api/defaults?symbol=${sym}&kind=${state.kind}`);
  if (!d.error) {
    if (state.kind === "spot_perp") { $("short-venue").value = d.perp_venue; }
    else { $("short-venue").value = d.short_venue; $("long-venue").value = d.long_venue; }
  }
  loadApy();
}

async function loadApy() {
  const sym = $("symbol").value.trim().toUpperCase();
  if (!sym) return;
  const p = new URLSearchParams({ symbol: sym, kind: state.kind, days: state.days,
    short_venue: $("short-venue").value, long_venue: $("long-venue").value });
  const r = await jget("/api/apy?" + p.toString());
  $("apy-stats").textContent = r.stats
    ? `年化 APY: ${(r.stats.annualized_avg*100).toFixed(2)}%  avg_hourly: ${(r.stats.avg_hourly*100).toFixed(4)}%  pos: ${(r.stats.positive_ratio*100).toFixed(0)}%  score: ${r.stats.score.toFixed(2)}  signal: ${r.stats.signal}`
    : "資料不足";
  drawChart(r.series || []);
}

function drawChart(series) {
  const xs = series.map(s => s.ts_ms/1000);
  const ys = series.map(s => (s.spread_hourly ?? s.hourly) * 100);
  const opts = { width: $("apy-chart").clientWidth || 800, height: 220,
    series: [{}, { label: "spread %/h", stroke: "#2563eb" }], scales: { x: { time: true } } };
  if (chart) { chart.destroy(); }
  chart = new uPlot(opts, [xs, ys], $("apy-chart"));
}

function bodyFromForm(mode) {
  return { kind: state.kind, mode, exec_mode: $("exec-mode").value,
    symbol: $("symbol").value.trim().toUpperCase(), short_venue: $("short-venue").value,
    long_venue: state.kind === "spot_perp" ? "mexc" : $("long-venue").value,
    leverage: +$("leverage").value, total_size: $("total-size").value, clip_size: $("clip-size").value,
    interval_ms: +$("interval-ms").value, debounce_ms: +$("debounce-ms").value };
}

async function doPreview() {
  const body = bodyFromForm($("mode").value);
  const r = await fetch("/api/preview", { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify(body) });
  const data = await r.json();
  $("preview-box").textContent = JSON.stringify(data, null, 2);
  $("btn-submit").disabled = !r.ok || body.exec_mode === "taker";
  if (body.exec_mode === "taker") $("preview-box").textContent += "\n\n純吃單尚未實作，無法送出。";
}

function doExecute() {
  const body = bodyFromForm($("mode").value);
  const ws = new WebSocket(`ws://${location.host}/ws/execute`);
  $("progress-log").textContent = "";
  ws.onopen = () => ws.send(JSON.stringify({ live: true, params: body }));
  ws.onmessage = (e) => {
    const ev = JSON.parse(e.data);
    $("progress-log").textContent += formatEvent(ev) + "\n";
    if (ev.type === "result" || ev.type === "error") ws.close();
  };
}

function formatEvent(ev) {
  switch (ev.type) {
    case "batch_start": return `batch ${ev.index}/${ev.total}  clip=${ev.clip_size}`;
    case "debounce_wait": return `  防抖等待 ${Math.round(ev.waited_ms)}ms`;
    case "batch_filled": return `  成交 限價方 ${ev.maker_price}  市價方 ${ev.taker_price}  qty ${ev.qty}`;
    case "batch_failed": return `  ✗ batch ${ev.index} 失敗: ${ev.error}`;
    case "done": return `完成: ${ev.batches} 批, 共 ${ev.executed_qty}`;
    case "result": return `結果: ${JSON.stringify(ev)}`;
    case "error": return `錯誤: ${ev.error}`;
    default: return JSON.stringify(ev);
  }
}

init();
```

- [ ] **Step 2: Manual verification**

Run the UI (Task 7 launcher), open `http://127.0.0.1:8765/`. With real `data/funding_history.json` + `data/monitor_signals.json` present: type a symbol → default venues fill, APY + chart show; change days → chart updates; click 預覽 → preview JSON shows. (Do not click 確認送出 unless you intend a real order.)

- [ ] **Step 3: Commit**

```bash
git add web/static/app.js
git commit -m "feat(web): frontend app.js data flow + chart + execute"
```

---

## Task 7: Launcher script

**Files:**
- Create: `scripts/run_order_ui.py`
- Test: `tests/test_order_web.py` (import smoke)

**Interfaces:**
- Produces: `python scripts/run_order_ui.py [--port 8765]` runs uvicorn on `127.0.0.1`.

- [ ] **Step 1: Write the failing test**

```python
class LauncherImportTests(unittest.TestCase):
    def test_launcher_builds_app(self):
        import scripts.run_order_ui as launcher
        app = launcher.build_app()
        self.assertTrue(hasattr(app, "router"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_order_web.LauncherImportTests -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.run_order_ui'`.

- [ ] **Step 3: Write minimal implementation**

`scripts/run_order_ui.py`:
```python
from __future__ import annotations

import argparse

from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.runtime import configure_windows_event_loop_policy
from web.app import create_app

load_environment()


def build_app():
    return create_app()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the order UI web app (localhost only).")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    import uvicorn
    configure_windows_event_loop_policy()
    uvicorn.run(build_app(), host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_order_web -v`
Expected: PASS (all web tests).

- [ ] **Step 5: End-to-end manual smoke**

Run: `python scripts/run_order_ui.py`; open `http://127.0.0.1:8765/`; verify full read-only flow (symbol → defaults → APY → chart → 預覽). Leave 確認送出 for a real, intended trade only.

- [ ] **Step 6: Commit**

```bash
git add scripts/run_order_ui.py tests/test_order_web.py
git commit -m "feat(web): uvicorn launcher for order UI"
```

---

## Phase 3 Done — Definition of Done

- `python -m unittest tests.test_order_web` green.
- `python scripts/run_order_ui.py` serves the UI at `http://127.0.0.1:8765/`.
- Full flow works against real data files: symbol → default long/short venue, venue/day change → APY + spread chart recompute, 預覽 shows plan, 確認送出 streams live batch progress with 間隔/防抖.
- 純吃單 (taker-only) option is visible but disabled/blocked (Phase 2 TODO).
- All earlier suites remain green (`tests.test_apy_service`, `tests.test_order_service`, `tests.test_execution_engine`, `tests.test_single_order`, `tests.test_risk_management`).
```
