# 下單 UI（perp-perp / spot-perp）設計文件

日期：2026-07-06
狀態：已通過設計討論，待寫實作計畫

## 目標

為現有的下單與 spot-perp 套利流程建立一個本機 Web UI（繁體中文），讓使用者能：

- 選擇 **perp-perp** 或 **spot-perp** 套利，支援 **開倉 / 平倉**。
- 選擇執行方式：**掛單+吃單（maker+taker）** 或 **純吃單（taker-only，先列 TODO）**。
- 輸入幣種後，自動帶入 `run_funding_monitor` 結果的預設做多 / 做空交易所，使用者可改選其他交易所。
- 使用者變更交易所或天數時，即時（本地重算）顯示該做多/做空組合的歷史 APY 與 funding spread 走勢圖，天數可選。
- 下單時可輸入**批次間隔（interval, ms）**與**防抖（debounce, ms）**。
- 先預覽（dry-run）計畫，再按「確認送出」呼叫真實執行引擎（preview → confirm → live）。

整個 UI 使用繁體中文。

## 非目標（YAGNI / 延後）

- **taker-only（兩腿都市價）執行邏輯**：UI 會提供選項，但後端執行先列為 TODO（選此模式時顯示「尚未實作」並禁用送出）。
- UI 不負責觸發 backfill / funding monitor 重跑；只讀取最新結果檔並在本地重算 APY。
- 不對外開放（僅綁 `127.0.0.1`）、不做多使用者 / 帳號權限。

## 關鍵決策

- **UI 型態**：本機 Web app（FastAPI 後端 + 瀏覽器前端）。
- **執行範圍**：preview（dry-run）→ 確認 → live，真實呼叫既有執行引擎。
- **重用策略（方案 A）**：把 `place_order.py` / `run_spot_perp_arbitrage.py` 內的非互動核心抽成共用 service layer；**CLI 保留**，改為薄的互動包裝呼叫同一份 service。UI 後端與 CLI 共用同一顆執行核心，行為保證一致。
- **間隔 / 防抖語意**：間隔 = 每批成交後、下一批前的固定等待（ms）；防抖 = 送單前 orderbook 需連續穩定 N ms（mid 變動 < 門檻）才送出，逾時則放棄該批並回報。
- **APY 呈現**：數字統計（annualized / avg_hourly / positive_ratio / score / signal）+ funding spread 走勢圖。

## 架構總覽

```
前端 (browser, 繁中)  index.html + app.js + uPlot
   │  HTTP + WebSocket (127.0.0.1)
FastAPI (web/app.py)  唯讀查詢 API + preview + execute(WS)
   │  import & 呼叫
共用核心 (hydra_basis)
   order_service.py (新)  ← 由 CLI 抽出的非互動執行邏輯
   apy_service.py    (新)  ← 讀 history/signals + 算 APY
   既有 execution_engine / analyze_spread / adapters
   ▲
   │  同一份 service
既有 CLI (保留)  place_order.py / run_spot_perp_arbitrage.py（薄互動包裝）
```

### 新增檔案

- `hydra_basis/execution_engine/order_service.py` — perp-perp / spot-perp 的 open/close 非互動函式。
- `hydra_basis/execution_engine/apy_service.py` — 讀 `funding_history.json` / `monitor_signals.json`，用 `analyze_spread` / `analyze_positive_funding` 算 APY 與預設 venue。
- `web/app.py` — FastAPI 後端。
- `web/static/index.html`、`web/static/app.js`、`web/static/style.css` — 前端。
- `scripts/run_order_ui.py` — 以 uvicorn 啟動 UI，綁 `127.0.0.1`。

### 改動檔案

- `scripts/place_order.py`、`scripts/run_spot_perp_arbitrage.py`：內部改呼叫 `order_service`；互動介面與 CLI 參數不變。

## Service Layer 介面

所有 execute 函式接受 `on_progress` async callback：CLI 傳「印出來」，Web 傳「推 WebSocket」。

### apy_service.py（唯讀查詢，無網路下單）

```python
list_venues() -> list[str]                      # 從 VENUE_CONFIG enabled
list_symbols() -> list[str]                     # history / signals 有的幣種
get_default_venues(symbol, kind) -> dict
   # perp_perp -> {short_venue, long_venue, annualized}
   # spot_perp -> {perp_venue, annualized}
compute_pair_apy(symbol, short_venue, long_venue, days) -> dict
   # 用 analyze_spread；回 annualized / avg_hourly / positive_ratio / score / signal
   #   + spread_series[{ts, spread_pct}] 給走勢圖
compute_spot_perp_apy(symbol, perp_venue, days) -> dict   # 用 analyze_positive_funding
```

### order_service.py（preview + 執行）

```python
@dataclass
class OpenParams:
    kind: str            # "perp_perp" | "spot_perp"
    symbol: str
    short_venue: str
    long_venue: str      # spot_perp 時代表現貨腿
    exec_mode: str       # "maker_taker" | "taker"（taker 先 TODO）
    leverage: int
    total_size: Decimal
    clip_size: Decimal
    interval_ms: int
    debounce_ms: int

build_open_preview(params) -> Preview
   # 不下單。回 maker_venue / taker_venue、各腿 spread、預估 usd、批次數、requires_confirm
execute_open(params, *, live, on_progress) -> Result
build_close_preview(...) -> Preview
execute_close(..., *, live, on_progress) -> Result
```

- `exec_mode="maker_taker"`：沿用現有（一腿限價、一腿市價）。
- `exec_mode="taker"`：**TODO**——回「尚未實作」，不執行。
- `execute_*` 內部是把現有 `run_open_execution_once` / spot-perp 的批次迴圈搬過來，去掉 `input()`，改用 `on_progress` 回報、`params` 帶入 interval / debounce。
- `live=False` 走現有 dry-run 路徑（只算不送）。

### progress 事件

```
{type:"batch_start", index, total, clip_size}
{type:"debounce_wait", waited_ms}          # 等 orderbook 穩定
{type:"batch_filled", index, maker_price, taker_price, qty}
{type:"batch_failed", index, error}
{type:"done", executed_qty, batches}
```

## Web API

```
GET  /api/venues                          → 交易所清單
GET  /api/symbols                         → 幣種清單
GET  /api/defaults?symbol=&kind=          → 預設 venue + annualized
GET  /api/apy?symbol=&short_venue=&long_venue=&days=&kind=
                                          → 統計數字 + spread_series（走勢圖）
POST /api/preview   (body=OpenParams/CloseParams)
                                          → Preview（限價方/市價方/spread/預估USD/批次數）
WS   /ws/execute                          → 送 {params, live} → 串流 progress → done
```

- `/ws/execute` 開始時 server 會**先重算 preview 並比對送來的參數**，避免「看到的 preview」與「實際執行」不一致。
- 綁 `127.0.0.1`；非 GET 端點只接受本機來源。

## 前端畫面（繁中，單頁）

```
下單面板   [ perp-perp ] [ spot-perp ]      模式 ◉開倉 ○平倉
幣種 [BTC ▼]        執行方式 ◉掛單+吃單 ○純吃單(TODO)
做空交易所 [aster ▼]      做多交易所 [hyperliquid ▼]  ←預設
槓桿 [3]   總數量 [0.5]   每批數量 [0.1]
批次間隔 [500] ms         防抖 [300] ms
歷史 APY  天數 [1][3][●7][14][30]
  年化 APY: 42.3%   avg_hourly: 0.004%   pos: 71%
  score: 1.85   signal: strong
  [ spread 走勢圖 (uPlot) ]
  [ 預覽 ]        [ 確認送出 ]
執行進度：
  batch 1/5  防抖等待 280ms
  batch 1/5 成交  限價方 63012.5  市價方 63020.0  qty 0.1
```

行為：

- 輸入幣種 → 自動帶入預設做多 / 做空 venue + APY。
- 改 venue 或天數 → 即時（本地）重算 APY + 走勢圖，不下單。
- 「預覽」→ 顯示 plan；「確認送出」→ WebSocket 執行、下方即時滾動進度。
- spot-perp 分頁欄位微調（perp 做空 venue + 現貨腿 + 槓桿），APY 用單邊 positive funding。
- **spot-perp 的現貨腿目前只支援 mexc**：UI 現貨交易所固定為 mexc（顯示但不可改），`OpenParams.long_venue` 在 spot_perp 模式固定為 `mexc`。

## 執行流程（間隔 / 防抖）

```
preview = build_open_preview(params)
for index in 1..num_batches:
    on_progress(batch_start, index, total)

    # 防抖：等 orderbook 穩定
    wait_for_stable_orderbook(venues, debounce_ms):
        持續抓兩腿 orderbook，記錄 mid
        直到「連續 debounce_ms 內 mid 變動 < 門檻」才回傳
        （超過 debounce timeout 上限則放棄該批並回報，不硬送）
    on_progress(debounce_wait, waited_ms)

    result = 執行這批 (maker 限價→確認成交→taker 市價)   # 沿用現有 execute
    if not result.ok:
        on_progress(batch_failed) ; 停止整個執行（同現行 CLI 行為）
    on_progress(batch_filled, maker_price, taker_price, qty)
    記錄倉位到 position_registry（成功才記）

    if index < num_batches:
        await sleep(interval_ms)     # 批次間隔
on_progress(done)
```

- 防抖用注入的 `clock` / `sleep`（可測試）；穩定門檻預設「mid 變動 < 0.05%」，可調。
- 間隔是每批成交後、下一批前的固定等待。
- 防抖逾時上限 → 該批放棄並回報（避免無限等待硬送）。
- 這些只是包在**現有**每批執行邏輯外層，不改實際下單 / 對沖流程。

## 錯誤處理與安全

- 綁 `127.0.0.1`；execute 前 server 端重算 preview 比對參數。
- 前端驗證：數量 > 0、每批 ≤ 總量、兩 venue 不同、幣種存在、ms ≥ 0。
- 單批失敗即停止並回報（與現行 CLI 一致）。
- Variational 需 browser/extension，與 CLI 同限制；不可用時回清楚錯誤訊息。
- WebSocket 中斷 → 以批為單位，不會把單批下單腰斬到一半；回報最後狀態。

## 測試

- `order_service`：preview 計算、maker/taker 腿判定、間隔 / 防抖（注入 clock/sleep 驗證等待邏輯）、單批失敗即停、taker-only 回「尚未實作」。
- `apy_service`：`compute_pair_apy` / spot-perp APY、天數換算、缺資料處理。
- Web API：FastAPI `TestClient`，mock service，驗證 endpoints 與參數驗證。
- 回歸：確保重構後既有 `test_execution_engine` / `test_single_order` 等 CLI 測試仍綠。

## 相依

- 新增 Python 套件：`fastapi`、`uvicorn`（後端）。前端圖表用 uPlot（靜態檔，不需 build）。
