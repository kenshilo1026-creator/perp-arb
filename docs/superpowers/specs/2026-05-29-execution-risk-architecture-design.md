# Execution And Risk Architecture Design

## Goal

建立第一版 `execution + risk` 架構，供未來跨交易所 funding arbitrage 下單系統使用。  
這一版先不接真實自動下單，只建立：

- signal 讀取與新鮮度檢查
- execution venue priority 決策
- execution preview / confirm gate
- 分批建倉計算
- risk state machine
- execution adapter interface

## Scope

本設計只涵蓋：

- 跨交易所 funding arbitrage execution
- user 輸入 `ticker`、`總倉位 USD`、`每次開倉 USD`
- 從 monitor 輸出結果選出最佳 signal
- 根據交易所 priority 決定 maker / taker 腿
- 下單前即時 spread 檢查與 confirm gate
- 黑天鵝風控框架

本設計暫不涵蓋：

- 單交易所 spot-perp execution
- 真實 API/browser 下單實作
- 自動現貨腿 execution
- 持久化訂單資料庫
- PnL accounting

## Venue Model

Execution 第一版的 venue 分類：

- `hyperliquid`：API venue
- `mexc`：API venue
- `lighter`：API venue
- `aster`：API venue
- `variational`：browser-bridged venue

其中：

- 除 `variational` 外，其餘 venue 以 API execution 為目標
- `variational` 不視為正式交易 API venue，而是保留為 browser-bridged execution 類型

## Data Sources

### Monitor Output

`scripts/run_funding_monitor.py` 每次執行後，需輸出：

- `data/monitor_signals.json`

內容至少包含：

- `updated_at`
- `cross_exchange_signals`
- 每個 ticker 的最佳 signal
- `short_venue`
- `long_venue`
- `annualized_avg`
- `score`
- `signal`

Execution 不重跑全市場 funding 分析。  
Execution 只讀 `monitor_signals.json`。

### Signal Freshness

若 `monitor_signals.json` 的 `updated_at` 超過 `24 小時`，execution 必須直接拒絕開始。

## Venue Priority

建立：

- `configs/execution_venues.json`

內容：

```json
{
  "lighter": 0,
  "variational": 1,
  "hyperliquid": 2,
  "mexc": 2,
  "aster": 2
}
```

規則：

- `priority` 數字越小，越適合做 `taker / 市價腿`
- `priority` 數字越大，越適合做 `maker / 掛單腿`

對於最佳跨所 signal：

- `priority` 較大的一方 = `maker leg`
- `priority` 較小的一方 = `taker leg`

若兩邊 priority 相同，第一版可採固定 tie-break：

1. 優先保留 `short_venue` 為 maker
2. `long_venue` 為 taker

這個 tie-break 必須明確寫在程式與測試中。

## User Input

Execution preview 的使用者輸入：

- `ticker`
- `total_usd`
- `clip_usd`

例子：

- `ticker = BTC`
- `total_usd = 10000`
- `clip_usd = 500`

則：

- `batch_count = 20`

若 `total_usd` 不能被 `clip_usd` 整除，最後一筆吃剩餘量。

## Execution Preview Flow

建立入口：

- `scripts/run_execution_preview.py`

流程：

1. 讀 `monitor_signals.json`
2. 檢查 `updated_at`
3. 取該 `ticker` 最佳跨所 signal
4. 讀 `configs/execution_venues.json`
5. 決定 maker / taker venue
6. 即時拉兩邊 order book snapshot
7. 計算各自 spread %
8. 計算分批數量
9. 產生 preview
10. 若有 confirm gate，停止等待 user 確認

Preview 內容應包含：

- `ticker`
- `signal annualized`
- `short_venue`
- `long_venue`
- `maker_venue`
- `taker_venue`
- `total_usd`
- `clip_usd`
- `batch_count`
- 各 venue 即時 spread %

## Spread Confirm Gate

對選中的兩個 venue，各自計算即時 bid/ask spread 百分比。

若任一 venue：

- `spread > 0.1%`

則系統不得直接繼續，必須通知 user confirm。

此 gate 是 execution 前的人工確認條件，不是自動忽略 signal。

## State Machine

建立 execution state machine，狀態如下：

- `idle`
- `preview_ready`
- `awaiting_confirm`
- `placing_maker_leg`
- `hedging_taker_leg`
- `retrying_hedge`
- `paused_risk`
- `emergency_exit`
- `completed`

狀態說明：

- `idle`：尚未載入 signal
- `preview_ready`：signal、priority、spread、batch 計算完成
- `awaiting_confirm`：等待 user 確認開始
- `placing_maker_leg`：先掛 maker 腿
- `hedging_taker_leg`：maker 成交後立刻送市價 hedge
- `retrying_hedge`：市價腿失敗後進入重試
- `paused_risk`：偵測到高風險事件後暫停
- `emergency_exit`：嘗試對殘留部位做平倉處理
- `completed`：所有批次完成或策略正常結束

第一版先完成 state machine 與 transition，真實下單行為先以 adapter interface / fake adapter 驗證。

## Risk Rules

### 1. Stale Signal

若 `monitor_signals.json` 超過 `24 小時` 未更新：

- 直接拒絕 execution

### 2. Order Book Anomaly

若任一 venue 發生以下狀況：

- 缺 bid 或 ask
- `best_bid > best_ask`
- quote stale
- spread 異常擴大

則：

- 停止新單
- 進入 `paused_risk`

### 3. Maker Filled, Taker Failed

若 maker 腿已成交，但 taker 腿失敗：

- 立刻重試市價 hedge
- 使用明確的 `retry policy`
- 若重試後仍失敗，進入 `emergency_exit`

### 4. ADL / Position Mismatch / Unexpected Reduce

若任一 venue 偵測：

- ADL
- 倉位不匹配
- 非預期減倉

則：

- 另一邊必須立即平倉
- 狀態切到 `paused_risk`

### 5. Kill Switch

若偵測：

- venue health fail
- API / browser bridge 失聯
- 極端 spread
- 手動 stop

則：

- 停止所有新單
- 僅允許減倉 / 平倉

## Module Boundaries

新增目錄：

- `hydra_basis/execution_engine/`

建議模組：

### `signal_store.py`

責任：

- 讀 `monitor_signals.json`
- 驗證 `updated_at`
- 取得指定 ticker 的最佳跨所 signal

### `priority.py`

責任：

- 讀 `configs/execution_venues.json`
- 決定 maker / taker venue

### `preview.py`

責任：

- 接 user input
- 產生 execution preview
- 算 batch count
- 執行 spread confirm gate

### `state_machine.py`

責任：

- 定義 execution state
- transition 規則
- retry / emergency exit path

### `risk.py`

責任：

- order book anomaly 判斷
- stale signal 判斷
- ADL / position mismatch 判斷
- kill switch 條件

### `interfaces.py`

責任：

- 定義 execution adapter interface

介面至少包含：

- `place_limit_order`
- `place_market_order`
- `cancel_order`
- `get_open_orders`
- `get_position`
- `get_orderbook`
- `close_position`

## Testing Strategy

### Unit Tests

必須涵蓋：

- `monitor_signals.json` freshness 檢查
- priority 選 maker / taker
- batch count 計算
- `spread > 0.1%` confirm gate
- order book anomaly 判斷
- state machine transition

### Integration Tests

使用 fake adapter 模擬：

- maker 成交、taker 一次成功
- maker 成交、taker 第一次失敗第二次成功
- maker 成交、taker 多次失敗 -> `emergency_exit`
- ADL / position mismatch -> 對側平倉流程觸發

## Out Of Scope Follow-Ups

後續階段再做：

- 真實 API 下單 adapter
- `variational` browser-bridged execution adapter
- 現貨腿 execution
- 永續與現貨庫存管理
- 部位與保證金持久化
- 自動 resume / restart recovery
