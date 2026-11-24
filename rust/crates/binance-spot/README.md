# Binance Spot 原生私人交易元件

2026-11-23：現貨／永續移植第三小批的私人元件。交易方法已以本機 HTTP／WS 替身測試；第四批由 [spot-perp owner](../spot-perp/README.md) 接成 Aster／Hyperliquid Maker → Binance MARKET 的實盤測試入口，尚未真實送單驗收。本 crate 本身不會自行執行策略、補倉、重掛或反向平倉。

## 已實作

- `Client::submit(Permit)`：精確 base quantity 的 `MARKET` 或 post-only `LIMIT_MAKER`。Permit 只能由 `Journal::prepare_dispatch` 在 `sync_all` 成功後產生，消耗一次；同一 Client 的重複 client ID 也會拒絕。
- `Client::cancel`、`query`、`lookup`：以精確 symbol／訂單 ID 或 client ID 查證；驗證方向及原始數量。取消失敗不代表零成交。取消呼叫本身尚未有持久化取消排程，須由後續 owner 接線。
- 送單、撤單不自動重試；逾時、格式錯誤、5xx 均不能當成未送出。`-2013` 只代表本次查不到，不能解除已持久化的 dispatch fence。即使查證已成交，也不重用該 client ID。
- HTTP 長連線池、共用短鎖限流，網路請求期間不持有全域互斥鎖，沒有每次固定等待 100 ms。每分鐘本地預算 600 weight，讀取最多 500；418／429 遵守 `Retry-After` 冷卻，不暴力重試。這不是跨程序／跨 API key 的帳戶配額管理。
- 公開 server time 校時，簽名窗口 5 秒；校時超過 30 分鐘必須重新同步。簽名使用排序後的相同傳輸位元組，不經第二次編碼。
- 私人 WS 使用 `userDataStream.subscribe.signature`，必須收到成功 ACK 與 subscription ID，後續事件綁定該 ID；保留 socket 收到完整訊息的 monotonic 時間。處理 Ping、斷線、訂閱終止與格式錯誤。
- `coordination::observe_ws` 接入現有累計成交 actor；`reconcile` 在查單前取得 ticket，REST 舊回覆不能覆蓋新 WS 成交。socket owner 必須在斷線時失效 actor session，新連線 ACK 後建立新 session 並查單；此批沒有自動重連交易 owner。
- `Fees` 按 order ID／trade ID 去重，分別計算 gross quantity、quote amount、各資產佣金與淨 base delta。BUY 扣 base fee 時淨入帳較少；SELL 扣 base fee 時淨支出較多。覆蓋數量或 quote 不完整時不輸出假設零費用的淨量。
- 相同 trade ID 的內容衝突會使佣金帳本持續失效，不能忽略錯誤後繼續當作已對帳；數值無法精確表示時回報錯誤，不靜默捨棄微小佣金。
- `myTrades` 只查指定 order ID；達 1,000 筆即拒絕視為完整，本批未實作多頁查詢。佣金帳本暫為記憶體資料，重啟須重新取得精確 trades；單筆送單 journal 保留委託身份與 gross 執行證據，不是完整雙腿損益／資產帳本。

`Journal` 使用獨佔檔案鎖、雜湊串連及序號。截斷／錯誤身份／寫入失敗不授予送單許可。它的同步磁碟操作應由獨立 blocking worker 執行，不應放在 WS 讀取熱路徑；每份 journal 綁定一個 Intent。所有重啟必須重開原 journal，不可建立新目錄／新 ID 來規避未知送單。`View.order` 是持久化證據，不代表重啟後已完成新鮮對帳。

固定使用 Binance mainnet，只有編譯測試可指定 loopback endpoint。憑證不實作 Debug／Serialize，錯誤只保留狀態碼／錯誤碼，不輸出原始 API 回覆、簽名 URL 或密鑰。

## 唯讀連線驗收

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-spot-perp --release --locked
python scripts/run_spot_perp_arbitrage.py --engine rust --rust-binance-preflight --rust-watch-seconds 10
```

Python 入口沿用 `.env` 載入，Rust 從子程序環境讀取成對的 `BINANCE_SPOT_API_KEY`／`BINANCE_SPOT_API_SECRET`，或 `BINANCE_API_KEY`／`BINANCE_API_SECRET`。若 SPOT 前綴任一值已設定，兩項必須齊全，避免混用兩組密鑰。這一批只支援 HMAC，不支援 RSA／Ed25519 key。

這個命令只校時、驗證私人 WS 訂閱並讀取帳戶，輸出布林狀態及事件數，不列出資產／金額／API key。不會送單／撤單；沒有新成交亦能驗證訂閱，但不宣稱已完成真實成交或帳戶完整對帳。預設 10 秒，範圍 1–300 秒。其他 CLI 公開預覽不會啟動此私人連線。

帳戶 scope 目前用 API key 指紋區分，並非 Binance UID；同一帳戶多把 key 的全域交易鎖、完整雙腿 spot cash accounting、部分成交尾量、跨所退出／恢復、實際 hedge 排程仍需接線。原有 one-way perp journal 明確不接受 Binance Spot maker，避免把 gross 永續數量模型直接用於現貨扣費。

## 驗證

```powershell
cargo test --manifest-path rust/Cargo.toml -p perp-arb-binance-spot -p perp-arb-private-events -p perp-arb-execution -p perp-arb-spot-perp --locked
cargo clippy --manifest-path rust/Cargo.toml -p perp-arb-binance-spot -p perp-arb-spot-perp --all-targets --locked -- -D warnings
```

包含 HMAC 獨立 Python fixture、精確送出位元組、一次提交、未知結果重啟、檔案鎖／截斷、WS 簽名 ACK／Ping／錯誤訂閱、部分成交取消、取消與完全成交競態、WS／REST 亂序、重複 trade／佣金、429 冷卻及不完整成交清單。沒有以替身結果宣稱真實成交延遲或 spread 改善。

本批 16 個 Binance 元件測試、21 個 spot/perp 決策測試及現有私人事件／持久化執行器回歸通過；Python 入口與原流程 77 測試通過，公開決策 6,000 組比對零差異。Clippy `-D warnings` 通過。真實 10 秒唯讀驗收：帳戶讀取成功、canTrade=true、私人 WS 訂閱成功；收到成交事件 0 筆，送單／撤單 0 筆。這不是交易或完整庫存對帳驗收。結果：`rust/artifacts/binance-private-preflight-20261123.json`。

協定參考：[Binance REST 交易端點](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints)、[私人 WS 簽名訂閱](https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests)、[私人事件](https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream)。
