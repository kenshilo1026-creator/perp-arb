# Binance Spot／永續原生現貨對沖

2026-11-23 第四批已提供 **Rust 雙腿實盤測試入口**。目前固定 **Aster 或 Hyperliquid Post-only Maker → Binance Spot MARKET Taker**。開倉：永續賣／現貨買；平倉：永續 reduce-only 買／現貨賣。Python 只載入環境、收取參數及啟動 binary，兩腿交易、WS 和成交協調都在 Rust。尚未做真實送單驗收，也未證明實際 spread 或交易延遲優於 Python。

最新掛單保留規則：兩邊行情必須新鮮才可開新單或追價。已存在的限價單，若只是 Maker 公開報價超過 1 秒，但對沖報價仍新鮮、原單價差合格，會顯示 `[maker-hold]` 並保留原單，不重新取數量、不失去排隊位置。價差用原單價格計算，不用過期的 Maker bid/ask；對沖行情過期、原單價差達到門檻、公開行情確認斷線／缺失、私人通道失效或停止要求，仍會撤回。成交後對沖不受 Maker 公開報價年齡限制。此規則是刻意修正舊版 Python／Rust「任一邊過期一律撤單」行為；舊版零差異報告不能代表這項新規則。

## 開始測試

在 project 根目錄執行：

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-spot-perp --release --locked
python scripts/run_spot_perp_arbitrage.py --engine rust --live
```

依次輸入 `open/close`、幣種、`aster/hyperliquid` 及 `total_size_token`。不用輸入 clip；每次新價格按 Binance 對沖方向最佳一檔數量分批，也可用 `--clip-size` 限制每單上限。最後仍須輸入 `PLACE LIVE SPOT PERP ORDER`。數量不足交易所最低限額會拒絕，不會擅自加大總量。

使用現有 `.env` 的 Binance HMAC key/secret，以及 `ASTER_USER_ADDRESS`／`ASTER_API_WALLET_PRIVATE_KEY`，或 `HYPERLIQUID_ACCOUNT_ADDRESS`／`HYPERLIQUID_PRIVATE_KEY`。沿用交易所現有槓桿，這個入口拒絕 `--leverage`，不會默默修改。`--live` 不接受 `--rust-watch-seconds`。

`--rust-run-dir` 可指定全新目錄，省略會產生 `rust/runs/spot-<id>`。保留整個目錄；`run.jsonl`、各 hedge journal 是恢復依據，`summary.json` 和 `timing.json` 為最後輸出的摘要。未解決的 run 會留下 `rust/runs/spot-perp-active.json` 阻止另起新單；不要刪檔繞過。

2026-11-23 撤單排查修正：執行中會顯示 `[maker-plan]` 預估金額／數量／價格、`[maker-submit]`、`[maker-ack]`、`[cancel]` 原因／當時雙邊報價／行情年齡、`[cancel-result]`、`[reprice]`、`[hedge]` 及確認。相同等待提示最多每 5 秒一次，詳細 timing 仍只在最後輸出。每個 run 新增 `process.jsonl`（毫秒時間戳及結構化行情）；獨立有界佇列／寫入執行緒，不把終端和診斷寫檔放進對沖傳輸。這是診斷紀錄，不取代 `run.jsonl`，若佇列滿會在最後報告丟棄數量。

撤單原因：`best_price_changed` 是可接受的新最佳價格，`adverse_gap` 是原單價差超標，`stale_quote` 是對沖行情超過 1 秒或報價缺失／斷線，`user_stop` 是停止要求，`maker_lookup_unconfirmed` 是送單確認後查單無法確認，`*_private_unhealthy` 是私人通道失效。送單期間短暫追價會在實際撤單前重新判斷；送單確認前啟動、確認後才返回的查無訂單／失敗回覆，不再錯誤取消已確認掛單。撤單回覆成功後仍會查最終成交量，處理途中成交才掛下一單。

Binance 公開 WS 超過 500 ms 沒有新鮮報價時，現在會獨立補查 `/api/v3/depth?limit=5`，每程序最多每秒 2 次、每條連線最多一個請求，750 ms 逾時；429／418 至少退避 60 秒並尊重更長的 `Retry-After`，重連不重置限流。按 [Binance 官方文件](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#order-book)，每次權重為 5。只在 WS 連線期間補查；完全斷線仍使報價失效。REST 年齡從請求開始計算，較舊序號或較舊的同序號回覆不覆蓋新 WS，相同序號內容衝突仍拒絕；沒有放寬 1 秒行情期限或 0.25% 價差門檻。

本次驗收：55 個 Rust 測試、80 個 Python 回歸測試通過，6,000 筆決策比對零差異。舊版 SAGA 公開行情觀察重現 Binance 接收年齡超過 1 秒；補回快照後最後一輪 30 秒取得 37 次有效 REST 快照，沒有 Binance 過期切換，仍有一次 Hyperliquid 過期，保護照常生效。不同時段行情不能當作實盤撤單率或成交速度保證。助理未送單／撤單；本次報告為 `rust/artifacts/spot-perp-churn-validation-20261123.json`。使用者舊 run 的 19 張同價零成交委託沒有撤單原因／時間戳，無法事後逐張證明觸發條件。

```powershell
python scripts/run_spot_perp_arbitrage.py --engine rust --rust-recover --rust-run-dir rust/runs/原本的目錄 --live
```

Recovery 只查證／撤回原 Maker、對沖已成交部分，不另開 Maker；不需要重新輸入幣種、數量或方向。Ctrl+C 會要求停止、等待撤單及對沖收尾；Python 不強殺仍有委託的 Rust 子程序。也可在 run-dir 建立 `STOP` 檔案。異常退出務必查看結果是否為 `recovery_required`。

## 已接通的交易規則

- Maker 買掛最新 bid、賣掛最新 ask，不加 tick。只有價格變動才重新取深度；深度數量變動不取消排隊。原單合格但追價超標則保留；Maker 行情暫舊時保留合格原單並停止追價，原單超標、對沖行情過期或通道失效則撤回。
- `.env MAKER_TAKER_MAX_GAP_PCT` 預設 0.25%，採嚴格小於；不合格時不掛新單，持續 60 秒無合格報價／深度就退出。門檻只限制掛單，成交後即使價差變差仍進行現貨市價對沖。
- 私人累計成交 watch 驅動對沖，重複事件不重複下單；訂單 ID 綁定前的事件保存在 coordinator。部分成交達 Binance 最低可交易量便送對沖，不等待 Maker 全部成交。首次精確身份綁定仍可能需要 ACK 或 client-ID 查詢，不能保證每筆在 ACK 前觸發。
- 公共 WS、私人 WS、Maker 查詢／撤單和 Binance 對沖獨立排程；熱路徑不重新抓 REST 報價。送單前仍須寫入並同步持久化紀錄，沒有用省略記帳換取速度。
- Binance POST 回覆不明或超過一秒未確認時，通知 owner 撤回 Maker；繼續查原 client ID，不因 NotFound 重送。撤單後查最終累計量並處理途中成交；只有完成對沖才可換下一張 Maker。
- 首次檢查帳戶可交易、現貨可用餘額、同商品現有永續委託及平倉所需 short。只處理本次 total，沒有比較或自動修補原先兩所總持倉差額。
- 每單確認後用精確 order ID 查 trades、核對 gross／quote／佣金，保存淨 base delta。最終 `target_completed` 表示有否完成指定總量；`net_base_delta_including_spot_fees` 顯示費用造成的淨差額，不會將 gross 匹配冒充完全零曝險。

## 範圍與限制

Binance 當 Maker 的反向路徑、Lighter／Variational 的現貨雙腿交易尚未開放；公開 preview 仍會比較兩邊 Maker，與這個固定角色 live 路徑有所不同。Hyperliquid 為 USDC、Binance 為 USDT，目前未換算穩定幣匯率。

最低 lot／notional 以下的部分成交可能留下無法單獨市價成交的零頭；會保留 run 並報錯，不增大使用者總量或假報對沖成功。交易所拒單、斷線、未知送單仍可能有暫時或需人工處理的曝險，不保證所有情況自動歸零。佣金以 base 支付的差額會明確列出，不自動補交易。`myTrades` 滿 1,000 筆需人工處理，尚無多頁取齊；沒有完整全帳戶資產／PnL registry。上限一小時、1,000 張 Maker；單張最長 15 分鐘後先撤回，再以新價格重評。

時間記錄包含「本機收到成交→呼叫對沖」、「呼叫→ACK」、「呼叫→WS／REST 成交確認」，只在收尾列印並保存。`source` 區分私人 WS、REST 和回覆；`resumed=true` 是恢復查證，不是原送單延遲。這些不是兩所交易引擎實際成交時間差，不能據此保證 <100 ms 或 0.25% 最終成交價差。

## 前三批紀錄（以下為當時完成範圍，現況以上文為準）

第四批驗收：49 個 Rust 測試、80 個 Python 回歸測試、Clippy `-D warnings`、fmt 及 release 編譯通過；6,000 筆離線決策回放零差異。最後一次 10 秒 Binance／Hyperliquid 公開 WS 測試取得新鮮雙邊報價。報告位於 `rust/artifacts/spot-perp-live-validation-20261123.json`，包含 binary／source hash；實際送單與撤單均為零，尚未取得真實成交延遲樣本。

此 crate 是 `scripts/run_spot_perp_arbitrage.py` 的分批移植。已完成公開行情、精度／分單及報價決策；**CLI 沒有真實下單能力**。公開 preview 不讀取憑證；第三小批新增明確選用的 `binance-preflight` 私人帳戶／WS 唯讀驗收，底層交易方法仍未接到雙腿 CLI。Python 原流程保持預設；選用 Rust 不會自動回退 Python。

2026-11-23 第三小批：新增 [Binance Spot 原生私人交易元件](../binance-spot/README.md)，包含 HMAC、Maker／market 單次送單、撤單／精確查單、私人 WS、佣金與送單持久化。送單／撤單只完成替身驗收，雙腿 owner、現貨庫存／手續費持久化與恢復尚未完成，`--live` 仍拒絕。

```powershell
python scripts/run_spot_perp_arbitrage.py --engine rust --rust-binance-preflight --rust-watch-seconds 10
```

2026-11-23 第二小批加入 Hyperliquid 與 Lighter 公開行情及選腿計劃。現貨一律 Binance Spot，`--short-venue` 可選 `aster`、`hyperliquid`、`lighter`；每次監控一組配對，不是同時跨多所送單。

| 永續交易所 | 原生公開行情／計劃 | 此 spot-perp 入口實盤 |
|---|---|---|
| Aster | BBO＋完整部分深度 | 未接入 |
| Hyperliquid | BBO＋l2Book，沿用 fast 訂閱 | 未接入 |
| Lighter | ticker 完整最佳一檔及數量 | 未接入 |
| Variational | 此入口未接 extension，明確拒絕 | 未接入；現有獨立 paired-order 不等於 Binance 現貨對沖 |

## 使用

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-spot-perp --release --locked
python scripts/run_spot_perp_arbitrage.py --engine rust --mode close --symbol ETH --short-venue aster --total-size 0.01 --rust-watch-seconds 30
python scripts/run_spot_perp_arbitrage.py --engine rust --mode close --symbol ETH --short-venue hyperliquid --total-size 0.02 --rust-watch-seconds 30
python scripts/run_spot_perp_arbitrage.py --engine rust --mode close --symbol ETH --short-venue lighter --total-size 0.02 --rust-watch-seconds 30
```

上例只是公開行情預覽，`close` 表示分析賣現貨／買永續的方向，不查持倉也不平倉。省略 watch 時等待首個合格計劃，最多等待 60 秒；超標／行情缺失時不製造虛假的可成交價格。指定 watch 可持續觀察 1–3600 秒。所有行情均無法使用或價差一直不合格時，60 秒退出。這個等待條件尚未接到真實 Maker 委託。

`--clip-size` 可限制每筆計劃上限，省略則按對沖最佳一檔可用數量選擇。`.env` 的 `MAKER_TAKER_MAX_GAP_PCT` 由 Python wrapper 換算為比例傳入，預設 0.25%；直接呼叫 binary 則使用 `--max-gap 0.0025`、`--perp-venue hyperliquid`（預設 Aster）。`--live` 明確拒絕；Variational 不能用公開 indicative quote 假裝成可成交深度或猜測委託精度。

## 已移植規則

- 開倉買 Binance 現貨、賣所選永續；平倉方向相反。同時評估兩邊 Maker，優先選較小不利價差，同價差選較小 Taker spread。這個排序亦套用 Hyperliquid／Lighter；Python 舊入口這兩所主要按 spread 選腿，因此不是宣稱舊入口所有控制流程逐字等價。
- Maker 買掛 bid、賣掛 ask；價格不加一格。最佳一檔數量上限、共同 LOT_SIZE／MARKET_LOT_SIZE 步長、最低／最高名義值及保留可交易尾單。
- 嚴格 `< max_gap`，等於門檻不掛；閾值使用精確乘法比較，不靠格式化百分比。
- resting 決策：深度數量跳動不改單；原單合格但追價超標則保留；原單超標則撤回；價格變動且新價格合格才先撤後追。這些是決策輸出，目前不會真的撤單或重掛。
- BBO 和完整 top-20 snapshot 不拼接。序號較舊忽略，同序號數值衝突失效；斷線清除資料再重連。過期 1 秒的報價不能產生新計劃。
- 公開 REST 只在啟動取得商品規格，後續以兩條公共 WS 驅動。HTTP／WS 逾時、回覆大小和佇列有界限；429 不立即重試轟炸。此 batch 不是跨程序共用 HTTP quota，也沒有長時間商品規格刷新。

只接受同名原生 base，不猜測乘數或別名。Aster 與 Binance 為 USDT，Hyperliquid 主 dex／Lighter 為 USDC；目前比較的是名義報價，不包含 USDT／USDC 匯率、費用及 funding。Hyperliquid 沿用共用 catalog 的動態有效位數／szDecimals 規則，不杜撰固定 tick；Lighter 驗證 active、非 frozen、非 force-reduce-only、單位乘數及下單量／名義值約束。商品價格／下單額度最終驗證、maker post-only 拒單、百分比價格帶、帳戶餘額／現貨手續費、leverage、最小 partial-fill 對沖仍必須在執行器接線時完成。

Lighter ticker 每筆包含完整雙邊最佳價格及量，nonce 可跳號；不將 `order_book` 增量訊息當成完整深度。Hyperliquid 以來源時間比較 BBO／完整深度，較舊資料忽略，同時間數值衝突則失效。缺邊、零量、錯幣種／market ID、過期、斷線不產生合格計劃。指定觀察期結束仍從未收到 fresh 配對時會回傳錯誤，不將空連線當成驗收成功。

## 驗證與時間記錄

```powershell
cargo test --manifest-path rust/Cargo.toml -p perp-arb-spot-perp --locked
python rust/tools/compare_spot_perp.py --cases 2000
python rust/tools/compare_spot_perp.py --cases 6000 --output rust/artifacts/spot-perp-multivenue-parity.json
python -m pytest tests/test_rust_spot_perp.py tests/test_spot_perp_arbitrage_script.py tests/test_spot_perp_latency.py tests/test_spot_perp_position_deltas.py -q
```

`compare_spot_perp.py` 是離線決策回放，**不是歷史獲利回測**。比較 open/close、角色選擇、候選數量／價格、門檻、舊單保留與追價，以及過期報價。報告輸出至忽略提交的 `rust/artifacts/spot-perp-parity.json`。候選搜尋將每個角色的最低量不足當成不可用候選；Python 原執行迴圈可能直接退出，這個搜尋差異不代表可略過真實最低下單檢查。

初次 2,000 筆結果零差異，Rust JSON 解析＋決策中位數約 6.2 µs。Python oracle 包括測試包裝及 async guard 建立，兩個數字不是相同環境下交易延遲比較，不能據此宣稱送單加速倍數。WS 預覽最後列印本機收到完整 WS 訊息→決策完成（含解析及排隊）以及純 policy 時間。不是網路單程延遲、私人 WS 成交延遲或兩所實際成交時間差。

公開行情只驗收短時間 ETH 流量，不表示所有幣種、斷線長期恢復和滿載情境已驗收。此批實際訂單數／撤單數均為零。

2026-11-23 驗收：13 個 Rust 測試、73 個 Python 入口／原流程回歸測試通過；Clippy `-D warnings` 及 release 編譯通過。最後一次 15 秒公開 ETH 觀察包含 1,860 個有接收時間的決策樣本，本機 WS 訊息到決策 p50 0.0665 ms、p95 0.2252 ms、最大 11.8023 ms；這包含本機排程抖動，也說明中位數不代表延遲上限。報告保存在 `rust/artifacts/spot-perp-validation-20261123.json`。

## 後續批次

第二小批驗收：21 個 Rust 測試、75 個 Python 回歸測試、Clippy `-D warnings`、release 編譯通過。6,000 個回放案例（Aster／Hyperliquid／Lighter 各 2,000）零差異。15 秒 ETH 公開 WS 短測，Hyperliquid 接受 145 筆、Lighter 582 筆永續報價，均產生合格計劃且無連線錯誤；本機 WS→決策 p95 分別 0.2903／0.3050 ms，並非成交時間。資料見 `rust/artifacts/spot-perp-multivenue-public.json`、`spot-perp-multivenue-parity.json`。沒有執行真實交易，沒有以這些數字宣稱獲利或實際 spread 改善。

1. 已完成 Binance HMAC／WS-API、executionReport、餘額／佣金讀取、精確查單與單筆送單持久化的元件及替身驗收。後續補全帳戶身份／跨程序鎖、佣金持久化、多頁 trades 與自動重連 owner。
2. Rust Maker／market hedge 雙腿執行器：先持久化再提交、成交熱路徑不等待報價查詢、取消競態、未知送單不重送、Ctrl+C 及相同 run-dir 恢復。
3. 接 Hyperliquid／Lighter 實際 hedge transport 及 Variational extension；沿用 Variational Maker 優先，確認其精確委託限制。跨所委託／成交實測及完整時間量度逐個 venue 驗收，不能用公開行情測試代替實盤驗收。

設計參考：[Binance 交易端點](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints)、[私人 WS 訂閱](https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests)、[Aster 官方 API 文件](https://github.com/asterdex/api-docs)。交易端點只供下一批實作參考，本批沒有呼叫。

Hyperliquid 訂閱參考：[官方 WS 格式](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)。Lighter 使用與既有 Python dispatcher 相同的 `ticker/{market_id}`，已以公開 ETH 回覆核實價格、數量、nonce、timestamp 格式。
