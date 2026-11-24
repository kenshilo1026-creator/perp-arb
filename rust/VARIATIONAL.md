# Variational 分批接入

目標架構（2026-11-19）：保留現有 Variational browser extension；Rust 接收它的成交事件，維護去重／未對沖數量與持久化狀態，向 Aster／Hyperliquid 的交易介面提交對沖。瀏覽器資產保留 JavaScript，不以 Rust 重寫 DOM 操作。公開行情及原生唯讀橋接已實作；帳戶恢復驗證、精確成交歸屬與真實對沖尚未完成。

## 本批完成範圍

`market-data::variational` 提供獨立的公開參考行情 client，不加入訂單簿 WS hub。

- 固定 GET `/metadata/v2/stats`；不讀憑證、不送交易請求、不啟動 extension。
- 保留 v2 原生 ticker、perpetual／swap 類型；永續查詢拒絕 swap，不將 USDT 或不同合約單位自動視為同一標的。
- 價格使用十進位字串，拒絕浮點價格、交叉／非正值與無法精確表示的價格。
- 按請求 USDC 名義金額選擇不小於該金額的最小已發布數字檔位。檔位是參考價格樣本，不是可成交深度；超出最大檔位或只有 base 時拒絕，不外推。不推定 tick／lot／最低下單量。
- 保留 `quotes.updated_at`（RFC3339，截取至毫秒），使用既有 freshness 檢查；HTTP 新回覆不會把來源舊報價更新成新行情。
- 同一 client 的 clones 共用 all-market 快取與請求鎖，正常間距／快取 2 秒；429／418、Retry-After 與 JSON retry_after_seconds 共同限制下一次請求，失敗退避；每次呼叫最多等待 15 秒（包括排隊）。無自動背景輪詢或重試迴圈。
- HTTP body 上限 8 MB、禁止 redirect；失敗／取消刷新先清除舊快取。呼叫者持有的舊快照不可當成當前服務健康證明，使用時仍須查驗來源年齡。
- 預算僅在共享 client 範圍內生效，不能協調 Python、獨立 client 或同 IP 的其他程序。

## 唯讀使用

```powershell
cargo run --manifest-path rust/Cargo.toml -p perp-arb-market-data --bin variational-probe -- --symbol ETH --notional-usd 100
```

讀取保存的 v2 回覆、不連網：加 `--input stats-v2.json`。輸出標明 `indicative_only`、`can_submit_orders=false`、來源年齡及 freshness。預設 freshness policy 為 1 秒年齡／500 ms 傳輸延遲／250 ms 未來時鐘容忍；這是診斷，不表示 Fresh 的參考價就能成交。

官方說明：公開 bid／ask 可快取達 600 秒；交易 API 尚未公開。Omni 的畫面報價是 indicative，提交時才產生 firm quote。參考行情不能用於觸發毫秒級實盤對沖，也不是套利收益保證。

來源：[公開 API](https://docs.variational.io/technical-documentation/api)、[報價語意](https://docs.variational.io/omni/trading/quoted-index-and-mark-prices)。

## 驗證與差異

```powershell
cargo test --manifest-path rust/Cargo.toml -p perp-arb-market-data --locked
```

新增 14 個測試涵蓋價格／檔位邊界、時間與缺值、perpetual／swap、共用快取、失敗刷新、429 冷卻、取消等待與超大冷卻時間。行情 crate 合計 91 個測試通過；這是離線／本機 HTTP 測試，沒有真實下單或速度驗收。

與 Python 刻意不同：使用 v2，不接受 v1 未標合約類型的 ticker；不將超出最大數字檔位的請求退回 base；無有效數字檔位時不使用 base 冒充可成交報價。公開行情模組不處理 funding 分析、商品交易精度或策略；私人事件橋接見下一節。這兩批沒有修改原有 Python 交易行為。

## 第二批：原生唯讀 extension 橋接

`crates/variational-bridge` 直接監聽 IPv4 loopback 的 8766（私人事件）、8767（參考報價）、8768（命令），沿用 extension 協議，不啟動 Python broker。三個埠必須一起取得，衝突時退出，不停止其他程式。WebSocket Origin 必須符合指定 extension ID；這是瀏覽器來源限制，不是對本機其他程式的身分認證。

- `wire` 僅解析已知封包，保留 browser capture time、Rust 收到時間、monotonic 收包／解析時間與連線世代。超過 5 秒的捕捉封包、超前 1 秒以上、無時區時間、來源不符、無效或重複持倉均報資料缺口；未知格式只錄根鍵名，不遞迴拼湊成交欄位。
- `Evidence` 在斷線、缺口與換線後清除持倉基準；重複快照不更新來源年齡。持倉快照不是訂單成交或帳戶所有權證明。
- `BoundTrades` 是離線成交綁定元件：明確 order ID／trade ID、商品、方向及增量語意確定後才可累計；重複不增加、衝突與超量要求對帳。唯讀 probe 不會以它觸發交易。
- 只提供價錢預覽與 `checkOnly=true` 掛單列查閱。預覽會切換 Limit／Mid 表單；掛單查閱會切換 Open Orders 分頁，但兩者都不提交／取消委託。掛單列查閱可能由 ID 退回商品比對，故結果固定標記 `order_identity_proven=false`、`terminal_proven=false`。
- 命令使用每次啟動獨立的隨機 request namespace，序列化等待回覆；逾時／斷線不重送。事件讀取在另一 task，不等待 UI 查詢。
- 有界事件佇列滿載就停止並報錯；錄製另用磁碟 worker、獨立有界佇列及最後 flush／sync。不保存原始 headers、cookies、tokens 或完整帳戶 payload；輸出仍可能含商品／數量／訂單 ID，應作私人檔案保存。

```powershell
cargo run --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin variational-bridge-probe -- --extension-id YOUR_CHROME_EXTENSION_ID --output rust/artifacts/variational-new.jsonl --seconds 180
# 可選：另外查閱 ETH 的掛單列，不能用 absence 判定某筆訂單已撤銷或已成交
# 加 --check-symbol ETH
cargo test --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --locked
```

輸出路徑必須尚不存在。啟動 extension 後，原有 auto-reload 可能重新整理 Variational 頁面。probe 到期會關閉連線，extension 保持原有重連行為。

### 本機真實連線驗收（2026-11-19）

180 秒接收窗口內三條 WS 均連線一次，收到 648 個事件：614 筆空持倉快照、31 筆未知訊息（根鍵 `timestamp/type`）、3 筆連線事件；記錄中無缺口／拒絕握手。未知訊息沒有當作成交。當時未觀察到成交，`account_identity_verified` 與 `live_execution_validated` 均為 false。

| Debug 版本、同機短測 | P50 | P95 | P99 | 最大 |
| --- | --- | --- | --- | --- |
| Browser capture → Rust 收到（wall clock、ms） | 0 | 1 | 2 | 43 |
| Rust 收到 → 解析完成（monotonic、μs） | 111 | 223 | 295 | 430 |

0 ms 代表毫秒時間戳內無可見差值，不是零延遲。以上未包含交易所事件產生前的時間、DOM 送單、對沖 HTTP、撮合或成交確認，也不是與 Python 同負載的效能比較。

另外重新連線後，`--check-symbol ETH` 命令往返 2,547 ms，回覆沒有匹配掛單列；此 UI 路徑內含 extension 原有等待，不能當成 Rust 解析延遲或交易所查單終態。這段 60 秒錄製共 564 事件：重新連線首約 24 ms 內有 298 筆捕捉時間不合格的緩存訊息被標為 gap，之後收到 250 筆有效持倉快照，結束時快照仍新鮮。這次觀察證實 extension 會補送積壓訊息；新快照只恢復診斷基準，不代表已補齊成交或完成帳戶對帳。

錄製及統計：`rust/artifacts/variational-bridge-readonly-20261119-02.jsonl`、`...-02-stats.json`；查詢錄製 `...-03.jsonl`。本 crate 21 個測試與 Clippy `-D warnings` 通過，覆蓋重複／部分成交、錯身份、超量、過期／亂序、斷線世代、Origin、命令 ID、無自動重播、佇列滿載、埠衝突及慢頁面查詢不阻塞私人事件。另已跑過 workspace 295 個測試；其後新增的慢查詢隔離案例以本 crate 全部 21 個測試重新驗證。實際下單、真實成交漏補、部分成交撤單競態及完整帳戶恢復仍未驗收。

## 第三批：持久化 Maker 狀態與紙上對沖交接

新增 `execution::durable::browser::Ledger`，重用既有單寫入者／檔案鎖／SHA-256 鏈／`sync_all` 帳本，不另建弱化的 JSON 訂單檔。保存帳戶標籤、request ID、商品、方向、數量、限價、reduce-only 及精確名義金額上限；request ID 僅供本機關聯，沒有假設 Variational 支援冪等送單。

- `dispatch` 在同步磁碟後才發出一次本機許可。未知回覆、UI 錯誤、程序重啟或查無訂單都不能重新取得同一筆的送單許可。
- `bind_verified` 嚴格比對 request／帳戶／商品／方向／數量／reduce-only，再綁定 order ID。它是供已驗證來源呼叫的介面，不會自行驗證頁面身分，也不接受同商品持倉變化當成委託歸屬。
- `observe_verified` 處理已驗證的累計成交；相同累計不新增對沖量，較少的舊值不覆蓋已知成交。撤單期間或已聲稱終態後出現更大的成交，仍保留新數量並要求再次查證。
- 撤單開始、cancel ACK 或 UI 掛單列消失均不確認終態。單筆查詢必須在同一帳本版本、同一程序實例及 5 秒內完成；並行成交、斷線、過期回覆只能增加已確認數量，不能確認剩餘量已終止。來源時間必須有值且不倒退；實際來源語意仍待驗收。
- 重啟保留成交與未知送單，清除即時查證狀態。磁碟失敗保留記憶體可見的已知曝險、封鎖後續許可；不刪除或修補殘缺帳本來重送。
- `DurableEngine::observe_browser_maker` 將此帳本交給既有對沖預留流程。雙帳本的查證票據另檢查對沖協調器版本及查詢開始時間，舊查證不能跨過新斷線屏障；只清除 Maker 的恢復要求，未知對沖單仍需獨立查證。
- `variational_bridge::orders::prepare_submit` 僅準備 `PLACE_ORDER` JSON，保留 `reduceOnly`；`record_submit_result` 只記 ACK／未知。現有 `Bridge` 的公開傳輸仍只有唯讀命令，沒有送出這份下單 JSON。尚無真正雙腿平倉或新 Maker 重掛排程。

同步帳本須由專用 blocking worker 擁有，不能在私人 WS reader 直接寫磁碟；本批未把帳本接進唯讀 reader，也沒有宣稱磁碟同步能減少延遲。沿用既有兩腿對帳後才能啟用協調器；測試中的 Variational 帳戶報告是合成資料，實盤報告來源尚未建立。

另依現有 Python 測試補上 `/portfolio` 的 `type=trade`／`data.instrument.underlying` 訊息解析。只讀取單一明確物件；外層訂單 ID 與內層數量混雜或多筆陣列會拒絕。缺少 order ID／trade ID 仍只是 candidate，不會自動觸發對沖。

本批新增 21 個 Rust 測試，涵蓋 WAL 錯誤／重啟、精確身份、ACK 前成交、部分成交、撤單競態、過期與跨程序查證、雙帳本斷線屏障及不精確十進位增量。相關 execution＋bridge 共 72 個測試、Clippy `-D warnings`、31 個 Python Variational 回歸測試通過。兩個對沖 venue 的紙上案例均驗證「已成交 2，撤單中再成交 1，只再預留 1」；恢復 Maker 不會解除原本未知的對沖 2 單位。

擴大驗收：workspace 共 317 個測試通過；重新編譯 release `execution-replay`，與 Python 既有終態／剩餘量邏輯比較 1,200 組案例，差異 0。記錄為 `rust/artifacts/variational-lifecycle-workspace-tests-20261119.txt`、`variational-lifecycle-tests-20261119.txt`、`variational-lifecycle-parity-20261119.json`。這些是執行正確性與回歸比對，不是歷史行情盈虧回測，也沒有量測真實成交速度。

```powershell
cargo test --manifest-path rust/Cargo.toml -p perp-arb-execution -p perp-arb-variational-bridge --locked
python -m unittest tests.test_variational_fill_safety tests.test_variational_extension -q
```

## 第四批：真實 RFQ 來源、一次性傳輸與 CLI 唯讀入口

已保留 extension 架構，新增 `browser_extensions/variational/native_orders.js`；在已登入的 Omni 頁面 **ISOLATED world** 執行固定同源請求。不是官方交易 API SDK，也不是任意 URL／任意 POST proxy；不匯出 cookies、JWT、auth headers 或私鑰。唯讀請求包含前後兩次 `/api/me`，只帶回經新回覆取得的地址與 session 一致性；這不是 JWT 本機簽章驗證，也不等於資金／持倉完整對帳。

來源線索來自 [公開 client 的原始 schema](https://github.com/Magicianhax/variational-trading-api/blob/main/src/schemas.ts)，隨後已使用本機真實 session 核對讀取路徑及欄位：

| 用途 | 實際來源與身份 |
| --- | --- |
| 帳戶 | `GET /api/me`；token 僅在瀏覽器函式中暫存，回傳地址前檢查過期 |
| 委託 | `GET /api/orders/v2`，`rfq_id` 為委託 key |
| 成交 | `GET /api/trades`，`id` 為成交 key、`source_rfq` 指向原委託 |
| 送單回覆 | library 路徑 `POST /api/orders/new/limit` 回覆 `rfq_id`；僅模擬驗證，未真實送單 |
| 撤單 | library 路徑 `POST /api/orders/cancel {rfq_id}`；僅模擬驗證，ACK 不是終態 |

查詢不是臆造的 `/orders/{id}`：目前以固定大小、最多 5 頁的列表查找精確 RFQ。正向匹配後停止掃描，`window_complete=false` 保留；缺列不表示拒單／撤單／零成交。分頁不是原子快照，不能用來聲稱整個帳戶已恢復。每次命令內請求相隔至少 500 ms、有 15 秒總期限、1 MiB 回覆限制；無自動 HTTP 重試。這是有限查證的負載限制，不是已確認的交易所私人 API 限額。

`native::Query::snapshot` 比對持久化意圖的帳戶、RFQ、普通 crypto perpetual／USDC 商品、方向、原量、限價及 reduce-only；不支援把 `swap` 或 RWA 當普通 perpetual。已確認成交按 trade ID 去重，數量精確累計；錯 RFQ、矛盾重複、超量或未知狀態要求查證。只有 cleared RFQ、成功 clearing、confirmed 成交總量等於原量且有合理 HTTP Date 才可提出 Filled 快照。HTTP Date 是查詢來源時間，不是成交時間。取消／拒絕零成交終態尚未完成真實競態驗收，保持待核對。WS 解析新增 `source_rfq`／`id` 映射，但仍只是 candidate；沒有將未確認 WS 事件直接用作對沖授權。

`Bridge::submit_native`／`cancel_native` 消耗既有 WAL permit、保留 reduce-only、精確商品與限價，命令序列化；native limit payload 限定非 trigger、非 auto-resize、slippage_limit=0。這份 POST payload 的可接受性及實際價格語意還沒有經過真實送單驗證。extension 的 `nativeTradingEnabled` 預設 false，CLI 沒有啟用選項；請不要手動打開來代替尚未完成的雙腿 owner。

extension 在任何 native mutation 前保存 request ID 屏障；送單結果再保存 RFQ receipt，Rust 的 `recover_native` 可在新 session 核對後恢復身份，不授權重送。最多保存 1,000 個 dispatch，不自動驅逐舊項目。若送單成功但瀏覽器在保存 receipt 前崩潰，仍可能只有 UNKNOWN；本機 request ID 不是交易所冪等 key，不能靠 symbol／size 推測是哪筆訂單。沒有 receipt 就保持未知。WAL 與同步磁碟方法仍須由 blocking owner 執行，未接入私人 WS reader。

### 真實唯讀結果（2026-11-19）

- 取得 100 筆歷史委託與 100 筆成交，59 筆成交的 `source_rfq` 對上該頁 RFQ；含 crypto perpetual、RWA、swap，未將它們混合成同一商品。
- 指定一筆既有 crypto limit RFQ，查得 1 筆委託及 1 筆 confirmed trade。初版掃頁命令往返 8,968 ms；改為正向命中停止後，經 `place_order.py` → Rust → extension 的同一 RFQ 查詢往返 3,519 ms。兩次短測不是分位數或 Python／Rust 同負載效能比較，也不是送單／成交延遲。
- 第一次 CLI 驗收失敗時未退回 Python。觀察到重連自動 reload 可與命令重疊，已將 reload 與命令互斥、等待頁面載入完成，再次唯讀查詢成功。三條 WS 持續分開處理；重連積壓事件仍被標 gap。
- 沒有執行真實 POST、撤單、Aster／Hyperliquid 對沖或損益回測。真實送單身份仍須小額訂單驗收；目前驗證的是歷史 RFQ／成交來源與模擬 receipt 生命週期。

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin variational-bridge-probe --locked
python scripts/place_order.py --engine rust --rust-preflight --extension-id YOUR_CHROME_EXTENSION_ID
# 精確歷史查單，另外加：--rust-account 0x... --rust-order-id REAL_RFQ_ID
# 可加 --rust-output NEW_PRIVATE.jsonl；路徑不得已存在。
```

`--engine` 預設仍為 python；Rust 選項不能混入 Python 模式。`--engine rust` 未加 `--rust-preflight` 會明確拒絕，尚無 Rust 雙腿 live 入口，不會靜默切回 Python。Rust 子程式退出／中斷後釋放本機埠，不啟動 Python broker。

測試包括 Native source 身份／錯帳戶／類型／十進位／重複與未知成交、RFQ ACK 與 Fill 分離、未知送單重啟、receipt 恢復、儲存失敗、page 形狀／錯誤回覆、同時 UI 命令、reload 競態及預設 mutation 關閉。相關命令：

本批最終驗收：Rust workspace **331** 個測試、Python **35** 個測試、Node extension **15** 個測試全部通過；相關 crates 的 Clippy `-D warnings`、Rust 格式檢查及 `git diff --check` 通過。結果記錄在 `rust/artifacts/variational-native-validation-20261119.json` 及同前綴測試輸出。這是正確性／故障與唯讀來源驗收，並非盈虧或真實下單速度回測。

```powershell
cargo test --manifest-path rust/Cargo.toml --workspace --locked
cargo clippy --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge -p perp-arb-execution --all-targets --locked -- -D warnings
node --test tests/variational_native_orders.test.mjs
python -m unittest tests.test_rust_place_order tests.test_variational_fill_safety tests.test_variational_extension -q
```

私人錄製位於忽略追蹤的 `rust/artifacts/variational-native-sources-20261119-01.jsonl`、`variational-native-exact-20261119-01.jsonl`、`variational-place-order-preflight-20261119-02.jsonl`；仍含地址、訂單與成交量，勿公開上傳。

## 第五批：持久化 trade 去重與雙帳本 blocking owner

`native::Query::record` 將精確 RFQ 查詢的 confirmed trade IDs、數量、價格與累計成交，在同一筆 Maker WAL 記錄中同步寫入。跨查詢／分頁／重啟以 trade ID 取聯集；同 ID 不重複累計，數量或價格矛盾留下持久化衝突，要求人工查證。上限 8,192 個 ID，不驅逐舊 ID；未知狀態及 WS candidate 仍不能輸入已確認成交路徑。

舊 WAL 只有匿名累計量時，採 `max(原累計, 已確認 trade 聯集總量)`，不把兩者相加。這是已知成交下限，不代表已完成舊 WS／REST 基準交接。有限分頁合併得到全量也不能自行確認終態，仍須同一次新查詢的完整證據。全量成交缺少 HTTP Date 時保留 Filled 狀態及數量，但 `requires_reconciliation=true`；不會誤判已查證終態，也不會因非法 PartiallyFilled 狀態而漏記全量。

新增 `variational_bridge::owner::Owner`：

- blocking worker 擁有兩份 WAL；建立、重開、成交去重、對沖預留、許可的磁碟同步均在此執行，網路等待留在外部 async task。
- 啟動核對帳戶、RFQ、商品、方向、原量；程序若在 Maker 寫入後、paired 寫入前中斷，重啟先交接已知成交，再公布曝險。既有帳戶報告門檻及恢復限制保留。
- `begin_lookup` → 外部 `Bridge::native_query(ticket.request())` → `apply_lookup` 串起精確查詢與雙帳本版本檢查。正常運行的已確認部分成交可預留對沖；重啟／斷線後仍需恢復。完成 Maker 查證不會清除未知對沖單。
- 提供 Aster／Hyperliquid 對沖預留、一次性 dispatch permit、ACK、私人累計快照及精確 REST 恢復入口。撤單僅返回持久化 cancel permit，ACK 不是終態。
- 佇列滿載立即報錯，在後續命令前要求兩腿查證。取消等待者不丟棄已接收的成交證據；遺失許可回覆維持未知。`shutdown` 等待工作完成並釋放檔案鎖。

這是雙帳本協調元件，**尚未接到真實雙腿送單排程**。外部 transport 仍須轉交所有 gap、驗證即時 WS trade 語意、提供真實帳戶報告，以及補上規格、價格、reduce-only 路由與取消終態。`place_order.py --engine rust` 仍僅開放 preflight。本批沒有更改 extension，無需重新載入，也沒有執行真實交易。

新增 16 個故障測試：兩種 hedge venue 的部分成交、撤單中遲到成交、重複／截短分頁、未知對沖重啟及精確恢復；另包括 trade 衝突、舊累計交接、超量、磁碟失敗、版本競態、滿載、取消等待、執行緒隔離、帳本不匹配及兩次寫入間中斷。這些不是盈虧回測或實盤速度驗收。

最終驗收：Rust workspace **347**、Python **35**、extension **15** 個測試全部通過；Clippy `-D warnings`、格式與 diff 檢查通過。重新編譯 release `execution-replay` 後，Python／Rust 原有累計對沖量與終態剩餘量比對 **1,200 組、差異 0**；該比對不涵蓋新 owner 的整條交易流程，新增部分由上述故障測試覆蓋。記錄：`rust/artifacts/variational-owner-workspace-tests-20261119.txt`、`variational-owner-extension-tests-20261119.txt`、`variational-owner-parity-20261119.json`、`variational-owner-validation-20261119.json`。

## 第六批：全帳戶讀取、WS 交叉驗證與送單接線

extension 新增唯讀 `inventory` 命令：兩次讀取 `/positions`、`/portfolio?compute_margin=true` 及完整分頁的 pending `/orders/v2`，前後驗證登入帳戶。既有請求間隔、15 秒期限、大小限制保留；不自動重試 HTTP 或 429。缺欄、分頁不完整、未遵守 pending 篩選、重複 ID、兩輪數量／餘額／商品／均價變動都不能核發穩定庫存。此方法不是交易所原子快照。

Rust `inventory` 保留 signed positions、cross 抵押金及逐項 isolated 抵押金，檢查總額相等。零 allocated 的 swap 抵押金列可以入帳，但不授權交易 swap。RWA／非普通持倉、非零 isolated allocation、尚未確認 remaining 語意的活躍 RFQ，均阻止帳戶通過；不把不支援的項目丟掉後聲稱全帳戶已對帳。

`account_feed` 接私人 WS 的連線世代、gap、持倉／餘額變更及未知帳戶事件，使舊報告／查詢票據失效；heartbeat 不會無故失效。WS portfolio 是增量觀察，不能冒充完整庫存。`inventory::Query::reconcile` 接受獨立帳本的 expected inventory；CLI 刻意只查來源，不從這次交易所回覆複製 expected，所以 `account_report_created=true` 與 `account_reconciled=false` 可以同時成立。

`ws_validation` 只把具備 confirmed、trade type、RFQ、trade ID、商品、方向、數量、價格的事件列為候選，逐筆與 authenticated exact REST 核對。同 ID 矛盾、缺口、舊世代及容量溢位拒絕驗證；缺口包括同一條 socket 上的 gap。`execution_io::reconcile_maker_with_ws` 在 REST 等待期間不鎖住事件接收；即使 WS 不符，也先將 REST 已確認成交寫入帳本，保留曝險。**仍沒有啟用未經 REST 確認的 WS-only 對沖。** 本次真實錄製沒有新成交，不能聲稱已驗收私人即時成交語意或速度。

送單 library 的具體接線：

- Maker POST 前建立兩份 WAL，以本機 pending identity 初始化 paired plan；最新帳戶／價格檢查通過才取得一次性 permit。extension 回覆的 RFQ 先持久化，再綁定 paired plan；ACK 不增加成交量。重啟可補上兩份 WAL 之間中斷的交接。
- `execution_io::submit_maker`／`cancel_maker`／`recover_maker_identity` 接既有 native transport。mutation 仍需 extension 明確啟用；預設關閉。
- `dispatch_hedge` 接 Aster／Hyperliquid `Venue`，先檢查 IOC、精度及名義值，將完整 network／asset／價格／數量／reduce-only／client ID 同步寫入，再發送一次。部分成交經精確查單確認終態後保留可再次預留的剩餘量；UNKNOWN／NotFound 不重送。
- `reconcile_hedge` 以帳本身份查證。caller 仍須管理帳戶鎖、兩所私人監控及所有 gap、有效商品規格與行情、IOC 剩餘量後續處理及停機恢復。尚未開放雙腿 live CLI，也沒有 Maker 自動撤回／重掛排程。

### 本批真實唯讀驗收

更新 extension 後，`variational-inventory-live-20261119-03.jsonl` 記錄的雙讀耗時 **6,889 ms**，成功產生帳戶報告，0 持倉、0 待成交單、2 項抵押金帳目。這是啟動對帳往返，並非送單或對沖延遲；未提供獨立 expected inventory，因此尚不能授權交易。

45 秒錄製共有 1,385 事件、188 個 portfolio 觀察、0 個新成交候選。1,170 個 stale gap 全部集中在重連初期 0.001–2.510 秒，已排除舊積壓後再建立新基準；不是 1,170 次斷線。前次查詢確實被 gap 以 WrongSession 擋下，probe 現在僅對此競態重新建立票據並最多再查一次。完整紀錄只存於忽略提交的私人 artifacts；未送單、未撤單。

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin variational-bridge-probe --locked
python scripts/place_order.py --engine rust --rust-preflight --rust-inventory --rust-account 0xYOUR_ACCOUNT --extension-id YOUR_EXTENSION_ID
```

`--rust-inventory` 不得與 `--rust-order-id` 混用。需要單筆 RFQ 查證時移除 inventory，使用既有 exact lookup 選項。Python 仍是預設執行引擎，不會在 Rust 驗收失敗後偷偷啟動 Python 交易。

本批 Rust workspace **360**、Python **36**、Node extension **18** 個測試通過。新增測試涵蓋帳戶非原子讀取、錯帳戶／缺欄／特殊抵押金、事件期間失效、WS／REST 矛盾保留成交、RFQ 綁定與重啟、兩所 IOC 部分成交、錯精度／reduce-only／上限、遺失 ACK 不重送。Clippy `-D warnings` 通過。這些是正確性及故障測試，不是盈虧回測。

重新編譯 release `execution-replay` 後，與 Python 的既有累計對沖量／終態剩餘量比對 **1,200 組、差異 0**；範圍不包含整條新送單流程。格式與 diff 檢查通過。摘要記錄於 `rust/artifacts/variational-account-validation-20261119.json`，測試輸出及回放比對使用同前綴檔名；未提交或推送。

## 第七批：有界限雙腿 CLI

新增 `paired-order` 與 `place_order.py --engine rust --rust-action preview/audit/submit/inspect/recover`。正式 runner 持有兩所帳戶鎖和手動 CLI 共用鎖，接收私人事件，驗證獨立基準與新鮮原生深度，再建立兩份 WAL 送出一筆 Variational 固定限價。已確認的 Maker 成交在 Maker 恢復屏障存在時仍可產生對沖責任；未知 Hedge 仍保留在途量，不能重複送單。終態 IOC 部分成交可繼續處理剩餘量。

兩組真實唯讀對帳均成功：Variational／Hyperliquid 無持倉或掛單；Aster 有兩筆持倉及一筆既有掛單，因此新單前置檢查仍會阻止該帳戶新增委託。本批未送出真實委託或撤單。完整操作、帳戶基準、開平倉參數及未驗收限制見 [PAIRED_ORDER.md](PAIRED_ORDER.md)。

新增 runner loopback 測試涵蓋兩所部分成交、終態剩餘重試，以及未知 POST／NotFound 不重送。Ctrl+C 向子程序送出 STOP 並等待恢復，不直接殺死持有真實 Maker 的 Rust 程序。擴充功能 popup 新增原生交易 opt-in，預設仍關閉。唯讀對帳只對明確帳戶世代競態重試一次，不對一般 HTTP 錯誤重試。

第七批最終驗收：Rust workspace **367**、相關 Python **44**、Node extension **19** 個測試通過；Clippy、格式、release 編譯及 CLI 公開預覽通過。既有累計對沖／終態重試回放 **1,200 組，差異 0**，不涵蓋整條新實盤流程或收益回測。舊 Variational Maker 預設測試仍預期 MEXC，已按現有 Binance 現貨入口修正；移植清單補入未提交的新來源及現況雜湊，不表示其餘 Python 功能已等價移植。私人驗收摘要在忽略提交的 `rust/artifacts/paired-validation-20261119.json`。

## 剩餘接線

1. 提供獨立預期帳戶基準；取得真正新成交樣本驗證 WS confirmed 語意、重連補漏及時間記錄，再評估移除正常成交熱路徑的 REST 等待。現有全帳戶來源不能涵蓋尚未支援的活躍／特殊委託。
2. 完成取消／拒絕的明確零成交終態、真實部分／遲到成交競態與斷線補漏契約，再開放新 Maker 重掛；目前取消 ACK／查無 RFQ 仍不能釋放曝險。
3. 第七批已完成單筆 bounded runner、當前規格解析、行情、IOC 剩餘處理與停機恢復。尚需補自動 Maker 重掛／長時間規格刷新、費用與盈虧帳本、Python 倉位登記整合。真實送單驗收仍需明確幣種、數量與價格上限。
4. 量測 extension 觀測→Rust 收到→完成解析→對沖送出→ACK→對沖成交確認，分開本機時間與跨所來源時間。

最終熱路徑應由 extension 直接連 Rust 的相容本機介面；過渡期若沿用 Python broker，需要明確量測它的排隊耗時，不能宣稱已去除 Python 路徑。零延遲或必然搶贏其他 bot 均非驗收承諾。
