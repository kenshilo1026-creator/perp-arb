# Rust 第一批及公共行情小步驗收

最新狀態：2026-11-12 完成 4b-2 單筆限價下單測試入口；已通過離線／loopback 與公開 metadata 預覽，尚未執行實際委託驗收。下方先列本批結果，其後保留歷史基準。

## 4b-2：受控單筆下單測試入口

新增 `order-test`，支援 Aster v3 mainnet 與 Hyperliquid mainnet／testnet 的單筆 LIMIT 委託：公開預覽、送單、按 client ID 查證、到期撤回剩餘量、查閱帳本及顯式恢復。Aster 使用 GTX／IOC、BOTH、reduce-only；HL 使用 Alo／Ioc，先核對主帳戶／agent 所屬帳戶。兩所固定交易端點只嘗試送單一次，回覆不明不自動重送。

| 驗收項目 | 結果 |
| --- | --- |
| Rust workspace | 261 個測試通過，較 4b-1 增加 17 個；包含既有退出子程序入口 |
| 格式／Clippy／release | 全部通過，`batch4b2-acceptance.log` |
| Python 驗證器 | 18 個測試通過 |
| 新增 HL 交易簽名 | 8 組 mainnet／testnet、委託／撤單、TIF 的 action 與簽名比對通過 |
| 執行回放 | 1,200 個案例、0 mismatch；限定終態剩餘量規則，不是新 CLI 全流程 Python 等價證明 |
| 既有回歸 | 計算 571、深度 120、共享快取 560、商品 1,018、Aster 私人事件 901 及 2 項刻意差異、Aster 認證 4、HL 狀態 601；全部通過既定規格 |
| 新執行檔公開預覽 | Aster mainnet／HL mainnet／HL testnet 的 ETH 商品精度與 asset ID 解析通過；僅讀公開 metadata |
| CLI 旗標 | `--help` 成功；缺 `--live` 的 submit 在讀取 plan／帳戶前拒絕 |
| 來源清單 | 202 個既有來源／設定／測試／資產檔案核對通過 |

新增測試涵蓋完整委託與名義金額上限先持久化、同步失敗不交出許可、同一帳本不能再次提交、錯身份／過期查證不釋放未知、歷史終態重開後不算新鮮證明、ACK 明確成交數量在查不到單及重啟後仍保留。模擬交易測試驗證固定 URL／HTTP method／client ID、精確字串價格與數量、HL 帳戶綁定、錯網路拒絕、timeout／錯誤不重送、部分成交後撤單、取消 ACK 不當成終態、Ctrl+C 送單前停止，以及慢唯讀查詢不能延後撤單觸發。

帳本由獨立 blocking worker 擁有，私人 WS reader 不執行磁碟同步；按 OS 使用者／網路／帳戶的本機鎖避免不同 journal 同時測同一帳戶。恢復只針對保存的 client ID 查證／撤單，不重新發送委託。帳本含交易資料但不含金鑰。原 Python 與既有持倉檔案未修改。

本批沒有載入真實帳戶金鑰、私人帳戶查詢或送出任何真實委託。新 CLI 的實盤下單／部分成交／斷線恢復驗收仍待使用者指定參數後進行。完整紀錄及雜湊見 `batch4b2-summary.json`、`batch4b2-*-regression.json`、`batch4b2-public-preview.json`。

### 本批能力邊界

這是**單筆限價交易測試器**。已成交部位會保留；撤單只處理剩餘量。尚未接自動兩腿對沖、價差護欄重掛、Variational、spot、HIP-3、vault／subaccount、槓桿調整或完整資金帳本。交易所拒單但查不到訂單時仍可能保守退出未知，不自行重送或反向交易。

查證器約每 500 ms 輪詢並保存私人協調器的累計變化；共享 HTTP gate／配額仍可能增加等待。`submit_ack_ms` 含磁碟與限流，`first_coordinator_fill_ms` 可能來自 WS 或 REST，`terminal_lookup_ms` 是終態核對時間，均不能當成兩所實際成交差。沒有實盤速度提升、低於 100 ms 或收益驗證。Python 不遵守本機帳戶鎖；日誌仍無跨主機擁有權、回滾偵測或實體斷電保證。

完整參數、範例及恢復指令見 [操作說明](crates/order-test/README.md)。

## 4b-1：持久化對沖帳本與未知送單查證

新增 `execution::durable` 與共用 `private_events::lookup`。Aster／HL 私人 HTTP client 提供按保存的 client ID 查證接口，使用原有 gate／權重／冷卻。此批只在本機檔案與 loopback HTTP／WS 替身驗證，沒有連真實帳戶、建立／取消委託或覆寫 Python 持倉檔案。

| 驗收項目 | 結果 |
| --- | --- |
| Rust workspace | 244 個測試通過，較上批增加 26 個；包含一個由父測試啟動的退出子程序入口 |
| 格式／Clippy／release | 通過；完整指令與結果見 `batch4b1-final.log` |
| Python 驗證器 | 18 個測試通過 |
| 執行回放 | 1,200 個案例與原 Python 終態剩餘量規則一致，0 mismatch |
| 既有回歸 | 計算 571、深度 120、共享快取 560、商品 1,018、Aster 私人事件 901 及 2 項刻意差異、認證 4、HL 訂單狀態 601；通過既定比對要求 |
| 原專案清單 | 202 個來源／設定／測試／資產檔案核對通過 |

新增測試驗證：

- 送單標記必須寫入並 `sync_all()` 成功才給許可；同一意圖不能再次取得許可。重啟可列出已預留但未送出、已標記送出但未知、已綁定及已確認的意圖。
- **實際子程序**在取得許可後以 `process::exit(73)` 直接退出，父程序重開帳本，曝險及整份未知預留量保持正確；不是單純 drop 物件模擬，也不是斷電測試。
- 查無訂單不釋放量；完整身份、累計量與終態核對後才重試未成交剩餘量。錯 client ID／帳戶／方向／商品／數量／order ID 不影響已知成交。
- 查詢期間新增成交、斷線屏障、過期 ticket、舊實例 ticket、較少累計量及 WS／REST 時間欄位差異；重複歷史終態不重新佔用已重試量，終態後矛盾成交保留負曝險並標記故障。
- 檔案鎖、既有檔案拒絕覆寫、計劃身份、殘缺尾端與位元損壞；故障注入涵蓋部分寫入，以及完整寫入後尚未確認同步成功。I/O 失敗不交出許可、標記故障，並保留新得知的記憶體成交量。
- Aster 簽名 GET `origClientOrderId`、`-2013` 的 HTTP 200／400、其他錯誤與 429 冷卻；HL `/info orderStatus` 的字串 cloid、`unknownOid`、錯帳戶及 429；不呼叫交易端點。

首輪完整結果保留在 `batch4b1-acceptance.log`；最後補強 HTTP 400 missing-order 解碼與不可由外部直接建構的送單許可後，完整重跑並記錄於 `batch4b1-final.log`。版本、來源／二進位／證據雜湊和本批回歸報告集中於 `batch4b1-summary.json` 及 `batch4b1-*-regression.json`，舊批次檔案保持不變。

### 本批範圍

這是**單向永續、已有 order ID 的一個 Maker 與對沖意圖**的執行帳本，未涵蓋新 Maker 未知送單、完整送單參數、真實 broker 或自動恢復排程。帳本保存獨立庫存基準並推導策略成交後的預期持倉；尚未處理完整現金／費用／funding／轉帳會計，不能藉此自動解除上批 HL 的特殊事件。

磁碟同步在未來整合時須放專用 blocking worker，不能阻塞私人 WS reader；取得許可前仍有同步磁碟成本。這批沒有送單延遲基準、收益回測或低於 100 ms 的實盤證明。檔案鎖只限同一檔案；尚無跨檔案／跨主機策略擁有權、日誌輪替、備份回滾偵測或實體斷電驗收。詳細 API、限制及後續接線見 [執行協調器](crates/execution/README.md)。

協議依據：[Aster 訂單查詢](https://asterdex.github.io/aster-api-website/futures-v3/account%26trades/#query-order-user_data)、[Hyperliquid orderStatus](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)。查不到歷史委託不能證明從未收單，故不轉換成拒單或零成交。

## 3c-1／4a：帳戶對帳、特殊事件及紙上執行

所有程式與測試只改在 `rust/`。新增 `account` 型別與明確覆蓋範圍、雙採樣唯讀客戶端、共用權重限流、資金費率／非 funding 帳戶事件失效通知，以及 `crates/execution`。既有 Python 交易入口未切換。

| 驗收項目 | 結果 |
| --- | --- |
| Rust workspace | 218 個測試通過，比上一批新增 23 個 |
| 格式／Clippy／release | 通過；最後的最低可對沖數量檢查另重跑 execution 測試、全 workspace Clippy、release 與比對 |
| Python 驗證器 | 18 個測試通過 |
| 新 execution 回放 | 1,200 個案例與現有 Python `terminal_fill_quantity()` 終態重試規則一致；包含終態／非終態、待對帳、重複成交 |
| 既有回歸 | 計算 571、深度 120、共享快取 560、商品 1,018、Aster 私人事件 901 及 2 項刻意差異、認證 4、HL 訂單狀態 601，全部通過既定比對要求 |
| 原專案清單 | 202 個來源／設定／測試／資產檔案核對通過 |

新增測試涵蓋：Aster hedge mode 不抵銷、HL dex／同名 token 隔離、重複／缺失資料拒絕、無獨立預期庫存不通過、持倉／掛單／覆蓋範圍不符、報告過期、跨 actor／斷線／查詢期間事件隔離，以及開始新對帳時立即使舊報告失效。特殊事件保留資料，不合成訂單；現貨 dust 不借用永續水位解除。

執行測試將兩腿真實 `Coordinator` 介面接入紙上 `Engine`，驗證 Maker 已成交後仍可保留對沖責任、對沖未知時只預留一次、ACK 不算成交、ACK 前成交、部分成交／撤單缺口、終態剩餘量重試、終態後矛盾成交、dust、錯帳戶／錯單量。最初 clip 小於最低可對沖量時拒絕啟動；執行中的部分成交尾數則明確保留曝險。

### 真實帳戶唯讀短測

兩所均以 `--live-read-only --verify-recovery --seconds 90 --audit-account` 測試，不送單／撤單、不存原始私人 payload。`--audit-account` 明確查詢兩輪；不把實際快照自動當成獨立預期帳本。最終 release 首輪兩所出現 `Network` 並恢復，嚴格無錯誤驗收失敗；保留 `validation/batch3c1-aster-live-final.json` 及 `validation/batch3c1-hyperliquid-live-final.json`。使用同一二進位再做一次 90 秒重測，沒有重現網路錯誤；結果另存 `batch3c1-aster-recovery-retry.json`、`batch3c1-hyperliquid-recovery-retry.json`，不覆蓋前輪失敗。

- Aster 重測：4 次連線、6 次續期、無錯誤，exit 0；2 筆持倉、35 筆錢包資料、0 掛單。未收到真實成交事件，仍不能宣稱已驗證成交補漏或真實訂單查詢。
- Hyperliquid 重測：4 次連線、16 ACK、4 初始快照、30 唯一成交及 90 重複成交；90 次成功查詢、0 查詢失敗、0 `unknownOid`，`transport_recovery_passed=true`。與上一批相比，特殊事件已轉入帳戶對帳，避免重複錯查；仍有 1 筆特殊事件待核對，故嚴格恢復 probe **維持 exit 1**。這不是把資料刪掉後宣稱恢復。
- 兩所 `expected_supplied=false`、`matched=false`：現有實網測試僅證明穩定採樣與連線，沒有獨立完整策略／資金帳本，因此未授權新增真實風險。

證據：`validation/batch3c1-acceptance.log`、`batch3c1-execution-final.log`、`batch3c1-execution-validation.json`、`batch3c1-summary.json`。先前各批封存報告保持不變。

### 本批界線與下一批

雙採樣與 WS 版本隔離不是交易所原子快照。庫存證明只適用明確列出的 perp dex／spot 範圍；沒有全歷史分頁、vault／所有子帳戶、完整條件單會計或持續保證金管理。特殊事件的提示文字不能單獨證明經濟結果；需要獨立預期帳本及足夠來源水位，才能解除待核對。

下一批先補持久化策略帳本／intent、重啟與 client ID 未知送單關聯，再接真實交易 adapter、撤單／重掛排程及停機交接。紙上 `Intent` 不會觸發任何真實交易。本批回放不是收益回測，也未證明實盤對沖低於 100 ms。

協議依據：[Aster 帳戶／持倉與請求權重](https://asterdex.github.io/aster-api-website/futures-v3/account%26trades/)、[HL 私人訂閱與事件](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)、[HL 永續帳戶](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)、[HL 現貨帳戶](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot)。

## 歷史驗收

2026 年 11 月 5 日，Windows x64、Rust 1.99.1、Python 3.11.9。第一批計算核心通過後，才開始第二批 2a 的公共行情接入。測試沒有載入帳戶金鑰、送出真實交易或改動 Python 策略。

## 驗收結果

| 項目 | 結果 |
| --- | --- |
| 格式、Clippy 警告視為錯誤、release 建置 | 全部通過 |
| Rust 測試 | 24 個通過，含 14 個核心／協議、8 個行情狀態、2 個錄製安全測試 |
| Python 比對工具測試 | 7 個通過 |
| 計算案例 | 原本 571 個全部通過；140 個除法結果存在已記錄的十進位末位差異，均在既定容差內；tick／最差成交價／布林／深度不足要求完全一致 |
| 合成行情重播 | 120 則，兩所各 60 則，深度價格／數量／時間／新鮮度一致 |
| 公開行情錄製 | 30 秒；Aster 151 則深度，Hyperliquid 55 則深度及 2 則其他文字訊息；無連線錯誤／重連 |
| 真實訊息重播 | 208 則全部與 Python 一致；206 筆有效快照；Rust 錄製時與離線重播結果亦一致 |
| 原專案來源清單 | 202 個檔案核對通過；其餘功能未宣稱完成 |

驗收修正了測試使用 Rust 保留字、超過 28 小數位的 Python 中間值在 tick 取整前被拒絕，以及 WS TLS 提供者未設定。原本失敗的 22 個取整案例沒有刪除或放寬要求；以保留係數／尺度的整數計算修正，並新增 tick 上下兩側邊界測試。另驗證未來時間戳不能阻止後續正常行情更新。

## 本機效能

| 同一工作負載 | Python | Rust release | 解讀 |
| --- | --- | --- | --- |
| 571 計算案例，批次平均耗時的中位數 | 0.964 µs／案例 | 0.0703 µs／案例 | 約 13.7 倍；排除解析與 I/O |
| 208 真實訊息離線 JSON 解析及快取，p50 | 27.5 µs | 7.8 µs | 約 3.5 倍；節省約 0.0197 ms／訊息 |
| 同上 p95 | 33.0 µs | 9.8 µs | 小樣本描述，非長期尾延遲保證 |
| 同上 p99 | 53.9 µs | 17.3 µs | 同上 |

計算微基準暖機後執行 21 組、每組 100 次案例集合。行情重播是一批短樣本；Rust 額外執行深度驗證，兩邊未含磁碟序列化／網路／下單。實際連線錄製中的 Rust 解析及快取 p50 為 Aster 20.6 µs、Hyperliquid 19.3 µs，與離線熱快取數字不同。

**尚未量測私人 WS 成交到對沖送單，也未證明實盤成交延遲降低。** 約 200 ms 的下單回覆仍可能主要在網路或交易所；不能將計算加速倍數直接套用到該時間。

## 證據與後續範圍

完整數值報告保存於 `validation/`；原始公開錄製位於 `artifacts/batch2-eth-20261105-02.jsonl`，不加入 Git，報告含資料與執行檔雜湊。第一次 TLS 失敗的不完整錄製 `...-01.jsonl` 保留，不納入成功驗收。

目前停止在 2a。下一步為 2b：共享行情快取、多配對連線、BBO 與 depth 一致性、商品／合約單位及真實斷線故障注入。這次 30 秒沒有觸發重連；單元測試中的斷線狀態重置不等於完整網路重連已驗收。私人 WS、簽名與下單、對沖協調、資金費率、風控及 UI 後端尚未移植。

## 2026-11-09：2b-1 共享公共行情

新增 `shared::MarketState`、`hub::MarketHub`、`shared-replay` 與 `shared-probe`。相同原生商品的多個消費者共用一條 WS、一份不可變行情版本；Aster combined depth＋bookTicker，HL fast depth＋bbo。不同商品仍分開連線，原型最多 8 個商品。沒有修改原有 Python 下單、面板或倉位檔案，也沒有讀取帳戶金鑰。

| 驗收項目 | 結果 |
| --- | --- |
| fmt、Clippy `-D warnings`、release `--locked` | 通過 |
| Rust 測試 | 37 個通過；較上批新增 8 個快取／協議邊界測試、5 個本機 TCP WebSocket 測試 |
| Python 驗證工具測試 | 9 個通過，含錯誤來源時間／錯誤容量／少輸出必須驗證失敗 |
| 原有計算及行情回歸 | 571 計算、120 合成深度、208 既有真實錄製訊息全部通過；計算仍有原已記錄的 140 個除法末位表示差異，沒有放寬要求 |
| 新增共享行情回放 | 560 筆事件，兩所各 280；價格、可見數量、來源時間、新鮮度、完整數量 VWAP、最差成交價、深度不足完全一致，零不一致 |
| 原始來源清單 | 202 個來源／配置／測試／資產檔雜湊未變；更新 4 個部分移植項目的範圍描述 |

回放的 Python 參考直接使用 `QuoteStore`／`StoreFeed`。560 筆是 20 組、兩所各 14 個連續步驟，包含未變 BBO、變動數量、深度過期、容量恢復、斷線與重新建立快取；不是 560 種獨立策略情境，也不是收益回測。

Rust 額外更嚴格的行為獨立驗證：同時戳衝突保守採用單檔 BBO；價差回到原價也不復用失效深度；錯誤／零數量／空側／未來時間戳使快取失效；舊連線世代訊息、解析錯誤或斷線回報不影響新連線。覆核時發現「BBO 先抵達、同時戳深度後抵達」也需要保留失效標記，已修正並涵蓋兩種到達順序。這些刻意差異沒有混入完全等價的案例。

本機 WS 故障注入使用 loopback TCP WebSocket 伺服器，驗證：兩個訂閱者只建立一條連線、最後訂閱者離開釋放連線、伺服器 Close 先清空快取再重連並接受重置序號、畸形 HL 訊息觸發重連且重新訂閱兩種 channel、持續心跳不能掩蓋行情停滯、關閉時取消未完成握手。這比只呼叫狀態清除的單元測試更接近真實連線，但仍不代替公網長時間故障測試。

### 公開 WS 短測

使用最後驗收的 release 執行 `shared-probe --seconds 30`，四個消費者共用兩條 ETHUSDT／ETH 連線：

| 指標 | Aster | Hyperliquid |
| --- | --- | --- |
| 連線嘗試 | 1 | 1 |
| 接受深度快照 | 233 | 56 |
| 接受 BBO | 345 | 240 |
| watch 通知 | 497 | 258 |
| 每 100 ms 取樣的新鮮報價 | 296／300 | 292／300 |
| 不可用取樣（包含建立連線時段，未細分原因） | 4 | 8 |

兩所均收到深度及 BBO，無重連，正常結束且快取清空。通知數小於收包數是 `watch` 合併公共版本的預期行為；這種機制不可用於不允許丟失的私人成交。HL 本次 BBO 數高於 depth，證明已接到更頻密的頂層更新，但不代表深層流動性、網路延遲或兩所成交速度同幅改善。

### 證據及下一批

`validation/batch2b-shared.json` 保存 560 事件比對及來源／執行檔雜湊；`batch2b-shared-live.json` 保存公開短測與執行檔雜湊。`batch2b-core-regression.json`、`batch2b-depth-regression.json`、`batch2b-recording-regression.json` 保存本輪原有基準重跑結果，舊的 11 月 5 日報告保留。

本輪純計算批次均值中位數 Python 1.023 µs、Rust 0.0743 µs，仍約 13.8 倍；新增共享回放著重行為正確性，沒有為整個共享流程宣稱加速倍數。**沒有完成私人 WS、簽名、下單或對沖端到端量測。** 不能據此宣稱已減少實盤約 200 ms 延遲或已能搶先其他機器人。

下一小步 2b-2：先處理 Aster／Hyperliquid 商品識別、合約單位、tick／lot 精度及數量換算，再做多商品同連線訂閱與全域限流；其他 venue、跨所時間一致性、策略連接及長時間驗收仍列為待完成。

## 2026-11-09：2b-2a 商品識別、單位與精度

新增 `core::instruments`、`market-data::catalog`、`instrument-replay`、公開 metadata 錄製與 Python 比對工具。原專案 202 個來源／配置／測試／資產雜湊未變，所有重寫留在 `rust/`；沒有金鑰、帳戶查詢或下單。

| 項目 | 最終結果 |
| --- | --- |
| fmt、Clippy、release `--locked` | 全部通過 |
| Rust 測試 | 52 個通過；本小步新增 6 個精度／單位測試、9 個商品解析測試 |
| Python 驗證工具測試 | 12 個通過，包含錯誤 tick 必須失敗、原生 USD1 查詢的參考路徑 |
| 合成商品／精度比對 | 1,018 項：15 metadata、913 精度與限制、6 共同 lot、84 映射；全部精確一致 |
| 真實 metadata＋精度組合 | 3,767 項：2,764 商品查詢、同一組 913 精度／6 lot／84 映射；零不一致 |
| 活躍原生合約防漏檢查 | Aster v1／v3 共 1,288 個原生 row 全部納入；交易中的原生合約零誤拒絕 |
| 既有回歸 | 571 計算、120 合成深度、560 共享事件、208 真實訊息全部通過；原計算除法的 140 個末位差異仍按原規格記錄 |
| 中文商品公開 WS | 12 秒，Aster「哈基米USDT」63 筆深度、HL ETH 21 筆深度＋1 筆 ACK；85 則訊息與 Python 及錄製時結果完全一致，兩所零錯誤／重連 |

公開錄製僅各查詢一次 Aster v1、v3 exchangeInfo 與 HL meta，保存 644＋644＋235 個 row。完整組合的 2,764 商品查詢同時涵蓋 base symbol 優先選擇與所有原生 Aster 合約，沒有只驗 ETH／BTC 或忽略 USDC／USD1／U。2,499 個查詢成功、265 個按規則拒絕；數量包含不同查詢方式及 API 版本的重複商品，不是可套利配對數。兩組驗證共用精度案例，不應相加當成獨立案例數。

### 實作與驗收發現

- Aster 限價與市價數量規則分開保存，亦保留最大數量及價格上下限；缺失／錯誤欄位不以小數位數猜測。HL 整數價格例外、有效位數與小數位限制均逐值對照現有 Instrument；不涵蓋 adapter 預設 HALF_EVEN 格式化入口。
- 最低名義金額及價格容忍值的中間乘積可能超過 Decimal 的 28 小數位。最初比對暴露了這個限制；改用 checked `u128` 係數＋scale 保存精確中間值，未刪除數值案例或放寬門檻。測試輸入的科學記號改用同值固定小數文字，符合 wire 格式。
- 完整資料發現 5 個中文名稱商品，修正先前只接受英數字的限制並對 URL 做 UTF-8 編碼；單深度 WS 公網短測及原始訊息回放均通過。
- 拒絕原因二次稽核發現 29 個 USD1／U 活躍原生合約（兩個 API 版本共 58 row）。舊 Python base resolver 未涵蓋這兩種後綴。Rust 明確原生名稱可解析這些規格，保留報價幣／結算幣；base fallback 仍照舊。驗證時把原生 row 注入 Python 現有 `_aster` filter parser，比對規格解析；不把舊 resolver 的缺項宣稱為原有等價行為。
- `1000PEPE／kPEPE` 的 canonical 名稱不能證明單位相同。預設阻擋未知單位的共同 lot 計算；外部核實倍率後才允許精確換算。名稱相同亦不能取代標的、報價幣或費用審核。

### 範圍與證據

此批是靜態規格及解析驗收，沒有移植完整下單前驗證器、動態百分比價格限制、reduce-only 例外、metadata 刷新、HIP-3、其他 venue 或交易協調。共同 lot 只處理限價數量步長，市價腿需使用自身規則。低於步長的取整可得到零，必須另經 `price_error`／`size_error`；不能因這些檢查而丟棄已成交 Maker 的對沖義務。

封存報告為 `validation/batch2b2-instruments-synthetic.json`、`batch2b2-instruments-live.json`、`batch2b2-unicode.json`，包含資料、來源及執行檔雜湊；原始錄製保留在 `artifacts/instruments-20261109-01.json`、`artifacts/instruments-unicode-ws-20261109-01.jsonl`。原有回歸另存 `batch2b2-core-regression.json`、`batch2b2-depth-regression.json`、`batch2b2-shared-regression.json`、`batch2b2-recording-regression.json`。

本批未為商品解析做獨立效能 benchmark；既有微基準有本機負載波動，其比率變動不當成此批效能改善。沒有收益回測或私人成交至對沖送單量測。

下一小步 2b-2b 先建立 Rust 公共 metadata 抓取／快取刷新與共用限流，再處理多商品同 WS 訂閱；更新規格失敗不能悄悄保留過期交易規則，並需測試 429、重試、刷新競態與訂閱交接。

## 2026-11-10：2b-2b 公共商品 HTTP、快取刷新與共用限流

新增 `market-data::metadata`、27 個本機／虛擬時鐘測試及 `metadata-probe`。HTTP 固定為 Aster v1／v3 exchangeInfo、Hyperliquid 主 dex meta；每個 runtime 的 clones 共用連線池、每所滾動權重及冷卻。`MetadataCache` 以一個刷新鎖合併重疊請求，原子發布 generation；平常讀取無網路，刷新失敗及過期明確拒用。背景刷新可停止並回收。詳細預設值與使用方式見 README。

| 項目 | 結果 |
| --- | --- |
| fmt、Clippy `-D warnings`、release `--locked` | 全部通過；probe 加入緩衝寫檔後另通過 Clippy 及 release 編譯 |
| Rust 測試 | 79 個通過，包含新增 27 個 HTTP／快取／限流測試 |
| Python 驗證器測試 | 12 個通過 |
| 合成商品／精度 | 1,018 項，零不一致；資料集雜湊與前批相同 |
| 前批 metadata 回歸 | 3,767 項，零不一致；2,499 成功／265 按規則拒絕，與前批相同 |
| Rust 新抓取 metadata 比對 | 3,767 項，零不一致；2,491 成功／273 按規則拒絕；活躍原生 Aster 合約零誤拒 |
| 原有計算／行情回歸 | 571 計算、120 合成深度、560 共享事件、208 ETH 及 85 中文商品錄製訊息通過；除法仍保留原有 140 個末位表示差異與原容差 |
| 原專案清單 | 202 個來源／設定／測試／資產雜湊未變；只更新 Rust 目錄內的移植範圍描述 |

### HTTP 與快取的實網驗證

最終錄製為 `artifacts/metadata-20261110-02.json`，由最終 `metadata-probe.exe` 產生。Aster v1 644、v3 644、HL 235 rows，各一個 HTTP attempt 成功，無重試。各自後續 1,000 次 `current()` 及 20 次未到刷新時間的 `refresh(false)` 共 1,020 次操作，額外請求皆為零；三個來源均可解析 ETH 並驗證版本仍有效。首份錄製 `metadata-20261110-01.json` 也通過，最後一版改用 BufWriter／明確 flush，避免逐小段寫檔；只各追加一次公開查詢，沒有壓測真實服務。

兩日快照逐項檢查發現 Aster `DELTAUSDT`、`APMUSDT` 由 TRADING 轉為 SETTLING。因此兩版 API 的原生／base 查詢共多出 8 個拒絕案例；這是商品狀態變更，不是解析回歸。新舊錄製都由現有 Python production parser 與 Rust 對照，所有活躍原生 row 均覆蓋。3,767 是商品查詢與精度等案例的組合，包含重複商品與兩份資料集共用的計算案例，不能解讀為套利配對數或獨立樣本相加。

最終首次載入約 Aster v1 907 ms、v3 425 ms、HL 259 ms，包含網路與解析；只各一筆，不能當成可靠的網路延遲分布。snapshot 記憶體讀取的 p50 約 100 ns、p99 100–200 ns，接近本機時鐘解析度，僅作診斷，排除商品解析、簽名、下單及撮合。沒有把此值換算成實盤加速倍數；本批尚無成交至對沖的延遲或收益回測。

### 故障與競態覆蓋

本機 TCP HTTP 替身驗證正確 GET／POST、公開 body／無認證欄位、快取命中、20 個並行刷新只取一次、並行失敗共用結果、精度／asset index／下架原子更換、失敗後立即拒用舊規則、失敗退避、到期拒用、取消刷新後可恢復、週期刷新與停止／中途關閉。單一錯誤商品拒絕解析，但不遮蔽其他商品更新；壞 JSON、缺少／空清單、重複身分、錯誤 collateral、轉址及 Content-Length／chunked 超大回應均拒絕。

限流案例驗證滾動權重、取消排隊不消耗額度、Aster v1/v3 共用 429 冷卻、其他 venue 不受其冷卻影響、秒數／HTTP 日期 Retry-After、429／503 退避後成功、418 不自動重試、伺服器長等待超過呼叫 deadline 後仍保留、缺失 Retry-After／Aster IP 使用量標頭、5xx／連線失敗重試上限、慢回應總逾時。超出本機 Instant 範圍的 Retry-After 永久阻擋該預算而不 panic、不縮短等待。

首輪完整測試有一項 Windows 連線拒絕案例先碰到整體 deadline，無法驗證三次重試。將該測試單次 request timeout 設為 50 ms，保留重試三次的斷言；另有獨立測試驗證總 deadline。正式 15／45 秒預設不變。最後 79 個測試全部通過。真實交易所未刻意觸發 429／418，這些結果來自本機故障注入。

### 範圍與封存

共用限流只涵蓋同一 `PublicHttp` 與其 clones；不同實例、Python 程序或同 IP 的其他裝置不共用預算。Aster 使用量標頭只提供保守補充，沒有協調整個 IP。背景刷新沒有磁碟持久化；generation 是本機發布序號，不能證明 HTTP 回應是交易所最新版本。定時刷新也不能立即得知兩次刷新之間的規格變更。這些是公共讀取基礎設施，**尚未接入策略或下單**；快取／精度檢查不可攔截已成交 Maker 的對沖義務。

封存摘要 `validation/batch2b2b-metadata.json` 記錄本批測試數、公開抓取結果、來源／Cargo.lock／binary／recording 雜湊。解析比對與回歸報告為 `batch2b2b-instruments-{synthetic,live,regression}.json`、`batch2b2b-{core,depth,shared,recording,unicode}-regression.json`，與前批檔案分開保留。

下一小步 2b-2c 為同連線多商品訂閱、WS 請求限流及商品規格更新後的訂閱交接；其他 venue、長時間運作、跨所時間一致性與私人 WS／交易執行仍按後續批次進行。

## 2026-11-10：2b-2c 多商品共用 WS、限流及訂閱交接

`MarketHub` 由每商品一條連線改成每 venue 一條，新增 `multiplex.rs` 的單一狀態擁有者，保留 `Subscription` 的 watch／Arc 報價介面。Aster raw WS 動態訂閱 partial depth＋bookTicker；HL l2Book fast＋bbo。每 venue 上限 32 商品，原生名稱分流、各商品序號／可用性／停滯檢查、ACK 對應、控制與心跳額度、重連預算與生命週期交接均分開管理。商品 JSON 不重複解析；規格只在新版本或新商品時解析，日常行情檢查快取指標／有效性。

| 項目 | 驗收結果 |
| --- | --- |
| fmt、Clippy `-D warnings`、release `--locked` | 全部通過 |
| Rust 測試 | 97 個通過；原 5 個單商品 hub 測試以新版 23 個多商品／限流／交接測試取代，涵蓋原有共享、斷線、錯誤訊息、停滯、停止行為，總數淨增加 18 |
| Python 驗證工具 | 12 個通過 |
| 既有純計算 | 571 案例通過；原有 140 個除法末位差異與容差保持不變 |
| 深度／共享快取回放 | 120 合成深度、560 共享事件，零不一致 |
| 原始實網行情回放 | 208 ETH 訊息、85 中文商品訊息，Python／Rust 與錄製時結果一致 |
| 商品規格回歸 | 1,018 合成、前批 Rust 公開錄製的 3,767 組合案例全部一致；活躍原生合約零誤拒 |
| 原 Python 專案 | 202 個來源／設定／測試／資產雜湊未變；只更新 Rust 移植範圍描述 |

### 四市場實網交接

最終 `multiplex-probe` 分 5／8／5 秒進行：先訂閱兩所 ETH，再加入兩所 BTC，最後將 ETH 消費者交接至 BTC。Aster v3／HL meta 各公開讀取一次，綁定快取後才啟動 WS；無帳戶或下單操作。最終摘要 `artifacts/multiplex-20261110-02.json`：

| 結果 | Aster | Hyperliquid |
| --- | --- | --- |
| 全程連線嘗試／成功使用的 socket | 1 | 1 |
| 控制訊息／ACK | 3／3 | 6／6 |
| 故障／需重連的交接 | 0／0 | 0／0 |
| 累計接受的行情 | 514 | 265 |
| 確認前／撤訂後等不適用行情 | 2 | 2 |
| 最後剩餘商品 | BTCUSDT | BTC |

四市場階段的快照：Aster ETH 有 75 depth／128 BBO、BTC 有 39／166；HL ETH 有 23／103、BTC 有 14／66，四者皆曾且在取樣當下提供新鮮報價。ETH 撤訂後 BTC 的四位消費者都有持續新鮮樣本；shutdown 後全部報價清空。一般消費者交接不需重連，因此 `handovers=0`；該欄位只計同名／metadata 世代隔離造成的 socket 重建。前一份 `multiplex-20261110-01.json` 亦通過，但最終證據使用加入離線切換清理後的最終 binary 與第二份報告。

這是短期連線／協議驗證，不是全天、滿 32 商品或高負載壓測。watch 樣本會合併，不能當逐筆資料量；上述接受數亦非交易成交數。沒有量測下單、對沖或收益，原純計算微基準的浮動不歸因於本批 WS 改動。

### 本機故障及安全交接

23 個新版測試涵蓋：雙商品／多消費者單 socket、HL 原生大小寫及 fast ACK、ACK 前行情不發布、錯配／重複／錯誤 ACK、ACK 逾時、動態加入與撤訂、撤訂前尚未確認的 subscribe、最後消費者與握手取消、直接 drop hub、斷線清空全部市場並重設序號、BTC 活躍及心跳不能掩蓋 ETH 停滯、限流等待不阻塞既有市場讀取、中文商品控制訊息、ping/pong、訊息預留額度、5／分鐘重連預算、32 商品容量及釋放、999 次離線切換後無殘留商品。

metadata 測試以本機 HTTP＋WS 串接：刷新精度版本後必須重建 socket 再接受報價；HTTP 失敗時停止全部，恢復後僅重訂仍有效商品；ETH 下架後即使舊行情再到也不恢復；規格過期後清空且不由 WS task 偷發 HTTP。當同一原生名稱移除再加入，舊 socket 的延遲行情不能進新 view。

Windows 並行測試曾在啟動階段先碰到刻意縮短的 ACK／過期計時，測不到原本想驗證的停滯狀態。替身 socket 改用 TCP_NODELAY、一般 ACK 測試留 2 秒；過期案例在初次有效報價後暫停 Tokio 時鐘再推進 31 秒，保持明確到期斷言。正式 5 秒 ACK／15 秒資料 watchdog 未放寬。最後完整測試皆通過。

### 範圍、取捨與證據

限流只共用於同一 hub，包含 Aster／HL 的控制、主動心跳、pong 與連線嘗試，跨 reconnect 保留額度；不協調 Python、其他 hub、舊錄製器或同 IP 的其他主機。沒有刻意觸發真實交易所限流。一般 limiter／ACK 等待繼續接收行情；socket 寫入最長仍可等待 2 秒，不宣稱硬即時。行情事件用 watch 保留最新版本，不適用私人成交。

metadata 健康／版本在每次行情發布前檢查，無行情時每 50 ms 檢查。報價 view 不附帶交易所原子鎖，後續策略仍須在新增風險前重查 metadata 與價格；既有 Maker 已成交的必要對沖不能由這個公共資料閘門攔截。相同名稱重新訂閱、metadata 新版本、任一市場停滯或嚴重協議錯誤會清空整個 venue 並重連，其他商品會短暫受影響；這是隔離無訂閱世代標記行情的保守選擇。

封存 `validation/batch2b2c-multiplex-live.json` 與 `batch2b2c-summary.json`，後者含來源、工具鏈、binary、錄製／摘要及各回歸報告雜湊。回歸另存 `batch2b2c-{core,depth,shared,recording,unicode}-regression.json`、`batch2b2c-instruments-{synthetic,regression}.json`。原始資料位於既有 artifacts 路徑；live probe 只保存摘要，未錄製所有本輪原始 WS。

下一小步 2b-2d 為其他 venue、長時間錄製與恢復、跨所時間一致性及負載驗收；私人 WS、策略、UI 與交易執行仍未切換。

## 批次 3a：私人事件協調核心（2026-11-10）

本批依使用者選擇先進行私人事件協調；2b-2d 仍待後續。新增獨立 `perp-arb-private-events` crate，涵蓋 Aster WS／REST 累計訂單訊息解析、單一 actor、每單狀態及消費者 cursor。**本批沒有連線真實私人 WS、HTTP 查單、金鑰載入、簽名、送單、對沖或倉位恢復；不得以此驗收認定 Rust 能交易。**

最終執行 `powershell -NoProfile -File rust/tools/check.ps1 -Format` 通過：

| 驗證 | 結果 |
| --- | --- |
| Rust workspace | 131 個測試通過，其中 34 個為新私人事件測試 |
| 品質／編譯 | fmt、Clippy all-targets `-D warnings`、release `--locked` 通過 |
| Python 驗證器本身 | 14 個測試通過，包含新增兩項私人事件驗證器測試 |
| 私人事件共同回放 | 100 張委託、900 筆訂單事件＋1 筆無關事件，共 901 筆；與實際 Python `OrderEventStream` 相同，零不符 |
| 刻意差異 | 兩項「較舊時間戳但累計成交增加」案例，Rust 保留新增量；不混入共同案例數 |
| 既有回歸 | 571 計算、120 合成深度、560 共用快取、1,018 商品規格案例全部通過；維持原計算除法容差 |
| 移植清單 | 202 個原始來源／設定／測試／資產檔雜湊相符；僅更新 `order_stream.py` 的部分移植範圍描述 |

### 成交與狀態語意

34 個測試涵蓋 ACK 前成交保存、ACK 非成交、WS／REST 兩種到達順序、獨立訂單／商品／帳戶／venue／actor instance、零成交取消、撤單後遲到成交、終態不回退、部分成交、斷線及舊 socket、查詢發出後斷線／新事件到達、重連需對帳、REST fallback 先於 WS、REST 更新時間與 WS 發送時間的差別、容量上限、精確十進位及不可表示差量拒絕、無效格式／數量／狀態、listenKey 過期、慢消費者、並行來源、有界入口等待、caller 取消後已入列命令仍處理、shutdown 排空、producer 全部 drop、worker abort 撤銷健康狀態。另在一個測試內窮舉六則事件的 **720 種排列**，每次最終新增量總和都是 10，終態皆為 FILLED。

每單只保存累計量，`FillCursor` 用尚未記帳的差量通知下游；不同觀察者不互相消耗事件。watch 可合併中間快照，仍保留最終累計與狀態，並非逐筆交易紀錄。記帳亦不等於對沖已成交；下游必須另記錄責任量、送出量及確認量。本批沒有實作該執行器。

有界 mpsc 在入口滿載時等待，已入列的事件不因呼叫者取消而消失。停止時排空已入列命令並撤銷健康狀態。這只描述程序運作中的協調：強制終止／崩潰尚無磁碟恢復能力。每單去重狀態不自動淘汰；order capacity 滿載會回報錯誤且留下 sticky fault，既有成交仍保留，未知新單必須經後續帳戶全面對帳恢復。不能把這個限制說成任意負載或崩潰下都不丟資料。

一般更新只通知對應訂單，健康計數為常數時間讀取；缺口才通知所有訂單。事件使用本機 `Instant` 記錄進入解碼器及套用時間，交易所時間另存。不在 actor 內執行慢 REST 或對沖 I/O。

### Python 差異、證據與下一批

Python `OrderEventStream.publish` 遇到較舊事件時間會先返回。例如已知累計 2，後到較舊時間的累計 4，Python 保留 2；Rust 保存 4 並產生新增 2。取消後遇到這類訊息，Rust 同時保留取消終態、標記需要查單。這是避免隱藏已知成交的刻意差異，未修改現有 Python 行為。其他刻意差異包括不做 1,024 筆 LRU 淘汰、解析器更嚴格、每帳戶統一協調 WS／REST，以及明確的查單與連線 token。

協議核對使用 [Aster 官方私人事件格式](https://asterdex.github.io/aster-api-website/futures/user-data-streams/#event-order-update)，`z` 為累計量；未用真實帳戶產生測試成交。Hyperliquid `userFills` 的逐筆量／歷史快照不套入這個累計解碼器，其私人適配器尚未實作。

封存 `validation/batch3a-private-parity.json`、`batch3a-{core,depth,shared,instruments}-regression.json`、`batch3a-check.log` 與 `batch3a-summary.json`。摘要包含最終來源、binary、工具鏈及回歸報告雜湊。這些是合成事件／正確性回放，並非收益回測或實際對沖延遲基準；舊計算微基準結果不歸因於本批改動。

下一批 3b 先接 Aster listenKey、私人 socket 生命週期與獨立 REST 查單排程，再處理 Hyperliquid 的私人事件與快照。client ID／未知送單、持久化、去重狀態清理、重啟與帳戶對帳需配合階段 4／6。Python 正式下單入口尚未切換，所有新增內容留在 `rust/`。

## 批次 3b-1：Aster 私人 WS 與獨立查單（2026-11-11）

新增 `perp-arb-aster-private`，完成 v3 EIP-712 認證、listenKey 建立／續期、私人 socket 生命週期與獨立 REST 查單排程，接入上一批單一事件協調器。只提供 POST／PUT listenKey 與 GET order，沒有交易寫入 API。驗收使用本機 HTTP／WS 替身及公開固定測試金鑰，**未讀取真實金鑰、未連接真實私人帳戶、未下單**。唯讀 probe 已可編譯；本批只執行其 `--help`，真實帳戶驗收仍待下一步。

最終 `powershell -NoProfile -File rust/tools/check.ps1 -Format` 全部通過：

| 驗證 | 結果 |
| --- | --- |
| Rust workspace | 163 個測試；新增 30 個 Aster transport 測試、2 個 probe 參數測試，原 34 個私人協調測試保留 |
| 品質／編譯 | fmt、Clippy all-targets `-D warnings`、release `--locked` 通過 |
| Python 驗證器 | 14 個測試通過 |
| Aster 認證 | 4 組固定向量，由實際 Python adapter 重算；Rust digest、簽名及 form encoding 完全一致 |
| 私人事件回歸 | 901 共同事件相符；保留原兩項較舊時間戳新增成交的刻意差異 |
| 既有回歸 | 571 計算、120 深度、560 共用快取、1,018 商品規格案例通過；維持既有除法容差 |
| 原始來源清單 | 202 個來源／設定／測試／資產雜湊相符；Python 正式程式未變 |

### 故障注入與排程

測試涵蓋實際 loopback HTTP 簽名與方法白名單、同 signer 跨 credentials 的 nonce 協調、時間回退、429／418／503 冷卻、重導拒絕、回覆大小與金鑰格式限制、慢 REST／慢續期期間仍接收成交、斷線與 listenKey 過期、正常／失敗續期、有效期限及定期換線、缺失或錯誤 Pong、伺服器 Ping、控制訊息與重連限流、重複 transport 阻擋、錯帳戶、401、無效 WS、查單逾時／找不到單，以及停止／Drop 生命週期。

查單錯誤不合成零成交或拒單。停止時排空已排程查詢，完整 HTTP 回覆進入 coordinator 不再受 HTTP timeout 取消。慢查單／續期的測試以 350 ms 替身延遲確認 WS 累計量仍先更新；這驗證工作分離，**不是實網延遲或收益回測**。

正常每單成功查詢後等待 5 秒；斷線／有缺口時目標 500 ms；失敗從 500 ms 指數退避至 60 秒，均受每分鐘 120 次共用子配額限制。重連時活動訂單優先於保留的終態紀錄，各級內按到期時間排程。測試確認 active ID 999 先於六張舊終態查核，也確認空檔會處理舊紀錄。大量訂單不保證每單 500 ms；歷史終態仍需逐一核對，可能延後整體健康恢復。

開發中修正替身 socket 無啟用 select 分支的 panic、Clippy 條件格式，以及優先級測試誤以總 GET 次數代表全部舊單已查詢的競態；最終測試直接等待各訂單都被查核。正式時限未因測試放寬。

### 使用界線與封存

`aster-private-probe` 必須明確指定 `--live-read-only` 才讀取程序環境與連線，不自動讀 `.env`。支援追蹤既有 order ID、Ctrl+C 與統計摘要，不輸出 listenKey、簽名 URL 或原始帳戶資料。停止不 DELETE 可能由同帳戶其他 client 共用的 listenKey。詳細預設與使用方法見 [Aster transport 契約](crates/aster-private/README.md)。

目前只恢復已追蹤訂單，不掃描完整帳戶；未完成 client ID／未知送單、持久化、崩潰恢復、終態清理、Hyperliquid 私人事件及交易執行。限流只涵蓋同一 private client 的 clones，nonce 只在同程序同 signer 共用，尚未協調 Python 或其他程序。故障期間仍保留已知成交，健康旗標不能用作阻止已成交 Maker 必要對沖的理由。

封存 `validation/batch3b1-summary.json`、`batch3b1-check.log`、`batch3b1-auth-validation.json` 與 `batch3b1-{private,core,depth,shared,instruments}-regression.json`；摘要包含來源／Cargo.lock／工具鏈／binary／報告雜湊。舊 3a 封存保持不變，日常私人回歸改輸出到 artifacts。下一小步為 Aster 真實帳戶唯讀驗收與長時間恢復驗證，再處理 Hyperliquid 私人事件；Rust 仍不能下單。

## 批次 3b-2：兩所唯讀實網與 Hyperliquid 私人事件（2026-11-11）

本批依使用者指示，先驗證 Aster 真實帳戶唯讀連線與受控恢復，再新增 Hyperliquid 私人 WS、逐筆成交去重與獨立查單。所有程式留在 `rust/`，沒有下單、撤單、轉帳、DELETE 共用 listenKey 或改動倉位檔案。帳戶設定透過測試啟動程序從既有環境／專案設定取出所需欄位，沒有印出金鑰、地址、listenKey 或原始帳戶封包。

### 最終實網結果

兩個 release probe 同時各觀察 90 秒，每 25 秒主動關閉自己的 socket，再由正常重連流程恢復。Aster 另每 10 秒續期。這是受控換線，沒有切斷整台機器網路、破壞其他程式的訂閱或製造交易。

| 項目 | Aster | Hyperliquid |
| --- | --- | --- |
| 連線嘗試／成功 | 4／4 | 4／4 |
| 續期 | 6 | 不適用 |
| 訂閱 ACK／歷史快照 | 不適用 | 8／4 |
| 成交事件 | 0 | 首次觀察 30 筆，重播去重 90 筆 |
| 新 `orderUpdates` | 不適用 | 0 |
| 成功查單回覆 | 0 | 88，包含重新核對及輪詢，並非 88 張不同訂單 |
| 查單失敗 | 0 | 31，全部為 `unknownOid` |
| 已建立的訂單 view | 0 | 13 |
| 結束前累計 view 待核對／逐筆待核對 | 0／不適用 | 0／1 |
| transport 錯誤／sticky fault | 0／無 | 0／無 |
| 正常排空停止 | 是 | 是 |
| probe 退出碼 | 0 | 1：完整對帳尚未完成 |

**Aster 短期認證、續期及連線恢復通過。** 期間沒有成交，也沒有可供本次測試直接追蹤的既有 order ID，因此未實測真實 Aster GET order 或斷線期間的成交補漏；這些仍由本機故障測試覆蓋，不能寫成實網已通過。

**Hyperliquid transport 恢復與本次快照去重通過，完整訂單恢復未通過。** 仍有一張歷史成交對應的 orderStatus 回傳 `unknownOid`。`transport_recovery_passed=true`，`order_reconciliation_complete=false`，保留 `unresolved_fill_orders=1`，嚴格恢復 probe 正確以非零退出。沒有以忽略這筆紀錄、清空 ledger 或把回覆改成零成交取得綠燈。

補充唯讀診斷中，REST userFills 提供 464 筆紀錄、179 個不同 oid，其中 176 個可查回、3 個 unknown；unknown 紀錄的顯示方向包含 ADL、結算與現貨零碎資產轉換。WS 初始快照只提供 30 筆，不能據此視為全歷史。此診斷說明帳戶變動不全都能由普通委託查詢恢復，**沒有據顯示方向自動排除任何成交**，也未斷言 WS 的單一 unknown 必定是哪一種類型。完整帳戶／持倉及非普通委託事件對帳留待 3c。

上述均為短期唯讀驗證；本輪沒有新成交，無法量測撮合至本機、對沖成交延遲或收益。快照重播驗證不等於主機崩潰後恢復，沒有全天或滿容量負載驗收。原始私有資料不落盤，只保存統計摘要。

### 實作與離線／loopback 驗收

新增 `hyperliquid-private` crate，固定地址 WS 及 `/info` orderStatus，沒有私鑰或交易端點。`private-events::hyperliquid` 以原生 coin／oid／tid 去重逐筆成交，累計訂單則由 `orderUpdates`／REST 的 `origSz - sz` 更新，兩者分開保存，避免把重疊歷史再次加到累計量。新成交提示會要求獨立查單，不冒充完整對沖差量。ACK 前使用未啟用 Session 保存資料，兩種訂閱 ACK 與初始快照到齊才啟用；斷線保留量，停止排空已排程查單。

最終執行 `powershell -NoProfile -File rust/tools/check.ps1 -Format` 全部通過，該腳本不連接真實帳戶：

| 驗證 | 結果 |
| --- | --- |
| Rust workspace | 195 個測試：新增 HL 核心 17、transport 12、probe 2，以及 Aster 恢復 CLI 1；原 163 保留 |
| 品質／建置 | fmt、Clippy all-targets `-D warnings`、release `--locked` 通過 |
| Python 驗證器 | 16 個測試通過，新增 2 個比對器反例測試 |
| HL 跨語言比對 | 601 案例，實際 Python `get_order_execution` 與 Rust 的精確數量、order ID、終態／unknown 一致 |
| Aster 回歸 | 901 筆共同事件、2 項既有刻意差異及 4 組認證向量通過 |
| 既有公共／計算回歸 | 571 計算、120 深度、560 共用行情、1,018 商品規格通過；維持既有除法容差 |
| 來源清單 | 202 個 Python 來源／設定／測試／資產雜湊相符 |

HL 核心測試涵蓋大整數 ID／原生大小寫、終態分類、無效量及狀態、初始／重連快照重播、填單資訊先於原始委託量、REST 與逐筆重疊、查單中有新成交、舊回覆、取消後成交、衝突 ID、容量上限不淘汰、不同 oid／coin 相同 tid、錯帳戶／actor、舊世代、unknownOid、六種重排、ACK 前狀態及不可表示的十進位加總。transport 測試覆蓋雙訂閱／非聚合參數、慢查單仍收成交與 Pong、斷線去重、缺 ACK／快照、錯 ACK、心跳逾時、重複 transport、停止排空、unknownOid、共用冷卻、無效／過大 HTTP 及 Drop 清理。

測試揭露過一個實作缺口：拿已合併 view 的最大時間檢查 REST 新鮮度，會掩蓋本次回覆較舊。已改用本次回覆原始時間，反例通過。編譯時補明確型別及修正 Clippy 條件格式；最終沒有未解決的編譯／測試失敗。實網 unknownOid 保留為外部資料缺口，不混入單元測試通過的結論。

### 剩餘工作與證據

下一步是帳戶／持倉對帳與非普通委託變動處理，以及 unknown／client ID、持久化／重啟、去重狀態清理。尚未接 Rust 交易執行器；只收到 `userFills` 時，本批要求查單確認累計，並未實作可立即對沖的完整責任量模型。健康狀態不可當作阻擋已成交 Maker 必要對沖的條件。

兩所預設及配額詳見 [Aster 契約](crates/aster-private/README.md) 與 [HL 契約](crates/hyperliquid-private/README.md)。HL 查單每分鐘 120 次的子配額只共用於同一 client clones，不涵蓋 Python／公共行情或其他程序；大量歷史與持續 unknown 會受限，無低於 100 ms 的實盤保證。

封存 `validation/batch3b2-summary.json`、`batch3b2-check.log`、兩份 `batch3b2-{aster,hyperliquid}-recovery-final.json`、`batch3b2-hyperliquid-private-validation.json` 及 `batch3b2-{core,depth,shared,instruments,private,auth}-regression.json`。摘要包含最終 source／lockfile／binary／報告雜湊；兩份實網報告亦各含當時執行檔雜湊。舊批次封存保持原狀。
