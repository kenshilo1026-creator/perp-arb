# 私人事件協調：3a 至 3c-1

這個 library 接收已取得的 Aster／Hyperliquid WS 訊息及 REST 查單回覆，提供每單累計成交快照。核心本身沒有網路、金鑰讀取、下單或對沖入口。[aster-private](../aster-private/README.md) 與 [hyperliquid-private](../hyperliquid-private/README.md) 已另行接上私人連線及獨立查單；訂單送出仍未實作。

3b-2 新增 Hyperliquid 專用正規化及逐筆成交保留。`userFills` 的觀察總量與 `orderUpdates`／REST 累計量分開，不能相加當對沖責任。ACK 前使用 `connecting()` Session 保存資料，兩種訂閱 ACK 與初始快照到齊才 `activate()`；舊 Session 不能啟用新世代。`Health.unresolved_fill_orders` 包含尚待查單核對的逐筆紀錄，即使沒有原始委託量、尚未建立累計訂單 view 也不會被漏掉。詳見 HL transport 契約。

## 使用順序與責任

1. 每個 venue／account label 建立一個 `actor::channel(scope, order_capacity, ingress_capacity)`，啟動 `worker.spawn()`；同一帳戶只由一個 coordinator 管理。
2. 私人 transport 確認可用後呼叫 `connected()` 取得 `Session`，每條連線持有自己的 token。舊連線 callback 不可借用新 token。
3. `ingest_aster_ws(session, raw).await` 接收 WS；與獨立 REST 查詢工作分開執行，不等對沖 HTTP 回覆才接收成交。已知 order ID 可先 `register`，HTTP ACK 前到達的 WS 會自行建立快取；ACK 後再次 `register` 不會歸零成交。尚未取得 order ID 的請求／client ID 關聯不在本批。
4. `subscribe(key)` 後先讀 `borrow_and_update()`，再等待 `changed()`。watch 合併的是**累計快照**；不能將其當逐筆成交紀錄。`FillCursor.pending` 算出尚未記帳的新增量，業務層接管並保存後才 `acknowledge`。每個讀者有自己的 cursor；是否已送出／已成交的對沖量屬下一批執行器，不由這個 cursor 假定完成。
5. 開始每次查單**之前**呼叫 `begin_query(key)`，把 ticket 隨回覆交回 `ingest_aster_rest`。舊查詢仍可補上更高成交量，但不能解除較新的斷線／事件缺口。查單失敗或逾時不能合成拒單或零成交，重送與反向交易亦不在本批。
6. 斷線呼叫 `disconnected(session)`，保留所有成交並標記需對帳。重連不自動解除此標記。`Health.ready_for_new_risk` 只檢查已追蹤訂單，是必要條件，不是完整交易許可；尚未檢查帳戶完整委託清單、倉位、風控或重啟狀態。此健康狀態**不可用來阻擋已知成交的必要對沖**。
7. 停止時 `shutdown().await`，再等待 worker JoinHandle；已入列命令會排空。直接中止 worker 會撤銷健康狀態，但沒有磁碟 journal，程序崩潰／強制中止後仍需下一批的持久化恢復，不能宣稱 exactly-once 交易。

## 正確性與容量

- `Scope` 區分交易所及帳戶，`OrderKey` 區分原生商品及 order ID。不同訂單的累計量不可相減；相同 ID 但原始數量變動會報衝突，尚不支援原地改量。
- ACK 不代表成交；零成交的取消仍可觀察。WS／REST 重複回報不會使 cursor 重複產生新增量。FILLED 不會退回 NEW。
- 舊事件含較高累計量時仍接收新增量；取消後收到較高成交量會保留終態並要求查單。舊 socket 的已知成交也不能直接當成公共行情般丟棄。
- ingress 為有界 mpsc，滿載時等待容量，沒有 `try_send` 後丟包。呼叫者取消尚未入列的 send 時仍須自行保留事件；已入列事件不因 caller future 取消而撤銷。慢 watch 消費者不阻塞事件處理，也不會漏掉最後累計量；不保證保留每筆交易明細。
- 每單狀態全程保留，不做 LRU 或自動終態刪除，避免延遲事件被當成新單重複對沖。達到設定 order capacity 時明確回報 `Capacity`、保留舊資料並鎖住健康狀態；新未知單需帳戶全面恢復。安全清理、tombstone、持久化與自動解除 sticky fault 尚未實作。不要用重建空 ledger 當作恢復。
- Aster 一般事件只更新該單 watch，健康計數不掃描所有訂單；缺口／停止通知全體。Hyperliquid 批次訊息目前會發布全部已訂閱訂單 view，尚未做大量帳戶訂單負載優化；不要據短測推論滿容量的延遲。數量只用精確十進位字串，不能表示的差量回傳錯誤，不靜默四捨五入。
- 保留本機 `Instant` 收到／套用時間，與交易所 `E` 分開。未量測真實 WS 收包至對沖下單、網路、撮合或收益。
- `ingest_aster_ws` 記錄進入解碼器前的時間；3b-1 transport 使用 `ingest_aster_ws_at` 傳入 socket 取出文字訊息時的本機時間。REST `updateTime` 與 WS `E` 不當成同一個版本序號比較；查詢的 instance／generation／revision token、累計量與終態共同決定能否解除缺口。
- Aster 解碼只涵蓋一般委託狀態及累計數量，未知狀態、清算特例或不完整資料會要求恢復；成交均價、費用、方向及持倉的完整交易模型留待後續批次。

## 驗證與來源

從專案根目錄執行：

```powershell
cargo test --manifest-path rust/Cargo.toml -p perp-arb-private-events --locked
cargo build --manifest-path rust/Cargo.toml -p perp-arb-private-events --release --locked
python rust/tools/verify_private.py
```

`private-replay` 接收 stdin JSON array（Aster 訊息），輸出累計數量、狀態及新增量；只是離線工具。完整回歸仍使用 `powershell -File rust/tools/check.ps1 -Format`。

協議依據：[Aster 訂單事件與 listenKey 說明](https://asterdex.github.io/aster-api-website/futures/user-data-streams/#event-order-update)。Aster 的 `z` 是累計成交量，`E` 是事件時間；官方提醒訊息可能亂序。Rust 與原本 Python 的刻意差異：較舊時間戳若包含更高累計量，仍保留數量；狀態和恢復資格分開判斷。

Hyperliquid 的 `orderUpdates`／`userFills` 與 `isSnapshot` 需獨立設計，不能把逐筆數量直接套進 Aster 累計欄位；見 [官方訂閱格式](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)。本批沒有宣稱支援其私人事件。
# 3c-1 補充：帳戶對帳與特殊事件（2026-11-12）

`account::Inventory` 保留每個原生商品與持倉方向，Aster LONG／SHORT 不相抵；HL HIP-3 dex 明確列入覆蓋範圍，現貨以 numeric token ID 區分同名資產。錢包／raw USD 與現貨 hold 分開；不以會隨市價變動的未實現盈虧充當庫存。

`begin_account_query()` 的票據綁定協調器實例、帳戶、連線世代、帳戶版本與開始時間。重連、成交、查單、Aster 帳戶事件、HL funding／非 funding 流水會使舊票據或報告失效。比對要求持倉、餘額、錢包與掛單完全一致，沒有數值容差；缺欄位不是零。無獨立 `expected` 時只有觀察結果，不能允許新增風險。開始新對帳亦會使舊報告失效。

HL `dir` 的 ADL／Settlement／Spot Dust Conversion 文字與 liquidation 結構只作路由提示：特殊成交完整保存在去重帳本，列入 `external_pending` 並阻止新增風險，不偽造成委託成交，也不無限用 `orderStatus` 查它。一般 `unknownOid` 仍保持原來的待查狀態。成交去重鍵包含原生商品／order ID／block time／tid。

特殊永續事件只有在獨立預期持倉／錢包／餘額／掛單一致、沒有該委託、沒有同 ID 的普通訂單狀態、連線健康且來源快照時間嚴格晚於事件時，才可標為「已由帳戶庫存涵蓋」。重連後要重新證明。這不是補造歷史訂單或已知對沖量。現貨沒有本批可驗證的來源水位，不能借用永續時間解除 dust 事件。

仍不涵蓋資產轉移流水的完整會計帳、歷史損益、手續費分攤、保證金可用額、所有 HIP-3／vault／子帳戶、策略條件單細節或持久化。快照對帳不提供交易所原子性；執行器還需持續事件檢查。既有 sticky fault 也不會被一次相同庫存自動消除。

