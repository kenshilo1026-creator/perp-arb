# Hyperliquid 私人事件：批次 3b-2

提供單帳戶 `orderUpdates`／`userFills`／`userFundings`／`userNonFundingLedgerUpdates` 共用 WS、ACK 與初始快照確認、心跳／重連及獨立 `orderStatus` 查單。3c-1 加入顯式庫存對帳。4b-2 新增獨立 `TradingClient`：驗證主帳戶／agent 綁定後，才可向固定 `/exchange` 簽署單筆 Alo／IOC 限價委託及 `cancelByCloid`。支援 mainnet／testnet；8 組簽名與現有 Python 比對。使用方式見 [order-test](../order-test/README.md)。下方 probe 仍只需地址、保持唯讀。真實唯讀結果與本機故障測試見 [驗收報告](../../VALIDATION.md)。Python 正式交易入口保持原狀。

## 使用

4b-1 新增 library `lookup_submission(&lookup::Request)`：固定 `/info` 的 `orderStatus`，`oid` 可為既有數字 ID 或 16-byte hex cloid 字串；仍共用 HTTP gate、權重及冷卻。回覆須符合帳戶、商品、方向、原始量及 client ID，`unknownOid` 保留未知狀態，不代表可以重送。尚未接入自動交易／恢復排程，loopback 驗證見 `tests.rs`。

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-hyperliquid-private --release --locked
.\rust\target\release\hyperliquid-private-probe.exe --help
```

上述指令不讀取帳戶設定。要連線時，在程序環境設定 `HYPERLIQUID_ACCOUNT_ADDRESS`，必須是實際主帳戶／子帳戶地址，不是 agent 地址。明確選擇唯讀模式：

```powershell
.\rust\target\release\hyperliquid-private-probe.exe --live-read-only --seconds 35
.\rust\target\release\hyperliquid-private-probe.exe --live-read-only --verify-recovery --seconds 90
.\rust\target\release\hyperliquid-private-probe.exe --live-read-only --seconds 30 --audit-account
```

程式不自動讀 `.env`；支援 Ctrl+C。恢復模式每 25 秒主動關閉本機 socket 並重連，至少觀察 70 秒，預設 90 秒；不改變交易所帳戶狀態。輸出只有統計，不輸出地址、訂單 ID、商品／數量／價格或原始私人成交。正常模式驗證能接收訂閱；恢復模式額外要求連線與訂單核對皆完成，否則非零退出。`transport_recovery_passed` 與 `order_reconciliation_complete` 分開，避免把有 ACK 當作帳戶已對帳。

## 成交與查單語意

- `orderUpdates` 及 `orderStatus` 以十進位 `origSz - sz` 解讀累計成交，驗證完整數量、明確狀態與原生商品名稱；沿用單一 actor 的累計去重、終態不回退、查詢 ticket 及世代隔離。coin 保留大小寫與 HIP-3 前綴。
- `userFills` 明確訂閱 `aggregateByTime:false`。以帳戶 actor 內 `(coin, oid, time, tid)` 去重，驗證同一身份的量、價、方向及交易 hash 一致；重連快照重播不重加。整數 ID 不經浮點數。
- **逐筆成交總量不加在 REST 累計量上。** 兩者可能重疊，初始快照也不保證是完整歷史。逐筆紀錄以 `hyperliquid_fill_summary` 供觀察；新觀察觸發獨立查單並標記 `unresolved_fill_orders`。既有累計量保留，直到訂單來源提供更完整數量。該觀察總量不是可直接送出對沖的差量。
- 尚無原始委託量的成交先保存，查回 `orderStatus` 後才建立累計訂單 view。查單進行中又收到新成交、世代變更、數量不足或該回覆時間早於觀察到的成交時，都不能清除缺口；使用本次回覆的時間，不拿舊 view 的最大時間代替。這是保守恢復條件，若交易所時間欄位不足以證明新鮮度，狀態會保持待核對。
- `unknownOid` 只代表這次查不到該訂單。保留觀察到的成交並退避，不合成零成交或拒單，也不自動平倉。帳戶歷史可能含 ADL、結算、現貨零碎資產轉換等紀錄；`dir` 是顯示文字，不能憑它自動忽略資金變化或宣稱已對帳。
- ACK 前收到的事件直接保存到尚未啟用的 Session，不能提升健康狀態；四種訂閱 ACK 與初始 `userFills.isSnapshot:true` 都收到才啟用。ACK 失敗或斷線仍保留已知事件。舊 Session 不能重新啟用新連線。
- 強制中斷、容量溢出／衝突留下明確缺口或 sticky fault。停止時不排新查詢，排空已排入的查單；已取得完整 HTTP 回覆後的 actor 入列不被 HTTP timeout 取消。

## 配額與界線

| 項目 | 預設 |
| --- | --- |
| 帳戶訂閱 | 一條 socket、四個 channel；每個 coordinator 只能有一位 transport 擁有者 |
| ACK／初始快照等待 | 10 秒 |
| JSON Ping／Pong | 每 20 秒／等待 10 秒；一般資料及 WebSocket 協議 Pong 不冒充 JSON Pong |
| 連線嘗試／退避 | 每分鐘最多 5 次，1–30 秒；穩定 60 秒才重設退避 |
| WS 控制訊息 | 每分鐘 120 則，含訂閱與心跳／Pong，跨重連保留 |
| 私人 HTTP 子配額 | 每分鐘 240 權重，`orderStatus`／`clearinghouseState`／`spotClearinghouseState` 各 2、`openOrders` 20；開始間隔至少 100 ms，單一 HTTP 在途 |
| 查單間隔 | 正常 5 秒，缺口或斷線目標 500 ms；失敗指數退避至 60 秒 |
| HTTP 期限／大小 | 單請求 10 秒、含配額等待 30 秒；查單回覆最多 64 KiB，帳戶回覆 2 MiB |
| WS 大小 | 每則 2 MiB、每個資料陣列最多 2,000 筆 |
| 記憶體保留 | probe 4,096 張訂單、最多 65,536 筆去重成交；不 LRU 淘汰 |

429／418／帶 Retry-After 的 503 會冷卻同一 client 的所有 clones，支援秒數及 HTTP 日期。不跟隨重導、不使用環境 proxy。子配額不涵蓋其他 client、公共行情、Python 或同 IP 主機。正在核對的訂單優先於已終結的歷史紀錄，大量資料仍受配額限制；500 ms 是排程目標，並非實際確認或對沖保證。

`--audit-account` 對主永續 dex 與現貨做兩次庫存採樣，涵蓋 positions、raw USD、spot total／hold、open orders；只輸出數目。函式 `audit_account()` 可明確指定最多四個 dex（受共用配額／30 秒總期限限制，不保證一輪可完成），不會把未查 dex 當作空倉。probe 沒有獨立預期帳本，所以 `expected_supplied=false`、`matched=false` 是正確結果。雙採樣相同加上 WS 版本隔離仍不是原子快照。這是顯式診斷，沒有放在行情或成交接收熱路徑；會佔用同一唯讀 HTTP gate 與配額，不應高頻呼叫。

特殊事件轉入帳戶待對帳集合，來源提示不會抹掉成交。現貨來源水位不足時保持待核對；詳見 [事件核心](../private-events/README.md)。本批無完整歷史分頁、持久化、client ID／未知送單關聯或真實 broker。WS 初始快照不是全帳戶歷史；累計 view 仍由 `orderUpdates`／查單更新。健康狀態只描述已知資料，不能用它攔截既有已成交 Maker 的必要對沖。

官方依據：[訂閱與資料型別](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)、[orderStatus／成交查詢](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)、[心跳](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)、[配額](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)。
