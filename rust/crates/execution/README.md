# 執行協調器：4a／4b-1 紙上批次

第五批：browser WAL 新增 confirmed trade ID／數量／價格的持久化聯集與衝突屏障；舊匿名累計只作下限，不與 ID 集合相加。`variational_bridge::owner::Owner` 已在 blocking worker 協調 Maker 與 paired WAL，串起精確查詢、對沖預留／許可及恢復，並於重啟補交兩次寫入間的已知成交。仍未接實際雙腿網路送單；詳見 [第五批](../../VARIATIONAL.md)。

後續來源接線：`variational-bridge::native` 已提供精確 RFQ 查詢快照、原生 submit receipt 綁定及 extension 持久化 receipt 恢復。它要求帳戶、商品類型、數量、方向、限價、reduce-only 相符，且不把取消 ACK／查無列轉成零成交終態。這些 library 方法尚未由雙腿 live owner 呼叫；本 crate 仍不直接交易。

2026-11-19：新增 `durable::browser::Ledger`，保存 Variational browser Maker 的限價意圖、reduce-only、單次送單／撤單待確認與累計成交，沿用本 crate 的 WAL。`observe_browser_maker` 及 `begin_browser_reconciliation`／`reconcile_browser_maker` 接入既有對沖預留與恢復流程；合成案例已覆蓋 Variational → Aster／Hyperliquid 的部分成交、撤單競態與未知對沖重啟。詳見 [Variational 第三批](../../VARIATIONAL.md)。此處的 `verified` 方法要求呼叫端已獨立證明帳戶與訂單歸屬，並未替 extension 自動建立該證明，也沒有新增真實 browser 交易 transport。

`Engine` 接收 Aster／Hyperliquid 共用 `Coordinator` 的 `OrderSnapshot`，產生對沖意圖 `Intent`。此 library 不執行網路交易；4b-2 的獨立 [order-test](../order-test/README.md) 已可測試真實單筆限價委託。原 Python 執行器仍負責既有完整套利流程。

4b-2 新增 `durable::single::Ledger`，沿用鎖、雜湊與同步寫入格式，保存完整網路／帳戶／商品／方向／價格／數量／TIF／reduce-only／client ID／asset ID／名義金額上限。`dispatch` 只能取得一次；ACK 的明確成交數量立即保存但不當成終態證明。每次重啟需要重新查證，`NotFound` 不允許重送。`order-test` 以獨立 blocking worker 擁有此帳本，再接兩所送單／撤單接口；它不會使用舊紙上 `Engine` 自動對沖，也不是完整現金帳本。

`Engine::arm` 要求兩腿帳戶對帳與獨立預期庫存吻合、報告在同一協調器／連線世代／帳戶版本內且未過期、沒有未完成委託／待核對事件，並通過呼叫者的價格檢查。範圍只限一個新 Maker、一比一相同基礎資產數量；不同合約乘數尚未接入。

執行順序：

1. 訂閱 `maker_stream()`，先讀 `borrow_and_update()`，再等 `changed()`；交給 `observe_maker()`。所有變化使用累計量，重複事件不重複對沖。
2. `reserve_hedge()` **先**保存預留量，再交由未來 broker 發送。沒有價格或報價健康參數，避免已知成交因價差超標而停止對沖。最小量以下仍留在 `exposure()`，不歸零。
3. `acknowledge()` 只綁定訂單身份；回覆不明保留整份預留量。安全關聯訂單後，可用 `hedge_stream()` 或直接送入私人快照；ACK 前成交同樣可以處理。
4. `observe_hedge()` 按累計成交更新；僅在終態且 `requires_reconciliation=false` 時釋放剩餘量。撤單 ACK 沒有釋放預留量的介面。終態後矛盾成交保留數量並停止自動重送。

`tests/coordination.rs` 連接兩個真正的事件協調器，以合成事件驗證完整紙上流程。`execution-replay` 是離線 JSON fixtures 工具，配合 `tools/verify_execution.py` 對照現有 Python `terminal_fill_quantity()` 的終態剩餘量重試規則；不讀帳戶或網路。

## 4b-1：持久化與未知送單

`durable::DurableEngine` 是可選的持久化執行帳本。適用 Aster／Hyperliquid **單向永續、一個已有 order ID 的 Maker 及其對沖意圖**。不接管 Python 或自動開始交易。沒有新 Maker 送單、價格／order type／reduce-only 的完整送單資料，因此這批不能當作可重送任意委託的交易 outbox。

帳本保存啟動時獨立核對過的持倉／餘額基準、Maker 累計成交、對沖意圖與預留量、送單標記、client ID、ACK 綁定和已驗證累計成交。`expected_positions()` 用基準加策略成交計算預期持倉；沒有從交易所現況反向覆寫預期值。費用、資金費率、轉帳及現金損益尚未入帳，不能用它解除整個帳戶的特殊事件。

操作順序：

1. `create()` 要求前述獨立對帳與價格門檻，使用全新檔案。`open(path, expected_plan)` 嚴格比對原計劃、回放帳本；不恢復舊程序的健康狀態或 revision。`intents()` 可列出所有恢復的意圖。
2. `observe()` 保存已知成交，再 `reserve_hedge()` 保存對沖預留量。價格門檻仍不作用於這些已知成交。
3. `prepare_dispatch(id)` **寫入送單標記並成功 `sync_all()` 後**才回傳 `DispatchPermit`。client ID 用已保存的隨機命名空間與意圖產生；同一帳本同一意圖不能第二次取得許可。未來 broker 必須消耗這個許可，禁止自行複製參數重送；這個 library 本身沒有網路送單能力。
4. HTTP 不明、timeout、取消本機 future、程序中斷，一律保留可能已送出的量。用 `lookup_ticket(Target::Hedge(id))` 取得保存的 client ID，交給 `PrivateHttp::lookup_submission()` 或 HL `Client::lookup_submission()`，成功回覆才交回 `apply_lookup(ticket, outcome)`。錯誤不轉換成零成交，不自動重送；排程器須沿用退避。Aster 用 GET `origClientOrderId`，HL 用 `orderStatus.oid` 字串 cloid，仍共用既有限流／冷卻。
5. 重啟後逐一查證 Maker 及已標記送出的對沖單。查詢票據綁定本次開啟的實例、帳本序號和 30 秒時限；其間有新事件或斷線屏障，回覆仍可增加已知成交，但不能釋放未知剩餘量。WS 的 dispatch time 不與 REST 的 order update time 比大小。查無訂單及回覆量較少都保留已知量與待查狀態。
6. 查證完整身份及終態後，才釋放未成交剩餘量，產生**新的**對沖意圖。私人快照沒有 client ID，未知 order ID 必須先經查證或安全關聯的 ACK 綁定。歷史終態的重複事件不重新佔用已釋放量；矛盾的增加成交仍記入曝險並標示 `faulted()`。

`require_reconciliation(target)` 用於斷線／缺口，立即使舊查詢票據失效。未來 transport owner 必須接上這個通知；目前沒有自動執行／恢復排程。恢復完成也只代表這份執行帳本可繼續處理原有對沖責任，**不授權新 Maker 或代表全帳戶已對帳**。從未取得送單許可的意圖可在恢復後首次取得許可；取得過但查不到的意圖維持未知，不能藉改 client ID 繞過。

### 磁碟與執行緒

使用單一寫入者、OS 檔案鎖（Windows 同時禁止分享開啟）、逐筆長度／schema／連續序號／SHA-256 鏈及 `sync_all()`。殘缺尾端、錯誤雜湊與未知 schema 拒絕開啟，不自動截斷修復。I/O 失敗會保留新得知的記憶體曝險並封鎖後續許可；由操作端處理故障及唯讀查證，不能刪帳本重建來恢復交易。上限 128 MiB、單筆 1 MiB；不自動輪替或清除歷史去重資料。

同步帳本 I/O 應在專用 blocking worker 執行；私人 socket reader 繼續處理事件，不能等待磁碟或 REST。取得 durable 許可前仍需等待磁碟，沒有宣稱新增 I/O 會降低延遲。資料夾由部署者預先建立並限制存取；帳本包含帳戶／持倉資料，不含私鑰或簽名，`.wal` 已忽略提交。適用本機磁碟；鎖不提供跨主機或多個不同檔名的策略所有權。雜湊鏈不防整段有效尾端被刪除、舊備份回滾或人工重算；沒有斷電／磁碟控制器可靠性驗收。

測試涵蓋實際子程序直接退出、重開／鎖、未知查證、錯身份、事件競態、REST／WS 時間語意、殘缺／損壞、寫入／同步不明故障和終態後矛盾成交。網路使用 loopback 替身；未以真實下單測試這個帳本。

尚未完成：新 Maker 未知送單、真實交易 adapter、Maker 撤回／重掛排程、最低名義值／reduce-only／合約單位與價格保護接線、完整資金帳本、持續策略與停機交接。帳戶快照不能代替持續風控，也不是兩所原子成交保證。

## Variational 第六批增量（2026-11-19）

`pending_browser_order()` 讓 paired WAL 在 Maker POST 前建立，原始 plan 的本機 `intent:request_id` 不會送到交易所。`validate_new_browser_risk()` 再檢查兩腿獨立帳戶報告與新風險價格條件；只有持久化 native receipt 才能 `bind_browser_maker()` 綁定 RFQ。重開時傳入原始 pending plan，owner 先恢復 receipt 綁定再交接累計成交，不以 symbol／數量猜單。

`prepare_wire_dispatch()` 先保存 hedge 的 network、asset、數量、限價、TIF、reduce-only、WAL 衍生 client ID 及名義值上限，再保存單次 dispatch 標記。相同意圖不得替換 wire 參數或重送。已保存參數不代表可自動重放；未知結果仍需精確查證，恢復 transport 必須使用原 network／account。這是明確送單資料，並非自動重試 outbox。

`variational-bridge::execution_io` 將上述 permit 交給 extension 及既有 Aster／Hyperliquid `Venue` transport，涵蓋 submit、cancel、exact lookup、部分成交及未知回覆。caller 仍負責帳戶鎖、行情／商品規格、監控與 gap 轉交、重啟排程；沒有正式雙腿 live CLI，沒有真實送單驗收。完整限制及測試見 [VARIATIONAL.md](../../VARIATIONAL.md)。
