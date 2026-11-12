# Aster 私人事件 transport：批次 3b-1

提供 Aster v3 listenKey、私人 WebSocket、續期／重連，以及獨立訂單查詢排程。3c-1 擴充唯讀庫存查詢：**POST／PUT `/fapi/v3/listenKey` 與 GET `/fapi/v3/order`、`positionRisk`、`openOrders`、`balance`**。4b-2 新增 `submit_limit`／`cancel_limit`：固定 POST／DELETE `/fapi/v3/order`、client ID、GTX／IOC、單向 BOTH 及 reduce-only；DELETE 使用簽名 query。單次送單，未知回覆交帳本查證。使用獨立的 [order-test](../order-test/README.md) 做單筆測試。下方私人 probe 保持唯讀；沒有調槓桿、轉帳或自動跨所策略。

3b-1 驗收為本機 HTTP／WS 替身與固定測試金鑰；3b-2 已完成真實帳戶短期認證、續期及受控換線驗證，見 [驗收結果](../../VALIDATION.md)。觀察期間沒有成交，不能將這項結果視為真實成交補漏或全天連線驗收。

## 唯讀驗證入口

4b-1 新增 library `lookup_submission(&lookup::Request)`，使用相同簽名、HTTP gate、權重及冷卻，以 `origClientOrderId` 或 order ID 查證未知送單。驗證商品、買賣方向、原始數量及 client ID；查無訂單不等於安全重送。訂單 GET 的明確 `-2013` 回覆（含 HTTP 400）映射 `NotFound`，其餘非成功狀態保留 typed error；兩者皆不釋放預留量。此 API 尚未接入自動交易／恢復排程，loopback 驗證見 `tests.rs`。

從專案根目錄建置並查看說明；這兩個指令不讀取帳戶金鑰或連接交易所：

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-aster-private --release --locked
.\rust\target\release\aster-private-probe.exe --help
```

需要連接真實帳戶時，程序環境須已設定 `ASTER_API_WALLET_PRIVATE_KEY` 與 `ASTER_USER_ADDRESS`。程式不自動載入專案 `.env`，也不接受命令列私鑰；可選的 `ASTER_API_WALLET_ADDRESS` 若存在，必須符合私鑰推導出的 signer。只有明確指定下列旗標才會讀取這些環境變數並連線：

```powershell
.\rust\target\release\aster-private-probe.exe --live-read-only --seconds 30
.\rust\target\release\aster-private-probe.exe --live-read-only --seconds 30 --audit-account
```

另可使用 `--live-read-only --verify-recovery --seconds 90`：每 10 秒續期、每 25 秒更換本機 socket，至少觀察 70 秒，預設 90 秒。只有此明確旗標使用短測節奏；正式預設仍為 30 分鐘續期及 23 小時 55 分鐘換線。不 DELETE 共用 listenKey。恢復驗證要求至少三次連線及三次續期、結束前健康且沒有待核對訂單或查詢失敗。

若要查詢既有訂單，可加入 `--track ETHUSDT:123456:0.01`，三欄依序是原生商品、真實 order ID、該委託原始數量；這是登記查詢目標，**不是建立訂單**。最多登記 64 張，數量不符會要求恢復，不能用猜測的原始數量驗收。未指定時只接收訂閱後的帳戶事件，並對已觀察到的訂單做必要查詢，不補掃帳戶歷史。

probe 支援 1–3600 秒、Ctrl+C、明確停止及排空在途查單回覆。輸出只有連線／續期／事件／查單計數、需核對的已追蹤訂單數及經縮減的錯誤碼，不輸出私鑰、listenKey、簽名 URL 或原始帳戶資料。若整段期間沒有成功建立私人 socket，回傳失敗。`connected_before_stop` 只表示連線狀態，不代表整個帳戶已對帳或具備交易資格。

## 元件與生命週期

`--audit-account` 額外查兩輪持倉／全商品掛單／錢包，權重各 5／40／5，單輪 50、完整雙採樣 100，與 listenKey／查單共用配額。兩次庫存不一致或期間收到帳戶事件／斷線即拒絕作為證明。帳戶回覆上限 2 MiB；一般訂單回覆沿用原上限。`HttpPolicy.requests_per_minute` 保留名稱但現為權重預算。未供獨立預期帳本時只有觀察資料，不授權交易。

對帳是顯式診斷，沒有自動放入熱路徑；會佔用唯讀 HTTP gate 與配額，禁止以高頻輪詢取代私人事件。Aster 沒有用本機時間或歷史 `updateTime` 冒充全帳戶來源水位。支援 hedge mode 分開 LONG／SHORT；複雜條件單如缺少原始數量時會拒絕採樣，仍需後續擴充。

- `auth::Credentials` 接收呼叫者明確提供的 API wallet key，推導 signer，產生 Aster v3 EIP-712 簽名。固定公開測試金鑰的 digest、簽名字節及 URL encoding 與 Python adapter 比對；實際金鑰型別不提供 Debug。nonce 在同一 Rust 程序按 signer 共用，等 HTTP 配額之後才產生，避免等候期間過期。主機時鐘須正確，尚未加入自動對時與跨程序 nonce 協調。
- `client::PrivateHttp` 共用 reqwest 連線池、序列化 HTTP gate、每分鐘權重配額與服務端冷卻。只有固定 Aster 主機及上述操作；不跟隨重導、不自動使用環境 proxy，錯誤不保留 URL、回應本文或金鑰。loopback URL 只在 `cfg(test)` 可替換。
- `monitor::start(...).await` 綁定 client 的 user 與 coordinator scope，同一 client 或 coordinator 不能重複啟動 transport。先標記缺口，再並行啟動 socket 工作與查單工作。
- WS 取得 listenKey 並握手成功後建立 Session；Aster 這條串流沒有另一個訂閱 ACK。每則文字訊息取出後立即記錄本機 `Instant` 再交給 coordinator。HTTP 查單與續期等待不阻塞 WS 接收。
- 斷線、listenKey 過期、心跳失效或續期失敗會保留累計量、標記缺口並重新取得 listenKey。重連後透過帶有 instance／generation／revision ticket 的查單確認缺口；新連線本身不能宣稱已恢復成交資訊。
- 查單只處理已知、尚未終結或仍需核對的訂單。同一張單只有一個排程中查詢；活動訂單優先於保留的歷史終態紀錄，各優先級內按到期時間排程；活動訂單尚未到期的空檔仍可查核歷史紀錄。完整且已核對的終態停止輪詢，重連後才重新核對。
- 查單逾時、404、找不到訂單、5xx、解析失敗都保留已有成交並要求再查，不合成拒單／零成交，不重送交易、不反向平倉。HTTP 完整回覆取得後，送入 coordinator 的動作不再受 HTTP timeout 取消。
- 停止時不再安排新查詢，排空已排入的查單工作並撤銷連線健康。只關閉本機 socket，**不 DELETE listenKey**：同帳戶可能共用交易所回傳的有效 key，不能以停止這個 probe 使其他讀取端斷線。閒置 key 由交易所到期處理。

## 預設節奏與限制

| 項目 | 預設 |
| --- | --- |
| listenKey 續期 | 每 30 分鐘；本機有效期限保守設為 55 分鐘 |
| socket 定期更換 | 23 小時 55 分鐘 |
| 主動 Ping／等待相符 Pong | 20 秒／10 秒；非相符 Pong 不解除等待 |
| WS 控制訊息 | 每秒最多 8 則，含 Ping／Pong，跨 reconnect 保留計數 |
| 連線嘗試 | 每分鐘最多 5 次；退避 1–30 秒，穩定 60 秒才重設 |
| HTTP 子配額 | 每分鐘 120 權重、啟動間隔至少 100 ms；一個實際 HTTP 請求同時執行 |
| HTTP 期限 | 單請求 10 秒，含 gate／配額等待的總期限 30 秒；無內部立即重試 |
| 正常／缺口或斷線查單 | 每單成功回覆後 5 秒／500 ms；仍受共用配額限制 |
| 查單失敗 | 500 ms 起指數退避，最多 60 秒；服務端 Retry-After 優先 |
| 排程 | 每 250 ms 檢查，最多 2 個在途或等候配額的查單工作 |

HTTP 429／418 與帶 Retry-After 的 503 會讓同一 client 的 listenKey／查單共同冷卻；支援秒數與 HTTP 日期。沒有標頭時，429 至少 60 秒、418 至少 180 秒。錯誤與逾時不扣回已使用配額。讀取回覆、WS frame 及事件 ledger 都有容量上限，超限不能假裝資料完整。

共用限流僅涵蓋同一 `PrivateHttp` 的 clones，不包含公共行情 client、獨立 client、Python、其他程序或同 IP 主機。重連仍會標記全部保留紀錄需核對，舊終態紀錄很多時，整體恢復健康可能較慢；清理與持久化留待後續批次。大量訂單受此子配額限制，500 ms 是排程目標，並非每單實際刷新保證；不能據此承諾實盤對沖低於 100 ms。WS 控制寫入亦有最多 2 秒的等待期限，不是硬即時系統。

## 驗收與剩餘範圍

```powershell
powershell -NoProfile -File rust/tools/check.ps1 -Format
```

全套檢查只使用離線／loopback，不執行 `--live-read-only`。固定 auth vectors 放在 `fixtures/auth-vectors.json`；其中 key 是公開的測試常數。Python 驗證器以實際 `AsterExecutionAdapter` 重算，Rust 測試核對完全相同的 digest／signature／wire encoding，不把自我回圈驗證當成跨語言比對。

尚未完成：真實 Aster 成交補漏與查單驗收、長時間錄製及回放、完整帳戶委託／持倉掃描、未知送單與 client ID 關聯、落盤與崩潰恢復、終態去重紀錄清理、實際交易執行。Hyperliquid 私人適配器另見 [使用契約](../hyperliquid-private/README.md)。健康資訊仍只涵蓋已追蹤訂單，不能阻擋已知成交後必要的對沖，也不能當作新增風險的唯一許可。

官方協議依據：[v3 認證與 nonce](https://asterdex.github.io/aster-api-website/futures-v3/general-info/)、[listenKey 與私人串流](https://asterdex.github.io/aster-api-website/futures-v3/user-data-streams/)、[訂單狀態 GET](https://asterdex.github.io/aster-api-website/futures-v3/account&trades/#query-order-user_data)。
