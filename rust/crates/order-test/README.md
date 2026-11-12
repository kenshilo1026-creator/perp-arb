# Rust 單筆下單測試入口：4b-2

`order-test` 可直接使用 Rust 送出、查證及撤回 **一筆永續限價委託**。支援 Aster v3 mainnet 與 Hyperliquid mainnet／testnet；不呼叫 Python 交易程式。`preview` 不讀金鑰，`submit`／`recover` 必須明確帶 `--live`。

此批用來驗證 adapter 與單筆交易生命週期，尚非自動跨所套利。沒有自動對沖、反向平倉、雙腿提交、重掛或策略帳戶接管。委託若已成交，撤單只撤剩餘量，成交部位會保留在交易所；輸出 `state.cumulative` 保留已收到的最高累計量，包括 ACK 中明確的成交量，但仍須核對終態。

## 使用順序

從專案根目錄建置、看說明：

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-order-test --release --locked
.\rust\target\release\order-test.exe --help
```

複製並編輯 `rust/configs/order-test.hyperliquid.example.json` 或 `order-test.aster.example.json`，另存為例如 `rust/artifacts/my-order.json`。帳戶地址是刻意無效的佔位值；請填實際主帳戶地址，並自行選擇價格、方向及數量。示例價格是測試欄位示範，不代表當前市場價格或交易建議。

| 欄位 | 語意 |
| --- | --- |
| `venue`／`network` | `aster` 只接受 `mainnet`；`hyperliquid` 接受 `testnet`／`mainnet`，使用各自固定端點及簽名 domain |
| `account` | 實際交易帳戶地址。HL agent key 的地址不能當成主帳戶地址；程式會查 `userRole` 核對 key 所屬帳戶 |
| `symbol` | 原生商品，例如 Aster `ETHUSDT`、HL `ETH`；不支援 spot、HIP-3、vault 或 subaccount 路由 |
| `side`／`quantity` | `BUY`／`SELL`；數量是商品原生基礎單位，使用 JSON 字串 |
| `limit_price` | 明確限價，以字串輸入；不自動改價格或數量來通過精度檢查 |
| `tif` | `post_only`：Aster GTX／HL Alo；`ioc`：限價立即成交、剩餘撤回。沒有無限滑點市價單 |
| `reduce_only` | 是否只減少既有部位；不自動設定槓桿、保證金模式或建立對沖部位 |
| `max_notional_usd` | 精確檢查「輸入限價 × 數量」不超出此值；不是成交金額、手續費或總帳戶風險的交易所硬上限 |
| `wait_seconds` | 1–30 秒，從準備本次提交時計算；到期要求撤回剩餘委託。不保證網路／交易所會在此時間前完成撤單 |

先執行公開資料預覽，會從相同網路讀取 metadata，檢查商品、tick、lot、最低名義值及 HL asset ID：

```powershell
.\rust\target\release\order-test.exe preview --plan rust/artifacts/my-order.json
```

金鑰由你已設定的**程序環境**提供，不自動載入 `.env`，不接受命令列私鑰：

- Aster：`ASTER_API_WALLET_PRIVATE_KEY`，使用 plan 的 account 作為簽名 user。
- Hyperliquid：`HYPERLIQUID_PRIVATE_KEY`。主帳戶 key 或已授權的 API agent key；agent 關聯會先查證。testnet 需相應網路已備妥的測試帳戶／資金。

確定內容後，以下指令會真的送單；mainnet 會涉及真實資金：

```powershell
.\rust\target\release\order-test.exe submit --plan rust/artifacts/my-order.json --journal rust/artifacts/order-001.wal --live
```

每筆測試使用全新帳本檔案，`submit` 拒絕覆寫既有檔案。私人 WS 未就緒時不送新單。若留下一份 `dispatched=false` 的帳本，表示未取得送單許可，可先 inspect；`recover` 不會幫它開始送單。勿以刪除未知帳本或換檔名重試來繞過查證。

## 中斷及查證

```powershell
.\rust\target\release\order-test.exe inspect --journal rust/artifacts/order-001.wal
.\rust\target\release\order-test.exe recover --journal rust/artifacts/order-001.wal --live
```

`inspect` 只讀帳本。`recover` 從保存的網路、帳戶、完整參數與 client ID 恢復，要求撤回同一 client ID 並查證，絕不重送原委託；私人 WS 不可用時仍可走 REST 查證／撤回。錯誤／查不到委託不變成零成交。每輪最多十次撤單後查詢，仍無法確認終態時退出 2，保留帳本供下一次 recover 或人工核對。

Ctrl+C 會要求清理；已送出的 POST 不因取消本機 future 而被當成未送出。未開始的提交會停止；在途送單回覆後仍會查證及撤回。強制殺程序／斷電則須重新 recover。磁碟或協調器失敗後，會嘗試針對該 client ID 撤單，但仍回報失敗，不宣稱已消除部位。

交易所明確拒單目前也保守保留為「送單錯誤，待查證」；沒有把所有 HTTP 4xx／5xx 或 `status:error` 都當成確定未成交。若拒單未產生可查的訂單，可能持續 `NotFound`，需對照交易所紀錄，不會自動改參數重送。

## 輸出與量測

- exit 0：完整核對到委託終態；**不代表零成交、平倉或賺錢**。`terminal=true` 且 `reconciled=true` 才是本次核對通過；inspect 重開帳本時歷史終態不再被標示為新鮮證明。
- exit 2：未知／未完成，需保留帳本恢復。
- exit 1：設定、連線、檔案或執行故障；如可能已送單，仍須 inspect／recover。
- `submit_ack_ms`：本次處理開始到收到提交 ACK，包含同步磁碟及限流等待。
- `first_coordinator_fill_ms`：協調器中首次觀察到正成交量的本機時間差，來源可能是 WS 或 REST，**不是純 WS 延遲或兩所實際成交差**。協調器可能合併更新，此值不是第一筆撮合時間。
- `terminal_lookup_ms`：本次處理開始到完整 REST 終態核對。尚未有實盤速度比較或低於 100 ms 的驗收。

帳本同步在專用 blocking worker，私人 socket reader 獨立工作；此測試器以最多約每 500 ms 一次 REST 查證，並保存協調器的累計變化。它不代表未來自動對沖熱路徑的排程效能。單次 POST 等待最多使用 client 的 30 秒總時限；到期撤單優先於慢唯讀查詢，仍會受 HTTP gate／交易所配額／網路影響。

同一 OS 使用者的 order-test 程序共用按網路／帳戶建立的本機鎖，避免兩份不同帳本同時測同一帳戶；Python 不遵守此鎖。請使用由本測試獨立管理的帳戶／測試窗口。帳本包含帳戶與交易資料，不含私鑰，`.wal` 已忽略提交；它不是完整資金帳本或防回滾資料庫。

驗證使用固定公開測試 key、loopback 交易所與 Python 簽名對照；沒有在驗收腳本中載入真實金鑰或送真實委託。協議依據：[Aster v3](https://asterdex.github.io/aster-api-website/futures-v3/account%26trades/)、[HL 交易端點](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)、[HL 官方簽名實作](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/signing.py)。
