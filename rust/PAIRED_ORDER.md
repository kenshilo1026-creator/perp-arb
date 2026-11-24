# Variational＋Aster／Hyperliquid 雙腿下單

這是 `place_order.py --engine rust` 的有界限手動下單入口。Python 只載入 `.env` 及啟動 Rust；Variational 透過現有 extension，Aster／Hyperliquid 使用 Rust 原生簽名、送單、私人 WS 及查單。預設 Python 引擎沒有切換；Rust 失敗不會回退到 Python 下單。

## 本版範圍

- 每次執行一筆 Variational 固定限價 Maker；收到精確 RFQ 查詢確認的成交增量，送出另一所 IOC 限價對沖。
- 支援開倉及兩腿 `reduce_only=true` 的平倉，兩腿方向相反、基礎幣種和數量相同。Aster 使用單向持倉；Hyperliquid 使用主永續市場，不含 HIP-3／現貨。
- 價差、深度或行情不合格時阻止新 Maker；掛單期間不合格則要求撤回。已確認 Maker 成交仍保留對沖責任，不以入場價差阻止對沖。
- IOC 對沖仍受計劃中的明確限價、名義值及交易所最小量限制；不能保證全部成交。終態部分成交可繼續對沖剩餘量，UNKNOWN／NotFound 保留在途量，絕不盲目重送。
- 送單前同步寫入兩份 WAL；重啟讀取原計劃及身份。Ctrl+C 透過父程序控制通道要求撤回 Maker、查證及對沖已知成交。強制殺程序後必須使用同一目錄恢復。

沒有 Maker 自動追價／重掛、多批分單、資金費率掃描、自動進出場或完整費用／盈虧帳本，也不更新 Python position registry。這不是整個 Python `place_order.py` 的功能等價替代。

## 準備計劃

### 沿用 funding monitor 選擇配對

本機 `rust/artifacts/paired-settings.json` 已設定 extension、三所帳戶地址，以及新計劃的 `maker_fill_mode="ws_confirmed"`。此檔不含私鑰且不納入 Git；既有計劃及交易帳本不受影響。第一次成交也可以使用此模式，無須先以 REST 模式成交解鎖。

```powershell
python scripts/run_funding_monitor.py 7 --required-venue variational
python scripts/place_order.py --engine rust --rust-action prepare --rust-plan rust/artifacts/my-ws-plan.json
```

若已有 24 小時內的 monitor 結果，不必重新執行 monitor（該命令仍會依原流程發 Telegram）。backfill 補的是歷史資料；自動選擇讀取的是 monitor 產生的 `data/monitor_signals.json`。

`prepare` 只問幣種、本筆數量及另一所 IOC 限價。Variational Maker 價格預設由 Rust 的 `mid-price` 唯讀指令，透過現有 extension 在該幣種頁面按 Limit／Mid 並讀取價格欄，不再要求手動輸入，也不使用公開報價的平均值代替按鈕。先讓 extension 連著所選幣種頁面並按 Start；此預覽不需要開啟「允許 Rust 原生送單／撤單」。無連線、讀價失敗或無效價格會停止，不產生新計劃、不回退 Python 下單。現有 extension 已支援此預覽命令，無須為此更新 extension，但需重新編譯 Rust release。

取得 Mid 後將該價格及對應名義值固定寫入新計劃，再呼叫 Rust 唯讀 preview，不送單。之後的 audit／submit 不會暗中重按 Mid 或改寫計劃；隔太久需重取價格時，重新 prepare 成另一份計劃。`--engine rust` 省略 `--rust-action` 也進入此流程。開倉是預設模式；省略交易所及方向時，在 `signal=true` 且 Rust 支援的 Variational＋Aster／Hyperliquid 配對中選年化最高者，保留訊號的 SHORT／LONG 方向，不再要求選方向。資料過期／沒有合格訊號時停止，不回退 Python。

可用 `--ticker`、`--rust-quantity`、`--rust-maker-price`、`--rust-hedge-price` 取代互動輸入。IOC BUY 價格是最高買價，SELL 是最低賣價；固定 IOC 限價仍可能導致對沖未完全成交。價格及數量須通過 Rust 商品精度、最小量和最低名義值預覽。此流程只準備一筆，不會把輸入數量拆成多批。

指定單邊 `--short_venue` 或 `--long_venue` 會限制自動候選；兩邊都指定則採用明確指定的配對，不必有 monitor 訊號。Variational 固定為 Maker；不支援的交易所會明確報錯。`--mode close` 必須明確填寫**原持倉**的 `--short_venue` 和 `--long_venue`，生成的兩腿反向且 reduce-only；不能用今天的最佳訊號猜測原倉位。下單前仍須對帳。

可用 `--rust-settings` 指定另一份私人設定，或用 `--rust-fill-mode rest_verified` 改變本次新計劃。這些準備參數不能用於 submit／recover 覆寫已保存的計劃。輸出檔必須不存在；不要覆寫已送單計劃。

準備成功後依下方唯讀對帳、核對基準及 submit 步驟實測，將範例的 `my-plan.json` 換成 `my-ws-plan.json`。時間記錄及分析方式見文末。

### 手動編輯完整計劃

先編譯目前入口：

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin paired-order --release --locked
Copy-Item rust/examples/paired-variational-aster.json rust/artifacts/my-plan.json
```

編輯 `my-plan.json`。範例帳戶、extension ID 及價格都是示意，不能直接用於交易。填入真實帳戶（小寫地址）、extension ID、商品、兩腿方向、相同數量、Maker 限價及對沖 IOC 限價／名義值上限。`run_id` 必須每筆新委託唯一，且等於 `maker.request_id`。`funding_interval_s` 需與 Variational 的實際商品一致。

`max_entry_gap="0.0025"` 表示以 Maker 價格為分母的不利價差上限 0.25%；入場使用整筆數量的對沖深度 VWAP，另檢查最末檔是否在 IOC 限價內。此值不會自動讀取 Python 的 `.env` 價差預設。

若對沖所改為 Hyperliquid，設定 `hedge.venue="hyperliquid"`、`hedge.symbol="ETH"` 和該所帳戶。其他參數也要按商品規格調整；不能只換交易所名稱而保留不合規的價格或數量。

平倉要建立新 run：兩腿方向與各自既有持倉相反，**兩腿** `reduce_only=true`，數量不超過各自可平倉量。基準檢查會拒絕不足持倉；現有 Python 倉位記錄不會自動轉換為計劃。

Python wrapper 載入現有 `.env`。Aster 使用 `ASTER_API_WALLET_PRIVATE_KEY`；Hyperliquid 使用 `HYPERLIQUID_PRIVATE_KEY`，主帳戶／代理關係由既有原生 adapter 查證。不要把私鑰寫入計劃。直接執行 Rust binary 時須自行設定程序環境。

## 預覽及唯讀對帳

```powershell
python scripts/place_order.py --engine rust --rust-action preview --rust-plan rust/artifacts/my-plan.json
python scripts/place_order.py --engine rust --rust-action audit --rust-plan rust/artifacts/my-plan.json --rust-baseline-output rust/artifacts/observed.json
```

預覽只查公開商品規格；對帳需要已登入的 Variational 頁面、extension Start，以及另一所憑證。保持 extension「允許 Rust 原生送單／撤單」關閉即可做唯讀驗收。三個 endpoint 為 `ws://127.0.0.1:8766`、`8767`、`8768`。

`audit` 保存兩所實際持倉、抵押金／錢包及已支援的既有委託，`reviewed=false`。核對自己的獨立記錄後另存為 `reviewed.json`，再將 `reviewed` 設成 `true`。送單前會重新查詢並與此基準比對；不可直接自動把本次查詢當成預期庫存。輸出包含私人帳戶資料，留在忽略提交的 `rust/artifacts/`。

目前新單入口拒絕有既有活躍委託或非單向持倉的帳戶；不會替你撤掉舊單。2026-11-19 的 Aster 唯讀驗收發現一筆既有掛單，因此該帳戶尚不能直接通過新單前置檢查。

## 送單及恢復

完成計劃和基準核對後，才在 extension 勾選「允許 Rust 原生送單／撤單」並重新啟動轉發。

```powershell
python scripts/place_order.py --engine rust --rust-action submit --rust-plan rust/artifacts/my-plan.json --rust-baseline rust/artifacts/reviewed.json --rust-run-dir rust/artifacts/run-001 --live
python scripts/place_order.py --engine rust --rust-action inspect --rust-run-dir rust/artifacts/run-001
python scripts/place_order.py --engine rust --rust-action recover --rust-run-dir rust/artifacts/run-001 --live
```

`submit` 的 run-dir 必須不存在。`inspect` 離線讀取兩份帳本，顯示 Maker、未對沖量及在途量；不查交易所。`recover` **可能撤單及對沖真實成交**，所以需要 `--live`；只使用原計劃，不能另給價格、數量或新 Maker。不要刪除／修改 WAL 或另建 run 來處理未知回覆。

`maker_wait_seconds` 為 1–60 秒，`recovery_seconds` 為 15–120 秒；到期會報告未完成狀態。網路查詢自身有逾時，實際程序時間可能超過迴圈期限。程序退出不代表所有交易所委託已終止。最小量以下的殘餘曝險也會保留，不能假裝歸零。

手動 Python `place_order.py`、`run_spot_perp_arbitrage.py` 和 Rust 下單 CLI 共用 OS 執行鎖，防止同時管理帳戶；其他策略程序、網頁手動操作或別台電腦不受此鎖約束。

## 驗收界限與延遲

目前使用 mock／loopback 驗收兩所完整 runner、部分成交、IOC 剩餘重試、未知提交只查不重送、中斷控制及持久化。真實帳戶只做唯讀驗收，沒有執行實際送單或撤單。

預設 `maker_fill_mode="rest_verified"`：Variational 私人 WS 觸發精確 REST 核對後才授權對沖；查單可能花數秒。另有明確選用的 `ws_confirmed` 快速路徑，詳見下節。尚未取得真實新成交樣本驗收 WS-only 熱路徑，不能宣稱小於 100 ms 或保證搶贏其他 bot。

Variational 的取消／拒絕零成交終態尚無充分契約證據。撤單 ACK、pending 清單消失或 NotFound 都不會被當成安全終態；未全成交的取消 run 可能在已知未對沖量為零時仍回報未完成，需保留帳本查證。這版因此不自動重新掛 Maker。

Python 雙腿 wrapper 明確使用最佳化的 `rust/target/release/paired-order.exe`，缺少時要求先編譯，不會默默回退到 debug 或 Python broker。每次更新 Rust 原始碼後都要重新編譯。亦可直接使用該 binary（原生參數名稱見 `--help`）。舊有唯讀 `--rust-preflight` 仍使用獨立的 debug probe。

## WS 快速對沖及實測時間記錄

本批新增 `maker_fill_mode`，舊計劃省略此欄仍使用 `rest_verified`，不會默默改成信任未經實網驗收的 WS 成交語意。

| 模式 | 行為 |
| --- | --- |
| `rest_verified` | 保留 REST 成交核對，同時記錄 WS 到達及後續核對耗時。建議第一次真實成交樣本使用此模式。 |
| `ws_confirmed` | 明確選用的試驗路徑：匹配 authenticated submit 綁定的 RFQ、商品完整規格、方向、正數增量、trade ID、confirmed/trade 狀態及 Maker 限價；成交寫入兩份 WAL，再直接送出原生對沖。REST 在旁核對，不位於首次 WS 對沖送單前。 |

快速路徑使用獨立 task；主迴圈的 Maker 查單／撤單 HTTP 等待不持有快速送單鎖。對沖送單鎖只涵蓋預留、完整 wire intent 持久化、送單及 ACK 寫入；後續 REST 查證已移出該鎖。多個增量仍受序列化送單、精度、最低名義值及在途量約束，並非每個事件都可以獨立下單。

WS／REST 透過同一 trade ID 帳本去重，重啟仍保留。WS 不能證明 Maker 撤單／終態；即使全部數量已對沖，runner 仍會取得 REST 終態。私人連線世代改變、gap、衝突或未知送單會停止快速路徑，撤回尚未成交 Maker 並查證已知責任，不自動重新啟用。`recover` 固定走查證路徑，不用新 WS 事件擴大未知曝險。

**本批只有離線／loopback 驗收，沒有實網新成交樣本。** 先用 `rest_verified` 核對 WS／REST 一致性是建議，並非解鎖條件；使用者也可以在第一筆新計劃直接設定 `"maker_fill_mode": "ws_confirmed"`，實測後檢查成交、去重與延遲。這不是在原有 run-dir 修改模式後重新送單。

每次 submit／recover 正常收尾時，會在該 run-dir 寫入 `latency-*.jsonl`，只於最後列印檔案位置；不逐事件列印。正常與錯誤收尾均會輸出已有記錄。為免增加熱路徑磁碟負擔，時間事件先保存在有界限記憶體，強制殺程序可能遺失這份時間記錄；交易 WAL 仍然同步持久化。超過 32,768 筆會標示 dropped，不把缺失樣本當成零。

```powershell
python rust/tools/summarize_paired_latency.py rust/artifacts/run-001
```

摘要顯示每段的樣本數、p50／p95／p99 及最大值，包括 Rust 收包→解析、WS 收到→成交帳本完成、WS 收到→REST 核對、送單意圖帳本、對沖呼叫→ACK，以及正數成交確認。快速分支另提供該批 WS 觸發→對沖呼叫；先前未達最低量的碎量可能比該批更早出現，因此這不是完整曝險持續時間。

所有差值使用同一程序的 monotonic 時間。`submit_call` 位於原生簽名／HTTP 呼叫之前，**不是封包真正送出的時間**；ACK 不是成交；對沖 coordinator 觀察可能來自私人 WS 或其 REST 恢復，未冒充純 WS 延遲。來源交易所時間及 extension wall clock 僅供診斷，不拿來與本機 monotonic 時間相減。只有一兩筆成交時，p99 不能當成長期服務保證。

本次沒有改 extension，不需要為此重新載入。實測後保留完整 run-dir；可由時間檔與兩份 WAL 核對延遲、去重、剩餘曝險及 WS／REST 差異。不要將帳戶基準或憑證貼到公開位置。

2026-11-19 本批驗收：Rust workspace 369 個測試通過，另有 2 個時間分析器測試；Clippy、release 編譯及 CLI smoke check 通過。快速分支的兩所 mock 測試要求送單前不發出任何 REST lookup，並驗證重複 WS、REST 及重啟不重送，錯方向／價格／數量及私人 gap 不送單。沒有實際送單或撤單；實盤時間數值仍待使用者測試。
