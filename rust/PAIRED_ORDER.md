# Rust 跨所雙腿下單

這是 `place_order.py --engine rust` 的有界限手動下單入口。Python 負責互動輸入、載入 `.env`、準備公開資料及啟動 Rust；選擇判斷與交易執行在 Rust。Variational 透過現有 extension，Aster／Hyperliquid／Lighter 使用原生送單、私人 WS 及查單。Lighter 簽章由 Rust 直接呼叫固定版本的官方原生 DLL，不經 Python。預設 Python 引擎沒有切換；Rust 失敗不會回退到 Python 下單。

## 一次執行的互動下單

```powershell
python scripts/place_order.py --engine rust
```

未指定 `--rust-action` 時，依序輸入開／平倉、幣種、數量及開倉槓桿；顯示簡短交易摘要，最後一個 `[y/N]` 輸入 `y`／`yes` 才啟動實盤送單。不再執行全帳戶 audit，也不需要 reviewed 基準。拒絕、空白或 stdin 結束均不送單。平倉仍須明確指定原持倉的 `--short_venue`／`--long_venue`。

Variational 須保持商品頁面及 extension Start，送單前開啟「允許 Rust 操作頁面下單／撤單」。計劃及本次幣種的持倉起始數量存入 `rust/artifacts/order-<唯一 ID>/`。正常終端只顯示交易摘要、掛單價格／數量、成交進度、撤單與重掛；JSON 預覽及帳戶明細不列印。錯誤才顯示診斷及恢復路徑；完整訂單與時間紀錄仍保存於檔案。

只準備而不下單請明確使用 `--rust-action prepare`；下面的分步命令保留供診斷與恢復使用。互動模式不接受 `--live` 跳過確認。

交易檢查範圍與 Python 執行器一致：只讀本次幣種的兩腿持倉，檢查初始數量平衡及平倉方向／可平數量，成交後核對持倉增量。其他幣種的手動持倉、掛單及帳戶餘額不參與比對；不自動調整既有倉位。同幣種的手動交易仍會影響該幣種數量。成交後持倉端點延遲時最多讀取三次，間隔 250ms，不重送訂單。Aster 直接查指定 symbol 的 positionRisk；Hyperliquid／Lighter 只取帳戶回覆中的指定商品持倉；Variational 沿用 extension 查詢傳輸後只比對指定幣種。私人 WS、精確訂單身份、未知回覆查單及持久化恢復保留。獨立 `audit`／probe 仍可用於診斷，但不再是下單前置步驟。

## Aster／Hyperliquid／Lighter 原生雙腿

非 Variational 配對已接上 `native_perp_pair_v1` 計劃及相同的 preview／audit／submit／inspect／recover 入口，不需要 extension。支援 Aster、Hyperliquid 主永續市場及 Lighter 永續之間的不同交易所配對。Lighter 設定與驗收方式見 [LIGHTER.md](LIGHTER.md)。本批只新增 Lighter，未新增其他交易所。

```powershell
python scripts/place_order.py --engine rust --rust-action prepare --ticker ETH --short_venue aster --long_venue hyperliquid --rust-quantity 0.02 --rust-clip-quantity 0.01 --rust-leverage 1 --rust-plan rust/artifacts/native-eth.json
```

省略 SHORT／LONG 時，沿用下方 funding monitor 的候選排序；角色仍按 Python spread＋設定優先級決定，不固定 Aster 做 Taker。`--rust-quantity` 是總量，`--rust-clip-quantity` 是每批上限，省略則整筆；數量須符合兩所共同 lot。每批舊 Maker 已證實終態且所有成交已完成對沖核對後，才安排下一張。這是固定批量，未移植現貨腳本的按最佳檔容量自動縮量。

- Maker 買掛最新 bid、賣掛最新 ask，使用 PostOnly。價格改變且新價格符合門檻才追價；原單合格但新價格超標則保留原單。只改深度數量不觸發改單，但深度不足以對沖、原單價差超標、行情／私人連線失效及掛單期限仍可撤單。
- Taker 為 Aster 時送 MARKET；為 Hyperliquid 時按當下 mid ± 50 bps、向外捨入形成 IOC，與 Python `place_market` 的方式一致。`--rust-hedge-price` 明確提供時保留固定 IOC。Maker 成交後不再以入場價差拒絕對沖；IOC、交易所拒絕、網路故障或最小下單量仍可能留下待處理曝險。
- 已綁定訂單身份的私人事件增量寫入 WAL 後喚醒對沖，REST 查單並行；重複累計成交不會重送。撤單 ACK 不等於終態，必須查明最終成交量。回覆不明／NotFound 保留在途量，原 run 恢復不重送未知訂單。
- 開倉送單前，兩所設定逐倉及指定槓桿；平倉／audit／recover 不改槓桿。`--mode close` 指定原倉位 SHORT／LONG，兩腿反向、reduce-only，對帳檢查可平數量。
- 終端列印送單價格／數量／名義值、撤單原因及進度；結束保存 `latency-*.jsonl`、逐單 WAL、`orders.jsonl` 及 `finished.json`。原生路徑目前保存原始時間事件，既有 Variational 延遲分析器並未完整解讀原生配對事件。

設定檔沿用 `rust/artifacts/paired-settings.json` 的 `accounts.aster`、`accounts.hyperliquid`；Lighter 使用 `accounts.lighter` 的十進制帳戶 index 字串。可增加：

```json
"native_pair": {
  "max_gap": "0.0025",
  "hedge_slippage_bps": "50",
  "maker_wait_seconds": 30,
  "recovery_seconds": 60,
  "max_reprices": 1000
}
```

`max_gap` 使用該批對沖深度 VWAP、Maker 價格作分母；不自動讀取 Python `.env` 的價差預設。`maker_wait_seconds` 1–120、`recovery_seconds` 10–120、`max_reprices` 1–1000（含首張 Maker）、槓桿 1–125，交易所仍可能有更低上限。無合格入場報價 60 秒停止；單次執行上限一小時。原生 run 未解決時由 `rust/runs/native-pair-active.json` 阻止另一個新原生 run，勿刪除以繞過。

這批已以模擬交易所測試雙向開平倉、部分成交、慢 REST／快 WS、撤單競態、未知回覆、批量與重啟去重；尚未執行實盤送單。不包含資費掃描、自動策略進出場、Python position registry 或完整成交均價／費用／盈虧帳本，因此尚非整個 Python 程式功能等價替代。

2026-11-25 驗收：Rust workspace 445 個測試通過、相關 crate 的 Clippy（`-D warnings`）通過；Python 入口／選擇／計劃共 33 個測試通過，含既有角色選擇比對。ETH 即時公開資料的兩種 Maker／Taker 方向均通過原生 preview；release `paired-order.exe` 已重建。私人送單、撤單及槓桿設定僅使用 mock，沒有操作真實帳戶。驗收輸出保存於忽略提交的 `rust/artifacts/native-pair-*.log` 及 `native-pair-public-preview-validation.json`。

## Variational 配對範圍

2026-11-27：Variational 下單及撤單改為 **UI 操作**。extension 的直接 HTTP 送單／撤單已停用；查詢持倉、訂單及成交仍使用既有登入工作階段的唯讀來源。需要重新載入 `browser_extensions/variational` 並按 Start；舊版 extension 不具 UI 能力標記，Rust 不會授權其送單。

送單前使用與 Python 相同的頁面函式選 Limit／Mid、方向、Reduce Only 及數量，先停在 Submit 前。Rust 讀回實際 Mid 價，重新檢查入場價差並寫入本次計劃／帳本後，才允許點擊已準備的按鈕。若頁面被刷新或輸入改變，停止並顯示錯誤。CDP 在頁面請求離開瀏覽器前核對商品、方向、數量、價格、Reduce Only；只放行一筆符合意圖的請求。真正 RFQ ID 取自該請求的回覆，不以相似掛單列或持倉變化猜測。撤單也由頁面點擊觸發，實際請求須對應原 RFQ；取消 ACK 不等於成交終態。

獨立唯讀 UI 驗收（填表但不點 Submit，交易勾選可保持關閉）：

```powershell
rust/target/release/paired-order.exe ui-prepare --plan rust/artifacts/my-plan.json
```

送單未確認且無訂單身份時，保留帳本並報告原錯誤，不再假裝有掛單而繼續等到 `maker_timeout`。不自動重新點擊；未知結果仍需查證原 run。

- 每次執行一筆 Variational 固定限價 Maker；收到已驗證的成交增量後送出另一腿。Variational＋Aster 新計劃預設使用 Aster MARKET 對沖；Hyperliquid 目前仍使用明確限價的 IOC；Lighter 在 prepare 自動計算 IOC 限價後固定保存。
- 支援開倉及兩腿 `reduce_only=true` 的平倉，兩腿方向相反、基礎幣種和數量相同。Aster 使用單向持倉；Hyperliquid 使用主永續市場，不含 HIP-3／現貨。
- 價差、深度或行情不合格時阻止新 Maker；掛單期間不合格則要求撤回。已確認 Maker 成交仍保留對沖責任，不以入場價差阻止對沖。
- IOC 對沖仍受計劃中的明確限價、名義值及交易所最小量限制；不能保證全部成交。終態部分成交可繼續對沖剩餘量，UNKNOWN／NotFound 保留在途量，絕不盲目重送。
- Aster MARKET 使用市價數量規格，送單不帶價格或 timeInForce。計劃的 `limit_price` 此時只是估值參考，`max_notional_usd` 是按參考價檢查的名義值，兩者都不能限制實際市價成交的滑點或成交金額。部分成交與未知回覆沿用同一查證及持久化流程。
- 送單前同步寫入兩份 WAL；重啟讀取原計劃及身份。Ctrl+C 透過父程序控制通道要求撤回 Maker、查證及對沖已知成交。強制殺程序後必須使用同一目錄恢復。

沒有 Maker 自動追價／重掛、多批分單、資金費率掃描、自動進出場或完整費用／盈虧帳本，也不更新 Python position registry。這不是整個 Python `place_order.py` 的功能等價替代。

## 準備計劃

### 所有啟用交易所的配對及 Maker／Taker 選擇

```powershell
python scripts/place_order.py --engine rust --rust-action select --ticker ETH --rust-quantity 0.01
python scripts/place_order.py --engine rust --rust-action select --ticker ETH --rust-quantity 0.01 --short_venue aster --long_venue hyperliquid
```

`select` 是唯讀流程，不需要 extension 或帳戶設定。Python 讀取 `VENUE_CONFIG`、monitor 檔案、`configs/execution_venues.json`，並抓取兩所公開報價；Rust 負責候選排序、方向、spread 計算及角色選擇。預設只選啟用交易所、24 小時內且 `signal=true` 的最高年化訊號，年化相同保留原訊號順序。指定一腿會限制候選，兩腿都指定則不需要 monitor 檔案；不會因執行器尚未支援而暗中改選另一對。

角色規則與 Python `resolve_execution_legs` 一致：Variational 固定 Maker；其他配對任一 spread **大於** 0.1% 時取較寬的一方做 Maker，否則取設定優先級較高者；優先級相同時取較寬 spread，完全相同則 SHORT 一方做 Maker。spread 分母為各所 `(bid + ask) / 2`。Rust 使用十進制定點運算；0.1% 邊界不依二進制浮點誤差決定。這是 spread＋優先級規則，未加入動態手續費模型。`trade_xyz`、`entropy` 已補上與 Hyperliquid 相同的優先級 2；沒有設定優先級時會明確報錯。

支援 `--maker_venue`／`--taker_venue` 單邊或雙邊指定，必須是配對內不同交易所；Variational 不可改成 Taker。原生 Maker 買方參考最新 bid、賣方參考最新 ask；Variational 的真實 Maker 價格仍由後續 prepare 按 Mid 取得。Taker 參考買 ask、賣 bid，參考價不是成交保證。`--mode close` 必須指定原持倉的 SHORT／LONG，兩腿方向反轉並標示 reduce-only；即使停用新訊號的交易所也可以預覽既有持倉平倉。

結果保存為 `rust_execution_selection_v1` 報告，預設路徑為 `rust/artifacts/selection-<id>.json`，可用 `--rust-plan` 指定新檔名。報告列出選擇原因、spread、方向、參考價、spread 確認提示及 `paired_execution_supported`。**選擇報告不是可送單計劃**，Rust preview／submit 會拒絕它。所有啟用交易所均可參與選擇；可執行的配對是 Variational＋Aster／Hyperliquid／Lighter，以及 Aster、Hyperliquid、Lighter 之間的不同交易所配對。其他配對 prepare 時保存報告後明確停止，沒有自動回退 Python 下單。

驗證：`cargo test --manifest-path rust/Cargo.toml -p perp-arb-execution selection --locked`；先建立 debug paired-order 後執行 `python -m pytest tests/test_rust_selection.py -q`，涵蓋所有啟用交易所的有方向配對、兩種開平倉模式、spread／優先級矩陣，與 Python 結果比對。

### 沿用 funding monitor 選擇配對

本機 `rust/artifacts/paired-settings.json` 已設定 extension、三所帳戶地址，以及新計劃的 `maker_fill_mode="ws_confirmed"`。此檔不含私鑰且不納入 Git；既有計劃及交易帳本不受影響。第一次成交也可以使用此模式，無須先以 REST 模式成交解鎖。

```powershell
python scripts/run_funding_monitor.py 7 --required-venue variational
python scripts/place_order.py --engine rust --rust-action prepare --rust-plan rust/artifacts/my-ws-plan.json
```

若已有 24 小時內的 monitor 結果，不必重新執行 monitor（該命令仍會依原流程發 Telegram）。backfill 補的是歷史資料；自動選擇讀取的是 monitor 產生的 `data/monitor_signals.json`。

`prepare` 在 Variational＋Aster 配對詢問幣種、本筆數量及開倉槓桿；Hyperliquid 配對仍需指定 IOC 限價。Variational Maker 價格預設由 Rust 的 `mid-price` 唯讀指令，透過現有 extension 在該幣種頁面按 Limit／Mid 並讀取價格欄，不再要求手動輸入，也不使用公開報價的平均值代替按鈕。先讓 extension 連著所選幣種頁面並按 Start；此預覽不需要開啟「允許 Rust 操作頁面下單／撤單」。無連線、讀價失敗或無效價格會停止，不產生新計劃、不回退 Python 下單。現有 extension 已支援此預覽命令，無須為此更新 extension，但需重新編譯 Rust release。

取得 Mid 後將參考價格及對應名義值寫入新計劃，再呼叫 Rust 唯讀 preview，不送單。實盤 submit 會沿用 Python 的頁面流程，重新按 Limit／Mid 並填入數量，以當下 Mid 重新檢查入場價差，再將實際價格及名義值保存到本次執行帳本後點擊 Submit；audit 不操作填單。明確指定 `--rust-action prepare` 時預設為開倉；省略 action 則使用上方互動下單流程並詢問模式。先執行所有啟用交易所的選擇流程，保留訊號的 SHORT／LONG 方向，不再要求選方向。資料過期／沒有合格訊號時停止；選到未接上雙腿執行器的配對時輸出選擇報告並停止。

可用 `--ticker`、`--rust-quantity`、`--rust-maker-price`、`--rust-hedge-price` 取代互動輸入。IOC BUY 價格是最高買價，SELL 是最低賣價；固定 IOC 限價仍可能導致對沖未完全成交。價格及數量須通過 Rust 商品精度、最小量和最低名義值預覽。此流程只準備一筆，不會把輸入數量拆成多批。

Variational＋Aster 省略 `--rust-hedge-price` 才自動使用 MARKET；明確提供該參數則保留 IOC。既有計劃不會自動改成市價。其他配對依上方 Rust 選擇器決定角色，Aster 不固定為 Taker。

指定單邊 `--short_venue` 或 `--long_venue` 會限制自動候選；兩邊都指定則採用明確指定的配對，不必有 monitor 訊號。Variational 固定為 Maker；不支援的交易所會明確報錯。`--mode close` 必須明確填寫**原持倉**的 `--short_venue` 和 `--long_venue`，生成的兩腿反向且 reduce-only；不能用今天的最佳訊號猜測原倉位。下單前仍須對帳。

可用 `--rust-settings` 指定另一份私人設定，或用 `--rust-fill-mode rest_verified` 改變本次新計劃。這些準備參數不能用於 submit／recover 覆寫已保存的計劃。輸出檔必須不存在；不要覆寫已送單計劃。

開倉準備會詢問 `leverage_x`，也可用 `--rust-leverage 3` 指定。CLI 不再默默採用 `native_pair.leverage` 或 1 倍。原生配對設定兩腿逐倉槓桿；Variational 配對將倍數存為 `hedge_leverage`，只設定 Aster／Hyperliquid／Lighter 對沖腿，與 Python 一樣不修改 Variational 的保證金機制。數量不乘槓桿。輸入範圍 1–125，商品／交易所另有限制；設定或確認失敗時不掛 Maker。

preview／audit 不改槓桿；僅新開倉 submit 在掛 Maker 前設定並確認，平倉與 recover 不改槓桿。舊 Variational 開倉計劃缺少 `hedge_leverage` 必須重新 prepare 才能 submit；既有 run 仍可讀取和 recover。

準備成功後依下方唯讀對帳、核對基準及 submit 步驟實測，將範例的 `my-plan.json` 換成 `my-ws-plan.json`。時間記錄及分析方式見文末。

### 手動編輯完整計劃

先編譯目前入口：

```powershell
cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin paired-order --release --locked
Copy-Item rust/examples/paired-variational-aster.json rust/artifacts/my-plan.json
```

編輯 `my-plan.json`。範例帳戶、extension ID 及價格都是示意，不能直接用於交易。填入真實帳戶（小寫地址）、extension ID、商品、兩腿方向、相同數量、Maker 限價及對沖 IOC 限價／名義值上限。`run_id` 必須每筆新委託唯一，且等於 `maker.request_id`。`funding_interval_s` 需與 Variational 的實際商品一致。

`max_entry_gap="0.0025"` 表示以 Maker 價格為分母的不利價差上限 0.25%；入場使用整筆數量的對沖深度 VWAP，IOC 另檢查最末檔是否在限價內。MARKET 也必須通過入場價差及深度檢查，但 Maker 成交後不會以此門檻阻止對沖。此值不會自動讀取 Python 的 `.env` 價差預設。

若對沖所改為 Hyperliquid，設定 `hedge.venue="hyperliquid"`、`hedge.symbol="ETH"` 和該所帳戶。其他參數也要按商品規格調整；不能只換交易所名稱而保留不合規的價格或數量。

平倉要建立新 run：兩腿方向與各自既有持倉相反，**兩腿** `reduce_only=true`，數量不超過各自可平倉量。基準檢查會拒絕不足持倉；現有 Python 倉位記錄不會自動轉換為計劃。

Python wrapper 載入現有 `.env`。Aster 使用 `ASTER_API_WALLET_PRIVATE_KEY`；Hyperliquid 使用 `HYPERLIQUID_PRIVATE_KEY`，主帳戶／代理關係由既有原生 adapter 查證。不要把私鑰寫入計劃。直接執行 Rust binary 時須自行設定程序環境。

## 預覽及唯讀對帳

```powershell
python scripts/place_order.py --engine rust --rust-action preview --rust-plan rust/artifacts/my-plan.json
python scripts/place_order.py --engine rust --rust-action audit --rust-plan rust/artifacts/my-plan.json --rust-baseline-output rust/artifacts/observed.json
```

預覽只查公開商品規格；對帳需要已登入的 Variational 頁面、extension Start，以及另一所憑證。保持 extension「允許 Rust 操作頁面下單／撤單」關閉即可做唯讀驗收。三個 endpoint 為 `ws://127.0.0.1:8766`、`8767`、`8768`。

`audit` 是可選診斷，保存全帳戶觀測資料，`reviewed=false`。此檔案不再是送單前置條件，也不會作為本次交易的預期庫存。輸出包含私人帳戶資料，留在忽略提交的 `rust/artifacts/`。

新單不檢查全帳戶活躍委託；本次幣種仍要求單向持倉，不會替你撤掉手動舊單。

## 送單及恢復

確認交易計劃後，在 extension 勾選「允許 Rust 操作頁面下單／撤單」並重新啟動轉發。

```powershell
python scripts/place_order.py --engine rust --rust-action submit --rust-plan rust/artifacts/my-plan.json --rust-run-dir rust/artifacts/run-001 --live
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
