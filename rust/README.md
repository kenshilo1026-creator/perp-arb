# perp arb Rust 分批移植

2026-11-23 現貨／永續第四批：`run_spot_perp_arbitrage.py --engine rust --live` 已接通 **Aster／Hyperliquid Maker → Binance Spot 市價對沖**，包含 WS 部分成交、最佳一檔分批、追價撤單、持久化未知送單及相同 run 恢復。兩腿均由 Rust 執行；沒有實際送單驗收，實際延遲及 spread 等待使用者測試。操作、角色限制及未完成項目見 [現貨對沖](crates/spot-perp/README.md)。下列段落保留歷史批次範圍。

2026-11-23 現貨／永續第三小批：新增 `crates/binance-spot` 原生私人傳輸、佣金／累計成交協調與持久化單次送單許可；交易方法完成本機替身故障測試。新增 `--rust-binance-preflight` 唯讀帳戶／WS 入口。**尚未完成雙腿 spot/perp live CLI**；詳見 [Binance 私人元件與限制](crates/binance-spot/README.md)。

2026-11-23 現貨／永續第二小批：同一個唯讀入口加入 Hyperliquid、Lighter 原生公開 WS／商品約束，支援兩邊 Maker 選擇與最佳一檔數量計劃。**此入口仍不能實盤對沖**，Binance 私人執行器及 Variational extension 接線尚未完成。參見 [能力表與命令](crates/spot-perp/README.md)。

2026-11-23 現貨／永續第一批：新增 `crates/spot-perp` 與 `run_spot_perp_arbitrage.py --engine rust`。Binance Spot／Aster 公開商品規格、原生 BBO＋完整部分深度 WS、最佳一檔分單、Maker 選擇及保留／撤回／追價決策已移植。2,000 組 Python 決策回放一致；已連真實公開行情，沒有私人帳戶存取、送單或撤單。**目前只有唯讀預覽，不能用此入口實盤套利。** 詳見 [現貨套利批次與操作](crates/spot-perp/README.md)。

2026-11-19 速度優化批：新增明確選用的 Variational `ws_confirmed` 事件驅動對沖，與 Maker REST 查單分開排程；保留 trade ID 去重、送單前 WAL、未知送單與 gap 隔離。預設仍為 `rest_verified`，先取得真實成交樣本。新增收尾輸出的 monotonic 時間記錄與分析工具，使用方式見 [WS 快速對沖及實測](PAIRED_ORDER.md#ws-快速對沖及實測時間記錄)。本批沒有送出真實委託。

2026-11-19 第七批：已提供 `place_order.py --engine rust --rust-action ...` 雙腿入口，Variational 固定限價 Maker＋Aster／Hyperliquid 原生 IOC 對沖，包含獨立基準比對、兩份 WAL、部分成交、未知送單查證及同目錄恢復。兩組真實帳戶唯讀對帳通過；實際送單僅以 mock／loopback 驗收，尚未執行真實交易。使用方法及限制見 [雙腿下單](PAIRED_ORDER.md)。以下日期段落保留各批完成時的範圍。

2026-11-19 第六批：Variational 全帳戶雙重讀取及私人事件失效屏障已接入，真實唯讀查詢成功；新增 WS trade 與精確 REST 成交交叉驗證、送單前 Maker 帳本、RFQ 綁定及 Aster／Hyperliquid IOC 對沖 transport 接線。實際下單只做本機替身測試；真實即時成交尚無樣本，CLI 仍是唯讀，新增 `--rust-inventory`。詳見 [第六批與剩餘限制](VARIATIONAL.md)。

2026-11-19 第五批：Variational confirmed trade ID 去重已持久化，新增雙帳本 blocking owner，串接精確查詢、Maker 成交、Aster／Hyperliquid 對沖預留及未知送單恢復；涵蓋兩次 WAL 寫入間中斷。這是協調元件與故障測試，尚未接實際雙腿送單，CLI 仍只開放 Variational preflight。詳見 [第五批](VARIATIONAL.md)。

2026-11-19 第四批：Variational 已加入真實 RFQ／成交查詢來源、一次性原生送單／撤單 transport、extension 持久化回覆及未知送單身份恢復；`place_order.py --engine rust --rust-preflight` 可直接啟動 Rust 唯讀驗收，不經 Python broker。真實帳戶歷史委託與成交已讀取比對；送單／撤單只做模擬，尚未開放雙腿實盤。詳見 [接入範圍與實測](VARIATIONAL.md)。

同日下一批：補上 browser Maker 送單／撤單持久化狀態、查單與重啟隔離，以及已驗證累計成交到既有 Aster／Hyperliquid 紙上對沖協調器的交接；修正 `/portfolio` 巢狀 trade 辨識。這是執行核心與協議準備，還沒有開放 Variational 真實送單。

這是獨立的 Rust 重寫目錄，目標是降低行情處理、成交事件協調與對沖送單前的本機延遲。目前包含計算核心、Aster／Hyperliquid 公共行情、商品精度解析，以及兩所私人 WS／事件協調／獨立 REST 查單。原有 Python 程式照常使用；Rust 新增受控單筆下單測試入口。

目前進度（2026-11-12）：4b-2 提供 `order-test`，涵蓋公開商品預覽、單筆限價送單、私人事件觀察、按 client ID 查單、到期撤回剩餘量及重啟恢復。完整委託先同步帳本才送出，未知回覆不自動重送；已成交部分不會自動反向平倉。支援 Aster v3 mainnet 與 Hyperliquid mainnet／testnet，尚未接成兩腿自動套利或移植 Variational。詳見 [操作說明與範例](crates/order-test/README.md)、[驗收結果](VALIDATION.md) 及 [執行協調器範圍](crates/execution/README.md)。

```powershell
.\rust\target\release\order-test.exe --help
```

## 第一批範圍

- `crates/core`：十進位價差、不利價差、價格步長取整、完整數量 VWAP、最差一檔成交價、行情新鮮度及連線健康檢查。
- `crates/replay`：純離線 JSON 計算入口，供 Python 行為比對與本機微基準使用。
- `crates/private-events`：Aster／HL 私人訊息與 REST 正規化、每帳戶單一協調器、累計去重、特殊事件與庫存對帳；已接兩所唯讀 transport 及紙上執行協調器。
- `crates/aster-private`：Aster v3 認證、listenKey／私人 WS、獨立查單及顯式唯讀 probe。
- `crates/hyperliquid-private`：地址限定的四 channel 私人 WS（委託、成交、funding、其他帳戶流水）、快照／成交去重、獨立查單、顯式唯讀 probe。
- `crates/execution`：紙上單筆 Maker／對沖協調、兩腿私人快照橋接、對沖預留量與終態後剩餘量；可選持久化帳本、查證票據與重啟恢復，不含交易網路請求。
- `crates/order-test`：單筆真實永續限價委託測試；`--live` 才載入金鑰與交易，預覽只讀公開資料。
- `crates/variational-bridge`：直接接收 extension 三條 WS；原生 RFQ 查詢、一次性送單／撤單傳輸及有界限雙腿 CLI。變更請求需 WAL permit、`--live` 及 extension 顯式 opt-in，預設關閉。UI ACK、持倉變化及未驗證 trade candidate 不授權對沖。
- `tools/verify_parity.py`：直接呼叫現有 Python 函式，產生 571 個固定／固定種子案例，再與 Rust 結果比較。這是驗證工具，不是 Rust 執行時的 Python 依賴。
- `tools/inventory.py`、`migration-inventory.json`：逐檔記錄既有功能、來源雜湊與移植批次，避免只搬核心而遺漏 CLI、風控或 UI。

金額使用 `rust_decimal`，不是 `f64`。該型別使用 96 位整數及十進位尺度；輸入以 `from_str_exact` 拒絕不可表示的數值，運算使用 checked 方法處理溢位。其除法精度與 Python 預設 28 位有效數字並非完全相同。[套件文件](https://docs.rs/rust_decimal/latest/rust_decimal/)

價格取整另提供 `RoundingInput`：Python 中間值可能超過 28 個小數位，先以 checked `i128` 保留原始十進位係數／尺度，再對正數 tick 做精確整數商餘數取整，最後轉回 Decimal；不先四捨五入輸入。係數超出 i128、尺度超過 38、對齊溢位或結果不可表示仍明確報錯。原先失敗的 22 個案例完整保留，已全部通過。

比對要求價格步長、最差成交價、布林值與深度不足結果完全一致；除法結果容許 `1e-24 + abs(Python 結果) * 1e-26` 的表示誤差，並記錄每個非完全相同的結果。**這項容差只用於本批計算測試，不能直接用作下單門檻容差。** 進出場門檻、費用與 Maker 限價尚未移植，下一批涉及決策前須對門檻上下相鄰 tick 另做嚴格驗證。

與 Python 的刻意差異：Rust 分開 `Depth::Unsized` 與 `Depth::Visible([])`。前者只供刻意不發布數量的報價；後者代表沒有可見流動性，帶數量的查詢回傳無法成交。報價建構時也拒絕交叉盤、深度與 BBO 不一致、非正數層及未依價格排序／重複價格。零／負下單量直接報錯。這些差異有 Rust 測試，不混入「完全等價」的案例數。

## 執行驗證

先安裝 Rust stable 的 Windows MSVC 工具鏈及 C++ Build Tools；本機已找到 Visual Studio 2022 C++ 工具。安裝方式見 [Rust 官方說明](https://rust-lang.org/tools/install/)。若系統原則阻擋，需由管理員允許，勿關閉安全政策。

在專案根目錄執行：

```powershell
rustup component add rustfmt clippy
powershell -File rust/tools/check.ps1 -Format
```

`check.ps1` 依序格式化、編譯、測試、lint、建立 release、驗證比對工具、跑 Python/Rust 計算與合成行情比對及檢查清單；任何一步失敗即停止。腳本不連接交易所。也可以逐項執行：

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo test --manifest-path rust/Cargo.toml --workspace --locked
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo build --manifest-path rust/Cargo.toml --workspace --release --locked
python -m unittest discover -s rust/tools -p "test_*.py"
python rust/tools/verify_parity.py --benchmark
python rust/tools/verify_market.py
python rust/tools/verify_shared.py
python rust/tools/verify_instruments.py
python rust/tools/verify_private.py
python rust/tools/verify_aster_auth.py
python rust/tools/verify_hl_trade_signing.py
python rust/tools/verify_hyperliquid_private.py
python rust/tools/verify_execution.py
python rust/tools/inventory.py --check
```

已產生 `rust/Cargo.lock`，應隨程式一起提交；驗收使用 `--locked` 防止無意更新依賴。`--python-only` 只準備 Python 基準，不代替跨語言驗收。

結果存放 `rust/artifacts/`，不加入 Git；包含來源雜湊、案例、結果差異及執行檔雜湊。不得用過時的 release 執行檔驗證新原始碼：修改後先重建，再跑比對。

計算微基準在解析完成後量測 21 組、每組 100 次全案例迭代；輸出的是每組每案例平均耗時的中位數／第 95 百分位。**不是單事件尾延遲，也不是兩所成交時間差。** 不包含 WS、JSON 解析、簽名、網路、撮合及磁碟。本次中位數 Python 0.964 µs、Rust 0.0703 µs，約 13.7 倍；不能推論既有約 200 ms 的網路／交易所回覆也會同幅縮短。

## 第二批第一步 公共行情

`crates/market-data` 提供 `market-probe` 及 `market-replay`。Aster 固定使用 `depth10@100ms` 的部分深度快照，Hyperliquid 使用 `l2Book`、`fast:true`。每則快照整體替換深度，非增量拼接；不使用 Aster diff-depth。保留來源時間與本機單調時鐘接收時間，拒絕倒退序號／時間、錯誤幣種及不合法深度；斷線清除快取、指數退避重連、處理 ping/pong。超過未來時間容忍的訊息會清除快取，避免錯誤時間戳壓住後續正常行情。

錄製使用有界佇列與獨立磁碟執行緒；滿佇列明確失敗，既有檔案不覆寫，沒有成功 footer 的資料不得通過驗證。`complete` 表示錄製正常結束且兩邊有快照，不保證網路無缺口；需一併查看 health、errors、reconnects。

```powershell
New-Item -ItemType Directory -Force rust/artifacts
./rust/target/release/market-probe.exe --seconds 30 --output rust/artifacts/eth-new-session.jsonl
python rust/tools/verify_market.py --recording rust/artifacts/eth-new-session.jsonl --output rust/artifacts/eth-new-session-validation.json
```

預設 ETHUSDT／ETH，也可傳入 `--aster-symbol` 與 `--hyperliquid-coin`；兩者是交易所原生名稱，Hyperliquid 大小寫必須正確。2a 錄製器保持單一深度訂閱，供原始訊息回放；下面的 2b 各批提供另行驗收的共享訂閱、商品解析與 metadata 查詢。策略及面板連接仍待後續小步。

協議來源：[Aster 公共 WS](https://asterdex.github.io/aster-api-website/futures/websocket-market-streams/)、[Hyperliquid 訂閱](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)。

## 第二批共享行情小步（2b-1）

`shared::MarketState` 同時保存 BBO 與整份深度快照。2b-1 的初版 `MarketHub` 每個原生商品開一條 WS；2b-2c 已改為每個交易所一條，詳見後面的多商品小節。Aster 訂閱 `depth10@100ms`＋`bookTicker`；Hyperliquid 訂閱 `l2Book fast:true`＋`bbo`。Aster 大小寫別名合併；HL 保留 `kPEPE` 等原生大小寫，不據此推斷跨所單位相同。

每份報價透過 `Arc` 共用，`watch` 喚醒消費者並保留最新版本，不累積無界行情佇列。慢消費者可能略過中間公共快照；這個通道**不得用來傳遞逐筆私人成交**。每次使用必須取最新 `Subscription::snapshot()` 再呼叫 `View::fresh(now)`；`selected()` 僅做快照選擇，未通過新鮮度檢查。沒有新訊息時也必須重新檢查年齡。

- BBO 價格或數量改變時，只承認其一檔可見量，不把舊深度接在後面。
- BBO 未變時，保留舊深度原始時間，不延長其可用期限；深度快取超過 5 秒才退回 BBO，仍需通過新鮮度檢查。
- 價格改變後回到原價，不能恢復先前失效的深度；須等較新來源時間的深度快照。相同來源時間但內容衝突，保守使用 BBO。這兩項比 Python 更嚴格，有獨立 Rust 邊界測試，不算作完全等價案例。
- 斷線／不合法資料／未來時間戳先清空兩種快取，再關閉連線與退避重試；舊連線世代的訊息和清除請求不影響新世代。
- 最後一位商品訂閱者離開時撤訂該商品；該交易所已無訂閱者時停止 socket。`shutdown()` 等待全部任務結束並清空快取。握手、發送均有逾時；商品只有心跳但沒有可用行情，15 秒後觸發連線復原。

2b-1 原型的 8 個商品／各自連線限制已由 2b-2c 取代，目前每個 venue 最多 32 個商品共用同一 socket。限流作用於同一 hub，不協調不同程序或同 IP 的其他程式。

```powershell
python rust/tools/verify_shared.py
./rust/target/release/shared-probe.exe --seconds 30 --output rust/artifacts/shared-new-session.json
```

`shared-replay` 接受離線 NDJSON 的 begin／frame／query／disconnect 事件；`verify_shared.py` 直接呼叫既有 Python `QuoteStore`／`StoreFeed`，比對價格、數量、來源時間、新鮮度、完整數量 VWAP、最差成交價及深度不足結果。560 筆事件是行為回放，並非收益回測。HL BBO 欄位在驗證器做協議正規化後交給原有快取選擇函式。

`shared-probe` 固定以 ETHUSDT／ETH 建立四個消費者，實際共用兩條公開行情連線；輸出僅是短期連線摘要，並不錄製原始資料。它要求兩所 BBO、深度均有接收且曾提供新鮮報價，結束時確認快取清空；`watch_notifications` 可能合併，不能當成原始收包數。本機 TCP WebSocket 替身另驗證斷線、錯誤訊息、重訂閱、心跳下行情停滯、訂閱釋放與握手取消；仍未完成長時間實網故障驗收。

## 第二批商品與精度小步（2b-2a）

`core::instruments` 提供 Aster tick、Hyperliquid 五位有效數字／`6 - szDecimals` 小數位及整數價格例外、數量向下取整、最小／最大數量與最低名義金額檢查、價格上下限及共同數量步長。共同 lot 使用最小公倍數：例如 `0.002` 與 `0.003` 的共同步長是 `0.006`。

`market-data::catalog` 接受已取得的公開 JSON，產生原生 `MarketKey` 與靜態商品規則。Aster 分開保留 `LOT_SIZE`／`MARKET_LOT_SIZE`，缺少市價規則時回傳未提供；不以 `quantityPrecision` 猜步長。HL 保留 `kPEPE` 等大小寫及原始 universe index，包含前面的下架項目，避免錯誤壓縮 asset ID。此階段僅限主 dex；HIP-3 仍需獨立 metadata／asset ID 邏輯。

原生名稱會優先精確匹配；以 base symbol 查 Aster 時仍依現有 `USDT → USDC → USD` 順序挑選交易中合約。明確指定 `BTCUSD1`／`BTCU` 等原生名稱可讀取規格，報價幣與結算幣保持 USD1／U。此延伸不將不同穩定幣視為等值，不修改現有 Python 的自動選幣行為。中文原生名稱保留原字，僅在 Aster WS 網址中將 UTF-8 編碼。

名稱映射不代表單位換算。`1000PEPE` 與 `kPEPE` 映射到 PEPE 後，`units()` 預設未知；`common_limit_base_lot()` 會拒絕，必須先以外部核實的倍率呼叫 `with_verified_units()`。數量乘倍率、價格除倍率，除不盡或無法精確表示均報錯。共同 base lot 只判斷數量相容，不代表費用、報價幣、合約標的或策略條件已通過。

中間名義金額及價格容忍值使用 `ExactAmount` 保留 checked `u128` 係數與小數位，避免超過 Decimal 的 28 小數位時被捨入。係數溢位仍報錯。單位換算結果必須能精確放回 Decimal；共同 lot 使用 checked `i128`，範圍超出時明確失敗。沒有用浮點或容差放寬最低名義金額門檻。

取整不等於委託已通過檢查：小於步長的數量／向下取整的極小價格可以變成零，呼叫者仍須檢查 `size_error()`／`price_error()`。目前檢查是新增風險前的靜態工具；私人訂單生命週期、動態價格範圍、reduce-only 例外及 dust 處理尚未接入，不能用它停止已成交 Maker 的必要對沖。

```powershell
python rust/tools/verify_instruments.py
# 只有這個工具會連線；每個公開端點查詢一次，既有檔案不覆寫。
python rust/tools/capture_instruments.py --output rust/artifacts/instruments-new.json
python rust/tools/verify_instruments.py --recording rust/artifacts/instruments-new.json --output rust/artifacts/instruments-new-validation.json
```

`instrument-replay` 是離線 JSON 入口；驗證器直接呼叫 Python Instrument、common_lot、映射及 metadata 解析函式。原生 Aster 查詢會把選定 row 注入 Python 現有 filter parser，因為舊 base resolver 不支援 USD1／U；此部分不宣稱舊 resolver 已具備相同行為。最大數量與市價 filter 欄位另從同份 metadata 正規化驗證。缺失、重複、錯誤型別、停牌、單位未知與 offset grid 拒絕策略有獨立 Rust 測試。

2b-2a 原先只接受提供的 JSON；2b-2b 已補上 Rust 公共 HTTP 抓取與刷新，2b-2c 再加入多商品訂閱及 WS 限流。交易策略連接仍待後續批次。

規則依據：[Hyperliquid tick／lot](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size)、[HL 最低名義金額錯誤](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses)、[Aster 官方 futures API](https://github.com/asterdex/api-docs/blob/master/V3%28Recommended%29/EN/aster-finance-futures-api-v3.md)。

## 公共商品資料抓取、快取與共用限流（2b-2b）

`market-data::metadata` 的 `PublicHttp` 僅接受 Aster v1／v3 exchangeInfo 與主 dex Hyperliquid meta 三種固定公開查詢。共用 reqwest 連線池，關閉自動轉址、底層自動重試與環境 proxy；不讀取金鑰。v3 用於新路徑，v1 保留作相容性比對，正常部署毋須同時刷新兩版。

同一執行時建立一個 `PublicHttp` 並 clone 給所有 metadata 快取。Aster v1／v3 共用一個滾動 60 秒權重預算及冷卻狀態，Hyperliquid 有自己的預算；每所最多一個進行中的 HTTP 查詢。預設本機配額為 Aster 120、HL 600 weight／分鐘，每次分別消耗 1／20，請求開始間隔至少 100 ms。這是保守的本機分配，**不會協調其他 Python 程序、其他 `PublicHttp::new()` 或同 IP 的其他機器**，也尚未涵蓋 WS 或未移植的 API。

429／503 若有 `Retry-After`，支援秒數及 HTTP 日期並共用冷卻；無有效值的 429 等待 60 秒。418 立即回報、不在同一呼叫重試；無有效等待值時冷卻 180 秒。收到 Aster `X-MBX-USED-WEIGHT-1M` 達到本機分配額時，保守等待一整分鐘。5xx／連線失敗最多三次，退避 1、2 秒；單次 HTTP 預設 15 秒，整次含排隊、冷卻及讀取最多 45 秒。總逾時不清除共用冷卻，後續呼叫仍須等待；不把伺服器的長等待值截短。回應上限 8 MB，非 JSON、空清單或重複商品身分明確失敗。資料解析及 body 讀取錯誤不在同次呼叫重試。

同一 source 建立一個 `MetadataCache`，消費者 clone 該物件，共用資料與刷新鎖。`run(stop)` 初次立即載入，預設每 5 分鐘刷新；`current()`／`resolve()` 只讀記憶體，15 分鐘即過期，每次讀取都重新檢查 monotonic age。刷新成功原子替換整份資料與 generation；失敗立即標為不可用，5 秒後才容許下一次刷新，連 `force=true` 也不能繞過退避或限流。重疊刷新合併成一次，失敗結果亦共用；取消公開查詢不留下卡住的 refreshing 狀態。停止通道送 true 或關閉後，背景 task 可取消並由呼叫者 await 回收。

generation 代表刷新結果版本，失敗亦會遞增。保留解析結果的呼叫者須在使用前再做 `is_current_generation()`；這只是當下有效性檢查，並非交易所鎖或可永久使用的憑證。`health()` 提供 age、版本及失敗／未載入／過期原因。單個商品不受支援或缺少 filter 時在 resolve 拒絕，其他商品仍能接收最新下架及精度變更；整份身分清單損壞才拒絕發布。沒有跨重啟的磁碟快取或 HTTP metadata 版本排序，age 是本機收到資料後的年齡。

```rust,ignore
// 在既有 Tokio runtime 中；下例只示範背景生命週期。
let http = PublicHttp::new(HttpPolicy::default())?;
let aster = MetadataCache::new(http.clone(), Source::AsterV3, CachePolicy::default())?;
let hl = MetadataCache::new(http, Source::Hyperliquid, CachePolicy::default())?;
let (stop, rx) = tokio::sync::watch::channel(false);
let worker_a = aster.clone();
let worker_h = hl.clone();
let rx_h = rx.clone();
let a = tokio::spawn(async move { worker_a.run(rx).await });
let h = tokio::spawn(async move { worker_h.run(rx_h).await });
// 策略只讀快取；尚未載入時回報不可用。停止時：
stop.send(true)?;
a.await?;
h.await?;
```

以上新鮮度及規格檢查只適合新增風險前的判斷；未接上交易執行器，不能用來中止既有 Maker 成交的必要對沖。快取的 `resolve()` 仍執行記憶體內的規格解析；snapshot 讀取微基準不包含這部分成本。

```powershell
# 完整離線／本機故障注入驗收，不會連到交易所。
powershell -NoProfile -File rust/tools/check.ps1
# 明確執行時才連到三個公開端點；輸出檔必須尚不存在。
./rust/target/release/metadata-probe.exe --output rust/artifacts/metadata-new.json
python rust/tools/verify_instruments.py --recording rust/artifacts/metadata-new.json --output rust/artifacts/metadata-new-validation.json
```

`metadata-probe` 每端點首次載入後執行 1,000 次 snapshot 讀取及 20 次未到期 refresh，要求零額外 HTTP，並核對 ETH 規格；錄製解析後 JSON，供前批同一驗證器比對。429／418 使用本機伺服器注入，沒有故意衝擊真實交易所限流。

權重與狀態碼依據：[Aster 官方 futures v3 文件](https://github.com/asterdex/api-docs/blob/master/V3%28Recommended%29/EN/aster-finance-futures-api-v3.md)、[Hyperliquid 官方限流文件](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)。本批讀取重試只適用這三種公開查詢，不可沿用至未知結果的下單請求。

## 多商品共用 WS、限流與訂閱交接（2b-2c）

`hub::MarketHub` 現在為每個 venue 維持一個 owner task／socket，在該 socket 動態訂閱最多 32 個原生商品的 BBO 與深度。同商品的多個 `Subscription` 共用 `Arc<View>`；不同商品各有 `MarketState`、序號、新鮮度及 15 秒資料 watchdog。Aster 使用 `/ws` 與帶 id 的 SUBSCRIBE／UNSUBSCRIBE，每個控制訊息包含該商品兩個 stream；HL 每個商品發送兩個控制訊息。原始市場 JSON 只解析一次後依原生 symbol 分流，沿用既有深度與 BBO 選擇規則。

控制訊息每個 venue 同時只允許一個等待 ACK，確認 id／method／coin／channel；HL depth 亦確認 `fast:true`。兩個頻道都確認後，才接受該商品行情，ACK 本身不算有效報價；確認前或撤訂後的行情會丟棄並計數。缺少、重複、錯配或錯誤 ACK 會清空該 socket 的全部商品並重連，不猜測訂閱已成功。等待速率額度／ACK 時繼續讀取其他商品；單次 socket 寫入仍有 2 秒逾時，未宣稱在網路寫入阻塞時有硬即時保證。

| 本機預設 | Aster | Hyperliquid |
| --- | --- | --- |
| 一般控制／主動心跳最小間隔 | 250 ms | 100 ms |
| 全部送出訊息滾動預算 | 8／秒 | 600／分鐘 |
| 一般訊息保留的 pong 額度 | 2 格 | 2 格 |
| 新連線嘗試 | 5／分鐘 | 5／分鐘 |
| 訂閱 ACK 逾時 | 5 秒 | 5 秒 |

額度含 subscribe、unsubscribe、主動 ping 及回覆 pong，重連不重置。一般訊息在全部訊息額度中預留兩格；pong 只在尚有總額度時立即回覆，超額則直接丟棄連線，避免隱性自動 pong 繞過限制。每 20 秒主動心跳，重連退避 1 至 30 秒。這些是本機保守配額，未與其他 hub、舊錄製器、Python 或同 IP 流量共用；不是整個 IP 的全域限流。官方限制依據：[Aster futures WS](https://github.com/asterdex/api-docs/blob/master/V3%28Recommended%29/EN/aster-finance-futures-api-v3.md)、[Hyperliquid 限流](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)。

增添新商品不重連；移除某商品最後一位消費者時先清空／停止接收該商品，再優先發送 unsubscribe，其他商品仍使用原連線。`handoff(&mut subscription, next_key)` 只替換該消費者；若目標已有人訂閱，可直接共用其現有有效報價，否則新 view 等待行情。舊商品若仍有其他消費者就保留。每 50 ms 檢查最後一位消費者釋放；沒有消費者時中止握手／socket，但保留 venue worker 及額度供再次訂閱，直到 hub 停止。

**同名商品移除後重新加入，或 metadata 版本更新，會重建該 venue 的整條 socket。** WS 行情不帶本機訂閱世代，僅靠 unsubscribe ACK 無法證明後續同名行情一定屬於新訂閱，因此採用斷開舊 socket 的隔離方式。代價是同 venue 其他商品短暫不可用，恢復前必須重新 ACK 並收到新行情。一般 ETH→BTC 消費者交接不需重連。舊商品記錄清除，單連線曾使用的名字超過 256 時亦換新連線，避免長期切換的狀態無界累積。

若需要規格生命週期交接，在開始該 venue 的訂閱前呼叫 `hub.bind_metadata(cache.clone(), mappings)`，並另外啟動上一批的 `cache.run(stop)`。owner 對快取的有效性／版本進行記憶體檢查；規格解析只在版本變更或新增商品時執行。metadata 失敗／過期時清空並暫停；恢復後建立新 socket，只訂閱仍可解析且原生 key 相符的商品，下架商品保持不可用。每則行情在發布前再檢查 metadata 版本；沒有行情時靠 50 ms maintenance 檢查。消費端仍需在新增風險前檢查 metadata、行情與自身規則，不能把 watch view 當成原子交易鎖。

```powershell
./rust/target/release/multiplex-probe.exe --output rust/artifacts/multiplex-new.json
```

上述工具先抓 Aster v3／HL 公開 metadata 並綁定快取，分 5／8／5 秒三階段測試 ETH、加入 BTC、將 ETH 消費者轉到 BTC。要求四個市場都有新鮮 BBO／深度、每所全程僅一次連線、unsubscribe ACK 完整、零故障及結束時清空。它保存摘要，不錄製原始封包，也不量測下單或收益。

`HubStats.connections_started` 計數連線嘗試，`handovers` 計數需要重建 socket 的隔離交接，不計一般消費者 `handoff()` 呼叫。`View.generation` 是各市場狀態世代，不等於連線數；`shared-probe` 已改為分開輸出 market generation 與 transport counters。

任一商品停滯、錯誤市場資料、ACK 故障或連線斷開，會清空整個 venue 後重連；單一商品故障會影響同 socket 其他商品，這是本批共享架構的明確取捨。其餘交易所、分片多 socket、長時間穩定性、跨所時鐘估算、策略／面板及私人事件接入仍未完成。舊 `market-probe` 錄製器保持獨立兩條深度 socket，不受此 hub 的額度管理。

## 後續分批驗收

每次只完成一個可驗證批次；未通過本批驗收不把後一批切入實盤。詳細拆分與不可遺漏條件見 [移植計劃](MIGRATION.md)。

第一批不宣稱已移植 Config、帳本、進出場策略、訂單協調、交易所 adapter、歷史回填、資金費率、Telegram、風控、UI 或瀏覽器橋接。沒有執行真實下單或更新倉位檔案。
