# perp arb Rust 移植計劃與驗收

2026-11-23 spot/perp 第四批：原生雙腿 live/recover owner 已接入 Aster／Hyperliquid Post-only Maker 與 Binance Spot MARKET hedge。新增 hashed run WAL、對沖數量預留、雙重 dispatch 紀錄、私人累計量觸發、取消競態／未知回覆查證、佣金淨 base delta 持久化與最終時間摘要。Python 保留預設，Rust 實盤需明確 `--live` 及確認文字；不得宣稱真實成交延遲或 spread 已改善。Binance Maker 反向路徑、Lighter／Variational spot hedge、槓桿管理、全帳戶資產／PnL、細碎成交自動消除未完成。以下是各批歷史範圍。

2026-11-23 spot/perp 第三小批：Binance Spot HMAC REST、私人 WS 簽名訂閱、LIMIT_MAKER／MARKET 傳輸、精確查單、一次送單持久化、佣金去重與 shared cumulative actor 接線已完成元件實作。替身測試涵蓋未知送單、重啟、取消成交競態及 WS 新成交／REST 舊回覆。新增明確唯讀 preflight；尚未完成現貨雙腿 owner、淨庫存持久化及實盤交易驗收。原 one-way perp durable engine 保持拒絕 spot maker，避免錯用庫存模型。

2026-11-23 spot/perp 第二小批：加入 Hyperliquid BBO／l2Book 及 Lighter ticker、原生商品精度與名義值約束，統一最低不利價差優先的選腿、開平倉方向與舊單維持決策。6,000 個 Python 共同決策函式回放（每所 2,000）零差異；不是歷史損益回測，也不表示 Python 舊入口的 HL／Lighter 以 spread 優先選腿已逐字移植。Binance 私人傳輸與雙腿實盤、Lighter 下單、Variational extension 此入口接線仍未完成；不得視為實盤對沖驗收。以下保留先前各批交付範圍。

2026-11-23 spot/perp 第一小批：新增 Binance Spot／Aster 原生公開行情及同一檔深度計劃，接上 `run_spot_perp_arbitrage.py --engine rust` **唯讀預覽**；不是 live 執行器。2,000 個 replay 案例比對選腿、數量、價格、價差與 resting action；公開 ETH 行情短測通過。待完成 Binance 私人事件、送單／撤單／精確查單、費用與庫存處理、雙腿 WAL 與重啟恢復、撤單成交競態及其餘永續交易所，才可宣告腳本功能等價。參見 [spot-perp](crates/spot-perp/README.md)。

2026-11-19 第七小批：正式接上 bounded paired-order CLI 及 Python `--engine rust` handoff；Variational 固定限價 Maker＋Aster／HL 原生 IOC、獨立基準對帳、私人事件接收、持久化與同 run 恢復。兩組真實唯讀對帳成功，送單仍只完成替身驗收。自動重掛、零成交取消終態、WS-only 快速對沖及策略／盈虧帳本未完成。操作與範例見 [PAIRED_ORDER.md](PAIRED_ORDER.md)；以下段落保留先前各批範圍。

2026-11-19 第六小批：全帳戶持倉／抵押金／pending RFQ 雙讀及私人事件屏障完成；真實空持倉帳戶來源驗收成功，獨立預期庫存仍須由操作端提供。WS trade 逐筆對照精確 REST 的路徑已實作、故障測試通過，但本次沒有實際新成交可驗證。library 已接 native Maker submit/cancel 與 Aster／HL IOC hedge transport，保存完整 wire order 再送出；尚無正式雙腿 live CLI／重掛排程。詳見 [第六批驗收](VARIATIONAL.md)。

2026-11-19 第五小批：完成 confirmed trade ID 與累計量的原子持久化，以及專用 blocking worker 的雙帳本協調，補上分頁去重、重啟與未知對沖恢復。兩所對沖流程均有故障測試；實盤 transport 排程、真實 Variational 全帳戶對帳及取消終態尚未接妥，CLI 未開放雙腿交易。詳見 [第五批驗收](VARIATIONAL.md)。

2026-11-19 第四小批：已從真實登入 session 驗證 `/me`、`/orders/v2`、`/trades`，按 `rfq_id`／`source_rfq`／trade `id` 精確關聯，拒絕同商品替代。新增 browser-session 原生 limit／cancel 傳輸、extension 重複 dispatch 屏障、持久化 submit receipt 與重啟身份恢復；變更傳輸僅模擬驗收，預設關閉。`place_order.py --engine rust --rust-preflight` 已真實唯讀驗收；雙腿 live owner、帳戶完整對帳及取消終態仍待接線，不得把這個 CLI 選項當作交易引擎已完成。詳見 [第四批驗收](VARIATIONAL.md)。

2026-11-19 Variational 第一小批：v2 公開參考行情／共用 HTTP 快取與限流已接入，行情 crate 91 個離線／loopback 測試通過。詳見 [VARIATIONAL.md](VARIATIONAL.md)。後續保留 browser extension，接 Rust 私人成交協調與其他交易所對沖；此批尚無 Variational 下單或雙腿實盤能力。

2026-11-19 第二小批：`variational-bridge` 已直接接收現有 extension（不經 Python broker），完成三條 WS 連線、180 秒持倉錄製、唯讀掛單查詢與重連觀察。ACK／UI 掛單列／未驗證 trade candidate 不轉成成交。`place_order.py` 開平倉移植仍待送單持久化、精確成交歸屬、撤單終態與雙腿執行器接線；不能把本批唯讀橋接當成已支援 Variational 實盤。

2026-11-19 第三小批：`execution::durable::browser` 已補完整限價 Maker 意圖、單次送單許可、撤單待確認、累計成交、版本限定查證、持久化與重啟恢復；已驗證的訂單快照可交給原有 `DurableEngine` 產生 Aster／HL 對沖預留量。原始 extension 訊息尚未具備已驗收的帳戶／訂單身份與查單來源；本批以合成證據做故障測試，沒有開放實盤傳輸或把 `place_order.py` 切換為 Rust。

基準為 Python commit `2f0dbdc803e0723887a7efb061548b4e592bd168`。所有重寫內容放在 `rust/`。私人事件進行至 3c-1 的庫存對帳，4b-2 加入 Aster／Hyperliquid 單筆限價交易測試入口，尚未接成自動兩腿套利。兩所短期唯讀結果與仍待核對的特殊事件分開記錄。其他階段逐批進行。原本未提交的 `data/position_registry.json` 與未追蹤 `variational-v1/` 保留原狀。

## 功能對照

| 批次 | Python 功能範圍 | Rust 工作與驗收門檻 | 狀態 |
| --- | --- | --- | --- |
| 1 | `spread_strategy/core.py`、`feeds.py`、`maker_price_guard.py` 的純計算子集 | 十進位、深度、新鮮度；固定案例／邊界／Python 比對；本機微基準 | 限定子集通過，571 案例；不是交易執行驗收 |
| 2a | Aster／Hyperliquid 公共深度快照 | 只讀 WS、HL fast、整體替換／時間與序號／健康狀態、錄製與重播 | 完成；120 合成訊息及 208 真實 WS 訊息比對通過 |
| 2b-1 | Aster／Hyperliquid、`QuoteStore`／`StoreFeed` 公共快取子集 | 同一原生商品共用連線、BBO／depth 一致性、連線世代隔離、本機 WS 故障注入、Python 行為回放 | 完成限定範圍；560 筆事件完全一致，5 個本機 WS 故障／生命週期測試通過 |
| 2b-2a | Aster／Hyperliquid 商品識別與精度 | 原生名稱／映射、靜態 tick／lot／限價與市價數量限制、共同數量步長、顯式單位換算；完整公開 metadata 與 Python 比對 | 完成限定範圍；1,018 合成及 3,767 完整快照案例通過；中文商品 WS 85 則訊息重播通過 |
| 2b-2b | 公共 metadata 載入／刷新與共用 HTTP 限流 | 固定公開端點、連線池、每所權重／冷卻、單次合併刷新、generation／過期、停止與取消、429／418／5xx 故障注入 | 完成限定範圍；新增 27 個測試、公開抓取及新舊 metadata 比對通過；未連接策略、掃描器或面板 |
| 2b-2c | 多商品公共 WS 與交接 | 每所同連線多商品、控制／心跳／重連配額、ACK、消費者交接、metadata 更新／失效、同名商品 socket 隔離 | 完成限定範圍；23 個新版 hub 測試、四市場共兩條連線實網通過；尚未接策略／下單 |
| 2b-2d | 其餘公共行情與長期驗收 | 其餘 venue、長時間錄製／斷線恢復、跨所來源時鐘一致性、負載與多程序配額協調 | 待開始；本批短測不代表全天或全市場驗收 |
| 3a | `order_stream.py` 的 Aster 正規化及累計快取子集 | typed 成交、ACK 前保存、累計去重、亂序／撤單競態、重連與 REST 查單世代、單一狀態擁有者及有界入口；純離線驗收 | 完成限定範圍；34 個新測試、901 筆 Python 比對通過，兩項刻意差異另驗證。未接真實私人連線、HTTP 或執行器 |
| 3b-1 | Aster 私人 WS transport 及 REST fallback | v3 唯讀認證、listenKey 建立／續期／重連、私人配額、獨立查單排程、顯式唯讀 probe；本機故障注入及 Python 認證比對 | 離線／loopback 驗收通過（workspace 163 測試）；見 VALIDATION.md。尚未使用真實帳戶驗收或接入交易執行 |
| 3b-2 | 其他私人事件及實網驗收 | Aster 真實帳戶短測；Hyperliquid 私人訂閱、逐筆成交與快照去重、獨立查單；唯讀 probe | 已實作；195 Rust 測試及 601 HL Python 比對通過。Aster 換線／續期驗證通過；HL 訂單 unknownOid 保持待核對。詳見 VALIDATION.md；尚未完成全天、完整帳戶恢復或真實 Aster 成交補漏 |
| 3c-1 | 帳戶／持倉與非普通委託事件 | Aster 持倉／wallet／掛單；HL 顯式 dex、spot／wallet／掛單；獨立預期帳本比對、兩次快照及 WS 版本隔離、特殊事件轉入帳戶對帳 | 已實作；兩所 90 秒唯讀短測。HL 1 筆特殊事件仍待獨立帳本／來源時間證明，未宣告全帳戶恢復；詳見 VALIDATION.md |
| 3c-2 | 其餘 venue／可靠交接 | MEXC protobuf、其他 venue、client ID／未確認送單關聯、持久化與重啟、去重保留期、完整 account-wide 對帳與長時間驗收 | client ID／策略對沖意圖子集見 4b-1，其餘待開始；與階段 4／6 配合 |
| 4a | 紙上單筆 Maker／Hedge 協調 | 兩腿私人累計快照、先預留對沖量、ACK／未知回覆不釋放、終態剩餘量重試、dust／重複／撤單後成交測試 | 已實作紙上子集；無真實 broker，Python/Rust 終態重試規則比對見 VALIDATION.md |
| 4b-1 | 持久化與未知送單查證子集 | 單一寫入者帳本、送單前同步磁碟、client ID 查證、重啟／中斷／錯身份／磁碟故障測試 | 已實作；單向永續、已知 Maker order ID 與對沖意圖。無新 Maker 送單持久化、完整現金帳或 broker；見 VALIDATION.md |
| 4b-2 | Aster／HL 單筆 LIMIT 交易 adapter 與測試入口 | 完整委託持久化、單次提交、帳戶鎖、主帳戶／agent 簽名、私人 WS、REST 查證、到期撤單與顯式恢復 | 已實作；離線／loopback 及公開 metadata 預覽，尚未執行實際委託驗收；見 VALIDATION.md |
| 4b | `executor.py`、`hedge_safety.py`、`maker_price_guard.py` 及各交易 adapter 的其餘部分 | 真實 Maker/Taker、Taker/Taker、spot/perp 分單、改價／撤單競態、未知送單查證、reduce-only、費用／精度、持久化；每個 venue 獨立驗收 | 單筆測試子集見 4b-2；兩腿整合及實網下單驗收待進行 |
| 5 | `spread_strategy/engine.py`、`broker.py`、`dispatcher.py`、完整 `core.py`、估值／鎖／歷史／telemetry | 策略設定、進出場／持倉帳本、多配對調度、紙上交易；既有 `replay_spread_execution.py` 全情境逐一對照 | 待開始 |
| 6 | `risk_management/` 與 position registry | 倉位恢復、對帳、保證金、資金費率風控、平倉／補保證金、原子寫入、鎖與停機；故障注入和重啟恢復 | 待開始 |
| 7 | `funding_engine/`、`spread_monitor/`、funding adapters、backfill、history、alerts、Telegram、Variational bridge | 歷史與缺值語意、依天數正收益占比、required venue、Maker/Taker 價差過濾、通知／去重、429退避；通知測試使用替身 | 待開始 |
| 8 | `web/app.py`、`web/monitor.py`、`monitor_cache.py`、所有 CLI／設定檔 | Rust HTTP/WS 後端；保持繁中面板及 API 契約、歷史快取與共用連線；原生瀏覽器 JS/HTML/CSS 保留為資產，不為「全 Rust」強行破壞擴充功能 | 待開始 |
| 9 | 部署、效能及切換 | 同機同時段的 shadow run；同一錄製行情／私人事件驅動兩版；確認未重複下單後才分 venue 小範圍切換；保留可停止及回退方式 | 待開始 |

逐檔清單為 `migration-inventory.json`；`partial_verified` 僅代表列出的限定子集通過，不代表整個來源檔完成。新增或修改 Python 功能後先執行 `inventory.py --check`，檢視差異才更新清單。`tests/` 是逐批移植規格，不要求把 Python 測試語法翻成 Rust 才算功能完成；每個案例須對應 Rust 測試或記錄不適用原因。

交易所矩陣需分「funding」、「公共行情」、「Maker」、「Taker」、「私人 WS」、「查單／撤單」能力，不能用一個 supported 布林值涵蓋。現有模組涉及 Aster、Hyperliquid、Entropy／HIP-3、Lighter、MEXC 永續／現貨、Variational、Arcus、Ondo、Trade.xyz；每項以實際 adapter 與入口為準。

特別注意 Variational：既有使用者偏好、spot/perp 入口及 spread strategy 的能力限制並不相同。目前 `spread_strategy/core.py` 明確排除可撤 post-only Maker，瀏覽器交易路徑另有自己的限制。後續必須逐入口寫能力測試，不能把一個全域 Maker-only 設定硬套到所有流程。

## 不能回歸的執行行為

1. 新增風險前檢查新鮮行情／深度／不利價差；Maker 已成交的已知曝險不能因報價過期或超過 0.2% 而停止對沖。價差保護作用在仍可撤回的 Maker。
2. Maker 的同一累計成交只處理新增量；每筆委託分開累計，不將舊訂單的累計值套到新單。
3. 訂單送出後等待確認仍要記錄 Maker 增量、處理危險掛單撤回；不能讓慢 REST 或對沖 ACK 阻塞事件接收。
4. ACK 不等於成交。未知／逾時先查單與對帳，不能把未知當拒單而重送，也不能直接反向平倉造成雙重交易。
5. 撤單回覆後仍需處理撤單競態中的成交；未確認安全撤單前不能放大 Maker 風險。
6. 正確保留 hedge in-flight 數量與方向、部分成交、拒單、剩餘量及 dust。無法消除的真實曝險必須記錄／通知，不能假裝歸零。
7. 不以 stale depth 加新 BBO 製造虛假深度。HL fast 只有五檔；不足則縮量或停止新增風險。
8. 金融決策不使用 binary float。十進位精度、價格 tick、數量 lot、最低名義值、費用／成本及門檻附近需嚴格比對；本批除法顯示值的容差不授權放寬門檻。
9. 程式停止／重啟須保存與恢復未確認委託；只取消可安全取消的查詢任務，不能以取消本機 future 假定交易所未收單。

## 延遲與回放量測

後續使用本機 monotonic clock（Rust `Instant`）分開記錄：socket 收包、解析完成、事件排隊、策略開始、簽名完成、對沖提交、ACK、私人 WS 成交確認。交易所來源時間只用於行情年齡／來源延遲，須評估時鐘偏移；不得混用它和 monotonic clock 相減。

每批比較相同資料與同等工作量，報告事件數、丟失／重連、p50／p95／p99、本機 CPU 段、I/O 段及精度差異。先穩定 warm-up，再跑多輪；release 與 Python 都應在同一機器與相同電源／負載環境測量。離線計算速度提升不等於網路或撮合提升。

既有 `docs/benchmarks/spread-execution-results.md` 及 00 至 06 JSON 是合成事件回放基準；既有最後一批的慢對沖、慢撤單與未知回覆情境必須保留。收益驗證另需逐筆市場資料、費用、滑點、排隊與樣本外時段，不能拿分鐘 K 線或本批計算測試宣稱可搶過其他機器人。

正式切換前須能停止新單、對帳未確認委託並交接狀態；禁止 Python 與 Rust 同時管理同一個真實策略倉位。Rust 能減少部分本機開銷，但不保證兩所原子成交或低於 100 ms。
