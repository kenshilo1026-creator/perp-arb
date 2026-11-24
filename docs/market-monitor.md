# 跨所套利監控台

監控頁沿用專案的 FastAPI 與 uPlot，提供繁體中文價差圖、資金費率、雙交易所報價及本地策略狀態。頁面只查閱資料，不啟動策略、不送單，也不發送 Telegram。

## 啟動

在專案根目錄執行：

```powershell
python scripts/run_order_ui.py
```

開啟 <http://127.0.0.1:8765/monitor.html>。原有下單頁保留在 `/`，並新增監控頁連結。若環境尚未安裝網頁依賴，先執行：

```powershell
python -m pip install -r requirements-web.txt
```

服務只綁定 localhost。監控 WebSocket 亦檢查 Host 與 Origin。

## 操作

- 預設 ETH，多方 Aster、空方 Hyperliquid。交易所選項包括 Aster、Hyperliquid、Lighter、Variational、Entropy、Arcus、Ondo，以及 Binance 現貨；以當次公開市場目錄確認兩所共同支援的標的，排除可辨識的數量倍率別名。目錄讀取失敗的交易所暫時停用。
- Binance 現貨採 USDT 交易對，只可選為多方，搭配永續空方；此時停用交換方向，仍可切換哪一方掛單。即時報價使用公開 WebSocket，無須 API 金鑰。歷史參考自動補抓最多 24 小時的已收盤分鐘 K 線，僅比較兩方都有資料的分鐘，不補零或延用前值；實際開／平倉報價差則由監控開始後累積。與 USDC 計價永續比較時，圖表沒有另外校正 USDT／USDC 匯率。
- `⇄` 交換方向；「圖表設定」切換多方掛單、空方掛單或雙方吃單，並設定觀察數量。數量單位是標的幣，不是美元。
- 「疊加兩所價格」顯示各所中間價，使用獨立價格軸。拖曳圖表放大；「即時追蹤」恢復所選時間範圍。
- 進入頁面預設自動載入「歷史參考」，顯示最近最多 24 小時的分鐘收盤／中間價差；切換配對或時間範圍會重新抓取。按「歷史參考」可切回已錄製的開／平倉報價差。參考資料與本地錄製資料獨立載入，沒有歷史來源的配對保持空白。
- 「示範預覽」在瀏覽器產生明確標示的模擬圖表，不寫入行情資料庫；「返回即時行情」恢復公開行情連線。
- 「策略狀態」查看目前方向配對的兩腿數量、均價、已實現價差損益、估算費用，以及最近委託／成交。這些是策略帳本，不代表交易所帳戶的全部部位。

## 計算方式

### 全市場歷史價差振幅掃描

在專案根目錄執行 `python scripts/run_basis_scan.py 1`，掃描最近 1 天、目前仍上市且與 Binance USDT 現貨共用的幣種，按「最大價差減最小價差」排列前十組「幣種＋永續交易所」。同一幣種在不同交易所可分別入榜。這是命令列工具，監控頁不會自動啟動全市場補抓。

```powershell
python scripts/run_basis_scan.py 7
python scripts/run_basis_scan.py 1 --venues hyperliquid aster --top 10
python scripts/run_basis_scan.py 1 --venues hyperliquid --symbols SAGA
```

天數支援 1–30 天；預設沿用 `hydra_basis/config.py` 的 `VENUE_CONFIG.enabled`，`--venues` 可指定本次掃描交易所。現貨固定為 Binance；不使用帳戶金鑰、不送單、不發 Telegram。支援 Aster、Hyperliquid、Entropy、Trade.xyz 的公開 K 線；Arcus 使用既有分鐘 K 線介面。Lighter、Variational、Ondo 使用 `data/spread_strategies/minute_mids.json.gz` 的既有本地中間價紀錄，沒有紀錄便列為 `no_recorded_history`，不宣稱公開補抓失敗。

預設名單直接取全部 `VENUE_CONFIG.enabled=true` 項目，不與另一份白名單取交集；啟動時會印出本次名單。MEXC 若重新啟用亦使用既有分鐘 K 線來源。日後加入尚未接入歷史介面的交易所，會明確列出 `history_adapter_not_supported`，不會悄悄忽略或阻止其他交易所完成。終端會分交易所列出跳過原因，詳細配對保存在 JSON。

所有市場以同一個已收盤 UTC 時間窗比較。1–3 天使用 1 分鐘、4–15 天使用 5 分鐘、16–30 天使用 15 分鐘；較粗採樣會漏掉期間內短暫極值。[Hyperliquid 歷史 K 線只提供最近 5,000 根](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#candle-snapshot)，因此長區間不能當作逐分鐘完整搜尋。只對齊兩所都有有效價格的時間點，不向前補值；預設共同資料覆蓋率至少 70% 才排名，可用 `--min-coverage` 調整。覆蓋率的分母仍為整個搜尋時間窗。最高／最低時間是 K 線開啟時間，數值採該根收盤價。

掃描價差為 `(永續價格 − 現貨價格) / 現貨價格 × 100%`，永續溢價顯示正值，與 SELL Maker 日誌的「不利價差」符號及分母不同。最大 1%、最小 −0.5% 的振幅為 **1.5 個百分點**；這不是保證利潤，也不是扣費回測。另列「先高後低」最大收斂幅度；若最低點在最高點之前，這個數字可能小於振幅。JSON 另附該收斂區間以固定幣數、開倉現貨名義本金計算的毛回報，價格本金變動時未必等於兩個百分比之差，亦不代表最大可實現損益。

結果未校正穩定幣匯率，未計開平倉手續費、資金費率、滑點、深度和延遲。K 線異常尖峰、缺值及當前上市篩選都會影響排名。單所失敗不阻止其他配對完成；不足十組時只列有效組數。完整結果及跳過原因存於 `data/market_monitor/basis_scan_latest.json`，可用 `--output` 指定另一檔案。

掃描使用分頁、同主機共用節流與 429 退避控制；成功頁面存於 `basis_scan_cache.sqlite3`，再次執行可重用。含缺值或空頁的快取 5 分鐘後重查；網路失敗不寫入成功快取，舊資料保留約 31 天。全市場初次下載可能需要數分鐘以上；限制只適用此程序，不與另一個監控或交易程序共用 IP 額度。

開倉價差為 `(空方賣出價 − 多方買入價) / 多方買入價 × 100%`；平倉價差為 `(空方買回價 − 多方賣出價) / 多方賣出價 × 100%`。兩者未扣手續費、滑點及資金費率，平倉價差不等於實際損益。

雙 Taker 模式按觀察數量計算可見深度的加權均價；深度不足時該方向不顯示數值。Maker 曲線採最佳限價的估算，不能當成已成交價或策略已接受的掛單價。有有效心跳、實盤狀態及對應開倉掛單時，摘要另外列出該筆剩餘委託量的實際掛單價差。它不取代圖表的估算曲線。

公開報價沿用既有行情品質檢查：來源年齡上限 1 秒、來源至接收上限 0.5 秒、未來時間容忍 0.25 秒。過期報價仍可在右側查看並標示「已過期」，不再產生有效價差點。右側毫秒數是報價年齡，不是下單或成交延遲。

Hyperliquid 的正式 `l2Book` WebSocket 訂閱採 `fast: true`，涵蓋監控頁／單策略、dispatcher 與價差監控程式，共用路徑的 HIP-3 市場亦適用。快速快照每邊只有 5 檔；每次完整替換舊深度，不拼接舊的第 6–20 檔。超出可見深度的觀察數量仍無法估算整筆成交價，新鮮度門檻維持不變。已運行的程序需正常重啟才會使用新訂閱。

## 資金費率與 Variational

兩所費率統一換算成 `% / 小時`，摘要差值為空方費率減去多方費率。原始每期費率與結算週期仍顯示在副圖上方；缺資料不補零。即時費率每 30 秒更新，失敗時移除該次結果；歷史曲線來自既有本地費率紀錄。

Binance 現貨顯示「不適用（現貨）」，不繪製虛構的零費率曲線；現貨／永續配對的費率摘要採永續空方費率。

Aster 即時費率匹配買賣盤實際使用的合約，避免 USDT、USDC 與逆向 USD 合約的週期混用。

Variational 監控使用 `/metadata/v2/stats`，只納入 `perpetual` 市場；取 `funding.last.interval_rate` 與 `interval_seconds` 計算每小時費率。舊介面的 `funding_rate` 是年化值，不能直接除以結算小時數。[官方 API 文件](https://docs.variational.io/technical-documentation/api.md)

Variational 顯示參考報價卡，不偽造完整深度。公開報價可能有較長快取，因此常會標示過期；不將剛收到 HTTP 回覆當成報價剛更新。所有監控配對共用 5 秒的 v2 統計快取，避免各頁面重複輪詢。這次 v2 修正只適用新監控頁，其他原有程式的資料管道未改寫。

Variational 請求失敗時共用 30 秒起、最高 300 秒的指數退避；HTTP 429 會同時讀取 `Retry-After`（秒數或日期）與 JSON 的 `retry_after_seconds`，採用較長等待時間。等待期間不將舊報價刷新為有效行情。

## 記錄與生命週期

同一幣種、方向及數量的瀏覽器共用一組觀察結果，最多同時 8 組。底層買賣盤依「交易所＋幣種」共用 WebSocket，即使不同方向、觀察數量或配對也不重複連線，各組仍按自己的數量計算深度。Variational 的報價分層與數量有關，因此保留各數量的報價選擇，但共用公開統計 HTTP 請求。資金費率與合約規格跟隨共用市場訂閱取得。

最後一個頁面離線後立即停止該組錄製；底層行情保留 5 秒再釋放，讓快速切換方向可沿用連線。服務關閉會清理全部訂閱。每個上游主機最多 8 條監控 WebSocket，新建連線至少間隔 3 秒；額滿時等待空位。

監控服務內的 REST 請求按上游主機共用佇列，至少間隔 1 秒。Hyperliquid 另按請求種類與 K 線筆數預留估算權重，以每秒 10 權重控制發送。HTTP 418、429 或 5xx 會暫停該主機後續請求並退避。這些控制只涵蓋同一監控服務程序；其他策略程序、其他服務或同 IP 的外部程式仍可能消耗交易所額度，不能保證永不遇到 429。

歷史參考的已收盤分鐘價格存於 `data/market_monitor/reference.sqlite3`，以交易所、幣種及分鐘為鍵，重啟服務仍能重用。相同市場的並行補抓會合併；只補缺少的連續分鐘區段，切換多空方向、數量或縮小範圍不重抓已有價格，調整圖表顯示間隔不重新請求歷史參考。快取保留 7 天、最多 150,000 筆，介面的歷史參考範圍仍為最多 24 小時。

空回覆不當成零價格：舊時段 5 分鐘後可再查，最近兩分鐘的缺值 1 分鐘後可再查。網路失敗時保留已快取價格並暫停該市場補抓 1 分鐘；每次查詢最多補 4 段，避免大量零碎缺口造成請求暴增。只有查閱歷史參考時才補抓，並不持續重抓完整 24 小時。

有訂閱時每 5 秒將觀察值寫入 `data/market_monitor/history.sqlite3`，保留最近 7 天，並每分鐘修剪至最多 150,000 筆。兩次修剪之間可能短暫超過筆數上限。關閉所有相關頁面後停止錄製，沒有補抓過去的買賣盤。資料庫已排除於 Git。

歷史查詢依顯示間隔取每桶最後一筆，最多約 6,000 桶；長範圍會提高顯示間隔。這不是每桶高低價或逐筆成交回放，可能略過桶內瞬間價差。未錄製時段不補值，分鐘 K 線也不冒充歷史可成交價差。

策略帳本從 `data/spread_strategies/*.json` 與 `groups/*.json` 讀取；自訂到其他路徑的帳本暫不列入。監控只傳回必要欄位，不輸出訂單原始回覆。策略 runner 與 dispatcher 新增獨立的 3 秒心跳，寫入背景執行緒；錯誤不阻止交易。心跳超過 12 秒、缺失或已退出，畫面顯示「上次保存狀態」，不宣稱策略正在運行。已經啟動的舊程序需要正常重啟才會有新心跳。

## 驗證

```powershell
python -m pytest tests/test_monitor_binance_spot.py tests/test_monitor_cache.py tests/test_market_monitor.py tests/test_order_web.py tests/test_spread_strategy.py tests/test_spread_dispatcher.py tests/test_spread_execution_events.py tests/test_spread_quote_freshness.py tests/test_spread_hedge_coordinator.py tests/test_spread_history.py -q
node --check web/static/monitor.js
```

監控測試覆蓋方向與深度計算、缺值／過期資料、歷史重開與修剪、共享訂閱回收、只讀 WebSocket、帳本欄位過濾、心跳停止、Aster 合約匹配，以及 Variational v2 的費率單位與永續市場分類。瀏覽器另檢查真實行情、示範模式、數量與方向切換、歷史參考、設定面板與窄螢幕寬度。
