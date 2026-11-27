# Rust Lighter 接入

支援 `place_order.py --engine rust` 的 Lighter ↔ Aster、Lighter ↔ Hyperliquid，以及 Variational Maker → Lighter IOC。原生配對可互換 Maker／Taker，延用 spread 與優先級選擇、部分成交對沖、撤單終態查證、逐單 WAL、未知送單不重送及相同 run 恢復。未新增 Lighter 現貨或 `run_spot_perp_arbitrage.py` 的 Lighter 路由。

原生 Maker 使用最新 bid／ask 的 post-only，Lighter Taker 使用 IOC 限價，按買 ask／賣 bid 加減 `hedge_slippage_bps`（預設 50）並向外取整。入場價差限制與 IOC 可接受滑點是兩個不同設定。Variational 配對的 Lighter IOC 限價在 prepare 自動產生並固定保存；可用 `--rust-hedge-price` 覆寫。Variational 仍使用現有 extension 的 Mid 按鈕與原成交確認模式。

## 安裝與設定

目前送單簽章支援 Windows x64。使用官方 `lighter-sdk==1.1.2` 內的原生 signer DLL，SHA-256 固定為 `6e5ede71ecc3f74002752421f823d9493459ade7f6e01b37539b5be4c4299b66`；不接受同名但不同 ABI 的 DLL。官方套件來源：[PyPI](https://pypi.org/project/lighter-sdk/1.1.2/)、[官方 SDK](https://github.com/elliottech/lighter-python)。Go runtime 的 DLL 保留至程序結束，回傳記憶體使用其 `Free` 釋放。簽章及送單熱路徑不啟動 Python subprocess。

```powershell
python scripts/setup_lighter_signer.py
cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin paired-order --release --locked
```

沿用 `.env` 的 `LIGHTER_ACCOUNT_INDEX`、`LIGHTER_API_KEY_INDEX` 與 `LIGHTER_PRIVATE_KEY`；有 `API_KEY_PRIVATE_KEY` 時依 Python adapter 的既有規則優先使用它。`rust/artifacts/paired-settings.json` 的 `accounts.lighter` 是十進制帳戶 index 字串，例如 `"100"`，不是 0x 地址，必須與環境設定相同。私鑰不可放進計劃或此設定檔。本機帳戶 index 已補入設定檔，未修改金鑰。

```powershell
python scripts/place_order.py --engine rust --rust-action prepare --ticker ETH --short_venue lighter --long_venue aster --rust-quantity 0.02 --rust-clip-quantity 0.01 --rust-plan rust/artifacts/lighter-eth.json
```

這一步只有公開資料預覽，不送單。省略兩所名稱仍使用原 funding monitor 的配對結果。後續沿用 [PAIRED_ORDER.md](PAIRED_ORDER.md) 的 audit、獨立核對 baseline、submit 及 recover 流程。

## 行情、成交及恢復

- 公開 WS 維護完整快照與增量深度，使用 `begin_nonce` 接續前一個 `nonce`；不要求 offset 連號。漏訊息、來源時間倒退、錯市場或壞深度會清空行情，重新連線取得快照。同一 MarketHub 的相同商品共用訂閱；不同 Lighter 商品目前各自使用 WS，最多 32 個。
- 私人 `account_all_orders` 收集累計成交，`account_all` 使帳戶對帳證據失效；重連重新認證，REST 與私人 WS 並行核對。私人 WS 快速路徑須有真實 `order_index` 綁定，`sendTx` 的交易雜湊不當作訂單編號。
- 送單前保存 client order index 與完整 intent。HTTP 超時／回覆不明／NotFound 保留未知狀態，只按精確 client index 查證；不重送相同訂單。`accountOrders` 無資料時，另查活躍委託與最多 5 頁、每頁 100 筆的歷史委託，仍須精確身份匹配；超出範圍不當作零成交。撤單使用查得的真實 order index，撤單 ACK 後仍需核對最終成交。
- 價格與數量採十進制定點，依公開商品規格縮放到整數，不使用 float。現有資產、持倉及活躍委託使用兩次一致快照對帳；不完整或變動中的結果不冒充成功。
- REST 使用共用讀取限流與 keep-alive；新送單／撤單／槓桿操作序列化 nonce，另有保守限流。讀取最低間隔 1.05 秒，交易最低間隔 1.6 秒，均只涵蓋此 client／程序，不能代表帳戶或 IP 的全部可用額度。私人 WS 成交接收不受 REST 查單等待阻塞；多筆 Lighter 送單仍受限流影響。

## 驗收與限制

離線測試涵蓋簽章 ABI／整數縮放、真實訂單與 client index 區分、缺失成交欄位、帳戶／商品錯配、深度增量漏包、部分成交、IOC 殘餘、未知送單及重啟去重。Lighter 與 Aster／Hyperliquid 的雙向開平倉，以及 Variational 的 WS 快速對沖流程，使用模擬交易所驗收。

2026-11-26 真實帳戶唯讀驗收：私人 WS 連線、強制重連後再次對帳、資產／持倉／活躍委託對帳、10 筆 ETH 歷史訂單解碼及精確 client index 歷史查證通過。重連後觀察 40 秒，沒有因帳戶無交易訊息而錯誤斷線；私人串流使用 Ping／Pong 判定存活。原生公開 WS 連續取得 50 筆新鮮深度修訂，該次樣本最大來源行情 age 為 94 ms，這不是成交延遲。沒有真實送單、撤單或槓桿變更；實際成交延遲、滑點及成交 WS 樣本仍待小額實測。

可重跑唯讀探針（需先載入環境變數，不會自動讀 `.env`）：

```powershell
cargo run --manifest-path rust/Cargo.toml -p perp-arb-lighter-private --example read_only --locked
```

IOC、最低數量、交易所拒絕及斷線仍可能留下未對沖責任；本批不保證兩腿同時成交或固定價差，也沒有宣稱整個 Python 專案已完成移植。
