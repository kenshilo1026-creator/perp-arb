"""
TriArb 三所套利策略核心
========================
架構：
  - 三所：Variational（鏈上 RFQ）、Hyperliquid（鏈上訂單簿）、Lighter（鏈上訂單簿）
  - 每輪掃描所有配對（C(3,2)=3 種），選擇當前價差最大者執行
  - Variational 動態角色：報價最優時作為買腿或賣腿，否則讓 HL 與 Lighter 配對
  - 全程由 RiskManager 保護，觸發熔斷或停機時停止下單
  - 所有關鍵事件透過 TelegramNotifier 推送通知

下單策略：
  - Variational（RFQ）：永遠用市價單；但加入軟限價保護——若 RFQ 報價
    偏離訂單簿超過 rfq_slippage_bps，則放棄本次機會。
  - Hyperliquid / Lighter（訂單簿）：優先使用限價單（掛在最佳對手價），
    等待 fill_timeout 秒；超時則取消並以市價補單。

資費過濾器：
  - 每 funding_refresh_interval 秒從各所抓取永續合約資費率。
  - 若任一腿的 8h 資費率絕對值超過 max_funding_rate_bps，則拒絕本次機會。
  - 同時計算雙腿淨資費成本並從有效價差中扣除，防止「名義正價差、實際虧損」。

使用方式：
  python arbitrage.py --ticker BTC --size 0.002 --max-position 0.1 \
      --threshold 5 --mode rotate --dry-run
"""

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from exchanges.base import BaseExchange, OrderBook, OrderResult
from exchanges.variational_onchain import VariationalOnChainExchange
from exchanges.hyperliquid import HyperliquidExchange
from exchanges.lighter import LighterExchange
from notifications.telegram import TelegramNotifier
from risk.manager import RiskManager, RiskConfig, CircuitState


# ── 資費快取 ──────────────────────────────────────────────────────────────────

@dataclass
class FundingRateCache:
    """
    各所永續合約資費率的本地快取。
    ttl 秒內不重複請求，避免過度佔用 API quota。

    rates 格式：{ exchange_name: 8h_funding_rate_as_decimal }
    例如 0.0001 表示每 8 小時收取 0.01%（多頭付空頭）。
    """
    ttl: float = 60.0
    rates: dict = field(default_factory=dict)   # {name: Decimal}
    _last_updated: float = field(default=0.0, repr=False)

    def is_stale(self) -> bool:
        return time.time() - self._last_updated > self.ttl

    def update(self, rates: dict):
        self.rates = {k: Decimal(str(v)) for k, v in rates.items()}
        self._last_updated = time.time()

    def get(self, exchange_name: str) -> Optional[Decimal]:
        return self.rates.get(exchange_name)


# ── 套利機會資料結構 ──────────────────────────────────────────────────────────

@dataclass
class ArbOpportunity:
    """
    一次套利機會的完整描述。
    由 _find_best_opportunity() 建立，傳給 _execute() 執行。

    net_spread 是扣除預估資費成本後的有效價差，作為真正的盈利估算基礎。
    """
    buy_exchange: BaseExchange   # 要買入的交易所
    sell_exchange: BaseExchange  # 要賣出的交易所
    buy_price: Decimal           # 買入交易所的最佳 ask 價
    sell_price: Decimal          # 賣出交易所的最佳 bid 價
    spread: Decimal              # 毛價差 = sell_price - buy_price
    quantity: Decimal            # 本次下單數量
    funding_cost: Decimal = Decimal("0")   # 預估資費成本（USD，正數代表成本）
    timestamp: float = field(default_factory=time.time)

    @property
    def net_spread(self) -> Decimal:
        """扣除資費後的有效價差"""
        return self.spread - self.funding_cost

    @property
    def gross_pnl(self) -> Decimal:
        """預估毛利（未扣除 gas、滑點等費用，但已扣除資費）"""
        return self.net_spread * self.quantity

    def __str__(self):
        return (
            f"BUY {self.buy_exchange.name.upper()} @ ${self.buy_price:.2f} | "
            f"SELL {self.sell_exchange.name.upper()} @ ${self.sell_price:.2f} | "
            f"spread=${self.spread:.4f} funding_cost=${self.funding_cost:.4f} "
            f"net=${self.net_spread:.4f} | est_pnl=${self.gross_pnl:.4f}"
        )


# ── 主策略類 ──────────────────────────────────────────────────────────────────

class TriArb:
    """
    三所套利機器人主體。
    負責協調訂單簿監控、機會偵測、下單執行、風險控制、通知推送。
    """

    # Variational 是 RFQ，永遠用市價單
    _MARKET_ORDER_ONLY_EXCHANGES = {"variational"}

    # Hyperliquid 永遠用限價單，超時後取消並回傳失敗（不補市價單）
    _ALWAYS_LIMIT_ORDER_EXCHANGES = {"hyperliquid"}

    # Lighter：bid/ask 價差低於此閾值時改用市價單（流動性足夠，滑點可控）
    _LIGHTER_MARKET_ORDER_SPREAD_THRESHOLD = Decimal("0.0005")  # 0.05%

    def __init__(
        self,
        ticker: str,
        order_quantity: Decimal,
        fill_timeout: int,
        max_position: Decimal,
        threshold: Decimal,
        mode: str = "rotate",
        variational_role: str = "dynamic",
        dry_run: bool = False,
        risk_config: Optional[RiskConfig] = None,
        # ── 限價單參數 ──────────────────────────────────────────────────
        rfq_slippage_bps: Decimal = Decimal("10"),
        # ── 資費過濾器參數 ──────────────────────────────────────────────
        funding_filter_enabled: bool = True,
        max_funding_rate_bps: Decimal = Decimal("10"),
        funding_refresh_interval: float = 60.0,
    ):
        """
        初始化機器人參數與所有子系統。

        Args:
            ticker:                  交易幣種，例如 "BTC"
            order_quantity:          每筆訂單數量（幣本位），例如 Decimal("0.002")
            fill_timeout:            限價單等待成交的秒數（超時取消並補市價單）
            max_position:            各所最大允許淨倉（幣本位），0 = 不限
            threshold:               觸發套利的最小淨價差（USD，已扣除資費）
            mode:                    策略模式（rotate / best_pair / fixed）
            variational_role:        Variational 角色（dynamic / taker / maker）
            dry_run:                 模擬模式，不實際下單
            risk_config:             自訂風險參數，不傳則使用預設值
            rfq_slippage_bps:        Variational 軟限價保護——若 RFQ 報價偏離
                                     訂單簿超過此 bps 值，放棄本次機會（預設 10bps）
            funding_filter_enabled:  是否啟用資費過濾器（預設 True）
            max_funding_rate_bps:    允許的最大 8h 資費率（bps），超過則拒絕開倉
                                     （預設 10bps = 0.10% per 8h）
            funding_refresh_interval:資費率快取刷新間隔（秒，預設 60）
        """
        self.ticker = ticker
        self.qty = order_quantity
        self.fill_timeout = fill_timeout
        self.max_position = max_position
        self.threshold = threshold
        self.mode = mode
        self.variational_role = variational_role
        self.dry_run = dry_run

        # ── 限價單參數 ────────────────────────────────────────────────
        self.rfq_slippage_bps = rfq_slippage_bps

        # ── 資費過濾器參數 ────────────────────────────────────────────
        self.funding_filter_enabled = funding_filter_enabled
        self.max_funding_rate_bps = max_funding_rate_bps / Decimal("10000")   # 轉換為小數
        self.funding_refresh_interval = funding_refresh_interval
        self._fr_cache = FundingRateCache(ttl=funding_refresh_interval)

        # ── 初始化三所交易所連接器 ────────────────────────────────────
        self.variational = VariationalOnChainExchange()
        self.hyperliquid = HyperliquidExchange()
        self.lighter = LighterExchange()
        self.exchanges: list[BaseExchange] = [
            self.variational, self.hyperliquid, self.lighter,
        ]

        # ── 各所倉位追蹤（正數=多頭，負數=空頭）─────────────────────
        self._positions: dict[str, Decimal] = {
            ex.name: Decimal("0") for ex in self.exchanges
        }

        # ── 統計資料 ──────────────────────────────────────────────────
        self._trade_count = 0        # 已執行交易總數
        self._pnl = Decimal("0")     # 累計毛利（USD）

        # ── 子系統 ────────────────────────────────────────────────────
        self.tg = TelegramNotifier()                              # Telegram 通知
        self.risk = RiskManager(config=risk_config, tg=self.tg)  # 風險管理

    # ── 啟動與生命週期 ────────────────────────────────────────────────────────

    async def run(self):
        """
        機器人主入口。
        依序完成：啟動通知、連接交易所、訂閱訂單簿、同步倉位、進入主迴圈。
        結束時（無論正常或異常）自動斷線並發送最終摘要。
        """
        print(
            f"[triArb] 啟動 | ticker={self.ticker} qty={self.qty} "
            f"threshold={self.threshold} dry_run={self.dry_run} "
            f"funding_filter={self.funding_filter_enabled} "
            f"max_fr={self.max_funding_rate_bps*10000:.1f}bps"
        )

        # 啟動 Telegram（失敗不阻塞主流程）
        await self.tg.start()

        # 逐一連接交易所，失敗的交易所會被風險管理器標記為不健康
        for ex in self.exchanges:
            try:
                await ex.connect()
            except Exception as e:
                print(f"[{ex.name}] 連線失敗: {e}")
                self.risk.mark_exchange_unhealthy(ex.name, f"連線失敗: {e}")
                await self.tg.notify_error(f"{ex.name}.connect", str(e))

        # 確認健康交易所數量是否足以執行套利
        if self.risk.healthy_exchange_count() < self.risk.cfg.min_healthy_exchanges:
            msg = "健康交易所不足，無法啟動套利"
            print(f"[triArb] ❌ {msg}")
            await self.tg.notify_error("startup", msg)
            return

        # 並行訂閱所有交易所的訂單簿 WebSocket
        await asyncio.gather(
            *[ex.subscribe_orderbook(self.ticker) for ex in self.exchanges],
            return_exceptions=True,
        )

        # 從各交易所查詢當前倉位（啟動時同步一次）
        await self._sync_positions()

        # 預先抓取一次資費率，確保第一輪就有資料
        if self.funding_filter_enabled:
            await self._refresh_funding_rates()

        try:
            await self._main_loop()
        except Exception as e:
            await self.tg.notify_error("main_loop", str(e))
            raise
        finally:
            # 無論如何都要清理
            for ex in self.exchanges:
                try:
                    await ex.disconnect()
                except Exception:
                    pass
            await self.tg.stop()
            print(f"[triArb] 停止 | 總交易={self._trade_count} | 累計PnL=${self._pnl:.4f}")

    # ── 主交易迴圈 ────────────────────────────────────────────────────────────

    async def _main_loop(self, poll_interval: float = 0.1):
        """
        核心輪詢迴圈。每 poll_interval 秒執行一次：
          1. 檢查訂單簿新鮮度
          2. 刷新資費率（若快取已過期）
          3. 尋找最佳套利機會
          4. 執行風險前置檢查
          5. 下單
        """
        while True:
            try:
                # 步驟 1：檢查各所訂單簿是否還新鮮
                self._check_all_orderbook_freshness()

                # 步驟 2：確認健康交易所數量
                if self.risk.healthy_exchange_count() < self.risk.cfg.min_healthy_exchanges:
                    await asyncio.sleep(1)
                    continue

                # 步驟 3：刷新資費率（非阻塞，快取未過期時立刻返回）
                if self.funding_filter_enabled and self._fr_cache.is_stale():
                    await self._refresh_funding_rates()

                # 步驟 4：掃描最佳套利機會
                opp = self._find_best_opportunity()
                if opp:
                    # 記錄價差歷史（供異常偵測使用）
                    self.risk.record_spread(opp.spread)

                    # 步驟 5：風險前置檢查
                    allowed, reason = self.risk.check_pre_trade(
                        buy_exchange=opp.buy_exchange.name,
                        sell_exchange=opp.sell_exchange.name,
                        spread=opp.spread,
                        positions=self._positions,
                    )
                    if not allowed:
                        print(f"[triArb] 風險管控阻止下單: {reason}")
                        await asyncio.sleep(1)
                        continue

                    # 步驟 6：執行套利
                    print(f"[triArb] 機會: {opp}")
                    await self.tg.notify_opportunity(
                        buy_exchange=opp.buy_exchange.name,
                        sell_exchange=opp.sell_exchange.name,
                        buy_price=opp.buy_price,
                        sell_price=opp.sell_price,
                        spread=opp.spread,
                        quantity=opp.quantity,
                        threshold=self.threshold,
                        gross_pnl=opp.gross_pnl,
                    )
                    await self._execute(opp)
                    self._trade_count += 1

            except asyncio.CancelledError:
                # 收到 Ctrl+C 或外部取消訊號，正常退出
                break
            except Exception as e:
                print(f"[triArb] 迴圈例外: {e}")
                await self.tg.notify_error("trading_loop", str(e))

            await asyncio.sleep(poll_interval)

    # ── 資費率刷新 ────────────────────────────────────────────────────────────

    async def _refresh_funding_rates(self):
        """
        從各所抓取最新資費率並更新本地快取。
        Variational 是 RFQ 現貨/現貨，沒有永續資費，跳過。
        抓取失敗時保留舊值繼續運行（不中斷套利），但打印警告。
        """
        new_rates: dict[str, Decimal] = {}
        for ex in self.exchanges:
            if ex.name in self._MARKET_ORDER_ONLY_EXCHANGES:
                # Variational 是 RFQ 現貨，無資費率
                new_rates[ex.name] = Decimal("0")
                continue
            if ex.name in self.risk._unhealthy_exchanges:
                continue
            try:
                rate = await ex.get_funding_rate(self.ticker)
                new_rates[ex.name] = Decimal(str(rate))
            except Exception as e:
                print(f"[triArb] 資費率抓取失敗 ({ex.name}): {e}，沿用舊值")
                if ex.name in self._fr_cache.rates:
                    new_rates[ex.name] = self._fr_cache.rates[ex.name]

        if new_rates:
            self._fr_cache.update(new_rates)
            rate_str = " | ".join(
                f"{k}={v*10000:.2f}bps" for k, v in new_rates.items()
            )
            print(f"[triArb] 資費率更新: {rate_str}")

    # ── 資費成本估算 ──────────────────────────────────────────────────────────

    def _estimate_funding_cost(
        self, buy_name: str, sell_name: str, mid_price: Decimal
    ) -> tuple[Decimal, bool]:
        """
        估算本次套利的預期資費成本，並判斷是否超過極端值閾值。

        套利結構：
          - 買入腿（多頭）：若 funding_rate > 0，多頭付費；若 < 0，多頭收費
          - 賣出腿（空頭）：若 funding_rate > 0，空頭收費；若 < 0，空頭付費
          - 淨資費成本 = fr_buy（多頭付出）- fr_sell（空頭收入）
            = fr_buy - (-fr_sell) 的鏡像結構
          - 正數代表持倉有淨成本，負數代表持倉有淨收益

        極端值判斷：
          任一腿的資費率絕對值超過 max_funding_rate_bps，視為極端資費事件，
          阻止開倉（即使方向有利，因為市場可能有異常風險）。

        Returns:
            (funding_cost_usd, is_extreme)
            funding_cost_usd : 以 USD 計算的預期資費成本（正=成本，負=收益）
            is_extreme       : 是否觸發極端資費過濾
        """
        fr_buy = self._fr_cache.get(buy_name) or Decimal("0")
        fr_sell = self._fr_cache.get(sell_name) or Decimal("0")

        # 極端資費檢查：任一腿超過閾值就阻止
        if (abs(fr_buy) > self.max_funding_rate_bps or
                abs(fr_sell) > self.max_funding_rate_bps):
            return Decimal("0"), True

        # 淨資費成本（以 mid_price 換算為 USD）
        # 多頭付 fr_buy，空頭收 fr_sell（fr_sell>0 時空頭收錢）
        net_fr = fr_buy - fr_sell
        funding_cost_usd = net_fr * mid_price * self.qty

        return funding_cost_usd, False

    # ── 機會偵測 ──────────────────────────────────────────────────────────────

    def _find_best_opportunity(self) -> Optional[ArbOpportunity]:
        """
        掃描所有健康交易所的兩兩組合，找出當前淨價差最大且超過閾值的配對。

        計算邏輯：
          gross_spread = sell_exchange.best_bid - buy_exchange.best_ask
          net_spread   = gross_spread - funding_cost（已扣除資費）
          若 net_spread > threshold 且通過倉位、角色、資費過濾，則為有效機會。

        複雜度：O(n²)，n=3 所以固定 6 次比較，不構成效能瓶頸。
        """
        # 收集所有有效（非 None）的訂單簿快照
        obs: dict[str, OrderBook] = {}
        for ex in self.exchanges:
            if ex.name in self.risk._unhealthy_exchanges:
                continue  # 跳過不健康的交易所
            ob = ex.get_orderbook()
            if ob:
                obs[ex.name] = ob

        if len(obs) < 2:
            return None  # 少於 2 所有報價，無法配對

        best: Optional[ArbOpportunity] = None
        ex_list = list(obs.keys())

        for i in range(len(ex_list)):
            for j in range(len(ex_list)):
                if i == j:
                    continue

                buy_name = ex_list[i]   # 買入方
                sell_name = ex_list[j]  # 賣出方
                gross_spread = obs[sell_name].bid - obs[buy_name].ask

                # 毛價差未超過閾值，提前剪枝（資費只會讓有效價差更小）
                if gross_spread <= self.threshold:
                    continue

                # 倉位限制檢查：避免單所倉位過度集中
                if not self._position_allows(buy_name, sell_name):
                    continue

                # Variational 角色限制（若設定為 taker，它必須參與）
                if not self._role_allows(buy_name, sell_name):
                    continue

                # ── 資費過濾 ──────────────────────────────────────────
                funding_cost = Decimal("0")
                if self.funding_filter_enabled:
                    mid_price = (obs[buy_name].ask + obs[sell_name].bid) / 2
                    funding_cost, is_extreme = self._estimate_funding_cost(
                        buy_name, sell_name, mid_price
                    )
                    if is_extreme:
                        print(
                            f"[triArb] 極端資費過濾: {buy_name}↔{sell_name} "
                            f"buy_fr={self._fr_cache.get(buy_name)*10000:.2f}bps "
                            f"sell_fr={self._fr_cache.get(sell_name)*10000:.2f}bps"
                        )
                        continue

                    net_spread = gross_spread - funding_cost
                    if net_spread <= self.threshold:
                        print(
                            f"[triArb] 資費吃掉價差: {buy_name}↔{sell_name} "
                            f"gross=${gross_spread:.4f} funding_cost=${funding_cost:.4f} "
                            f"net=${net_spread:.4f} < threshold=${self.threshold}"
                        )
                        continue

                # 選出當前淨價差最大的配對
                effective_spread = gross_spread - funding_cost
                if best is None or effective_spread > best.net_spread:
                    buy_ex = next(e for e in self.exchanges if e.name == buy_name)
                    sell_ex = next(e for e in self.exchanges if e.name == sell_name)
                    best = ArbOpportunity(
                        buy_exchange=buy_ex,
                        sell_exchange=sell_ex,
                        buy_price=obs[buy_name].ask,
                        sell_price=obs[sell_name].bid,
                        spread=gross_spread,
                        quantity=self.qty,
                        funding_cost=funding_cost,
                    )

        return best

    def _position_allows(self, buy_name: str, sell_name: str) -> bool:
        """
        檢查此次交易後，兩所的倉位是否仍在允許範圍內。
        max_position=0 表示不限制。
        防止單所累計過大的方向性暴露。
        """
        if self.max_position <= 0:
            return True
        after_buy = abs(self._positions[buy_name] + self.qty)
        after_sell = abs(self._positions[sell_name] - self.qty)
        return after_buy <= self.max_position and after_sell <= self.max_position

    def _role_allows(self, buy_name: str, sell_name: str) -> bool:
        """
        根據 variational_role 設定，決定 Variational 是否必須參與本次配對。
        - dynamic：不限制，任何兩所組合都可以
        - taker：Variational 必須是買腿或賣腿之一
        - maker：（Variational 原生不支援 maker，此設定等同 dynamic）
        """
        if self.variational_role == "taker":
            return "variational" in (buy_name, sell_name)
        return True  # dynamic / maker 不限制

    # ── 訂單執行 ──────────────────────────────────────────────────────────────

    async def _place_order_smart(
        self,
        exchange: BaseExchange,
        side: str,
        price: Decimal,
    ) -> OrderResult:
        """
        智能下單：根據交易所規則選擇訂單類型。

        Variational (RFQ)：
          永遠用市價單（RFQ 本質，無法掛限價）。
          加入軟限價保護：成交價偏離超過 rfq_slippage_bps 則標記 slippage_exceeded。

        Lighter（訂單簿）：
          動態判斷：取當前訂單簿 bid/ask 價差。
          - 價差 < 0.05%（_LIGHTER_MARKET_ORDER_SPREAD_THRESHOLD）→ 市價單
            （流動性充足，滑點可控，換取速度優勢）
          - 價差 >= 0.05% → 限價單流程（同下）

        Hyperliquid（訂單簿）：
          永遠用限價單。
          超時後取消限價單並回傳失敗——不補市價單，讓上層的緊急對沖處理。
          （HL 的市場深度通常足夠，若限價單長時間未成交代表市場已大幅移動，
          此時補市價單風險更高，寧可放棄本次機會。）

        Args:
            exchange : 目標交易所
            side     : "buy" 或 "sell"
            price    : 參考訂單簿價（buy=ask, sell=bid），用於軟限價保護和掛單
        """
        # ── Variational：RFQ 市價單 + 軟限價保護 ─────────────────────
        if exchange.name in self._MARKET_ORDER_ONLY_EXCHANGES:
            result = await exchange.place_market_order(self.ticker, side, self.qty)
            if isinstance(result, Exception) or not result.success:
                return result

            slippage_limit = self.rfq_slippage_bps / Decimal("10000")
            if side == "buy":
                max_acceptable = price * (1 + slippage_limit)
                if result.price > max_acceptable:
                    print(
                        f"[triArb] RFQ 軟限價觸發 ({exchange.name} buy): "
                        f"成交={result.price:.4f} 上限={max_acceptable:.4f}"
                    )
                    result.slippage_exceeded = True
            else:
                min_acceptable = price * (1 - slippage_limit)
                if result.price < min_acceptable:
                    print(
                        f"[triArb] RFQ 軟限價觸發 ({exchange.name} sell): "
                        f"成交={result.price:.4f} 下限={min_acceptable:.4f}"
                    )
                    result.slippage_exceeded = True
            return result

        # ── Lighter：根據即時價差動態選擇訂單類型 ─────────────────────
        if exchange.name == "lighter":
            ob = exchange.get_orderbook()
            if ob and ob.ask > 0:
                spread_pct = (ob.ask - ob.bid) / ob.ask
                if spread_pct < self._LIGHTER_MARKET_ORDER_SPREAD_THRESHOLD:
                    print(
                        f"[triArb] Lighter 價差={spread_pct*100:.4f}% < 0.05%，使用市價單"
                    )
                    return await exchange.place_market_order(self.ticker, side, self.qty)
                # 否則落入下方的限價單流程，並允許超時後補市價
            else:
                # 訂單簿資料不可用，保守起見使用限價單
                print("[triArb] Lighter 訂單簿不可用，使用限價單")

        # ── 限價單流程（Lighter 價差寬 / Hyperliquid 全程）────────────
        allow_market_fallback = exchange.name not in self._ALWAYS_LIMIT_ORDER_EXCHANGES

        try:
            limit_result = await exchange.place_limit_order(
                self.ticker, side, self.qty, price
            )
        except Exception as e:
            if allow_market_fallback:
                print(f"[triArb] 限價單下單失敗 ({exchange.name})，改用市價: {e}")
                return await exchange.place_market_order(self.ticker, side, self.qty)
            return OrderResult(success=False, error_message=f"限價單下單失敗: {e}")

        if not limit_result.success:
            if allow_market_fallback:
                print(f"[triArb] 限價單拒絕 ({exchange.name})，改用市價")
                return await exchange.place_market_order(self.ticker, side, self.qty)
            return OrderResult(success=False, error_message="限價單被拒絕")

        # 等待成交，最多 fill_timeout 秒
        deadline = time.time() + self.fill_timeout
        while time.time() < deadline:
            await asyncio.sleep(0.2)
            try:
                info = await exchange.get_order_status(limit_result.order_id)
            except Exception:
                break  # 查詢失敗，視為未知狀態，繼續等

            if info and info.status == "FILLED":
                return OrderResult(
                    success=True,
                    order_id=limit_result.order_id,
                    price=info.price,
                    filled=info.filled_size,
                )
            if info and info.status == "CANCELED":
                print(f"[triArb] 限價單被取消 ({exchange.name})")
                if allow_market_fallback:
                    return await exchange.place_market_order(self.ticker, side, self.qty)
                return OrderResult(success=False, error_message="限價單被取消")
            # PARTIALLY_FILLED：繼續等

        # ── 超時處理 ──────────────────────────────────────────────────
        print(
            f"[triArb] 限價單 {limit_result.order_id} 超時 ({self.fill_timeout}s) "
            f"({exchange.name})"
        )
        try:
            await exchange.cancel_order(limit_result.order_id)
        except Exception as e:
            print(f"[triArb] 取消限價單失敗 ({exchange.name}): {e}")

        if allow_market_fallback:
            print(f"[triArb] ({exchange.name}) 超時後改用市價補單")
            return await exchange.place_market_order(self.ticker, side, self.qty)

        # Hyperliquid：超時不補市價，回傳失敗讓上層緊急對沖
        print(f"[triArb] ({exchange.name}) 永遠限價，超時後放棄本次機會")
        return OrderResult(success=False, error_message=f"限價單超時 ({self.fill_timeout}s)")

    async def _execute(self, opp: ArbOpportunity):
        """
        執行套利交易。
        根據交易所類型智能選擇市價單或限價單（見 _place_order_smart）。
        成功：更新倉位與 PnL，發送成功通知。
        失敗：觸發緊急對沖，發送失敗通知，並告知風險管理器。
        dry_run 模式下不實際下單，僅模擬記錄。
        """
        # ── dry_run 模擬模式 ──────────────────────────────────────────
        if self.dry_run:
            print(f"[triArb] DRY RUN — 模擬下單，跳過實際執行")
            self._pnl += opp.gross_pnl
            self.risk.record_trade_result(True, opp.gross_pnl, opp.buy_exchange.name, opp.sell_exchange.name)
            await self.tg.notify_execution_success(
                opp.buy_exchange.name, opp.sell_exchange.name,
                opp.buy_price, opp.sell_price,
                self.qty, opp.gross_pnl,
                "DRY_RUN", "DRY_RUN",
            )
            return

        # ── 同時向兩所下單（並行執行降低延遲）────────────────────────
        # buy_price = ask（對方賣給我的最低價），sell_price = bid（對方買的最高價）
        buy_task = asyncio.create_task(
            self._place_order_smart(opp.buy_exchange, "buy", opp.buy_price)
        )
        sell_task = asyncio.create_task(
            self._place_order_smart(opp.sell_exchange, "sell", opp.sell_price)
        )
        results = await asyncio.gather(buy_task, sell_task, return_exceptions=True)
        buy_result, sell_result = results

        buy_ok = not isinstance(buy_result, Exception) and buy_result.success
        sell_ok = not isinstance(sell_result, Exception) and sell_result.success

        # ── 兩腿都成功 ────────────────────────────────────────────────
        if buy_ok and sell_ok:
            filled = min(buy_result.filled, sell_result.filled)
            gross = opp.spread * filled
            self._pnl += gross

            # 更新倉位追蹤
            self._positions[opp.buy_exchange.name] += buy_result.filled
            self._positions[opp.sell_exchange.name] -= sell_result.filled

            # 通知風險管理器（更新連續失敗計數、虧損追蹤等）
            self.risk.record_trade_result(True, gross, opp.buy_exchange.name, opp.sell_exchange.name)

            # 推送成功通知
            await self.tg.notify_execution_success(
                opp.buy_exchange.name, opp.sell_exchange.name,
                buy_result.price, sell_result.price,
                filled, gross,
                buy_result.order_id, sell_result.order_id,
            )
            await self.tg.notify_position_update(self._positions)

        # ── 至少一腿失敗 ──────────────────────────────────────────────
        else:
            error_msg = str(buy_result if not buy_ok else sell_result)
            print(f"[triArb] 執行失敗: {error_msg}")

            # 嘗試緊急對沖（平掉成功的那一腿，避免裸倉）
            unwind_ok = await self._emergency_hedge(opp, buy_result, sell_result)

            # 告知風險管理器記錄失敗（可能觸發熔斷）
            self.risk.record_trade_result(False, Decimal("0"), opp.buy_exchange.name, opp.sell_exchange.name)

            # 推送失敗通知
            await self.tg.notify_execution_failed(
                opp.buy_exchange.name, opp.sell_exchange.name,
                error_msg, unwind_ok,
            )

    # ── 緊急對沖 ──────────────────────────────────────────────────────────────

    async def _emergency_hedge(
        self,
        opp: ArbOpportunity,
        buy_result,
        sell_result,
    ) -> bool:
        """
        黑天鵝第一道防線：緊急對沖。
        當兩腿中有一腿失敗時，立刻對已成交的那腿下反向市價單，
        將裸倉平掉，防止單方向暴露在市場波動中。
        緊急對沖永遠用市價單（速度優先，不等限價成交）。

        情境 A：買腿成功，賣腿失敗 → 在買入交易所賣出
        情境 B：買腿失敗，賣腿成功 → 在賣出交易所買入

        Returns:
            True 表示對沖成功，False 表示對沖也失敗（需人工處理）
        """
        buy_ok = not isinstance(buy_result, Exception) and buy_result.success
        sell_ok = not isinstance(sell_result, Exception) and sell_result.success

        if buy_ok and not sell_ok:
            # 買腿已成交，需在買入交易所反向平倉
            print(f"[triArb] ⚠️ 緊急對沖：在 {opp.buy_exchange.name} 賣出 {buy_result.filled}")
            try:
                await opp.buy_exchange.place_market_order(
                    self.ticker, "sell", buy_result.filled
                )
                print(f"[triArb] ✅ 緊急對沖成功")
                return True
            except Exception as e:
                # 對沖失敗是最糟糕的情況，立即觸發緊急停機並告警
                print(f"[triArb] 🚨 緊急對沖失敗: {e}，觸發緊急停機")
                self.risk._trigger_halt(f"緊急對沖失敗: {e}")
                await self.tg.notify_error("emergency_unwind", f"對沖失敗，需立即人工處理！{e}")
                return False

        elif not buy_ok and sell_ok:
            # 賣腿已成交，需在賣出交易所反向平倉
            print(f"[triArb] ⚠️ 緊急對沖：在 {opp.sell_exchange.name} 買入 {sell_result.filled}")
            try:
                await opp.sell_exchange.place_market_order(
                    self.ticker, "buy", sell_result.filled
                )
                print(f"[triArb] ✅ 緊急對沖成功")
                return True
            except Exception as e:
                print(f"[triArb] 🚨 緊急對沖失敗: {e}，觸發緊急停機")
                self.risk._trigger_halt(f"緊急對沖失敗: {e}")
                await self.tg.notify_error("emergency_unwind", f"對沖失敗，需立即人工處理！{e}")
                return False

        # 兩腿都失敗，沒有倉位需要對沖
        return True

    # ── 訂單簿新鮮度監控 ─────────────────────────────────────────────────────

    def _check_all_orderbook_freshness(self):
        """
        每輪主迴圈都執行此檢查。
        若某所訂單簿超過 ob_stale_seconds 未更新（可能 WebSocket 斷線），
        風險管理器會將其標記為不健康，後續下單時自動排除該交易所。
        """
        for ex in self.exchanges:
            ob = ex.get_orderbook()
            if ob:
                self.risk.check_orderbook_freshness(ex.name, ob.timestamp)
            else:
                # 訂單簿從未收到，直接標記不健康
                self.risk.mark_exchange_unhealthy(ex.name, "訂單簿未收到任何資料")

    # ── 倉位同步 ──────────────────────────────────────────────────────────────

    async def _sync_positions(self):
        """
        啟動時從各交易所 API 查詢真實倉位並同步到本地追蹤。
        防止程式重啟後用錯誤的初始倉位進行風控判斷。
        """
        for ex in self.exchanges:
            try:
                pos = await ex.get_position(self.ticker)
                self._positions[ex.name] = pos
                print(f"[{ex.name}] 初始倉位 = {pos}")
            except Exception as e:
                # 查詢失敗不阻塞啟動，保持 0 作為預設值
                print(f"[{ex.name}] 倉位查詢失敗（使用預設值 0）: {e}")
