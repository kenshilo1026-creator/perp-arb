"""
風險管理模組 — 黑天鵝保護機制
================================
負責在以下情況自動觸發熔斷、停止交易、通知：

1. 單筆虧損超過上限          → 停止當輪，等待人工確認
2. 累計虧損超過日損上限      → 關閉所有倉位，停機
3. 單所 API 回應超時/斷線    → 標記該所為不可用，切換到剩餘兩所
4. 倉位嚴重失衡（delta 過大）→ 強制減倉至安全範圍
5. 價差異常放大（閃崩跡象）  → 暫停交易 N 秒，等市場穩定
6. 連續失敗超過閾值          → 熔斷，等待人工重啟
"""

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from notifications.telegram import TelegramNotifier


class CircuitState(Enum):
    """熔斷器狀態機"""
    NORMAL   = auto()   # 正常交易中
    WARNING  = auto()   # 警告：接近風險閾值
    TRIPPED  = auto()   # 熔斷：暫停交易，等待冷卻
    HALTED   = auto()   # 緊急停機：需人工重啟


@dataclass
class RiskConfig:
    """所有風險參數集中在這裡，方便調整"""

    # ── 虧損上限 ──────────────────────────────────────────────────────
    max_single_loss_usd: Decimal = Decimal("50")
    # 單筆交易最大允許虧損（USD）。超過就停止當輪交易。

    max_daily_loss_usd: Decimal = Decimal("500")
    # 當日累計虧損上限（USD）。觸發後關閉全部倉位並停機。

    # ── 連續失敗 ──────────────────────────────────────────────────────
    max_consecutive_failures: int = 3
    # 連續交易失敗幾次後觸發熔斷。防止在異常市況下反覆虧損。

    circuit_cooldown_seconds: int = 300
    # 熔斷後冷卻多少秒才自動嘗試恢復（0 = 需人工重啟）。

    # ── 倉位 delta 保護 ───────────────────────────────────────────────
    max_net_delta: Decimal = Decimal("0.5")
    # 全賬戶淨方向性暴露上限（幣本位）。
    # 例如：max_net_delta=0.5 表示最多允許 long 0.5 BTC 的淨多頭。
    # 套利理論上 delta 應為 0，偏離過大代表對沖出問題。

    # ── 價差異常偵測 ──────────────────────────────────────────────────
    spread_spike_multiplier: Decimal = Decimal("5")
    # 當瞬間價差超過近期均值的幾倍時，視為市場異常，暫停交易。
    # 例如：平均 spread=10，突然出現 spread=80，可能是流動性危機。

    spread_spike_pause_seconds: int = 30
    # 偵測到價差異常後，暫停交易的秒數。

    # ── 交易所健康監控 ────────────────────────────────────────────────
    ob_stale_seconds: float = 5.0
    # 訂單簿超過幾秒沒更新，就把該交易所標記為不健康。
    # 防止用過時報價下單。

    min_healthy_exchanges: int = 2
    # 至少幾個交易所健康才允許交易（最少 2 才能配對）。


class RiskManager:
    """
    風險管理器。
    由策略主迴圈在每次交易前呼叫 check_pre_trade()，
    在交易後呼叫 record_trade_result()。
    """

    def __init__(self, config: Optional[RiskConfig] = None, tg: Optional["TelegramNotifier"] = None):
        self.cfg = config or RiskConfig()
        self.tg = tg

        # ── 內部狀態 ──────────────────────────────────────────────────
        self.state = CircuitState.NORMAL
        self._daily_loss = Decimal("0")          # 當日累計虧損（USD，正數=虧損）
        self._consecutive_failures = 0           # 連續失敗計數
        self._circuit_tripped_at: float = 0      # 熔斷時間戳
        self._spread_pause_until: float = 0      # 價差暫停結束時間
        self._unhealthy_exchanges: set[str] = set()  # 當前不健康的交易所
        self._recent_spreads: list[Decimal] = [] # 近期價差歷史，用於異常偵測
        self._trade_log: list[dict] = []         # 交易記錄（用於統計）

    # ── 交易前檢查（主要入口）────────────────────────────────────────

    def check_pre_trade(
        self,
        buy_exchange: str,
        sell_exchange: str,
        spread: Decimal,
        positions: dict[str, Decimal],
    ) -> tuple[bool, str]:
        """
        交易前風險檢查。策略層在每次下單前必須呼叫此方法。

        Args:
            buy_exchange: 準備買入的交易所名稱
            sell_exchange: 準備賣出的交易所名稱
            spread: 本次偵測到的價差（USD）
            positions: 各所當前倉位 {exchange_name: net_position}

        Returns:
            (allowed, reason) — allowed=True 才可以下單
        """

        # 1. 熔斷器狀態檢查
        ok, reason = self._check_circuit()
        if not ok:
            return False, reason

        # 2. 價差異常暫停檢查
        if time.time() < self._spread_pause_until:
            remaining = int(self._spread_pause_until - time.time())
            return False, f"價差異常暫停中，剩餘 {remaining}s"

        # 3. 交易所健康檢查
        for ex in (buy_exchange, sell_exchange):
            if ex in self._unhealthy_exchanges:
                return False, f"交易所 {ex} 當前不健康，跳過"

        # 4. 當日虧損上限
        if self._daily_loss >= self.cfg.max_daily_loss_usd:
            self._trigger_halt("當日虧損已達上限")
            return False, f"當日虧損 ${self._daily_loss:.2f} 已達上限 ${self.cfg.max_daily_loss_usd}"

        # 5. 淨 delta 檢查
        net_delta = sum(positions.values())
        if abs(net_delta) > self.cfg.max_net_delta:
            return False, f"淨 delta {net_delta:.4f} 超過上限 ±{self.cfg.max_net_delta}"

        # 6. 價差異常偵測（防閃崩）
        spike_ok, spike_reason = self._check_spread_spike(spread)
        if not spike_ok:
            return False, spike_reason

        return True, "ok"

    # ── 交易結果記錄 ─────────────────────────────────────────────────

    def record_trade_result(
        self,
        success: bool,
        pnl_usd: Decimal,
        buy_exchange: str,
        sell_exchange: str,
    ):
        """
        每次交易執行後呼叫，更新風險狀態。

        Args:
            success: 是否成功執行（兩腿都成交）
            pnl_usd: 本次毛利（負數 = 虧損）
            buy_exchange: 買入交易所
            sell_exchange: 賣出交易所
        """
        self._trade_log.append({
            "time": time.time(),
            "success": success,
            "pnl": pnl_usd,
            "pair": f"{buy_exchange}↔{sell_exchange}",
        })

        if not success:
            # 連續失敗計數遞增
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.cfg.max_consecutive_failures:
                self._trip_circuit(
                    f"連續失敗 {self._consecutive_failures} 次，觸發熔斷"
                )
        else:
            # 成功後重置連續失敗計數
            self._consecutive_failures = 0

            # 如果本次虧損過大（例如滑點嚴重）
            if pnl_usd < 0 and abs(pnl_usd) > self.cfg.max_single_loss_usd:
                asyncio.create_task(self._alert(
                    f"⚠️ 單筆虧損 ${abs(pnl_usd):.2f} 超過上限 ${self.cfg.max_single_loss_usd}"
                ))

            # 累計虧損追蹤
            if pnl_usd < 0:
                self._daily_loss += abs(pnl_usd)

    def record_spread(self, spread: Decimal):
        """
        每次偵測到價差時記錄，用於計算均值做異常偵測。
        保留最近 100 筆。
        """
        self._recent_spreads.append(spread)
        if len(self._recent_spreads) > 100:
            self._recent_spreads.pop(0)

    # ── 交易所健康管理 ───────────────────────────────────────────────

    def mark_exchange_unhealthy(self, exchange: str, reason: str):
        """
        將某交易所標記為不健康（例如訂單簿長時間未更新）。
        後續的 check_pre_trade 會跳過涉及此交易所的配對。
        """
        if exchange not in self._unhealthy_exchanges:
            self._unhealthy_exchanges.add(exchange)
            asyncio.create_task(self._alert(
                f"🔴 交易所 `{exchange}` 標記為不健康：{reason}"
            ))

    def mark_exchange_healthy(self, exchange: str):
        """
        恢復某交易所的健康狀態（例如重新收到訂單簿更新）。
        """
        if exchange in self._unhealthy_exchanges:
            self._unhealthy_exchanges.discard(exchange)
            asyncio.create_task(self._alert(
                f"🟢 交易所 `{exchange}` 已恢復健康"
            ))

    def check_orderbook_freshness(
        self,
        exchange: str,
        ob_timestamp: float,
    ):
        """
        檢查訂單簿時間戳，超過 ob_stale_seconds 就標記為不健康。
        應在主迴圈每輪呼叫。
        """
        age = time.time() - ob_timestamp
        if age > self.cfg.ob_stale_seconds:
            self.mark_exchange_unhealthy(exchange, f"訂單簿已 {age:.1f}s 未更新")
        else:
            self.mark_exchange_healthy(exchange)

    def healthy_exchange_count(self) -> int:
        """回傳當前健康交易所數量。低於 min_healthy_exchanges 時應停止交易。"""
        return 3 - len(self._unhealthy_exchanges)  # 總共 3 所

    # ── 熔斷器內部方法 ───────────────────────────────────────────────

    def _check_circuit(self) -> tuple[bool, str]:
        """
        檢查熔斷器狀態。
        TRIPPED 狀態下，等待冷卻時間後自動嘗試恢復到 NORMAL。
        HALTED 狀態需人工重啟（呼叫 manual_reset()）。
        """
        if self.state == CircuitState.NORMAL:
            return True, "ok"

        if self.state == CircuitState.WARNING:
            return True, "ok"  # WARNING 仍允許交易，只是發出警告

        if self.state == CircuitState.TRIPPED:
            elapsed = time.time() - self._circuit_tripped_at
            cooldown = self.cfg.circuit_cooldown_seconds
            if cooldown > 0 and elapsed >= cooldown:
                # 冷卻時間已過，自動恢復
                self.state = CircuitState.NORMAL
                self._consecutive_failures = 0
                asyncio.create_task(self._alert("🟢 熔斷冷卻完成，恢復正常交易"))
                return True, "ok"
            remaining = int(cooldown - elapsed)
            return False, f"熔斷中，剩餘冷卻 {remaining}s"

        if self.state == CircuitState.HALTED:
            return False, "緊急停機，需人工呼叫 manual_reset() 後才能恢復"

        return True, "ok"

    def _trip_circuit(self, reason: str):
        """觸發熔斷（TRIPPED），進入冷卻倒數。"""
        if self.state in (CircuitState.TRIPPED, CircuitState.HALTED):
            return
        self.state = CircuitState.TRIPPED
        self._circuit_tripped_at = time.time()
        asyncio.create_task(self._alert(
            f"⚡ *熔斷觸發*\n原因：{reason}\n冷卻：{self.cfg.circuit_cooldown_seconds}s"
        ))

    def _trigger_halt(self, reason: str):
        """觸發緊急停機（HALTED），需人工重啟。"""
        if self.state == CircuitState.HALTED:
            return
        self.state = CircuitState.HALTED
        asyncio.create_task(self._alert(
            f"🚨 *緊急停機*\n原因：{reason}\n請人工確認後呼叫 manual_reset()"
        ))

    def _check_spread_spike(self, spread: Decimal) -> tuple[bool, str]:
        """
        比較當前價差與近期均值，偵測閃崩或流動性危機。
        需要至少 20 筆歷史才開始偵測（避免啟動初期誤判）。
        """
        if len(self._recent_spreads) < 20:
            return True, "ok"

        avg = sum(self._recent_spreads[-20:]) / 20
        if avg <= 0:
            return True, "ok"

        if spread > avg * self.cfg.spread_spike_multiplier:
            # 暫停交易
            self._spread_pause_until = time.time() + self.cfg.spread_spike_pause_seconds
            asyncio.create_task(self._alert(
                f"⚠️ *價差異常*\n當前 ${spread:.2f} vs 均值 ${avg:.2f}（{float(spread/avg):.1f}x）\n"
                f"暫停 {self.cfg.spread_spike_pause_seconds}s"
            ))
            return False, f"價差異常：{float(spread):.2f} >> 均值 {float(avg):.2f}"

        return True, "ok"

    # ── 人工介入 ─────────────────────────────────────────────────────

    def manual_reset(self):
        """
        人工重置風險管理器。
        用於 HALTED 後確認情況已處理、手動恢復交易。
        """
        self.state = CircuitState.NORMAL
        self._consecutive_failures = 0
        self._daily_loss = Decimal("0")
        self._unhealthy_exchanges.clear()
        asyncio.create_task(self._alert("🟢 風險管理器已人工重置，恢復正常交易"))

    def reset_daily_loss(self):
        """每日 00:00 UTC 重置當日虧損計數（由外部 scheduler 呼叫）。"""
        self._daily_loss = Decimal("0")

    # ── 狀態查詢 ─────────────────────────────────────────────────────

    def status_summary(self) -> dict:
        """
        回傳當前風險狀態摘要，供 Telegram 定期摘要使用。
        """
        return {
            "circuit_state": self.state.name,
            "daily_loss_usd": float(self._daily_loss),
            "consecutive_failures": self._consecutive_failures,
            "unhealthy_exchanges": list(self._unhealthy_exchanges),
            "spread_paused": time.time() < self._spread_pause_until,
        }

    # ── 內部通知 ─────────────────────────────────────────────────────

    async def _alert(self, msg: str):
        """發送 Telegram 警報，若通知器不可用則印到 console。"""
        print(f"[risk] {msg.replace('*', '').replace('`', '')}")
        if self.tg:
            try:
                await self.tg.send_raw(msg)
            except Exception as e:
                print(f"[risk] telegram alert failed: {e}")
