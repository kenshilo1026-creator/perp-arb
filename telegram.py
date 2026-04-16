"""
Telegram 通知模組
==================
功能：
  - 套利機會偵測通知（含三所即時 bid/ask、價差、預估利潤）
  - 執行結果通知（成功/失敗/緊急對沖）
  - 每 5 分鐘定期摘要報告（累計交易次數、PnL、各所倉位）
  - 錯誤警報

設定方式：
  1. 在 Telegram 搜尋 @BotFather，建立 bot，取得 token
  2. 傳一則訊息給你的 bot，再用以下 URL 取得 chat_id：
     https://api.telegram.org/bot<TOKEN>/getUpdates
  3. 填入 .env：
       TELEGRAM_BOT_TOKEN=your_token
       TELEGRAM_CHAT_ID=your_chat_id

Env vars:
  TELEGRAM_BOT_TOKEN   - BotFather 給的 token
  TELEGRAM_CHAT_ID     - 你的 chat id（或群組 id）
  TELEGRAM_ENABLED     - "true" / "false"（預設 true）
"""

import asyncio
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import aiohttp


# ── 訊息模板 ──────────────────────────────────────────────────────────────────

def _fmt(v: Decimal, decimals: int = 4) -> str:
    return f"{float(v):,.{decimals}f}"

def _sign(v: Decimal) -> str:
    return "+" if v >= 0 else ""


class TelegramNotifier:
    """
    非同步 Telegram 通知器。
    所有發送操作為 fire-and-forget，不阻塞主交易迴圈。
    """

    TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self):
        self._token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self._enabled = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"
        self._session: Optional[aiohttp.ClientSession] = None

        # 摘要報告用的統計緩衝
        self._stats = SummaryStats()
        self._summary_task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self):
        if not self._enabled:
            return
        if not self._token or not self._chat_id:
            print("[telegram] WARNING: BOT_TOKEN or CHAT_ID not set, notifications disabled")
            self._enabled = False
            return
        self._session = aiohttp.ClientSession()
        self._summary_task = asyncio.create_task(self._summary_loop())
        await self.send_raw("🤖 *套利機器人已啟動*\n三所：Variational ✦ Hyperliquid ✦ Lighter")

    async def stop(self):
        if self._summary_task:
            self._summary_task.cancel()
        await self._send_final_summary()
        if self._session:
            await self._session.close()

    # ── 對外介面（主要） ──────────────────────────────────────────────────

    async def notify_opportunity(
        self,
        buy_exchange: str,
        sell_exchange: str,
        buy_price: Decimal,
        sell_price: Decimal,
        spread: Decimal,
        quantity: Decimal,
        threshold: Decimal,
        gross_pnl: Decimal,
    ):
        """偵測到套利機會時發送通知。"""
        self._stats.opportunities_detected += 1

        # 只在超過閾值 2 倍以上才發通知（避免洗版）
        if spread < threshold * 2:
            return

        msg = (
            f"📊 *套利機會偵測*\n"
            f"━━━━━━━━━━━━━━\n"
            f"⬆️  買入：`{buy_exchange.upper()}`  @  `${_fmt(buy_price, 2)}`\n"
            f"⬇️  賣出：`{sell_exchange.upper()}`  @  `${_fmt(sell_price, 2)}`\n"
            f"💹  價差：`${_fmt(spread, 2)}`  （閾值 ${_fmt(threshold, 2)}）\n"
            f"📦  數量：`{_fmt(quantity, 4)}`\n"
            f"💰  預估毛利：`${_fmt(gross_pnl, 4)}`\n"
            f"⏱️  `{_ts()}`"
        )
        await self._send(msg)

    async def notify_execution_success(
        self,
        buy_exchange: str,
        sell_exchange: str,
        buy_price: Decimal,
        sell_price: Decimal,
        filled: Decimal,
        gross_pnl: Decimal,
        buy_tx: str = "",
        sell_tx: str = "",
    ):
        """訂單成功執行。"""
        self._stats.trades_success += 1
        self._stats.cum_pnl += gross_pnl

        tx_info = ""
        if buy_tx:
            tx_info += f"\n🔗  買單 TX：`{buy_tx[:16]}...`"
        if sell_tx:
            tx_info += f"\n🔗  賣單 TX：`{sell_tx[:16]}...`"

        msg = (
            f"✅ *執行成功*\n"
            f"━━━━━━━━━━━━━━\n"
            f"⬆️  買 `{buy_exchange.upper()}`  @  `${_fmt(buy_price, 2)}`\n"
            f"⬇️  賣 `{sell_exchange.upper()}`  @  `${_fmt(sell_price, 2)}`\n"
            f"📦  成交量：`{_fmt(filled, 4)}`\n"
            f"💰  毛利：`{_sign(gross_pnl)}${_fmt(gross_pnl, 4)}`\n"
            f"📈  累計 PnL：`{_sign(self._stats.cum_pnl)}${_fmt(self._stats.cum_pnl, 4)}`"
            f"{tx_info}\n"
            f"⏱️  `{_ts()}`"
        )
        await self._send(msg)

    async def notify_execution_failed(
        self,
        buy_exchange: str,
        sell_exchange: str,
        reason: str,
        emergency_unwind: bool = False,
    ):
        """訂單執行失敗。"""
        self._stats.trades_failed += 1

        icon = "🚨" if emergency_unwind else "❌"
        unwind_note = "\n⚠️  *緊急對沖已觸發*" if emergency_unwind else ""

        msg = (
            f"{icon} *執行失敗*\n"
            f"━━━━━━━━━━━━━━\n"
            f"配對：`{buy_exchange.upper()}` ↔ `{sell_exchange.upper()}`\n"
            f"原因：`{reason[:120]}`"
            f"{unwind_note}\n"
            f"⏱️  `{_ts()}`"
        )
        await self._send(msg)

    async def notify_position_update(
        self,
        positions: dict[str, Decimal],
    ):
        """倉位變化通知（由策略層按需呼叫）。"""
        lines = []
        for ex, pos in positions.items():
            arrow = "📈" if pos > 0 else ("📉" if pos < 0 else "➖")
            lines.append(f"{arrow}  `{ex.upper()}`：`{_sign(pos)}{_fmt(pos, 4)}`")

        msg = (
            f"📦 *倉位更新*\n"
            f"━━━━━━━━━━━━━━\n"
            + "\n".join(lines) + "\n"
            f"⏱️  `{_ts()}`"
        )
        await self._send(msg)

    async def notify_error(self, context: str, error: str):
        """嚴重錯誤警報。"""
        msg = (
            f"🔴 *系統錯誤*\n"
            f"━━━━━━━━━━━━━━\n"
            f"位置：`{context}`\n"
            f"錯誤：`{error[:200]}`\n"
            f"⏱️  `{_ts()}`"
        )
        await self._send(msg)

    async def send_raw(self, text: str):
        """直接發送原始訊息（Markdown 格式）。"""
        await self._send(text)

    # ── 5 分鐘定期摘要 ───────────────────────────────────────────────────

    async def _summary_loop(self, interval: int = 300):
        """每 5 分鐘發送一次摘要報告。"""
        while True:
            await asyncio.sleep(interval)
            try:
                await self._send_summary()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[telegram] summary error: {e}")

    async def _send_summary(self):
        s = self._stats
        elapsed_min = (time.time() - s.start_time) / 60
        rate = s.trades_success / elapsed_min if elapsed_min > 0 else 0

        pnl_icon = "📈" if s.cum_pnl >= 0 else "📉"

        msg = (
            f"📋 *5 分鐘定期摘要*\n"
            f"━━━━━━━━━━━━━━\n"
            f"🔍  偵測機會：`{s.opportunities_detected}` 次\n"
            f"✅  執行成功：`{s.trades_success}` 筆\n"
            f"❌  執行失敗：`{s.trades_failed}` 筆\n"
            f"📊  成交率：`{_pct(s.trades_success, s.trades_success + s.trades_failed)}`\n"
            f"⚡  頻率：`{rate:.1f}` 筆/分鐘\n"
            f"━━━━━━━━━━━━━━\n"
            f"{pnl_icon}  累計 PnL：`{_sign(s.cum_pnl)}${_fmt(s.cum_pnl, 4)}`\n"
            f"━━━━━━━━━━━━━━\n"
            f"⏱️  運行 `{elapsed_min:.0f}` 分鐘 | `{_ts()}`"
        )
        await self._send(msg)
        # 重置本輪統計（累計 PnL 不重置）
        s.reset_interval()

    async def _send_final_summary(self):
        s = self._stats
        elapsed_min = (time.time() - s.start_time) / 60
        msg = (
            f"🛑 *機器人已停止*\n"
            f"━━━━━━━━━━━━━━\n"
            f"運行時間：`{elapsed_min:.0f}` 分鐘\n"
            f"總執行：`{s.trades_success}` 筆成功 / `{s.trades_failed}` 筆失敗\n"
            f"最終 PnL：`{_sign(s.cum_pnl)}${_fmt(s.cum_pnl, 4)}`\n"
            f"⏱️  `{_ts()}`"
        )
        await self._send(msg)

    # ── 底層發送 ─────────────────────────────────────────────────────────

    async def _send(self, text: str):
        if not self._enabled or not self._session:
            return
        url = self.TELEGRAM_API.format(token=self._token)
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            async with self._session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                if r.status != 200:
                    body = await r.text()
                    print(f"[telegram] send failed {r.status}: {body[:100]}")
        except Exception as e:
            # 不讓通知失敗影響主交易邏輯
            print(f"[telegram] send error: {e}")


# ── 統計緩衝 ──────────────────────────────────────────────────────────────────

@dataclass
class SummaryStats:
    start_time: float = field(default_factory=time.time)
    opportunities_detected: int = 0
    trades_success: int = 0
    trades_failed: int = 0
    cum_pnl: Decimal = field(default_factory=lambda: Decimal("0"))

    def reset_interval(self):
        """重置本輪計數（保留累計 PnL）。"""
        self.opportunities_detected = 0
        self.trades_success = 0
        self.trades_failed = 0


# ── 工具函數 ──────────────────────────────────────────────────────────────────

def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

def _pct(a: int, total: int) -> str:
    if total == 0:
        return "N/A"
    return f"{a / total * 100:.1f}%"
