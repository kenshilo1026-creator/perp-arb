"""Dry-run profit estimates: what an opportunity would earn at take profit, and
where existing groups stand. Read-only: nothing here places or simulates orders.

Assumption for every "at take profit" figure: the strategy exits when the exit
spread reaches ``take_profit_bps`` (the original's rule) and the long leg's
price is unchanged, so the short leg converges to long * (1 + tp). Fees are the
configured rates on all four fills. Funding, slippage and depth are not
modelled; quantities are notional / price without lot rounding.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from hydra_basis.spread_strategy.core import BPS, ONE, ZERO, Config, State, entry_ratio_required, number


@dataclass(frozen=True)
class OpportunityEstimate:
    symbol: str
    short_venue: str
    long_venue: str
    entry_bps: Decimal
    exit_bps_now: Decimal
    required_bps: Decimal
    qualifies: bool
    blocked_by: str
    quantity: Decimal
    notional_usd: Decimal
    gross_at_tp: Decimal
    fees: Decimal
    net_at_tp: Decimal
    # Entry spread at the clip size from visible depth; None when the book is too thin.
    depth_bps: Decimal | None = None
    depth_status: str = "n/a"


def estimate_opportunity(config: Config, opportunity, notional_usd: Decimal, *,
                         depth_prices: tuple[Decimal, Decimal] | None = None,
                         depth_status: str = "n/a") -> OpportunityEstimate:
    """Profit at take profit; uses the depth-average entry prices when visible depth covers the clip."""
    fs, fl = config.fee_rate(config.short_venue), config.fee_rate(config.long_venue)
    tp = config.take_profit_bps / BPS
    short_entry, long_entry = depth_prices or (opportunity.short_bid, opportunity.long_ask)
    quantity = notional_usd / long_entry
    long_exit = long_entry
    short_exit = long_exit * (ONE + tp)
    gross = quantity * ((short_entry - short_exit) + (long_exit - long_entry))
    fees = quantity * (short_entry * fs + long_entry * fl + short_exit * fs + long_exit * fl)
    return OpportunityEstimate(
        opportunity.symbol, opportunity.short_venue, opportunity.long_venue, opportunity.entry_bps,
        (opportunity.short_ask - opportunity.long_bid) / opportunity.long_bid * BPS,
        (entry_ratio_required(config) - ONE) * BPS, opportunity.qualifies, opportunity.blocked_by,
        quantity, notional_usd, gross, fees, gross - fees,
        None if depth_prices is None else (short_entry - long_entry) / long_entry * BPS, depth_status)


@dataclass(frozen=True)
class GroupEstimate:
    group: str
    symbol: str
    short_venue: str
    long_venue: str
    status: str
    quantity: Decimal
    entry_spread_bps: Decimal
    exit_bps_now: Decimal | None
    close_now: Decimal | None
    at_tp: Decimal | None
    realized: Decimal
    fees_paid: Decimal


def estimate_group(group_id: str, config: Config, state: State, books: dict | None) -> GroupEstimate:
    """``books`` maps venue -> (bid, ask); None when either quote is unavailable."""
    short_q, long_q = number(state.short.quantity), number(state.long.quantity)
    quantity = min(-short_q, long_q) if short_q < 0 < long_q else ZERO
    sa, la = number(state.short.average), number(state.long.average)
    entry_spread = (sa - la) / la * BPS if quantity > 0 else ZERO
    realized = number(state.short.realized) + number(state.long.realized)
    close_now = at_tp = exit_now = None
    if quantity > 0 and books is not None:
        fs, fl = config.fee_rate(config.short_venue), config.fee_rate(config.long_venue)
        short_ask, long_bid = books[config.short_venue][1], books[config.long_venue][0]
        exit_now = (short_ask - long_bid) / long_bid * BPS
        # Opening fees already paid are in ``fees_paid``; these figures count closing fees only.
        close_now = quantity * ((sa - short_ask) + (long_bid - la)) - quantity * (short_ask * fs + long_bid * fl)
        short_tp = long_bid * (ONE + config.take_profit_bps / BPS)
        at_tp = quantity * ((sa - short_tp) + (long_bid - la)) - quantity * (short_tp * fs + long_bid * fl)
    return GroupEstimate(group_id, config.symbol, config.short_venue, config.long_venue, state.status, quantity,
                         entry_spread, exit_now, close_now, at_tp, realized, number(state.fees_usd))


# ---------------------------------------------------------------------------
# Console report
# ---------------------------------------------------------------------------

def _usd(value: Decimal | None) -> str:
    return "-" if value is None else f"{value:+.3f}"


def _bps(value: Decimal | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def format_report(*, now_text: str, feeds: dict[str, bool], take_profit_bps: Decimal,
                  qualifying: list[OpportunityEstimate], near: list[OpportunityEstimate],
                  groups: list[GroupEstimate], histories: list | None = None,
                  history_check_bps: float = 30) -> str:
    lines = [f"===== 乾跑報告 {now_text} | 行情 " + " ".join(
        f"{venue}:{'OK' if ok else 'DOWN'}" for venue, ok in feeds.items()) + " =====",
             f"假設：在價差收斂到止盈門檻 {take_profit_bps} bps 時平倉；只扣手續費，未計資金費率。"
             "「深度後bps」= 按每筆下單量吃掉掛單後的平均價差（有深度數據時毛利/淨利按此計算）；"
             "「不足」= 可見掛單不夠成交一筆；「無數據」= 深度訂閱剛開始，下一份報告會有。", ""]
    header = (f"{'幣種':<10}{'做空':<12}{'做多':<12}{'開倉bps':>8}{'深度後bps':>10}{'門檻bps':>8}{'現平倉bps':>10}"
              f"{'名目USD':>9}{'毛利':>9}{'手續費':>9}{'淨利':>9}")

    def depth_text(e):
        if e.depth_bps is not None:
            return _bps(e.depth_bps)
        return {"thin": "不足", "no_data": "無數據"}.get(e.depth_status, "-")

    def rows(items):
        return [f"{e.symbol:<10}{e.short_venue:<12}{e.long_venue:<12}{_bps(e.entry_bps):>8}{depth_text(e):>10}"
                f"{_bps(e.required_bps):>8}{_bps(e.exit_bps_now):>10}{e.notional_usd:>9.0f}{_usd(e.gross_at_tp):>9}"
                f"{_usd(e.fees):>9}{_usd(e.net_at_tp):>9}" + ("" if e.qualifies else f"  ({_blocked(e.blocked_by)})")
                for e in items]

    lines.append(f"[符合開倉條件] {len(qualifying)} 個")
    lines += [header, *rows(qualifying)] if qualifying else ["  （目前沒有）"]
    lines += ["", "[接近門檻，未達開倉條件] 前幾名"]
    lines += [header, *rows(near)] if near else ["  （沒有正價差）"]
    lines += ["", f"[價差歷史 過去24小時] 現價差 ≥ {history_check_bps} bps 的機會"]
    if histories:
        lines.append(f"{'幣種':<10}{'做空':<12}{'做多':<12}{'現在bps':>8}{'24h中位':>8}{'p90':>7}"
                     f"{'≥門檻時間%':>10}{'達門檻次數':>9}{'收斂次數':>8}{'收斂中位分':>9}{'本次持續分':>9}"
                     f"{'距上次收斂分':>11}{'覆蓋%':>7}  判斷")
        for opportunity, p in histories:
            def num(value, fmt="{:.1f}"):
                return "-" if value is None else fmt.format(value)
            lines.append(f"{opportunity.symbol:<10}{opportunity.short_venue:<12}{opportunity.long_venue:<12}"
                         f"{opportunity.entry_bps:>8.1f}{num(p.median_bps):>8}{num(p.p90_bps):>7}"
                         f"{p.above_pct:>10.1f}{p.episodes:>9}{p.converged_episodes:>8}"
                         f"{num(p.median_minutes_to_converge, '{:.0f}'):>9}{p.current_episode_minutes:>9}"
                         f"{num(p.minutes_since_converged, '{:.0f}'):>11}{p.coverage_pct:>7.0f}  {p.label_text}")
        lines.append(f"  價差以每分鐘收盤/中間價計算；「收斂」= 之後回到 ≤ {take_profit_bps} bps。"
                     "Lighter 沒有公開K線，靠調度器運行時自行記錄，首天覆蓋率會較低。")
        lines.append("  持續型＝幾乎整天都在門檻以上且從未收斂（結構性，開倉後可能無法止盈）；"
                     "反覆收斂型＝曾達門檻後回到止盈位；瞬間型＝極少出現且剛剛才出現。")
    else:
        lines.append("  （目前沒有）")
    lines += ["", f"[現有倉位] {len(groups)} 組"]
    if groups:
        lines.append(f"{'組別':<28}{'狀態':<9}{'數量':>12}{'開倉bps':>8}{'現平倉bps':>10}"
                     f"{'現在平倉':>10}{'止盈時':>10}{'已實現毛利':>11}{'已付手續費':>11}")
        for g in groups:
            lines.append(f"{g.group:<28}{g.status:<9}{g.quantity:>12.4f}{_bps(g.entry_spread_bps):>8}"
                         f"{_bps(g.exit_bps_now):>10}{_usd(g.close_now):>10}{_usd(g.at_tp):>10}"
                         f"{_usd(g.realized):>11}{_usd(g.fees_paid):>11}")
        lines.append("  「現在平倉」「止盈時」= 持倉價差損益扣平倉手續費；開倉手續費見「已付手續費」。")
    else:
        lines.append("  （沒有）")
    return "\n".join(lines)


def _blocked(reason: str) -> str:
    return {"entry_gate": "未達門檻", "funding": "資金費率過高"}.get(reason, reason)
