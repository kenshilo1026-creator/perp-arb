from __future__ import annotations


def fmt_pct(value: float) -> str:
    return f"{value * 100:.4f}%"


def build_signal_message(symbol: str, short_venue: str, long_venue: str, stats: dict) -> str:
    return (
        f"🚨 <b>資金費套利訊號</b>\n"
        f"幣種: <b>{symbol}</b>\n"
        f"操作: 做空 <b>{short_venue}</b> / 做多 <b>{long_venue}</b>\n"
        f"{stats.get('analysis_days', 7)}天平均每小時利差: <b>{fmt_pct(stats['avg_hourly'])}</b>\n"
        f"{stats.get('analysis_days', 7)}天樣本簡單年化估算(未扣成本): <b>{fmt_pct(stats['annualized_avg'])}</b>\n"
        f"近{stats.get('analysis_days', 7)}天正收益占比（有效樣本）: <b>{stats['positive_ratio']:.1%}</b>\n"
        f"樣本數: <b>{stats['samples']}</b>\n"
        f"評分: <b>{stats['score']:.2f}</b>\n\n"
        f"提醒: 下單前請確認盤口深度、手續費、滑點、保證金，以及下一次資金費結算時間。"
    )


def build_spot_perp_signal_message(symbol: str, venue: str, stats: dict) -> str:
    return (
        f"🚨 <b>期現套利訊號</b>\n"
        f"幣種: <b>{symbol}</b>\n"
        f"交易所: <b>{venue}</b>\n"
        f"操作: 做空永續 <b>{venue}</b> / 買入現貨 <b>{symbol}</b>\n"
        f"{stats.get('analysis_days', 7)}天平均每小時資金費: <b>{fmt_pct(stats['avg_hourly'])}</b>\n"
        f"{stats.get('analysis_days', 7)}天樣本簡單年化估算(未扣成本): <b>{fmt_pct(stats['annualized_avg'])}</b>\n"
        f"近{stats.get('analysis_days', 7)}天正資費占比（有效樣本）: <b>{stats['positive_ratio']:.1%}</b>\n"
        f"樣本數: <b>{stats['samples']}</b>\n"
        f"評分: <b>{stats['score']:.2f}</b>\n\n"
        f"提醒: 下單前請確認現貨流動性、借貸/持幣成本、手續費、滑點，以及下一次資金費結算時間。"
    )
