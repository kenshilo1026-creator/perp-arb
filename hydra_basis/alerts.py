from __future__ import annotations

import math
import time
from decimal import Decimal

from hydra_basis.config import ASSUMED_LEVERAGE
from hydra_basis.formatting import fmt_pct
from hydra_basis.execution_engine.priority import default_maker_for_pair


def _spot_perp_roc(annualized_avg: float, leverage: int = ASSUMED_LEVERAGE) -> float:
    return annualized_avg * leverage / (leverage + 1)


def _perp_perp_roc(annualized_avg: float, leverage: int = ASSUMED_LEVERAGE) -> float:
    return annualized_avg * leverage / 2


def estimate_maker_taker_entry(
    opportunity: dict, books: dict, *, now_ms: int, max_quote_age_ms: int,
) -> dict:
    """Estimate passive top-of-book maker prices, not guaranteed fills or net P&L."""
    prices = {}
    for venue in (opportunity["short_venue"], opportunity["long_venue"]):
        book = books.get((venue, opportunity["symbol"]), {})
        try:
            bid, ask = Decimal(str(book["bid"])), Decimal(str(book["ask"]))
            timestamp = float(book["ts_ms"])
            if not bid.is_finite() or not ask.is_finite() or not 0 < bid <= ask:
                raise ValueError("invalid prices")
            if not math.isfinite(timestamp) or timestamp <= 0 or not -5000 <= now_ms - timestamp <= max_quote_age_ms:
                return {"status": "unconfirmed", "reason": "stale_or_undated_quote"}
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return {"status": "unconfirmed", "reason": "missing_or_invalid_quote"}
        prices[venue] = bid, ask
    short, long = opportunity["short_venue"], opportunity["long_venue"]
    short_bid, short_ask = prices[short]
    long_bid, long_ask = prices[long]
    # Positive means buy dearer than sell; negative is a favorable entry basis.
    choices = [
        (long, short, "BUY", long_bid, short_bid, (long_bid - short_bid) / long_bid),
        (short, long, "SELL", short_ask, long_ask, (long_ask - short_ask) / short_ask),
    ]
    preferred_maker = default_maker_for_pair(short, long)
    if preferred_maker is not None:
        choices = [choice for choice in choices if choice[0] == preferred_maker]
    maker, taker, side, maker_price, taker_price, gap = min(choices, key=lambda item: item[-1])
    return {"status": "estimated", "maker_venue": maker, "taker_venue": taker,
            "maker_side": side, "maker_price": float(maker_price), "taker_price": float(taker_price),
            "adverse_gap_pct": float(gap)}


def select_best_alerts_by_symbol(
    opportunities: list[dict],
    *,
    min_annualized_avg: float,
    spreads_by_venue_symbol: dict[tuple[str, str], dict] | None = None,
    max_adverse_gap_pct: float = 0.003,
    max_quote_age_ms: int = 120_000,
    now_ms: int | None = None,
) -> list[dict]:
    best_by_symbol: dict[str, dict] = {}
    current_ms = int(time.time() * 1000) if now_ms is None else now_ms

    for opportunity in opportunities:
        if not opportunity["stats"]["signal"]:
            continue
        if opportunity["stats"]["annualized_avg"] <= min_annualized_avg:
            continue
        estimate = estimate_maker_taker_entry(
            opportunity, spreads_by_venue_symbol or {}, now_ms=current_ms, max_quote_age_ms=max_quote_age_ms,
        )
        if estimate["status"] == "estimated" and estimate["adverse_gap_pct"] > max_adverse_gap_pct:
            print(f"alert_skip symbol={opportunity['symbol']} short={opportunity['short_venue']} "
                  f"long={opportunity['long_venue']} reason=maker_taker_gap "
                  f"gap={fmt_pct(estimate['adverse_gap_pct'])} limit={fmt_pct(max_adverse_gap_pct)}")
            continue
        opportunity = {**opportunity, "entry_estimate": estimate}

        symbol = opportunity["symbol"]
        current_best = best_by_symbol.get(symbol)
        if current_best is None or opportunity["stats"]["annualized_avg"] > current_best["stats"]["annualized_avg"]:
            best_by_symbol[symbol] = opportunity

    return sorted(best_by_symbol.values(), key=lambda item: item["symbol"])


def select_best_spot_perp_alerts_by_symbol(
    opportunities: list[dict],
    *,
    min_annualized_avg: float,
) -> list[dict]:
    best_by_symbol: dict[str, dict] = {}

    for opportunity in opportunities:
        if not opportunity["stats"]["signal"]:
            continue
        if opportunity["stats"]["annualized_avg"] <= min_annualized_avg:
            continue

        symbol = opportunity["symbol"]
        current_best = best_by_symbol.get(symbol)
        if current_best is None or opportunity["stats"]["annualized_avg"] > current_best["stats"]["annualized_avg"]:
            best_by_symbol[symbol] = opportunity

    return sorted(best_by_symbol.values(), key=lambda item: item["symbol"])


def build_ranked_alert_digest(
    *,
    cross_exchange_alerts: list[dict],
    spot_perp_alerts: list[dict],
    max_rows: int = 20,
) -> str:
    rows: list[tuple[float, str]] = []

    for alert in cross_exchange_alerts:
        roc = _perp_perp_roc(float(alert["stats"]["annualized_avg"]))
        entry = alert.get("entry_estimate")
        entry_text = ""
        if entry:
            if entry["status"] == "estimated":
                gap = entry["adverse_gap_pct"]
                label = "不利價差" if gap >= 0 else "有利價差"
                entry_text = f" | Maker {entry['maker_venue']} | 預估{label} {fmt_pct(abs(gap))}"
            else:
                entry_text = " | Maker/Taker 入場價差未確認（報價缺失或過期）"
        rows.append(
            (
                roc,
                (
                    f"跨所 | {alert['symbol']} | 做空 {alert['short_venue']} / 做多 {alert['long_venue']} | "
                    f"年化 {fmt_pct(alert['stats']['annualized_avg'])} | "
                    f"資本回報 {fmt_pct(roc)} | "
                    f"正收益占比 {alert['stats']['positive_ratio']:.1%}{entry_text}"
                ),
            )
        )

    for alert in spot_perp_alerts:
        roc = _spot_perp_roc(float(alert["stats"]["annualized_avg"]))
        rows.append(
            (
                roc,
                (
                    f"SPOT-PERP | {alert['symbol']} | 做空永續 {alert['venue']} / 買入現貨 | "
                    f"年化 {fmt_pct(alert['stats']['annualized_avg'])} | "
                    f"資本回報 {fmt_pct(roc)} | "
                    f"正收益占比 {alert['stats']['positive_ratio']:.1%}"
                ),
            )
        )

    rows.sort(key=lambda item: item[0], reverse=True)
    if not rows:
        return ""

    lines = ["<b>套利訊號總表 Top 20</b>"]
    for index, (_, line) in enumerate(rows[:max_rows], start=1):
        lines.append(f"#{index} {line}")
    return "\n".join(lines)
