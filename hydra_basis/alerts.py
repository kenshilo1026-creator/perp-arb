from __future__ import annotations

from hydra_basis.config import ASSUMED_LEVERAGE
from hydra_basis.formatting import fmt_pct


def _spot_perp_roc(annualized_avg: float, leverage: int = ASSUMED_LEVERAGE) -> float:
    return annualized_avg * leverage / (leverage + 1)


def _perp_perp_roc(annualized_avg: float, leverage: int = ASSUMED_LEVERAGE) -> float:
    return annualized_avg * leverage / 2


def _safe_spread_pct(spread: dict[str, float | int] | None) -> float | None:
    if spread is None or "spread_pct" not in spread:
        return None
    try:
        return float(spread["spread_pct"])
    except (TypeError, ValueError):
        return None


def select_best_alerts_by_symbol(
    opportunities: list[dict],
    *,
    min_annualized_avg: float,
    spreads_by_venue_symbol: dict[tuple[str, str], dict] | None = None,
    max_spread_pct: float = 0.001,
) -> list[dict]:
    best_by_symbol: dict[str, dict] = {}

    for opportunity in opportunities:
        if not opportunity["stats"]["signal"]:
            continue
        if opportunity["stats"]["annualized_avg"] <= min_annualized_avg:
            continue
        if spreads_by_venue_symbol is not None:
            short_spread = spreads_by_venue_symbol.get((opportunity["short_venue"], opportunity["symbol"]))
            long_spread = spreads_by_venue_symbol.get((opportunity["long_venue"], opportunity["symbol"]))
            short_spread_pct = _safe_spread_pct(short_spread)
            long_spread_pct = _safe_spread_pct(long_spread)
            if (
                short_spread_pct is not None
                and long_spread_pct is not None
                and short_spread_pct > max_spread_pct
                and long_spread_pct > max_spread_pct
            ):
                continue

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
        rows.append(
            (
                roc,
                (
                    f"跨所 | {alert['symbol']} | 做空 {alert['short_venue']} / 做多 {alert['long_venue']} | "
                    f"年化 {fmt_pct(alert['stats']['annualized_avg'])} | "
                    f"資本回報 {fmt_pct(roc)} | "
                    f"正收益占比 {alert['stats']['positive_ratio']:.1%}"
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
