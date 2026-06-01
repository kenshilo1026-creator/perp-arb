from __future__ import annotations

from hydra_basis.formatting import fmt_pct
from hydra_basis.spread_monitor.models import SpreadOpportunity, SpreadPairFilterDiagnostic


FILTER_REASON_LABELS = {
    "less_than_2_venues": "少於兩個交易所有報價",
    "stale_quotes": "報價過舊",
    "funding_rate_too_high": "資金費過高",
    "missing_bid_ask": "缺少 bid/ask",
    "price_deviation_too_high": "跨所價格偏離過大",
    "no_executable_cross_spread": "沒有可成交價差",
    "orderbook_spread_too_wide": "單所 spread 過闊",
    "cross_spread_below_threshold": "跨所價差低於門檻",
}


def format_spread_filter_reason(reason: str) -> str:
    return FILTER_REASON_LABELS.get(reason, reason)


def format_spread_opportunity(opportunity: SpreadOpportunity) -> str:
    return (
        f"價差機會 | 幣種 {opportunity.symbol} | "
        f"做空 {opportunity.short_venue} @ {opportunity.short_price:.8f} | "
        f"做多 {opportunity.long_venue} @ {opportunity.long_price:.8f} | "
        f"跨所價差 {fmt_pct(opportunity.cross_exchange_spread_pct)} | "
        f"預估利潤 ${opportunity.expected_profit_usd:.4f}"
    )


def format_spread_digest(opportunities: list[SpreadOpportunity]) -> str:
    ranked = sorted(
        opportunities,
        key=lambda item: item.cross_exchange_spread_pct,
        reverse=True,
    )
    lines = ["價差機會彙總"]
    for index, opportunity in enumerate(ranked, start=1):
        lines.append(
            f"#{index} | 幣種 {opportunity.symbol} | "
            f"做空 {opportunity.short_venue} @ {opportunity.short_price:.8f} | "
            f"做多 {opportunity.long_venue} @ {opportunity.long_price:.8f} | "
            f"跨所價差 {fmt_pct(opportunity.cross_exchange_spread_pct)} | "
            f"預估利潤 ${opportunity.expected_profit_usd:.4f}"
        )
    return "\n".join(lines)


def format_spread_opportunity_console(opportunity: SpreadOpportunity) -> str:
    return (
        f"spread opportunity | symbol {opportunity.symbol} | "
        f"short {opportunity.short_venue} @ {opportunity.short_price:.8f} | "
        f"long {opportunity.long_venue} @ {opportunity.long_price:.8f} | "
        f"cross spread {fmt_pct(opportunity.cross_exchange_spread_pct)} | "
        f"expected profit ${opportunity.expected_profit_usd:.4f}"
    )


def format_spread_progress_console(
    *,
    venues: list[str],
    asset_ctx_counts: dict[str, int],
    quote_counts: dict[str, int],
    shared_symbols: int,
) -> str:
    return (
        "spread progress | "
        + "asset_ctx="
        + ",".join(f"{venue}:{asset_ctx_counts.get(venue, 0)}" for venue in venues)
        + " | "
        + "quotes="
        + ",".join(f"{venue}:{quote_counts.get(venue, 0)}" for venue in venues)
        + f" | shared_symbols={shared_symbols}"
    )


def format_spread_live_status_console(
    *,
    venue: str,
    asset_ctx_count: int,
    quote_count: int,
) -> str:
    return f"spread live | venue={venue} | asset_ctx={asset_ctx_count} | quotes={quote_count}"


def format_spread_debug_console(*, symbol: str, reason: str) -> str:
    return f"spread debug | symbol {symbol} | filtered={reason}"


def format_spread_debug_telegram(*, symbol: str, reason: str) -> str:
    return f"價差除錯 | 幣種 {symbol} | 過濾原因={format_spread_filter_reason(reason)}"


def format_spread_pair_debug_console(diagnostic: SpreadPairFilterDiagnostic) -> str:
    spread_text = (
        fmt_pct(diagnostic.cross_exchange_spread_pct)
        if diagnostic.cross_exchange_spread_pct is not None
        else "n/a"
    )
    return (
        f"spread debug | venues {diagnostic.left_venue}/{diagnostic.right_venue} | "
        f"symbol {diagnostic.symbol} | cross spread {spread_text} | "
        f"filtered={diagnostic.reason}"
    )


def format_spread_pair_debug_telegram(diagnostic: SpreadPairFilterDiagnostic) -> str:
    spread_text = (
        fmt_pct(diagnostic.cross_exchange_spread_pct)
        if diagnostic.cross_exchange_spread_pct is not None
        else "n/a"
    )
    return (
        f"價差除錯 | 交易所 {diagnostic.left_venue} / {diagnostic.right_venue} | "
        f"幣種 {diagnostic.symbol} | 目前價差 {spread_text} | "
        f"過濾原因={format_spread_filter_reason(diagnostic.reason)}"
    )
