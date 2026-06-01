from __future__ import annotations

import json
from pathlib import Path

from hydra_basis.spread_monitor.models import SpreadMonitorConfig


def _strip_json_line_comments(text: str) -> str:
    lines: list[str] = []
    for line in text.splitlines():
        comment_index = line.find("//")
        if comment_index >= 0:
            line = line[:comment_index]
        lines.append(line)
    return "\n".join(lines)


def load_spread_monitor_config(path: Path) -> SpreadMonitorConfig:
    payload = json.loads(_strip_json_line_comments(path.read_text(encoding="utf-8")))
    return SpreadMonitorConfig(
        venues=[str(venue).lower() for venue in payload["venues"]],
        max_funding_rate_pct=float(payload["max_funding_rate_pct"]),
        max_orderbook_spread_pct=float(payload["max_orderbook_spread_pct"]),
        min_cross_exchange_spread_pct=float(payload["min_cross_exchange_spread_pct"]),
        execution_notional_usd=float(payload["execution_notional_usd"]),
        max_stale_seconds=int(payload["max_stale_seconds"]),
        max_price_deviation_pct=float(payload["max_price_deviation_pct"]),
        max_short_term_move_pct=float(payload["max_short_term_move_pct"]),
        hedge_retry_count=int(payload["hedge_retry_count"]),
    )
