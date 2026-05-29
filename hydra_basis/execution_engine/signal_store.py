from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from hydra_basis.execution_engine.models import ExecutionSignal


def save_monitor_signals(*, path: Path, cross_exchange_signals: list[dict[str, Any]]) -> None:
    payload = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "cross_exchange_signals": [
            {
                "symbol": row["symbol"],
                "short_venue": row["short_venue"],
                "long_venue": row["long_venue"],
                "annualized_avg": row["stats"]["annualized_avg"],
                "score": row["stats"]["score"],
                "signal": row["stats"]["signal"],
            }
            for row in sorted(
                cross_exchange_signals,
                key=lambda item: item["stats"]["annualized_avg"],
                reverse=True,
            )
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def load_best_signal_for_symbol(*, path: Path, symbol: str, max_age_hours: int = 24) -> ExecutionSignal:
    payload = json.loads(path.read_text(encoding="utf-8"))
    updated_at = dt.datetime.fromisoformat(payload["updated_at"])
    age = dt.datetime.now(dt.timezone.utc) - updated_at
    if age > dt.timedelta(hours=max_age_hours):
        raise RuntimeError("monitor_signals.json is older than 24 hours")

    for row in payload.get("cross_exchange_signals", []):
        if row.get("symbol", "").upper() == symbol.upper():
            return ExecutionSignal(
                symbol=row["symbol"],
                short_venue=row["short_venue"],
                long_venue=row["long_venue"],
                annualized_avg=float(row["annualized_avg"]),
                score=float(row["score"]),
                updated_at=payload["updated_at"],
            )
    raise RuntimeError(f"signal not found for symbol={symbol}")
