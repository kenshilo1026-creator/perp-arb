from __future__ import annotations

import json
from pathlib import Path


class OrderbookSpreadStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[tuple[str, str], dict[str, float | int]]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        return {
            tuple(key.split("::", 1)): {
                "bid": float(value["bid"]),
                "ask": float(value["ask"]),
                "spread_pct": float(value["spread_pct"]),
                "ts_ms": int(value["ts_ms"]),
            }
            for key, value in raw.items()
        }

    def save(self, spreads: dict[tuple[str, str], dict[str, float | int]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serialized = {
            f"{venue}::{symbol}": {
                "bid": float(value["bid"]),
                "ask": float(value["ask"]),
                "spread_pct": float(value["spread_pct"]),
                "ts_ms": int(value["ts_ms"]),
            }
            for (venue, symbol), value in spreads.items()
        }
        self.path.write_text(json.dumps(serialized, ensure_ascii=True, indent=2), encoding="utf-8")
