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
        loaded: dict[tuple[str, str], dict[str, float | int]] = {}
        for key, value in raw.items():
            split_key = tuple(key.split("::", 1))
            if "status" in value:
                loaded[split_key] = {"status": str(value["status"])}
                continue
            loaded[split_key] = {
                "bid": float(value["bid"]),
                "ask": float(value["ask"]),
                "spread_pct": float(value["spread_pct"]),
                "ts_ms": int(value["ts_ms"]),
            }
        return loaded

    def save(self, spreads: dict[tuple[str, str], dict[str, float | int]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serialized: dict[str, dict[str, float | int | str]] = {}
        for (venue, symbol), value in spreads.items():
            if "status" in value:
                serialized[f"{venue}::{symbol}"] = {"status": str(value["status"])}
                continue
            if "bid" not in value:
                continue
            serialized[f"{venue}::{symbol}"] = {
                "bid": float(value["bid"]),
                "ask": float(value["ask"]),
                "spread_pct": float(value["spread_pct"]),
                "ts_ms": int(value["ts_ms"]),
            }
        self.path.write_text(json.dumps(serialized, ensure_ascii=True, indent=2), encoding="utf-8")
