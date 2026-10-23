"""Durable, non-secret reporting for unresolved hedge exposure."""
from __future__ import annotations

import json
import time
from pathlib import Path


def append_residual_report(path: Path | str, report: dict) -> dict:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "event": "UNRESOLVED_HEDGE_EXPOSURE",
        "recorded_unix_ms": int(time.time() * 1000),
        **report,
    }
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n")
        handle.flush()
    return payload
