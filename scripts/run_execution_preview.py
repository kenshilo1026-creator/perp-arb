from __future__ import annotations

import asyncio
import math

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover - module import path for package mode
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.config import EXECUTION_VENUES_PATH, MONITOR_SIGNALS_PATH
from hydra_basis.execution_engine.interfaces import FakeExecutionAdapter
from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
from hydra_basis.execution_engine.preview import build_execution_preview
from hydra_basis.execution_engine.priority import load_execution_priorities
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol
from hydra_basis.formatting import fmt_pct


def compute_batch_count(total_usd: float, clip_usd: float) -> int:
    if total_usd <= 0 or clip_usd <= 0:
        raise RuntimeError("total_usd and clip_usd must be positive")
    return math.ceil(total_usd / clip_usd)


def prompt_text(label: str) -> str:
    value = input(f"{label}: ").strip().lstrip("\ufeff")
    if not value:
        raise RuntimeError(f"{label} cannot be empty")
    return value


def prompt_float(label: str) -> float:
    value = prompt_text(label)
    try:
        number = float(value)
    except ValueError as exc:
        raise RuntimeError(f"{label} must be a number") from exc
    if number <= 0:
        raise RuntimeError(f"{label} must be positive")
    return number


async def run_preview() -> None:
    symbol = prompt_text("ticker").upper()
    total_usd = prompt_float("total_usd")
    clip_usd = prompt_float("clip_usd")
    signal = load_best_signal_for_symbol(path=MONITOR_SIGNALS_PATH, symbol=symbol)
    priorities = load_execution_priorities(EXECUTION_VENUES_PATH)

    async with aiohttp.ClientSession() as session:
        short_book = await fetch_orderbook_snapshot(
            session,
            venue=signal.short_venue,
            symbol=signal.symbol,
            clip_usd=clip_usd,
        )
        long_book = await fetch_orderbook_snapshot(
            session,
            venue=signal.long_venue,
            symbol=signal.symbol,
            clip_usd=clip_usd,
        )

    preview = build_execution_preview(
        request=type("Request", (), {"symbol": symbol, "total_usd": total_usd, "clip_usd": clip_usd})(),
        signal=signal,
        priorities=priorities,
        adapters={
            signal.short_venue: FakeExecutionAdapter(signal.short_venue, short_book),
            signal.long_venue: FakeExecutionAdapter(signal.long_venue, long_book),
        },
    )

    print("execution preview")
    print(f"ticker: {preview.symbol}")
    print(f"signal_annualized: {fmt_pct(signal.annualized_avg)}")
    print(f"short_venue: {signal.short_venue}")
    print(f"long_venue: {signal.long_venue}")
    print(f"限價方: {preview.maker_venue}")
    print(f"市價方: {preview.taker_venue}")
    print(f"total_usd: {preview.total_usd:.2f}")
    print(f"clip_usd: {preview.clip_usd:.2f}")
    print(f"batch_count: {preview.batch_count}")
    print(f"{signal.short_venue}_spread: {fmt_pct(preview.maker_spread_pct if preview.maker_venue == signal.short_venue else preview.taker_spread_pct)}")
    print(f"{signal.long_venue}_spread: {fmt_pct(preview.maker_spread_pct if preview.maker_venue == signal.long_venue else preview.taker_spread_pct)}")
    if preview.requires_confirm:
        answer = input("spread > 0.1%, continue? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            print("execution preview cancelled")
            return
    print("execution preview ready")


def main() -> None:
    asyncio.run(run_preview())


if __name__ == "__main__":
    main()
