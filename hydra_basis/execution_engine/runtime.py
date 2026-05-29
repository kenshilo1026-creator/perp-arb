from __future__ import annotations

import aiohttp

from hydra_basis.config import EXECUTION_VENUES_PATH, MONITOR_SIGNALS_PATH
from hydra_basis.execution_engine.interfaces import FakeExecutionAdapter
from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
from hydra_basis.execution_engine.preview import build_execution_preview
from hydra_basis.execution_engine.priority import load_execution_priorities
from hydra_basis.execution_engine.signal_store import load_best_signal_for_symbol


async def prepare_execution_preview(*, symbol: str, total_usd: float, clip_usd: float):
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
    return signal, preview, short_book, long_book
