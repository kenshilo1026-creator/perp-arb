from __future__ import annotations

import aiohttp
from decimal import Decimal

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


def estimate_clip_usd_from_size(*, clip_size: Decimal, short_book: dict, long_book: dict) -> float:
    short_mid = (float(short_book["bid"]) + float(short_book["ask"])) / 2
    long_mid = (float(long_book["bid"]) + float(long_book["ask"])) / 2
    mid = (short_mid + long_mid) / 2
    return float(clip_size) * mid


async def prepare_execution_preview_for_size(*, symbol: str, total_size: Decimal, clip_size: Decimal):
    if total_size <= 0 or clip_size <= 0:
        raise RuntimeError("total_size and clip_size must be positive")

    signal = load_best_signal_for_symbol(path=MONITOR_SIGNALS_PATH, symbol=symbol)
    priorities = load_execution_priorities(EXECUTION_VENUES_PATH)

    async with aiohttp.ClientSession() as session:
        initial_clip_usd = 1000.0
        short_book = await fetch_orderbook_snapshot(
            session,
            venue=signal.short_venue,
            symbol=signal.symbol,
            clip_usd=initial_clip_usd,
        )
        long_book = await fetch_orderbook_snapshot(
            session,
            venue=signal.long_venue,
            symbol=signal.symbol,
            clip_usd=initial_clip_usd,
        )
        clip_usd = estimate_clip_usd_from_size(
            clip_size=clip_size,
            short_book=short_book,
            long_book=long_book,
        )
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

    total_usd = clip_usd * float(total_size / clip_size)
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
