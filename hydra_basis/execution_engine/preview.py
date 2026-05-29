from __future__ import annotations

import math

from hydra_basis.execution_engine.models import ExecutionPreview, ExecutionRequest, ExecutionSignal
from hydra_basis.execution_engine.priority import resolve_execution_legs
from hydra_basis.execution_engine.risk import compute_spread_pct, orderbook_is_anomalous, spread_requires_confirm


def build_execution_preview(
    *,
    request: ExecutionRequest,
    signal: ExecutionSignal,
    priorities: dict[str, int],
    adapters: dict[str, object],
) -> ExecutionPreview:
    short_book = adapters[signal.short_venue].get_orderbook(signal.symbol)
    long_book = adapters[signal.long_venue].get_orderbook(signal.symbol)
    if orderbook_is_anomalous(short_book) or orderbook_is_anomalous(long_book):
        raise RuntimeError("orderbook anomaly detected")

    spreads = {
        signal.short_venue: compute_spread_pct(short_book),
        signal.long_venue: compute_spread_pct(long_book),
    }
    maker_venue, taker_venue = resolve_execution_legs(
        short_venue=signal.short_venue,
        long_venue=signal.long_venue,
        priorities=priorities,
        spreads=spreads,
    )
    return ExecutionPreview(
        symbol=signal.symbol,
        maker_venue=maker_venue,
        taker_venue=taker_venue,
        total_usd=request.total_usd,
        clip_usd=request.clip_usd,
        batch_count=math.ceil(request.total_usd / request.clip_usd),
        maker_spread_pct=spreads[maker_venue],
        taker_spread_pct=spreads[taker_venue],
        requires_confirm=(
            spread_requires_confirm(spreads[maker_venue]) or spread_requires_confirm(spreads[taker_venue])
        ),
    )
