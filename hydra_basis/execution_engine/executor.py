from __future__ import annotations

import asyncio
from decimal import Decimal


def execution_sides_for_signal(*, maker_venue: str, short_venue: str, long_venue: str) -> tuple[str, str]:
    maker_side = "SELL" if maker_venue == short_venue else "BUY"
    taker_side = "BUY" if maker_venue == short_venue else "SELL"
    return maker_side, taker_side


async def execute_single_clip(
    *,
    symbol: str,
    clip_usd: float,
    quantity: Decimal,
    maker_venue: str,
    taker_venue: str,
    short_venue: str,
    long_venue: str,
    maker_adapter,
    taker_adapter,
    max_hedge_retries: int,
    state_machine,
) -> dict[str, object]:
    maker_side, taker_side = execution_sides_for_signal(
        maker_venue=maker_venue,
        short_venue=short_venue,
        long_venue=long_venue,
    )

    state_machine.to_preview_ready()
    state_machine.to_awaiting_confirm()
    state_machine.to_placing_maker_leg()
    maker_result = await maker_adapter.place_limit_order(
        symbol=symbol,
        side=maker_side,
        amount=str(quantity),
        clip_usd=clip_usd,
    )
    if not maker_result.get("ok", False):
        raise RuntimeError(f"maker order failed on {maker_venue}")

    state_machine.to_hedging_taker_leg()
    last_error: Exception | None = None
    for attempt in range(max_hedge_retries + 1):
        try:
            hedge_result = await taker_adapter.place_market_order(
                symbol=symbol,
                side=taker_side,
                amount=str(quantity),
                clip_usd=clip_usd,
            )
            state_machine.to_completed()
            return {
                "ok": True,
                "maker_result": maker_result,
                "hedge_result": hedge_result,
            }
        except Exception as exc:
            last_error = exc
            if attempt >= max_hedge_retries:
                state_machine.to_emergency_exit()
                raise RuntimeError(f"hedge failed on {taker_venue}: {exc}") from exc
            state_machine.to_retrying_hedge()
            await asyncio.sleep(0)
            state_machine.to_hedging_taker_leg()

    state_machine.to_emergency_exit()
    raise RuntimeError(f"hedge failed on {taker_venue}: {last_error}")
