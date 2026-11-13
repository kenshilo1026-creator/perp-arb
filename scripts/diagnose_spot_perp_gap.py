"""Read-only public-book capture; never prepares accounts or submits orders."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import time
from collections import Counter
from decimal import Decimal
from pathlib import Path

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from scripts._bootstrap import ensure_project_root_on_path
ensure_project_root_on_path()

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.binance_spot_adapter import BinanceSpotExecutionAdapter
from hydra_basis.execution_engine.maker_price_guard import MakerPriceGuard, adverse_price_gap


async def capture(symbol, quantity, seconds, output):
    maker = AsterExecutionAdapter(skip_margin_setup=True)
    taker = BinanceSpotExecutionAdapter(api_key="", api_secret="")
    counts = Counter()
    examples = {}
    held = None
    replace_at = 0
    gaps = []
    async def books():
        return await maker.hedge_quote(), await taker.hedge_quote()
    try:
        tick = await maker.get_price_tick_size(symbol)  # public exchangeInfo only
        await asyncio.gather(maker.prepare_hedge_quotes(symbol), taker.prepare_hedge_quotes(symbol))
        guard = MakerPriceGuard(fetch_books=books, tick_size=lambda: maker.get_price_tick_size(symbol),
            maker_side="BUY", taker_side="SELL", quantity=quantity, improve_maker_by_one_tick=True)
        output.parent.mkdir(parents=True, exist_ok=True)
        start = time.monotonic()
        with output.open("w", encoding="utf-8") as log:
            while time.monotonic() - start < seconds:
                now = time.monotonic()
                row = {"utc_ms": int(time.time()*1000), "elapsed_ms": round((now-start)*1000)}
                try:
                    a, b = await books()
                    vwap = guard._depth_price(b)
                    effective = guard.taker_price(b)
                    a_best = a.get("fast_quote", a)
                    b_best = b.get("fast_quote", b)
                    if held is None and now >= replace_at:
                        with contextlib.redirect_stdout(io.StringIO()):
                            held = Decimal(await guard.prepare(str(a["bid"])))
                        counts["hypothetical_submissions"] += 1
                    row.update(aster=a, binance=b, depth_vwap=vwap, guard_taker=effective,
                               latest_aster_bid=a_best["bid"], latest_binance_bid=b_best["bid"],
                               hypothetical_maker_price=held)
                    if held is not None:
                        gap = adverse_price_gap(held, effective, "BUY")
                        top_gap = adverse_price_gap(held, Decimal(str(b_best["bid"])), "BUY")
                        screen_gap = adverse_price_gap(Decimal(str(a_best["bid"])), Decimal(str(b_best["bid"])), "BUY")
                        row.update(guard_gap=gap, latest_bid_gap=top_gap, current_bid_to_bid_gap=screen_gap)
                        gaps.append(float(gap))
                        if gap >= guard.max_gap:
                            counts["hypothetical_gap_cancels"] += 1
                            category = "top_bid_also_exceeds" if top_gap >= guard.max_gap else "depth_or_conservative_price_only"
                            counts[category] += 1
                            examples.setdefault(category, row.copy())
                            if screen_gap < guard.max_gap:
                                counts["current_bid_to_bid_below_limit_at_cancel"] += 1
                                examples.setdefault("current_bid_to_bid_below_limit", row.copy())
                            held = None
                            replace_at = now + 0.3  # explicit simulation assumption, not measured order latency
                    counts["fresh_samples"] += 1
                except Exception as exc:
                    counts["unavailable_samples"] += 1
                    row["error"] = str(exc)
                    held = None
                    replace_at = now + 0.3
                log.write(json.dumps(row, default=str) + "\n")
                await asyncio.sleep(0.05)
        summary = {"symbol": symbol, "quantity": str(quantity), "seconds": seconds, "tick": tick,
                   "simulation": "No orders. Reprice 300ms after a hypothetical cancel; no fill model.",
                   "counts": dict(counts), "examples": examples,
                   "guard_gap_min_pct": min(gaps)*100 if gaps else None,
                   "guard_gap_max_pct": max(gaps)*100 if gaps else None,
                   "capture": str(output.resolve())}
        output.with_suffix(".summary.json").write_text(json.dumps(summary, default=str, indent=2), encoding="utf-8")
        print(json.dumps({k:v for k,v in summary.items() if k != "examples"}, default=str))
    finally:
        await asyncio.gather(maker.close(), taker.close())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="SYN")
    parser.add_argument("--quantity", type=Decimal, default=Decimal("1000"))
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--output", type=Path, default=Path("artifacts/spot_perp_gap.jsonl"))
    args = parser.parse_args()
    if not args.quantity.is_finite() or args.quantity <= 0 or not 0 < args.seconds <= 600:
        parser.error("quantity must be positive and duration must be in (0, 600]")
    asyncio.run(capture(args.symbol.upper(), args.quantity, args.seconds, args.output))
