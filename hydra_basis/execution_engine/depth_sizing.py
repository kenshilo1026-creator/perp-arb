"""Size a maker clip against complete hedge depth, using exact base units."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from math import lcm

from .maker_price_guard import MAKER_TAKER_MAX_GAP, MakerPriceGuard, adverse_price_gap, bounded_maker_price

D = Decimal


@dataclass(frozen=True)
class SizeRules:
    step: D
    minimum: D
    maximum: D
    min_notional: D
    max_notional: D


def combined_size_rules(*filter_sets):
    steps, minima, maxima, notionals, max_notionals = [], [], [], [D("5")], []
    for filters in filter_sets:
        if "LOT_SIZE" not in filters:
            raise ValueError("automatic sizing requires exchange LOT_SIZE metadata")
        for name in ("LOT_SIZE", "MARKET_LOT_SIZE"):
            f = filters.get(name, {})
            for key, dest in (("stepSize", steps), ("minQty", minima), ("maxQty", maxima)):
                n = D(str(f.get(key, "0")))
                if not n.is_finite() or n < 0:
                    raise ValueError("invalid exchange quantity filter")
                if n > 0:
                    dest.append(n)
        for name in ("MIN_NOTIONAL", "NOTIONAL"):
            f = filters.get(name, {})
            for raw, dest in ((f.get("minNotional", f.get("notional", "0")), notionals),
                              (f.get("maxNotional", "0"), max_notionals)):
                n = D(str(raw))
                if not n.is_finite() or n < 0:
                    raise ValueError("invalid exchange notional filter")
                if n > 0:
                    dest.append(n)
    if not steps:
        raise ValueError("automatic sizing requires a positive quantity step")
    unit = D(10) ** min(s.as_tuple().exponent for s in steps)
    step = D(lcm(*(int(s / unit) for s in steps))) * unit
    return SizeRules(step, max(minima, default=step), min(maxima, default=D("Infinity")),
                     max(notionals), min(max_notionals, default=D("Infinity")))


def floor_step(value, step):
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def depth_clip(*, remaining, maker_book, taker_book, maker_side, tick, rules,
               improve=False, max_gap=MAKER_TAKER_MAX_GAP):
    """Return (quantity, maker_price, hedge_vwap); zero quantity means wait.

    Reserve a tradable final clip instead of deliberately leaving dust. No
    rounding of the user's total and no trading beyond its remaining amount.
    """
    if not remaining.is_finite() or remaining <= 0 or remaining % rules.step:
        raise ValueError(f"remaining quantity must be a positive multiple of {rules.step}: {remaining}")
    taker_side = "SELL" if maker_side == "BUY" else "BUY"
    probe = MakerPriceGuard(fetch_books=None, tick_size=None, maker_side=maker_side, taker_side=taker_side)
    best = probe.taker_price(taker_book)  # includes quote freshness and faster BBO warnings
    bid, ask = D(str(maker_book["bid"])), D(str(maker_book["ask"]))
    desired = bid + (tick if improve else 0) if maker_side == "BUY" else ask - (tick if improve else 0)
    price = bounded_maker_price(desired=desired, taker=best, bid=bid, ask=ask,
                                side=maker_side, tick=tick, max_gap=max_gap)
    # Conservative bounds apply to both venues and either execution role.
    low = min(price, bid, D(str(taker_book["bid"]))) * (1 - max_gap)
    high = max(price, ask, D(str(taker_book["ask"]))) * (1 + max_gap)
    minimum = (max(rules.minimum, rules.min_notional / low) / rules.step).to_integral_value(rounding=ROUND_CEILING) * rules.step
    if remaining < minimum:
        raise ValueError(f"unexecuted remainder {remaining} is below current exchange minimum {minimum}; no extra quantity submitted")
    maximum = min(remaining, rules.maximum, rules.max_notional / high)
    levels = taker_book.get("bids" if taker_side == "SELL" else "asks")
    if not levels:
        raise ValueError("automatic sizing requires full hedge depth with quantities")
    boundary = price * (1 - max_gap if maker_side == "BUY" else 1 + max_gap)
    qty, budget, previous = D(0), D(0), None
    for raw_p, raw_q in levels:
        p, q = D(str(raw_p)), D(str(raw_q))
        if not p.is_finite() or not q.is_finite() or p <= 0 or q < 0:
            raise ValueError("invalid hedge depth level")
        if previous is not None and (p >= previous if taker_side == "SELL" else p <= previous):
            raise ValueError("hedge depth must be sorted with unique prices")
        previous = p
        edge = p - boundary if taker_side == "SELL" else boundary - p
        take = min(q, maximum - qty)
        if edge < 0:
            take = min(take, budget / -edge)
        qty += take
        budget += take * edge
        if qty >= maximum or take < q:
            break
    qty = floor_step(qty, rules.step)
    if 0 < remaining - qty < minimum:
        qty = floor_step(remaining - minimum, rules.step)
    if qty < minimum:
        return D(0), price, None
    probe.quantity = qty
    vwap = probe.taker_price(taker_book)
    # The guard uses a strict boundary; don't submit a rounded equality.
    if adverse_price_gap(price, vwap, maker_side) >= max_gap:
        qty -= rules.step
        if 0 < remaining - qty < minimum:
            qty = floor_step(remaining - minimum, rules.step)
        if qty < minimum:
            return D(0), price, None
        probe.quantity = qty
        vwap = probe.taker_price(taker_book)
        if adverse_price_gap(price, vwap, maker_side) >= max_gap:
            return D(0), price, None
    return qty, price, vwap
