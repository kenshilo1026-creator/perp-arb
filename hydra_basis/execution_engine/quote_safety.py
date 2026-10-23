"""Pure helpers for fresh quotes and explicitly bounded hedge orders."""
from __future__ import annotations

import inspect
import time
from decimal import Decimal


RECEIVED_MONOTONIC_NS = "received_monotonic_ns"
SOURCE_TIMESTAMP_MS = "ts_ms"
_MIN_PLAUSIBLE_EPOCH_MS = Decimal("946684800000")  # 2000-01-01
_MAX_PLAUSIBLE_EPOCH_MS = Decimal("4102444800000")  # 2100-01-01


def validate_hedge_limits(*, max_hedge_slippage_bps, max_quote_age_ms, max_maker_reprices):
    if any(x is None for x in (max_hedge_slippage_bps, max_quote_age_ms, max_maker_reprices)):
        raise ValueError("live execution requires explicit max_hedge_slippage_bps, max_quote_age_ms, and max_maker_reprices")
    bps, age = Decimal(str(max_hedge_slippage_bps)), Decimal(str(max_quote_age_ms))
    if (not bps.is_finite() or not 0 <= bps < 10_000 or not age.is_finite() or age <= 0
            or isinstance(max_maker_reprices, bool) or not isinstance(max_maker_reprices, int)
            or max_maker_reprices < 0):
        raise ValueError("invalid explicit hedge safety limits")
    return dict(max_hedge_slippage_bps=bps, max_quote_age_ms=age,
                max_maker_reprices=max_maker_reprices)


def stamp_quote(book: dict, *, received_ns: int | None = None) -> dict:
    """Attach a local monotonic receive time without trusting venue timestamps."""
    return {**book, RECEIVED_MONOTONIC_NS: int(received_ns or time.monotonic_ns())}


def quote_age_ms(book: dict, *, now_ns: int | None = None) -> Decimal:
    received = book.get(RECEIVED_MONOTONIC_NS)
    if received in (None, ""):
        # Streaming snapshots may expose an already-computed monotonic age.
        # It is safe only if it is finite and non-negative.
        stream_age = Decimal(str(book.get("age_ms", "NaN")))
        if stream_age.is_finite() and stream_age >= 0:
            return stream_age
        raise RuntimeError("quote missing local monotonic receive timestamp")
    age_ns = int(now_ns or time.monotonic_ns()) - int(received)
    if age_ns < 0:
        raise RuntimeError("quote monotonic timestamp is in the future")
    return Decimal(age_ns) / Decimal(1_000_000)


def source_quote_age_ms(book: dict, *, now_epoch_ms: int | Decimal | None = None) -> Decimal | None:
    """Return venue-event age when the snapshot carries a real epoch timestamp.

    Sequence numbers and absent timestamps must be omitted by fetchers rather
    than treated as clocks.  A present but implausible value fails closed.
    """
    raw = book.get(SOURCE_TIMESTAMP_MS)
    if raw in (None, "", 0, "0"):
        return None
    source_ms = Decimal(str(raw))
    if not source_ms.is_finite() or not _MIN_PLAUSIBLE_EPOCH_MS <= source_ms <= _MAX_PLAUSIBLE_EPOCH_MS:
        raise RuntimeError(f"invalid venue source timestamp: {raw}")
    current_ms = Decimal(str(now_epoch_ms if now_epoch_ms is not None else time.time_ns() // 1_000_000))
    return current_ms - source_ms


def validate_quote_age(
    book: dict,
    *,
    label: str,
    max_age_ms: Decimal | float | str,
    now_ns: int | None = None,
    now_epoch_ms: int | Decimal | None = None,
) -> dict[str, str | None]:
    limit = Decimal(str(max_age_ms))
    if not limit.is_finite() or limit <= 0:
        raise RuntimeError("max quote age must be an explicit positive number of milliseconds")
    local_age = quote_age_ms(book, now_ns=now_ns)
    source_age = source_quote_age_ms(book, now_epoch_ms=now_epoch_ms)
    if local_age > limit or (source_age is not None and (source_age > limit or source_age < -limit)):
        raise RuntimeError(
            f"stale executable quote: {label}_local_age_ms={local_age:.3f} "
            f"{label}_source_age_ms={source_age if source_age is not None else 'unavailable'} "
            f"max_age_ms={limit}"
        )
    return {
        f"{label}_age_ms": format(local_age, "f"),
        f"{label}_source_age_ms": format(source_age, "f") if source_age is not None else None,
    }


def validate_quote_pair_age(
    maker_book: dict,
    taker_book: dict,
    *,
    max_age_ms: Decimal | float | str,
    now_ns: int | None = None,
    now_epoch_ms: int | Decimal | None = None,
) -> dict[str, str | None]:
    limit = Decimal(str(max_age_ms))
    if not limit.is_finite() or limit <= 0:
        raise RuntimeError("max quote age must be an explicit positive number of milliseconds")
    now = int(now_ns or time.monotonic_ns())
    summary = {
        **validate_quote_age(
            maker_book, label="maker", max_age_ms=limit, now_ns=now,
            now_epoch_ms=now_epoch_ms,
        ),
        **validate_quote_age(
            taker_book, label="taker", max_age_ms=limit, now_ns=now,
            now_epoch_ms=now_epoch_ms,
        ),
        "max_age_ms": format(limit, "f"),
    }
    return summary


def executable_price(book: dict, *, side: str, quantity: Decimal) -> Decimal:
    """Return full-quantity VWAP when depth exists, otherwise a size-aware quote.

    A top-of-book-only snapshot is not accepted because it cannot prove that the
    requested hedge quantity is executable. Venues with an RFQ/size-tier quote
    may mark the snapshot ``size_aware=True``.
    """
    normalized = side.strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise RuntimeError(f"unsupported hedge side: {side}")
    if not quantity.is_finite() or quantity <= 0:
        raise RuntimeError("hedge quantity must be positive")
    levels = book.get("asks" if normalized == "BUY" else "bids")
    if levels:
        remaining = quantity
        quote = Decimal("0")
        for level in levels:
            if isinstance(level, dict):
                raw_price = level.get("price", level.get("px"))
                raw_size = level.get("size", level.get("sz", level.get("qty")))
            else:
                raw_price, raw_size = level[0], level[1]
            price, size = Decimal(str(raw_price)), Decimal(str(raw_size))
            if not price.is_finite() or not size.is_finite() or price <= 0 or size < 0:
                raise RuntimeError("invalid executable depth level")
            taken = min(remaining, size)
            quote += taken * price
            remaining -= taken
            if remaining == 0:
                return quote / quantity
        raise RuntimeError(
            f"insufficient executable hedge depth: requested={quantity} available={quantity - remaining}"
        )
    if book.get("size_aware") is True:
        price = Decimal(str(book["ask" if normalized == "BUY" else "bid"]))
        if price.is_finite() and price > 0:
            return price
    raise RuntimeError("full-quantity hedge depth or an explicit size-aware quote is required")


def bounded_hedge_limit(
    executable: Decimal,
    *,
    side: str,
    max_slippage_bps: Decimal | float | str,
) -> Decimal:
    bps = Decimal(str(max_slippage_bps))
    if not executable.is_finite() or executable <= 0:
        raise RuntimeError("invalid executable hedge price")
    if not bps.is_finite() or not 0 <= bps < 10_000:
        raise RuntimeError("max hedge slippage must be an explicit non-negative bps value")
    fraction = bps / Decimal(10_000)
    normalized = side.strip().upper()
    if normalized == "BUY":
        return executable * (Decimal("1") + fraction)
    if normalized == "SELL":
        return executable * (Decimal("1") - fraction)
    raise RuntimeError(f"unsupported hedge side: {side}")


def cap_hedge_to_maker(limit_price, *, maker_price, maker_side: str,
                       max_gap=Decimal("0.002")) -> Decimal:
    """Intersect the original slippage cap with the maker-denominated gap cap."""
    limit, maker, gap = map(lambda x: Decimal(str(x)), (limit_price, maker_price, max_gap))
    if not all(x.is_finite() for x in (limit, maker, gap)) or min(limit, maker) <= 0 or not 0 <= gap < 1:
        raise ValueError("invalid maker/hedge price cap")
    if maker_side.upper() == "SELL":
        return min(limit, maker * (1 + gap))
    if maker_side.upper() == "BUY":
        return max(limit, maker * (1 - gap))
    raise ValueError("invalid maker side")


def executable_worst_price(book: dict, *, side: str, quantity: Decimal) -> Decimal:
    """Worst level required for the full clip; VWAP alone cannot prove IOC coverage."""
    executable_price(book, side=side, quantity=quantity)  # validate depth and quantity
    levels = book.get("asks" if side.upper() == "BUY" else "bids")
    if not levels:
        return executable_price(book, side=side, quantity=quantity)
    remaining, prices = quantity, []
    for level in levels:
        if isinstance(level, dict):
            price = level.get("price", level.get("px"))
            size = level.get("size", level.get("sz", level.get("qty")))
        else:
            price, size = level[:2]
        price, size = Decimal(str(price)), Decimal(str(size))
        if size > 0:
            prices.append(price)
        remaining -= min(remaining, size)
        if remaining == 0:
            return max(prices) if side.upper() == "BUY" else min(prices)
    raise RuntimeError("insufficient hedge depth")


def assert_hedge_limit_still_marketable(
    executable: Decimal,
    *,
    side: str,
    limit_price: Decimal | float | str,
) -> None:
    limit = Decimal(str(limit_price))
    normalized = side.strip().upper()
    if normalized == "BUY" and executable <= limit:
        return
    if normalized == "SELL" and executable >= limit:
        return
    if normalized not in {"BUY", "SELL"}:
        raise RuntimeError(f"unsupported hedge side: {side}")
    raise RuntimeError(
        "hedge cap is no longer marketable; refusing maker replacement: "
        f"side={normalized} executable={executable} limit={limit}"
    )


def assert_bounded_hedge_supported(adapter, venue: str) -> None:
    method = getattr(adapter, "place_market_order", None)
    if not callable(method):
        raise RuntimeError(f"bounded hedge unsupported on {venue}: no market-order method")
    parameters = inspect.signature(method).parameters
    if "limit_price" not in parameters:
        raise RuntimeError(
            f"bounded hedge unsupported on {venue}: adapter cannot accept an explicit IOC limit_price"
        )


def assert_post_only_supported(adapter, venue: str) -> None:
    method = getattr(adapter, "place_limit_order", None)
    if not callable(method):
        raise RuntimeError(f"maker unsupported on {venue}: no limit-order method")
    parameters = inspect.signature(method).parameters
    if "post_only" not in parameters:
        raise RuntimeError(
            f"post-only maker unsupported on {venue}: adapter cannot guarantee maker-only placement"
        )
