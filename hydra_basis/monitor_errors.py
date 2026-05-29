from __future__ import annotations


def should_raise_immediately(error: Exception) -> bool:
    message = str(error).lower()
    return (
        "429" in message
        or "too many requests" in message
        or "rate limit" in message
        or "timeout" in message
        or "timed out" in message
    )


def build_exchange_error_message(*, phase: str, venue: str, symbol: str | None, error: Exception) -> str:
    symbol_part = f" symbol={symbol}" if symbol else ""
    return f"{phase} error venue={venue}{symbol_part}: {error!r}"


def raise_exchange_error(*, phase: str, venue: str, symbol: str | None, error: Exception) -> None:
    message = build_exchange_error_message(phase=phase, venue=venue, symbol=symbol, error=error)
    print(message)
    raise RuntimeError(message) from error
