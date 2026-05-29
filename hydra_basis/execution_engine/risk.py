from __future__ import annotations


def compute_spread_pct(orderbook: dict[str, float | int]) -> float:
    bid = float(orderbook["bid"])
    ask = float(orderbook["ask"])
    mid = (bid + ask) / 2.0
    if mid <= 0:
        raise RuntimeError("invalid mid price")
    return (ask - bid) / mid


def spread_requires_confirm(spread_pct: float, threshold: float = 0.001) -> bool:
    return spread_pct > threshold


def orderbook_is_anomalous(orderbook: dict[str, float | int]) -> bool:
    bid = float(orderbook.get("bid", 0.0))
    ask = float(orderbook.get("ask", 0.0))
    return bid <= 0 or ask <= 0 or bid > ask
