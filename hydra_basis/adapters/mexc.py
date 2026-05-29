from __future__ import annotations

from hydra_basis.adapters.base import fetch_json
from hydra_basis.config import FUNDING_INTERVAL_OVERRIDES, LOOKBACK_DAYS, SYMBOL_ALIASES, VENUE_CONFIG
from hydra_basis.adapters.hyperliquid import ms_days_ago
from hydra_basis.funding_engine.models import FundingPoint
from hydra_basis.funding_engine.normalization import infer_interval_hours_from_timestamps

MEXC_EMPTY_HISTORY_RETRIES = 2


def mexc_contract_symbol(symbol: str) -> str:
    normalized_symbol = symbol.upper()
    return SYMBOL_ALIASES.get("mexc", {}).get(normalized_symbol, f"{normalized_symbol}_USDT")


async def list_symbols(session) -> set[str]:
    url = "https://contract.mexc.com/api/v1/contract/detail"
    data = await fetch_json(session, "GET", url)
    rows = data.get("data") or []

    symbols: set[str] = set()
    for row in rows:
        if row.get("state") != 0:
            continue
        if not row.get("apiAllowed", True):
            continue
        if str(row.get("quoteCoin") or "").upper() != "USDT":
            continue
        base_coin = str(row.get("baseCoin") or "").upper()
        if base_coin:
            symbols.add(base_coin)
    return symbols


async def resolve_funding_interval_hours(session, venue: str, symbol: str, history_rows: list[dict] | None = None) -> float:
    normalized_symbol = symbol.upper()
    override = FUNDING_INTERVAL_OVERRIDES.get((venue, normalized_symbol))
    if override:
        return float(override)

    if venue == "mexc":
        if history_rows:
            for row in history_rows:
                collect_cycle = row.get("collectCycle")
                if collect_cycle is not None:
                    return float(collect_cycle)

        mexc_symbol = mexc_contract_symbol(normalized_symbol)
        try:
            detail_url = f"https://contract.mexc.com/api/v1/contract/detail?symbol={mexc_symbol}"
            detail = await fetch_json(session, "GET", detail_url)
            payload = detail.get("data") or {}
            if isinstance(payload, list):
                payload = payload[0] if payload else {}

            raw_interval = (
                payload.get("fundingInterval")
                or payload.get("fundingIntervalHours")
                or payload.get("fundingRateInterval")
                or payload.get("fundingRateIntervalHours")
            )
            if raw_interval is not None:
                interval = float(raw_interval)
                if interval > 10_000:
                    return interval / 3_600_000
                if interval > 100:
                    return interval / 3_600
                return interval
        except Exception as exc:
            print(f"interval metadata lookup failed ({venue}, {symbol}): {exc}")

        if history_rows:
            timestamps = []
            for row in history_rows:
                ts = row.get("settleTime") or row.get("time") or row.get("timestamp")
                if ts is not None:
                    timestamps.append(int(ts))
            inferred = infer_interval_hours_from_timestamps(timestamps)
            if inferred:
                return inferred

    raise RuntimeError(f"unable to determine funding interval from api/history for {venue}:{symbol}")


def extract_mexc_history_rows(data: dict) -> list[dict]:
    payload = data.get("data")
    if isinstance(payload, dict):
        rows = payload.get("resultList") or []
        return rows if isinstance(rows, list) else []
    if isinstance(payload, list):
        return payload
    return []


async def fetch_mexc_funding(session, symbol: str) -> list[FundingPoint]:
    mexc_symbol = mexc_contract_symbol(symbol)
    url = "https://contract.mexc.com/api/v1/contract/funding_rate/history"
    params = {"symbol": mexc_symbol, "page_num": 1, "page_size": 1000}
    rows: list[dict] = []
    raw_data: dict = {}
    for _attempt in range(MEXC_EMPTY_HISTORY_RETRIES + 1):
        raw_data = await fetch_json(session, "GET", url, params=params)
        rows = extract_mexc_history_rows(raw_data)
        if rows:
            break
    if not rows:
        print(f"mexc empty history for {mexc_symbol}: {raw_data}")
        await resolve_funding_interval_hours(session, "mexc", symbol, rows)
        return []
    interval_hours = await resolve_funding_interval_hours(session, "mexc", symbol, rows)
    cutoff = ms_days_ago(LOOKBACK_DAYS)
    points: list[FundingPoint] = []

    for row in rows:
        ts_raw = row.get("settleTime") or row.get("time") or row.get("timestamp")
        rate_raw = row.get("fundingRate")
        if rate_raw is None:
            rate_raw = row.get("rate")
        if ts_raw is None or rate_raw is None:
            continue
        ts = int(ts_raw)
        if ts < cutoff:
            continue
        rate = float(rate_raw)
        points.append(FundingPoint("mexc", symbol, ts, rate, interval_hours))
    return points
