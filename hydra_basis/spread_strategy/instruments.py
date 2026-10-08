"""Public instrument constraints: price tick, lot size, minimum size and notional."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from hydra_basis.adapters.base import fetch_json
from hydra_basis.execution_engine.aster_adapter import ASTER_EXECUTION_SUFFIXES
from hydra_basis.spread_strategy.core import ZERO, round_step

HYPERLIQUID_MIN_NOTIONAL = Decimal("10")
HYPERLIQUID_MAX_PERP_DECIMALS = 6


@dataclass(frozen=True)
class Instrument:
    venue: str
    tick_size: Decimal | None = None
    lot_size: Decimal | None = None
    min_size: Decimal | None = None
    min_notional: Decimal | None = None
    # Hyperliquid: prices have at most 5 significant figures and (6 - szDecimals) decimals.
    sz_decimals: int | None = None

    @property
    def validated(self) -> bool:
        return self.lot_size is not None and self.min_size is not None

    def round_quantity(self, quantity: Decimal) -> Decimal:
        return round_step(quantity, self.lot_size, "down")

    def round_price(self, price: Decimal, direction: str) -> Decimal:
        if self.sz_decimals is not None:
            return _hyperliquid_price(price, self.sz_decimals, direction)
        return round_step(price, self.tick_size, direction)

    def price_tolerance(self, price: Decimal) -> Decimal:
        if self.tick_size:
            return self.tick_size
        return price * Decimal("0.0001")

    def size_error(self, quantity: Decimal, price: Decimal | None) -> str | None:
        """Original orderSizeError: minimum size, lot multiple, minimum notional."""
        if quantity <= ZERO:
            return "below_minimum_size"
        if self.min_size is not None and quantity < self.min_size:
            return f"below_minimum_size {quantity} < {self.min_size}"
        if self.lot_size and quantity % self.lot_size != 0:
            return f"invalid_lot_size {quantity} not a multiple of {self.lot_size}"
        if price is not None and self.min_notional and quantity * price < self.min_notional:
            return f"below_minimum_notional {quantity * price} < {self.min_notional}"
        return None


def _hyperliquid_price(price: Decimal, sz_decimals: int, direction: str) -> Decimal:
    rounding = ROUND_CEILING if direction == "up" else ROUND_FLOOR
    max_decimals = max(0, HYPERLIQUID_MAX_PERP_DECIMALS - sz_decimals)
    value = price.quantize(Decimal(1).scaleb(-max_decimals), rounding=rounding)
    # Integer prices are always valid; otherwise keep at most 5 significant figures.
    if value != value.to_integral_value():
        digits = value.adjusted() + 1
        if digits < 5:
            places = 5 - digits
            value = value.quantize(Decimal(1).scaleb(-min(places, max_decimals)), rounding=rounding)
        else:
            value = value.quantize(Decimal(1), rounding=rounding)
    return value


async def fetch_instrument(session, venue: str, symbol: str) -> Instrument:
    if venue == "aster":
        return await _aster(session, symbol)
    if venue == "hyperliquid":
        return await _hyperliquid(session, symbol)
    # Variational publishes no lot/notional rules; it is used only as a taker leg.
    return Instrument(venue)


async def aster_contract(session, symbol: str) -> dict:
    """The trading contract for a canonical symbol, as the execution adapter resolves it."""
    info = await fetch_json(session, "GET", "https://fapi.asterdex.com/fapi/v1/exchangeInfo")
    contracts = {str(item.get("symbol", "")).upper(): item for item in info.get("symbols", [])}
    for suffix in ASTER_EXECUTION_SUFFIXES:
        contract = contracts.get(f"{symbol.upper()}{suffix}")
        if contract is not None and contract.get("status") == "TRADING":
            return contract
    raise RuntimeError(f"no trading aster contract for {symbol}")


async def _aster(session, symbol: str) -> Instrument:
    raw = await aster_contract(session, symbol)
    filters = {item.get("filterType"): item for item in raw.get("filters", [])}
    lot = filters.get("LOT_SIZE", {})
    notional = filters.get("MIN_NOTIONAL", {})
    def dec(value):
        return Decimal(str(value)) if value not in (None, "") else None
    return Instrument(
        "aster",
        tick_size=dec(filters.get("PRICE_FILTER", {}).get("tickSize")),
        lot_size=dec(lot.get("stepSize")),
        min_size=dec(lot.get("minQty")),
        min_notional=dec(notional.get("notional") or notional.get("minNotional")),
    )


async def _hyperliquid(session, symbol: str) -> Instrument:
    meta = await fetch_json(session, "POST", "https://api.hyperliquid.xyz/info", json={"type": "meta"})
    row = next((item for item in meta.get("universe", [])
                if str(item.get("name", "")).upper() == symbol.upper()), None)
    if row is None or row.get("isDelisted"):
        raise RuntimeError(f"symbol not tradable on hyperliquid: {symbol}")
    sz_decimals = int(row["szDecimals"])
    lot = Decimal(1).scaleb(-sz_decimals)
    return Instrument("hyperliquid", lot_size=lot, min_size=lot,
                      min_notional=HYPERLIQUID_MIN_NOTIONAL, sz_decimals=sz_decimals)
