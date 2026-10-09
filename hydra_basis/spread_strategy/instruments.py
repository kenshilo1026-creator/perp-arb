"""Public instrument constraints: price tick, lot size, minimum size and notional."""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from hydra_basis.adapters.base import fetch_json
from hydra_basis.execution_engine.aster_adapter import ASTER_EXECUTION_SUFFIXES
from hydra_basis.execution_engine.hyperliquid_adapter import hyperliquid_price_to_wire
from hydra_basis.spread_strategy.core import ZERO, round_step

HYPERLIQUID_MIN_NOTIONAL = Decimal("10")


@dataclass(frozen=True)
class Instrument:
    venue: str
    tick_size: Decimal | None = None
    lot_size: Decimal | None = None
    min_size: Decimal | None = None
    min_notional: Decimal | None = None
    # Hyperliquid: prices have at most 5 significant figures and (6 - szDecimals) decimals.
    sz_decimals: int | None = None
    # MEXC orders and positions count contracts of this many base units each.
    contract_size: Decimal | None = None

    @property
    def validated(self) -> bool:
        return self.lot_size is not None and self.min_size is not None

    def round_quantity(self, quantity: Decimal) -> Decimal:
        return round_step(quantity, self.lot_size, "down")

    def round_price(self, price: Decimal, direction: str) -> Decimal:
        if self.sz_decimals is not None:
            # Same rule the adapter applies on the wire, so a quote is never re-rounded.
            rounding = ROUND_CEILING if direction == "up" else ROUND_FLOOR
            return Decimal(hyperliquid_price_to_wire(price, sz_decimals=self.sz_decimals, rounding=rounding))
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


def common_lot(instruments) -> Decimal | None:
    """Smallest quantity step valid on every venue (least common multiple of the lots)."""
    lots = [inst.lot_size for inst in instruments if inst.lot_size]
    if not lots:
        return None
    places = max(max(0, -lot.normalize().as_tuple().exponent) for lot in lots)
    scale = Decimal(10) ** places
    value = 1
    for lot in lots:
        value = math.lcm(value, int(lot * scale))
    return Decimal(value) / scale


async def fetch_instrument(session, venue: str, symbol: str) -> Instrument:
    if venue == "aster":
        return await _aster(session, symbol)
    if venue == "hyperliquid":
        return await _hyperliquid(session, symbol)
    if venue == "entropy":
        return await _entropy(session, symbol)
    if venue == "arcus":
        return await _arcus(session, symbol)
    if venue == "ondo":
        return await _ondo(session, symbol)
    if venue == "lighter":
        return await _lighter(session, symbol)
    if venue == "mexc":
        return await _mexc(session, symbol)
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


async def _entropy(session, symbol: str) -> Instrument:
    """Entropy = Hyperliquid HIP-3 dex "io": Hyperliquid's size/price rules and 10 USD minimum."""
    meta, ctxs = await fetch_json(session, "POST", "https://api.hyperliquid.xyz/info",
                                  json={"type": "metaAndAssetCtxs", "dex": "io"})
    coin = f"IO:{symbol.upper()}"
    for row, ctx in zip(meta.get("universe", []), ctxs):
        if str(row.get("name", "")).upper() != coin:
            continue
        if row.get("isDelisted") or ctx.get("midPx") is None:
            raise RuntimeError(f"symbol not trading on entropy: {symbol}")
        if row.get("growthMode") != "enabled":
            # Configured entropy fees assume growth mode (0.1x); without it fees are 10x higher.
            raise RuntimeError(f"entropy {symbol} is not in growth mode; fees would be 10x the configured rate")
        sz_decimals = int(row["szDecimals"])
        lot = Decimal(1).scaleb(-sz_decimals)
        return Instrument("entropy", lot_size=lot, min_size=lot, min_notional=HYPERLIQUID_MIN_NOTIONAL,
                          sz_decimals=sz_decimals)
    raise RuntimeError(f"symbol not found on entropy: {symbol}")


async def _arcus(session, symbol: str) -> Instrument:
    from hydra_basis.adapters.arcus import arcus_base_url, arcus_market_name
    from hydra_basis.execution_engine.arcus_adapter import tick_for_price
    data = await fetch_json(session, "GET", f"{arcus_base_url()}/v1/markets",
                            params={"market": arcus_market_name(symbol)})
    market = next(iter(data.get("markets") or []), None)
    if market is None or market.get("status") != "ONLINE":
        raise RuntimeError(f"symbol not tradable on arcus: {symbol}")
    reference = Decimal(str(market.get("oraclePrice") or market.get("markPrice") or "0"))
    return Instrument(
        "arcus",
        # Ticks widen with price (tickTiers); use the increment at today's price level.
        tick_size=tick_for_price(market, reference) if reference > 0 else Decimal(str(market["tickSize"])),
        lot_size=Decimal(str(market["stepSize"])),
        min_size=Decimal(str(market.get("minOrderSize") or market["stepSize"])),
        min_notional=Decimal(str(market.get("minOrderNotional") or "0")) or None,
    )


async def _ondo(session, symbol: str) -> Instrument:
    from hydra_basis.adapters.ondo import fetch_ondo_contracts, fetch_ondo_markets, ondo_market_name
    name = ondo_market_name(symbol)
    market = next((row for row in await fetch_ondo_markets(session) if row.get("market") == name), None)
    contract = next((row for row in await fetch_ondo_contracts(session) if row.get("market") == name), None)
    if market is None or contract is None or contract.get("disabled"):
        raise RuntimeError(f"symbol not tradable on ondo: {symbol}")
    lot = Decimal(str(market["baseIncrement"]))
    return Instrument("ondo", tick_size=Decimal(str(market["quoteIncrement"])), lot_size=lot, min_size=lot)


async def _lighter(session, symbol: str) -> Instrument:
    data = await fetch_json(session, "GET", "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails")
    row = next((item for item in data.get("order_book_details", [])
                if str(item.get("symbol", "")).upper() == symbol.upper()), None)
    if row is None or str(row.get("status", "active")).lower() != "active":
        raise RuntimeError(f"symbol not tradable on lighter: {symbol}")
    return Instrument(
        "lighter",
        tick_size=Decimal(1).scaleb(-int(row["supported_price_decimals"])),
        lot_size=Decimal(1).scaleb(-int(row["supported_size_decimals"])),
        min_size=Decimal(str(row.get("min_base_amount") or "0")),
        min_notional=Decimal(str(row.get("min_quote_amount") or "0")) or None,
    )


async def _mexc(session, symbol: str) -> Instrument:
    from hydra_basis.adapters.mexc import mexc_contract_symbol
    contract = mexc_contract_symbol(symbol)
    data = await fetch_json(session, "GET", "https://contract.mexc.com/api/v1/contract/detail",
                            params={"symbol": contract})
    row = data.get("data") or {}
    if isinstance(row, list):
        row = row[0] if row else {}
    if row.get("symbol") != contract or row.get("state") != 0 or not row.get("apiAllowed", True):
        raise RuntimeError(f"symbol not tradable via mexc api: {symbol}")
    size = Decimal(str(row["contractSize"]))
    return Instrument(
        "mexc",
        tick_size=Decimal(str(row["priceUnit"])),
        lot_size=Decimal(str(row.get("volUnit") or 1)) * size,
        min_size=Decimal(str(row.get("minVol") or 1)) * size,
        contract_size=size,
    )
