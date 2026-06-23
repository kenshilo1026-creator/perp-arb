from __future__ import annotations


class MarketTypeRouterCloser:
    def __init__(self, *, perp=None, spot=None) -> None:
        self.perp = perp
        self.spot = spot

    async def get_open_position(self, **kwargs) -> dict | None:
        market_type = kwargs.get("market_type")
        if market_type == "perp" and self.perp is not None:
            return await self.perp.get_open_position(**kwargs)
        if market_type == "spot" and self.spot is not None:
            return await self.spot.get_open_position(**kwargs)
        raise RuntimeError(f"missing closer for market_type={market_type}")

    async def close_position(self, **kwargs) -> dict:
        market_type = kwargs.get("market_type")
        if market_type == "perp" and self.perp is not None:
            return await self.perp.close_position(**kwargs)
        if market_type == "spot" and self.spot is not None:
            return await self.spot.close_position(**kwargs)
        raise RuntimeError(f"missing closer for market_type={market_type}")

    async def list_open_positions(self) -> list[dict]:
        positions: list[dict] = []
        if self.perp is not None:
            lister = getattr(self.perp, "list_open_positions", None)
            if callable(lister):
                positions.extend(await lister())
        if self.spot is not None:
            lister = getattr(self.spot, "list_open_positions", None)
            if callable(lister):
                positions.extend(await lister())
        return positions
