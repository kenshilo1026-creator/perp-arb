from __future__ import annotations

from hydra_basis.funding_engine.models import FundingPoint


async def fetch_aster_funding(session, symbol: str) -> list[FundingPoint]:
    return []


async def fetch_variational_funding(session, symbol: str) -> list[FundingPoint]:
    return []
