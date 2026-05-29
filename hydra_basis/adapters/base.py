from __future__ import annotations

from typing import Any

import aiohttp


async def fetch_json(session: aiohttp.ClientSession, method: str, url: str, **kwargs: Any) -> Any:
    async with session.request(method, url, timeout=15, **kwargs) as response:
        response.raise_for_status()
        return await response.json()
