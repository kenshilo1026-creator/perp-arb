from __future__ import annotations

from typing import Any

import aiohttp


class BaseStreamClient:
    async def connect(self) -> None:
        raise NotImplementedError

    async def subscribe(self) -> None:
        raise NotImplementedError

    async def receive(self) -> Any:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
