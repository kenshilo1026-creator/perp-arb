from __future__ import annotations

import aiohttp


_DEFAULT_BASE_URL = "http://127.0.0.1:8769"
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=5.0)


def _normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    s = s.removesuffix("-PERP").removesuffix("PERP").removesuffix("USDT")
    return s


class VariationalMonitorPositionAdapter:
    """Read-only position adapter that queries the variational-v1 status HTTP server.

    The variational-v1 main.py exposes GET /positions at STATUS_HTTP_PORT (default 8769)
    which returns the live portfolio captured from the Variational portfolio WebSocket stream.
    """

    def __init__(self, base_url: str = _DEFAULT_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")

    async def get_open_position(self, *, symbol: str, market_type: str) -> dict | None:
        normalized = _normalize_symbol(symbol)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/positions",
                timeout=_REQUEST_TIMEOUT,
            ) as resp:
                if resp.status != 200:
                    raise RuntimeError(
                        f"variational monitor status server returned {resp.status}"
                    )
                data = await resp.json(content_type=None)

        positions: dict = data.get("positions") or {}
        pos = positions.get(normalized)
        if pos is None:
            return None

        try:
            qty = float(pos.get("qty") or 0)
        except (TypeError, ValueError):
            return None

        if qty == 0:
            return None

        return {
            "symbol": normalized,
            "market_type": "perp",
            "side": "LONG" if qty > 0 else "SHORT",
            "quantity": str(abs(qty)),
            "venue": "variational",
        }
