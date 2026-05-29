from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class ExecutionAdapter(Protocol):
    def get_orderbook(self, symbol: str) -> dict[str, float | int]:
        ...


@dataclass
class FakeExecutionAdapter:
    venue: str
    orderbook: dict[str, float | int]

    def get_orderbook(self, symbol: str) -> dict[str, float | int]:
        return dict(self.orderbook)
