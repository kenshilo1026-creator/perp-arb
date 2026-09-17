from __future__ import annotations

import unittest

from hydra_basis.execution_engine.lighter_adapter import LighterExecutionAdapter


class _FakeSignerClient:
    ORDER_TYPE_LIMIT = "limit"
    ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = "gtt"
    ISOLATED_MARGIN_MODE = 1

    def __init__(self, *, margin_error=None) -> None:
        self.margin_error = margin_error
        self.margin_calls: list[dict] = []
        self.order_calls: list[dict] = []

    async def update_leverage(self, **kwargs):
        self.margin_calls.append(kwargs)
        return None, None, self.margin_error

    async def create_order(self, **kwargs):
        self.order_calls.append(kwargs)
        return None, "tx-hash", None


def _adapter(client: _FakeSignerClient, *, leverage: int = 3) -> LighterExecutionAdapter:
    return LighterExecutionAdapter(
        signer_client_factory=lambda: client,
        market_config_loader=lambda symbol: {
            "market_index": 215,
            "base_amount_multiplier": 100,
            "price_multiplier": 10_000,
            "min_base_amount": None,
            "min_quote_amount": None,
            "market_margin_mode": 1,
        },
        orderbook_loader=lambda symbol: {"bid": 2.0, "ask": 2.1, "ts_ms": 1},
        leverage=leverage,
    )


class LighterMarginSetupTests(unittest.IsolatedAsyncioTestCase):
    async def test_open_order_sets_isolated_leverage_once_before_orders(self) -> None:
        client = _FakeSignerClient()
        adapter = _adapter(client)

        await adapter.place_limit_order(
            symbol="FOLKS", side="SELL", amount="5", clip_usd=10.0, price="2.1"
        )
        await adapter.place_limit_order(
            symbol="FOLKS", side="SELL", amount="5", clip_usd=10.0, price="2.1"
        )

        self.assertEqual(
            client.margin_calls,
            [{"market_index": 215, "margin_mode": 1, "leverage": 3}],
        )
        self.assertEqual(len(client.order_calls), 2)

    async def test_margin_setup_failure_prevents_order_submission(self) -> None:
        client = _FakeSignerClient(margin_error="invalid margin mode")
        adapter = _adapter(client)

        with self.assertRaisesRegex(RuntimeError, "update_leverage failed"):
            await adapter.place_market_order(
                symbol="FOLKS", side="SELL", amount="5", clip_usd=10.0
            )

        self.assertEqual(client.order_calls, [])

    async def test_reduce_only_close_does_not_change_margin_mode(self) -> None:
        client = _FakeSignerClient()
        adapter = _adapter(client)

        await adapter.close_position(
            venue="lighter",
            symbol="FOLKS",
            side="BUY",
            quantity="5",
            market_type="perp",
        )

        self.assertEqual(client.margin_calls, [])
        self.assertTrue(client.order_calls[0]["reduce_only"])


if __name__ == "__main__":
    unittest.main()
