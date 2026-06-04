import unittest
import asyncio
import json

import websockets

from scripts.test_single_order import (
    SUPPORTED_VENUES,
    build_variational_adapter,
    confirm_live_order,
    format_cli_error,
    normalize_order_args,
    single_order_method_name,
)
from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer


class SingleOrderScriptTests(unittest.TestCase):
    def test_supported_venues_include_every_execution_venue(self) -> None:
        self.assertEqual(
            SUPPORTED_VENUES,
            {"aster", "hyperliquid", "lighter", "mexc", "variational"},
        )

    def test_normalize_order_args_uppercases_symbol_and_side(self) -> None:
        args = normalize_order_args(
            venue="Aster",
            symbol="btc",
            side="buy",
            quantity="0.1",
            order_type="market",
        )

        self.assertEqual(args["venue"], "aster")
        self.assertEqual(args["symbol"], "BTC")
        self.assertEqual(args["side"], "BUY")
        self.assertEqual(args["quantity"], "0.1")

    def test_normalize_order_args_rejects_bad_side_and_order_type(self) -> None:
        with self.assertRaises(RuntimeError):
            normalize_order_args(
                venue="aster",
                symbol="BTC",
                side="hold",
                quantity="1",
                order_type="market",
            )
        with self.assertRaises(RuntimeError):
            normalize_order_args(
                venue="aster",
                symbol="BTC",
                side="BUY",
                quantity="1",
                order_type="stop",
            )

    def test_live_order_requires_exact_confirmation_phrase(self) -> None:
        self.assertTrue(confirm_live_order("PLACE LIVE ORDER"))
        self.assertFalse(confirm_live_order("y"))
        self.assertFalse(confirm_live_order("place live order"))

    def test_single_order_method_name(self) -> None:
        self.assertEqual(single_order_method_name("market"), "place_market_order")
        self.assertEqual(single_order_method_name("limit"), "place_limit_order")

    def test_format_cli_error(self) -> None:
        self.assertIn("下單失敗", format_cli_error(RuntimeError("aster quantity 0.0001 is below minimum stepSize=0.001")))

    def test_hyperliquid_adapter_imports_with_installed_eth_account_version(self) -> None:
        from hydra_basis.execution_engine.hyperliquid_adapter import encode_hyperliquid_typed_data

        self.assertTrue(callable(encode_hyperliquid_typed_data))

    def test_hyperliquid_wire_price_uses_max_five_significant_figures(self) -> None:
        from hydra_basis.execution_engine.hyperliquid_adapter import hyperliquid_float_to_wire

        self.assertEqual(hyperliquid_float_to_wire(1782.61875), "1782.6")
        self.assertEqual(hyperliquid_float_to_wire(0.012345678), "0.012346")

    def test_build_variational_adapter_uses_embedded_broker_url(self) -> None:
        adapter = build_variational_adapter("ws://127.0.0.1:9999")

        self.assertEqual(adapter.broker_url, "ws://127.0.0.1:9999")


class VariationalBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def test_broker_relays_place_order_to_extension_and_result_to_strategy(self) -> None:
        async with VariationalCommandBrokerServer(host="127.0.0.1", port=0, quiet=True) as server:
            extension_messages: list[dict] = []

            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                extension_ack = json.loads(await extension.recv())
                self.assertTrue(extension_ack["ok"])

                async with websockets.connect(server.ws_url) as strategy:
                    await strategy.send(json.dumps({"type": "REGISTER", "role": "strategy"}))
                    strategy_ack = json.loads(await strategy.recv())
                    self.assertTrue(strategy_ack["ok"])

                    await strategy.send(
                        json.dumps(
                            {
                                "type": "PLACE_ORDER",
                                "requestId": "req-1",
                                "side": "BUY",
                                "amount": "1",
                                "market": "ETH-PERP",
                            }
                        )
                    )

                    forwarded = json.loads(await extension.recv())
                    extension_messages.append(forwarded)
                    self.assertEqual(forwarded["type"], "PLACE_ORDER")
                    self.assertEqual(forwarded["requestId"], "req-1")
                    self.assertEqual(forwarded["market"], "ETH-PERP")

                    dispatched = json.loads(await strategy.recv())
                    self.assertEqual(dispatched["type"], "ORDER_DISPATCHED")

                    await extension.send(
                        json.dumps(
                            {
                                "type": "ORDER_RESULT",
                                "requestId": "req-1",
                                "ok": True,
                                "orderId": "var-1",
                            }
                        )
                    )
                    result = json.loads(await strategy.recv())

            self.assertEqual(result["orderId"], "var-1")


if __name__ == "__main__":
    unittest.main()
