import unittest
import asyncio
import json
from unittest import mock

import websockets

from scripts.test_single_order import (
    SUPPORTED_VENUES,
    build_variational_adapter,
    confirm_live_order,
    execute_single_order,
    format_cli_error,
    limit_price_from_orderbook,
    load_default_limit_orderbook,
    normalize_order_args,
    single_order_method_name,
)
from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
from hydra_basis.execution_engine.variational_browser import (
    VariationalBrowserExecutionAdapter,
    build_place_order_payload,
)


class SingleOrderScriptTests(unittest.TestCase):
    def test_supported_venues_include_every_execution_venue(self) -> None:
        self.assertEqual(
            SUPPORTED_VENUES,
            {"aster", "hyperliquid", "lighter", "mexc", "mexc_spot", "variational"},
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

    def test_variational_limit_order_can_omit_price_for_mid_button(self) -> None:
        result = asyncio.run(
            execute_single_order(
                venue="variational",
                symbol="GUA",
                side="BUY",
                quantity="10",
                order_type="limit",
                live=False,
            )
        )

        self.assertTrue(result["ok"])
        self.assertIsNone(result["request"]["price"])

    def test_limit_price_from_orderbook_uses_best_bid_for_buy_and_best_ask_for_sell(self) -> None:
        orderbook = {"bid": 100.0, "ask": 100.5, "ts_ms": 1}

        self.assertEqual(limit_price_from_orderbook(orderbook, "BUY"), "100")
        self.assertEqual(limit_price_from_orderbook(orderbook, "SELL"), "100.5")

    def test_non_variational_limit_order_defaults_price_from_adapter_orderbook(self) -> None:
        class FakeAdapter:
            def __init__(self) -> None:
                self.calls = []

            async def get_orderbook(self, symbol):
                return {"bid": 100.0, "ask": 100.5, "ts_ms": 1}

            async def place_limit_order(self, **kwargs):
                self.calls.append(kwargs)
                return {"ok": True}

        adapter = FakeAdapter()
        result = asyncio.run(
            execute_single_order(
                venue="aster",
                symbol="ETH",
                side="BUY",
                quantity="0.003",
                order_type="limit",
                live=True,
                adapter_override=adapter,
            )
        )

        self.assertEqual(result["request"]["price"], "100")
        self.assertEqual(adapter.calls[0]["price"], "100")

    def test_non_variational_limit_order_defaults_price_from_market_data_snapshot(self) -> None:
        async def fake_fetch_orderbook_snapshot(session, *, venue, symbol, clip_usd):
            return {"bid": 200.0, "ask": 201.0, "ts_ms": 1}

        with mock.patch(
            "scripts.test_single_order.fetch_orderbook_snapshot",
            side_effect=fake_fetch_orderbook_snapshot,
        ):
            result = asyncio.run(
                execute_single_order(
                    venue="aster",
                    symbol="ETH",
                    side="SELL",
                    quantity="0.003",
                    order_type="limit",
                    live=False,
                )
            )

        self.assertEqual(result["request"]["price"], "201")

    def test_mexc_spot_limit_price_uses_spot_orderbook_snapshot(self) -> None:
        async def fake_fetch_mexc_spot_orderbook(session, symbol):
            return {"bid": 3000.0, "ask": 3001.0, "ts_ms": 1}

        with mock.patch(
            "scripts.test_single_order.fetch_mexc_spot_orderbook",
            side_effect=fake_fetch_mexc_spot_orderbook,
        ):
            orderbook = asyncio.run(
                load_default_limit_orderbook(
                    venue="mexc_spot",
                    symbol="ETH",
                    clip_usd=0.0,
                )
            )

        self.assertEqual(orderbook["bid"], 3000.0)

    def test_variational_order_payload_includes_symbol_for_page_guard(self) -> None:
        payload = build_place_order_payload(
            request_id="req-1",
            symbol="SOL",
            side="BUY",
            amount="0.1",
            market="SOL-PERP",
        )

        self.assertEqual(payload["symbol"], "SOL")


class VariationalBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def test_broker_waits_for_extension_registration(self) -> None:
        async with VariationalCommandBrokerServer(host="127.0.0.1", port=0, quiet=True) as server:
            wait_task = asyncio.create_task(server.wait_for_extension(timeout_seconds=1.0))
            await asyncio.sleep(0.05)
            self.assertFalse(wait_task.done())

            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                extension_ack = json.loads(await extension.recv())
                self.assertTrue(extension_ack["ok"])

                self.assertTrue(await wait_task)

    async def test_variational_adapter_sends_order_details_to_extension(self) -> None:
        async with VariationalCommandBrokerServer(host="127.0.0.1", port=0, quiet=True) as server:
            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                extension_ack = json.loads(await extension.recv())
                self.assertTrue(extension_ack["ok"])

                adapter = VariationalBrowserExecutionAdapter(broker_url=server.ws_url)
                order_task = asyncio.create_task(
                    adapter.place_limit_order(
                        symbol="ETH",
                        side="BUY",
                        amount="0.003",
                        clip_usd=0.0,
                        price="1600",
                    )
                )

                forwarded = json.loads(await extension.recv())
                self.assertEqual(forwarded["type"], "PLACE_ORDER")
                self.assertEqual(forwarded["symbol"], "ETH")
                self.assertEqual(forwarded["orderType"], "LIMIT")
                self.assertEqual(forwarded["price"], "1600")

                await extension.send(
                    json.dumps(
                            {
                                "type": "ORDER_RESULT",
                                "requestId": forwarded["requestId"],
                                "ok": True,
                                "filled": True,
                                "status": "FILLED",
                                "details": {"symbol": forwarded["symbol"]},
                            }
                    )
                )

                result = await order_task
                self.assertTrue(result["ok"])

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
                                "filled": True,
                                "status": "FILLED",
                                "orderId": "var-1",
                            }
                        )
                    )
                    result = json.loads(await strategy.recv())

            self.assertEqual(result["orderId"], "var-1")

    async def test_broker_waits_for_variational_fill_event_before_strategy_result(self) -> None:
        async with VariationalCommandBrokerServer(
            host="127.0.0.1",
            port=0,
            fill_host="127.0.0.1",
            fill_port=0,
            quiet=True,
            order_fill_timeout_seconds=1.0,
        ) as server:
            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                self.assertTrue(json.loads(await extension.recv())["ok"])

                async with websockets.connect(server.fill_ws_url) as fill_feed:
                    async with websockets.connect(server.ws_url) as strategy:
                        await strategy.send(json.dumps({"type": "REGISTER", "role": "strategy"}))
                        self.assertTrue(json.loads(await strategy.recv())["ok"])

                        await strategy.send(
                            json.dumps(
                                {
                                    "type": "PLACE_ORDER",
                                    "requestId": "req-fill",
                                    "symbol": "ETH",
                                    "side": "BUY",
                                    "amount": "0.003",
                                    "orderType": "LIMIT",
                                    "price": "1600",
                                }
                            )
                        )

                        forwarded = json.loads(await extension.recv())
                        self.assertEqual(forwarded["type"], "PLACE_ORDER")
                        dispatched = json.loads(await strategy.recv())
                        self.assertEqual(dispatched["type"], "ORDER_DISPATCHED")

                        await extension.send(
                            json.dumps(
                                {
                                    "type": "ORDER_RESULT",
                                    "requestId": "req-fill",
                                    "ok": True,
                                    "orderId": "var-123",
                                    "details": {"clickedSubmitText": "Buy ETH"},
                                }
                            )
                        )
                        with self.assertRaises(asyncio.TimeoutError):
                            await asyncio.wait_for(strategy.recv(), timeout=0.05)

                        await fill_feed.send(
                            json.dumps(
                                {
                                    "kind": "ws_frame",
                                    "direction": "received",
                                    "payloadData": json.dumps(
                                        {
                                            "type": "order_update",
                                            "symbol": "ETH",
                                            "side": "BUY",
                                            "status": "filled",
                                            "order_id": "var-123",
                                            "filled_base_amount": "0.003",
                                            "filled_quote_amount": "4.8",
                                        }
                                    ),
                                }
                            )
                        )

                        result = json.loads(await strategy.recv())

            self.assertTrue(result["ok"])
            self.assertTrue(result["filled"])
            self.assertEqual(result["status"], "FILLED")
            self.assertEqual(result["orderId"], "var-123")

    async def test_broker_returns_failure_when_variational_fill_event_times_out(self) -> None:
        async with VariationalCommandBrokerServer(
            host="127.0.0.1",
            port=0,
            fill_host="127.0.0.1",
            fill_port=0,
            quiet=True,
            order_fill_timeout_seconds=0.05,
        ) as server:
            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                self.assertTrue(json.loads(await extension.recv())["ok"])

                async with websockets.connect(server.ws_url) as strategy:
                    await strategy.send(json.dumps({"type": "REGISTER", "role": "strategy"}))
                    self.assertTrue(json.loads(await strategy.recv())["ok"])

                    await strategy.send(
                        json.dumps(
                            {
                                "type": "PLACE_ORDER",
                                "requestId": "req-timeout",
                                "symbol": "ETH",
                                "side": "SELL",
                                "amount": "0.003",
                                "orderType": "LIMIT",
                            }
                        )
                    )
                    forwarded = json.loads(await extension.recv())
                    self.assertEqual(forwarded["type"], "PLACE_ORDER")
                    self.assertEqual(json.loads(await strategy.recv())["type"], "ORDER_DISPATCHED")
                    await extension.send(
                        json.dumps(
                            {
                                "type": "ORDER_RESULT",
                                "requestId": "req-timeout",
                                "ok": True,
                                "orderId": "var-timeout",
                            }
                        )
                    )
                    result = json.loads(await strategy.recv())

            self.assertFalse(result["ok"])
            self.assertIn("fill timeout", result["error"])

    async def test_broker_handles_variational_fill_event_before_submit_ack(self) -> None:
        async with VariationalCommandBrokerServer(
            host="127.0.0.1",
            port=0,
            fill_host="127.0.0.1",
            fill_port=0,
            quiet=True,
            order_fill_timeout_seconds=1.0,
        ) as server:
            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                self.assertTrue(json.loads(await extension.recv())["ok"])

                async with websockets.connect(server.fill_ws_url) as fill_feed:
                    async with websockets.connect(server.ws_url) as strategy:
                        await strategy.send(json.dumps({"type": "REGISTER", "role": "strategy"}))
                        self.assertTrue(json.loads(await strategy.recv())["ok"])

                        await strategy.send(
                            json.dumps(
                                {
                                    "type": "PLACE_ORDER",
                                    "requestId": "req-early-fill",
                                    "symbol": "H",
                                    "side": "SELL",
                                    "amount": "10",
                                    "orderType": "LIMIT",
                                }
                            )
                        )

                        forwarded = json.loads(await extension.recv())
                        self.assertEqual(forwarded["type"], "PLACE_ORDER")
                        self.assertEqual(json.loads(await strategy.recv())["type"], "ORDER_DISPATCHED")

                        await fill_feed.send(
                            json.dumps(
                                {
                                    "kind": "ws_frame",
                                    "direction": "received",
                                    "payloadData": json.dumps(
                                        {
                                            "type": "trade",
                                            "symbol": "H",
                                            "side": "SELL",
                                            "status": "filled",
                                            "order_id": "var-early",
                                            "filled_base_amount": "10",
                                        }
                                    ),
                                }
                            )
                        )
                        with self.assertRaises(asyncio.TimeoutError):
                            await asyncio.wait_for(strategy.recv(), timeout=0.05)

                        await extension.send(
                            json.dumps(
                                {
                                    "type": "ORDER_RESULT",
                                    "requestId": "req-early-fill",
                                    "ok": True,
                                    "orderId": "var-early",
                                }
                            )
                        )
                        result = json.loads(await strategy.recv())

            self.assertTrue(result["ok"])
            self.assertTrue(result["filled"])
            self.assertEqual(result["orderId"], "var-early")

    async def test_broker_accepts_single_pending_fill_even_when_variational_side_differs(self) -> None:
        async with VariationalCommandBrokerServer(
            host="127.0.0.1",
            port=0,
            fill_host="127.0.0.1",
            fill_port=0,
            quiet=True,
            order_fill_timeout_seconds=1.0,
        ) as server:
            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                self.assertTrue(json.loads(await extension.recv())["ok"])

                async with websockets.connect(server.fill_ws_url) as fill_feed:
                    async with websockets.connect(server.ws_url) as strategy:
                        await strategy.send(json.dumps({"type": "REGISTER", "role": "strategy"}))
                        self.assertTrue(json.loads(await strategy.recv())["ok"])

                        await strategy.send(
                            json.dumps(
                                {
                                    "type": "PLACE_ORDER",
                                    "requestId": "req-side-diff",
                                    "symbol": "BEAT",
                                    "side": "BUY",
                                    "amount": "10",
                                    "orderType": "LIMIT",
                                }
                            )
                        )
                        forwarded = json.loads(await extension.recv())
                        self.assertEqual(forwarded["type"], "PLACE_ORDER")
                        self.assertEqual(json.loads(await strategy.recv())["type"], "ORDER_DISPATCHED")

                        await extension.send(
                            json.dumps(
                                {
                                    "type": "ORDER_RESULT",
                                    "requestId": "req-side-diff",
                                    "ok": True,
                                    "orderId": None,
                                }
                            )
                        )
                        await fill_feed.send(
                            json.dumps(
                                {
                                    "kind": "ws_frame",
                                    "direction": "received",
                                    "payloadData": json.dumps(
                                        {
                                            "type": "trade",
                                            "symbol": "BEAT",
                                            "side": "SELL",
                                            "status": "filled",
                                            "filled_base_amount": "10",
                                        }
                                    ),
                                }
                            )
                        )
                        result = json.loads(await strategy.recv())

            self.assertTrue(result["ok"])
            self.assertTrue(result["filled"])
            self.assertEqual(result["status"], "FILLED")

    async def test_broker_extracts_underlying_from_variational_instrument_trade_event(self) -> None:
        async with VariationalCommandBrokerServer(
            host="127.0.0.1",
            port=0,
            fill_host="127.0.0.1",
            fill_port=0,
            quiet=True,
            order_fill_timeout_seconds=1.0,
        ) as server:
            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                self.assertTrue(json.loads(await extension.recv())["ok"])

                async with websockets.connect(server.fill_ws_url) as fill_feed:
                    async with websockets.connect(server.ws_url) as strategy:
                        await strategy.send(json.dumps({"type": "REGISTER", "role": "strategy"}))
                        self.assertTrue(json.loads(await strategy.recv())["ok"])

                        await strategy.send(
                            json.dumps(
                                {
                                    "type": "PLACE_ORDER",
                                    "requestId": "req-instrument",
                                    "symbol": "BEAT",
                                    "side": "BUY",
                                    "amount": "10",
                                    "orderType": "LIMIT",
                                }
                            )
                        )
                        forwarded = json.loads(await extension.recv())
                        self.assertEqual(forwarded["type"], "PLACE_ORDER")
                        self.assertEqual(json.loads(await strategy.recv())["type"], "ORDER_DISPATCHED")
                        await extension.send(
                            json.dumps(
                                {
                                    "type": "ORDER_RESULT",
                                    "requestId": "req-instrument",
                                    "ok": True,
                                    "orderId": None,
                                }
                            )
                        )
                        await fill_feed.send(
                            json.dumps(
                                {
                                    "kind": "ws_frame",
                                    "direction": "received",
                                    "payloadData": json.dumps(
                                        {
                                            "timestamp": "2026-06-08T09:21:52.512Z",
                                            "type": "trade",
                                            "data": {
                                                "id": "90cf813a-5910-4680-bfa8-5edf1f50fe38",
                                                "side": "buy",
                                                "instrument": {
                                                    "instrument_type": "perpetual_future",
                                                    "underlying": "BEAT",
                                                    "settlement_asset": "USDC",
                                                },
                                                "price": "4.31746",
                                                "qty": "10",
                                                "role": "taker",
                                                "trade_type": "trade",
                                                "status": "confirmed",
                                            },
                                        }
                                    ),
                                }
                            )
                        )
                        result = json.loads(await strategy.recv())

            self.assertTrue(result["ok"])
            self.assertTrue(result["filled"])
            self.assertEqual(result["details"]["fill"]["symbol"], "BEAT")
            self.assertEqual(result["orderId"], "90cf813a-5910-4680-bfa8-5edf1f50fe38")

    async def test_broker_timeout_includes_fill_diagnostics(self) -> None:
        async with VariationalCommandBrokerServer(
            host="127.0.0.1",
            port=0,
            fill_host="127.0.0.1",
            fill_port=0,
            quiet=True,
            order_fill_timeout_seconds=0.05,
        ) as server:
            async with websockets.connect(server.ws_url) as extension:
                await extension.send(json.dumps({"type": "REGISTER", "role": "extension"}))
                self.assertTrue(json.loads(await extension.recv())["ok"])

                async with websockets.connect(server.fill_ws_url):
                    async with websockets.connect(server.ws_url) as strategy:
                        await strategy.send(json.dumps({"type": "REGISTER", "role": "strategy"}))
                        self.assertTrue(json.loads(await strategy.recv())["ok"])

                        await strategy.send(
                            json.dumps(
                                {
                                    "type": "PLACE_ORDER",
                                    "requestId": "req-diagnostics",
                                    "symbol": "BEAT",
                                    "side": "BUY",
                                    "amount": "10",
                                    "orderType": "LIMIT",
                                }
                            )
                        )
                        forwarded = json.loads(await extension.recv())
                        self.assertEqual(forwarded["type"], "PLACE_ORDER")
                        self.assertEqual(json.loads(await strategy.recv())["type"], "ORDER_DISPATCHED")
                        await extension.send(
                            json.dumps(
                                {
                                    "type": "ORDER_RESULT",
                                    "requestId": "req-diagnostics",
                                    "ok": True,
                                }
                            )
                        )
                        result = json.loads(await strategy.recv())

            self.assertFalse(result["ok"])
            diagnostics = result["details"]["fill_diagnostics"]
            self.assertEqual(diagnostics["fill_feed_connections"], 1)
            self.assertEqual(diagnostics["fill_events_seen"], 0)


if __name__ == "__main__":
    unittest.main()
