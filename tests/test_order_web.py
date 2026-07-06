import unittest
from unittest import mock

from fastapi.testclient import TestClient

from hydra_basis.execution_engine.order_service import Preview


class VenuesSymbolsTests(unittest.TestCase):
    def test_venues_and_symbols(self):
        with mock.patch("hydra_basis.execution_engine.apy_service.list_venues", return_value=["aster", "hyperliquid"]), \
             mock.patch("hydra_basis.execution_engine.apy_service.list_symbols", return_value=["BTC", "ETH"]):
            from web.app import create_app
            client = TestClient(create_app())
            self.assertEqual(client.get("/api/venues").json(), {"venues": ["aster", "hyperliquid"]})
            self.assertEqual(client.get("/api/symbols").json(), {"symbols": ["BTC", "ETH"]})

    def test_rejects_non_localhost_host_header(self):
        from web.app import create_app
        client = TestClient(create_app())
        r = client.get("/api/venues", headers={"host": "evil.example.com"})
        self.assertEqual(r.status_code, 403)


class DefaultsApyTests(unittest.TestCase):
    def test_defaults_passthrough(self):
        with mock.patch("hydra_basis.execution_engine.apy_service.get_default_venues",
                        return_value={"short_venue": "aster", "long_venue": "hyperliquid", "annualized": 0.42}):
            from web.app import create_app
            client = TestClient(create_app())
            data = client.get("/api/defaults", params={"symbol": "BTC", "kind": "perp_perp"}).json()
            self.assertEqual(data["short_venue"], "aster")

    def test_apy_perp_perp(self):
        pts = {("aster", "BTC"): ["s"], ("hyperliquid", "BTC"): ["l"]}
        with mock.patch("hydra_basis.execution_engine.apy_service.load_points", return_value=pts), \
             mock.patch("hydra_basis.execution_engine.apy_service.compute_pair_apy",
                        return_value={"days": 7, "stats": {"annualized_avg": 0.4},
                                      "spread_series": [{"ts_ms": 1, "spread_hourly": 0.001}]}):
            from web.app import create_app
            client = TestClient(create_app())
            data = client.get("/api/apy", params={"symbol": "BTC", "short_venue": "aster",
                                                  "long_venue": "hyperliquid", "days": 7, "kind": "perp_perp"}).json()
            self.assertEqual(data["days"], 7)
            self.assertEqual(len(data["series"]), 1)


class PreviewTests(unittest.TestCase):
    def test_open_perp_perp_preview(self):
        preview = Preview(symbol="BTC", kind="perp_perp", maker_venue="aster", taker_venue="hyperliquid",
                          short_venue="aster", long_venue="hyperliquid", total_usd=5000.0, clip_usd=1000.0,
                          batch_count=5, maker_spread_pct=0.0004, taker_spread_pct=0.0006,
                          requires_confirm=False, exec_mode="maker_taker")
        with mock.patch("hydra_basis.execution_engine.order_service.build_open_preview",
                        new=mock.AsyncMock(return_value=preview)):
            from web.app import create_app
            client = TestClient(create_app())
            body = {"kind": "perp_perp", "mode": "open", "exec_mode": "maker_taker", "symbol": "BTC",
                    "short_venue": "aster", "long_venue": "hyperliquid", "leverage": 3,
                    "total_size": "0.5", "clip_size": "0.1", "interval_ms": 500, "debounce_ms": 300}
            data = client.post("/api/preview", json=body).json()
            self.assertEqual(data["maker_venue"], "aster")
            self.assertEqual(data["batch_count"], 5)

    def test_validation_error_returns_400(self):
        from web.app import create_app
        client = TestClient(create_app())
        body = {"kind": "perp_perp", "mode": "open", "exec_mode": "maker_taker", "symbol": "BTC",
                "short_venue": "aster", "long_venue": "aster", "leverage": 3,
                "total_size": "0.5", "clip_size": "0.1", "interval_ms": 500, "debounce_ms": 300}
        r = client.post("/api/preview", json=body)
        self.assertEqual(r.status_code, 400)

    def test_close_preview_returns_info_note(self):
        from web.app import create_app
        client = TestClient(create_app())
        body = {"kind": "perp_perp", "mode": "close", "exec_mode": "maker_taker", "symbol": "BTC",
                "short_venue": "aster", "long_venue": "hyperliquid", "leverage": 1,
                "total_size": "0.5", "clip_size": "0.1", "interval_ms": 0, "debounce_ms": 0}
        data = client.post("/api/preview", json=body).json()
        self.assertEqual(data["mode"], "close")
        self.assertIn("note", data)


class WsExecuteTests(unittest.TestCase):
    def test_streams_progress_then_result(self):
        async def fake_execute(params, *, live, on_progress, deps=None):
            await on_progress({"type": "batch_start", "index": 1, "total": 1, "clip_size": "0.1"})
            await on_progress({"type": "batch_filled", "index": 1, "maker_price": "100", "taker_price": "100.1", "qty": "0.1"})
            await on_progress({"type": "done", "executed_qty": "0.1", "batches": 1})
            return {"ok": True, "batches": 1, "executed_qty": "0.1"}
        with mock.patch("hydra_basis.execution_engine.order_service.execute_open", new=fake_execute):
            from web.app import create_app
            client = TestClient(create_app())
            with client.websocket_connect("/ws/execute") as ws:
                ws.send_json({"live": False, "params": {
                    "kind": "perp_perp", "mode": "open", "exec_mode": "maker_taker", "symbol": "BTC",
                    "short_venue": "aster", "long_venue": "hyperliquid", "leverage": 3,
                    "total_size": "0.1", "clip_size": "0.1", "interval_ms": 0, "debounce_ms": 0}})
                types = []
                while True:
                    msg = ws.receive_json()
                    types.append(msg["type"])
                    if msg["type"] in {"result", "error"}:
                        break
        self.assertIn("batch_filled", types)
        self.assertEqual(types[-1], "result")

    def test_close_streams_pair_and_result(self):
        async def fake_close(params, *, live, on_progress, deps=None):
            await on_progress({"type": "close_pair", "short_venue": "aster", "long_venue": "hyperliquid",
                               "short_qty": "0.1", "long_qty": "0.1"})
            await on_progress({"type": "done", "executed_qty": "0.1", "batches": 1})
            return {"ok": True, "batches": 1, "executed_qty": "0.1"}
        with mock.patch("hydra_basis.execution_engine.order_service.execute_close", new=fake_close):
            from web.app import create_app
            client = TestClient(create_app())
            with client.websocket_connect("/ws/execute") as ws:
                ws.send_json({"live": False, "params": {
                    "kind": "perp_perp", "mode": "close", "exec_mode": "maker_taker", "symbol": "BTC",
                    "short_venue": "aster", "long_venue": "hyperliquid", "leverage": 1,
                    "total_size": "0.1", "clip_size": "0.1", "interval_ms": 0, "debounce_ms": 0}})
                types = []
                while True:
                    msg = ws.receive_json()
                    types.append(msg["type"])
                    if msg["type"] in {"result", "error"}:
                        break
        self.assertIn("close_pair", types)
        self.assertEqual(types[-1], "result")


class LauncherImportTests(unittest.TestCase):
    def test_launcher_builds_app(self):
        import scripts.run_order_ui as launcher
        app = launcher.build_app()
        self.assertTrue(hasattr(app, "router"))


if __name__ == "__main__":
    unittest.main()
