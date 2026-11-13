import unittest
import tempfile
from pathlib import Path
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.order_service import Deps, run_batched_execution
from scripts.run_spot_perp_arbitrage import build_spot_perp_plan, execute_spot_perp_plan


class FilledAdapter:
    def __init__(self, position, *, missing_fill=D(0)):
        self.position = D(position)
        self.missing_fill = missing_fill
        self.orders = []
        self.last_fill = D(0)

    async def get_open_position(self, *, symbol, market_type):
        return {"symbol": symbol, "market_type": market_type,
                "side": "LONG" if self.position > 0 else "SHORT",
                "quantity": str(abs(self.position))} if self.position else None

    async def submit(self, **kwargs):
        self.orders.append(kwargs)
        self.last_fill = D(kwargs["amount"])
        actual = self.last_fill - self.missing_fill
        self.position += actual if kwargs["side"] == "BUY" else -actual
        return await self.get_order_execution()

    place_limit_order = submit
    place_market_order = submit

    async def get_order_execution(self, **kwargs):
        return {"ok": True, "filled": True, "terminal": True, "status": "FILLED",
                "filled_quantity": str(self.last_fill), "avg_price": "0.1209"}

    wait_for_order_fill = get_order_execution


class SpotPerpPositionDeltaTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.enterContext(patch("hydra_basis.execution_engine.execution_trace.TRACE_PATH",
                                Path(directory) / "test_timing.jsonl"))

    async def run_clip(self, maker, spot, *, quantity="1000", mode="close", venue="aster"):
        book = {"bid": 0.1209, "ask": 0.1211}
        plan = build_spot_perp_plan(symbol="SYN", mode=mode, short_venue=venue,
            quantity=D(quantity), clip_usd=None, spot_book=book, perp_book=book)
        with patch("scripts.run_spot_perp_arbitrage.build_spot_perp_adapter",
                   side_effect=lambda v, **kw: spot if v == "binance_spot" else maker), \
             patch("scripts.run_spot_perp_arbitrage.refresh_spot_perp_maker_price",
                   new=AsyncMock(return_value="0.1209")), \
             patch("scripts.run_spot_perp_arbitrage.record_successful_live_legs", return_value="test-only"), \
             patch("asyncio.sleep", new=AsyncMock()):
            return await execute_spot_perp_plan(plan=plan, leverage=1)

    async def test_close_preserves_existing_difference_and_only_trades_requested_clip(self):
        maker, spot = FilledAdapter("-18282"), FilledAdapter("18267.78")
        result = await self.run_clip(maker, spot)
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(result["executed_quantity"], "1000")
        self.assertEqual(maker.position, D("-17282"))
        self.assertEqual(spot.position, D("17267.78"))
        self.assertEqual(maker.position + spot.position, D("-14.22"))
        self.assertEqual([o["amount"] for o in maker.orders], ["1000"])
        self.assertEqual([o["amount"] for o in spot.orders], ["1000"])

    async def test_batches_stop_at_requested_total_without_correcting_old_difference(self):
        maker, spot = FilledAdapter("-18282"), FilledAdapter("18267.78")
        async def batch(q):
            return await self.run_clip(maker, spot, quantity=str(q))
        result = await run_batched_execution(symbol="SYN", total_size=D("2500"), clip_size=D("1000"),
            batch_count=3, clip_usd=121, venues=["aster", "binance_spot"], interval_ms=0, debounce_ms=0,
            run_batch=batch, on_progress=AsyncMock(), deps=Deps())
        self.assertEqual(result["executed_qty"], "2500")
        self.assertEqual([o["amount"] for o in maker.orders], ["1000", "1000", "500"])
        self.assertEqual([o["amount"] for o in spot.orders], ["1000", "1000", "500"])
        self.assertEqual(maker.position + spot.position, D("-14.22"))

    async def test_open_also_preserves_historical_difference(self):
        maker, spot = FilledAdapter("-18282"), FilledAdapter("18267.78")
        result = await self.run_clip(maker, spot, mode="open")
        self.assertTrue(result["hedge_verified"])
        self.assertEqual(maker.position, D("-19282"))
        self.assertEqual(spot.position, D("19267.78"))

    async def test_new_fill_mismatch_still_fails_verification(self):
        maker, spot = FilledAdapter("-18282"), FilledAdapter("18267.78", missing_fill=D(1))
        with self.assertRaisesRegex(RuntimeError, "hedge live position confirmation failed"):
            await self.run_clip(maker, spot)
        self.assertEqual(len(spot.orders), 1)

    async def test_close_cannot_exceed_available_perp_position(self):
        maker, spot = FilledAdapter("-999"), FilledAdapter("18267.78")
        with self.assertRaisesRegex(RuntimeError, "close quantity/direction exceeds live position"):
            await self.run_clip(maker, spot)
        self.assertEqual(maker.orders, [])
        self.assertEqual(spot.orders, [])
