from __future__ import annotations

import concurrent.futures
import unittest
from unittest.mock import patch

from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter


class AsterNonceTests(unittest.TestCase):
    def setUp(self) -> None:
        AsterExecutionAdapter._last_nonce_us_by_account.clear()

    @staticmethod
    def _adapter(*, signer: str = "0xsigner", user: str = "0xuser") -> AsterExecutionAdapter:
        return AsterExecutionAdapter(
            signer_address=signer,
            private_key="unused",
            user_address=user,
        )

    def test_nonce_is_unique_across_recreated_adapters_in_same_microsecond(self) -> None:
        first = self._adapter()
        second = self._adapter()

        with patch("hydra_basis.execution_engine.aster_adapter.time.time_ns", return_value=123_456_789_000):
            first_nonce = first._nonce_us()
            second_nonce = second._nonce_us()

        self.assertEqual(first_nonce, 123_456_789)
        self.assertEqual(second_nonce, first_nonce + 1)

    def test_nonce_generation_is_thread_safe_across_adapters(self) -> None:
        adapters = [self._adapter() for _ in range(8)]

        with patch("hydra_basis.execution_engine.aster_adapter.time.time_ns", return_value=999_000):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                nonces = list(pool.map(lambda adapter: adapter._nonce_us(), adapters * 25))

        self.assertEqual(len(nonces), 200)
        self.assertEqual(len(set(nonces)), 200)
        self.assertEqual(max(nonces) - min(nonces), 199)

    def test_nonce_sequence_is_independent_per_signer_user(self) -> None:
        first_account = self._adapter(signer="0xA", user="0x1")
        second_account = self._adapter(signer="0xB", user="0x2")

        with patch("hydra_basis.execution_engine.aster_adapter.time.time_ns", return_value=42_000):
            self.assertEqual(first_account._nonce_us(), 42)
            self.assertEqual(second_account._nonce_us(), 42)


class AsterNonceRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_post_action_resigns_after_nonce_used_rejection(self) -> None:
        signed_nonces: list[str] = []

        class Adapter(AsterExecutionAdapter):
            def __init__(self) -> None:
                super().__init__(
                    signer_address="0xsigner",
                    private_key="unused",
                    user_address="0xuser",
                )
                self.next_nonce = 100

            def build_signed_params(self, params: dict) -> dict:
                self.next_nonce += 1
                return {**params, "nonce": str(self.next_nonce), "signature": "sig"}

            async def _post_signed_query(self, url: str, params: dict) -> dict:
                signed_nonces.append(params["nonce"])
                if len(signed_nonces) == 1:
                    raise RuntimeError("aster order 400: {'code': -4226, 'msg': 'Nonce used'}")
                return {"orderId": 7}

        result = await Adapter()._post_signed_action(
            "https://aster.test/order",
            {"symbol": "GRVTUSDT", "side": "SELL"},
        )

        self.assertEqual(result["orderId"], 7)
        self.assertEqual(signed_nonces, ["101", "102"])

    async def test_post_action_does_not_retry_other_rejections(self) -> None:
        calls = 0

        class Adapter(AsterExecutionAdapter):
            def build_signed_params(self, params: dict) -> dict:
                return params

            async def _post_signed_query(self, url: str, params: dict) -> dict:
                nonlocal calls
                calls += 1
                raise RuntimeError("aster order 400: insufficient margin")

        adapter = Adapter(
            signer_address="0xsigner",
            private_key="unused",
            user_address="0xuser",
        )
        with self.assertRaisesRegex(RuntimeError, "insufficient margin"):
            await adapter._post_signed_action("https://aster.test/order", {})

        self.assertEqual(calls, 1)


if __name__ == "__main__":
    unittest.main()
