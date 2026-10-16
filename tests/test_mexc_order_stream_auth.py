import hashlib
import hmac
import unittest
from types import SimpleNamespace
from urllib.parse import urlencode
from unittest.mock import Mock

from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.execution_engine.order_stream import OrderStreamError, format_stream_error


class Response:
    def __init__(self, status=200, payload=None):
        self.status = status
        self.payload = payload if payload is not None else {"listenKey": "test-listen-key"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def json(self, **kwargs):
        return self.payload


class MexcStreamAuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_renew_and_delete_all_send_valid_signature(self):
        for method in ("POST", "PUT", "DELETE"):
            with self.subTest(method=method):
                adapter = MexcSpotExecutionAdapter(api_key="test-api-key", api_secret="test-secret")
                adapter._listen_key = None if method == "POST" else "test-listen-key"
                adapter._timestamp_ms = lambda: 1234567890000
                request = Mock(return_value=Response())
                adapter._session = SimpleNamespace(closed=False, request=request)
                result = await adapter._user_stream_request(method)
                self.assertEqual(result["listenKey"], "test-listen-key")
                args, kwargs = request.call_args
                self.assertEqual(args, (method, "https://api.mexc.com/api/v3/userDataStream"))
                params = dict(kwargs["params"])
                actual_signature = params.pop("signature")
                self.assertEqual(params["timestamp"], 1234567890000)
                self.assertEqual(params["recvWindow"], 60000)
                self.assertEqual(kwargs["headers"]["X-MEXC-APIKEY"], "test-api-key")
                if method != "POST":
                    self.assertEqual(params["listenKey"], "test-listen-key")
                else:
                    self.assertNotIn("listenKey", params)
                expected = hmac.new(b"test-secret", urlencode(params).encode(), hashlib.sha256).hexdigest()
                self.assertEqual(actual_signature, expected)

    async def test_reports_actual_missing_signature_error(self):
        adapter = MexcSpotExecutionAdapter(api_key="test-api-key", api_secret="test-secret")
        adapter._session = SimpleNamespace(closed=False, request=Mock(return_value=Response(400, {
            "code": 700004, "msg": "Mandatory parameter 'signature' was not sent, was empty/null, or malformed."})))
        with self.assertRaises(OrderStreamError) as error:
            await adapter._user_stream_request("POST")
        message = format_stream_error(error.exception)
        self.assertIn("listen-key POST", message)
        self.assertIn("HTTP 400", message)
        self.assertIn("code=700004", message)
        self.assertIn("Mandatory parameter 'signature'", message)

    async def test_error_redacts_credentials_and_listen_key(self):
        adapter = MexcSpotExecutionAdapter(api_key="short-key", api_secret="short-secret")
        adapter._listen_key = "short-listen"
        adapter._session = SimpleNamespace(closed=False, request=Mock(return_value=Response(401, {
            "code": 700002, "msg": "bad short-key short-secret short-listen signature=abcdef "
                "wss://host/ws?listenKey=another-token"})))
        with self.assertRaises(OrderStreamError) as error:
            await adapter._user_stream_request("PUT")
        message = format_stream_error(error.exception)
        self.assertIn("code=700002", message)
        for secret in ("short-key", "short-secret", "short-listen", "abcdef", "another-token"):
            self.assertNotIn(secret, message)

    async def test_successful_http_with_api_error_still_fails(self):
        adapter = MexcSpotExecutionAdapter(api_key="key", api_secret="secret")
        adapter._session = SimpleNamespace(closed=False, request=Mock(return_value=Response(200, {
            "code": 700004, "msg": "missing signature"})))
        with self.assertRaises(OrderStreamError):
            await adapter._user_stream_request("POST")

    def test_unknown_exception_never_logs_authenticated_url(self):
        message = format_stream_error(RuntimeError("request wss://host/ws?listenKey=secret"))
        self.assertEqual(message, "RuntimeError")


if __name__ == "__main__":
    unittest.main()
