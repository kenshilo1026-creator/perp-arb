from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import websockets


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class VariationalCommandBroker:
    def __init__(self, *, quiet: bool = False) -> None:
        self.quiet = quiet
        self._roles: dict[Any, str] = {}
        self._extension = None
        self._pending_requests: dict[str, Any] = {}

    async def on_connect(self, websocket) -> None:
        self._roles[websocket] = "unknown"

    async def on_disconnect(self, websocket) -> None:
        role = self._roles.pop(websocket, "unknown")
        if websocket is self._extension:
            self._extension = None
            failures = list(self._pending_requests.items())
            self._pending_requests.clear()
            for request_id, requester in failures:
                await self._send(
                    requester,
                    {
                        "type": "ORDER_RESULT",
                        "requestId": request_id,
                        "ok": False,
                        "error": "Extension disconnected before order result.",
                        "timestamp": utc_now(),
                    },
                )
        stale_request_ids = [
            request_id
            for request_id, requester in self._pending_requests.items()
            if requester is websocket
        ]
        for request_id in stale_request_ids:
            self._pending_requests.pop(request_id, None)
        if not self.quiet:
            print(f"[VARIATIONAL_BROKER] disconnected role={role}", flush=True)

    async def handle_raw_message(self, websocket, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            await self._send_error(websocket, "Invalid JSON payload.")
            return
        if not isinstance(payload, dict):
            await self._send_error(websocket, "Command payload must be an object.")
            return

        msg_type = str(payload.get("type", "")).upper()
        if msg_type == "REGISTER":
            await self._handle_register(websocket, payload)
            return
        if msg_type == "PING":
            await self._send(websocket, {"type": "PONG", "timestamp": utc_now()})
            return
        if msg_type == "PLACE_ORDER":
            await self._handle_place_order(websocket, payload)
            return
        if msg_type == "ORDER_RESULT":
            await self._handle_order_result(payload)
            return
        await self._send_error(websocket, f"Unsupported message type: {msg_type or 'UNKNOWN'}")

    async def _handle_register(self, websocket, payload: dict[str, Any]) -> None:
        role = str(payload.get("role", "")).strip().lower() or "unknown"
        self._roles[websocket] = role
        if role == "extension":
            self._extension = websocket
        await self._send(
            websocket,
            {
                "type": "REGISTER_ACK",
                "ok": True,
                "role": role,
                "timestamp": utc_now(),
            },
        )
        if not self.quiet:
            print(f"[VARIATIONAL_BROKER] registered role={role}", flush=True)

    async def _handle_place_order(self, websocket, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())
        side = str(payload.get("side", "")).upper()
        amount = str(payload.get("amount", "")).strip()
        if side not in {"BUY", "SELL"}:
            await self._send_order_result(websocket, request_id, ok=False, error="Invalid side. Use BUY or SELL.")
            return
        try:
            if float(amount) <= 0:
                raise ValueError
        except ValueError:
            await self._send_order_result(websocket, request_id, ok=False, error="Invalid amount. Must be positive.")
            return
        if self._extension is None:
            await self._send_order_result(
                websocket,
                request_id,
                ok=False,
                error="No extension command client connected.",
            )
            return

        self._pending_requests[request_id] = websocket
        await self._send(
            self._extension,
            {
                "type": "PLACE_ORDER",
                "requestId": request_id,
                "side": side,
                "amount": amount,
                "market": payload.get("market"),
                "account": payload.get("account"),
                "timeoutMs": payload.get("timeoutMs"),
                "timestamp": utc_now(),
            },
        )
        await self._send_order_result(websocket, request_id, ok=True, event_type="ORDER_DISPATCHED")

    async def _handle_order_result(self, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId", "")).strip()
        if not request_id:
            return
        requester = self._pending_requests.pop(request_id, None)
        if requester is not None:
            await self._send(requester, payload)

    async def _send_error(self, websocket, error: str) -> None:
        await self._send(websocket, {"type": "ERROR", "ok": False, "error": error, "timestamp": utc_now()})

    async def _send_order_result(
        self,
        websocket,
        request_id: str,
        *,
        ok: bool,
        error: str | None = None,
        event_type: str = "ORDER_RESULT",
    ) -> None:
        payload: dict[str, Any] = {
            "type": event_type,
            "requestId": request_id,
            "ok": ok,
            "timestamp": utc_now(),
        }
        if error is not None:
            payload["error"] = error
        await self._send(websocket, payload)

    async def _send(self, websocket, payload: dict[str, Any]) -> None:
        await websocket.send(json.dumps(payload, ensure_ascii=True))


class VariationalCommandBrokerServer:
    def __init__(self, *, host: str = "127.0.0.1", port: int = 8768, quiet: bool = False) -> None:
        self.host = host
        self.port = port
        self.broker = VariationalCommandBroker(quiet=quiet)
        self._server = None
        self.ws_url = ""

    async def __aenter__(self) -> "VariationalCommandBrokerServer":
        async def handler(websocket):
            await self.broker.on_connect(websocket)
            try:
                async for message in websocket:
                    await self.broker.handle_raw_message(websocket, message)
            finally:
                await self.broker.on_disconnect(websocket)

        self._server = await websockets.serve(
            handler,
            self.host,
            self.port,
            max_size=None,
            ping_interval=20,
            ping_timeout=20,
        )
        socket = self._server.sockets[0]
        actual_host, actual_port = socket.getsockname()[:2]
        self.ws_url = f"ws://{actual_host}:{actual_port}"
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
