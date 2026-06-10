from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import websockets


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_variational_symbol(value: object) -> str:
    if isinstance(value, dict):
        value = value.get("underlying") or value.get("symbol") or value.get("ticker") or value.get("market")
    text = str(value or "").strip().upper()
    for suffix in ("-PERP", "_PERP", " PERP", "PERP", "USDT"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def normalize_variational_side(value: object) -> str:
    text = str(value or "").strip().upper()
    if text in {"BUY", "LONG", "BID"}:
        return "BUY"
    if text in {"SELL", "SHORT", "ASK"}:
        return "SELL"
    return text


def first_value(payload: Any, keys: set[str]) -> Any:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key).lower() in keys and value not in (None, ""):
                return value
        for value in payload.values():
            found = first_value(value, keys)
            if found not in (None, ""):
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = first_value(item, keys)
            if found not in (None, ""):
                return found
    return None


def compact_debug_payload(payload: Any, *, limit: int = 2000) -> Any:
    try:
        text = json.dumps(payload, ensure_ascii=False)
    except TypeError:
        text = str(payload)
    if len(text) <= limit:
        return payload
    return {"truncated": True, "text": text[:limit]}


def recursively_contains_filled_status(payload: Any) -> bool:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key).lower() in {"status", "order_status", "state"}:
                if str(value).strip().lower() in {"filled", "fill", "closed", "executed"}:
                    return True
        return any(recursively_contains_filled_status(value) for value in payload.values())
    if isinstance(payload, list):
        return any(recursively_contains_filled_status(item) for item in payload)
    return False


def recursively_contains_trade_type(payload: Any) -> bool:
    """Detects Variational trade events that signal a fill by type field (e.g. type='trade')."""
    if isinstance(payload, dict):
        event_type = str(payload.get("type") or "").strip().lower()
        if event_type and "trade" in event_type:
            return True
        return any(recursively_contains_trade_type(v) for v in payload.values())
    if isinstance(payload, list):
        return any(recursively_contains_trade_type(item) for item in payload)
    return False


def parse_forwarded_ws_payload(payload: dict[str, Any]) -> Any:
    raw = payload.get("payloadData")
    if raw in (None, ""):
        return payload
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return payload


def extract_variational_fill_event(payload: dict[str, Any]) -> dict[str, Any] | None:
    decoded = parse_forwarded_ws_payload(payload)
    if not recursively_contains_filled_status(decoded) and not recursively_contains_trade_type(decoded):
        return None

    symbol = first_value(decoded, {"underlying", "symbol", "ticker", "market", "asset", "instrument"})
    side = first_value(decoded, {"side", "direction", "order_side"})
    order_id = first_value(decoded, {"order_id", "orderid", "id", "client_order_id"})
    filled_base = first_value(decoded, {"filled_base_amount", "filledbaseamount", "base_amount", "size", "amount", "qty"})
    filled_quote = first_value(decoded, {"filled_quote_amount", "filledquoteamount", "quote_amount"})

    return {
        "symbol": normalize_variational_symbol(symbol),
        "side": normalize_variational_side(side),
        "orderId": order_id,
        "filledBaseAmount": str(filled_base) if filled_base is not None else None,
        "filledQuoteAmount": str(filled_quote) if filled_quote is not None else None,
        "raw": decoded,
    }


def variational_fill_matches_order(
    fill: dict[str, Any],
    order: dict[str, Any],
    *,
    allow_side_mismatch: bool = False,
) -> bool:
    fill_symbol = normalize_variational_symbol(fill.get("symbol"))
    order_symbol = normalize_variational_symbol(order.get("symbol") or order.get("market"))
    if fill_symbol and order_symbol and fill_symbol != order_symbol:
        return False

    fill_side = normalize_variational_side(fill.get("side"))
    order_side = normalize_variational_side(order.get("side"))
    if fill_side and order_side and fill_side != order_side and not allow_side_mismatch:
        return False

    return True


class VariationalCommandBroker:
    def __init__(self, *, quiet: bool = False, order_fill_timeout_seconds: float | None = None) -> None:
        self.quiet = quiet
        self.order_fill_timeout_seconds = order_fill_timeout_seconds
        self._roles: dict[Any, str] = {}
        self._extension = None
        self._pending_requests: dict[str, dict[str, Any]] = {}
        self._extension_ready = asyncio.Event()
        self._fill_feed_connections = 0
        self._fill_events_seen = 0
        self._fill_candidates_seen = 0
        self._last_fill_event: dict[str, Any] | None = None
        self._last_fill_candidate: dict[str, Any] | None = None
        self._last_fill_reject_reason: str | None = None
        self._positions: dict[str, dict[str, Any]] = {}
        self._portfolio_received = asyncio.Event()

    async def on_connect(self, websocket) -> None:
        self._roles[websocket] = "unknown"

    async def on_disconnect(self, websocket) -> None:
        role = self._roles.pop(websocket, "unknown")
        if websocket is self._extension:
            self._extension = None
            self._extension_ready.clear()
            failures = list(self._pending_requests.items())
            self._pending_requests.clear()
            for request_id, pending in failures:
                self._cancel_pending_timeout(pending)
                await self._send(
                    pending["requester"],
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
            for request_id, pending in self._pending_requests.items()
            if pending["requester"] is websocket
        ]
        for request_id in stale_request_ids:
            pending = self._pending_requests.pop(request_id, None)
            if pending is not None:
                self._cancel_pending_timeout(pending)
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
        if msg_type == "GET_OPEN_POSITION":
            await self._handle_get_open_position(websocket, payload)
            return
        await self._send_error(websocket, f"Unsupported message type: {msg_type or 'UNKNOWN'}")

    async def handle_fill_event_raw(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return
        try:
            await self.handle_fill_event(payload)
        except Exception as exc:
            if not self.quiet:
                print(f"[VARIATIONAL_BROKER] fill event handling error: {exc!r}", flush=True)

    def _update_positions_from_portfolio(self, positions: list) -> None:
        self._positions.clear()
        for pos in positions:
            info = pos.get("position_info") or pos
            instrument = info.get("instrument") or {}
            symbol = normalize_variational_symbol(instrument)
            if not symbol:
                continue
            try:
                qty = Decimal(str(info.get("qty") or "0"))
            except Exception:
                continue
            if qty == 0:
                continue
            side = "LONG" if qty > 0 else "SHORT"
            self._positions[symbol] = {
                "symbol": symbol,
                "side": side,
                "quantity": format(abs(qty).normalize(), "f"),
                "market_type": "perp",
            }
        if not self._portfolio_received.is_set():
            self._portfolio_received.set()
            if not self.quiet:
                print(f"[VARIATIONAL_BROKER] portfolio received count={len(self._positions)} symbols={list(self._positions.keys())}", flush=True)

    async def wait_for_portfolio(self, *, timeout_seconds: float = 15.0) -> None:
        await asyncio.wait_for(self._portfolio_received.wait(), timeout=timeout_seconds)

    async def handle_fill_event(self, payload: dict[str, Any]) -> None:
        self._fill_events_seen += 1
        self._last_fill_event = compact_debug_payload(payload)
        decoded = parse_forwarded_ws_payload(payload)
        url = payload.get("url", "") if isinstance(payload, dict) else ""
        if isinstance(decoded, dict) and "/portfolio" in str(url):
            self._update_positions_from_portfolio(decoded.get("positions") or [])
            return
        fill = extract_variational_fill_event(payload)
        if fill is None:
            self._last_fill_reject_reason = "not_fill_event"
            return
        self._fill_candidates_seen += 1
        self._last_fill_candidate = compact_debug_payload(fill)
        allow_side_mismatch = len(self._pending_requests) == 1
        for request_id, pending in list(self._pending_requests.items()):
            if not variational_fill_matches_order(
                fill,
                pending["order"],
                allow_side_mismatch=allow_side_mismatch,
            ):
                self._last_fill_reject_reason = (
                    f"no_match pending_symbol={pending['order'].get('symbol')} "
                    f"pending_side={pending['order'].get('side')} "
                    f"fill_symbol={fill.get('symbol')} fill_side={fill.get('side')}"
                )
                continue
            if not pending.get("submitted"):
                pending["earlyFill"] = fill
                symbol = pending.get("order", {}).get("symbol", "?")
                side = pending.get("order", {}).get("side", "?")
                if not self.quiet:
                    print(f"[VARIATIONAL_BROKER] early fill cached {symbol} {side}", flush=True)
                continue
            self._pending_requests.pop(request_id, None)
            self._cancel_pending_timeout(pending)
            symbol = pending.get("order", {}).get("symbol", "?")
            side = pending.get("order", {}).get("side", "?")
            if not self.quiet:
                print(f"[VARIATIONAL_BROKER] fill confirmed {symbol} {side}", flush=True)
            await self._send(
                pending["requester"],
                {
                    "type": "ORDER_RESULT",
                    "requestId": request_id,
                    "ok": True,
                    "filled": True,
                    "status": "FILLED",
                    "orderId": fill.get("orderId") or pending.get("orderId"),
                    "details": {
                        "fill": fill,
                        "submitted": pending.get("submittedResult"),
                    },
                    "timestamp": utc_now(),
                },
            )
            return
        self._last_fill_reject_reason = "no_pending_match"

    async def _handle_get_open_position(self, websocket, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())
        symbol = normalize_variational_symbol(payload.get("symbol") or "")
        position = self._positions.get(symbol)
        if position is None:
            await self._send(websocket, {
                "type": "POSITION_RESULT",
                "requestId": request_id,
                "ok": False,
                "error": f"no open position for {symbol}",
            })
        else:
            await self._send(websocket, {
                "type": "POSITION_RESULT",
                "requestId": request_id,
                "ok": True,
                "position": position,
            })

    async def _handle_register(self, websocket, payload: dict[str, Any]) -> None:
        role = str(payload.get("role", "")).strip().lower() or "unknown"
        self._roles[websocket] = role
        if role == "extension":
            self._extension = websocket
            self._extension_ready.set()
        await self._send(
            websocket,
            {
                "type": "REGISTER_ACK",
                "ok": True,
                "role": role,
                "timestamp": utc_now(),
            },
        )

    async def wait_for_extension(self, *, timeout_seconds: float = 30.0) -> bool:
        if self._extension is not None:
            return True
        await asyncio.wait_for(self._extension_ready.wait(), timeout=timeout_seconds)
        return self._extension is not None

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

        self._pending_requests[request_id] = {
            "requester": websocket,
            "requestId": request_id,
            "order": {
                "symbol": payload.get("symbol") or payload.get("market"),
                "side": side,
                "amount": amount,
                "orderType": payload.get("orderType"),
                "price": payload.get("price"),
                "market": payload.get("market"),
            },
            "submitted": False,
            "orderId": None,
            "timeoutTask": None,
        }
        await self._send(
            self._extension,
            {
                "type": "PLACE_ORDER",
                "requestId": request_id,
                "symbol": payload.get("symbol"),
                "side": side,
                "amount": amount,
                "orderType": payload.get("orderType"),
                "price": payload.get("price"),
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
        pending = self._pending_requests.get(request_id)
        if pending is None:
            return
        if not payload.get("ok", False):
            self._pending_requests.pop(request_id, None)
            self._cancel_pending_timeout(pending)
            await self._send(pending["requester"], payload)
            return
        if payload.get("filled") or str(payload.get("status", "")).upper() == "FILLED":
            self._pending_requests.pop(request_id, None)
            self._cancel_pending_timeout(pending)
            await self._send(pending["requester"], payload)
            return
        pending["submitted"] = True
        pending["orderId"] = payload.get("orderId")
        pending["submittedResult"] = payload
        early_fill = pending.get("earlyFill")
        if isinstance(early_fill, dict):
            self._pending_requests.pop(request_id, None)
            self._cancel_pending_timeout(pending)
            await self._send_filled_order_result(request_id, pending, early_fill)
            return
        if pending.get("timeoutTask") is None and self.order_fill_timeout_seconds is not None:
            pending["timeoutTask"] = asyncio.create_task(self._fail_on_fill_timeout(request_id))

    async def _send_filled_order_result(
        self,
        request_id: str,
        pending: dict[str, Any],
        fill: dict[str, Any],
    ) -> None:
        symbol = pending.get("order", {}).get("symbol", "?")
        side = pending.get("order", {}).get("side", "?")
        if not self.quiet:
            print(f"[VARIATIONAL_BROKER] fill confirmed {symbol} {side}", flush=True)
        await self._send(
            pending["requester"],
            {
                "type": "ORDER_RESULT",
                "requestId": request_id,
                "ok": True,
                "filled": True,
                "status": "FILLED",
                "orderId": fill.get("orderId") or pending.get("orderId"),
                "details": {
                    "fill": fill,
                    "submitted": pending.get("submittedResult"),
                },
                "timestamp": utc_now(),
            },
        )

    async def _fail_on_fill_timeout(self, request_id: str) -> None:
        await asyncio.sleep(self.order_fill_timeout_seconds)
        pending = self._pending_requests.pop(request_id, None)
        if pending is None:
            return
        await self._send(
            pending["requester"],
            {
                "type": "ORDER_RESULT",
                "requestId": request_id,
                "ok": False,
                "error": "Variational order fill timeout after submit.",
                "orderId": pending.get("orderId"),
                "details": {
                    "order": pending.get("order"),
                    "submitted": pending.get("submittedResult"),
                    "fill_diagnostics": self.fill_diagnostics(),
                },
                "timestamp": utc_now(),
            },
        )

    def _cancel_pending_timeout(self, pending: dict[str, Any]) -> None:
        timeout_task = pending.get("timeoutTask")
        if timeout_task is not None:
            timeout_task.cancel()

    def note_fill_feed_connected(self) -> None:
        self._fill_feed_connections += 1
        if not self.quiet:
            print(
                f"[VARIATIONAL_BROKER] fill feed connected count={self._fill_feed_connections}",
                flush=True,
            )

    def fill_diagnostics(self) -> dict[str, Any]:
        return {
            "fill_feed_connections": self._fill_feed_connections,
            "fill_events_seen": self._fill_events_seen,
            "fill_candidates_seen": self._fill_candidates_seen,
            "last_fill_reject_reason": self._last_fill_reject_reason,
            "last_fill_event": self._last_fill_event,
            "last_fill_candidate": self._last_fill_candidate,
        }

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
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8768,
        fill_host: str | None = None,
        fill_port: int | None = None,
        quiet: bool = False,
        order_fill_timeout_seconds: float | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.fill_host = fill_host
        self.fill_port = fill_port
        self.broker = VariationalCommandBroker(
            quiet=quiet,
            order_fill_timeout_seconds=order_fill_timeout_seconds,
        )
        self._server = None
        self._fill_server = None
        self.ws_url = ""
        self.fill_ws_url = ""

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

        if self.fill_host is not None and self.fill_port is not None:
            async def fill_handler(websocket):
                self.broker.note_fill_feed_connected()
                async for message in websocket:
                    try:
                        await self.broker.handle_fill_event_raw(message)
                    except Exception as exc:
                        if not self.broker.quiet:
                            print(f"[VARIATIONAL_BROKER] fill_handler error: {exc!r}", flush=True)

            self._fill_server = await websockets.serve(
                fill_handler,
                self.fill_host,
                self.fill_port,
                max_size=None,
                ping_interval=20,
                ping_timeout=20,
            )
            fill_socket = self._fill_server.sockets[0]
            actual_fill_host, actual_fill_port = fill_socket.getsockname()[:2]
            self.fill_ws_url = f"ws://{actual_fill_host}:{actual_fill_port}"
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._fill_server is not None:
            self._fill_server.close()
            await self._fill_server.wait_closed()
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()

    async def wait_for_extension(self, *, timeout_seconds: float = 30.0) -> bool:
        return await self.broker.wait_for_extension(timeout_seconds=timeout_seconds)

    async def wait_for_portfolio(self, *, timeout_seconds: float = 15.0) -> None:
        await self.broker.wait_for_portfolio(timeout_seconds=timeout_seconds)
