from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from hydra_basis.execution_engine import apy_service, order_service

STATIC_DIR = Path(__file__).parent / "static"
ALLOWED_HOSTS = {"127.0.0.1", "localhost", "testserver"}


def _open_params_from_body(body: dict) -> order_service.OpenParams:
    long_venue = "mexc" if body["kind"] == "spot_perp" else body["long_venue"]
    return order_service.OpenParams(
        kind=body["kind"],
        symbol=str(body["symbol"]).upper(),
        short_venue=body["short_venue"],
        long_venue=long_venue,
        exec_mode=body["exec_mode"],
        leverage=int(body["leverage"]),
        total_size=Decimal(str(body["total_size"])),
        clip_size=Decimal(str(body["clip_size"])),
        interval_ms=int(body["interval_ms"]),
        debounce_ms=int(body["debounce_ms"]),
    )


def create_app() -> FastAPI:
    app = FastAPI(title="Order UI")

    @app.middleware("http")
    async def _localhost_only(request: Request, call_next):
        # Defense-in-depth against DNS-rebinding on top of binding 127.0.0.1.
        host = (request.headers.get("host") or "").split(":")[0]
        if host and host not in ALLOWED_HOSTS:
            return JSONResponse(status_code=403, content={"error": "forbidden host"})
        return await call_next(request)

    @app.get("/api/venues")
    def venues() -> dict:
        return {"venues": apy_service.list_venues()}

    @app.get("/api/symbols")
    def symbols() -> dict:
        return {"symbols": apy_service.list_symbols()}

    @app.get("/api/defaults")
    def defaults(symbol: str, kind: str) -> dict:
        return apy_service.get_default_venues(symbol, kind)

    @app.get("/api/apy")
    def apy(symbol: str, days: int, kind: str, short_venue: str, long_venue: str = "") -> dict:
        raw_points = apy_service.load_points()
        points = {(venue, sym.upper()): pts for (venue, sym), pts in raw_points.items()}
        sym = symbol.upper()
        if kind == "spot_perp":
            venue_points = points.get((short_venue, sym), [])
            result = apy_service.compute_spot_perp_apy(venue_points, days=days)
            return {"stats": result["stats"], "series": result["rate_series"], "days": days}
        short_points = points.get((short_venue, sym), [])
        long_points = points.get((long_venue, sym), [])
        result = apy_service.compute_pair_apy(short_points, long_points, days=days)
        return {"stats": result["stats"], "series": result["spread_series"], "days": days}

    @app.post("/api/preview")
    async def preview(request: Request):
        body = await request.json()
        if body.get("mode") == "close":
            # Close scans live positions on submit; preview is informational only.
            return {
                "kind": body["kind"],
                "mode": "close",
                "symbol": str(body["symbol"]).upper(),
                "note": "平倉將於送出時掃描實倉並平掉唯一 SHORT+LONG 配對（多配對請用 CLI）。",
            }
        try:
            if body["kind"] == "spot_perp":
                result = await order_service.build_spot_perp_preview(_open_params_from_body(body))
            else:
                result = await order_service.build_open_preview(_open_params_from_body(body))
        except (ValueError, NotImplementedError) as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        return asdict(result)

    @app.websocket("/ws/execute")
    async def ws_execute(websocket: WebSocket):
        await websocket.accept()
        try:
            msg = await websocket.receive_json()
            body = msg["params"]
            live = bool(msg.get("live", False))

            async def on_progress(event: dict) -> None:
                await websocket.send_json(event)

            if body.get("mode") == "close":
                params = order_service.CloseParams(
                    kind=body["kind"], symbol=str(body["symbol"]).upper(),
                    total_size=Decimal(str(body["total_size"])), clip_size=Decimal(str(body["clip_size"])),
                    interval_ms=int(body["interval_ms"]), debounce_ms=int(body["debounce_ms"]))
                result = await order_service.execute_close(params, live=live, on_progress=on_progress)
            elif body["kind"] == "spot_perp":
                result = await order_service.execute_spot_perp_open(
                    _open_params_from_body(body), live=live, on_progress=on_progress)
            else:
                result = await order_service.execute_open(
                    _open_params_from_body(body), live=live, on_progress=on_progress)
            await websocket.send_json({"type": "result", **result})
        except Exception as exc:  # noqa: BLE001 - surface to UI
            await websocket.send_json({"type": "error", "error": str(exc)})
        finally:
            await websocket.close()

    if STATIC_DIR.exists():
        app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
    return app
