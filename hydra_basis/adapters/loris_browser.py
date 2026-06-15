from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
from typing import Any
from urllib.parse import urlencode


LORIS_HOME_URL = "https://loris.tools/"
LORIS_HISTORICAL_URL = "https://api.loris.tools/funding/historical"
DEFAULT_LORIS_NODRIVER_TIMEOUT_SECONDS = 45.0
DEFAULT_LORIS_API_KEY_HEADER = "X-API-Key"
_browser_context_lock: asyncio.Lock | None = None
_shared_browser: Any | None = None
_shared_page: Any | None = None
_shared_loop: asyncio.AbstractEventLoop | None = None
_shared_start_error: BaseException | None = None


def env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def loris_nodriver_enabled() -> bool:
    return env_flag("LORIS_USE_NODRIVER")


def loris_auth_headers() -> dict[str, str]:
    api_key = os.getenv("LORIS_API_KEY", "").strip()
    if not api_key:
        return {}

    header_name = os.getenv("LORIS_API_KEY_HEADER", DEFAULT_LORIS_API_KEY_HEADER).strip()
    if not header_name:
        header_name = DEFAULT_LORIS_API_KEY_HEADER
    if header_name.lower() == "authorization" and not api_key.lower().startswith("bearer "):
        return {header_name: f"Bearer {api_key}"}
    return {header_name: api_key}


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _browser_lock() -> asyncio.Lock:
    global _browser_context_lock
    if _browser_context_lock is None:
        _browser_context_lock = asyncio.Lock()
    return _browser_context_lock


async def _evaluate_json(page: Any, expression: str) -> Any:
    evaluate = getattr(page, "evaluate")
    attempts = (
        {"await_promise": True, "return_by_value": True},
        {"await_promise": True},
        {},
    )
    last_type_error: TypeError | None = None
    for kwargs in attempts:
        try:
            return await _maybe_await(evaluate(expression, **kwargs))
        except TypeError as exc:
            last_type_error = exc
    if last_type_error is not None:
        raise last_type_error
    raise RuntimeError("nodriver evaluate failed")


async def _read_json_document(page: Any) -> dict:
    body_text = await _evaluate_json(
        page,
        """
            JSON.stringify({
                bodyText: document.body ? document.body.innerText : "",
                preText: document.querySelector("pre") ? document.querySelector("pre").innerText : ""
            })
        """,
    )
    if not isinstance(body_text, str):
        body_text = json.dumps(body_text)
    payload = json.loads(body_text)
    text = str(payload.get("preText") or payload.get("bodyText") or "").strip()
    if not text:
        raise RuntimeError("nodriver document body was empty")
    return json.loads(text)


async def _stop_shared_browser() -> None:
    global _shared_browser, _shared_page, _shared_loop, _shared_start_error
    browser = _shared_browser
    _shared_browser = None
    _shared_page = None
    _shared_loop = None
    _shared_start_error = None
    if browser is not None:
        await _maybe_await(browser.stop())


async def _navigate_page(*, browser: Any, page: Any, url: str) -> Any:
    page_get = getattr(page, "get", None)
    if page_get is not None:
        try:
            navigated = await _maybe_await(page_get(url))
            return navigated or page
        except Exception:
            pass
    return await browser.get(url)


async def _acquire_browser_context(uc: Any, *, headless: bool, user_data_dir: str | None) -> tuple[Any, Any]:
    global _shared_browser, _shared_page, _shared_loop, _shared_start_error

    current_loop = asyncio.get_running_loop()
    async with _browser_lock():
        if _shared_start_error is not None:
            raise RuntimeError(
                f"previous loris nodriver browser start failed: {_shared_start_error}"
            ) from _shared_start_error

        if _shared_browser is not None and _shared_loop is not current_loop:
            await _stop_shared_browser()

        if _shared_browser is None or _shared_page is None:
            start_kwargs: dict[str, Any] = {"headless": headless}
            if user_data_dir is not None:
                start_kwargs["user_data_dir"] = user_data_dir
            try:
                _shared_browser = await uc.start(**start_kwargs)
            except Exception as exc:
                _shared_start_error = exc
                raise
            _shared_page = await _shared_browser.get(LORIS_HOME_URL)
            _shared_loop = current_loop
        return _shared_browser, _shared_page


async def fetch_loris_historical_with_nodriver(
    *,
    symbol: str,
    start: str,
    end: str,
) -> dict:
    timeout_seconds = float(
        os.getenv("LORIS_NODRIVER_TIMEOUT_SECONDS", str(DEFAULT_LORIS_NODRIVER_TIMEOUT_SECONDS))
    )
    if _should_run_nodriver_in_proactor_thread():
        return await asyncio.wait_for(
            asyncio.to_thread(
                _fetch_loris_historical_with_nodriver_sync,
                symbol=symbol,
                start=start,
                end=end,
            ),
            timeout=timeout_seconds,
        )
    return await asyncio.wait_for(
        _fetch_loris_historical_with_nodriver_inner(symbol=symbol, start=start, end=end),
        timeout=timeout_seconds,
    )


def _should_run_nodriver_in_proactor_thread() -> bool:
    if sys.platform != "win32":
        return False
    proactor_loop = getattr(asyncio, "ProactorEventLoop", None)
    if proactor_loop is None:
        return False
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        return False
    return not isinstance(running_loop, proactor_loop)


def _fetch_loris_historical_with_nodriver_sync(
    *,
    symbol: str,
    start: str,
    end: str,
) -> dict:
    proactor_loop = getattr(asyncio, "ProactorEventLoop", None)
    if proactor_loop is None:
        raise RuntimeError("nodriver requires ProactorEventLoop on Windows")

    loop = proactor_loop()
    try:
        return loop.run_until_complete(
            _fetch_loris_historical_with_nodriver_inner(symbol=symbol, start=start, end=end)
        )
    finally:
        loop.close()


async def _fetch_loris_historical_with_nodriver_inner(
    *,
    symbol: str,
    start: str,
    end: str,
) -> dict:
    try:
        import nodriver as uc
    except ImportError as exc:
        raise RuntimeError(
            "LORIS_USE_NODRIVER is enabled but nodriver is not installed. "
            "Install requirements or run: pip install nodriver"
        ) from exc

    params = urlencode({"symbol": symbol.upper(), "start": start, "end": end})
    url = f"{LORIS_HISTORICAL_URL}?{params}"
    headless = env_flag("LORIS_NODRIVER_HEADLESS", default=False)
    user_data_dir = os.getenv("LORIS_NODRIVER_USER_DATA_DIR", "").strip() or None
    browser, page = await _acquire_browser_context(
        uc,
        headless=headless,
        user_data_dir=user_data_dir,
    )
    async with _browser_lock():
        try:
            api_page = await _navigate_page(browser=browser, page=page, url=url)
            return await _read_json_document(api_page)
        except Exception as exc:
            raise RuntimeError(f"loris nodriver navigation fetch failed for {symbol.upper()}: {exc}") from exc
