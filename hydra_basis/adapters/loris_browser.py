from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
import threading
import time
from typing import Any
from urllib.parse import urlencode


LORIS_HOME_URL = "https://loris.tools/"
LORIS_HISTORICAL_URL = "https://api.loris.tools/funding/historical"
DEFAULT_LORIS_NODRIVER_TIMEOUT_SECONDS = 45.0
DEFAULT_LORIS_API_KEY_HEADER = "X-API-Key"
# Loris historical endpoints allow ~30 requests/min per session; pace slightly
# under that so backfill bursts never trip a 429.
DEFAULT_LORIS_MIN_REQUEST_INTERVAL_SECONDS = 2.2
_throttle_mutex = threading.Lock()
_next_request_at_monotonic = 0.0
_browser_context_lock: asyncio.Lock | None = None
_shared_browser: Any | None = None
_shared_page: Any | None = None
_shared_loop: asyncio.AbstractEventLoop | None = None
_shared_start_error: BaseException | None = None
_browser_pool_lock: asyncio.Lock | None = None
_browser_pool_semaphore: asyncio.Semaphore | None = None
_browser_pool_loop: asyncio.AbstractEventLoop | None = None
_browser_pool_size: int | None = None
_browser_pool_start_error: BaseException | None = None
_browser_pool: list[dict[str, Any]] = []


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


def _nodriver_pool_size() -> int:
    raw = os.getenv("LORIS_NODRIVER_POOL_SIZE", "1").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def _nodriver_worker_delay_seconds() -> float:
    # Pacing is handled by the global request throttle, so no extra delay by default.
    raw = os.getenv("LORIS_NODRIVER_WORKER_DELAY_SECONDS", "0").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


def _loris_min_request_interval_seconds() -> float:
    raw = os.getenv("LORIS_MIN_REQUEST_INTERVAL_SECONDS", "").strip()
    if not raw:
        return DEFAULT_LORIS_MIN_REQUEST_INTERVAL_SECONDS
    try:
        return max(0.0, float(raw))
    except ValueError:
        return DEFAULT_LORIS_MIN_REQUEST_INTERVAL_SECONDS


async def _throttle_loris_request() -> None:
    """Reserve the next request slot; safe across event loops and threads."""
    global _next_request_at_monotonic
    interval = _loris_min_request_interval_seconds()
    if interval <= 0:
        return
    while True:
        with _throttle_mutex:
            now = time.monotonic()
            if now >= _next_request_at_monotonic:
                _next_request_at_monotonic = now + interval
                return
            wait = _next_request_at_monotonic - now
        await asyncio.sleep(wait)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _browser_lock() -> asyncio.Lock:
    global _browser_context_lock
    if _browser_context_lock is None:
        _browser_context_lock = asyncio.Lock()
    return _browser_context_lock


def _pool_lock() -> asyncio.Lock:
    global _browser_pool_lock
    if _browser_pool_lock is None:
        _browser_pool_lock = asyncio.Lock()
    return _browser_pool_lock


def _pool_semaphore() -> asyncio.Semaphore:
    global _browser_pool_semaphore, _browser_pool_loop, _browser_pool_size
    current_loop = asyncio.get_running_loop()
    size = _nodriver_pool_size()
    if (
        _browser_pool_semaphore is None
        or _browser_pool_loop is not current_loop
        or _browser_pool_size != size
    ):
        _browser_pool_semaphore = asyncio.Semaphore(size)
        _browser_pool_loop = current_loop
        _browser_pool_size = size
    return _browser_pool_semaphore


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


def _is_missing_api_key_response(data: object) -> bool:
    if not isinstance(data, dict):
        return False
    error = str(data.get("error") or data.get("detail") or "").lower()
    return "missing api key" in error


_PAGE_FETCH_SCRIPT_TEMPLATE = """
    (async () => {
        try {
            const response = await fetch(%s, { credentials: "include" });
            const text = await response.text();
            return JSON.stringify({ ok: true, status: response.status, text: text });
        } catch (error) {
            return JSON.stringify({ ok: false, error: String(error) });
        }
    })()
"""


async def _fetch_api_json_in_page(page: Any, *, url: str) -> tuple[int, dict]:
    """Run fetch() inside the loris.tools page so the session cookie and Origin
    match real frontend traffic, without paying a full page navigation."""
    await _throttle_loris_request()
    payload = await _evaluate_json(page, _PAGE_FETCH_SCRIPT_TEMPLATE % json.dumps(url))
    if not isinstance(payload, str):
        payload = json.dumps(payload)
    result = json.loads(payload)
    if not isinstance(result, dict) or not result.get("ok"):
        error = result.get("error") if isinstance(result, dict) else result
        raise RuntimeError(f"in-page loris fetch failed: {error}")
    text = str(result.get("text") or "").strip()
    if not text:
        raise RuntimeError("loris API response body was empty")
    return int(result.get("status") or 0), json.loads(text)


async def _stop_shared_browser() -> None:
    global _shared_browser, _shared_page, _shared_loop, _shared_start_error
    global _browser_pool, _browser_pool_loop, _browser_pool_start_error
    browser = _shared_browser
    _shared_browser = None
    _shared_page = None
    _shared_loop = None
    _shared_start_error = None
    if browser is not None:
        await _maybe_await(browser.stop())
    pool = list(_browser_pool)
    _browser_pool = []
    _browser_pool_loop = None
    _browser_pool_start_error = None
    for context in pool:
        pooled_browser = context.get("browser")
        if pooled_browser is not None:
            await _maybe_await(pooled_browser.stop())


async def _navigate_page(*, browser: Any, page: Any, url: str) -> Any:
    page_get = getattr(page, "get", None)
    if page_get is not None:
        try:
            navigated = await _maybe_await(page_get(url))
            return navigated or page
        except Exception:
            pass
    return await browser.get(url)


async def _acquire_browser_context(uc: Any, *, headless: bool, user_data_dir: str | None) -> tuple[dict[str, Any], Any, Any]:
    global _browser_pool_loop, _browser_pool_start_error

    current_loop = asyncio.get_running_loop()
    pool_size = _nodriver_pool_size()
    semaphore = _pool_semaphore()
    await semaphore.acquire()
    try:
        async with _pool_lock():
            if _browser_pool_start_error is not None:
                raise RuntimeError(
                    f"previous loris nodriver browser start failed: {_browser_pool_start_error}"
                ) from _browser_pool_start_error

            if _browser_pool and _browser_pool_loop is not current_loop:
                await _stop_shared_browser()

            _browser_pool_loop = current_loop

            for context in _browser_pool:
                lock = context["lock"]
                if not lock.locked():
                    await lock.acquire()
                    return context, context["browser"], context["page"]

            if len(_browser_pool) < pool_size:
                start_kwargs: dict[str, Any] = {"headless": headless}
                if user_data_dir is not None:
                    start_kwargs["user_data_dir"] = user_data_dir
                try:
                    browser = await uc.start(**start_kwargs)
                except Exception as exc:
                    _browser_pool_start_error = exc
                    raise
                page = await browser.get(LORIS_HOME_URL)
                context = {
                    "browser": browser,
                    "page": page,
                    "lock": asyncio.Lock(),
                }
                await context["lock"].acquire()
                _browser_pool.append(context)
                return context, browser, page

        # Defensive fallback: semaphore says a worker is available, so wait for
        # a context to unlock instead of opening more than the configured pool.
        while True:
            async with _pool_lock():
                for context in _browser_pool:
                    lock = context["lock"]
                    if not lock.locked():
                        await lock.acquire()
                        return context, context["browser"], context["page"]
            await asyncio.sleep(0.05)
    except Exception:
        semaphore.release()
        raise


async def _release_browser_context(context: dict[str, Any], *, page: Any | None = None) -> None:
    if page is not None:
        context["page"] = page
    delay = _nodriver_worker_delay_seconds()
    if delay > 0:
        await asyncio.sleep(delay)
    lock = context.get("lock")
    if lock is not None and lock.locked():
        lock.release()
    semaphore = _browser_pool_semaphore
    if semaphore is not None:
        semaphore.release()


async def _acquire_legacy_browser_context(uc: Any, *, headless: bool, user_data_dir: str | None) -> tuple[Any, Any]:
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
    global _shared_page
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
    context, browser, page = await _acquire_browser_context(
        uc,
        headless=headless,
        user_data_dir=user_data_dir,
    )
    try:
        refresh_reason: str | None = None
        status = 0
        data: dict = {}
        try:
            status, data = await _fetch_api_json_in_page(page, url=url)
        except RuntimeError as exc:
            # e.g. the page drifted off loris.tools so the CORS fetch failed;
            # reloading the home page below re-parks it on the right origin.
            refresh_reason = f"in-page fetch error: {exc}"
        if refresh_reason is None and (status == 401 or _is_missing_api_key_response(data)):
            # api.loris.tools authorizes anonymous browsers via the loris_session
            # cookie issued when loris.tools is loaded. That cookie expires after
            # ~17 minutes, so reload the home page for a fresh session and retry.
            refresh_reason = "session expired"
        if refresh_reason is not None:
            print(
                f"loris refreshing loris.tools session ({refresh_reason}) "
                f"symbol={symbol.upper()}",
                flush=True,
            )
            page = await _navigate_page(browser=browser, page=page, url=LORIS_HOME_URL)
            status, data = await _fetch_api_json_in_page(page, url=url)
            if status == 401 or _is_missing_api_key_response(data):
                raise RuntimeError(
                    "loris API still returned 'Missing API key' after session refresh"
                )
        _shared_page = page
        return data
    except Exception as exc:
        raise RuntimeError(f"loris nodriver fetch failed for {symbol.upper()}: {exc}") from exc
    finally:
        await _release_browser_context(context, page=page)
