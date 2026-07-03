from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse


LORIS_HOME_URL = "https://loris.tools/"
LORIS_FRONTEND_HISTORICAL_URL = "https://loris.tools/funding/historical"
LORIS_HISTORICAL_URL = "https://api.loris.tools/funding/historical"
DEFAULT_LORIS_NODRIVER_TIMEOUT_SECONDS = 45.0
DEFAULT_LORIS_API_KEY_HEADER = "X-API-Key"
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
    raw = os.getenv("LORIS_NODRIVER_WORKER_DELAY_SECONDS", "2.0").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 2.0


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


def _install_loris_fetch_hook_script() -> str:
    return """
        (() => {
            if (window.__lorisHistoricalFetchHookInstalled) {
                return;
            }
            window.__lorisHistoricalFetchHookInstalled = true;
            window.__lorisHistoricalResponses = [];
            const originalFetch = window.fetch.bind(window);
            window.fetch = async (...args) => {
                const response = await originalFetch(...args);
                try {
                    const requestUrl = typeof args[0] === "string" ? args[0] : (args[0] && args[0].url) || "";
                    if (requestUrl.includes("api.loris.tools/funding/historical")) {
                        const text = await response.clone().text();
                        window.__lorisHistoricalResponses.push({
                            url: requestUrl,
                            ok: response.ok,
                            status: response.status,
                            text,
                            ts: Date.now(),
                        });
                    }
                } catch (error) {
                    window.__lorisHistoricalResponses.push({
                        url: "",
                        ok: false,
                        status: 0,
                        error: error && error.message ? error.message : String(error),
                        ts: Date.now(),
                    });
                }
                return response;
            };
        })();
    """


def _is_loris_historical_response_for_symbol(entry: dict, *, symbol: str) -> bool:
    url = str(entry.get("url") or "")
    if "api.loris.tools/funding/historical" not in url:
        return False
    query = parse_qs(urlparse(url).query)
    return (query.get("symbol") or [""])[0].upper() == symbol.upper()


async def _read_loris_historical_responses(page: Any) -> list[dict]:
    payload = await _evaluate_json(
        page,
        "JSON.stringify(window.__lorisHistoricalResponses || [])",
    )
    if not isinstance(payload, str):
        raise RuntimeError(f"loris response buffer returned non-string response: {type(payload).__name__}")
    data = json.loads(payload or "[]")
    return data if isinstance(data, list) else []


async def _capture_loris_historical_response_from_frontend(
    *,
    browser: Any,
    page: Any,
    symbol: str,
    api_url: str,
) -> tuple[Any, dict]:
    from nodriver import cdp

    target_symbol = symbol.upper()
    script_id = None
    try:
        script_id = await _maybe_await(
            page.send(
                cdp.page.add_script_to_evaluate_on_new_document(
                    source=_install_loris_fetch_hook_script(),
                    run_immediately=True,
                )
            )
        )
        page = await _navigate_page(
            browser=browser,
            page=page,
            url=_frontend_historical_url(symbol),
        )
        timeout_seconds = float(os.getenv("LORIS_NODRIVER_NETWORK_TIMEOUT_SECONDS", "30"))
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            responses = await _read_loris_historical_responses(page)
            matches = [
                item for item in responses
                if isinstance(item, dict)
                and _is_loris_historical_response_for_symbol(item, symbol=target_symbol)
            ]
            if matches:
                latest = matches[-1]
                text = str(latest.get("text") or "")
                if not latest.get("ok"):
                    status = latest.get("status")
                    error = latest.get("error") or text[:500]
                    raise RuntimeError(f"loris historical response {status}: {error}")
                if not text.strip():
                    raise RuntimeError("loris historical response body was empty")
                return page, json.loads(text)
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError(f"timed out waiting for Loris frontend historical response for {target_symbol}")
            await asyncio.sleep(0.5)
    finally:
        if script_id is not None:
            try:
                await _maybe_await(
                    page.send(cdp.page.remove_script_to_evaluate_on_new_document(script_id))
                )
            except Exception:
                pass


def _frontend_historical_url(symbol: str) -> str:
    params = urlencode({
        "symbol": symbol.upper(),
        "range": "7d",
        "unit": "apy",
        "exchanges": "variational",
    })
    return f"{LORIS_FRONTEND_HISTORICAL_URL}?{params}"


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
        page = await _navigate_page(browser=browser, page=page, url=url)
        data = await _read_json_document(page)
        _shared_page = page
        return data
    except Exception as exc:
        raise RuntimeError(f"loris nodriver navigation fetch failed for {symbol.upper()}: {exc}") from exc
    finally:
        await _release_browser_context(context, page=page)
