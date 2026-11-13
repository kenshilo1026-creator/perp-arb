"""Local monotonic spans. Never serialize request arguments, bodies or secrets."""
from contextvars import ContextVar
from functools import wraps
import json
from pathlib import Path
import time
import uuid

_active = ContextVar("execution_trace", default=None)
TRACE_PATH = Path(__file__).resolve().parents[2] / "artifacts/execution_timing.jsonl"


def mark(stage, *, at_ns=None, **fields):
    trace = _active.get()
    if trace is None:
        return
    event = {"stage": stage, "monotonic_ns": time.monotonic_ns() if at_ns is None else at_ns, **fields}
    # Bound long-running repricing sessions; explicitly report truncation.
    if len(trace["events"]) >= 4096:
        del trace["events"][:1024]
        trace["dropped_events"] += 1024
    trace["events"].append(event)


def observation(stage, result):
    if not isinstance(result, dict):
        return
    fields = {"source": result.get("source", "unknown"),
              "observed_monotonic_ns": result.get("observed_monotonic_ns")}
    raw = result
    for _ in range(3):
        for key in ("eventTime", "tradeTime", "_received_unix_ms"):
            value = raw.get(key)
            if isinstance(value, (int, float)):
                fields[key] = value
        raw = raw.get("raw")
        if not isinstance(raw, dict):
            break
    mark(stage, **fields)


def timed_stage(stage):
    def decorate(fn):
        @wraps(fn)
        async def wrapped(*args, **kwargs):
            started = time.monotonic_ns()
            mark(stage + ".start", at_ns=started)
            status = "ok"
            try:
                result = await fn(*args, **kwargs)
                if isinstance(result, dict):
                    observation(stage + ".observation", result)
                return result
            except BaseException as exc:
                status = type(exc).__name__  # exception text may contain signed URLs
                raise
            finally:
                ended = time.monotonic_ns()
                mark(stage + ".end", at_ns=ended, duration_ms=(ended-started)/1e6, status=status)
        return wrapped
    return decorate


def traced_execution(fn):
    @wraps(fn)
    async def wrapped(*args, **kwargs):
        trace = {"schema_version": 1, "trace_id": uuid.uuid4().hex,
                 "started_unix_ms": time.time_ns() // 1_000_000,
                 "started_monotonic_ns": time.monotonic_ns(),
                 "symbol": kwargs.get("symbol"), "maker_venue": kwargs.get("maker_venue"),
                 "taker_venue": kwargs.get("taker_venue"), "events": [], "dropped_events": 0,
                 "clock_note": "Durations use local monotonic time. Exchange eventTime/tradeTime and local Unix time are not clock-offset corrected. Nested/parallel spans must not be summed."}
        token = _active.set(trace)
        try:
            result = await fn(*args, **kwargs)
            trace["status"] = "ok" if result.get("ok") else "failed"
            trace["execution_timing"] = result.get("execution_timing")
            trace["execution_price_summary"] = result.get("execution_price_summary")
            result["execution_trace"] = trace
            return result
        except BaseException as exc:
            trace["status"] = type(exc).__name__
            exc.execution_trace = trace
            raise
        finally:
            trace["ended_monotonic_ns"] = time.monotonic_ns()
            _active.reset(token)
    return wrapped


def record_execution(fn):
    """Persist after the executor/adapter cleanup, never during hedge dispatch."""
    @wraps(fn)
    async def wrapped(*args, **kwargs):
        trace = None
        try:
            result = await fn(*args, **kwargs)
            trace = result.get("execution_trace")
            return result
        except BaseException as exc:
            trace = getattr(exc, "execution_trace", None)
            raise
        finally:
            if trace is not None:
                try:
                    TRACE_PATH.parent.mkdir(parents=True, exist_ok=True)
                    with TRACE_PATH.open("a", encoding="utf-8") as out:
                        out.write(json.dumps(trace, ensure_ascii=False) + "\n")
                    print(f"[timing] trace={trace['trace_id']} saved={TRACE_PATH.resolve()}", flush=True)
                except (OSError, TypeError, ValueError):
                    print("[timing] 記錄寫入失敗；不重試已完成的交易", flush=True)
    return wrapped
