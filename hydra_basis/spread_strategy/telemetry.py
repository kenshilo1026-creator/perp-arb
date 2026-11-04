"""Best-effort UI heartbeat, independent of order/state persistence."""
import asyncio
import json
import os
from contextlib import asynccontextmanager

from hydra_basis.spread_strategy.core import now_ms


def write_heartbeat(engine, running):
    path = engine.store.path.with_suffix('.heartbeat.json')
    temp = path.with_suffix(f'.{os.getpid()}.tmp')
    try:
        temp.write_text(json.dumps({'strategy_id': engine.state.strategy_id,
                                   'running': running, 'ts_ms': now_ms()}), encoding='utf-8')
        os.replace(temp, path)
    except OSError:
        # UI availability must never prevent a hedge or strategy shutdown.
        pass
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


@asynccontextmanager
async def heartbeat(engine):
    stop = asyncio.Event()
    async def publish():
        while not stop.is_set():
            await asyncio.to_thread(write_heartbeat, engine, True)
            try:
                await asyncio.wait_for(stop.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass
    task = asyncio.create_task(publish())
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.to_thread(write_heartbeat, engine, False)
