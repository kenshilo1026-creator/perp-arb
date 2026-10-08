from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from scripts._bootstrap import ensure_project_root_on_path
ensure_project_root_on_path()

from hydra_basis.spread_strategy.core import Config, StateStore, Strategy
from hydra_basis.spread_strategy.broker import LiveBroker, PaperBroker, QuoteSource, build_adapters


@contextmanager
def symbol_lock(path: Path):
    """OS lock releases on crash; never delete a lock another process holds."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise RuntimeError("another spread strategy owns this symbol") from exc
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def parser():
    result = argparse.ArgumentParser(description="Perpetual cross-venue spread convergence; paper by default")
    result.add_argument("--config", type=Path, default=Path("configs/spread_strategy.example.json"))
    result.add_argument("--state", type=Path)
    result.add_argument("--registry", type=Path, default=Path("data/position_registry.json"))
    result.add_argument("--live", action="store_true", help="enable actual orders using project credentials")
    result.add_argument("--max-ticks", type=int, default=0, help="0 runs until the single cycle completes")
    return result


async def run(args):
    if args.max_ticks < 0:
        raise ValueError("max-ticks must not be negative")
    config = Config.load(args.config)
    mode = "live" if args.live else "paper"
    state_path = args.state or Path("data/spread_strategies") / f"{config.fingerprint()[:16]}.{mode}.json"
    # Different config/state filenames cannot start parallel cycles on a symbol.
    lock_path = Path("data/spread_strategies") / f"{hashlib.sha256(config.symbol.encode()).hexdigest()[:16]}.{mode}.lock"
    with symbol_lock(lock_path):
        store = StateStore(state_path)
        state = store.load(config, live=args.live)
        store.save(state)
        if state.phase == "DONE":
            print(json.dumps({"phase": "DONE", "state": str(state_path)}))
            return
        async with AsyncExitStack() as stack:
            session = await stack.enter_async_context(aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)))
            source = QuoteSource(config, session)
            if args.live:
                from hydra_basis.env import load_environment
                load_environment()
                broker_url = None
                if "variational" in config.venues:
                    from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
                    server = await stack.enter_async_context(VariationalCommandBrokerServer(
                        host="127.0.0.1", port=8768, fill_host="127.0.0.1", fill_port=8766,
                        order_fill_timeout_seconds=config.maker_timeout_seconds))
                    await server.wait_for_extension(timeout_seconds=30)
                    await server.wait_for_portfolio(timeout_seconds=15)
                    broker_url = server.ws_url
                broker = LiveBroker(config, source, build_adapters(config, broker_url), args.registry)
                stack.push_async_callback(broker.close)
                for adapter in broker.adapters.values():
                    warm_up = getattr(adapter, "warm_up", None)
                    if callable(warm_up):
                        await warm_up()
            else:
                broker = PaperBroker(config, source)
            strategy = Strategy(config, broker, store, live=args.live)
            print(json.dumps({"mode": mode, "state": str(state_path), "symbol": config.symbol,
                              "short": config.short_venue, "long": config.long_venue}))
            ticks = 0
            while True:
                event = await strategy.step()
                print(json.dumps(event), flush=True)
                ticks += 1
                if event["phase"] in {"DONE", "PAUSED"} or (args.max_ticks and ticks >= args.max_ticks):
                    break
                await asyncio.sleep(config.poll_seconds)


def main():
    args = parser().parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Stopped. Saved state is retained; open positions are not silently abandoned or adopted.")


if __name__ == "__main__":
    main()
