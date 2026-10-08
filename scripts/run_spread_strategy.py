from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack, contextmanager
import hashlib
import json
import os
import time
from decimal import Decimal
from pathlib import Path

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from scripts._bootstrap import ensure_project_root_on_path
ensure_project_root_on_path()

from hydra_basis.spread_strategy.broker import (
    PaperVenue, assert_registry_owner, build_adapters, sync_registry,
)
from hydra_basis.spread_strategy.core import Config, StateStore
from hydra_basis.spread_strategy.engine import Engine
from hydra_basis.spread_strategy.feeds import MarketFeed
from hydra_basis.spread_strategy.instruments import fetch_instrument

STATUS_EVERY_SECONDS = 10.0


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
    result = argparse.ArgumentParser(description="Perpetual cross-venue spread strategy; paper by default")
    result.add_argument("--config", type=Path, default=Path("configs/spread_strategy.example.json"))
    result.add_argument("--state", type=Path)
    result.add_argument("--registry", type=Path, default=Path("data/position_registry.json"))
    result.add_argument("--live", action="store_true", help="enable actual orders using project credentials")
    result.add_argument("--resume", action="store_true", help="resume a PAUSED strategy after manual review")
    result.add_argument("--settle-order", action="append", default=[], metavar="ID=QTY[@PRICE]",
                        help="record a manually verified fill (QTY may be 0) for an unresolved order")
    result.add_argument("--max-seconds", type=float, default=0, help="0 runs until stopped or paused")
    return result


async def wait_for_market(feed: MarketFeed, timeout_seconds: float = 30.0):
    deadline = time.monotonic() + timeout_seconds
    while feed.fresh_pair() is None:
        if time.monotonic() >= deadline:
            raise RuntimeError("no fresh quotes from both venues; check connectivity")
        await asyncio.sleep(0.2)


async def run(args):
    if args.max_seconds < 0:
        raise ValueError("max-seconds must not be negative")
    config = Config.load(args.config)
    mode = "live" if args.live else "paper"
    state_path = args.state or Path("data/spread_strategies") / f"{config.identity()}.{mode}.json"
    # Different config/state filenames cannot start parallel strategies on a symbol.
    lock_path = Path("data/spread_strategies") / f"{hashlib.sha256(config.symbol.encode()).hexdigest()[:16]}.{mode}.lock"
    with symbol_lock(lock_path):
        store = StateStore(state_path)
        state = store.load(config, live=args.live)
        if state.status == "STOPPED":
            print(json.dumps({"status": "STOPPED", "reason": state.reason, "state": str(state_path)}))
            return
        async with AsyncExitStack() as stack:
            session = await stack.enter_async_context(aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)))
            feed = MarketFeed(config)
            feed_task = asyncio.create_task(feed.run(session))
            stack.callback(feed_task.cancel)
            instruments = {venue: await fetch_instrument(session, venue, config.symbol) for venue in config.venues}
            on_exposure = None
            if args.live:
                from hydra_basis.env import load_environment
                load_environment()
                assert_registry_owner(args.registry, config, state)
                broker_url = None
                if "variational" in config.venues:
                    from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
                    server = await stack.enter_async_context(VariationalCommandBrokerServer(
                        host="127.0.0.1", port=8768, fill_host="127.0.0.1", fill_port=8766,
                        order_fill_timeout_seconds=config.order_timeout_seconds))
                    await server.wait_for_extension(timeout_seconds=30)
                    await server.wait_for_portfolio(timeout_seconds=15)
                    broker_url = server.ws_url
                adapters = build_adapters(config, broker_url)
                for adapter in adapters.values():
                    warm_up = getattr(adapter, "warm_up", None)
                    if callable(warm_up):
                        await warm_up()
                on_exposure = lambda current: sync_registry(args.registry, config, current)
            else:
                adapters = {venue: PaperVenue(venue, feed) for venue in config.venues}
            await wait_for_market(feed)
            engine = Engine(config, state, store, feed, adapters, instruments, live=args.live,
                            on_exposure=on_exposure)
            for item in args.settle_order:
                order_id, _, fill = item.partition("=")
                quantity, _, price = fill.partition("@")
                engine.settle_order(order_id, Decimal(quantity), Decimal(price) if price else None)
            await engine.start(resume=args.resume)
            print(json.dumps({"mode": mode, "state": str(state_path), "symbol": config.symbol,
                              "short": config.short_venue, "long": config.long_venue,
                              "method": config.execution_method, "strategy_id": state.strategy_id}), flush=True)
            started = last_status = time.monotonic()
            try:
                while state.status in {"RUNNING", "STOPPING"}:
                    if feed_task.done():
                        raise RuntimeError(f"market feed stopped: {feed_task.exception()}")
                    await engine.step()
                    now = time.monotonic()
                    if now - last_status >= STATUS_EVERY_SECONDS:
                        print(json.dumps({"event": "status", **engine.snapshot()}), flush=True)
                        last_status = now
                    if args.max_seconds and now - started >= args.max_seconds:
                        break
                    await asyncio.sleep(config.tick_seconds)
            finally:
                if state.status in {"RUNNING", "STOPPING"}:
                    await engine.shutdown()
                print(json.dumps({"event": "exit", **engine.snapshot()}), flush=True)


def main():
    args = parser().parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Stopped. Resting quotes were cancelled; open positions remain and resume on the next run.")


if __name__ == "__main__":
    main()
