from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack
from decimal import Decimal
import json
from pathlib import Path

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from scripts._bootstrap import ensure_project_root_on_path
ensure_project_root_on_path()

from hydra_basis.spread_strategy.dispatcher import Dispatcher, QuoteStore, Settings, run_venue_feed
from hydra_basis.spread_strategy.locks import SymbolLock


def parser():
    result = argparse.ArgumentParser(description="Scan all venues and run spread strategy groups; paper by default")
    result.add_argument("--config", type=Path, default=Path("configs/spread_dispatcher.json"))
    result.add_argument("--data-dir", type=Path, default=Path("data/spread_dispatcher"))
    result.add_argument("--registry", type=Path, default=Path("data/position_registry.json"))
    result.add_argument("--live", action="store_true", help="enable actual orders using project credentials")
    result.add_argument("--resume", action="append", default=[], metavar="GROUP_ID",
                        help="resume a PAUSED or BLOCKED group after manual review")
    result.add_argument("--settle", action="append", default=[], metavar="GROUP_ID:ORDER_ID=QTY[@PRICE]",
                        help="record a manually verified fill for an unresolved order, then resume the group")
    result.add_argument("--max-seconds", type=float, default=0, help="0 runs until Ctrl+C")
    return result


def telegram_notifier(tasks: set):
    from hydra_basis.notifications.telegram import send_telegram

    def notify(message: str):
        task = asyncio.create_task(send_telegram(message))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
    return notify


async def run(args):
    settings = Settings.load(args.config)
    mode = "live" if args.live else "paper"
    lock = SymbolLock(args.data_dir / f"dispatcher.{mode}.lock")
    lock.acquire()  # one dispatcher per mode
    notify_tasks: set = set()
    try:
        async with AsyncExitStack() as stack:
            session = await stack.enter_async_context(aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)))
            if args.live:
                from hydra_basis.env import load_environment
                load_environment()
            dispatcher = Dispatcher(settings, live=args.live, data_dir=args.data_dir, registry_path=args.registry,
                                    store=QuoteStore(), notify=telegram_notifier(notify_tasks))
            if args.live:
                if "variational" in settings.venues:
                    from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer
                    server = await stack.enter_async_context(VariationalCommandBrokerServer(
                        host="127.0.0.1", port=8768, fill_host="127.0.0.1", fill_port=8766,
                        order_fill_timeout_seconds=float(settings.strategy.get("order_timeout_seconds", 20))))
                    await server.wait_for_extension(timeout_seconds=30)
                    await server.wait_for_portfolio(timeout_seconds=15)
                    dispatcher.variational_broker_url = server.ws_url
                # Fail fast on missing credentials rather than rejecting every opportunity later.
                for venue in settings.venues:
                    dispatcher.venue_adapter(venue)
            feeds = [asyncio.create_task(run_venue_feed(venue, session, dispatcher.store, dispatcher.health,
                                                        settings, dispatcher.emit, dispatcher.active_symbols))
                     for venue in settings.venues]
            stack.callback(lambda: [task.cancel() for task in feeds])
            print(json.dumps({"mode": mode, "venues": settings.venues, "max_groups": settings.max_groups,
                              "group_notional_usd": str(settings.group_notional_usd),
                              "method": settings.execution_method}), flush=True)
            # Let the feeds fill before restored groups run their startup checks.
            await asyncio.sleep(5)
            await dispatcher.restore()
            for item in args.settle:
                gid, _, rest = item.partition(":")
                order_id, _, fill = rest.partition("=")
                quantity, _, price = fill.partition("@")
                await dispatcher.settle(gid, order_id, Decimal(quantity), Decimal(price) if price else None)
                if gid not in args.resume:
                    args.resume.append(gid)
            for gid in args.resume:
                await dispatcher.resume(gid)
            try:
                await dispatcher.run(max_seconds=args.max_seconds)
            finally:
                await dispatcher.shutdown()
                print(json.dumps(dispatcher.status([]), default=str), flush=True)
                if notify_tasks:
                    await asyncio.gather(*notify_tasks, return_exceptions=True)
    finally:
        lock.release()


def main():
    args = parser().parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Stopped. Quotes were cancelled; open group positions remain and resume on the next run.")


if __name__ == "__main__":
    main()
