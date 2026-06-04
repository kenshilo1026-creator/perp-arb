from __future__ import annotations

import argparse
import asyncio

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.config import MARGIN_TOPUP_CONFIG_PATH, POSITION_REGISTRY_PATH
from hydra_basis.env import load_environment
from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter
from hydra_basis.execution_engine.lighter_adapter import LighterExecutionAdapter
from hydra_basis.execution_engine.lighter_live import (
    build_lighter_client_factory_from_env,
    fetch_lighter_market_config,
    fetch_lighter_orderbook_live,
)
from hydra_basis.execution_engine.mexc_adapter import MexcExecutionAdapter
from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter
from hydra_basis.notifications.telegram import send_telegram
from hydra_basis.risk_management.closers import MarketTypeRouterCloser
from hydra_basis.risk_management.exchange_watchers import (
    AsterMarginHealthPoller,
    AsterForceOrdersPoller,
    HyperliquidMarginHealthPoller,
    HyperliquidUserEventsWatcher,
)
from hydra_basis.risk_management.manager import EmergencyRiskManager
from hydra_basis.risk_management.margin_topup import (
    MarginTopupManager,
    build_snapshots_for_signal,
    load_margin_topup_config,
)
from hydra_basis.risk_management.registry import PositionRegistry
from hydra_basis.risk_management.runtime import process_watcher_once


load_environment()


def build_closers() -> dict[str, object]:
    closers: dict[str, object] = {
        "aster": AsterExecutionAdapter(),
        "lighter": LighterExecutionAdapter(
            signer_client_factory=build_lighter_client_factory_from_env(),
            market_config_loader=lambda symbol: fetch_lighter_market_config(symbol),
            orderbook_loader=lambda symbol: fetch_lighter_orderbook_live(symbol),
        ),
        "mexc": MarketTypeRouterCloser(
            perp=MexcExecutionAdapter(),
            spot=MexcSpotExecutionAdapter(),
        ),
    }
    try:
        from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter

        closers["hyperliquid"] = HyperliquidExecutionAdapter()
    except Exception as exc:
        print(f"risk manager hyperliquid closer disabled: {exc!r}")
    return closers


def build_watchers(enabled: set[str]) -> list[object]:
    watchers: list[object] = []
    if "aster" in enabled:
        watchers.append(AsterForceOrdersPoller())
    if "hyperliquid" in enabled:
        try:
            watchers.append(HyperliquidUserEventsWatcher())
        except Exception as exc:
            print(f"risk manager hyperliquid watcher disabled: {exc!r}")
    if "mexc" in enabled:
        print("risk manager mexc watcher pending: private liquidation payload not verified yet")
    if "lighter" in enabled:
        print("risk manager lighter watcher pending: private liquidation payload not verified yet")
    if "variational" in enabled:
        print("risk manager variational watcher pending: browser/private risk payload not verified yet")
    return watchers


def build_margin_watchers(enabled: set[str]) -> list[object]:
    watchers: list[object] = []
    if "aster" in enabled:
        watchers.append(AsterMarginHealthPoller())
    if "hyperliquid" in enabled:
        try:
            watchers.append(HyperliquidMarginHealthPoller())
        except Exception as exc:
            print(f"risk manager hyperliquid margin watcher disabled: {exc!r}")
    if "lighter" in enabled:
        print("risk manager lighter margin top-up pending: no verified position health/top-up API")
    return watchers


async def run_risk_manager(*, venues: set[str], live: bool) -> None:
    dry_run = not live
    watchers = build_watchers(venues)
    margin_watchers = build_margin_watchers(venues)
    closers = build_closers()
    margin_config = load_margin_topup_config(MARGIN_TOPUP_CONFIG_PATH)
    mode = "LIVE" if live else "DRY_RUN"
    print(f"risk manager running mode={mode} registry={POSITION_REGISTRY_PATH}")
    print(f"margin top-up config={MARGIN_TOPUP_CONFIG_PATH} enabled={margin_config.enabled}")
    await send_telegram(f"風控監控已啟動 mode={mode}")
    if not watchers and not margin_watchers:
        raise RuntimeError("no risk or margin watchers enabled")

    async def run_one_watcher(watcher) -> None:
        while True:
            result = await process_watcher_once(
                registry_path=POSITION_REGISTRY_PATH,
                watcher=watcher,
                closers=closers,
                dry_run=dry_run,
            )
            if result.get("closed_leg_ids") or result.get("failed_leg_ids"):
                message = (
                    f"風控事件處理完成\n"
                    f"trigger={result.get('trigger_leg_id')}\n"
                    f"closed={result.get('closed_leg_ids')}\n"
                    f"failed={result.get('failed_leg_ids')}\n"
                    f"mode={mode}"
                )
                print(message)
                await send_telegram(message)
            await asyncio.sleep(0)

    async def run_one_margin_watcher(watcher) -> None:
        while True:
            async for signal in watcher.watch():
                registry = PositionRegistry.load(POSITION_REGISTRY_PATH)
                manager = MarginTopupManager(
                    registry=registry,
                    toppers=closers,
                    config=margin_config,
                    dry_run=dry_run,
                )
                emergency_manager = EmergencyRiskManager(
                    registry=registry,
                    closers=closers,
                    dry_run=dry_run,
                )
                for snapshot in build_snapshots_for_signal(registry=registry, signal=signal):
                    result = await manager.handle_snapshot(snapshot)
                    if result.get("action") in {"topup_done", "topup_dry_run", "topup_failed"}:
                        message = (
                            f"保證金風控\n"
                            f"venue={snapshot.venue} symbol={snapshot.symbol} leg={snapshot.leg_id}\n"
                            f"action={result.get('action')} distance={float(result.get('distance_pct', 0)):.4f}%\n"
                            f"amount={result.get('topup_amount_usd')} mode={mode}"
                        )
                        print(message)
                        await send_telegram(message)
                    if result.get("risk_event") is not None:
                        emergency_result = await emergency_manager.handle_event(result["risk_event"])
                        emergency_message = (
                            f"補保證金失敗，已觸發緊急平倉\n"
                            f"trigger={emergency_result.get('trigger_leg_id')}\n"
                            f"closed={emergency_result.get('closed_leg_ids')}\n"
                            f"failed={emergency_result.get('failed_leg_ids')}\n"
                            f"mode={mode}"
                        )
                        print(emergency_message)
                        await send_telegram(emergency_message)
                registry.save(POSITION_REGISTRY_PATH)
            await asyncio.sleep(0)

    await asyncio.gather(
        *(run_one_watcher(watcher) for watcher in watchers),
        *(run_one_margin_watcher(watcher) for watcher in margin_watchers),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run emergency risk manager watchers.")
    parser.add_argument(
        "--venues",
        default="aster,hyperliquid,mexc,lighter,variational",
        help="Comma-separated watcher venues to enable.",
    )
    parser.add_argument("--live", action="store_true", help="Actually submit emergency close orders.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    venues = {item.strip().lower() for item in args.venues.split(",") if item.strip()}
    asyncio.run(run_risk_manager(venues=venues, live=args.live))


if __name__ == "__main__":
    main()
