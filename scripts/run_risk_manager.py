from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import time

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.config import (
    FUNDING_RISK_CONFIG_PATH,
    FUNDING_RISK_STATE_PATH,
    MARGIN_TOPUP_CONFIG_PATH,
    POSITION_REGISTRY_PATH,
    VARIATIONAL_CURRENT_FUNDING_CACHE_PATH,
)
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
from hydra_basis.execution_engine.variational_monitor_adapter import VariationalMonitorPositionAdapter
from hydra_basis.risk_management.closers import MarketTypeRouterCloser
from hydra_basis.risk_management.exchange_watchers import (
    AsterMarginHealthPoller,
    AsterForceOrdersPoller,
    HyperliquidMarginHealthPoller,
    HyperliquidUserEventsWatcher,
    LighterMarginHealthPoller,
    LiveMarkPriceCache,
    run_aster_mark_price_cache,
    run_hyperliquid_mids_cache,
)
from hydra_basis.risk_management.manager import EmergencyRiskManager
from hydra_basis.risk_management.funding_risk import (
    ProjectedFundingRate,
    funding_cashflow_pct,
    load_funding_risk_config,
)
from hydra_basis.risk_management.funding_runtime import (
    FundingHistoryRiskDataProvider,
    process_funding_risk_once,
)
from hydra_basis.risk_management.margin_topup import (
    MarginTopupManager,
    build_snapshots_for_signal,
    liquidation_distance_pct,
    load_margin_topup_config,
)
from hydra_basis.risk_management.registry import PositionRegistry
from hydra_basis.risk_management.reconciliation import reconcile_registry_positions
from hydra_basis.risk_management.runtime import process_watcher_once


load_environment()

RECONCILIATION_INTERVAL_SECONDS = 60
FUNDING_CHECK_MINUTE = 1


def format_emergency_risk_message(*, result: dict[str, object], mode: str) -> str:
    lines = [
        "風控事件處理完成",
        f"event={result.get('event_type')}",
        f"trigger={result.get('trigger_leg_id')} venue={result.get('trigger_venue')} symbol={result.get('trigger_symbol')}",
        f"closed={result.get('closed_leg_ids')}",
        f"manual={result.get('manual_leg_ids')}",
        f"failed={result.get('failed_leg_ids')}",
        f"mode={mode}",
    ]
    manual_ids = result.get("manual_leg_ids") or []
    if manual_ids:
        lines.append("注意: Variational 需要手動平倉，系統不會用 browser 在 VPS 自動操作。")
    return "\n".join(lines)


def seconds_until_next_hourly_minute(
    *,
    now: dt.datetime | None = None,
    minute: int = FUNDING_CHECK_MINUTE,
) -> float:
    current = now or dt.datetime.now().astimezone()
    if current.tzinfo is None:
        current = current.astimezone()
    target = current.replace(minute=minute, second=0, microsecond=0)
    if target <= current:
        target += dt.timedelta(hours=1)
    return max(0.0, (target - current).total_seconds())


def format_funding_position_summary(
    *,
    registry: PositionRegistry,
    projected_rates_by_strategy: dict[str, list[ProjectedFundingRate]],
    mode: str,
) -> str:
    lines = [f"現有倉位資費檢查 mode={mode}"]
    has_rows = False
    for strategy_id in registry.open_strategy_ids():
        legs = [
            leg
            for leg in registry.legs_for_strategy(strategy_id)
            if leg.status == "open" and leg.market_type != "spot"
        ]
        if not legs:
            continue
        rates = {row.leg_id: row for row in projected_rates_by_strategy.get(strategy_id, [])}
        strategy_lines: list[str] = []
        net_cashflow = 0.0
        missing = False
        for leg in legs:
            rate = rates.get(leg.leg_id)
            if rate is None:
                missing = True
                strategy_lines.append(f"- {leg.venue} {leg.symbol} {leg.side} rate=缺少資料 leg={leg.leg_id}")
                continue
            cashflow = funding_cashflow_pct(side=leg.side, funding_rate=rate.funding_rate)
            net_cashflow += cashflow
            strategy_lines.append(
                f"- {leg.venue} {leg.symbol} {leg.side} "
                f"rate={rate.funding_rate:.6%}/{rate.interval_hours:g}h "
                f"cashflow={cashflow:.6%} leg={leg.leg_id}"
            )
        has_rows = True
        net_text = "資料不足" if missing else f"{net_cashflow:.6%}"
        lines.append(f"strategy={strategy_id} net={net_text}")
        lines.extend(strategy_lines)
    if not has_rows:
        lines.append("沒有 open perp position 需要檢查資費。")
    return "\n".join(lines)


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
    closers["variational"] = VariationalMonitorPositionAdapter()
    return closers


def build_watchers(enabled: set[str]) -> list[object]:
    watchers: list[object] = []
    if "aster" in enabled:
        async def notify_aster_rate_limit(exc: Exception) -> None:
            message = (
                "Aster API 出錯: Too many requests / 429\n"
                f"{exc}\n"
                "Aster user stream watcher 會 sleep 10 秒後再繼續。"
            )
            print(message)
            await send_telegram(message)

        watchers.append(AsterForceOrdersPoller(on_rate_limit=notify_aster_rate_limit))
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


def build_margin_watchers(enabled: set[str], *, mark_price_caches: dict[str, LiveMarkPriceCache]) -> list[object]:
    watchers: list[object] = []
    if "aster" in enabled:
        watchers.append(AsterMarginHealthPoller(mark_price_provider=mark_price_caches["aster"].get))
    if "hyperliquid" in enabled:
        try:
            watchers.append(HyperliquidMarginHealthPoller(mark_price_provider=mark_price_caches["hyperliquid"].get))
        except Exception as exc:
            print(f"risk manager hyperliquid margin watcher disabled: {exc!r}")
    if "lighter" in enabled:
        watchers.append(LighterMarginHealthPoller())
    return watchers


def _closer_key_for_venue(venue: str) -> str:
    normalized = venue.strip().lower()
    if normalized == "mexc_spot":
        return "mexc"
    return normalized


def _position_key(*, venue: str, symbol: str, market_type: str, side: str) -> tuple[str, str, str, str]:
    return (
        venue.strip().lower(),
        symbol.strip().upper(),
        market_type.strip().lower(),
        side.strip().upper(),
    )


async def build_startup_position_snapshot_message(
    *,
    registry: PositionRegistry,
    closers: dict[str, object],
    mode: str,
) -> str:
    lines = [f"風控啟動倉位快照 mode={mode}"]
    position_lines: list[str] = []
    issue_lines: list[str] = []
    registered_live_keys: set[tuple[str, str, str, str]] = set()
    open_legs = [
        leg
        for strategy_id in registry.open_strategy_ids()
        for leg in registry.legs_for_strategy(strategy_id)
        if leg.status == "open"
    ]

    for leg in open_legs:
        closer = closers.get(_closer_key_for_venue(leg.venue))
        if closer is None:
            issue_lines.append(f"- {leg.venue} {leg.symbol} leg={leg.leg_id}: 沒有 closer")
            continue
        getter = getattr(closer, "get_open_position", None)
        if not callable(getter):
            issue_lines.append(f"- {leg.venue} {leg.symbol} leg={leg.leg_id}: 不支援 live position 查詢")
            continue
        try:
            live = await getter(symbol=leg.symbol, market_type=leg.market_type)
        except Exception as exc:
            issue_lines.append(f"- {leg.venue} {leg.symbol} leg={leg.leg_id}: 查詢失敗 {exc}")
            continue
        if not live:
            issue_lines.append(f"- {leg.venue} {leg.symbol} leg={leg.leg_id}: live position 不存在")
            continue
        symbol = str(live.get("symbol") or leg.symbol).strip().upper()
        market_type = str(live.get("market_type") or leg.market_type).strip().lower()
        side = str(live.get("side", "")).strip().upper()
        quantity = str(live.get("quantity", "")).strip()
        registered_live_keys.add(
            _position_key(venue=leg.venue, symbol=symbol, market_type=market_type, side=side)
        )
        position_lines.append(
            f"- {leg.venue} {symbol} {market_type} {side} qty={quantity} "
            f"strategy={leg.strategy_id} leg={leg.leg_id}"
        )

    unregistered_lines: list[str] = []
    for venue, closer in closers.items():
        lister = getattr(closer, "list_open_positions", None)
        if not callable(lister):
            continue
        try:
            live_positions = await lister()
        except Exception as exc:
            issue_lines.append(f"- {venue}: list_open_positions 失敗 {exc}")
            continue
        for live in live_positions or []:
            live_venue = str(live.get("venue") or venue).strip().lower()
            symbol = str(live.get("symbol", "")).strip().upper()
            market_type = str(live.get("market_type", "")).strip().lower()
            side = str(live.get("side", "")).strip().upper()
            quantity = str(live.get("quantity", "")).strip()
            if not symbol or market_type not in {"perp", "spot"} or side not in {"LONG", "SHORT"}:
                continue
            key = _position_key(venue=live_venue, symbol=symbol, market_type=market_type, side=side)
            if key in registered_live_keys:
                continue
            unregistered_lines.append(f"- {live_venue} {symbol} {market_type} {side} qty={quantity}")

    if position_lines:
        lines.append("已登記 live 倉位:")
        lines.extend(position_lines)
    else:
        lines.append("已登記 live 倉位: 無")
    if unregistered_lines:
        lines.append("未登記 live 倉位:")
        lines.extend(unregistered_lines)
    if issue_lines:
        lines.append("查詢提示:")
        lines.extend(issue_lines)
    return "\n".join(lines)


async def run_risk_manager(*, venues: set[str], live: bool) -> None:
    dry_run = not live
    watchers = build_watchers(venues)
    mark_price_caches = {
        "aster": LiveMarkPriceCache(),
        "hyperliquid": LiveMarkPriceCache(),
    }
    margin_watchers = build_margin_watchers(venues, mark_price_caches=mark_price_caches)
    closers = build_closers()
    margin_config = load_margin_topup_config(MARGIN_TOPUP_CONFIG_PATH)
    funding_config = load_funding_risk_config(FUNDING_RISK_CONFIG_PATH)
    mode = "LIVE" if live else "DRY_RUN"
    print(f"risk manager running mode={mode} registry={POSITION_REGISTRY_PATH}")
    print(f"margin top-up config={MARGIN_TOPUP_CONFIG_PATH} enabled={margin_config.enabled}")
    print(f"funding risk config={FUNDING_RISK_CONFIG_PATH} enabled={funding_config.enabled}")
    await send_telegram(f"風控監控已啟動 mode={mode}")
    try:
        startup_snapshot = await build_startup_position_snapshot_message(
            registry=PositionRegistry.load(POSITION_REGISTRY_PATH),
            closers=closers,
            mode=mode,
        )
        print(startup_snapshot)
        await send_telegram(startup_snapshot)
    except Exception as exc:
        message = f"風控啟動倉位快照失敗: {exc!r}\nmode={mode}"
        print(message)
        await send_telegram(message)
    if not watchers and not margin_watchers and not funding_config.enabled:
        print("risk manager watcher set is empty; running reconciliation loop only")

    async def run_one_watcher(watcher) -> None:
        while True:
            try:
                result = await process_watcher_once(
                    registry_path=POSITION_REGISTRY_PATH,
                    watcher=watcher,
                    closers=closers,
                    dry_run=dry_run,
                )
                if result.get("event_type"):
                    message = format_emergency_risk_message(result=result, mode=mode)
                    print(message)
                    await send_telegram(message)
                await asyncio.sleep(0)
            except Exception as exc:
                print(f"risk watcher error {watcher.__class__.__name__}: {exc!r}")
                await asyncio.sleep(30)

    async def run_one_margin_watcher(watcher) -> None:
        lighter_alert_last_sent_ms: dict[str, int] = {}
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
                    if snapshot.venue.strip().lower() == "lighter":
                        distance_pct = liquidation_distance_pct(
                            side=snapshot.side,
                            mark_price=snapshot.mark_price,
                            liquidation_price=snapshot.liquidation_price,
                        )
                        if distance_pct > margin_config.liq_distance_trigger_pct:
                            continue
                        now_ms = int(time.time() * 1000)
                        cooldown_ms = margin_config.cooldown_seconds * 1000
                        last_sent_ms = lighter_alert_last_sent_ms.get(snapshot.leg_id)
                        if last_sent_ms is not None and now_ms - last_sent_ms < cooldown_ms:
                            continue
                        lighter_alert_last_sent_ms[snapshot.leg_id] = now_ms
                        message = (
                            "Lighter 強平風險\n"
                            f"symbol={snapshot.symbol} side={snapshot.side} leg={snapshot.leg_id}\n"
                            f"distance={distance_pct:.4f}% "
                            f"mark={snapshot.mark_price} liq={snapshot.liquidation_price}\n"
                            f"mode={mode}"
                        )
                        print(message)
                        await send_telegram(message)
                        continue
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
                        emergency_message = "補保證金失敗，已觸發緊急平倉\n" + format_emergency_risk_message(
                            result=emergency_result,
                            mode=mode,
                        )
                        print(emergency_message)
                        await send_telegram(emergency_message)
                registry.save(POSITION_REGISTRY_PATH)
            await asyncio.sleep(0)

    async def run_funding_risk_loop() -> None:
        async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-risk/0.1"}) as session:
            provider = FundingHistoryRiskDataProvider(
                session=session,
                current_funding_cache_path=VARIATIONAL_CURRENT_FUNDING_CACHE_PATH,
            )
            while True:
                sleep_seconds = seconds_until_next_hourly_minute()
                print(f"funding risk next check in {sleep_seconds:.1f}s at minute {FUNDING_CHECK_MINUTE:02d}")
                await asyncio.sleep(sleep_seconds)
                result = await process_funding_risk_once(
                    registry_path=POSITION_REGISTRY_PATH,
                    state_path=FUNDING_RISK_STATE_PATH,
                    provider=provider,
                    closers=closers,
                    config=funding_config,
                    dry_run=dry_run,
                )
                for message in result.get("messages", []):
                    text = f"資費風控\n{message}\nmode={mode}"
                    print(text)
                    await send_telegram(text)
                summary = format_funding_position_summary(
                    registry=PositionRegistry.load(POSITION_REGISTRY_PATH),
                    projected_rates_by_strategy=result.get("projected_rates_by_strategy", {}),
                    mode=mode,
                )
                print(summary)
                await send_telegram(summary)

    async def run_reconciliation_loop() -> None:
        while True:
            registry = PositionRegistry.load(POSITION_REGISTRY_PATH)
            result = await reconcile_registry_positions(registry=registry, closers=closers)
            if result.get("updated_leg_ids"):
                registry.save(POSITION_REGISTRY_PATH)
            if result.get("messages"):
                text = (
                    "倉位同步檢查\n"
                    + "\n".join(str(message) for message in result["messages"])
                    + f"\nmode={mode}"
                )
                print(text)
            await asyncio.sleep(RECONCILIATION_INTERVAL_SECONDS)

    await asyncio.gather(
        *((run_aster_mark_price_cache(mark_price_caches["aster"]),) if "aster" in venues else ()),
        *((run_hyperliquid_mids_cache(mark_price_caches["hyperliquid"]),) if "hyperliquid" in venues else ()),
        run_reconciliation_loop(),
        *(run_one_watcher(watcher) for watcher in watchers),
        *(run_one_margin_watcher(watcher) for watcher in margin_watchers),
        *((run_funding_risk_loop(),) if funding_config.enabled else ()),
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
