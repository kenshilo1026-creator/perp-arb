from __future__ import annotations

import argparse
import asyncio
import inspect
from decimal import Decimal

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

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
from hydra_basis.execution_engine.market_data import fetch_mexc_spot_orderbook, fetch_orderbook_snapshot
from hydra_basis.execution_engine.variational_browser import VariationalBrowserExecutionAdapter
from hydra_basis.execution_engine.variational_broker import VariationalCommandBrokerServer


SUPPORTED_VENUES = {"aster", "hyperliquid", "lighter", "mexc", "mexc_spot", "variational"}
SUPPORTED_ORDER_TYPES = {"market", "limit"}
SUPPORTED_SIDES = {"BUY", "SELL"}
LIVE_CONFIRMATION_PHRASE = "PLACE LIVE ORDER"


load_environment()


def normalize_order_args(
    *,
    venue: str,
    symbol: str,
    side: str,
    quantity: str,
    order_type: str,
) -> dict[str, str]:
    normalized = {
        "venue": venue.strip().lower(),
        "symbol": symbol.strip().upper(),
        "side": side.strip().upper(),
        "quantity": quantity.strip(),
        "order_type": order_type.strip().lower(),
    }
    if normalized["venue"] not in SUPPORTED_VENUES:
        raise RuntimeError(f"unsupported venue: {venue}")
    if normalized["side"] not in SUPPORTED_SIDES:
        raise RuntimeError(f"unsupported side: {side}")
    if normalized["order_type"] not in SUPPORTED_ORDER_TYPES:
        raise RuntimeError(f"unsupported order_type: {order_type}")
    if not normalized["symbol"]:
        raise RuntimeError("symbol cannot be empty")
    if not normalized["quantity"]:
        raise RuntimeError("quantity cannot be empty")
    return normalized


def single_order_method_name(order_type: str) -> str:
    if order_type == "market":
        return "place_market_order"
    if order_type == "limit":
        return "place_limit_order"
    raise RuntimeError(f"unsupported order_type: {order_type}")


def confirm_live_order(answer: str) -> bool:
    return answer == LIVE_CONFIRMATION_PHRASE


def format_cli_error(error: Exception) -> str:
    return f"下單失敗: {error}"


def decimal_to_plain(value: Decimal | float | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    plain = format(decimal_value.normalize(), "f")
    if "." in plain:
        plain = plain.rstrip("0").rstrip(".")
    return plain or "0"


def limit_price_from_orderbook(orderbook: dict[str, float | int], side: str) -> str:
    normalized_side = side.strip().upper()
    if normalized_side == "BUY":
        return decimal_to_plain(orderbook["bid"])
    if normalized_side == "SELL":
        return decimal_to_plain(orderbook["ask"])
    raise RuntimeError(f"unsupported side: {side}")


async def load_adapter_orderbook(adapter, symbol: str) -> dict[str, float | int]:
    loader = getattr(adapter, "get_orderbook", None)
    if loader is not None:
        result = loader(symbol)
        if inspect.isawaitable(result):
            return await result
        return result
    loader = getattr(adapter, "_load_orderbook", None)
    if loader is not None:
        result = loader(symbol)
        if inspect.isawaitable(result):
            return await result
        return result
    raise RuntimeError("limit order without --price requires adapter orderbook support")


async def load_default_limit_orderbook(
    *,
    venue: str,
    symbol: str,
    clip_usd: float,
    adapter=None,
) -> dict[str, float | int]:
    if adapter is not None:
        try:
            return await load_adapter_orderbook(adapter, symbol)
        except RuntimeError:
            pass
    async with aiohttp.ClientSession(headers={"User-Agent": "funding-arb-single-order/0.1"}) as session:
        if venue == "mexc_spot":
            return await fetch_mexc_spot_orderbook(session, symbol)
        return await fetch_orderbook_snapshot(
            session,
            venue=venue,
            symbol=symbol,
            clip_usd=clip_usd,
        )


def build_adapter_for_venue(venue: str):
    if venue == "aster":
        return AsterExecutionAdapter()
    if venue == "lighter":
        return LighterExecutionAdapter(
            signer_client_factory=build_lighter_client_factory_from_env(),
            market_config_loader=lambda symbol: fetch_lighter_market_config(symbol),
            orderbook_loader=lambda symbol: fetch_lighter_orderbook_live(symbol),
        )
    if venue == "mexc":
        return MexcExecutionAdapter()
    if venue == "mexc_spot":
        return MexcSpotExecutionAdapter()
    if venue == "variational":
        return VariationalBrowserExecutionAdapter()
    if venue == "hyperliquid":
        from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter

        return HyperliquidExecutionAdapter()
    raise RuntimeError(f"unsupported venue: {venue}")


def build_variational_adapter(broker_url: str) -> VariationalBrowserExecutionAdapter:
    return VariationalBrowserExecutionAdapter(broker_url=broker_url)


async def execute_single_order(
    *,
    venue: str,
    symbol: str,
    side: str,
    quantity: str,
    order_type: str,
    live: bool,
    price: str | None = None,
    clip_usd: float = 0.0,
    adapter_override=None,
) -> dict[str, object]:
    normalized = normalize_order_args(
        venue=venue,
        symbol=symbol,
        side=side,
        quantity=quantity,
        order_type=order_type,
    )
    adapter = adapter_override or build_adapter_for_venue(normalized["venue"]) if live else adapter_override
    resolved_price = price
    if normalized["order_type"] == "limit" and resolved_price is None and normalized["venue"] != "variational":
        orderbook = await load_default_limit_orderbook(
            venue=normalized["venue"],
            symbol=normalized["symbol"],
            clip_usd=clip_usd,
            adapter=adapter,
        )
        resolved_price = limit_price_from_orderbook(orderbook, normalized["side"])
    dry_run_payload = {
        "venue": normalized["venue"],
        "symbol": normalized["symbol"],
        "side": normalized["side"],
        "quantity": normalized["quantity"],
        "order_type": normalized["order_type"],
        "price": resolved_price,
        "live": live,
    }
    if not live:
        return {"ok": True, "dry_run": True, "request": dry_run_payload}

    try:
        method_name = single_order_method_name(normalized["order_type"])
        method = getattr(adapter, method_name)
        kwargs = {
            "symbol": normalized["symbol"],
            "side": normalized["side"],
            "amount": normalized["quantity"],
            "clip_usd": clip_usd,
        }
        if normalized["order_type"] == "limit":
            kwargs["price"] = resolved_price
        result = await method(**kwargs)
        return {"ok": True, "dry_run": False, "request": dry_run_payload, "result": result}
    finally:
        close = getattr(adapter, "close", None)
        if close is not None:
            result = close()
            if asyncio.iscoroutine(result):
                await result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test a single exchange order safely.")
    parser.add_argument("--venue", required=True, choices=sorted(SUPPORTED_VENUES))
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True, choices=sorted(SUPPORTED_SIDES))
    parser.add_argument("--quantity", required=True)
    parser.add_argument("--order-type", default="market", choices=sorted(SUPPORTED_ORDER_TYPES))
    parser.add_argument("--price", default=None)
    parser.add_argument("--clip-usd", type=float, default=0.0)
    parser.add_argument("--variational-broker-host", default="127.0.0.1")
    parser.add_argument("--variational-broker-port", type=int, default=8768)
    parser.add_argument("--variational-extension-timeout", type=float, default=30.0)
    parser.add_argument("--live", action="store_true", help="Actually place the order.")
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()
    normalized = normalize_order_args(
        venue=args.venue,
        symbol=args.symbol,
        side=args.side,
        quantity=args.quantity,
        order_type=args.order_type,
    )
    print("single order request")
    print(normalized)
    if args.price is not None:
        print({"price": args.price})

    if args.live:
        print("LIVE order requested.")
        print(f"Type exactly {LIVE_CONFIRMATION_PHRASE!r} to continue:")
        answer = input("> ")
        if not confirm_live_order(answer):
            print("cancelled")
            return
    else:
        print("dry-run only. Add --live to place a real order.")

    try:
        if args.live and normalized["venue"] == "variational":
            print(
                "starting embedded Variational broker. "
                "Open Variational page with the Chrome extension connected to "
                f"ws://{args.variational_broker_host}:{args.variational_broker_port}"
            )
            async with VariationalCommandBrokerServer(
                host=args.variational_broker_host,
                port=args.variational_broker_port,
            ) as server:
                print(
                    "waiting for Variational extension command client "
                    f"timeout={args.variational_extension_timeout:.1f}s"
                )
                await server.wait_for_extension(timeout_seconds=args.variational_extension_timeout)
                result = await execute_single_order(
                    venue=args.venue,
                    symbol=args.symbol,
                    side=args.side,
                    quantity=args.quantity,
                    order_type=args.order_type,
                    price=args.price,
                    clip_usd=args.clip_usd,
                    live=args.live,
                    adapter_override=build_variational_adapter(server.ws_url),
                )
        else:
            result = await execute_single_order(
                venue=args.venue,
                symbol=args.symbol,
                side=args.side,
                quantity=args.quantity,
                order_type=args.order_type,
                price=args.price,
                clip_usd=args.clip_usd,
                live=args.live,
            )
    except Exception as exc:
        print(format_cli_error(exc))
        raise
    print("single order result")
    print(result)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
