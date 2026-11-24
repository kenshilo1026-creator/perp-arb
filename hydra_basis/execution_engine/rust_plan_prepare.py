"""Read-only signal-to-plan preparation. Never constructs a trading adapter."""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import re
import uuid
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

import aiohttp

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.variational import VARIATIONAL_BASE_URL
from hydra_basis.config import MONITOR_SIGNALS_PATH


PREPARE_OPTIONS = ("rust_settings", "rust_quantity", "rust_maker_price", "rust_hedge_price", "rust_fill_mode")


def positive(value: object, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise RuntimeError(f"{label} must be a positive decimal") from exc
    if not result.is_finite() or result <= 0:
        raise RuntimeError(f"{label} must be a positive finite decimal")
    return result


def select_signal(path: Path, symbol: str, *, short: str | None = None,
                  long: str | None = None, now: dt.datetime | None = None) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    updated = dt.datetime.fromisoformat(payload["updated_at"])
    now = now or dt.datetime.now(dt.timezone.utc)
    if updated.tzinfo is None or not dt.timedelta(0) <= now - updated <= dt.timedelta(hours=24):
        raise RuntimeError("monitor_signals timestamp invalid or older than 24 hours; run funding monitor first")
    candidates = []
    for row in payload.get("cross_exchange_signals", []):
        s, l = str(row.get("short_venue", "")).lower(), str(row.get("long_venue", "")).lower()
        if str(row.get("symbol", "")).upper() != symbol.upper():
            continue
        if {s, l} not in ({"variational", "aster"}, {"variational", "hyperliquid"}):
            continue
        if (short and s != short.lower()) or (long and l != long.lower()):
            continue
        # Only an affirmative funding signal may select an automatic trade direction.
        if row.get("signal") is not True:
            continue
        try:
            annualized = Decimal(str(row["annualized_avg"]))
        except (KeyError, InvalidOperation):
            continue
        if not annualized.is_finite():
            continue
        candidates.append((annualized, {**row, "symbol": symbol.upper(), "short_venue": s,
                                       "long_venue": l, "updated_at": payload["updated_at"]}))
    if not candidates:
        raise RuntimeError(f"no eligible Variational + Aster/Hyperliquid signal for {symbol}; no plan/order created")
    return max(candidates, key=lambda pair: pair[0])[1]


def build_plan(*, signal: dict, settings: dict, mode: str, quantity: str,
               maker_price: str, hedge_price: str, native_symbol: str,
               funding_interval_s: int, fill_mode: str | None = None) -> dict:
    if mode not in {"open", "close"}:
        raise RuntimeError("mode must be open or close")
    venue = signal["long_venue"] if signal["short_venue"] == "variational" else signal["short_venue"]
    if venue not in {"aster", "hyperliquid"} or "variational" not in {signal["short_venue"], signal["long_venue"]}:
        raise RuntimeError("unsupported paired venues")
    accounts = settings.get("accounts", {})
    for key in ("variational", venue):
        if not re.fullmatch(r"0x[0-9a-f]{40}", accounts.get(key, "")):
            raise RuntimeError(f"settings need canonical account address for {key}")
    if not re.fullmatch(r"[a-p]{32}", settings.get("extension_id", "")):
        raise RuntimeError("settings need Chrome extension_id")
    selected_mode = fill_mode or settings.get("maker_fill_mode", "rest_verified")
    if selected_mode not in {"rest_verified", "ws_confirmed"}:
        raise RuntimeError("invalid maker_fill_mode")
    if type(funding_interval_s) is not int or not 1 <= funding_interval_s <= 86400:
        raise RuntimeError("invalid Variational funding interval")
    q = positive(quantity, "quantity")
    mp = positive(maker_price, "maker limit")
    hp = positive(hedge_price, "hedge IOC limit")
    maker_side = "SELL" if signal["short_venue"] == "variational" else "BUY"
    if mode == "close":
        maker_side = "BUY" if maker_side == "SELL" else "SELL"
    run_id = "ws-test-" + uuid.uuid4().hex
    with localcontext() as ctx:
        ctx.prec = 80
        maker_cap, hedge_cap = format(q * mp, "f"), format(q * hp, "f")
    return {
        "run_id": run_id, "extension_id": settings["extension_id"],
        "maker": {"request_id": run_id, "account": accounts["variational"],
                  "symbol": signal["symbol"], "side": maker_side, "quantity": format(q, "f"),
                  "limit_price": format(mp, "f"), "reduce_only": mode == "close", "max_notional": maker_cap},
        "instrument": {"underlying": signal["symbol"], "instrument_type": "perpetual_future",
                       "settlement_asset": "USDC", "funding_interval_s": funding_interval_s},
        "hedge": {"venue": venue, "network": "mainnet", "account": accounts[venue],
                  "symbol": native_symbol, "side": "BUY" if maker_side == "SELL" else "SELL",
                  "quantity": format(q, "f"), "limit_price": format(hp, "f"), "tif": "ioc",
                  "reduce_only": mode == "close", "max_notional_usd": hedge_cap, "wait_seconds": 20},
        "max_entry_gap": "0.0025", "maker_wait_seconds": 30, "recovery_seconds": 60,
        "maker_fill_mode": selected_mode,
    }


async def instrument_metadata(symbol: str, venue: str) -> tuple[str, int]:
    from hydra_basis.adapters.aster import fetch_aster_symbol_metadata
    from hydra_basis.adapters.hyperliquid import fetch_hyperliquid_universe, hyperliquid_coin_name
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        stats = await fetch_json(session, "GET", f"{VARIATIONAL_BASE_URL}/metadata/stats")
        listing = next((x for x in stats.get("listings", []) if x.get("ticker", "").upper() == symbol), None)
        if listing is None:
            raise RuntimeError(f"Variational instrument not found: {symbol}")
        interval = listing.get("funding_interval_s")
        if type(interval) is not int or not 1 <= interval <= 86400:
            raise RuntimeError("missing/invalid Variational funding interval; no guessed default")
        if venue == "aster":
            metadata = await fetch_aster_symbol_metadata(session)
            if symbol not in metadata:
                raise RuntimeError(f"Aster instrument not found: {symbol}")
            native_symbol = metadata[symbol]["raw_symbol"]
        else:
            universe = await fetch_hyperliquid_universe(session)
            if symbol not in universe:
                raise RuntimeError(f"Hyperliquid instrument not found: {symbol}")
            native_symbol = hyperliquid_coin_name(symbol)
    return native_symbol, interval


def prompt(label: str) -> str:
    try:
        value = input(label + ": ").strip()
    except EOFError as exc:
        raise RuntimeError(f"stdin closed; specify {label} with CLI options") from exc
    if not value:
        raise RuntimeError(f"{label} cannot be empty")
    return value


async def preview_mid_price(*, project_root: Path, symbol: str, extension_id: str) -> str:
    """Use native read-only transport to click the page's Mid button, not a guessed mid."""
    if not re.fullmatch(r'[a-p]{32}', extension_id or ''):
        raise RuntimeError('settings need Chrome extension_id for Mid preview')
    if not re.fullmatch(r'[A-Z0-9]{1,32}', symbol):
        raise RuntimeError('invalid Variational symbol for Mid preview')
    binary = project_root / 'rust/target/release/paired-order.exe'
    if not binary.is_file():
        raise RuntimeError('Build first: cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin paired-order --release --locked')
    print(f'[variational-mid] 請讓 extension 連著 {symbol} 頁面並按 Start；正在自動按 Limit／Mid 讀價，不送單。', flush=True)
    process = await asyncio.create_subprocess_exec(
        str(binary), 'mid-price', '--extension-id', extension_id, '--symbol', symbol,
        cwd=project_root, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=70)
    except asyncio.TimeoutError as exc:
        raise RuntimeError('Variational Mid 預覽逾時；確認幣種頁面與 extension Start 後重試，未建立計劃或送單。') from exc
    finally:
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            await process.wait()
    if process.returncode != 0:
        detail = stderr.decode('utf-8', errors='replace').strip()[:1000]
        raise RuntimeError(f'Variational Mid 預覽失敗；未建立計劃或送單。請確認 extension、幣種頁面及 Rust release 已更新。{detail}')
    try:
        result = json.loads(stdout)
        if (result['mode'] != 'mid-price' or result['symbol'] != symbol
                or result['source'] != 'variational_page_mid_button' or result['can_submit_orders'] is not False):
            raise ValueError('unexpected preview result')
        price = positive(result['price'], 'Variational Mid price')
    except (ValueError, KeyError, TypeError) as exc:
        raise RuntimeError('invalid Rust Mid preview response; no plan/order created') from exc
    print(f'[variational-mid] {symbol} Maker 限價={price}', flush=True)
    return format(price, 'f')


async def prepare_signal_plan(args, *, project_root: Path) -> Path:
    if any(getattr(args, key, None) for key in ("live", "rust_preflight", "rust_inventory",
            "rust_account", "rust_order_id", "rust_output", "extension_id", "rust_baseline",
            "rust_baseline_output", "rust_run_dir")):
        raise RuntimeError("prepare is read-only; omit live/preflight/baseline/run options")
    symbol = (getattr(args, "ticker", None) or prompt("ticker")).strip().upper()
    mode = getattr(args, "mode", None) or "open"
    # A new funding signal cannot identify which historical position should be closed.
    if mode == "close" and not (getattr(args, "short_venue", None) and getattr(args, "long_venue", None)):
        raise RuntimeError("close requires --short_venue and --long_venue for existing positions; do not infer holdings from today's signal")
    short, long = getattr(args, "short_venue", None), getattr(args, "long_venue", None)
    if short and long:
        signal = {"symbol": symbol, "short_venue": short.lower(), "long_venue": long.lower(),
                  "updated_at": "explicit", "annualized_avg": None}
        if {signal["short_venue"], signal["long_venue"]} not in ({"variational", "aster"}, {"variational", "hyperliquid"}):
            raise RuntimeError("explicit pair must be Variational + Aster/Hyperliquid")
    else:
        signal = select_signal(project_root / MONITOR_SIGNALS_PATH, symbol, short=short, long=long)
    venue = signal["long_venue"] if signal["short_venue"] == "variational" else signal["short_venue"]
    if getattr(args, "maker_venue", None) not in (None, "variational") or getattr(args, "taker_venue", None) not in (None, venue):
        raise RuntimeError("Rust paired executor requires Variational maker and selected native venue taker")
    settings_path = Path(getattr(args, "rust_settings", None) or project_root / "rust/artifacts/paired-settings.json")
    settings = json.loads(settings_path.read_text(encoding="utf-8-sig"))
    annualized = "explicit" if signal['annualized_avg'] is None else f"{float(signal['annualized_avg']):.2%}"
    print(f"訊號 {signal['updated_at']}: {symbol} SHORT {signal['short_venue']} / LONG {signal['long_venue']}; 年化 {annualized}")
    print(f"模式 {mode}; Variational Maker / {venue} IOC 對沖；本次只準備一筆計劃，不送單。")
    quantity = getattr(args, "rust_quantity", None) or prompt("quantity_token（本筆兩腿相同數量）")
    maker_price = getattr(args, "rust_maker_price", None)
    hedge_price = getattr(args, "rust_hedge_price", None) or prompt(f"{venue} IOC 限價（BUY 最高買價／SELL 最低賣價）")
    for value, name in ((quantity, "quantity"), (hedge_price, "hedge price")):
        positive(value, name)
    if maker_price is not None:
        positive(maker_price, 'maker price')
    native_symbol, interval = await instrument_metadata(symbol, venue)
    if maker_price is None:
        maker_price = await preview_mid_price(project_root=project_root, symbol=symbol,
                                              extension_id=settings.get('extension_id'))
    plan = build_plan(signal=signal, settings=settings, mode=mode, quantity=quantity,
                      maker_price=maker_price, hedge_price=hedge_price, native_symbol=native_symbol,
                      funding_interval_s=interval, fill_mode=getattr(args, "rust_fill_mode", None))
    path = Path(getattr(args, "rust_plan", None) or project_root / f"rust/artifacts/{plan['run_id']}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        json.dump(plan, stream, indent=2)
        stream.write("\n")
    print(f"計劃已保存：{path}; maker_fill_mode={plan['maker_fill_mode']}; 接著執行 Rust 唯讀 preview。")
    from hydra_basis.execution_engine.rust_place_order import run_native_preflight
    await run_native_preflight(argparse.Namespace(rust_action="preview", rust_plan=str(path)))
    return path
