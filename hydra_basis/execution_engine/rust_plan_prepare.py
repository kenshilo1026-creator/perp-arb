"""Read-only signal-to-plan preparation. Never constructs a trading adapter."""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import uuid
from decimal import Decimal, InvalidOperation, localcontext, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

import aiohttp

from hydra_basis.adapters.base import fetch_json
from hydra_basis.adapters.variational import VARIATIONAL_BASE_URL
from hydra_basis.execution_engine.rust_selection import select_pair_for_args, select_legs_for_pair, print_selection


PREPARE_OPTIONS = ("rust_settings", "rust_quantity", "rust_clip_quantity", "rust_leverage", "rust_maker_price", "rust_hedge_price", "rust_fill_mode")


def positive(value: object, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise RuntimeError(f"{label} must be a positive decimal") from exc
    if not result.is_finite() or result <= 0:
        raise RuntimeError(f"{label} must be a positive finite decimal")
    return result


def build_plan(*, signal: dict, settings: dict, mode: str, quantity: str,
               maker_price: str, hedge_price: str | None, native_symbol: str,
               funding_interval_s: int, fill_mode: str | None = None,
               leverage: int | None = None) -> dict:
    if mode not in {"open", "close"}:
        raise RuntimeError("mode must be open or close")
    if leverage is not None:
        validate_leverage(leverage)
    venue = signal["long_venue"] if signal["short_venue"] == "variational" else signal["short_venue"]
    if venue not in {"aster", "hyperliquid", "lighter"} or "variational" not in {signal["short_venue"], signal["long_venue"]}:
        raise RuntimeError("unsupported paired venues")
    accounts = settings.get("accounts", {})
    for key in ("variational", venue):
        if key == 'lighter':
            value = str(accounts.get(key, ''))
            if not value.isascii() or not value.isdecimal() or str(int(value)) != value or not 0 <= int(value) < 2**48:
                raise RuntimeError('settings accounts.lighter must be the canonical LIGHTER_ACCOUNT_INDEX')
        elif not re.fullmatch(r"0x[0-9a-f]{40}", accounts.get(key, "")):
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
    market = venue == 'aster' and hedge_price is None
    if hedge_price is None and not market:
        raise RuntimeError('hedge IOC limit required')
    # MARKET keeps a valuation reference for preflight/lot/notional checks.
    # It is not a slippage limit and is never transmitted as price/timeInForce.
    hp = mp if market else positive(hedge_price, "hedge IOC limit")
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
        "hedge": {"venue": venue, "network": "mainnet", "account": str(accounts[venue]),
                  "symbol": native_symbol, "side": "BUY" if maker_side == "SELL" else "SELL",
                  "quantity": format(q, "f"), "limit_price": format(hp, "f"), "tif": "market" if market else "ioc",
                  "reduce_only": mode == "close", "max_notional_usd": hedge_cap, "wait_seconds": 20},
        "max_entry_gap": "0.0025", "maker_wait_seconds": 30, "recovery_seconds": 60,
        "maker_fill_mode": selected_mode,
        "hedge_leverage": leverage if mode == "open" else None,
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
        elif venue == 'lighter':
            data = await fetch_json(session, 'GET', 'https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails')
            rows = [r for r in data.get('order_book_details', []) if r.get('symbol') == symbol and r.get('market_type') == 'perp' and r.get('status') == 'active']
            if len(rows) != 1:
                raise RuntimeError(f'Lighter instrument missing or ambiguous: {symbol}')
            native_symbol = symbol
        else:
            universe = await fetch_hyperliquid_universe(session)
            if symbol not in universe:
                raise RuntimeError(f"Hyperliquid instrument not found: {symbol}")
            native_symbol = hyperliquid_coin_name(symbol)
    return native_symbol, interval


async def lighter_ioc_price(symbol: str, side: str, reference: str, slippage_bps: str = '50') -> str:
    """Preparation only: bound Lighter IOC at executable bid/ask plus the existing taker tolerance."""
    try:
        slip = Decimal(str(slippage_bps))
    except InvalidOperation as exc:
        raise RuntimeError('invalid Lighter IOC policy') from exc
    if not slip.is_finite() or not 0 <= slip <= 1000 or side not in ('BUY', 'SELL'):
        raise RuntimeError('invalid Lighter IOC policy')
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
        data = await fetch_json(session, 'GET', 'https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails')
    rows = [r for r in data.get('order_book_details', []) if r.get('symbol') == symbol and r.get('market_type') == 'perp' and r.get('status') == 'active']
    if len(rows) != 1:
        raise RuntimeError('Lighter IOC instrument unavailable')
    places = rows[0].get('supported_price_decimals')
    if type(places) is not int or not 0 <= places <= 12:
        raise RuntimeError('invalid Lighter price precision')
    tick = Decimal(1).scaleb(-places)
    with localcontext() as ctx:
        ctx.prec = 80
        price = positive(reference, 'Lighter executable reference') * (Decimal(1) + (slip if side == 'BUY' else -slip) / 10000)
        return format(price.quantize(tick, rounding=ROUND_CEILING if side == 'BUY' else ROUND_FLOOR), 'f')


def prompt(label: str) -> str:
    try:
        value = input(label + ": ").strip()
    except EOFError as exc:
        raise RuntimeError(f"stdin closed; specify {label} with CLI options") from exc
    if not value:
        raise RuntimeError(f"{label} cannot be empty")
    return value


def validate_leverage(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 125:
        raise RuntimeError("leverage_x must be an integer in 1..125; venue/instrument limits also apply")
    return value


def prepare_leverage(args, mode: str) -> int | None:
    """Explicit CLI or interactive input; closing never changes margin settings."""
    if mode == "close":
        return None
    value = getattr(args, "rust_leverage", None)
    if value is None:
        raw = prompt("leverage_x")
        if not re.fullmatch(r"[0-9]+", raw):
            raise RuntimeError("leverage_x must be an integer in 1..125")
        value = int(raw)
    return validate_leverage(value)


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
    if getattr(args, 'rust_action', None) == 'select' and any(
            getattr(args, key, None) for key in ('rust_settings', 'rust_clip_quantity', 'rust_leverage', 'rust_maker_price', 'rust_hedge_price', 'rust_fill_mode')):
        raise RuntimeError('select uses public books and execution priorities; omit settings/price/fill-mode execution options')
    symbol = (getattr(args, "ticker", None) or prompt("ticker")).strip().upper()
    mode = getattr(args, "mode", None) or "open"
    # A new funding signal cannot identify which historical position should be closed.
    if mode == "close" and not (getattr(args, "short_venue", None) and getattr(args, "long_venue", None)):
        raise RuntimeError("close requires --short_venue and --long_venue for existing positions; do not infer holdings from today's signal")
    signal = await select_pair_for_args(args, symbol=symbol, project_root=project_root)
    quantity = getattr(args, "rust_quantity", None) or prompt("quantity_token（本筆兩腿相同數量）")
    positive(quantity, 'quantity')
    report = await select_legs_for_pair(args, signal=signal, quantity=quantity, project_root=project_root)
    quiet = bool(getattr(args, 'rust_quiet', False))
    if not quiet:
        print_selection(report)
    selection = report['selection']
    if getattr(args, 'rust_action', None) == 'select' or not selection['paired_execution_supported']:
        path = Path(getattr(args, 'rust_plan', None) or project_root / f'rust/artifacts/selection-{uuid.uuid4().hex}.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x', encoding='utf-8') as stream:
            json.dump(report, stream, indent=2, ensure_ascii=False)
            stream.write('\n')
        print(f'唯讀選擇報告已保存：{path}；不是送單計劃。')
        if getattr(args, 'rust_action', None) != 'select':
            raise RuntimeError(f"selected pair has no Rust paired executor; selection report={path}; no executable plan/order created")
        return path
    venue = selection['taker_venue']
    settings_path = Path(getattr(args, "rust_settings", None) or project_root / "rust/artifacts/paired-settings.json")
    settings = json.loads(settings_path.read_text(encoding="utf-8-sig"))
    leverage = prepare_leverage(args, mode)
    args.rust_leverage = leverage
    if leverage is not None and not quiet:
        scope = venue if selection['maker_venue'] == 'variational' else '兩腿'
        print(f"leverage_x: {leverage}；送單前設定 {scope} 逐倉槓桿；數量不乘槓桿倍數。")
        if selection['maker_venue'] == 'variational':
            print("Variational 沿用帳戶保證金機制；此倍數只套用到對沖交易所。")
    if selection['maker_venue'] != 'variational':
        from hydra_basis.execution_engine.rust_native_pair_prepare import prepare_native_pair
        return await prepare_native_pair(args, report=report, settings=settings, project_root=project_root)
    if getattr(args, 'rust_clip_quantity', None) is not None:
        raise RuntimeError('rust-clip-quantity applies to native Aster/Hyperliquid/Lighter pairs only')
    annualized = "explicit" if signal['annualized_avg'] is None else f"{float(signal['annualized_avg']):.2%}"
    if not quiet:
        print(f"訊號 {signal['updated_at']}: {symbol} SHORT {signal['short_venue']} / LONG {signal['long_venue']}; 年化 {annualized}")
    market_hedge = venue == 'aster' and getattr(args, 'rust_hedge_price', None) is None
    if not quiet:
        print(f"模式 {mode}; Variational Maker / {venue} {'MARKET 市價' if market_hedge else 'IOC'} 對沖；本次只準備一筆計劃，不送單。")
    maker_price = getattr(args, "rust_maker_price", None)
    hedge_price = getattr(args, "rust_hedge_price", None)
    if hedge_price is None and venue == 'lighter':
        hedge_price = await lighter_ioc_price(symbol, selection['taker_side'], selection['taker_reference_price'],
                                            str(settings.get('native_pair', {}).get('hedge_slippage_bps', '50')))
        print(f'[lighter-ioc] 自動按 Taker 報價及商品精度建立 IOC 限價={hedge_price}')
    elif hedge_price is None and venue != 'aster':
        hedge_price = prompt(f"{venue} IOC 限價（BUY 最高買價／SELL 最低賣價）")
    if hedge_price is not None:
        positive(hedge_price, 'hedge price')
    if maker_price is not None:
        positive(maker_price, 'maker price')
    native_symbol, interval = await instrument_metadata(symbol, venue)
    if maker_price is None:
        maker_price = await preview_mid_price(project_root=project_root, symbol=symbol,
                                              extension_id=settings.get('extension_id'))
    plan = build_plan(signal=signal, settings=settings, mode=mode, quantity=quantity,
                      maker_price=maker_price, hedge_price=hedge_price, native_symbol=native_symbol,
                      funding_interval_s=interval, fill_mode=getattr(args, "rust_fill_mode", None), leverage=leverage)
    path = Path(getattr(args, "rust_plan", None) or project_root / f"rust/artifacts/{plan['run_id']}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        json.dump(plan, stream, indent=2)
        stream.write("\n")
    if not quiet:
        print(f"計劃已保存：{path}; maker_fill_mode={plan['maker_fill_mode']}; 接著執行 Rust 唯讀 preview。")
    from hydra_basis.execution_engine.rust_place_order import run_native_preflight
    await run_native_preflight(argparse.Namespace(rust_action="preview", rust_plan=str(path), rust_quiet=quiet))
    return path
