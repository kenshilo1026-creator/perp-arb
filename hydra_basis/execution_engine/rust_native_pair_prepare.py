"""Prepare native perpetual pairs. All submission stays in the Rust runner."""
import argparse
import json
import re
import uuid
from decimal import Decimal, localcontext
from pathlib import Path


def build_native_plan(args, *, report: dict, settings: dict) -> dict:
    from hydra_basis.execution_engine.rust_plan_prepare import positive
    from hydra_basis.adapters.hyperliquid import hyperliquid_coin_name
    from hydra_basis.symbol_mapping import venue_symbol
    if getattr(args, 'rust_maker_price', None) or getattr(args, 'rust_fill_mode', None):
        raise RuntimeError('native maker uses live bid/ask and native private WS; omit rust-maker-price/rust-fill-mode')
    selected, symbol = report['selection'], report['signal']['symbol']
    maker, hedge = selected['maker_venue'], selected['taker_venue']
    if maker == hedge or not {maker, hedge} <= {'aster', 'hyperliquid', 'lighter'}:
        raise RuntimeError('native paired transport requires two different Aster/Hyperliquid/Lighter venues')
    quantity = positive(report['quantity'], 'quantity')
    clip = positive(getattr(args, 'rust_clip_quantity', None) or report['quantity'], 'clip')
    if clip > quantity:
        raise RuntimeError('clip exceeds total quantity')
    accounts = settings.get('accounts', {})
    for v in (maker, hedge):
        if v == 'lighter':
            value = str(accounts.get(v, ''))
            if not value.isascii() or not value.isdecimal() or str(int(value)) != value or not 0 <= int(value) < 2**48:
                raise RuntimeError('settings accounts.lighter must be the canonical LIGHTER_ACCOUNT_INDEX')
        elif not re.fullmatch(r'0x[0-9a-f]{40}', accounts.get(v, '')):
            raise RuntimeError(f'settings need canonical account address for {v}')
    native_policy = settings.get('native_pair', {})
    leverage = getattr(args, 'rust_leverage', None)
    if leverage is None:
        leverage = native_policy.get('leverage', 1)
    if type(leverage) is not int or not 1 <= leverage <= 125:
        raise RuntimeError('native leverage must be an integer in 1..125')
    max_gap = str(native_policy.get('max_gap', '0.0025'))
    hp = getattr(args, 'rust_hedge_price', None)
    price_mode = 'fixed_ioc' if hp else ('market' if hedge == 'aster' else 'dynamic_ioc')
    maker_price = positive(selected['maker_price'], 'maker reference')
    hedge_price = positive(hp or selected['taker_reference_price'], 'hedge reference')
    def leg(v, side, price, tif):
        native = hyperliquid_coin_name(symbol) if v == 'hyperliquid' else venue_symbol(symbol, venue=v)
        if v == 'aster' and not native.endswith('USDT'):
            native += 'USDT'
        with localcontext() as ctx:
            ctx.prec = 80
            cap = quantity * price * Decimal('1.1')  # New maker exposure cap, not a hedge slippage promise.
        return dict(venue=v, network='mainnet', account=str(accounts[v]), symbol=native, side=side,
                    quantity=format(quantity, 'f'), limit_price=format(price, 'f'), tif=tif,
                    reduce_only=report['mode'] == 'close', max_notional_usd=format(cap, 'f'), wait_seconds=20)
    return dict(schema='native_perp_pair_v1', run_id='native-pair-' + uuid.uuid4().hex, symbol=symbol,
                maker=leg(maker, selected['maker_side'], maker_price, 'post_only'),
                hedge=leg(hedge, selected['taker_side'], hedge_price, 'market' if price_mode == 'market' else 'ioc'),
                clip_quantity=format(clip, 'f'), max_gap=max_gap, hedge_price_mode=price_mode,
                hedge_slippage_bps=str(native_policy.get('hedge_slippage_bps', '50')),
                maker_wait_seconds=native_policy.get('maker_wait_seconds', 30),
                recovery_seconds=native_policy.get('recovery_seconds', 60),
                max_reprices=native_policy.get('max_reprices', 1000),
                leverage=leverage)


async def prepare_native_pair(args, *, report: dict, settings: dict, project_root: Path) -> Path:
    plan = build_native_plan(args, report=report, settings=settings)
    path = Path(getattr(args, 'rust_plan', None) or project_root / f"rust/artifacts/{plan['run_id']}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as stream:
        json.dump(plan, stream, indent=2)
        stream.write('\n')
    quiet = bool(getattr(args, 'rust_quiet', False))
    if not quiet:
        print(f"[Rust 原生雙腿] Maker={plan['maker']['venue']} {plan['maker']['side']} 最新 bid/ask；"
              f"Taker={plan['hedge']['venue']} {plan['hedge']['side']} {plan['hedge_price_mode']}")
        print(f"總量={plan['maker']['quantity']} 每批={plan['clip_quantity']}；計劃={path}；本次只做公開資料 preview。")
    from hydra_basis.execution_engine.rust_place_order import run_native_preflight
    await run_native_preflight(argparse.Namespace(rust_action='preview', rust_plan=str(path), rust_quiet=quiet))
    return path
