"""Interactive Rust prepare/confirm/submit with selected-symbol checks."""
from __future__ import annotations
import argparse
from decimal import Decimal
import json
from pathlib import Path
import uuid
from hydra_basis.execution_engine.rust_plan_prepare import prepare_signal_plan, prompt
from hydra_basis.execution_engine.rust_place_order import run_native_preflight


def show_review(plan: dict) -> None:
    native = plan.get('schema') == 'native_perp_pair_v1'
    maker, hedge = plan['maker'], plan['hedge']
    print(f"模式：{'平倉 reduce-only' if maker['reduce_only'] else '開倉'}")
    for role, leg, venue in (('Maker', maker, maker.get('venue', 'variational')),
                             ('Taker', hedge, hedge['venue'])):
        price = '最新 bid/ask' if native and role == 'Maker' else leg['limit_price']
        if role == 'Taker' and leg['tif'] == 'market':
            price = 'MARKET 市價'
        elif role == 'Taker' and native and plan.get('hedge_price_mode') == 'dynamic_ioc':
            price = f"即時 IOC，容許偏移 {plan['hedge_slippage_bps']} bps"
        print(f"{role}：{venue} {leg['symbol']} {leg['side']} 數量={leg['quantity']} 價格={price}")
    if maker['reduce_only']:
        print('槓桿：沿用現有設定，平倉不修改')
    else:
        leverage = plan['leverage'] if native else plan['hedge_leverage']
        print(f"槓桿：{leverage}x（{'兩腿' if native else hedge['venue'] + ' 對沖腿'}逐倉）")
    gap_pct = Decimal(plan['max_gap'] if native else plan['max_entry_gap']) * 100
    print(f"入場不利價差上限：{gap_pct.normalize():f}%")


async def run_interactive_order(args, *, project_root: Path) -> None:
    if any(getattr(args, key, None) for key in (
            'live', 'rust_preflight', 'rust_inventory', 'rust_account', 'rust_order_id',
            'rust_output', 'extension_id', 'rust_baseline', 'rust_baseline_output', 'rust_run_dir', 'rust_action')):
        raise RuntimeError('互動下單不接受既有 run／baseline／--live；進階操作請明確指定 --rust-action。')
    prepared = argparse.Namespace(**vars(args))
    mode = getattr(args, 'mode', None) or prompt('mode [open/close/開倉/平倉]')
    modes = {'open': 'open', '開倉': 'open', '开仓': 'open', 'close': 'close', '平倉': 'close', '平仓': 'close'}
    if mode.strip().lower() not in modes:
        raise RuntimeError('mode must be open/close or 開倉/平倉')
    prepared.mode = modes[mode.strip().lower()]
    prepared.rust_quiet = True
    session = project_root / 'rust/artifacts' / ('order-' + uuid.uuid4().hex)
    session.mkdir(parents=True, exist_ok=False)
    prepared.rust_action = 'prepare'
    prepared.rust_plan = getattr(args, 'rust_plan', None) or str(session / 'plan.json')
    plan_path = await prepare_signal_plan(prepared, project_root=project_root)
    plan_bytes = plan_path.read_bytes()
    plan = json.loads(plan_bytes)
    run = session / 'run'
    show_review(plan)
    if plan.get('schema') != 'native_perp_pair_v1':
        print('請保持 Variational extension 連線，勾選「允許 Rust 操作頁面下單／撤單」並按 Start。')
    try:
        answer = input('立即真實送單？[y/N]: ').strip().lower()
    except EOFError:
        answer = ''
    if answer not in ('y', 'yes'):
        print('[Rust] 已取消，沒有送單。')
        return
    if plan_path.read_bytes() != plan_bytes:
        raise RuntimeError('確認期間計劃已改變，請重新執行；未送單。')
    try:
        await run_native_preflight(argparse.Namespace(rust_action='submit', rust_plan=str(plan_path),
            rust_run_dir=str(run), live=True, rust_quiet=True))
    except BaseException:
        if run.exists():
            print(f'[Rust] 執行未完成；保留紀錄並使用同一 run 恢復：python scripts/place_order.py --engine rust --rust-action recover --rust-run-dir "{run}" --live')
        raise

