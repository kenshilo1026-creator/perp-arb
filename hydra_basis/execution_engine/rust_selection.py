"""Public-data input for Rust selection. All ranking and leg decisions run in Rust.

This is preparation, outside the live fill-to-hedge path. It never builds an
execution adapter, starts an extension, or sends an order.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path

import aiohttp

from hydra_basis.config import EXECUTION_VENUES_PATH, MONITOR_SIGNALS_PATH, VENUE_CONFIG
from hydra_basis.execution_engine.market_data import fetch_orderbook_snapshot
from hydra_basis.execution_engine.priority import load_execution_priorities


def selector_binary(project_root: Path) -> Path:
    return project_root / 'rust/target/release/paired-order.exe'


def decimal_text(value: object) -> str:
    """JSON floats may use scientific notation; Rust's wire decimals are fixed."""
    try:
        number = Decimal(str(value))
        return format(number, 'f') if number.is_finite() else str(value)
    except InvalidOperation:
        return str(value)  # Native validation decides whether to reject/skip it.


async def native_select(action: str, payload: dict, *, project_root: Path) -> dict:
    if action not in {'select-pair', 'select-legs'}:
        raise RuntimeError('invalid read-only selection action')
    binary = selector_binary(project_root)
    if not binary.is_file():
        raise RuntimeError('Build first: cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin paired-order --release --locked')
    process = await asyncio.create_subprocess_exec(
        str(binary), action, cwd=project_root, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(json.dumps(payload, allow_nan=False).encode('utf-8')), timeout=15)
    except asyncio.TimeoutError as exc:
        raise RuntimeError('Rust selection timed out; no plan/order created') from exc
    finally:
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            await process.wait()
    if process.returncode != 0:
        raise RuntimeError(stderr.decode('utf-8', errors='replace').strip()[:1500] or 'Rust selection failed')
    try:
        response = json.loads(stdout)
        if response['mode'] != action or response['can_submit_orders'] is not False:
            raise ValueError('unexpected selection response')
        result = response['selection']
        if not isinstance(result, dict):
            raise ValueError('selection must be an object')
        return result
    except (ValueError, KeyError, TypeError) as exc:
        raise RuntimeError('invalid Rust selection response; no plan/order created') from exc


def pair_input(args, *, symbol: str, project_root: Path, now: dt.datetime | None = None) -> tuple[dict, str]:
    now = now or dt.datetime.now(dt.timezone.utc)
    short, long = getattr(args, 'short_venue', None), getattr(args, 'long_venue', None)
    mode = getattr(args, 'mode', None) or 'open'
    payload = {'symbol': symbol, 'mode': mode, 'short_venue': short, 'long_venue': long,
               'enabled_venues': [v for v, config in VENUE_CONFIG.items() if config.enabled],
               'updated_ms': None, 'now_ms': int(now.timestamp() * 1000), 'signals': []}
    # Rust validates explicit pairs/close direction; neither needs today's signals.
    if short and long or mode == 'close':
        return payload, 'explicit'
    data = json.loads((project_root / MONITOR_SIGNALS_PATH).read_text(encoding='utf-8-sig'))
    updated_text = data.get('updated_at', '')
    try:
        updated = dt.datetime.fromisoformat(updated_text)
        if updated.tzinfo is not None and updated.timestamp() >= 0:
            payload['updated_ms'] = int(updated.timestamp() * 1000)
    except (ValueError, TypeError, OverflowError):
        pass  # Rust reports missing/invalid timestamp, never substitutes receipt time.
    for row in data.get('cross_exchange_signals', []):
        if not isinstance(row, dict):
            continue
        payload['signals'].append({
            'symbol': str(row.get('symbol', '')), 'short_venue': str(row.get('short_venue', '')),
            'long_venue': str(row.get('long_venue', '')), 'annualized_avg': decimal_text(row.get('annualized_avg', '')),
            'signal': row.get('signal') is True})
    return payload, updated_text


async def select_pair_for_args(args, *, symbol: str, project_root: Path) -> dict:
    payload, updated = pair_input(args, symbol=symbol, project_root=project_root)
    selected = await native_select('select-pair', payload, project_root=project_root)
    return {**selected, 'updated_at': updated}


async def select_legs_for_pair(args, *, signal: dict, quantity: str, project_root: Path) -> dict:
    venues = [signal['short_venue'], signal['long_venue']]
    priorities = load_execution_priorities(project_root / EXECUTION_VENUES_PATH)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async def books(clip_usd):
            snapshots = await asyncio.gather(*(fetch_orderbook_snapshot(
                session, venue=v, symbol=signal['symbol'], clip_usd=clip_usd) for v in venues))
            # These are snapshots only. Live submission still requires live guards.
            return {v: {'bid': decimal_text(b['bid']), 'ask': decimal_text(b['ask'])} for v, b in zip(venues, snapshots)}
        quotes = await books(1000.0)
        if 'variational' in venues:
            # Match Python's quantity-aware RFQ preview: first estimate notional,
            # then request the actual clip. Never reuse a $1000 RFQ for a tiny order.
            mids = [(Decimal(b['bid']) + Decimal(b['ask'])) / 2 for b in quotes.values()]
            if any(not m.is_finite() or m <= 0 for m in mids):
                raise RuntimeError('invalid orderbook mid')
            clip = Decimal(quantity) * sum(mids) / 2
            quotes = await books(float(clip))
    result = await native_select('select-legs', {
        'short_venue': signal['short_venue'], 'long_venue': signal['long_venue'],
        'mode': getattr(args, 'mode', None) or 'open', 'priorities': priorities, 'books': quotes,
        'maker_venue': getattr(args, 'maker_venue', None), 'taker_venue': getattr(args, 'taker_venue', None),
    }, project_root=project_root)
    return {'schema': 'rust_execution_selection_v1', 'signal': signal, 'quantity': quantity,
            'mode': getattr(args, 'mode', None) or 'open', 'books': quotes,
            'selection': result, 'can_submit_orders': False}


def print_selection(report: dict) -> None:
    signal, legs = report['signal'], report['selection']
    print(f"[Rust 選擇] {signal['symbol']} SHORT {signal['short_venue']} / LONG {signal['long_venue']}")
    print(f"Maker: {legs['maker_venue']} {legs['maker_side']}；Taker: {legs['taker_venue']} {legs['taker_side']}；原因={legs['reason']}")
    for venue, spread in legs['spread_by_venue'].items():
        print(f"{venue} spread={Decimal(spread):.4%}")
    print(f"Maker 參考價={legs['maker_price'] or 'Variational Mid 按鈕'}；Taker 盤口參考={legs['taker_reference_price']}")
    if legs['requires_spread_confirmation']:
        print('提示：至少一方 spread > 0.1%；此選擇預覽未授權送單。')
    print('選擇規則：Python spread＋設定優先級；未計算動態手續費。')
    if not legs['paired_execution_supported']:
        print('此配對的 Rust 雙腿執行器尚未接上；選擇結果僅供預覽，不能 submit。')
