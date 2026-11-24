"""Deterministic Python/Rust decision replay. No network, credentials or orders."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import redirect_stdout
from decimal import Decimal as D
import io
import json
from pathlib import Path
import random
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from hydra_basis.execution_engine.depth_sizing import SizeRules, depth_clip
from hydra_basis.execution_engine.maker_price_guard import MakerPriceGuard, MakerPriceRecheck, adverse_price_gap
from hydra_basis.execution_engine.risk import compute_spread_pct


def book(row):
    return {**row, 'ts_ms': 0, 'bids': [(row['bid'], row['bid_size'])], 'asks': [(row['ask'], row['ask_size'])]}


async def reference(row):
    perp_venue = row.get('perp_venue', 'aster')
    rules = SizeRules(**{key: D(value) if value is not None else D('Infinity') for key, value in row['rules'].items()})
    books = {perp_venue: book(row['perp']), 'binance_spot': book(row['spot'])}
    sides = {perp_venue: 'SELL' if row['mode'] == 'open' else 'BUY',
             'binance_spot': 'BUY' if row['mode'] == 'open' else 'SELL'}
    stale = any(b['age_ms'] > 1000 for b in books.values())
    action = None
    if row['resting']:
        old = row['resting']
        maker = old['maker']
        taker = perp_venue if maker == 'binance_spot' else 'binance_spot'
        async def fetch():
            return books[maker], books[taker]
        async def tick():
            return '0.00001'
        guard = MakerPriceGuard(fetch_books=fetch, tick_size=tick, maker_side=sides[maker], taker_side=sides[taker],
                                max_gap=D(row['max_gap']), quantity=D(old['quantity']), follow_maker_quote=True,
                                top_level_only=True)
        if stale:
            action = 'cancel_for_quote'
        else:
            try:
                with redirect_stdout(io.StringIO()):
                    await guard.check(old['price'])
                action = 'keep'
            except MakerPriceRecheck as exc:
                action = 'cancel_for_gap' if str(exc).startswith('adverse maker/taker') else 'cancel_then_reprice'
    candidates = []
    if not stale:
        for maker in (perp_venue, 'binance_spot'):
            taker = perp_venue if maker == 'binance_spot' else 'binance_spot'
            try:
                q, p, h = depth_clip(remaining=D(row['remaining']), maker_book=books[maker], taker_book=books[taker],
                    maker_side=sides[maker], tick=D('.00001'), rules=rules, improve=False,
                    max_gap=D(row['max_gap']), wait_for_gap=True, top_level_only=True,
                    clip_limit=D(row['clip_limit']) if row['clip_limit'] else None)
            except ValueError as exc:
                if 'below current exchange minimum' in str(exc):
                    continue
                raise
            if q > 0:
                gap = adverse_price_gap(p, h, sides[maker])
                candidates.append((gap, compute_spread_pct(books[taker]), {
                    'maker': maker, 'maker_side': sides[maker], 'taker': taker, 'taker_side': sides[taker],
                    'quantity': str(q), 'maker_price': str(p), 'hedge_price': str(h), 'adverse_gap': str(gap)}))
    return {'candidate': min(candidates, key=lambda c: c[:2])[2] if candidates else None, 'resting_action': action}


def fixtures(count):
    randomizer = random.Random(20261123)
    for index in range(count):
        perp_venue = ('aster', 'hyperliquid', 'lighter')[index % 3]
        center = randomizer.choice([11000, 200000, 200000000])
        def quote():
            bid = D(center + randomizer.randint(-80, 80)) / 100000
            ask = bid + D(randomizer.randint(1, 80)) / 100000
            return dict(bid=str(bid), ask=str(ask), bid_size=str(randomizer.randint(1, 1800)),
                        ask_size=str(randomizer.randint(1, 1800)), age_ms=1001 if index % 79 == 0 else 0)
        spot, perp = quote(), quote()
        mode = 'open' if index % 2 else 'close'
        resting = None
        if index % 4:
            maker = perp_venue if index % 2 else 'binance_spot'
            side = 'BUY' if (maker == perp_venue) == (mode == 'close') else 'SELL'
            price = D((perp if maker == perp_venue else spot)['bid' if side == 'BUY' else 'ask'])
            resting = dict(maker=maker, price=str(price + D(randomizer.randint(-30, 30))/100000), quantity='100')
        yield dict(mode=mode, perp_venue=perp_venue, remaining=str(randomizer.randint(100, 3000)), clip_limit='200' if index % 7 == 0 else None,
                   max_gap=randomizer.choice(['0.0025', '0.002', '0.003']), spot=spot, perp=perp,
                   rules=dict(step='1', minimum='1', maximum='1000', min_notional='5', max_notional=None), resting=resting)


def equivalent(a, b):
    if a['resting_action'] != b['resting_action']:
        return False
    pa, pb = a['candidate'], b['candidate']
    if pa is None or pb is None:
        return pa is pb
    for key in pa:
        if key in ('maker', 'taker', 'maker_side', 'taker_side'):
            if pa[key] != pb[key]:
                return False
        elif key == 'adverse_gap':
            if abs(D(pa[key]) - D(pb[key])) > D('1e-25'):
                return False
        elif D(pa[key]) != D(pb[key]):
            return False
    return True


async def run(args):
    rows = list(fixtures(args.cases))
    expected, timings = [], []
    for row in rows:
        start = time.perf_counter_ns()
        expected.append(await reference(row))
        timings.append(time.perf_counter_ns() - start)
    binary = ROOT / 'rust/target/release/spot-perp.exe'
    result = subprocess.run([str(binary), 'evaluate'], input=''.join(json.dumps(r)+'\n' for r in rows),
                            capture_output=True, text=True, check=True, timeout=60)
    actual = [json.loads(line) for line in result.stdout.splitlines()]
    if len(actual) != len(rows):
        raise RuntimeError('Rust output count mismatch')
    mismatch = [i for i, (a, b) in enumerate(zip(expected, actual)) if not b['ok'] or not equivalent(a, b['decision'])]
    def summary(values):
        ordered = sorted(values)
        return {'n': len(values), 'p50_ns': statistics.median(values), 'p95_ns': ordered[int((len(values)-1)*.95)], 'max_ns': max(values)}
    output = {'cases': len(rows), 'mismatches': len(mismatch), 'actual_orders_sent': 0,
              'perp_venues': {venue: sum(r['perp_venue'] == venue for r in rows) for venue in ('aster', 'hyperliquid', 'lighter')},
              'measurement': 'offline decision only; not exchange execution latency; Python includes oracle scaffolding, Rust includes JSON decode',
              'python_reference': summary(timings), 'rust_parse_and_decide': summary([a['parse_and_decide_ns'] for a in actual]),
              'differences': [{'case': i, 'input': rows[i], 'python': expected[i], 'rust': actual[i]} for i in mismatch[:5]]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({k:v for k,v in output.items() if k != 'differences'}, indent=2))
    if mismatch:
        raise SystemExit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cases', type=int, default=2000)
    parser.add_argument('--output', type=Path, default=ROOT/'rust/artifacts/spot-perp-parity.json')
    options = parser.parse_args()
    if not 1 <= options.cases <= 100000:
        parser.error('cases must be 1..100000')
    asyncio.run(run(options))
