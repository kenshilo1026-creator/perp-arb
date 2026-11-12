"""Offline fixed-key order/cancel signatures against the existing Python adapter."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from hydra_basis.execution_engine.hyperliquid_adapter import _sign_l1_action


def vectors():
    rows = []
    for network in ('mainnet', 'testnet'):
        for tif in ('post_only', 'ioc'):
            for cancel in (False, True):
                order = dict(venue='hyperliquid', network=network, account='0x'+'0'*39+'1', symbol='ETH',
                             side='BUY' if tif == 'ioc' else 'SELL', quantity='0.012', limit_price='2345.6',
                             tif=tif, reduce_only=tif == 'ioc', client_id='0x'+'1'*32, asset=4)
                action = dict(type='cancelByCloid', cancels=[dict(asset=4, cloid=order['client_id'])]) if cancel else dict(
                    type='order', orders=[dict(a=4, b=order['side'] == 'BUY', p='2345.6', s='0.012', r=order['reduce_only'],
                    t=dict(limit=dict(tif='Alo' if tif == 'post_only' else 'Ioc')), c=order['client_id'])], grouping='na')
                nonce = 1700000000000 + len(rows)
                sig = _sign_l1_action('0'*63+'1', action, None, nonce, is_mainnet=network == 'mainnet')
                sig = dict(r=f"0x{int(sig['r'],16):064x}", s=f"0x{int(sig['s'],16):064x}", v=sig['v'])
                rows.append(dict(order=order, action=action, nonce=nonce, cancel=cancel, signature=sig))
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--write-fixtures', action='store_true')
    args = p.parse_args()
    fixture = ROOT/'rust/crates/hyperliquid-private/fixtures/trade-signing.json'
    expected = vectors()
    if args.write_fixtures:
        fixture.parent.mkdir(exist_ok=True)
        fixture.write_text(json.dumps(expected, indent=2)+'\n', encoding='utf-8')
    assert json.loads(fixture.read_text()) == expected
    report = dict(vectors=len(expected), status='passed', fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),
                  python_source_sha256=hashlib.sha256((ROOT/'hydra_basis/execution_engine/hyperliquid_adapter.py').read_bytes()).hexdigest())
    (ROOT/'rust/artifacts/hl-trade-signing-validation.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
