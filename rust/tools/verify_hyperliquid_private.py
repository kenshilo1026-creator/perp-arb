"""Offline parity against the real Python order-status adapter, no credentials/network."""
import asyncio
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
import sys

RUST = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RUST.parent))
from hydra_basis.execution_engine.hyperliquid_adapter import HyperliquidExecutionAdapter


def fixtures():
    rows = []
    for i in range(100):
        for remaining, status in [('10', 'open'), ('7.125', 'open'), ('0', 'filled'),
                                  ('5.25', 'canceled'), ('8', 'scheduledCancel'), ('10', 'oracleRejected')]:
            rows.append({'status': 'order', 'order': {'order': {
                'coin': 'ETH' if i % 2 else 'BTC', 'oid': 9007199254740993+i,
                'origSz': '10', 'sz': remaining}, 'status': status, 'statusTimestamp': i+1}})
    return rows + [{'status': 'unknownOid'}]


async def reference(frames):
    adapter = object.__new__(HyperliquidExecutionAdapter)
    adapter.dex = None
    rows = []
    for raw in frames:
        async def fetch(_oid):
            return raw
        adapter._get_order_status = fetch
        order = raw.get('order', {}).get('order', {})
        oid, symbol = order.get('oid', 1), order.get('coin', 'ETH')
        result = await adapter.get_order_execution(order_result={'oid': oid}, symbol=symbol)
        rows.append({'unknown': True} if result['status'] == 'UNKNOWN' else {
            'order_id': str(oid), 'quantity': result['filled_quantity'], 'terminal': result['terminal']})
    return rows


def compare(expected, actual):
    if len(expected) != len(actual):
        raise AssertionError('row count mismatch')
    for i, (a, b) in enumerate(zip(expected, actual)):
        if set(a) != set(b):
            raise AssertionError(f'row {i}: fields mismatch')
        for name, value in a.items():
            other = b[name]
            if name == 'quantity':
                if not isinstance(other, str) or not Decimal(other).is_finite():
                    raise AssertionError('invalid decimal quantity')
                value, other = Decimal(value), Decimal(other)
            if value != other:
                raise AssertionError(f'row {i}: {name} mismatch')


def main():
    binary = RUST / 'target/release/hyperliquid-private-replay.exe'
    if not binary.exists():
        binary = binary.with_suffix('')
    cases = fixtures()
    output = subprocess.run([str(binary)], input=json.dumps(cases), text=True,
                            capture_output=True, check=True, timeout=30)
    compare(asyncio.run(reference(cases)), json.loads(output.stdout))
    source = RUST.parent / 'hydra_basis/execution_engine/hyperliquid_adapter.py'
    report = dict(cases=len(cases), mismatches=0,
                  scope='Order-status quantity, order ID and terminal parity only. Trade snapshot dedup is tested separately in Rust; no live trading or PnL backtest.',
                  intentional_differences=['Preserve native symbol case; reject unknown status and inconsistent filled sizes. User fills never added to REST cumulative totals.'],
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  python_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    (RUST/'artifacts/hyperliquid-private-validation.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report,indent=2))


if __name__ == '__main__':
    main()
