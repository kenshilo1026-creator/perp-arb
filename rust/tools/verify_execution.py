"""Offline residual-retry parity with the existing Python hedge safety function.

No trading or profitability simulation. Unknown submission, races, scope fencing
and full coordinator integration have separate Rust tests.
"""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
import sys

RUST = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RUST.parent))
from hydra_basis.execution_engine.hedge_safety import terminal_fill_quantity


def fixtures():
    cases = []
    for n in range(1, 101):
        qty = Decimal(n) / 10
        for status, filled in [('FILLED', qty), ('CANCELED', qty / 2),
                               ('EXPIRED', qty / 2), ('REJECTED', Decimal(0)),
                               ('NEW', Decimal(0)), ('PARTIALLY_FILLED', qty / 2)]:
            for gap in (False, True):
                cases.append(dict(quantity=str(qty), filled=str(filled), status=status, gap=gap))
    return cases


def reference(case):
    quantity, filled = Decimal(case['quantity']), Decimal(case['filled'])
    response = dict(status=case['status'], filled_quantity=case['filled'])
    if case['gap']:
        response['terminal'] = False
    terminal = terminal_fill_quantity(response)
    return dict(confirmed=filled, exposure=quantity-filled,
                retry=Decimal(0) if terminal is None else quantity-terminal)


def compare(cases, actual):
    if len(cases) != len(actual):
        raise AssertionError('row count mismatch')
    for i, (case, row) in enumerate(zip(cases, actual)):
        expected = reference(case)
        if set(row) != set(expected):
            raise AssertionError(f'row {i}: field mismatch')
        for name, value in row.items():
            if not isinstance(value, str) or not Decimal(value).is_finite() or Decimal(value) != expected[name]:
                raise AssertionError(f'row {i}: {name} mismatch')


def main():
    binary = RUST / 'target/release/execution-replay.exe'
    if not binary.exists():
        binary = binary.with_suffix('')
    cases = fixtures()
    result = subprocess.run([str(binary)], input=json.dumps(cases), text=True,
                            capture_output=True, check=True, timeout=60)
    compare(cases, json.loads(result.stdout))
    source = RUST.parent / 'hydra_basis/execution_engine/hedge_safety.py'
    report = dict(cases=len(cases), mismatches=0,
                  scope='Cumulative hedge quantity and terminal-only residual retry parity; no live trades or PnL backtest.',
                  python_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    (RUST / 'artifacts/execution-validation.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
