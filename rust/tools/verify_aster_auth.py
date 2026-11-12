"""Offline Aster v3 EIP-712 vectors using the actual Python adapter and a public test key.

No environment credentials, account requests or order submission.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from urllib.parse import urlencode

RUST = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RUST.parent))
from eth_account import Account
from eth_utils import keccak
from hydra_basis.execution_engine.aster_adapter import AsterExecutionAdapter, encode_aster_v3_typed_data

FIXTURE = RUST / "crates/aster-private/fixtures/auth-vectors.json"
TEST_KEY = "0" * 63 + "1"  # Published deterministic test key, never fund it.
USER = "0x" + "0" * 39 + "1"


def vectors():
    signer = Account.from_key(TEST_KEY).address.lower()
    adapter = AsterExecutionAdapter(private_key=TEST_KEY, signer_address=signer, user_address=USER)
    rows = []
    for i, params in enumerate([{}, {"symbol": "ETHUSDT", "orderId": "9007199254740993"},
                               {"symbol": "币安人生USDT", "orderId": "2"},
                               {"symbol": "A B+%~/USDT", "orderId": "3"}]):
        nonce = 1794326400000000 + i
        adapter._nonce_us = lambda: nonce
        signed = adapter.build_signed_params(params)
        sig = signed.pop("signature")
        encoded = urlencode(signed)
        message = encode_aster_v3_typed_data(encoded)
        digest = keccak(b"\x19" + message.version + message.header + message.body).hex()
        rows.append(dict(params=[list(pair) for pair in params.items()], nonce=nonce, encoded=encoded,
                         signature="0x" + sig.removeprefix("0x"), digest=digest))
    return dict(test_key=TEST_KEY, user=USER, signer=signer, vectors=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Only for explicit fixture creation after review")
    args = parser.parse_args()
    current = vectors()
    if args.write:
        FIXTURE.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE.write_text(json.dumps(current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    saved = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if current != saved:
        raise SystemExit("Python Aster signing changed; review vectors instead of accepting drift")
    report = dict(status="passed", vectors=len(current["vectors"]),
                  scope="Python fixture validation; Rust tests compare exact digest/signature/wire encoding to this fixture. Public deterministic test key only.",
                  fixture_sha256=hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
                  python_source_sha256=hashlib.sha256((RUST.parent / "hydra_basis/execution_engine/aster_adapter.py").read_bytes()).hexdigest())
    (RUST / "artifacts/batch3b1-auth-validation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
