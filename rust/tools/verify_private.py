"""Compare Aster cumulative event handling with the actual Python OrderEventStream.

This is offline event replay, not a fill latency or profitability backtest.
Rust's intentional handling of older timestamps with higher fills is checked
separately rather than counted as Python parity.
"""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
import sys

RUST = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RUST.parent))
from hydra_basis.execution_engine.order_stream import OrderEventStream, decode_aster_order


def frame(order, qty, status, stamp, symbol="ETHUSDT"):
    return {"e": "ORDER_TRADE_UPDATE", "E": stamp, "o": {
        "s": symbol, "i": order, "q": "10", "z": qty, "X": status}}


def fixtures():
    frames = []
    for index in range(100):
        for qty, status, stamp in [
            ("0", "NEW", 1), ("2", "PARTIALLY_FILLED", 2),
            ("2", "PARTIALLY_FILLED", 2), ("1", "PARTIALLY_FILLED", 1),
            ("4.125", "PARTIALLY_FILLED", 3), ("4.125", "CANCELED", 4),
            ("4.125", "PARTIALLY_FILLED", 3), ("10", "FILLED", 5), ("0", "NEW", 0),
        ]:
            frames.append(frame(index + 1, qty, status, stamp + index * 100,
                                "ETHUSDT" if index % 2 == 0 else "BTCUSDT"))
    frames.append({"e": "ACCOUNT_UPDATE"})
    return frames


def reference(frames):
    stream = OrderEventStream(venue="aster", connect=None, renew=None, decode=decode_aster_order)
    accounted, rows = {}, []
    for raw in frames:
        event = decode_aster_order(raw)
        if event is None:
            rows.append({"ignored": True})
            continue
        stream.publish(event)
        key = event["symbol"], str(event["orderId"])
        snapshot = stream.cached_update(symbol=key[0], order_id=key[1])
        qty = Decimal(snapshot["executedQty"])
        delta = qty - accounted.get(key, Decimal(0))
        accounted[key] = qty
        rows.append(dict(symbol=key[0], order_id=key[1], quantity=str(qty),
                         status=snapshot["status"], delta=str(delta)))
    return rows


def compare(expected, actual):
    if len(expected) != len(actual):
        raise AssertionError("Replay output length mismatch")
    for index, (left, right) in enumerate(zip(expected, actual)):
        if set(left) != set(right):
            raise AssertionError(f"Row {index}: fields mismatch")
        for name in left:
            a, b = left[name], right[name]
            if name in {"quantity", "delta"}:
                if not isinstance(b, str) or not Decimal(b).is_finite():
                    raise AssertionError(f"Row {index}: invalid decimal")
                a, b = Decimal(a), Decimal(b)
            if a != b:
                raise AssertionError(f"Row {index}: {name} mismatch")


def run(binary, frames):
    result = subprocess.run([str(binary)], input=json.dumps(frames), text=True,
                            capture_output=True, check=True, timeout=30)
    return json.loads(result.stdout)


def main():
    binary = RUST / "target/release/private-replay.exe"
    if not binary.exists():
        binary = RUST / "target/release/private-replay"
    frames = fixtures()
    compare(reference(frames), run(binary, frames))
    differences = []
    for status in ["PARTIALLY_FILLED", "CANCELED"]:
        case = [frame(1, "2", status, 200), frame(1, "4", "PARTIALLY_FILLED", 100)]
        py, rs = reference(case), run(binary, case)
        assert Decimal(py[-1]["quantity"]) == Decimal(2)
        assert Decimal(rs[-1]["quantity"]) == Decimal(4)
        assert Decimal(rs[-1]["delta"]) == Decimal(2)
        assert rs[-1]["status"] == status
        differences.append(dict(case="older_timestamp_more_fills_after_" + status.lower(),
                                python_quantity=py[-1]["quantity"], rust_quantity=rs[-1]["quantity"],
                                reason="Preserve newly observed fills; never hide exposure due to timestamp order."))
    summary = dict(batch="private_events_regression", common_events=len(frames), mismatches=0,
                   intentional_differences=differences,
                   scope="Offline Aster cumulative event normalization and coordination only; no live private WS or order submission.",
                   binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                   python_sources={str(p.relative_to(RUST.parent)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in [RUST.parent / "hydra_basis/execution_engine/order_stream.py",
                                             RUST.parent / "hydra_basis/execution_engine/order_fill.py"]})
    destination = RUST / "artifacts/private-events-validation.json"
    destination.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
