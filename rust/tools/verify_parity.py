"""Offline comparison with the existing Python functions; never loads credentials.

Run from any directory. All outputs stay under rust/artifacts by default.
--python-only prepares fixtures/baseline but DOES NOT certify the Rust implementation.
"""
from __future__ import annotations

import argparse
from decimal import Decimal as D
from functools import partial
import hashlib
import json
from pathlib import Path
import platform
import random
import subprocess
import sys
import time

RUST = Path(__file__).resolve().parents[1]
ROOT = RUST.parent
sys.path.insert(0, str(ROOT))

from hydra_basis.spread_strategy.core import Config, round_step, spread_bps
from hydra_basis.spread_strategy.feeds import MarketFeed, Ticker, vwap
from hydra_basis.execution_engine.maker_price_guard import adverse_price_gap

SOURCES = [
    "hydra_basis/spread_strategy/core.py",
    "hydra_basis/spread_strategy/feeds.py",
    "hydra_basis/execution_engine/maker_price_guard.py",
]


def quote(bid="99", ask="100", bids=None, asks=None, received_ms=10_000, source_ms=10_000):
    return dict(bid=bid, ask=ask, bids=bids, asks=asks,
                received_ms=received_ms, source_ms=source_ms)


def fixtures():
    cases = []
    for maker, taker in [("0.225900531", "0.2268"), ("0.00365", "0.003642"),
                         ("100", "100.2"), ("100", "99.8"), ("100", "100")]:
        cases.append(dict(op="spread", short=maker, long=taker))
        for side in ["BUY", "SELL"]:
            cases.append(dict(op="adverse", maker=maker, taker=taker, side=side))
    for value in ["1.001", "-1.001", "0", "1.01", "0.000000012345"]:
        for step in [None, "0", "-1", "0.01", "0.00000001"]:
            for direction in ["up", "down"]:
                cases.append(dict(op="round", value=value, step=step, direction=direction))
    for rows in [[], [["100", "1"], ["101", "1"], ["103", "2"]]]:
        for quantity in ["0.5", "1", "2", "3", "4", "5"]:
            cases.append(dict(op="vwap", levels=rows, quantity=quantity))
    for sizes in [False, True]:
        q = quote(bids=[["99", "0.5"], ["98", "1"]] if sizes else None,
                  asks=[["100", "1"], ["101", "1"], ["103", "2"]] if sizes else None)
        for side in ["BUY", "SELL"]:
            for amount in [None, "0.5", "1", "2", "3", "5"]:
                cases.append(dict(op="executable", quote=q, side=side, quantity=amount))
                if amount is not None:
                    cases.append(dict(op="marginal", quote=q, side=side, quantity=amount))
    for source in [None, 0, 10_000]:
        for lag in [0, 500, 501]:
            received = (source if source is not None else 10_000) + lag
            origin = source if source is not None else received
            for age in [-251, -250, 0, 1000, 1001]:
                for healthy in [True, False]:
                    cases.append(dict(op="fresh", quote=quote(received_ms=received, source_ms=source),
                                      now_ms=origin + age, healthy=healthy, policy_ms=[1000, 500, 250]))
    # Freeze a seed so later batches replay exactly the same price/depth cases.
    rng = random.Random(20261105)
    for _ in range(60):
        tick = D(rng.choice(["0.000001", "0.01", "0.1"]))
        mid = D(rng.randint(20, 200_000)) * tick
        bids = [[str(mid - tick * i), str(D(rng.randint(1, 100)) / 10)] for i in range(5)]
        asks = [[str(mid + tick * (i + 1)), str(D(rng.randint(1, 100)) / 10)] for i in range(5)]
        q = quote(bid=bids[0][0], ask=asks[0][0], bids=bids, asks=asks)
        amount = str(D(rng.randint(1, 500)) / 10)
        side = rng.choice(["BUY", "SELL"])
        cases.extend([
            dict(op="executable", quote=q, side=side, quantity=amount),
            dict(op="marginal", quote=q, side=side, quantity=amount),
            dict(op="spread", short=bids[0][0], long=asks[0][0]),
            dict(op="adverse", maker=bids[0][0], taker=asks[0][0], side=side),
            dict(op="round", value=str(mid + tick / 3), step=str(tick), direction="up"),
            dict(op="round", value=str(-mid - tick / 3), step=str(tick), direction="down"),
        ])
    return cases


def prepare(case):
    op = case["op"]
    if op == "spread":
        return partial(spread_bps, D(case["short"]), D(case["long"]))
    if op == "adverse":
        return partial(adverse_price_gap, D(case["maker"]), D(case["taker"]), case["side"])
    if op == "round":
        return partial(round_step, D(case["value"]), None if case["step"] is None else D(case["step"]), case["direction"])
    if op == "vwap":
        return partial(vwap, tuple((D(p), D(s)) for p, s in case["levels"]), D(case["quantity"]))
    q = case["quote"]
    ticker = Ticker(D(q["bid"]), D(q["ask"]), q["received_ms"], q["source_ms"],
                    tuple((D(p), D(s)) for p, s in q["bids"] or []),
                    tuple((D(p), D(s)) for p, s in q["asks"] or []))
    if op == "executable":
        return partial(ticker.executable, case["side"], None if case["quantity"] is None else D(case["quantity"]))
    if op == "marginal":
        return partial(ticker.marginal, case["side"], D(case["quantity"]))
    if op != "fresh":
        raise ValueError(op)
    age, lag, future = case["policy_ms"]
    config = Config(symbol="ETH", short_venue="aster", long_venue="hyperliquid", maker_venue="aster",
                    total_quantity=D(1), clip_quantity=D(1), entry_bps=D(40), take_profit_bps=D(10),
                    fees={venue: {"maker": D(0), "taker": D(0)} for venue in ("aster", "hyperliquid")},
                    market_freshness_seconds=age / 1000, max_transport_lag_seconds=lag / 1000,
                    future_tolerance_seconds=future / 1000)
    feed = MarketFeed(config, clock=lambda: case["now_ms"])
    feed.update("aster", ticker.bid, ticker.ask, received_ms=ticker.received_ms, source_ms=ticker.source_ms)
    feed.set_health("aster", case["healthy"])
    return lambda: feed.fresh("aster") is not None


def benchmark(operations):
    repeats = 100
    for _ in range(repeats):
        for operation in operations:
            operation()
    samples = []
    for _ in range(21):
        start = time.perf_counter_ns()
        for _ in range(repeats):
            for operation in operations:
                operation()
        samples.append((time.perf_counter_ns() - start) / (repeats * len(operations)))
    samples.sort()
    return dict(metric="batch_mean_ns_per_case", rounds=21, repeats=repeats, cases=len(operations),
                samples=samples, p50=samples[10], p95=samples[19])


def compare(cases, expected, actual):
    if len(actual) != len(expected):
        raise AssertionError(f"result count {len(actual)} != {len(expected)}")
    mismatches, rounded = [], []
    for index, (case, wanted, result) in enumerate(zip(cases, expected, actual)):
        if "error" in result:
            mismatches.append(dict(index=index, case=case, expected=str(wanted), actual=result))
            continue
        got = result["value"]
        if wanted is None or isinstance(wanted, bool):
            same = got is wanted
        else:
            if not isinstance(got, str):
                mismatches.append(dict(index=index, case=case, expected=str(wanted), actual=result))
                continue
            got = D(got)
            if not got.is_finite():
                mismatches.append(dict(index=index, case=case, expected=str(wanted), actual=result))
                continue
            error = abs(wanted - got)
            # Only division outputs allow decimal representation differences.
            # No tolerance for ticks, marginal prices, booleans or liquidity availability.
            tolerance = D(0) if case["op"] in {"round", "marginal"} else D("1e-24") + abs(wanted) * D("1e-26")
            same = error <= tolerance
            if same and error:
                rounded.append(dict(index=index, absolute_error=str(error)))
        if not same:
            mismatches.append(dict(index=index, case=case, expected=str(wanted), actual=result))
    return mismatches, rounded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-only", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path, default=RUST / "artifacts/batch1-validation.json")
    args = parser.parse_args()
    cases = fixtures()
    operations = [prepare(case) for case in cases]
    expected = [operation() for operation in operations]
    # Independently anchored examples, so fixture construction mistakes fail early.
    assert spread_bps(D("2010"), D("2000")) == D(50)
    assert adverse_price_gap(D(100), D("100.2"), "SELL") == D("0.002")
    assert vwap(((D(100), D(1)),), D(2)) is None
    report = dict(status="python_baseline_only", cases=len(cases),
                  python_version=sys.version, platform=platform.platform(),
                  source_sha256={name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES},
                  rust_source_sha256={str(path.relative_to(RUST)).replace("\\", "/"): hashlib.sha256(path.read_bytes()).hexdigest()
                                      for path in sorted((RUST / "crates").rglob("*.rs"))},
                  numeric_policy="round/marginal/bool/null exact; divisions abs <= 1e-24 + abs(expected)*1e-26; not a trading gate",
                  benchmark_scope="Prepared pure calculations; excludes parse/WS/signing/network/matching. Percentiles of batch means, not event tail latency.")
    if args.benchmark:
        report["python_benchmark"] = benchmark(operations)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    (args.output.parent / "batch1-cases.json").write_text(json.dumps(cases, indent=2), encoding="utf-8")
    if not args.python_only:
        binary = args.binary or RUST / "target/release" / ("core-replay.exe" if sys.platform == "win32" else "core-replay")
        if not binary.is_file():
            parser.error(f"Build Rust first: cargo build --release --manifest-path {RUST / 'Cargo.toml'}")
        command = [str(binary.resolve())] + (["--benchmark"] if args.benchmark else [])
        completed = subprocess.run(command, input=json.dumps(cases), text=True, encoding="utf-8", capture_output=True, check=True)
        result = json.loads(completed.stdout)
        mismatches, rounded = compare(cases, expected, result["results"])
        report.update(status="passed" if not mismatches else "failed", mismatches=mismatches,
                      nonidentical_decimal_count=len(rounded), decimal_differences=rounded,
                      rust_benchmark=result["benchmark"], binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
        if args.benchmark:
            if result["benchmark"]["profile"] != "release":
                raise AssertionError("Latency comparison requires a release binary")
            report["batch_mean_speedup_p50"] = report["python_benchmark"]["p50"] / result["benchmark"]["p50"]
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k not in {"decimal_differences", "source_sha256", "rust_source_sha256", "mismatches"}}, indent=2, ensure_ascii=False))
    print(f"report: {args.output}")
    if report["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
