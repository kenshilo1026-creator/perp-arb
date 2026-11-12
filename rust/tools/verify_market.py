"""Replay identical raw public frames through Python and Rust; no network/orders."""
import argparse
from collections import Counter
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

RUST = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RUST.parent))
from hydra_basis.spread_strategy.core import Config
from hydra_basis.spread_strategy.feeds import MarketFeed, parse_aster_depth, parse_hyperliquid_book


def fixtures():
    records = []
    for index in range(60):
        for venue in ["aster", "hyperliquid"]:
            symbol = "ETHUSDT" if venue == "aster" else "ETH"
            depth = 20 if index % 3 == 0 else 5
            stamp = 10_000 + index * 100
            bid = D("2000") + D(index) / 100
            asks = [[str(bid + D(i + 1) / 100), str(D(i + 1) / 100)] for i in range(depth)]
            bids = [[str(bid - D(i) / 100), str(D(i + 1) / 100)] for i in range(depth)]
            if venue == "aster":
                raw = dict(e="depthUpdate", T=stamp, E=stamp+1, s=symbol, u=index, b=bids, a=asks)
                if index % 2 == 0:
                    raw = dict(stream="ethusdt@depth10@100ms", data=raw)
            else:
                raw = dict(channel="l2Book", data=dict(coin=symbol, time=stamp,
                    levels=[[dict(px=p, sz=s, n=1) for p, s in rows] for rows in [bids, asks]]))
            records.append(dict(type="frame", venue=venue, symbol=symbol, raw=json.dumps(raw), received_ms=stamp+100))
    return records


def percentile(samples, p):
    if not samples:
        return None
    values = sorted(samples)
    return values[(len(values)-1)*p//100]


def reference(records):
    config = Config(symbol="ETH", short_venue="aster", long_venue="hyperliquid", maker_venue="aster",
                    total_quantity=D(1), clip_quantity=D(1), entry_bps=D(40), take_profit_bps=D(10),
                    fees={v:{"maker":D(0),"taker":D(0)} for v in ["aster","hyperliquid"]})
    clock = [0]
    feeds, previous, result = {}, {}, []
    for row in records:
        key = (row.get("venue"), row.get("symbol"))
        if row["type"] == "health" and not row["healthy"]:
            previous.pop(key, None)
            feeds.pop(key, None)
        if row["type"] != "frame":
            continue
        venue = row["venue"]
        clock[0] = row["received_ms"]
        if key not in feeds:
            feeds[key] = MarketFeed(config, clock=lambda: clock[0])
        feed = feeds[key]
        start = time.perf_counter_ns()
        raw = json.loads(row["raw"])
        data = raw.get("data", raw) if venue == "aster" else raw
        parsed = parse_aster_depth(data) if venue == "aster" else parse_hyperliquid_book(data)
        sequence = data.get("u") if venue == "aster" else None
        update = "ignored"
        if parsed:
            old = previous.get(key)
            if old and (parsed["ts_ms"] < old[0] or (sequence is not None and old[1] is not None and sequence <= old[1])):
                update = "out_of_order"
            else:
                update = "accepted"
                previous[key] = (parsed["ts_ms"], sequence)
                feed.update(venue, parsed["bid"], parsed["ask"], source_ms=parsed["ts_ms"], received_ms=clock[0],
                            bids=parsed["bids"], asks=parsed["asks"])
        elapsed = time.perf_counter_ns() - start
        normalized = None
        if update == "accepted":
            normalized = dict(symbol=row["symbol"], source_ms=parsed["ts_ms"], bid=str(parsed["bid"]), ask=str(parsed["ask"]),
                              bids=[[str(p),str(s)] for p,s in parsed["bids"]], asks=[[str(p),str(s)] for p,s in parsed["asks"]])
        result.append(dict(update=update, fresh=feed.fresh(venue) is not None, book=normalized,
                           error=None, parse_and_cache_ns=elapsed))
    return result


def canonical(item):
    item = {key: value for key, value in item.items() if key != "parse_and_cache_ns"}
    if item["book"]:
        book = dict(item["book"])
        for field in ["bid", "ask"]:
            book[field] = D(book[field])
        for field in ["bids", "asks"]:
            book[field] = [[D(p), D(s)] for p,s in book[field]]
        item["book"] = book
    return item


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recording", type=Path)
    parser.add_argument("--output", type=Path, default=RUST / "artifacts/batch2-market-validation.json")
    args = parser.parse_args()
    if args.recording:
        records = [json.loads(line) for line in args.recording.read_text(encoding="utf-8").splitlines()]
        if not records or records[0].get("type") != "header" or records[-1].get("type") != "footer" or records[-1].get("complete") is not True:
            raise SystemExit("Recording is missing a complete header/footer; cannot certify it")
    else:
        records = fixtures()
    frames = [row for row in records if row["type"] == "frame"]
    if not frames or {row["venue"] for row in frames} != {"aster", "hyperliquid"}:
        raise SystemExit("Both venues need recorded frames")
    # Malformed-feed safety tests live in Rust. Never swallow a reference parser failure.
    expected = reference(records)
    binary = RUST / "target/release" / ("market-replay.exe" if sys.platform == "win32" else "market-replay")
    wire = "\n".join(json.dumps(row) for row in records) + "\n"
    completed = subprocess.run([str(binary)], input=wire, capture_output=True, text=True, encoding="utf-8", check=True)
    actual = [json.loads(line) for line in completed.stdout.splitlines()]
    if len(actual) != len(expected):
        raise AssertionError("Rust output count differs from raw frame count")
    mismatches = [i for i,(wanted,got) in enumerate(zip(expected,actual)) if canonical(wanted) != canonical(got)]
    # A live file also records the Rust result at receipt. Verify the offline replay agrees.
    live_mismatches = [i for i,(frame,got) in enumerate(zip(frames,actual)) if "update" in frame and
                       canonical({k:frame[k] for k in ["update","fresh","book","error"]}) != canonical(got)]
    accepted_by_venue = Counter(frame["venue"] for frame, got in zip(frames, actual) if got["update"] == "accepted")
    if set(accepted_by_venue) != {"aster", "hyperliquid"}:
        raise SystemExit("Both venues must have accepted books; acknowledgements alone cannot pass")
    report = dict(status="passed" if not mismatches and not live_mismatches else "failed",
                  frames=len(frames), counts=dict(Counter(row["venue"] for row in frames)),
                  accepted=sum(item["update"]=="accepted" for item in actual),
                  accepted_by_venue=dict(accepted_by_venue),
                  fresh_frames_by_venue=dict(Counter(frame["venue"] for frame,got in zip(frames,actual) if got["fresh"])),
                  mismatches=mismatches, live_replay_mismatches=live_mismatches,
                  dataset_sha256=hashlib.sha256(wire.encode()).hexdigest(),
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  source_sha256={str(path.relative_to(RUST)).replace("\\", "/"):hashlib.sha256(path.read_bytes()).hexdigest()
                                 for path in sorted((RUST / "crates/market-data").rglob("*.rs"))},
                  scope="Exact book/time/availability parity; sampled local JSON parse and cache times, not fill latency. Rust additionally validates book consistency.")
    for name, results in [("python",expected),("rust",actual)]:
        samples = [r["parse_and_cache_ns"] for r in results]
        report[name] = {f"parse_and_cache_ns_p{p}":percentile(samples,p) for p in [50,95,99]}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({k:v for k,v in report.items() if k != "source_sha256"},indent=2))
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
