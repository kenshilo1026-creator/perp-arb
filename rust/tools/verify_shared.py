"""Replay common BBO/depth behaviours against the actual Python QuoteStore/StoreFeed.

Rust's stricter equal-time conflict / price round-trip / malformed-data policies
are tested separately in Rust, not quietly treated as Python parity.
"""
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
from hydra_basis.spread_strategy.dispatcher import QuoteStore
from hydra_basis.spread_strategy.feeds import (
    StoreFeed, parse_aster_depth, parse_aster_book_ticker, parse_hyperliquid_book, to_levels,
)


def frame(venue, symbol, channel, stamp, size="2", sequence=1):
    bids, asks = [["100", size], ["99", "2"]], [["101", "2"], ["102", "2"]]
    if venue == "aster":
        data = dict(e="depthUpdate" if channel == "depth" else "bookTicker", s=symbol, T=stamp, u=sequence)
        data.update(dict(b=bids, a=asks) if channel == "depth" else dict(b="100", B=size, a="101", A="2"))
        return json.dumps(dict(stream=symbol.lower() + ("@depth10@100ms" if channel == "depth" else "@bookTicker"), data=data))
    sides = [[dict(px=p, sz=s, n=1) for p, s in rows] for rows in [bids, asks]]
    data = dict(coin=symbol, time=stamp)
    data.update(dict(levels=sides) if channel == "depth" else dict(bbo=[sides[0][0], sides[1][0]]))
    return json.dumps(dict(channel="l2Book" if channel == "depth" else "bbo", data=data))


def fixtures():
    events = []
    for index in range(20):
        for venue, symbol in [("aster", "ETHUSDT"), ("hyperliquid", "ETH")]:
            base = 10000 + index * 10000
            def add(action, offset, channel=None, size="2", sequence=1):
                events.append(dict(action=action, venue=venue, symbol=symbol, now=base+offset+10,
                    raw=frame(venue,symbol,channel,base+offset,size,sequence) if channel else None, quantity="4"))
            add("begin", 0)
            add("frame", 0, "depth")
            add("frame", 200, "bbo")  # unchanged BBO preserves original depth age
            add("query", 991)  # depth source age 1001 -> unavailable
            add("frame", 1100, "bbo", "1", 2)  # quantity change permits only one level
            add("frame", 1200, "depth", "2", 2)  # new depth restores capacity
            add("query", 1500)
            add("frame", 1300, "bbo", "2", 3)  # same top cannot retimestamp depth
            add("query", 2200)
            add("frame", 6500, "bbo", "2", 4)  # >5s depth cache age -> top only
            add("disconnect", 6510)
            add("query", 6520)
            add("begin", 6530)
            add("frame", 6540, "depth")  # new connection accepts reset sequence
    return events


def reference(events):
    states, output, clock = {}, [], [0]
    for event in events:
        venue, symbol = event["venue"], event["symbol"]
        key = venue, symbol
        clock[0] = event["now"]
        start = time.perf_counter_ns()
        update = None
        if event["action"] == "begin":
            config = Config(symbol=symbol, short_venue="aster", long_venue="hyperliquid", maker_venue="aster",
                total_quantity=D(4), clip_quantity=D(4), entry_bps=D(40), take_profit_bps=D(10),
                fees={v:{"maker":D(0),"taker":D(0)} for v in ["aster","hyperliquid"]})
            store, health = QuoteStore(clock=lambda: clock[0]), {venue:False}
            states[key] = store, health, StoreFeed(config,store,health,clock=lambda: clock[0])
        store, health, feed = states[key]
        if event["action"] == "disconnect":
            health[venue] = False
        elif event["action"] == "frame":
            payload = json.loads(event["raw"])
            data = payload["data"]
            if venue == "aster":
                is_depth = data["e"] == "depthUpdate"
                book = parse_aster_depth(data) if is_depth else parse_aster_book_ticker(data)
            else:
                is_depth = payload["channel"] == "l2Book"
                if is_depth:
                    book = parse_hyperliquid_book(payload)
                else:
                    # Protocol normalization only; cache selection below calls production StoreFeed.
                    bids, asks = to_levels([data["bbo"][0]]), to_levels([data["bbo"][1]])
                    book = dict(bid=bids[0][0],ask=asks[0][0],bids=bids,asks=asks,ts_ms=data["time"])
            if not book:
                raise AssertionError("Reference parser did not produce a book")
            if is_depth:
                store.update_depth(venue,symbol,book["bids"],book["asks"],source_ms=book["ts_ms"])
            else:
                store.update_quotes(venue,{symbol:book})
            health[venue] = True
            update = "accepted"
        fresh = feed.fresh(venue)
        elapsed = time.perf_counter_ns()-start
        normalized, prices = None, []
        if fresh:
            normalized = dict(symbol=symbol,source_ms=fresh.source_ms,bid=str(fresh.bid),ask=str(fresh.ask),
                bids=[[str(p),str(s)] for p,s in fresh.bids],asks=[[str(p),str(s)] for p,s in fresh.asks])
            def text(v):
                return None if v is None else str(v)
            prices = [dict(vwap=text(fresh.executable(side,D(event["quantity"]))),
                marginal=text(fresh.marginal(side,D(event["quantity"])))) for side in ["BUY","SELL"]]
        output.append(dict(update=update,fresh=fresh is not None,book=normalized,prices=prices,parse_cache_select_ns=elapsed))
    return output


def canonical(value):
    value = {k:v for k,v in value.items() if k != "parse_cache_select_ns"}
    if value["book"]:
        book = dict(value["book"])
        for field in ["bid","ask"]:
            book[field] = D(book[field])
        for field in ["bids","asks"]:
            book[field] = [[D(p),D(s)] for p,s in book[field]]
        value["book"] = book
    value["prices"] = [{k:None if v is None else D(v) for k,v in row.items()} for row in value["prices"]]
    return value


def mismatches(expected, actual):
    if len(expected) != len(actual):
        raise AssertionError("Output count mismatch")
    return [i for i,(a,b) in enumerate(zip(expected,actual)) if canonical(a) != canonical(b)]


def main():
    events = fixtures()
    expected = reference(events)
    binary = RUST / "target/release" / ("shared-replay.exe" if sys.platform == "win32" else "shared-replay")
    wire = "\n".join(json.dumps(e) for e in events)+"\n"
    result = subprocess.run([str(binary)],input=wire,text=True,encoding="utf-8",capture_output=True,check=True)
    actual = [json.loads(line) for line in result.stdout.splitlines()]
    differences = mismatches(expected,actual)
    paths = sorted((RUST / "crates/market-data").rglob("*.rs")) + [Path(__file__),
        RUST.parent / "hydra_basis/spread_strategy/dispatcher.py", RUST.parent / "hydra_basis/spread_strategy/feeds.py"]
    report = dict(status="passed" if not differences else "failed",events=len(events),mismatches=differences,
        fresh_by_venue=dict(Counter(e["venue"] for e,r in zip(events,actual) if r["fresh"])),
        dataset_sha256=hashlib.sha256(wire.encode()).hexdigest(),binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
        source_sha256={p.relative_to(RUST.parent).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
        scope="Exact common-policy book, source-time, freshness, VWAP, marginal and insufficient-capacity parity. Not a trading or P&L backtest; stricter Rust safety cases tested separately.")
    # Avoid a speed ratio: begin resets construct different objects; this replay is behavioural.
    output = RUST / "artifacts/batch2b-shared-validation.json"
    output.parent.mkdir(exist_ok=True,parents=True)
    output.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({k:v for k,v in report.items() if k != "source_sha256"},indent=2))
    if differences:
        i = differences[0]
        print(json.dumps(dict(event=events[i],expected=expected[i],actual=actual[i]),indent=2))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
