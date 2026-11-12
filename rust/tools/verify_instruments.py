"""Exact Python/Rust instrument parity. Offline by default; no orders or accounts."""
import argparse
import asyncio
from collections import Counter
from contextlib import nullcontext
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import random
import subprocess
import sys
from unittest.mock import patch

RUST=Path(__file__).resolve().parents[1]
ROOT=RUST.parent
sys.path.insert(0,str(ROOT))
from hydra_basis.spread_strategy import instruments as py
from hydra_basis.symbol_mapping import canonicalize_symbol, venue_symbol, load_symbol_mappings


def synthetic_catalogs():
    def aster(base,quote="USDT",status="TRADING"):
        return dict(symbol=base+quote,baseAsset=base,quoteAsset=quote,marginAsset=quote,status=status,contractType="PERPETUAL",
            pricePrecision=8,quantityPrecision=8,filters=[
                dict(filterType="PRICE_FILTER",tickSize="0.01",minPrice="0.01",maxPrice="1000000"),
                dict(filterType="LOT_SIZE",stepSize="0.001",minQty="0.001",maxQty="10000"),
                dict(filterType="MARKET_LOT_SIZE",stepSize="0.002",minQty="0.002",maxQty="2000"),
                dict(filterType="MIN_NOTIONAL",notional="5")])
    return dict(aster=dict(symbols=[aster("ETH"),aster("ETH","USDC"),aster("BTC"),aster("1000PEPE"),aster("OLD",status="SETTLING")]),
        hyperliquid=dict(collateralToken=0,universe=[dict(name="OLD",szDecimals=2,isDelisted=True),dict(name="ETH",szDecimals=4),
            dict(name="BTC",szDecimals=5),dict(name="kPEPE",szDecimals=0)]))


def fixtures(catalogs):
    cases=[]
    for key,payload in catalogs.items():
        venue="aster" if key.startswith("aster") else "hyperliquid"
        names={r["baseAsset"] for r in payload["symbols"]} if venue=="aster" else {r["name"] for r in payload["universe"]}
        for name in sorted(names|{"DOES_NOT_EXIST"}):
            cases.append(dict(op="resolve",catalog=key,venue=venue,symbol=name))
        # Also resolve every native Aster row, including alternative settlements.
        # Base-symbol preference alone would omit lower-priority USDC/USD rows.
        if venue=="aster":
            for row in payload["symbols"]:
                cases.append(dict(op="resolve",catalog=key,venue=venue,symbol=row["symbol"],native_request=True))
    # Fixed price boundaries plus seeded values at multiple magnitudes, all exact.
    prices=["2345.678","0.00012345678","101234.56","123456","9.99999","0.012345","99999.9","0.0000001",
        "999.999999999999999999","1000.000000000000000001","1.000000000000000000000000001"]
    rng=random.Random(20261109)
    prices += [str(D(rng.randint(1,999999999)).scaleb(-rng.randint(0,12))) for _ in range(80)]
    for sz in range(7):
        lot=D(1).scaleb(-sz)
        inst=dict(sz=sz,lot=str(lot),min_size=str(lot),min_notional="10")
        for index,price in enumerate(prices):
            quantity=[lot,lot/2,lot*3,lot*D("1.5"),D(0)][index%5]
            cases.append(dict(op="math",instrument=inst,price=price,quantity=str(quantity)))
    for tick,lot in [("0.01","0.001"),("0.0000001","1"),("0.25","0.003")]:
        inst=dict(tick=tick,lot=lot,min_size=lot,min_notional="5")
        for index,price in enumerate(prices):
            cases.append(dict(op="math",instrument=inst,price=price,quantity=str(D(lot)*[D(0),D("0.5"),D(1),D("1.5"),D(4)][index%5])))
    # Exact minimum-notional boundary and its immediate decimal neighbours.
    inst=dict(tick="0.01",lot="0.001",min_size="0.001",min_notional="5")
    for price in ["2499.999999999999999999","2500","2500.000000000000000001"]:
        cases.append(dict(op="math",instrument=inst,price=price,quantity="0.002"))
    for lots in [[],["0.002","0.003"],["0.0100","0.001"],["0.25","0.1"],["2","3"],["0.00001","0.001","0.0005"]]:
        cases.append(dict(op="common_lot",lots=lots))
    mapping=load_symbol_mappings()
    for venue in ["aster","hyperliquid","mexc_spot","variational"]:
        for symbol in sorted(set(mapping["global"])|{"eth","ASP","ASPECTA","XCU","COPPER","DOES_NOT_EXIST"}):
            cases.append(dict(op="map",venue=venue,symbol=symbol))
    # Preserve every numerical case, but serialize financial wire inputs in fixed
    # decimal notation just as the execution adapters do (never binary floats).
    for case in cases:
        if case["op"]=="math":
            for key in ["price","quantity"]:
                case[key]=format(D(case[key]),"f")
            for key,value in case["instrument"].items():
                if isinstance(value,str):
                    case["instrument"][key]=format(D(value),"f")
    return cases


def num(v):
    return None if v is None else str(v)


def summary(i,max_size=None):
    return dict(tick=num(i.tick_size),sz=i.sz_decimals,lot=num(i.lot_size),min_size=num(i.min_size),min_notional=num(i.min_notional),max_size=num(max_size))


async def reference_case(case,catalogs):
    op=case["op"]
    if op=="resolve":
        venue,symbol=case["venue"],case["symbol"]
        payload=catalogs[case["catalog"]]
        native=None
        if case.get("native_request"):
            native=next(row for row in payload["symbols"] if row["symbol"]==symbol)
            if native["status"]!="TRADING":
                return {"rejected":True}
        async def fetch(*args,**kwargs):
            return payload
        try:
            # Native names (including USD1/U) select a row explicitly. Exercise
            # production _aster's filter parser with that row; Python's old base
            # resolver only tries USDT/USDC/USD and is tested separately above.
            selection=patch.object(py,"aster_contract",return_value=native) if native is not None else nullcontext()
            with patch.object(py,"fetch_json",side_effect=fetch), selection:
                i=await py.fetch_instrument(None,venue,symbol)
                if venue=="aster":
                    row=await py.aster_contract(None,symbol)
        except RuntimeError:
            return {"rejected":True}
        if venue=="aster":
            native,base,quote,settlement,index=row["symbol"],row["baseAsset"],row["quoteAsset"],row["marginAsset"],None
            filters={r["filterType"]:r for r in row["filters"]}
            maximum=D(filters["LOT_SIZE"]["maxQty"])
            m=filters.get("MARKET_LOT_SIZE")
            market=None if m is None else summary(py.Instrument("aster",tick_size=i.tick_size,lot_size=D(m["stepSize"]),min_size=D(m["minQty"]),min_notional=i.min_notional),D(m["maxQty"]))
        else:
            index,row=next((n,r) for n,r in enumerate(payload["universe"]) if r["name"].upper()==symbol.upper())
            native,base,quote,settlement=row["name"],row["name"],"USD","USDC"
            maximum=None
            market=summary(i)
        canonical=canonicalize_symbol(base,venue=venue)
        return {"value":dict(native=native,base=base,canonical=canonical,quote=quote,settlement=settlement,asset_id=index,
            base_per_native="1" if base.upper()==canonical else None,limit=summary(i,maximum),market=market)}
    if op=="map":
        return {"value":dict(canonical=canonicalize_symbol(case["symbol"],venue=case["venue"]),native=venue_symbol(case["symbol"],venue=case["venue"]))}
    if op=="common_lot":
        return {"value":num(py.common_lot([py.Instrument("reference",lot_size=D(s)) for s in case["lots"]]))}
    row=case["instrument"]
    i=py.Instrument("hyperliquid" if "sz" in row else "aster",tick_size=D(row["tick"]) if "tick" in row else None,
        lot_size=D(row["lot"]),min_size=D(row["min_size"]),min_notional=D(row["min_notional"]),sz_decimals=row.get("sz"))
    price,quantity=D(case["price"]),D(case["quantity"])
    error=i.size_error(quantity,price)
    return {"value":dict(down=num(i.round_price(price,"down")),up=num(i.round_price(price,"up")),quantity=num(i.round_quantity(quantity)),
        size_error=None if error is None else error.split()[0],tolerance=num(i.price_tolerance(price)))}


def canonical(value):
    if "error" in value or "rejected" in value:
        return {"rejected":True}
    numeric={"tick","lot","min_size","min_notional","max_size","base_per_native","down","up","quantity","tolerance"}
    def walk(v,key=None):
        if isinstance(v,dict):
            return {k:walk(n,k) for k,n in v.items()}
        if isinstance(v,str) and (key in numeric or key=="value"):
            return D(v)
        return v
    return walk(value)


def mismatches(expected,actual):
    if len(expected)!=len(actual):
        raise AssertionError("Instrument replay output count differs")
    return [i for i,(a,b) in enumerate(zip(expected,actual)) if canonical(a)!=canonical(b)]


async def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recording",type=Path)
    parser.add_argument("--output",type=Path,default=RUST/"artifacts/batch2b-instruments-validation.json")
    args=parser.parse_args()
    if args.recording:
        capture=json.loads(args.recording.read_text(encoding="utf-8"))
        if capture.get("complete") is not True or capture.get("public_only") is not True:
            raise RuntimeError("Incomplete public metadata recording")
        if set(capture["records"])!={"aster_v1","aster_v3","hyperliquid"}:
            raise RuntimeError("Expected Aster v1/v3 and Hyperliquid snapshots")
        catalogs={k:v["payload"] for k,v in capture["records"].items()}
    else:
        catalogs=synthetic_catalogs()
    cases=fixtures(catalogs)
    request=dict(catalogs=catalogs,mappings=json.loads((ROOT/"configs/symbol_mappings.json").read_text()),cases=cases)
    wire=json.dumps(request)
    expected=[await reference_case(c,catalogs) for c in cases]
    binary=RUST/"target/release"/("instrument-replay.exe" if sys.platform=="win32" else "instrument-replay")
    result=subprocess.run([str(binary)],input=wire,text=True,encoding="utf-8",capture_output=True,check=True)
    actual=json.loads(result.stdout)["results"]
    differences=mismatches(expected,actual)
    active_native_rejections=[]
    for case,outcome in zip(cases,actual):
        if case.get("native_request") and "error" in outcome:
            row=next(r for r in catalogs[case["catalog"]]["symbols"] if r["symbol"]==case["symbol"])
            if row["status"]=="TRADING":
                active_native_rejections.append(dict(catalog=case["catalog"],symbol=case["symbol"],reason=outcome["error"]))
    sources=sorted((RUST/"crates/core").rglob("*.rs"))+sorted((RUST/"crates/market-data").rglob("*.rs"))+[Path(__file__),
        ROOT/"hydra_basis/spread_strategy/instruments.py",ROOT/"hydra_basis/execution_engine/hyperliquid_adapter.py",ROOT/"hydra_basis/symbol_mapping.py",ROOT/"configs/symbol_mappings.json"]
    report=dict(status="passed" if not differences and not active_native_rejections else "failed",cases=len(cases),operations=dict(Counter(c["op"] for c in cases)),
        accepted_metadata=sum(c["op"]=="resolve" and "value" in a for c,a in zip(cases,actual)),
        rejected_metadata=sum(c["op"]=="resolve" and "error" in a for c,a in zip(cases,actual)),
        native_aster_cases=sum(bool(c.get("native_request")) for c in cases),
        active_native_rejections=active_native_rejections,
        metadata_rejection_reasons=dict(Counter(a["error"] for c,a in zip(cases,actual) if c["op"]=="resolve" and "error" in a)),
        mismatches=[dict(index=i,case=cases[i],expected=expected[i],actual=actual[i]) for i in differences],
        dataset_sha256=hashlib.sha256(wire.encode()).hexdigest(),binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
        recording_sha256=None if args.recording is None else hashlib.sha256(args.recording.read_bytes()).hexdigest(),
        source_sha256={p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        scope="Exact instrument rounding/common lots/static size error categories/mapping/base-resolution parity. Native Aster rows are injected into Python's production filter parser; explicit USD1/U lookup extends the old base suffix resolver. Max/market fields separately normalized from metadata; stricter safety cases have Rust tests. No order execution or latency claim.")
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({k:v for k,v in report.items() if k not in {"source_sha256","mismatches"}},indent=2))
    if differences:
        print(json.dumps(report["mismatches"][:8],indent=2))
        raise SystemExit(1)
    if active_native_rejections:
        raise SystemExit("Active native contracts rejected; inspect the report before certification")


if __name__=="__main__":
    asyncio.run(main())
