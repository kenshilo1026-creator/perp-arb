"""Capture public metadata once per endpoint. No credentials, accounts or orders."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.request import Request, urlopen


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    # Reserve before I/O; incomplete files cannot pass metadata replay.
    with args.output.open("x",encoding="utf-8") as output:
        records={}
        for key,url,body in [
            ("aster_v1","https://fapi.asterdex.com/fapi/v1/exchangeInfo",None),
            ("aster_v3","https://fapi.asterdex.com/fapi/v3/exchangeInfo",None),
            ("hyperliquid","https://api.hyperliquid.xyz/info",b'{"type":"meta"}')]:
            request=Request(url,data=body,headers={"Content-Type":"application/json","User-Agent":"perp-arb-public-metadata-validation"})
            with urlopen(request,timeout=20) as response:
                raw=response.read(8_000_001)
            if len(raw)>8_000_000:
                raise RuntimeError("Oversized metadata response")
            payload=json.loads(raw)
            rows=payload.get("symbols" if key.startswith("aster") else "universe")
            if not isinstance(rows,list) or not rows:
                raise RuntimeError(f"Missing metadata rows: {key}")
            records[key]=dict(url=url,payload=payload,response_sha256=hashlib.sha256(raw).hexdigest(),
                captured_utc=datetime.now(timezone.utc).isoformat())
            print(f"{key}: {len(rows)} public instrument rows")
        json.dump(dict(complete=True,public_only=True,records=records),output,indent=2)
        output.write("\n")


if __name__=="__main__":
    main()
