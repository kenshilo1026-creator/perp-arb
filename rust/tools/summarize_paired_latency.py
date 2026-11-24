"""Summarize one paired run's local monotonic timing; no trading or network I/O."""
from __future__ import annotations
import argparse
import json
import math
from decimal import Decimal
from pathlib import Path


def summarize(rows):
    events = [r for r in rows if 'stage' in r]
    events.sort(key=lambda r: r['at_us'])
    first = {}
    sources = {}
    samples = {}
    def add(name, start, end):
        if start is not None and end is not None and end >= start:
            samples.setdefault(name, []).append((end-start)/1000)
    for r in events:
        key = (r['stage'], r['id'])
        first.setdefault(key, r['at_us'])
        if r['stage'] == 'hedge_sources':
            sources[r['id']] = r['data'].get('trade_ids', [])
        if r['stage'] == 'maker_ws_candidate':
            add('rust_receive_to_parse_ms', r['at_us'], r['data'].get('parsed_us'))
    def at(stage, identity):
        return first.get((stage, identity))
    ids = {r['id'] for r in events}
    for identity in ids:
        for name, start, end in [
            ('ws_receive_to_durable_fill_ms', 'maker_ws_candidate', 'maker_ws_commit_done'),
            ('maker_fill_wal_ms', 'maker_ws_commit_begin', 'maker_ws_commit_done'),
            ('ws_receive_to_rest_verified_ms', 'maker_ws_candidate', 'maker_rest_verified_trade'),
            ('hedge_wire_intent_wal_ms', 'hedge_wal_begin', 'hedge_wal_done'),
            ('hedge_submit_call_to_ack_ms', 'hedge_submit_call', 'hedge_ack'),
        ]:
            add(name, at(start, identity), at(end, identity))
        send = at('hedge_submit_call', identity)
        trigger = [at('maker_ws_candidate', t) for t in sources.get(identity, [])]
        trigger = [t for t in trigger if t is not None]
        if trigger:
            add('ws_trigger_to_submit_call_ms', min(trigger), send)
        # ACK is deliberately excluded. Positive cumulative evidence is needed;
        # private observations needing reconciliation are not confirmed fills.
        for stage, label in [('hedge_coordinator_observation', 'coordinator_observed'), ('hedge_rest_confirmation', 'rest_confirmed')]:
            candidates = [r['at_us'] for r in events if r['id'] == identity and r['stage'] == stage
                          and Decimal(r['data'].get('filled','0')) > 0
                          and not r['data'].get('requires_reconciliation', False)]
            if candidates:
                add(f'hedge_submit_call_to_{label}_fill_ms', send, min(candidates))
    def stats(values):
        values = sorted(values)
        def pct(p): return round(values[max(0, math.ceil(len(values)*p)-1)], 3)
        return dict(n=len(values), p50=pct(.5), p95=pct(.95), p99=pct(.99), maximum=round(values[-1],3))
    return {
        'metrics': {k: stats(v) for k,v in sorted(samples.items())},
        'unqualified_ws_events': sum(r['stage']=='maker_ws_unqualified' for r in events),
        'final_state': next((r['data'] for r in reversed(events) if r['stage']=='final_progress'),None),
        'mode': next((r['data'] for r in events if r['stage']=='run_mode'),None),
        'dropped_records': sum(r.get('dropped',0) for r in rows if 'schema' in r),
        'notes': [
            'Local monotonic durations only; never subtract exchange or extension wall clocks.',
            'submit_call precedes signing/HTTP; it is not the socket write timestamp.',
            'WS trigger metric starts at this worker batch; earlier dust fills may predate it. It is not total exposure duration.',
            'Missing observations produce no metric, never a fabricated zero.',
            'Small sample percentiles are descriptive, not a latency guarantee.',
            'Coordinator observations may originate from private WS or REST; not certified WS-only transport latency.',
        ],
    }


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path',type=Path,help='run directory or one latency JSONL file')
    args=parser.parse_args()
    files=sorted(args.path.glob('latency-*.jsonl')) if args.path.is_dir() else [args.path]
    if not files: parser.error('no latency traces found')
    # Each invocation/recovery has a distinct monotonic origin. Never merge offsets.
    output={str(p):summarize([json.loads(line) for line in p.read_text(encoding='utf8').splitlines() if line.strip()]) for p in files}
    print(json.dumps(output,ensure_ascii=False,indent=2))


if __name__=='__main__': main()
