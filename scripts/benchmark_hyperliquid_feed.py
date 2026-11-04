"""Read-only, simultaneous ETH l2Book default/fast comparison. No credentials."""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

WS_URL = 'wss://api.hyperliquid.xyz/ws'
HKT = timezone(timedelta(hours=8))


def distribution(values):
    ordered = sorted(v for v in values if math.isfinite(v))
    if not ordered:
        return None
    def percentile(p):
        return round(ordered[min(len(ordered) - 1, math.ceil(len(ordered) * p) - 1)], 2)
    return {'count': len(ordered), 'min': round(ordered[0], 2),
            'median': round(statistics.median(ordered), 2), 'p95': percentile(.95),
            'p99': percentile(.99), 'max': round(ordered[-1], 2)}


async def benchmark(args):
    states = {name: {'ready': asyncio.Event(), 'latest': None, 'frames': [], 'samples': [],
                     'errors': [], 'ack': None, 'connected': False} for name in ('default', 'fast')}
    started = None

    async def stream(session, name):
        state = states[name]
        subscription = {'type': 'l2Book', 'coin': args.coin}
        if name == 'fast':
            subscription['fast'] = True
        try:
            async with session.ws_connect(WS_URL, heartbeat=20) as ws:
                state['connected'] = True
                await ws.send_json({'method': 'subscribe', 'subscription': subscription})
                async for message in ws:
                    if message.type != aiohttp.WSMsgType.TEXT:
                        if message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    received_mono, received_ms = time.monotonic(), time.time() * 1000
                    data = message.json()
                    if data.get('channel') == 'subscriptionResponse':
                        state['ack'] = data.get('data')
                    elif data.get('channel') == 'error':
                        state['errors'].append(str(data.get('data'))[:300])
                        state['ready'].set()
                        return
                    elif data.get('channel') == 'l2Book':
                        book = data['data']
                        if book.get('coin') != args.coin:
                            continue
                        bids, asks = book['levels']
                        if not bids or not asks:
                            continue
                        source_ms = int(book['time'])
                        frame = {'mono': received_mono, 'received_ms': received_ms, 'source_ms': source_ms,
                                 'transport_ms': received_ms - source_ms,
                                 'bid_levels': len(bids), 'ask_levels': len(asks),
                                 'bid': float(bids[0]['px']), 'ask': float(asks[0]['px']),
                                 'bid_size_top5': sum(float(r['sz']) for r in bids[:5]),
                                 'ask_size_top5': sum(float(r['sz']) for r in asks[:5])}
                        state['latest'] = frame
                        if started is not None:
                            state['frames'].append(frame)
                        state['ready'].set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state['errors'].append(f'{type(exc).__name__}: {str(exc)[:250]}')
        finally:
            state['connected'] = False
            state['ready'].set()

    tasks = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        try:
            tasks.append(asyncio.create_task(stream(session, 'default')))
            await asyncio.sleep(3)  # Space new connections; only two are opened.
            tasks.append(asyncio.create_task(stream(session, 'fast')))
            await asyncio.wait_for(asyncio.gather(*(s['ready'].wait() for s in states.values())), 20)
            if any(s['latest'] is None or not s['connected'] for s in states.values()):
                raise RuntimeError('Both streams must produce valid books before comparing: ' +
                                   json.dumps({n: s['errors'] for n, s in states.items()}))
            await asyncio.sleep(5)  # Exclude initial subscription snapshots.
            started = time.monotonic()
            start_wall = time.time()
            start_hkt = datetime.now(HKT).isoformat()
            print(json.dumps({'event': 'measurement_started', 'hkt': start_hkt,
                              'seconds': args.seconds, 'coin': args.coin}), flush=True)
            next_report = 20
            while time.monotonic() - started < args.seconds:
                mono, wall_ms = time.monotonic(), time.time() * 1000
                for state in states.values():
                    frame = state['latest']
                    age = wall_ms - frame['source_ms']
                    fresh = (state['connected'] and 0 < frame['bid'] <= frame['ask'] and
                             -250 <= age <= 1000 and frame['transport_ms'] <= 500)
                    state['samples'].append({'elapsed_ms': (mono - started) * 1000,
                                             'age_ms': age, 'fresh': fresh,
                                             'connected': state['connected']})
                if mono - started >= next_report:
                    print(json.dumps({'event': 'progress', 'elapsed_seconds': round(mono - started),
                        'streams': {n: {'frames': len(s['frames']),
                                       'fresh_pct': round(100 * sum(x['fresh'] for x in s['samples']) / len(s['samples']), 1)}
                                    for n, s in states.items()}}), flush=True)
                    next_report += 20
                await asyncio.sleep(.05)
            duration = time.monotonic() - started
            drift_ms = ((time.time() - start_wall) - duration) * 1000
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    result = {'coin': args.coin, 'started_hkt': start_hkt, 'duration_seconds': round(duration, 3),
              'clock_drift_during_test_ms': round(drift_ms, 2),
              'freshness_rule': {'max_source_age_ms':1000, 'max_transport_ms':500, 'future_tolerance_ms':250},
              'note': 'Transport includes clock offset; this measures public quotes, not order execution.',
              'streams': {}}
    raw = {}
    for name, state in states.items():
        frames, samples = state['frames'], state['samples']
        result['streams'][name] = {
            'frames': len(frames), 'unique_source_times': len({f['source_ms'] for f in frames}),
            'updates_per_second': round(len(frames) / duration, 3),
            'levels_per_side': sorted({(f['bid_levels'], f['ask_levels']) for f in frames}),
            'receive_interval_ms': distribution([(b['mono'] - a['mono']) * 1000 for a, b in zip(frames, frames[1:])]),
            'source_interval_ms': distribution([b['source_ms'] - a['source_ms'] for a, b in zip(frames, frames[1:])]),
            'transport_ms': distribution([f['transport_ms'] for f in frames]),
            'sampled_age_ms': distribution([s['age_ms'] for s in samples]),
            'fresh_time_pct': round(100 * sum(s['fresh'] for s in samples) / len(samples), 2),
            'connected_time_pct': round(100 * sum(s['connected'] for s in samples) / len(samples), 2),
            'sample_interval_ms': distribution([b['elapsed_ms'] - a['elapsed_ms'] for a, b in zip(samples, samples[1:])]),
            'top5_bid_quantity': distribution([f['bid_size_top5'] for f in frames]),
            'top5_ask_quantity': distribution([f['ask_size_top5'] for f in frames]),
            'ack': state['ack'], 'errors': state['errors']}
        raw[name] = {'frames': frames, 'samples': samples}
    args.output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(HKT).strftime('%Y%m%d_%H%M%S')
    path = args.output / f'hyperliquid_{args.coin}_{stamp}.json'
    path.write_text(json.dumps({'summary': result, 'raw': raw}, indent=2), encoding='utf-8')
    print(json.dumps({'event':'result', 'path': str(path), 'summary':result}, indent=2), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coin', default='ETH')
    parser.add_argument('--seconds', type=int, default=90)
    parser.add_argument('--output', type=Path, default=Path('data/market_monitor/benchmarks'))
    args = parser.parse_args()
    if not 20 <= args.seconds <= 600:
        parser.error('--seconds must be between 20 and 600')
    asyncio.run(benchmark(args))
