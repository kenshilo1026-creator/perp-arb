"""Record public depth and source/receive timestamps; never load credentials or trade."""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import asdict
import json
from pathlib import Path
import time

import aiohttp

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from scripts._bootstrap import ensure_project_root_on_path
ensure_project_root_on_path()

from hydra_basis.spread_strategy.core import Config
from hydra_basis.spread_strategy.feeds import MarketFeed
from hydra_basis.spread_strategy.instruments import fetch_instrument


class RecordingFeed(MarketFeed):
    def __init__(self, config, queue, **kwargs):
        super().__init__(config, **kwargs)
        self.queue = queue
        self.sequence = 0
        self.counts = Counter()
        self.dropped = 0

    def record(self, event):
        self.sequence += 1
        event.update(sequence=self.sequence, received_monotonic_ns=time.monotonic_ns(),
                     symbol=self.config.symbol)
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1

    def update(self, venue, bid, ask, **kwargs):
        super().update(venue, bid, ask, **kwargs)
        self.counts[venue] += 1
        self.record({'type': 'book', 'venue': venue, **asdict(self.ticker(venue))})

    def set_health(self, venue, healthy):
        changed = self._healthy.get(venue) != healthy
        super().set_health(venue, healthy)
        if changed:
            self.record({'type': 'health', 'venue': venue, 'healthy': healthy,
                         'received_ms': self.clock()})


async def write_events(queue, handle):
    while True:
        event = await queue.get()
        if event is None:
            return
        batch = [event]
        while len(batch) < 256 and not queue.empty():
            event = queue.get_nowait()
            if event is None:
                await asyncio.to_thread(handle.write, ''.join(json.dumps(e, default=str)+'\n' for e in batch))
                return
            batch.append(event)
        # File writes run outside the data-reader event loop.
        await asyncio.to_thread(handle.write, ''.join(json.dumps(e, default=str)+'\n' for e in batch))


async def record(config, output, seconds):
    if not 0 < seconds <= 7 * 24 * 3600:
        raise ValueError('seconds must be positive and at most one week')
    output.parent.mkdir(parents=True, exist_ok=True)
    queue = asyncio.Queue(maxsize=20000)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
        instruments = {v: await fetch_instrument(session, v, config.symbol) for v in config.venues}
        feed = RecordingFeed(config, queue, contract_sizes={v: i.contract_size for v, i in instruments.items()
                                                          if i.contract_size})
        # Exclusive creation prevents accidentally overwriting an earlier recording.
        with output.open('x', encoding='utf-8') as handle:
            handle.write(json.dumps({'type': 'header', 'schema': 1, 'config': asdict(config),
                'public_only': True, 'started_ms': int(time.time()*1000)}, default=str)+'\n')
            writer = asyncio.create_task(write_events(queue, handle))
            reader = asyncio.create_task(feed.run(session))
            deadline = time.monotonic()+seconds
            try:
                while time.monotonic() < deadline:
                    await asyncio.sleep(min(.25, max(0, deadline-time.monotonic())))
                    if writer.done():
                        await writer
                        raise RuntimeError('recording writer stopped')
                    if reader.done():
                        await reader
                        raise RuntimeError('market reader stopped')
                    if feed.dropped:
                        raise RuntimeError('recording queue overflow; dataset is incomplete')
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                if not writer.done():
                    await queue.put(None)
                await writer
                handle.write(json.dumps({'type': 'footer', 'counts': dict(feed.counts),
                    'dropped': feed.dropped, 'finished_ms': int(time.time()*1000)})+'\n')
    summary = {'output': str(output), 'counts': dict(feed.counts), 'dropped': feed.dropped,
               'both_venues_recorded': all(feed.counts[v] > 0 for v in config.venues),
               'historical_profit_backtest': False}
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/spread_strategy.example.json'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seconds', type=float, default=60)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(record(Config.load(args.config), args.output, args.seconds)), indent=2))


if __name__ == '__main__':
    main()
