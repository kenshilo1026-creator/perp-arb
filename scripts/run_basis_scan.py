"""Rank historical perp/Binance Spot spread ranges; no trading or notifications."""
from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from scripts._bootstrap import ensure_project_root_on_path
ensure_project_root_on_path()

import aiohttp
from hydra_basis.config import VENUE_CONFIG
from hydra_basis.spread_strategy.basis_scan import CandleCache, PublicHistory, enabled_venues, scan
from web.monitor_cache import MonitorSession


def parser():
    p = argparse.ArgumentParser(description='永續／Binance 現貨歷史價差振幅排名（非實際利潤）')
    p.add_argument('days', type=int, choices=range(1, 31), metavar='DAYS', help='最近 1–30 天')
    p.add_argument('--venues', nargs='+', choices=tuple(VENUE_CONFIG), help='選填：縮小本次範圍；預設包含所有 VENUE_CONFIG enabled=true 交易所')
    p.add_argument('--symbols', nargs='+', help='選填：只掃描這些幣種；預設全部共同標的')
    p.add_argument('--top', type=int, default=10)
    p.add_argument('--min-coverage', type=float, default=70, help='共同時間點最低覆蓋率百分比，預設 70')
    p.add_argument('--output', type=Path, default=Path('data/market_monitor/basis_scan_latest.json'))
    return p


def stamp(ms):
    return '-' if ms is None else datetime.fromtimestamp(ms/1000, timezone.utc).isoformat(timespec='minutes')


async def run(args):
    venues = list(dict.fromkeys(args.venues)) if args.venues is not None else enabled_venues()
    print('本次比較：' + ', '.join(venues) + ' / Binance 現貨', flush=True)
    cache = CandleCache(Path('data/market_monitor/basis_scan_cache.sqlite3'))
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as raw:
        source = PublicHistory(MonitorSession(raw), cache)
        report = await scan(source, days=args.days, venues=venues,
                            symbols=None if args.symbols is None else [s.upper() for s in args.symbols],
                            top=args.top, min_coverage=args.min_coverage,
                            progress=lambda m: print(m, flush=True))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_suffix(args.output.suffix+'.tmp')
    temp.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)+'\n', encoding='utf-8')
    temp.replace(args.output)
    print(f"\n最近 {args.days} 天；{report['interval_minutes']} 分鐘採樣；永續空方／Binance 現貨多方")
    print(f"UTC {stamp(report['start_ms'])} → {stamp(report['end_ms'])}")
    print('排名 幣種       永續交易所       最大價差    最小價差    振幅(百分點) 先高後低(百分點) 覆蓋率')
    for i, row in enumerate(report['top'], 1):
        print(f"{i:>3} {row['symbol']:<10} {row['venue']:<15} {row['max_gap_pct']:>9.4f}% "
              f"{row['min_gap_pct']:>9.4f}% {row['range_pp']:>12.4f} "
              f"{row['chronological_convergence_pp']:>16.4f} {row['coverage_pct']:>6.1f}%")
        print(f"    最高 {stamp(row['max_at_ms'])}；最低 {stamp(row['min_at_ms'])}")
    if not report['top']:
        print('沒有足夠歷史資料的配對通過覆蓋率門檻。')
    counts = Counter(r['reason'] for r in report['skipped'])
    print(f"有效 {report['ranked_pairs']} 配對；跳過摘要 {dict(counts)}")
    for (venue, reason), count in sorted(Counter((r['venue'], r['reason']) for r in report['skipped']).items()):
        print(f'  {venue}: {reason} ({count})')
    print('振幅是歷史觀察範圍，非可實現利潤；未扣手續費／滑點／資金費率，未校正 USDT／USDC 匯率。')
    print(f'完整排名、時間及跳過原因：{args.output}')
    return report


def main():
    p = parser()
    args = p.parse_args()
    if not 1 <= args.top <= 100 or not 0 <= args.min_coverage <= 100:
        p.error('--top 需為 1–100，--min-coverage 需為 0–100')
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print('\n已停止；已完成的歷史快取保留供下次使用。')


if __name__ == '__main__':
    main()
