import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from hydra_basis.funding_engine.models import FundingConfig

from hydra_basis.spread_strategy.basis_scan import (
    CandleCache, DAY, MINUTE, PublicHistory, analyse, enabled_venues, ranked, resolution, scan,
)
from scripts.run_basis_scan import parser
from web.monitor_cache import MonitorSession


class AnalysisTests(unittest.TestCase):
    def test_enabled_config_is_authoritative_even_for_new_venues(self):
        config = {'aster': FundingConfig('aster'), 'mexc': FundingConfig('mexc'),
                  'new_venue': FundingConfig('new_venue'), 'arcus': FundingConfig('arcus', enabled=False)}
        with patch('hydra_basis.spread_strategy.basis_scan.VENUE_CONFIG', config):
            self.assertEqual(enabled_venues(), ['aster', 'mexc', 'new_venue'])

    def test_requested_range_and_chronological_convergence(self):
        spot = {0: 100, MINUTE: 100, 2*MINUTE: 100}
        perp = {0: 101, MINUTE: 100, 2*MINUTE: 99.5}
        row, error = analyse('SAGA', 'hyperliquid', perp, spot, 0, 3*MINUTE, MINUTE)
        self.assertIsNone(error)
        self.assertAlmostEqual(row['range_pp'], 1.5)
        self.assertAlmostEqual(row['chronological_convergence_pp'], 1.5)
        self.assertAlmostEqual(row['convergence_pair_gross_return_pct'], 1.5)
        perp = {0: 99.5, MINUTE: 100, 2*MINUTE: 101}
        row, _ = analyse('SAGA', 'hyperliquid', perp, spot, 0, 3*MINUTE, MINUTE)
        self.assertAlmostEqual(row['range_pp'], 1.5)
        self.assertEqual(row['chronological_convergence_pp'], 0)
        self.assertIsNone(row['convergence_pair_gross_return_pct'])

    def test_coverage_uses_whole_window_no_forward_fill_and_excludes_bad_points(self):
        spot = {t: 100 for t in range(0, 10*MINUTE, MINUTE)}
        perp = {0: 101, MINUTE: float('nan'), 2*MINUTE: -1, 9*MINUTE: 99, 10*MINUTE: 200}
        row, error = analyse('ETH', 'aster', perp, spot, 0, 10*MINUTE, MINUTE)
        self.assertIsNone(row)
        self.assertEqual(error['coverage_pct'], 20)
        row, _ = analyse('ETH', 'aster', perp, spot, 0, 10*MINUTE, MINUTE, 20)
        self.assertEqual(row['range_pp'], 2)

    def test_spread_percentage_difference_is_not_fixed_quantity_pnl(self):
        row, _ = analyse('ETH', 'aster', {0: 101, MINUTE: 199}, {0: 100, MINUTE: 200},
                         0, 2*MINUTE, MINUTE)
        self.assertEqual(row['range_pp'], 1.5)
        self.assertEqual(row['convergence_pair_gross_return_pct'], 2)

    def test_top_ten_is_ranked_by_symbol_venue_pair(self):
        rows = [dict(symbol='X', venue=str(i), range_pp=i) for i in range(12)]
        self.assertEqual([r['range_pp'] for r in ranked(rows)], list(range(11, 1, -1)))

    def test_resolution_and_cli(self):
        self.assertEqual([resolution(d) for d in (1, 3, 7, 15, 30)], [1, 1, 5, 5, 15])
        for d in (0, 31):
            with self.assertRaises(ValueError):
                resolution(d)
        args = parser().parse_args(['7', '--venues', 'aster', 'hyperliquid', '--symbols', 'SAGA'])
        self.assertEqual((args.days, args.top, args.min_coverage), (7, 10, 70))

    def test_weight_uses_actual_candle_interval(self):
        spacing = MonitorSession.request_spacing('api.hyperliquid.xyz', {'json': {
            'type': 'candleSnapshot', 'req': {'interval': '5m', 'startTime': 0, 'endTime': 500*5*MINUTE-1}}})
        self.assertEqual(spacing, 2.9)


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_mexc_uses_existing_minute_history_when_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = PublicHistory(object(), CandleCache(Path(tmp)/'cache'), recorder_path=Path(tmp)/'missing')
            source.native['mexc'] = {'ETH': 'ETH'}
            with patch('hydra_basis.spread_strategy.basis_scan.fetch_minute_closes', new_callable=AsyncMock) as fetch:
                fetch.return_value = {t*MINUTE: 100+t for t in range(10)}
                self.assertEqual(await source.page('mexc', 'ETH', 0, 10*MINUTE, 5),
                                 {0: 104, 5*MINUTE: 109})
                self.assertEqual(fetch.call_args.args[1:], ('mexc', 'ETH', 0, 10*MINUTE-1))

    async def test_default_scan_reports_new_enabled_venue_without_stopping_others(self):
        source = AsyncMock()
        source.catalog.return_value = set()
        config = {'aster': FundingConfig('aster'), 'new_venue': FundingConfig('new_venue'),
                  'arcus': FundingConfig('arcus', enabled=False)}
        with patch('hydra_basis.spread_strategy.basis_scan.VENUE_CONFIG', config):
            result = await scan(source, now_ms=2*DAY)
        self.assertEqual(result['venues'], ['aster', 'new_venue'])
        self.assertIn({'venue': 'new_venue', 'reason': 'history_adapter_not_supported'}, result['skipped'])
        self.assertEqual([c.args[0] for c in source.catalog.await_args_list], ['binance_spot', 'aster'])

    async def test_fixed_pages_are_reused_and_closed_window_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = CandleCache(Path(tmp)/'cache.db', clock=lambda: 2000*MINUTE)
            source = PublicHistory(object(), cache, recorder_path=Path(tmp)/'missing')
            source.native['binance_spot'] = {'X': 'XUSDT'}
            async def page(v, s, first, stop, minutes):
                return {t: 1 for t in range(first, stop, minutes*MINUTE)}
            source.page = AsyncMock(side_effect=page)
            result = await source.series('binance_spot', 'X', 0, 1440*MINUTE, 1)
            self.assertEqual(len(result), 1440)
            self.assertEqual(source.page.await_count, 3)
            self.assertNotIn(1440*MINUTE, result)
            await source.series('binance_spot', 'X', 0, 1440*MINUTE, 1)
            self.assertEqual(source.page.await_count, 3)

    async def test_cache_gaps_retry_and_failure_is_not_cached(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = [DAY]
            cache = CandleCache(Path(tmp)/'cache.db', clock=lambda: now[0])
            cache.save('empty', MINUTE, False, {})
            self.assertEqual(cache.read('empty'), {})
            now[0] += 5*MINUTE
            self.assertIsNone(cache.read('empty'))
            source = PublicHistory(object(), cache, recorder_path=Path(tmp)/'missing')
            source.native['aster'] = {'X': 'XUSDT'}
            source.page = AsyncMock(side_effect=[RuntimeError('offline'), {0: 1}])
            with self.assertRaises(RuntimeError):
                await source.series('aster', 'X', 0, MINUTE, 1)
            self.assertEqual(await source.series('aster', 'X', 0, MINUTE, 1), {0: 1})

    async def test_venue_candle_request_and_invalid_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = PublicHistory(object(), CandleCache(Path(tmp)/'cache'), recorder_path=Path(tmp)/'missing')
            source.native = {'binance_spot': {'ETH': 'ETHUSDT'}, 'hyperliquid': {'ETH': 'ETH'}}
            with patch('hydra_basis.spread_strategy.basis_scan._json', new_callable=AsyncMock) as fetch:
                fetch.return_value = [[0, 0, 0, 0, '2', 0, 5*MINUTE-1],
                                      [5*MINUTE, 0, 0, 0, '3', 0, 10*MINUTE-1]]
                result = await source.page('binance_spot', 'ETH', 0, 5*MINUTE, 5)
                self.assertEqual(result, {0: 2})
                self.assertEqual(fetch.call_args.kwargs['params']['interval'], '5m')
                fetch.return_value = [{'t': 0, 'c': '2'}, {'t': 5*MINUTE, 'c': '3'}, {'t': 1, 'c': '4'}]
                self.assertEqual(await source.page('hyperliquid', 'ETH', 0, 5*MINUTE, 5), {0: 2})
                self.assertEqual(fetch.call_args.kwargs['json']['req']['endTime'], 5*MINUTE-1)

    async def test_catalog_keeps_native_spelling_and_omits_multiplier_aliases(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = PublicHistory(object(), CandleCache(Path(tmp)/'cache'), recorder_path=Path(tmp)/'missing')
            with patch('hydra_basis.spread_strategy.basis_scan._json', new_callable=AsyncMock) as fetch:
                fetch.return_value = {'universe': [{'name': 'xyz:BTC'}, {'name': 'xyz:OLD', 'isDelisted': True}]}
                self.assertEqual(await source.catalog('trade_xyz'), {'BTC'})
                self.assertEqual(source.native['trade_xyz']['BTC'], 'xyz:BTC')

    async def test_scan_partial_failures_shared_spot_and_local_unsupported(self):
        class Source:
            quote = {'aster': {'X': 'USDT'}, 'hyperliquid': {'X': 'USDC'}}
            def __init__(self):
                self.spot_calls = 0
            async def catalog(self, v):
                if v == 'arcus':
                    raise RuntimeError('offline')
                return {'X', 'Y'} if v in {'binance_spot', 'variational'} else {'X'}
            async def series(self, v, s, start, end, minutes):
                if v == 'variational':
                    return {}
                if v == 'binance_spot':
                    self.spot_calls += 1
                if v == 'hyperliquid':
                    raise RuntimeError('offline')
                return {t: (101 if t == start and v == 'aster' else 100)
                        for t in range(start, end, minutes*MINUTE)}
        source = Source()
        result = await scan(source, days=1, venues=['aster', 'hyperliquid', 'arcus', 'variational'], now_ms=2*DAY)
        self.assertEqual(result['ranked_pairs'], 1)
        self.assertEqual(source.spot_calls, 1)
        self.assertEqual(result['top'][0]['range_pp'], 1)
        self.assertEqual({r['reason'] for r in result['skipped']},
                         {'catalog_failed', 'perp_fetch_failed', 'no_recorded_history'})


if __name__ == '__main__':
    unittest.main()
