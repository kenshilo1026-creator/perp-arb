"""Native CLI differential tests: build paired-order debug before running."""
import argparse
import datetime as dt
import itertools
import json
from decimal import Decimal
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from hydra_basis.config import VENUE_CONFIG
from hydra_basis.execution_engine.priority import resolve_execution_legs
from hydra_basis.execution_engine.risk import compute_spread_pct
from hydra_basis.execution_engine.rust_selection import decimal_text, pair_input, select_legs_for_pair
from hydra_basis.execution_engine.rust_plan_prepare import prepare_signal_plan

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / 'rust/target/debug/paired-order.exe'
ENABLED = [v for v, c in VENUE_CONFIG.items() if c.enabled]


def cli(action, payload, *, fail=False):
    result = subprocess.run([str(BINARY), action], input=json.dumps(payload),
                            capture_output=True, text=True, encoding='utf-8', timeout=30)
    if fail:
        assert result.returncode != 0, result.stdout
        return result.stderr
    assert result.returncode == 0, result.stderr
    result = json.loads(result.stdout)
    assert result['can_submit_orders'] is False
    return result['selection']


@unittest.skipUnless(BINARY.exists(), 'build Rust paired-order debug for differential tests')
class DifferentialTests(unittest.TestCase):
    def test_all_enabled_ordered_pairs_spreads_priorities_and_modes_match_python(self):
        cases, expected = [], []
        for short, long in itertools.permutations(ENABLED, 2):
            for sh, lh, priorities, mode in itertools.product(
                    ('0.005', '0.04', '0.06', '0.5'), ('0.005', '0.04', '0.06', '0.5'),
                    ((0, 2), (2, 0), (2, 2)), ('open', 'close')):
                books = {v: {'bid': str(Decimal(100) - Decimal(h)), 'ask': str(Decimal(100) + Decimal(h))}
                         for v, h in ((short, sh), (long, lh))}
                priority = dict(zip((short, long), priorities))
                spreads = {v: compute_spread_pct(b) for v, b in books.items()}
                maker, taker = resolve_execution_legs(short_venue=short, long_venue=long,
                                                      priorities=priority, spreads=spreads)
                buy = (maker == long) != (mode == 'close')
                expected.append((maker, taker, buy, mode == 'close',
                    None if maker == 'variational' else Decimal(books[maker]['bid' if buy else 'ask']),
                    Decimal(books[taker]['bid' if buy else 'ask'])))
                cases.append(dict(short_venue=short, long_venue=long, mode=mode,
                                  books=books, priorities=priority))
        actual = cli('select-legs', cases)
        self.assertEqual(len(actual), len(expected))
        for index, (result, wanted) in enumerate(zip(actual, expected)):
            with self.subTest(case=index):
                self.assertEqual((result['maker_venue'], result['taker_venue'], result['maker_side'] == 'BUY',
                    result['reduce_only'], None if result['maker_price'] is None else Decimal(result['maker_price']),
                    Decimal(result['taker_reference_price'])), wanted)
                self.assertNotEqual(result['maker_side'], result['taker_side'])

    def test_pair_ranking_filters_direction_age_and_explicit_close(self):
        rows = [dict(symbol='ETH', short_venue=s, long_venue=l, annualized_avg=rate, signal=signal)
                for s, l, rate, signal in [('aster','hyperliquid','9',True), ('variational','aster','1',True),
                                         ('arcus','aster','99',True), ('aster','variational','100',False),
                                         ('hyperliquid','aster','NaN',True)]]
        request = dict(symbol='eth', mode='open', enabled_venues=['aster','hyperliquid','variational'],
                       updated_ms=100_000_000, now_ms=100_000_000, signals=rows)
        self.assertEqual(cli('select-pair', request)['long_venue'], 'hyperliquid')
        self.assertEqual(cli('select-pair', {**request, 'short_venue':'VARIATIONAL'})['short_venue'], 'variational')
        for updated in (None, 0, 100_000_001):
            self.assertIn('timestamp', cli('select-pair', {**request, 'updated_ms':updated}, fail=True))
        self.assertIn('close requires', cli('select-pair', {**request, 'mode':'close'}, fail=True))
        closed = cli('select-pair', {**request, 'mode':'close', 'signals':[], 'updated_ms':None,
                                    'short_venue':'arcus', 'long_venue':'aster'})
        self.assertEqual(closed['source'], 'explicit')
        self.assertIn('disabled', cli('select-pair', {**request, 'short_venue':'arcus', 'long_venue':'aster'}, fail=True))
        self.assertIn('no eligible', cli('select-pair', {**request, 'signals':[]}, fail=True))

    def test_override_validation_and_variational_stays_maker(self):
        r = dict(short_venue='aster', long_venue='hyperliquid', mode='open', priorities={'aster':2,'hyperliquid':2},
                 books={'aster':{'bid':'100','ask':'100.01'}, 'hyperliquid':{'bid':'100','ask':'100.01'}})
        for override in ({'maker_venue':'hyperliquid'}, {'taker_venue':'aster'},
                         {'maker_venue':'hyperliquid','taker_venue':'aster'}):
            self.assertEqual(cli('select-legs', {**r, **override})['maker_venue'], 'hyperliquid')
        for override in ({'maker_venue':'other'}, {'maker_venue':'aster','taker_venue':'aster'}):
            cli('select-legs', {**r, **override}, fail=True)
        r['long_venue'] = 'variational'
        r['books']['variational'] = r['books'].pop('hyperliquid')
        self.assertIsNone(cli('select-legs', r)['maker_price'])
        self.assertIn('Variational must remain maker', cli('select-legs', {**r,'maker_venue':'aster'}, fail=True))


@unittest.skipUnless(BINARY.exists(), 'build Rust paired-order debug for integration tests')
class SelectionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_small_token_scientific_quotes_survive_native_decimal_transport(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'configs').mkdir()
            (root/'configs/execution_venues.json').write_text('{"aster":2,"hyperliquid":2}')
            with patch('hydra_basis.execution_engine.rust_selection.selector_binary', return_value=BINARY), \
                 patch('hydra_basis.execution_engine.rust_selection.fetch_orderbook_snapshot', new=AsyncMock(return_value={'bid':1e-8, 'ask':1.001e-8})):
                result = await select_legs_for_pair(argparse.Namespace(), signal={'symbol':'TINY','short_venue':'aster','long_venue':'hyperliquid'}, quantity='10000000', project_root=root)
            self.assertEqual(Decimal(result['selection']['maker_price']), Decimal('0.00000001001'))

    async def test_real_rust_selection_through_place_order_for_non_variational_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'configs').mkdir()
            (root/'configs/execution_venues.json').write_text('{"aster":2,"hyperliquid":2}')
            (root/'data').mkdir()
            (root/'data/monitor_signals.json').write_text(json.dumps({'updated_at':dt.datetime.now(dt.timezone.utc).isoformat(),
                'cross_exchange_signals':[dict(symbol='ETH',short_venue='aster',long_venue='hyperliquid',annualized_avg='9',signal=True),
                                          dict(symbol='ETH',short_venue='variational',long_venue='aster',annualized_avg='1',signal=True)]}))
            args = argparse.Namespace(rust_action='select', ticker='ETH', rust_quantity='1', rust_plan=str(root/'selection.json'))
            async def quote(session, *, venue, symbol, clip_usd):
                return {'bid':100, 'ask':100.02 if venue == 'aster' else 100.4}
            with patch('hydra_basis.execution_engine.rust_selection.selector_binary', return_value=BINARY), \
                 patch('hydra_basis.execution_engine.rust_selection.fetch_orderbook_snapshot', side_effect=quote) as fetch, \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.preview_mid_price', new=AsyncMock()) as mid, \
                 patch('hydra_basis.execution_engine.rust_place_order.run_native_preflight', new=AsyncMock()) as executor:
                path = await prepare_signal_plan(args, project_root=root)
                data = json.loads(path.read_text())
                self.assertEqual(data['signal']['long_venue'], 'hyperliquid')
                self.assertEqual(data['selection']['maker_venue'], 'hyperliquid')
                self.assertEqual(data['selection']['maker_price_source'], 'best_bid')
                self.assertTrue(data['selection']['paired_execution_supported'])
                self.assertFalse(data['can_submit_orders'])
                self.assertEqual(fetch.call_count, 2)
                mid.assert_not_awaited(); executor.assert_not_awaited()
                from tests.test_rust_plan_prepare import SETTINGS
                (root/'rust/artifacts').mkdir(parents=True)
                (root/'rust/artifacts/paired-settings.json').write_text(json.dumps(SETTINGS))
                args.rust_action = 'prepare'; args.rust_plan = str(root/'native.json')
                args.rust_leverage = 1
                native = await prepare_signal_plan(args, project_root=root)
                plan = json.loads(native.read_text())
                self.assertEqual(plan['schema'], 'native_perp_pair_v1')
                self.assertEqual(plan['maker']['venue'], 'hyperliquid')
                self.assertEqual(plan['hedge']['tif'], 'market')
                executor.assert_awaited_once(); mid.assert_not_awaited()
                # A selection report cannot be consumed as an executable plan.
                process = subprocess.run([str(BINARY), 'preview', '--plan', str(path)], capture_output=True, timeout=10)
                self.assertNotEqual(process.returncode, 0)

    async def test_variational_supported_pair_routes_selection_into_mid_and_plan(self):
        from tests.test_rust_plan_prepare import SETTINGS
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'configs').mkdir()
            (root/'configs/execution_venues.json').write_text('{"aster":2,"variational":1}')
            (root/'rust/artifacts').mkdir(parents=True)
            (root/'rust/artifacts/paired-settings.json').write_text(json.dumps(SETTINGS))
            args = argparse.Namespace(rust_action='prepare', ticker='ETH', rust_quantity='0.1',
                short_venue='aster', long_venue='variational', mode='close', rust_plan=str(root/'plan.json'))
            async def quote(session, **kwargs): return {'bid':100, 'ask':100.01}
            with patch('hydra_basis.execution_engine.rust_selection.selector_binary', return_value=BINARY), \
                 patch('hydra_basis.execution_engine.rust_selection.fetch_orderbook_snapshot', side_effect=quote) as fetch, \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.instrument_metadata', new=AsyncMock(return_value=('ETHUSDT',3600))), \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.preview_mid_price', new=AsyncMock(return_value='100.005')) as mid, \
                 patch('hydra_basis.execution_engine.rust_place_order.run_native_preflight', new=AsyncMock()) as preview:
                path = await prepare_signal_plan(args, project_root=root)
            p = json.loads(path.read_text())
            self.assertEqual(p['maker']['side'], 'SELL'); self.assertEqual(p['hedge']['side'], 'BUY')
            self.assertTrue(p['maker']['reduce_only']); self.assertTrue(p['hedge']['reduce_only'])
            self.assertEqual(p['hedge']['tif'], 'market')
            self.assertEqual(fetch.call_count, 4)
            self.assertAlmostEqual(fetch.call_args_list[-1].kwargs['clip_usd'], 10.0005)
            mid.assert_awaited_once(); preview.assert_awaited_once()


class InputTests(unittest.TestCase):
    def test_scientific_rates_are_canonicalized_without_turning_missing_into_zero(self):
        self.assertEqual(decimal_text(1e-8), '0.00000001')
        self.assertEqual(decimal_text(None), 'None')
        self.assertEqual(decimal_text('NaN'), 'NaN')

    def test_explicit_close_needs_no_signal_file_and_bad_timestamp_is_not_refreshed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            r, source = pair_input(argparse.Namespace(mode='close', short_venue='aster', long_venue='hyperliquid'), symbol='ETH', project_root=root)
            self.assertEqual(source, 'explicit'); self.assertEqual(r['signals'], [])
            (root/'data').mkdir()
            for timestamp in ('bad', '2026-11-25T12:00:00', None):
                (root/'data/monitor_signals.json').write_text(json.dumps({'updated_at':timestamp,'cross_exchange_signals':[]}))
                r, _ = pair_input(argparse.Namespace(), symbol='ETH', project_root=root)
                self.assertIsNone(r['updated_ms'])


class ReadOnlyGuards(unittest.IsolatedAsyncioTestCase):
    async def test_select_rejects_trade_and_ignored_execution_options_before_network(self):
        for flag, value in [('live', True), ('rust_settings', 'settings.json'), ('rust_maker_price', '10'),
                            ('rust_hedge_price', '10'), ('rust_fill_mode', 'ws_confirmed')]:
            with patch('hydra_basis.execution_engine.rust_plan_prepare.select_pair_for_args', new=AsyncMock()) as select:
                args = argparse.Namespace(rust_action='select', ticker='ETH', **{flag: value})
                with self.assertRaises(RuntimeError):
                    await prepare_signal_plan(args, project_root=ROOT)
                select.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
