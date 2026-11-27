import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from hydra_basis.execution_engine.rust_native_pair_prepare import build_native_plan


class NativePlanTests(unittest.TestCase):
    def test_lighter_roles_open_close_and_numeric_account(self):
        for other in ('aster','hyperliquid'):
            for maker,hedge in (('lighter',other),(other,'lighter')):
                for mode in ('open','close'):
                    report=dict(signal={'symbol':'ETH'},mode=mode,quantity='1',selection=dict(maker_venue=maker,taker_venue=hedge,maker_side='BUY',taker_side='SELL',maker_price='2000',taker_reference_price='2001'))
                    settings={'accounts':{'lighter':'100','aster':'0x'+'a'*40,'hyperliquid':'0x'+'b'*40}}
                    p=build_native_plan(argparse.Namespace(),report=report,settings=settings)
                    light=p['maker'] if maker=='lighter' else p['hedge']
                    self.assertEqual(light['symbol'],'ETH')
                    self.assertEqual(light['account'],'100')
                    self.assertEqual(light['tif'],'post_only' if maker=='lighter' else 'ioc')
                    self.assertEqual(light['reduce_only'],mode=='close')
                    settings['accounts']['lighter']='0x'+'a'*40
                    with self.assertRaises(RuntimeError):
                        build_native_plan(argparse.Namespace(),report=report,settings=settings)

    def plan(self, maker='aster', mode='open', **args):
        hedge = 'hyperliquid' if maker == 'aster' else 'aster'
        report = dict(signal={'symbol': 'ETH'}, mode=mode, quantity='2', selection=dict(
            maker_venue=maker, taker_venue=hedge, maker_side='SELL' if mode == 'open' else 'BUY',
            taker_side='BUY' if mode == 'open' else 'SELL', maker_price='2000', taker_reference_price='2001'))
        settings = {'accounts': {'aster': '0x' + 'a' * 40, 'hyperliquid': '0x' + 'b' * 40}}
        settings.update(args.pop('settings', {}))
        return build_native_plan(argparse.Namespace(**args), report=report, settings=settings)

    def test_both_roles_open_close_no_browser_or_manual_prices(self):
        for maker in ('aster', 'hyperliquid'):
            for mode in ('open', 'close'):
                p = self.plan(maker, mode, rust_clip_quantity='0.5')
                self.assertEqual(p['schema'], 'native_perp_pair_v1')
                self.assertEqual(p['maker']['quantity'], '2')
                self.assertEqual(p['clip_quantity'], '0.5')
                self.assertEqual(p['maker']['tif'], 'post_only')
                self.assertEqual(p['hedge']['tif'], 'ioc' if maker == 'aster' else 'market')
                self.assertEqual(p['hedge_price_mode'], 'dynamic_ioc' if maker == 'aster' else 'market')
                self.assertEqual(p['maker']['reduce_only'], mode == 'close')
                self.assertEqual(p['hedge']['reduce_only'], mode == 'close')
                self.assertNotEqual(p['maker']['side'], p['hedge']['side'])
                self.assertNotIn('extension_id', p)
                self.assertEqual(p['leverage'], 1)

    def test_explicit_ioc_policy_and_invalid_options(self):
        p = self.plan('hyperliquid', rust_hedge_price='1990', rust_leverage=3)
        self.assertEqual(p['hedge_price_mode'], 'fixed_ioc')
        self.assertEqual(p['hedge']['tif'], 'ioc')
        self.assertEqual(p['leverage'], 3)
        self.assertEqual(self.plan(settings={'native_pair': {'leverage': 2}})['leverage'], 2)
        for options in ({'rust_leverage': 0}, {'rust_leverage': 126}, {'rust_leverage': True},
                        {'rust_clip_quantity': '3'}, {'rust_clip_quantity': '0'},
                        {'rust_fill_mode': 'ws_confirmed'}, {'rust_maker_price': '2000'}):
            with self.subTest(options=options), self.assertRaises((ValueError, RuntimeError)):
                self.plan(**options)

    @unittest.skipUnless((Path(__file__).resolve().parents[1] / 'rust/target/debug/paired-order.exe').is_file(), 'build paired-order first')
    def test_native_cli_rejects_missing_live_and_ambiguous_options_before_network(self):
        binary = Path(__file__).resolve().parents[1] / 'rust/target/debug/paired-order.exe'
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / 'plan.json'
            plan.write_text(json.dumps(self.plan()))
            for args in (['submit', '--plan', str(plan)], ['preview', '--plan', str(plan), '--live'],
                         ['audit', '--plan', str(plan), '--baseline', 'unexpected.json'],
                         ['recover', '--plan', str(plan), '--live']):
                with self.subTest(args=args):
                    r = subprocess.run([str(binary), *args], capture_output=True, timeout=5)
                    self.assertNotEqual(r.returncode, 0)
                    self.assertIn(b'invalid native pair action/options', r.stderr)


if __name__ == '__main__':
    unittest.main()
