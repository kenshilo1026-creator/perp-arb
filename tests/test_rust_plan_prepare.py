import argparse
import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.rust_plan_prepare import build_plan, prepare_signal_plan, preview_mid_price, lighter_ioc_price, prepare_leverage
from hydra_basis.execution_engine.rust_place_order import build_native_command


NOW = dt.datetime(2026, 11, 19, tzinfo=dt.timezone.utc)
SETTINGS = {"extension_id": "a" * 32, "maker_fill_mode": "ws_confirmed", "accounts": {
    "variational": "0x" + "a" * 40, "aster": "0x" + "b" * 40, "hyperliquid": "0x" + "c" * 40}}


def row(short="variational", long="aster", annualized="0.5", signal=True):
    return dict(symbol="ETH", short_venue=short, long_venue=long, annualized_avg=annualized, signal=signal)


class PlanTests(unittest.TestCase):
    def test_leverage_input_validation_and_close_never_prompts(self):
        with patch('hydra_basis.execution_engine.rust_plan_prepare.prompt', return_value='5') as prompt:
            self.assertEqual(prepare_leverage(argparse.Namespace(), 'open'), 5)
            prompt.assert_called_once_with('leverage_x')
            prompt.reset_mock()
            self.assertEqual(prepare_leverage(argparse.Namespace(rust_leverage=3), 'open'), 3)
            self.assertIsNone(prepare_leverage(argparse.Namespace(), 'close'))
            prompt.assert_not_called()
        for bad in ('0', '126', '1.5', 'NaN', '-1'):
            with patch('hydra_basis.execution_engine.rust_plan_prepare.prompt', return_value=bad), self.assertRaises(RuntimeError):
                prepare_leverage(argparse.Namespace(), 'open')
        for bad in (0, 126, True, 1.5):
            with self.assertRaises(RuntimeError):
                self.plan(leverage=bad)

    def test_leverage_is_native_hedge_policy_not_quantity_or_browser_command(self):
        for venue in ('aster', 'hyperliquid', 'lighter'):
            settings = {**SETTINGS, 'accounts': {**SETTINGS['accounts'], 'lighter': '100'}}
            for mode in ('open', 'close'):
                p = self.plan(signal=row('variational', venue), settings=settings, mode=mode, leverage=5)
                self.assertEqual(p['hedge_leverage'], 5 if mode == 'open' else None)
                self.assertEqual(p['maker']['quantity'], '0.0123')
                self.assertEqual(p['hedge']['quantity'], '0.0123')
                self.assertNotIn('leverage', p['maker'])

    def test_lighter_variational_pair_uses_numeric_account_and_ioc(self):
        settings={**SETTINGS,'accounts':{**SETTINGS['accounts'],'lighter':'100'}}
        for mode in ('open','close'):
            for signal in (row('lighter','variational'),row('variational','lighter')):
                p=self.plan(signal=signal,settings=settings,mode=mode,native_symbol='ETH')
                self.assertEqual(p['hedge']['venue'],'lighter')
                self.assertEqual(p['hedge']['account'],'100')
                self.assertEqual(p['hedge']['tif'],'ioc')
                self.assertEqual(p['maker']['reduce_only'],mode=='close')
                self.assertNotEqual(p['maker']['side'],p['hedge']['side'])
    def test_aster_market_only_in_supported_variational_pair_and_preserves_sides(self):
        for mode in ('open', 'close'):
            for signal in (row(), row('aster', 'variational')):
                p = self.plan(signal=signal, mode=mode, hedge_price=None)
                self.assertEqual(p['hedge']['tif'], 'market')
                self.assertEqual(p['hedge']['limit_price'], p['maker']['limit_price'])
                self.assertEqual(p['hedge']['reduce_only'], mode == 'close')
                self.assertNotEqual(p['hedge']['side'], p['maker']['side'])
        with self.assertRaisesRegex(RuntimeError, 'unsupported paired venues'):
            self.plan(signal=row('aster', 'hyperliquid'), hedge_price=None)
        self.assertEqual(self.plan()['hedge']['tif'], 'ioc')  # Explicit old limit remains explicit.

    def plan(self, **kwargs):
        options = dict(signal=row(), settings=SETTINGS, mode="open", quantity="0.0123", maker_price="3123.45",
                       hedge_price="3130.5", native_symbol="ETHUSDT", funding_interval_s=3600)
        options.update(kwargs)
        return build_plan(**options)

    def test_direction_close_reduce_only_caps_and_unique_identity(self):
        opened, closed = self.plan(), self.plan(mode="close")
        self.assertEqual(opened['maker']['side'], 'SELL')
        self.assertEqual(opened['hedge']['side'], 'BUY')
        self.assertEqual(closed['maker']['side'], 'BUY')
        self.assertTrue(closed['maker']['reduce_only'] and closed['hedge']['reduce_only'])
        self.assertEqual(opened['maker']['max_notional'], '38.418435')
        self.assertEqual(opened['maker_fill_mode'], 'ws_confirmed')
        self.assertEqual(opened['run_id'], opened['maker']['request_id'])
        self.assertNotEqual(opened['run_id'], closed['run_id'])
        reverse = self.plan(signal=row('hyperliquid', 'variational'), native_symbol='ETH')
        self.assertEqual(reverse['maker']['side'], 'BUY')
        self.assertEqual(reverse['hedge']['side'], 'SELL')
        self.assertEqual(reverse['hedge']['venue'], 'hyperliquid')

    def test_invalid_amounts_identity_interval_and_mode(self):
        for bad in ('NaN', 'Infinity', '0', '-1', 'abc'):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                self.plan(quantity=bad)
        for changes in ({'funding_interval_s': 0}, {'funding_interval_s': True}, {'mode': 'bad'},
                        {'fill_mode': 'bad'}, {'settings': {}}):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                self.plan(**changes)

    def test_prepare_flags_cannot_override_saved_submit_or_recovery(self):
        for action in ('submit', 'recover', 'preview'):
            with self.assertRaisesRegex(RuntimeError, 'existing plans/runs cannot be overridden'):
                build_native_command(argparse.Namespace(rust_action=action, rust_fill_mode='ws_confirmed'), project_root=Path('.'))


class PreparationTests(unittest.IsolatedAsyncioTestCase):
    async def test_lighter_ioc_uses_taker_side_tolerance_and_tick(self):
        metadata={'order_book_details':[{'symbol':'ETH','market_type':'perp','status':'active','supported_price_decimals':2}]}
        with patch('hydra_basis.execution_engine.rust_plan_prepare.fetch_json',new=AsyncMock(return_value=metadata)):
            self.assertEqual(await lighter_ioc_price('ETH','BUY','2000.01'),'2010.02')
            self.assertEqual(await lighter_ioc_price('ETH','SELL','2000.01'),'1990.00')
            self.assertEqual(await lighter_ioc_price('ETH','BUY','2000.01','0'),'2000.01')
        with self.assertRaises(RuntimeError):
            await lighter_ioc_price('ETH','BUY','2000','1001')

    def setUp(self):
        # These tests own plan construction. Native selection has separate CLI
        # differential and integration tests in test_rust_selection.py.
        async def pair(args, *, symbol, project_root):
            return {**row(getattr(args, 'short_venue', None) or 'aster',
                          getattr(args, 'long_venue', None) or 'variational'),
                    'symbol': symbol, 'updated_at': 'fixture'}
        async def legs(args, *, signal, quantity, project_root):
            taker = next(v for v in (signal['short_venue'], signal['long_venue']) if v != 'variational')
            buy = (signal['long_venue'] == 'variational') != (getattr(args, 'mode', 'open') == 'close')
            return {'signal': signal, 'selection': {
                'maker_venue': 'variational', 'taker_venue': taker,
                'maker_side': 'BUY' if buy else 'SELL', 'taker_side': 'SELL' if buy else 'BUY',
                'paired_execution_supported': True, 'reason': 'variational_maker',
                'spread_by_venue': {}, 'requires_spread_confirmation': False,
                'maker_price': None, 'taker_reference_price': '3000'}}
        for target, fn in (('select_pair_for_args', pair), ('select_legs_for_pair', legs)):
            p = patch('hydra_basis.execution_engine.rust_plan_prepare.' + target, new=fn)
            p.start()
            self.addCleanup(p.stop)

    def args(self, root, **overrides):
        options = dict(rust_action='prepare', ticker='ETH', mode='open', rust_quantity='0.01',
                       rust_maker_price='3000', rust_hedge_price='2990', rust_leverage=3, rust_plan=str(root/'plan.json'))
        options.update(overrides)
        return argparse.Namespace(**options)

    def setup_files(self, root):
        settings = root/'rust/artifacts/paired-settings.json'
        settings.parent.mkdir(parents=True)
        settings.write_text(json.dumps(SETTINGS), encoding='utf-8')
        signals = root/'data/monitor_signals.json'
        signals.parent.mkdir()
        signals.write_text(json.dumps({'updated_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                                      'cross_exchange_signals': [row('aster', 'variational')]}), encoding='utf-8')

    async def test_prepare_only_runs_preview_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.setup_files(root)
            with patch('hydra_basis.execution_engine.rust_plan_prepare.instrument_metadata', new=AsyncMock(return_value=('ETHUSDT', 3600))), \
                 patch('hydra_basis.execution_engine.rust_place_order.run_native_preflight', new=AsyncMock()) as runner:
                path = await prepare_signal_plan(self.args(root), project_root=root)
                args = runner.call_args.args[0]
                self.assertEqual(args.rust_action, 'preview')
                self.assertFalse(getattr(args, 'live', False))
                plan = json.loads(path.read_text())
                self.assertEqual(plan['maker']['side'], 'BUY')
                self.assertEqual(plan['maker_fill_mode'], 'ws_confirmed')
                self.assertEqual(plan['hedge_leverage'], 3)
                before = path.read_bytes()
                with self.assertRaises(FileExistsError):
                    await prepare_signal_plan(self.args(root), project_root=root)
                self.assertEqual(path.read_bytes(), before)
                self.assertEqual(runner.await_count, 1)

    async def test_missing_leverage_prompts_and_persists_before_readonly_preview(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.setup_files(root)
            with patch('hydra_basis.execution_engine.rust_plan_prepare.instrument_metadata', new=AsyncMock(return_value=('ETHUSDT', 3600))), \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.prompt', return_value='7') as prompt, \
                 patch('hydra_basis.execution_engine.rust_place_order.run_native_preflight', new=AsyncMock()) as runner:
                path = await prepare_signal_plan(self.args(root, rust_leverage=None), project_root=root)
            prompt.assert_called_once_with('leverage_x')
            self.assertEqual(json.loads(path.read_text())['hedge_leverage'], 7)
            self.assertEqual(runner.call_args.args[0].rust_action, 'preview')

    async def test_native_route_receives_prompted_leverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.setup_files(root)
            report = {'selection': {'maker_venue': 'aster', 'taker_venue': 'hyperliquid', 'paired_execution_supported': True}}
            with patch('hydra_basis.execution_engine.rust_plan_prepare.select_legs_for_pair', new=AsyncMock(return_value=report)), \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.print_selection'), \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.prompt', return_value='4') as prompt, \
                 patch('hydra_basis.execution_engine.rust_native_pair_prepare.prepare_native_pair', new=AsyncMock()) as native:
                await prepare_signal_plan(self.args(root, rust_leverage=None), project_root=root)
            prompt.assert_called_once_with('leverage_x')
            self.assertEqual(native.call_args.args[0].rust_leverage, 4)

    async def test_default_maker_uses_native_mid_without_prompt_or_trade(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.setup_files(root)
            with patch('hydra_basis.execution_engine.rust_plan_prepare.instrument_metadata', new=AsyncMock(return_value=('ETHUSDT', 3600))), \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.preview_mid_price', new=AsyncMock(return_value='3001.25')) as mid, \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.prompt', side_effect=AssertionError('must not prompt')), \
                 patch('hydra_basis.execution_engine.rust_place_order.run_native_preflight', new=AsyncMock()) as runner:
                path = await prepare_signal_plan(self.args(root, rust_maker_price=None, rust_hedge_price=None), project_root=root)
            mid.assert_awaited_once_with(project_root=root, symbol='ETH', extension_id=SETTINGS['extension_id'])
            plan = json.loads(path.read_text())
            self.assertEqual(plan['maker']['limit_price'], '3001.25')
            self.assertEqual(plan['maker']['max_notional'], '30.0125')
            self.assertEqual(plan['hedge']['tif'], 'market')
            self.assertEqual(runner.call_args.args[0].rust_action, 'preview')

    async def test_mid_failure_creates_no_plan_or_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.setup_files(root)
            with patch('hydra_basis.execution_engine.rust_plan_prepare.instrument_metadata', new=AsyncMock(return_value=('ETHUSDT', 3600))), \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.preview_mid_price', new=AsyncMock(side_effect=RuntimeError('page disconnected'))), \
                 patch('hydra_basis.execution_engine.rust_place_order.run_native_preflight', new=AsyncMock()) as runner:
                with self.assertRaisesRegex(RuntimeError, 'page disconnected'):
                    await prepare_signal_plan(self.args(root, rust_maker_price=None), project_root=root)
            self.assertFalse((root/'plan.json').exists())
            runner.assert_not_awaited()

    async def test_mid_subprocess_is_read_only_and_checks_symbol_and_price(self):
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root/'rust/target/release/paired-order.exe'
            binary.parent.mkdir(parents=True)
            binary.touch()
            result = dict(mode='mid-price', symbol='ETH', source='variational_page_mid_button',
                          price='3001.25', can_submit_orders=False)
            for changes in ({}, {'symbol': 'BTC'}, {'price': 'NaN'}, {'can_submit_orders': True}):
                child = Mock(returncode=0, communicate=AsyncMock(return_value=(json.dumps({**result, **changes}).encode(), b'')))
                with patch('hydra_basis.execution_engine.rust_plan_prepare.asyncio.create_subprocess_exec', new=AsyncMock(return_value=child)) as spawn:
                    if changes:
                        with self.assertRaises(RuntimeError):
                            await preview_mid_price(project_root=root, symbol='ETH', extension_id=SETTINGS['extension_id'])
                    else:
                        self.assertEqual(await preview_mid_price(project_root=root, symbol='ETH', extension_id=SETTINGS['extension_id']), '3001.25')
                    self.assertEqual(spawn.call_args.args, (str(binary), 'mid-price', '--extension-id', SETTINGS['extension_id'], '--symbol', 'ETH'))
                    child.terminate.assert_not_called()

    async def test_mid_timeout_terminates_readonly_child_and_releases_owner(self):
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root/'rust/target/release/paired-order.exe'
            binary.parent.mkdir(parents=True)
            binary.touch()
            child = Mock(returncode=None, communicate=AsyncMock(side_effect=TimeoutError()), wait=AsyncMock())
            with patch('hydra_basis.execution_engine.rust_plan_prepare.asyncio.create_subprocess_exec', new=AsyncMock(return_value=child)):
                with self.assertRaisesRegex(RuntimeError, 'Mid'):
                    await preview_mid_price(project_root=root, symbol='ETH', extension_id=SETTINGS['extension_id'])
            child.terminate.assert_called_once()
            child.wait.assert_awaited_once()

    async def test_live_and_implicit_close_rejected_before_network(self):
        with patch('hydra_basis.execution_engine.rust_plan_prepare.instrument_metadata', new=AsyncMock()) as metadata:
            for overrides in ({'live': True}, {'mode': 'close'}, {'rust_baseline': 'old.json'}):
                with self.assertRaises(RuntimeError):
                    await prepare_signal_plan(self.args(Path('.'), **overrides), project_root=Path('.'))
            metadata.assert_not_awaited()

    async def test_explicit_close_does_not_reselect_from_current_signals(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.setup_files(root)
            (root/'data/monitor_signals.json').unlink()
            with patch('hydra_basis.execution_engine.rust_plan_prepare.instrument_metadata', new=AsyncMock(return_value=('ETH', 3600))), \
                 patch('hydra_basis.execution_engine.rust_place_order.run_native_preflight', new=AsyncMock()):
                path = await prepare_signal_plan(self.args(root, mode='close', short_venue='variational', long_venue='hyperliquid'), project_root=root)
            plan = json.loads(path.read_text())
            self.assertEqual(plan['maker']['side'], 'BUY')
            self.assertTrue(plan['hedge']['reduce_only'])

    async def test_cli_routes_default_interactive_and_explicit_readonly_without_python_execution(self):
        from scripts.place_order import run_place_order
        with patch('hydra_basis.execution_engine.rust_plan_prepare.prepare_signal_plan', new=AsyncMock()) as prepare, \
             patch('hydra_basis.execution_engine.rust_interactive_order.run_interactive_order', new=AsyncMock()) as interactive, \
             patch('scripts.place_order.run_python_place_order', new=AsyncMock()) as python_execution:
            for action in (None, 'prepare', 'select'):
                await run_place_order(argparse.Namespace(engine='rust', rust_action=action))
            self.assertEqual(prepare.await_count, 2)
            interactive.assert_awaited_once()
            python_execution.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
