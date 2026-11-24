import argparse
import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from hydra_basis.execution_engine.rust_plan_prepare import build_plan, prepare_signal_plan, preview_mid_price, select_signal
from hydra_basis.execution_engine.rust_place_order import build_native_command


NOW = dt.datetime(2026, 11, 19, tzinfo=dt.timezone.utc)
SETTINGS = {"extension_id": "a" * 32, "maker_fill_mode": "ws_confirmed", "accounts": {
    "variational": "0x" + "a" * 40, "aster": "0x" + "b" * 40, "hyperliquid": "0x" + "c" * 40}}


def row(short="variational", long="aster", annualized="0.5", signal=True):
    return dict(symbol="ETH", short_venue=short, long_venue=long, annualized_avg=annualized, signal=signal)


class SelectionTests(unittest.TestCase):
    def select(self, rows, *, updated=NOW, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "signals.json"
            path.write_text(json.dumps({"updated_at": updated.isoformat(), "cross_exchange_signals": rows}), encoding="utf-8")
            return select_signal(path, "eth", now=NOW, **kwargs)

    def test_ranks_supported_qualified_pairs_and_keeps_direction(self):
        best = self.select([row("aster", "hyperliquid", "9"), row(annualized="8", signal=False),
                            row(annualized="NaN"), row(), row("hyperliquid", "variational", "0.8")])
        self.assertEqual((best['short_venue'], best['long_venue']), ("hyperliquid", "variational"))

    def test_optional_venue_constraints(self):
        rows = [row(), row("hyperliquid", "variational", "0.8")]
        self.assertEqual(self.select(rows, short="VARIATIONAL")["long_venue"], "aster")
        with self.assertRaisesRegex(RuntimeError, "no eligible"):
            self.select(rows, short="aster")

    def test_stale_future_naive_and_missing_signals_do_not_fallback(self):
        for timestamp in (NOW - dt.timedelta(hours=25), NOW + dt.timedelta(seconds=1), NOW.replace(tzinfo=None)):
            with self.subTest(timestamp=timestamp), self.assertRaisesRegex(RuntimeError, "timestamp"):
                self.select([row()], updated=timestamp)
        with self.assertRaisesRegex(RuntimeError, "no eligible"):
            self.select([row(signal=False)])


class PlanTests(unittest.TestCase):
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
    def args(self, root, **overrides):
        options = dict(rust_action='prepare', ticker='ETH', mode='open', rust_quantity='0.01',
                       rust_maker_price='3000', rust_hedge_price='2990', rust_plan=str(root/'plan.json'))
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
                before = path.read_bytes()
                with self.assertRaises(FileExistsError):
                    await prepare_signal_plan(self.args(root), project_root=root)
                self.assertEqual(path.read_bytes(), before)
                self.assertEqual(runner.await_count, 1)

    async def test_default_maker_uses_native_mid_without_prompt_or_trade(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.setup_files(root)
            with patch('hydra_basis.execution_engine.rust_plan_prepare.instrument_metadata', new=AsyncMock(return_value=('ETHUSDT', 3600))), \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.preview_mid_price', new=AsyncMock(return_value='3001.25')) as mid, \
                 patch('hydra_basis.execution_engine.rust_plan_prepare.prompt', side_effect=AssertionError('must not prompt')), \
                 patch('hydra_basis.execution_engine.rust_place_order.run_native_preflight', new=AsyncMock()) as runner:
                path = await prepare_signal_plan(self.args(root, rust_maker_price=None), project_root=root)
            mid.assert_awaited_once_with(project_root=root, symbol='ETH', extension_id=SETTINGS['extension_id'])
            plan = json.loads(path.read_text())
            self.assertEqual(plan['maker']['limit_price'], '3001.25')
            self.assertEqual(plan['maker']['max_notional'], '30.0125')
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

    async def test_cli_default_rust_route_prepares_without_python_execution(self):
        from scripts.place_order import run_place_order
        with patch('hydra_basis.execution_engine.rust_plan_prepare.prepare_signal_plan', new=AsyncMock()) as prepare, \
             patch('scripts.place_order.run_python_place_order', new=AsyncMock()) as python_execution:
            await run_place_order(argparse.Namespace(engine='rust'))
            prepare.assert_awaited_once()
            python_execution.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
