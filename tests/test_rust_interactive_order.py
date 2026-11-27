import argparse
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from hydra_basis.execution_engine.rust_interactive_order import run_interactive_order
from hydra_basis.execution_engine.rust_place_order import build_native_command


class InteractiveOrderTests(unittest.IsolatedAsyncioTestCase):
    def plan(self, native=False, close=False):
        leg = dict(account='0x' + 'a' * 40, symbol='ETH', side='BUY', quantity='0.01',
                   limit_price='3000', reduce_only=close, tif='post_only')
        p = dict(maker=leg, hedge={**leg, 'account': '0x' + 'b' * 40, 'venue': 'aster',
                                  'symbol': 'ETHUSDT', 'side': 'SELL', 'tif': 'market'},
                 hedge_leverage=3, max_entry_gap='0.0025')
        if native:
            p.update(schema='native_perp_pair_v1', leverage=3, max_gap='0.0025', clip_quantity='0.005')
            p['maker']['venue'] = 'hyperliquid'
        return p

    async def exercise(self, *, native=False, close=False, answer='yes', prepare_error=False,
                       mutate=False, live=False, mode=None):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'rust/target/release/paired-order.exe'
            binary.parent.mkdir(parents=True)
            binary.touch()
            plan = self.plan(native, close)
            state = dict(commands=[], text='')

            async def prepare(args, *, project_root):
                self.assertEqual(args.rust_action, 'prepare')
                self.assertFalse(getattr(args, 'live', False))
                self.assertTrue(args.rust_quiet)
                if prepare_error:
                    raise RuntimeError('preparation failed')
                state['mode'] = args.mode
                path = Path(args.rust_plan)
                path.write_text(json.dumps(plan), encoding='utf-8')
                state['plan'] = path
                return path

            async def execute(args):
                state['commands'].append(build_native_command(args, project_root=root))
                self.assertEqual(args.rust_action, 'submit')
                self.assertFalse(getattr(args, 'rust_baseline', None))
                self.assertTrue(args.live)
                self.assertTrue(args.rust_quiet)
                self.assertEqual(json.loads(Path(args.rust_plan).read_text()), plan)
                self.assertFalse(Path(args.rust_run_dir).exists())

            def confirm(label):
                self.assertEqual([c[1] for c in state['commands']], [])
                if mutate:
                    state['plan'].write_text('{}')
                if isinstance(answer, Exception):
                    raise answer
                return answer

            output = io.StringIO()
            with patch('hydra_basis.execution_engine.rust_interactive_order.prepare_signal_plan', new=prepare), \
                 patch('hydra_basis.execution_engine.rust_interactive_order.run_native_preflight', new=execute), \
                 patch('hydra_basis.execution_engine.rust_interactive_order.prompt', return_value='開倉') as prompt, \
                 patch('builtins.input', side_effect=confirm) as confirmation, redirect_stdout(output):
                try:
                    await run_interactive_order(argparse.Namespace(mode=mode, live=live), project_root=root)
                except RuntimeError as exc:
                    state['error'] = str(exc)
            state['text'] = output.getvalue()
            state['confirm_count'] = confirmation.call_count
            state['mode_prompt_count'] = prompt.call_count
            state['reviewed_files'] = len(list(root.glob('rust/artifacts/order-*/reviewed.json')))
            return state

    async def test_confirmation_submits_without_global_account_audit(self):
        for native in (False, True):
            with self.subTest(native=native):
                s = await self.exercise(native=native)
                self.assertNotIn('error', s)
                self.assertEqual([c[1] for c in s['commands']], ['submit'])
                self.assertNotIn('--baseline', s['commands'][0])
                self.assertIn('--live', s['commands'][0])
                self.assertIn('--parent-control', s['commands'][0])
                self.assertEqual(s['confirm_count'], 1)
                self.assertEqual(s['reviewed_files'], 0)
                self.assertIn('0.01', s['text'])
                self.assertIn('3x', s['text'])
                self.assertNotIn('0x' + 'a' * 40, s['text'])
                self.assertEqual(s['mode'], 'open')

    async def test_decline_blank_and_eof_never_review_or_submit(self):
        for answer in ('n', '', 'no', EOFError()):
            with self.subTest(answer=answer):
                s = await self.exercise(answer=answer)
                self.assertEqual([c[1] for c in s['commands']], [])
                self.assertEqual(s['reviewed_files'], 0)
                self.assertIn('已取消', s['text'])

    async def test_prepare_error_cannot_reach_confirmation(self):
        for args in ({'prepare_error': True},):
            with self.subTest(args=args):
                s = await self.exercise(**args)
                self.assertIn('error', s)
                self.assertEqual(s['confirm_count'], 0)
                self.assertEqual(s['reviewed_files'], 0)
                self.assertEqual([c[1] for c in s['commands']], [])

    async def test_modified_plan_during_confirmation_rejected(self):
        s = await self.exercise(mutate=True)
        self.assertIn('改變', s['error'])
        self.assertEqual(s['reviewed_files'], 0)
        self.assertEqual([c[1] for c in s['commands']], [])

    async def test_live_flag_cannot_bypass_interactive_confirmation(self):
        s = await self.exercise(live=True)
        self.assertIn('--rust-action', s['error'])
        self.assertEqual(s['commands'], [])
        self.assertEqual(s['confirm_count'], 0)

    async def test_explicit_close_is_preserved_without_mode_prompt(self):
        s = await self.exercise(close=True, mode='close')
        self.assertEqual(s['mode'], 'close')
        self.assertEqual(s['mode_prompt_count'], 0)
        self.assertIn('平倉不修改', s['text'])
        self.assertEqual([c[1] for c in s['commands']], ['submit'])


if __name__ == '__main__':
    unittest.main()
