import argparse
import asyncio
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from hydra_basis.execution_engine.rust_spot_perp import build_command, build_preflight_command, build_recover_command


class NativeSpotPerpTests(unittest.TestCase):
    def test_private_preflight_requires_explicit_readonly_action(self):
        args = argparse.Namespace(live=False, rust_watch_seconds=10)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            exe = root/'rust/target/release/spot-perp.exe'
            exe.parent.mkdir(parents=True)
            exe.touch()
            cmd = build_preflight_command(args, root=root)
            self.assertEqual(cmd, [str(exe), 'binance-preflight', '--watch-seconds', '10'])
            args.live = True
            with self.assertRaisesRegex(RuntimeError, 'read-only'):
                build_preflight_command(args, root=root)
            args.live = False
            args.rust_watch_seconds = 0
            with self.assertRaises(ValueError):
                build_preflight_command(args, root=root)

    def test_private_preflight_does_not_prompt_for_orders_or_fetch_public_books(self):
        from scripts import run_spot_perp_arbitrage as runner
        args = argparse.Namespace(engine='rust', live=False, rust_binance_preflight=True)
        with patch.object(runner, 'parse_args', return_value=args), \
             patch('builtins.input') as prompt, \
             patch.object(runner, 'fetch_plan_books', new=AsyncMock()) as books, \
             patch('hydra_basis.execution_engine.rust_spot_perp.run_preflight', new=AsyncMock()) as preflight:
            asyncio.run(runner.run_spot_perp_arbitrage())
            preflight.assert_awaited_once()
            prompt.assert_not_called()
            books.assert_not_awaited()
    def build(self, root, **changes):
        venue = changes.pop('venue', 'aster')
        args = argparse.Namespace(live=False, clip_size=None, rust_watch_seconds=0)
        for key, value in changes.items():
            setattr(args, key, value)
        return build_command(args, root=root, mode='close', symbol='SYN', venue=venue,
                             total=Decimal('1000'), max_gap=Decimal('.0025'))

    def test_release_only_handoff_preserves_gap_and_amount(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            exe = root/'rust/target/release/spot-perp.exe'
            exe.parent.mkdir(parents=True)
            exe.touch()
            cmd = self.build(root, clip_size='100', rust_watch_seconds=30)
            self.assertEqual(cmd[:2], [str(exe), 'preview'])
            self.assertEqual(cmd[cmd.index('--max-gap')+1], '0.0025')
            self.assertEqual(cmd[cmd.index('--total-size')+1], '1000')
            self.assertNotIn('--live', cmd)

    def test_live_requires_persistent_run_before_binary_check(self):
        with self.assertRaisesRegex(RuntimeError, 'run-dir'):
            self.build(Path('not-existing'), live=True)

    def test_live_roles_and_explicit_recovery_are_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            exe = root/'rust/target/release/spot-perp.exe'
            exe.parent.mkdir(parents=True)
            exe.touch()
            for venue in ('aster', 'hyperliquid'):
                cmd = self.build(root, live=True, venue=venue, rust_run_dir=str(root/'run'))
                self.assertEqual(cmd[1], 'live')
                self.assertNotIn('--watch-seconds', cmd)
                self.assertIn('PLACE_LIVE_SPOT_PERP_ORDER', cmd)
            with self.assertRaisesRegex(RuntimeError, 'maker'):
                self.build(root, live=True, venue='lighter', rust_run_dir=str(root/'run'))
            with self.assertRaisesRegex(RuntimeError, 'leverage'):
                self.build(root, live=True, leverage=5, rust_run_dir=str(root/'run'))
            args = argparse.Namespace(live=True, rust_run_dir=str(root/'run'))
            cmd = build_recover_command(args, root=root)
            self.assertEqual(cmd[1], 'recover')
            self.assertNotIn('--total-size', cmd)
            args.total_size = '10'
            with self.assertRaisesRegex(RuntimeError, 'original run'):
                build_recover_command(args, root=root)

    def test_missing_binary_does_not_fallback(self):
        with self.assertRaisesRegex(RuntimeError, 'Build first'):
            self.build(Path('not-existing'))

    def test_perp_selection_reaches_native_command_without_substitution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            exe = root/'rust/target/release/spot-perp.exe'
            exe.parent.mkdir(parents=True)
            exe.touch()
            for venue in ('aster', 'hyperliquid', 'lighter'):
                cmd = self.build(root, venue=venue)
                self.assertEqual(cmd[cmd.index('--perp-venue')+1], venue)

    def test_variational_indicative_quote_and_unknown_venues_are_not_tradable(self):
        with self.assertRaisesRegex(RuntimeError, 'extension'):
            self.build(Path('not-existing'), venue='variational')
        for venue in ('arcus', 'binance_spot', 'typo'):
            with self.assertRaisesRegex(RuntimeError, 'no Python fallback'):
                self.build(Path('not-existing'), venue=venue)

    def test_rust_live_handoff_does_not_start_python_books_or_execution(self):
        from scripts import run_spot_perp_arbitrage as runner
        args = argparse.Namespace(engine='rust', live=True, mode='close', symbol='SYN', short_venue='aster', total_size='1000')
        with patch.object(runner, 'parse_args', return_value=args), \
             patch.object(runner, 'fetch_plan_books', new=AsyncMock()) as books, \
             patch('hydra_basis.execution_engine.rust_spot_perp.run_live', new=AsyncMock()) as live, \
             patch('builtins.input') as prompt:
            asyncio.run(runner.run_spot_perp_arbitrage())
            live.assert_awaited_once()
            books.assert_not_awaited()
            prompt.assert_not_called()

    def test_recovery_does_not_prompt_new_symbol_or_quantity(self):
        from scripts import run_spot_perp_arbitrage as runner
        args = argparse.Namespace(engine='rust', live=True, rust_recover=True, rust_run_dir='saved')
        with patch.object(runner, 'parse_args', return_value=args), \
             patch('hydra_basis.execution_engine.rust_spot_perp.run_recover', new=AsyncMock()) as recover, \
             patch('builtins.input') as prompt:
            asyncio.run(runner.run_spot_perp_arbitrage())
            recover.assert_awaited_once()
            prompt.assert_not_called()

    def test_rust_preview_bypasses_python_books_and_execution(self):
        from scripts import run_spot_perp_arbitrage as runner
        args = argparse.Namespace(engine='rust', live=False, mode='close', symbol='SYN', short_venue='aster', total_size='1000')
        with patch.object(runner, 'parse_args', return_value=args), \
             patch.object(runner, 'fetch_plan_books', new=AsyncMock()) as books, \
             patch('hydra_basis.execution_engine.rust_spot_perp.run_preview', new=AsyncMock()) as preview:
            asyncio.run(runner.run_spot_perp_arbitrage())
            preview.assert_awaited_once()
            books.assert_not_awaited()

    def test_live_cancel_requests_cleanup_and_never_terminates_child(self):
        from hydra_basis.execution_engine.rust_spot_perp import _run_live

        async def check(root):
            started = asyncio.Event()
            done = asyncio.Event()
            process = Mock(returncode=None)

            async def wait():
                started.set()
                await done.wait()
                process.returncode = 0
                return 0

            process.wait = wait
            with patch('asyncio.create_subprocess_exec', new=AsyncMock(return_value=process)):
                task = asyncio.create_task(_run_live(['fake-binary'], root, root))
                await started.wait()
                task.cancel()
                for _ in range(20):
                    if (root/'STOP').exists():
                        break
                    await asyncio.sleep(.01)
                self.assertEqual((root/'STOP').read_text().strip(), 'STOP')
                process.terminate.assert_not_called()
                done.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task

        with tempfile.TemporaryDirectory() as tmp:
            asyncio.run(check(Path(tmp)))
