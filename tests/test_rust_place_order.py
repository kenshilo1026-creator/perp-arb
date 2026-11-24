import argparse
import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, AsyncMock
from hydra_basis.execution_engine.rust_place_order import build_native_command


class NativePlaceOrderTests(unittest.TestCase):
    def options(self, **overrides):
        values = dict(rust_preflight=True, extension_id="a" * 32, rust_account=None,
                      rust_order_id=None, rust_output="new-private.jsonl")
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_read_only_handoff_is_argument_list_without_python_broker(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "rust/target/debug/variational-bridge-probe.exe"
            binary.parent.mkdir(parents=True)
            binary.touch()
            command = build_native_command(self.options(), project_root=root)
            self.assertEqual(command[0], str(binary))
            self.assertIn("--native-preflight", command)
            self.assertNotIn("--live", command)

    def test_missing_action_never_falls_back(self):
        with self.assertRaisesRegex(RuntimeError, "--rust-action required"):
            build_native_command(self.options(rust_preflight=False), project_root=Path("."))

    def test_exact_query_requires_account_and_rejects_trade_flags(self):
        for options in (self.options(rust_order_id="rfq"), self.options(mode="open"),
                        self.options(extension_id="invalid")):
            with self.assertRaises(RuntimeError):
                build_native_command(options, project_root=Path("."))

    def test_missing_binary_fails_before_any_execution(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "Build first"):
                build_native_command(self.options(), project_root=Path(temp))

    def test_inventory_requires_full_account_scope(self):
        for options in (self.options(rust_inventory=True),
                        self.options(rust_inventory=True, rust_account="0x" + "a" * 40, rust_order_id="rfq")):
            with self.assertRaisesRegex(RuntimeError, "--rust-inventory requires"):
                build_native_command(options, project_root=Path("."))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "rust/target/debug/variational-bridge-probe.exe"
            binary.parent.mkdir(parents=True)
            binary.touch()
            command = build_native_command(self.options(rust_inventory=True, rust_account="0x" + "a" * 40), project_root=root)
            self.assertIn("--inventory", command)
            self.assertNotIn("--native-order-id", command)
            self.assertNotIn("--live", command)

    def test_paired_submit_requires_explicit_live_plan_baseline_and_new_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "rust/target/release/paired-order.exe"
            binary.parent.mkdir(parents=True)
            binary.touch()
            options = self.options(rust_preflight=False, extension_id=None, rust_output=None,
                                   rust_action="submit", rust_plan="a plan.json", rust_baseline="b.json", rust_run_dir="run1", live=True)
            command = build_native_command(options, project_root=root)
            self.assertEqual(command[:2], [str(binary), "submit"])
            self.assertIn("a plan.json", command)
            self.assertIn("--live", command)
            self.assertIn("--parent-control", command)
            options.live = False
            with self.assertRaisesRegex(RuntimeError, "require --live"):
                build_native_command(options, project_root=root)

    def test_recovery_cannot_replace_original_plan_and_read_actions_cannot_trade(self):
        for overrides in (
            dict(rust_action="recover", live=True, rust_run_dir="old", rust_plan="replacement.json"),
            dict(rust_action="audit", live=True, rust_plan="p.json", rust_baseline_output="b.json"),
            dict(rust_action="submit", live=True, rust_plan="p.json", rust_run_dir="new"),
            dict(rust_action="submit", live=True, rust_plan="p.json", rust_baseline="b.json", rust_run_dir="new", mode="open"),
        ):
            options = self.options(rust_preflight=False, extension_id=None, rust_output=None, **overrides)
            with self.assertRaises(RuntimeError):
                build_native_command(options, project_root=Path("."))

    def test_preflight_cannot_silently_ignore_live_flag(self):
        with self.assertRaisesRegex(RuntimeError, "cannot be combined"):
            build_native_command(self.options(live=True), project_root=Path("."))


class NativeProcessLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_cancellation_requests_cleanup_and_never_terminates_child(self):
        from hydra_basis.execution_engine.rust_place_order import run_native_preflight
        started, stopped = asyncio.Event(), asyncio.Event()

        class Process:
            returncode = None
            def __init__(self):
                self.stdin = self
                self.written = []
                self.terminated = False
            async def wait(self):
                started.set()
                await stopped.wait()
                return self.returncode
            def write(self, data):
                self.written.append(data)
            async def drain(self):
                self.returncode = 1
                stopped.set()
            def terminate(self):
                self.terminated = True

        process = Process()
        with patch('hydra_basis.execution_engine.rust_place_order.build_native_command', return_value=['fixture']), \
             patch('asyncio.create_subprocess_exec', new=AsyncMock(return_value=process)):
            task = asyncio.create_task(run_native_preflight(argparse.Namespace(live=True)))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(process.written, [b'STOP\n'])
        self.assertFalse(process.terminated)


if __name__ == "__main__":
    unittest.main()
