"""Native Rust preflight / bounded paired-order handoff. No Python broker fallback."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path


def build_native_command(args, *, project_root: Path) -> list[str]:
    from hydra_basis.execution_engine.rust_plan_prepare import PREPARE_OPTIONS
    if any(getattr(args, key, None) is not None for key in PREPARE_OPTIONS):
        raise RuntimeError("settings/quantity/price/fill-mode options require --rust-action prepare; existing plans/runs cannot be overridden")
    if not getattr(args, "rust_preflight", False):
        return build_paired_command(args, project_root=project_root)
    if any(getattr(args, k, None) for k in ("rust_action", "rust_plan", "rust_run_dir", "rust_baseline", "rust_baseline_output", "live")):
        raise RuntimeError("--rust-preflight cannot be combined with paired order options")
    extension_id = getattr(args, "extension_id", None)
    if not extension_id or len(extension_id) != 32 or any(c not in "abcdefghijklmnop" for c in extension_id):
        raise RuntimeError("--extension-id requires the 32-character Chrome extension ID")
    account = getattr(args, "rust_account", None)
    order_id = getattr(args, "rust_order_id", None)
    inventory = getattr(args, "rust_inventory", False)
    if inventory and (not account or order_id):
        raise RuntimeError("--rust-inventory requires --rust-account and no --rust-order-id")
    if order_id and not account:
        raise RuntimeError("--rust-order-id requires --rust-account; symbol matching is not used")
    if any(getattr(args, flag, None) for flag in ("mode", "ticker", "short_venue", "long_venue", "maker_venue", "taker_venue")):
        raise RuntimeError("--rust-preflight is read-only; omit trade mode/ticker/venue flags")
    binary = project_root / "rust/target/debug/variational-bridge-probe.exe"
    if not binary.is_file():
        raise RuntimeError("Build first: cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin variational-bridge-probe")
    output = getattr(args, "rust_output", None)
    if not output:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        directory = project_root / "rust/artifacts"
        directory.mkdir(exist_ok=True)
        output = str(directory / f"variational-native-{stamp}.jsonl")
    cmd = [str(binary), "--extension-id", extension_id, "--output", str(output), "--seconds", "45" if inventory else "30", "--native-preflight"]
    if inventory:
        cmd += ["--inventory"]
    if account:
        cmd += ["--native-account", account]
    if order_id:
        cmd += ["--native-order-id", order_id]
    return cmd


def build_paired_command(args, *, project_root: Path) -> list[str]:
    action = getattr(args, "rust_action", None)
    if action not in ("preview", "audit", "submit", "inspect", "recover"):
        raise RuntimeError("--rust-action required: preview/audit/submit/inspect/recover; no order sent")
    if any(getattr(args, k, None) for k in ("extension_id", "rust_account", "rust_order_id", "rust_output", "rust_inventory", "mode", "ticker", "short_venue", "long_venue", "maker_venue", "taker_venue")):
        raise RuntimeError("paired Rust parameters come exclusively from --rust-plan or the saved run; omit preflight/Python trade flags")
    live = bool(getattr(args, "live", False))
    if live != (action in ("submit", "recover")):
        raise RuntimeError("submit/recover require --live; preview/audit/inspect reject --live")
    required = {"preview": {"rust_plan"}, "audit": {"rust_plan", "rust_baseline_output"},
                "submit": {"rust_plan", "rust_run_dir"},
                "recover": {"rust_run_dir"}, "inspect": {"rust_run_dir"}}[action]
    options = {"rust_plan": "--plan", "rust_baseline": "--baseline", "rust_run_dir": "--run-dir", "rust_baseline_output": "--baseline-output"}
    supplied = {k for k in options if getattr(args, k, None)}
    allowed = required | ({"rust_baseline"} if action == "submit" else set())
    if not required <= supplied or not supplied <= allowed:
        raise RuntimeError(f"{action} requires exactly: " + ", ".join("--" + k.replace("_", "-") for k in sorted(required)))
    binary = project_root / "rust/target/release/paired-order.exe"
    if not binary.is_file():
        raise RuntimeError("Build first: cargo build --manifest-path rust/Cargo.toml -p perp-arb-variational-bridge --bin paired-order --release --locked")
    command = [str(binary), action]
    for k, flag in options.items():
        if k in supplied:
            command += [flag, str(getattr(args, k))]
    if live:
        command += ["--live", "--parent-control"]
    return command


async def run_native_preflight(args) -> None:
    root = Path(__file__).resolve().parents[2]
    command = build_native_command(args, project_root=root)
    live = bool(getattr(args, "live", False))
    quiet = bool(getattr(args, 'rust_quiet', False))
    process = await asyncio.create_subprocess_exec(*command, cwd=root,
        stdin=asyncio.subprocess.PIPE if live else None,
        stdout=asyncio.subprocess.PIPE if quiet else None,
        stderr=asyncio.subprocess.PIPE if quiet and not live else None)
    # Drain without communicate(): closing live stdin requests cancellation.
    readers = [asyncio.create_task(stream.read()) for stream in
               (process.stdout, process.stderr) if stream is not None] if quiet else []
    try:
        code = await asyncio.shield(process.wait())
    except asyncio.CancelledError:
        if live and process.returncode is None and process.stdin is not None:
            process.stdin.write(b"STOP\n")
            try:
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            print("[Rust] 正在撤回 Maker 並查證／對沖已成交部分；請勿刪除 run-dir。", flush=True)
            await asyncio.shield(process.wait())
        raise
    finally:
        if process.returncode is None and not live:
            # Read-only child has no live maker to cancel. Do not leave ports held.
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            await process.wait()
        if readers:
            await asyncio.gather(*readers)
    output = await asyncio.gather(*readers)
    if code != 0:
        for data in output:
            if data:
                print(data.decode('utf-8', errors='replace').rstrip(), flush=True)
        raise RuntimeError(f"Rust command incomplete/failed (exit {code}); inspect/recover the saved run if submitted. Python execution was not started")
