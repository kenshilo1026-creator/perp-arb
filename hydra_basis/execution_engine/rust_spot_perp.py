"""Native preview/live handoff; Python does not execute either trading leg."""
from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal
from pathlib import Path


def build_command(args, *, root: Path, mode: str, symbol: str, venue: str,
                  total: Decimal, max_gap: Decimal) -> list[str]:
    live = bool(getattr(args, "live", False))
    if live:
        if venue not in {"aster", "hyperliquid"}:
            raise RuntimeError("Rust live supports Aster/Hyperliquid maker -> Binance Spot taker only; no Python fallback")
        if not getattr(args, "rust_run_dir", None):
            raise RuntimeError("Rust live requires a new --rust-run-dir")
        if getattr(args, "leverage", None) is not None:
            raise RuntimeError("Rust uses existing exchange leverage; --leverage is not implemented")
        if getattr(args, "rust_watch_seconds", None):
            raise RuntimeError("--rust-watch-seconds is read-only; omit it for live")
    if venue == "variational":
        raise RuntimeError("Rust spot/perp Variational requires extension quotes and verified order constraints; indicative quotes cannot size a hedge. No Python fallback")
    if venue not in {"aster", "hyperliquid", "lighter"}:
        raise RuntimeError("Rust spot/perp supports Binance Spot with Aster/Hyperliquid/Lighter public preview; no Python fallback")
    if mode not in {"open", "close"} or not total.is_finite() or total <= 0:
        raise ValueError("invalid mode/total size")
    if not max_gap.is_finite() or not 0 < max_gap < 1:
        raise ValueError("invalid max adverse gap")
    binary = root / "rust/target/release/spot-perp.exe"
    if not binary.is_file():
        raise RuntimeError("Build first: cargo build --manifest-path rust/Cargo.toml -p perp-arb-spot-perp --release --locked")
    command = [str(binary), "live" if live else "preview", "--symbol", symbol, "--mode", mode,
               "--total-size", format(total, "f"), "--max-gap", format(max_gap, "f"),
               "--perp-venue", venue]
    if getattr(args, "clip_size", None) is not None:
        clip = Decimal(args.clip_size)
        if not clip.is_finite() or clip <= 0:
            raise ValueError("clip size must be positive and finite")
        command += ["--clip-size", format(clip, "f")]
    watch = getattr(args, "rust_watch_seconds", None) or 0
    if not 0 <= watch <= 3600:
        raise ValueError("watch seconds must be 0..3600")
    if live:
        command += ["--run-dir", str(Path(args.rust_run_dir).resolve()), "--confirm", "PLACE_LIVE_SPOT_PERP_ORDER"]
    else:
        command += ["--watch-seconds", str(watch)]
    return command


async def run_preview(args, *, root: Path, mode: str, symbol: str, venue: str,
                      total: Decimal, max_gap: Decimal) -> None:
    command = build_command(args, root=root, mode=mode, symbol=symbol, venue=venue, total=total, max_gap=max_gap)
    await _run_readonly(command, root)


def build_recover_command(args, *, root: Path) -> list[str]:
    if not getattr(args, "live", False) or not getattr(args, "rust_run_dir", None):
        raise RuntimeError("Recovery can cancel/hedge existing fills; requires --live and original --rust-run-dir")
    if any(getattr(args, key, None) for key in ("mode", "symbol", "short_venue", "total_size", "clip_size", "leverage", "rust_watch_seconds", "rust_binance_preflight")):
        raise RuntimeError("Recovery reads the original run; omit new trade/preview parameters")
    binary = root / "rust/target/release/spot-perp.exe"
    if not binary.is_file():
        raise RuntimeError("Build first: cargo build --manifest-path rust/Cargo.toml -p perp-arb-spot-perp --release --locked")
    return [str(binary), "recover", "--run-dir", str(Path(args.rust_run_dir).resolve()),
            "--confirm", "PLACE_LIVE_SPOT_PERP_ORDER"]


async def run_live(args, *, root: Path, mode: str, symbol: str, venue: str,
                   total: Decimal, max_gap: Decimal) -> None:
    if not getattr(args, "rust_run_dir", None):
        args.rust_run_dir = str(root / "rust/runs" / f"spot-{uuid.uuid4().hex}")
    Path(args.rust_run_dir).resolve().parent.mkdir(parents=True, exist_ok=True)
    command = build_command(args, root=root, mode=mode, symbol=symbol, venue=venue, total=total, max_gap=max_gap)
    print(f"Rust 原生現貨對沖：{mode} {symbol}，總量 {total}；{venue} Maker → Binance 現貨 Taker。")
    print(f"掛單不利價差上限 {max_gap:.2%}；使用交易所現有槓桿；run-dir={args.rust_run_dir}")
    print("成交後直接對沖；實際成交價差可能超過掛單門檻。")
    if input("Type PLACE LIVE SPOT PERP ORDER to continue: ") != "PLACE LIVE SPOT PERP ORDER":
        print("cancelled")
        return
    await _run_live(command, root, Path(args.rust_run_dir).resolve())


async def run_recover(args, *, root: Path) -> None:
    command = build_recover_command(args, root=root)
    print("恢復原 run：查證／撤回原 Maker，對沖已成交部分；不新增 Maker。")
    if input("Type PLACE LIVE SPOT PERP ORDER to continue: ") != "PLACE LIVE SPOT PERP ORDER":
        print("cancelled")
        return
    await _run_live(command, root, Path(args.rust_run_dir).resolve())


async def _run_live(command: list[str], root: Path, directory: Path) -> None:
    process = await asyncio.create_subprocess_exec(*command, cwd=root)
    try:
        code = await asyncio.shield(process.wait())
    except asyncio.CancelledError:
        print("[Rust] 正在撤單及查證／對沖已成交部分，請勿關閉終端或刪除 run-dir。", flush=True)
        while process.returncode is None and not directory.exists():
            await asyncio.shield(asyncio.sleep(.05))
        if process.returncode is None:
            (directory / "STOP").write_text("STOP\n", encoding="utf-8")
        await asyncio.shield(process.wait())
        raise
    if code != 0:
        raise RuntimeError(f"Rust 現貨對沖未完整完成 (exit {code})；查看 {directory}，必要時用 --rust-recover；沒有啟動 Python 下單")


def build_preflight_command(args, *, root: Path) -> list[str]:
    if getattr(args, "live", False):
        raise RuntimeError("Binance preflight is read-only; --live is not allowed")
    watch = getattr(args, "rust_watch_seconds", None)
    watch = 10 if watch is None else watch
    if not 1 <= watch <= 300:
        raise ValueError("private observation must be 1..300 seconds")
    binary = root / "rust/target/release/spot-perp.exe"
    if not binary.is_file():
        raise RuntimeError("Build first: cargo build --manifest-path rust/Cargo.toml -p perp-arb-spot-perp --release --locked")
    return [str(binary), "binance-preflight", "--watch-seconds", str(watch)]


async def run_preflight(args, *, root: Path) -> None:
    await _run_readonly(build_preflight_command(args, root=root), root)


async def _run_readonly(command: list[str], root: Path) -> None:
    process = await asyncio.create_subprocess_exec(*command, cwd=root)
    try:
        code = await process.wait()
    finally:
        if process.returncode is None:
            # These explicit CLI actions cannot trade; terminating reads is safe.
            process.terminate()
            await process.wait()
    if code != 0:
        raise RuntimeError(f"Rust read-only action failed (exit {code}); no Python execution started")
