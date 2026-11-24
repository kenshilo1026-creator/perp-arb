"""Track existing source coverage without reading .env, data or untracked projects."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
DEST = ROOT / "rust/migration-inventory.json"


def phase(path):
    if path.startswith("tests/"):
        return "validation_each_batch"
    if path.startswith("browser_extensions/"):
        return "7_browser_bridge"
    if path.startswith("web/") or path == "scripts/run_order_ui.py":
        return "8_web"
    if path.startswith("configs/"):
        return "config_compatibility_each_batch"
    if path.startswith("hydra_basis/risk_management/") or path == "scripts/run_risk_manager.py":
        return "6_risk_persistence"
    if path.startswith(("hydra_basis/execution_engine/", "hydra_basis/spread_strategy/")):
        if path.endswith(("/feeds.py", "/dispatcher.py", "/instruments.py")):
            return "2_market_then_5_strategy"
        if path.endswith(("/core.py", "/maker_price_guard.py")):
            return "1_math_then_4_execution"
        return "3_private_events_then_4_execution_then_5_strategy"
    if path.startswith(("hydra_basis/adapters/", "hydra_basis/streams/")):
        return "2_market_then_4_venue_execution"
    if path.startswith(("hydra_basis/funding_engine/", "hydra_basis/spread_monitor/", "hydra_basis/notifications/")):
        return "7_funding_monitor"
    if path.startswith("scripts/"):
        return "cli_parity_each_batch"
    if path.startswith("hydra_basis/") or path == "funding_arbitrage_monitor_demo.py":
        return "7_funding_history_shared_utilities"
    return "review"


def snapshot():
    names = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT).decode("utf-8").split("\0")
    prefixes = ("hydra_basis/", "scripts/", "tests/", "web/", "browser_extensions/", "configs/")
    selected = sorted(p for p in names if p.startswith(prefixes) or p in {
        "funding_arbitrage_monitor_demo.py", "requirements.txt", "requirements-web.txt"})
    partial = {
        "hydra_basis/execution_engine/rust_spot_perp.py": "Read-only release-binary handoff for public preview and explicit Binance account/private WS preflight; live rejection, no fallback and readonly child cleanup",
        "scripts/run_spot_perp_arbitrage.py": "Explicit native live/recover owner: Aster/Hyperliquid post-only maker to Binance MARKET, WS cumulative partial fills, best-level batching, price-only repricing, cancel races, durable reservations/dispatch fences, fee net-base totals and final local timing. Python handoff retains confirmation and graceful stop; existing exchange leverage. Public preview also supports Lighter/both roles. Offline state-machine/fault tests only; no real trading acceptance, Binance-maker reverse route, Lighter/Variational spot execution, complete account/PnL registry or guaranteed zero dust exposure",
        "hydra_basis/execution_engine/binance_spot_adapter.py": "Native HMAC REST, MARKET/LIMIT_MAKER single-attempt submission with persistent dispatch permit, exact query/cancel, WS signed subscription/executionReport, fee trade-ID dedup and cumulative actor reconciliation. Local HTTP/WS tests and real read-only account/WS preflight passed; no real trading acceptance, auto reconnect owner, complete account identity/locking, full fee pagination/persistence, batch execution or spot position registry parity",
        "hydra_basis/execution_engine/depth_sizing.py": "Rust spot-perp top_level_only sizing, common limit/market lot and min/max notional, tail reservation and optional clip cap; 2000 deterministic decision fixtures. No multi-level VWAP mode or live quantity changes",
        "scripts/place_order.py": "Explicit Rust paired-order plan/preview/audit/submit/inspect/recover handoff; Variational fixed-limit maker and Aster/Hyperliquid IOC, independent reviewed inventory, durable identities and bounded recovery. Offline/loopback runner and real read-only account checks passed; no real trade acceptance, automatic reprice, Python position registry or PnL parity",
        "hydra_basis/execution_engine/rust_place_order.py": "Argument-list subprocess handoff, no Python fallback, live STOP control and bounded child cleanup; native trading path lives in Rust variational-bridge paired module",
        "hydra_basis/execution_engine/order_stream.py": "Aster cumulative decoder, per-account coordination, bounded ingress, per-order cursors, WS/REST dedup and gap/query fencing; Aster private transport plus short live authentication/renewal/local socket rotation verified without trading; Hyperliquid separate cumulative orders/trade dedup and independent query recovery; scoped account reconciliation and special-event routing; paper-only maker/hedge watches; single-order journal bridge in manual test runner; no MEXC protobuf or automated two-leg execution",
        "hydra_basis/execution_engine/aster_adapter.py": "V3 EIP-712 read-request authentication, per-process signer nonce sharing, fixed POST/PUT listenKey and GET order transport with quotas/timeouts/redacted errors; Python auth vectors, loopback WS/HTTP and short live renewal/rotation tests; read-only double-sample positions, wallet balances and open orders with weighted quota and WS revision fencing; explicit single-limit submit/cancel transport and complete-intent manual test runner with loopback validation; no real Aster fill/query acceptance, margin/leverage management or automated strategy wiring",
        "hydra_basis/execution_engine/hedge_safety.py": "Paper cumulative hedge reservation, terminal-only residual retry parity and signed exposure; single-writer journal for known-maker/hedge intents, sync-before-dispatch permit and unknown-client-ID read-only lookup; no broker, compensation, full cash ledger or live unknown-submission acceptance",
        "hydra_basis/execution_engine/executor.py": "Paper single-maker cumulative obligations and hedge reservation, private coordinator watches; optional durable intents/client IDs and restart replay with process-exit/corruption/fault fixtures; separate single-limit test runner has complete intent persistence, submit/cancel and explicit client-ID resume; no automated two-leg real execution, reprice, batching, background recovery or full-account persistence",
        "hydra_basis/spread_strategy/core.py": "spread_bps and round_step only",
        "hydra_basis/spread_strategy/feeds.py": "Ticker/depth math/freshness; Aster/Hyperliquid depth+BBO shared read-only cache and StoreFeed common selection policy; stricter conflict invalidation tested separately; no other venues or strategy wiring",
        "hydra_basis/spread_strategy/dispatcher.py": "QuoteStore common read-only selection/revisions; Aster/Hyperliquid per-venue multi-symbol WS ownership, bounded controls/pacing, consumer and metadata handoff; no scanner, group strategy scheduling, other venues or trading dispatcher",
        "hydra_basis/execution_engine/maker_price_guard.py": "adverse_price_gap only; no bounded maker price or guard lifecycle",
        "hydra_basis/spread_strategy/instruments.py": "Aster/Hyperliquid metadata parsing, rounding/static checks/common lot; public HTTP v1/v3/meta reads with pooled client, shared per-venue weight/cooldown and versioned refresh cache verified separately; no other venues, cross-process limiter or execution wiring",
        "hydra_basis/symbol_mapping.py": "Provided JSON mapping precedence and native/canonical lookup for current config; strict malformed/case-collision validation; no filesystem default-path cache",
        "hydra_basis/execution_engine/hyperliquid_adapter.py": "Instrument up/down rounding and szDecimals; orderStatus quantity/terminal parity, address-only private orderUpdates/userFills subscriptions, unaggregated trade dedup, ACK/snapshot gating and independent bounded REST polling. Funding/nonfunding ledger invalidation, scoped per-dex positions/cash, spot balances and open orders; special events remain pending independent expected-ledger proof, never zero fills. Paper cumulative execution bridge only. Account-bound mainnet/testnet EIP-712 order/cancel signing and manual complete-intent test runner with eight Python signature vectors and loopback validation; no real live order acceptance, durable full-account recovery or default HALF_EVEN wire formatter",
    }
    files = []
    for path in selected:
        bucket = "dependencies_each_batch" if path.startswith("requirements") else phase(path)
        files.append(dict(path=path, sha256=hashlib.sha256((ROOT / path).read_bytes()).hexdigest(),
                          phase=bucket, status="partial_verified" if path in partial else "planned",
                          ported_scope=partial.get(path)))
    return dict(schema=1, files=files,
                excluded="Untracked variational-v1, secrets, live data, generated caches and git internals are not copied.",
                notes="Partial verification covers only the listed scope; no production replacement; manual Aster/Hyperliquid single-order test capability is separate from automated strategy parity. JS/HTML/CSS remain browser assets; Rust replaces their backend/bridge where appropriate.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    current = snapshot()
    if args.check:
        saved = json.loads(DEST.read_text(encoding="utf-8"))
        if saved != current:
            raise SystemExit("Source inventory changed: review new/modified files before refreshing it.")
        print(f"Inventory matches: {len(current['files'])} source/config/test/assets files")
    else:
        DEST.write_text(json.dumps(current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(dict(files=len(current["files"]), phases=dict(Counter(f["phase"] for f in current["files"]))), indent=2))


if __name__ == "__main__":
    main()
