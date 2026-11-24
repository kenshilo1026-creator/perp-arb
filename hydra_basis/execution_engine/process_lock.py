"""Cross-language lock for the manual Python/Rust execution CLIs."""
from pathlib import Path
import tempfile
from hydra_basis.spread_strategy.locks import symbol_lock


def execution_process_lock():
    return symbol_lock(Path(tempfile.gettempdir()) / "perp-arb-live-execution.lock")
