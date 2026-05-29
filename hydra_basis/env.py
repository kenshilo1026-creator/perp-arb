from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_environment(dotenv_path: str | Path | None = None) -> None:
    if dotenv_path is not None:
        load_dotenv(dotenv_path=dotenv_path, override=False)
        return

    repo_root = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=repo_root / ".env", override=False)
