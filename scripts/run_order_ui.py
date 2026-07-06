from __future__ import annotations

import argparse

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from hydra_basis.env import load_environment
from hydra_basis.runtime import configure_windows_event_loop_policy
from web.app import create_app

load_environment()


def build_app():
    return create_app()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the order UI web app (localhost only).")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    import uvicorn

    configure_windows_event_loop_policy()
    print(f"order UI running at http://127.0.0.1:{args.port}/")
    uvicorn.run(build_app(), host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
