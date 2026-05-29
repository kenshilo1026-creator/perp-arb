from __future__ import annotations

import sys
from pathlib import Path


def ensure_project_root_on_path() -> None:
    project_root = Path(__file__).resolve().parent.parent
    project_root_str = str(project_root)
    if sys.path and sys.path[0] == project_root_str:
        return
    sys.path.insert(0, project_root_str)
