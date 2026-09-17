"""Repo-root resolution for tests that load files by path.

Several suites load a script (the dashboard server) or assert against a file
(the Dockerfile) by PATH rather than by import. Those paths must be resolved by
walking ancestors for the `.git` marker, never by counting `parents[N]`: the
workspace reorganization moved both of those files, and a fixed depth would
have silently pointed at whatever sat at the old depth — a suite that passes
while testing nothing.

`repo_root()` is the one implementation; `tests/unit/dashboard/conftest.py`
re-exports it as a fixture for convenience.
"""

from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    """The first ancestor of THIS file carrying a `.git` entry."""
    here = Path(__file__).resolve()
    for candidate in (here.parent, *here.parents):
        if (candidate / ".git").exists():
            return candidate
    raise RuntimeError(
        f"cannot locate the repository root above {here}: no ancestor carries "
        "a `.git` entry"
    )


def dashboard_server_path() -> Path:
    """Path to the dashboard server script, wherever the dashboard lives."""
    path = repo_root() / "services" / "dashboard" / "server.py"
    assert path.exists(), f"dashboard server not found at {path}"
    return path
