"""Shared fixtures for the dashboard tests.

The dashboard server is loaded by PATH (it is a script, not an importable
package), so these tests need its location. That location is resolved by
walking ancestors for the `.git` marker, never by counting `parents[N]`: this
suite moved from `dashboard/` to `services/dashboard/`, and a fixed depth would
have silently pointed at whatever file sat at the old depth — the exact break
class the reorg introduced elsewhere.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def repo_root() -> Path:
    """The first ancestor of this file carrying a `.git` entry."""
    here = Path(__file__).resolve()
    for candidate in (here.parent, *here.parents):
        if (candidate / ".git").exists():
            return candidate
    raise RuntimeError(f"cannot locate the repository root above {here}")


@pytest.fixture(scope="session")
def dashboard_server_path() -> Path:
    """Path to the dashboard's server.py, wherever the dashboard currently lives."""
    path = repo_root() / "services" / "dashboard" / "server.py"
    assert path.exists(), f"dashboard server not found at {path}"
    return path
