"""Serving-side path helpers for the dashboard.

The dashboard owns the avatar/thumbnail byte caches it serves; it does NOT own
the lake or the media cache the pipeline fills. Those few helpers are declared
here rather than imported from the orchestration layer, so the dashboard's
dependency on that layer is zero — the boundary the creator-roster cutover
establishes (the pipeline reads the roster over the HTTP API, never the reverse).

Root resolution mirrors `orchestration.defs.platform.paths`: `IG_DATA_DIR` first,
then a walk to the `.git` marker. Counting `parents[N]` is what silently
repointed `data/` when the dashboard moved under `services/`; the walk cannot.
"""

from __future__ import annotations

import os
from pathlib import Path


def _repo_root() -> Path:
    """Repo root — the first ancestor of this file carrying a `.git` entry."""
    here = Path(__file__).resolve()
    for candidate in (here.parent, *here.parents):
        if (candidate / ".git").exists():
            return candidate
    raise RuntimeError(
        f"cannot locate the repository root above {here}: no ancestor carries "
        "a `.git` entry; set IG_DATA_DIR explicitly"
    )


def _default_data_dir() -> Path:
    """`IG_DATA_DIR` when set, else `<repo>/data` via the marker walk."""
    configured = os.environ.get("IG_DATA_DIR")
    if configured:
        return Path(configured)
    return _repo_root() / "data"


DATA_DIR = _default_data_dir()

DB_PATH = Path(os.environ.get("IG_DB_PATH", str(DATA_DIR / "state.duckdb")))
OPS_PATH = Path(os.environ.get("OPS_DB_PATH", str(DATA_DIR / "ops.sqlite")))

MEDIA_ROOT = DATA_DIR / "media"
THUMBNAIL_DIR = MEDIA_ROOT / "thumbnails"
AVATAR_DIR = MEDIA_ROOT / "avatars"


def thumbnail_path(shortcode: str) -> Path:
    """Path to a cached post thumbnail on disk."""
    THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
    return THUMBNAIL_DIR / f"{shortcode}.jpg"


def avatar_path(username: str) -> Path:
    """Path to a cached profile picture on disk."""
    AVATAR_DIR.mkdir(parents=True, exist_ok=True)
    return AVATAR_DIR / f"{username}.jpg"
