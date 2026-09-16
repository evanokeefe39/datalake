"""Parquet lake path helpers — env-overridable, partition-key → file path.

This module reads the environment into module constants at import time, so it
loads `.env` first — the one sanctioned import-time side effect in the
codebase. Without it, `.env`-provided `IG_*` paths are silently ignored
whenever this module imports before a caller that loads the environment (ADR-0015).
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _env_path(key: str, default: str) -> Path:
    return Path(os.environ.get(key, default))


# Anchor the default data dir to the repository root so the dashboard server
# (run from dashboard/) and the pipeline (run from root) resolve the same
# path regardless of cwd. Still overridable via IG_DATA_DIR.
#
# The root is found by walking up to the `.git` marker rather than by counting
# parents: a fixed `parents[N]` silently repoints `data/` — and with it the
# whole media corpus — the moment this file moves, which it just did (ADR-0015).
def _repo_root() -> Path:
    """Repo root — the first ancestor of this file carrying a `.git` entry."""
    here = Path(__file__).resolve()
    for candidate in (here.parent, *here.parents):
        if (candidate / ".git").exists():
            return candidate
    raise RuntimeError(
        f"cannot locate the repository root above {here}: no ancestor carries a "
        "`.git` entry; set IG_DATA_DIR explicitly"
    )


def _default_data_dir() -> Path:
    """`<repo>/data` via the marker walk — only when nothing configured it.

    Lazy on purpose: a container built from a `.dockerignore`d context has no
    `.git`, so an eager `_repo_root()` call at import time would make the whole
    package unimportable there even though `IG_DATA_DIR` names the root
    explicitly. Placing the env check BEFORE the walk is what lets the walk keep
    raising loudly on a host that configured nothing.
    """
    configured = os.environ.get("IG_DATA_DIR")
    if configured:
        return Path(configured)
    return _repo_root() / "data"


class _LazyRoot:
    """`paths._PROJECT_ROOT` — the repo root, walked only on first access.

    Kept as an attribute (not a function) because `_PROJECT_ROOT` is the public
    name tests and callers assert against, and it must keep raising loudly when
    no walk can succeed.
    """

    __slots__ = ("_value",)

    def __init__(self) -> None:
        self._value: Path | None = None

    def _resolve(self) -> Path:
        if self._value is None:
            self._value = _repo_root()
        return self._value

    def __fspath__(self) -> str:
        return str(self._resolve())

    def __truediv__(self, other: str) -> Path:
        return self._resolve() / other

    def __eq__(self, other: object) -> bool:
        return self._resolve() == other

    def __hash__(self) -> int:
        return hash(self._resolve())

    def __repr__(self) -> str:
        return repr(self._resolve())


_PROJECT_ROOT = _LazyRoot()

DATA_DIR = _default_data_dir()

BRONZE_LAKE = _env_path("IG_BRONZE_DIR", str(DATA_DIR / "lake" / "bronze"))
SILVER_LAKE = _env_path("IG_SILVER_DIR", str(DATA_DIR / "lake" / "silver"))
GOLD_LAKE = _env_path("IG_GOLD_DIR", str(DATA_DIR / "lake" / "gold"))


# ── Persisted-path translation (host ↔ container) ──────────────────────────
#
# Paths are persisted in `ops.sqlite` media_cache as whatever the WRITER's
# filesystem looked like — today Windows-absolute (`C:\...\data\media\posts\x`).
# A Linux container reading that store cannot open those paths, and the media
# cache looks empty rather than misconfigured. `runtime_path` translates a
# stored path into one THIS process can open, from a configured pair of
# prefixes.
#
# Empty `IG_HOST_PATH_PREFIX` means identity — a host run, unchanged behaviour.
# A stored path outside the configured host prefix passes through unchanged: it
# may already be runtime-valid (e.g. written by a containerized run).

_HOST_PATH_PREFIX = os.environ.get("IG_HOST_PATH_PREFIX", "").strip()
_CONTAINER_PATH_PREFIX = (
    os.environ.get("IG_CONTAINER_PATH_PREFIX", "/data").strip().rstrip("/") or "/data"
)


def _normalize(path: str) -> str:
    """Comparison form: separators unified, trailing slash dropped, case-folded."""
    return path.replace("\\", "/").rstrip("/").lower()


def runtime_path(stored: str) -> Path:
    """Translate a persisted path into one THIS process can open.

    Identity when `IG_HOST_PATH_PREFIX` is unset, or when `stored` does not sit
    under the configured host prefix. Comparison and rewrite are 1:1 in length —
    the prefix is matched on its normalized form and the ORIGINAL-case suffix is
    appended to the container prefix, so a path's real casing survives.
    """
    if not stored:
        return Path(stored)
    if not _HOST_PATH_PREFIX:
        return Path(stored)

    stored_slashed = stored.replace("\\", "/")
    host_slashed = _HOST_PATH_PREFIX.replace("\\", "/").rstrip("/")
    if not _normalize(stored_slashed).startswith(_normalize(host_slashed)):
        return Path(stored)

    suffix = stored_slashed[len(host_slashed):].lstrip("/")
    return Path(f"{_CONTAINER_PATH_PREFIX}/{suffix}" if suffix else _CONTAINER_PATH_PREFIX)


def bronze_path(dataset_id: str) -> Path:
    """Path to a bronze dataset Parquet file."""
    BRONZE_LAKE.mkdir(parents=True, exist_ok=True)
    return BRONZE_LAKE / f"{dataset_id}.parquet"


def silver_path(dataset_id: str) -> Path:
    """Path to a silver dataset Parquet file."""
    SILVER_LAKE.mkdir(parents=True, exist_ok=True)
    return SILVER_LAKE / f"{dataset_id}.parquet"


def gold_path(post_id: str) -> Path:
    """Path to a gold post analysis Parquet file."""
    GOLD_LAKE.mkdir(parents=True, exist_ok=True)
    return GOLD_LAKE / f"{post_id}.parquet"


def bronze_glob() -> str:
    """Glob for all bronze Parquet files — usable in ``read_parquet()``."""
    return str(BRONZE_LAKE / "*.parquet")


def silver_glob() -> str:
    """Glob for all silver Parquet files."""
    return str(SILVER_LAKE / "*.parquet")


def gold_glob() -> str:
    """Glob for all gold Parquet files."""
    return str(GOLD_LAKE / "*.parquet")


# ── Media cache (dashboard + pipeline shared paths) ──────────────────────

MEDIA_ROOT = DATA_DIR / "media"
THUMBNAIL_DIR = MEDIA_ROOT / "thumbnails"
AVATAR_DIR = MEDIA_ROOT / "avatars"
POST_MEDIA_DIR = MEDIA_ROOT / "posts"


def thumbnail_path(shortcode: str) -> Path:
    """Path to a cached post thumbnail on disk."""
    THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
    return THUMBNAIL_DIR / f"{shortcode}.jpg"


def avatar_path(username: str) -> Path:
    """Path to a cached profile picture on disk."""
    AVATAR_DIR.mkdir(parents=True, exist_ok=True)
    return AVATAR_DIR / f"{username}.jpg"


def post_media_path(cache_key: str) -> Path:
    """Path to a scrape-time cached post media file (image/video bytes)."""
    POST_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    return POST_MEDIA_DIR / cache_key
