"""Parquet lake path helpers — env-overridable, partition-key → file path."""

from __future__ import annotations

import os
from pathlib import Path


def _env_path(key: str, default: str) -> Path:
    return Path(os.environ.get(key, default))


# Anchor the default data dir to the repository root so the dashboard server
# (run from dashboard/) and the pipeline (run from root) resolve the same
# path regardless of cwd. Still overridable via IG_DATA_DIR.
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
DATA_DIR = _env_path("IG_DATA_DIR", str(_PROJECT_ROOT / "data"))

BRONZE_LAKE = _env_path("IG_BRONZE_DIR", str(DATA_DIR / "lake" / "bronze"))
SILVER_LAKE = _env_path("IG_SILVER_DIR", str(DATA_DIR / "lake" / "silver"))
GOLD_LAKE = _env_path("IG_GOLD_DIR", str(DATA_DIR / "lake" / "gold"))


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
