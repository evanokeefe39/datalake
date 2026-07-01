"""Parquet lake path helpers — env-overridable, partition-key → file path."""

from __future__ import annotations

import os
from pathlib import Path


def _env_path(key: str, default: str) -> Path:
    return Path(os.environ.get(key, default))


DATA_DIR = _env_path("IG_DATA_DIR", "data")

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
