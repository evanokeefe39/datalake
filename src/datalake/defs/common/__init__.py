"""Shared infrastructure — resources, path helpers, schedules."""

from .lake import (
    BRONZE_LAKE,
    GOLD_LAKE,
    SILVER_LAKE,
    bronze_glob,
    bronze_path,
    gold_glob,
    gold_path,
    silver_glob,
    silver_path,
)
from .resources import (
    ApifyResource,
    DuckDBResource,
    GeminiResource,
    PolarsIOManager,
)
from .schedules import weekly_medallion

__all__ = [
    # Resources
    "ApifyResource",
    "DuckDBResource",
    "GeminiResource",
    "PolarsIOManager",
    # Paths
    "BRONZE_LAKE",
    "SILVER_LAKE",
    "GOLD_LAKE",
    "bronze_path",
    "silver_path",
    "gold_path",
    "bronze_glob",
    "silver_glob",
    "gold_glob",
    # Schedules
    "weekly_medallion",
]
