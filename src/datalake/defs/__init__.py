"""Domains — self-contained asset groupings.

common/ — shared infrastructure (resources, paths, schedules)
instagram/ — Instagram-specific assets and configs
serving/ — cross-domain shared dimensions and views
"""

from .common import (
    BRONZE_LAKE,
    GOLD_LAKE,
    SILVER_LAKE,
    ApifyResource,
    DuckDBResource,
    GeminiResource,
    PolarsIOManager,
    bronze_glob,
    bronze_path,
    gold_glob,
    gold_path,
    silver_glob,
    silver_path,
    weekly_medallion,
)
from .instagram import GoldConfig, ScrapeConfig
from .serving import assets as serving_assets

__all__ = [
    # Resources
    "ApifyResource",
    "DuckDBResource",
    "GeminiResource",
    "PolarsIOManager",
    # Configs
    "ScrapeConfig",
    "GoldConfig",
    # Path helpers
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
    # Serving
    "serving_assets",
]
