"""Dagster Definitions — assets, resources, schedules.

``dg dev`` reads this module. Workloads register their assets here.
"""

import os

from dagster import Definitions
from dagster_duckdb import DuckDBResource
from dotenv import load_dotenv

from . import resources as _resources

load_dotenv()

# ── Resources ─────────────────────────────────────────────────────────────────
# DuckDBResource from dagster-duckdb owns the full connection lifecycle.

all_resources = {
    "duckdb": DuckDBResource(
        database=os.environ.get("IG_DB_PATH", "data/state.duckdb"),
    ),
    "apify": _resources.ApifyResource(),
    "gemini": _resources.GeminiResource(),
}

# ── Assets ────────────────────────────────────────────────────────────────────
# (uncomment as assets land)
# from .defs.bronze import bronze_posts_raw, bronze_posts_parquet
# from .defs.silver import silver_posts
# from .defs.gold import gold_analyses
# from .defs.serving import dim_time, dim_profile, analytics_views

# ── Schedules ─────────────────────────────────────────────────────────────────
# from .schedules import weekly_pipeline

# ── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=[],
    resources=all_resources,
)
