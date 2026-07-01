"""Dagster Definitions — assets, resources, schedules.

``dg dev`` reads this module. Workloads register their assets here.
"""

import os

from dagster import Definitions
from dagster_duckdb import DuckDBResource
from dotenv import load_dotenv

from .defs.common import ApifyResource, GeminiResource, PolarsIOManager, weekly_medallion
from .defs.instagram import ig_checks, ig_posts_gld, ig_posts_raw, ig_posts_slv
from .defs.serving import assets as serving_assets
from .defs.serving import serving_checks

load_dotenv()

# ── Resources ─────────────────────────────────────────────────────────────────

all_resources = {
    "io_manager": PolarsIOManager(lake_root="data/lake"),
    "duckdb": DuckDBResource(
        database=os.environ.get("IG_DB_PATH", "data/state.duckdb"),
    ),
    "apify": ApifyResource(),
    "gemini": GeminiResource(),
}

# ── Assets ────────────────────────────────────────────────────────────────────

all_assets = [
    ig_posts_raw,
    ig_posts_slv,
    ig_posts_gld,
    *serving_assets,
]

# ── Schedules ─────────────────────────────────────────────────────────────────

# ── Definitions ───────────────────────────────────────────────────────────────
defs = Definitions(
    assets=all_assets,
    asset_checks=[*ig_checks, *serving_checks],
    resources=all_resources,
    schedules=[weekly_medallion],
)
