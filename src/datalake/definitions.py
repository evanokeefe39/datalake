"""Dagster Definitions — assets, resources, schedules, sensors, jobs.

``dg dev`` reads this module. Workloads register their assets here.
"""

import os

from dagster import Definitions
from dagster_duckdb import DuckDBResource
from dotenv import load_dotenv

from .defs.common import (
    ApifyResource,
    GeminiResource,
    PolarsIOManager,
    SQLiteResource,
    daily_medallion,
)
from .defs.enrichment import (
    ENRICHMENT_CHECKS,
    gold_analyses,
)
from .defs.instagram import (
    ig_checks,
    ig_comments_slv,
    ig_posts_gen_batches,
    ig_posts_raw,
    ig_posts_slv,
    ig_profiles_slv,
)
from .defs.serving import assets as serving_assets
from .defs.serving import serving_checks

load_dotenv()

# ── Resources ─────────────────────────────────────────────────────────────────

all_resources = {
    "io_manager": PolarsIOManager(lake_root="data/lake"),
    "duckdb": DuckDBResource(
        database=os.environ.get("IG_DB_PATH", "data/state.duckdb"),
    ),
    "ops": SQLiteResource(),
    "apify": ApifyResource(),
    "gemini": GeminiResource(),
}

# ── Assets ────────────────────────────────────────────────────────────────────

all_assets = [
    ig_posts_raw,
    ig_posts_slv,
    ig_profiles_slv,
    ig_comments_slv,
    ig_posts_gen_batches,
    gold_analyses,
    *serving_assets,
]

# ── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=all_assets,
    asset_checks=[*ig_checks, *ENRICHMENT_CHECKS, *serving_checks],
    resources=all_resources,
    schedules=[daily_medallion],
)
