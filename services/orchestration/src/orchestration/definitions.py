"""Dagster Definitions — assets, resources, schedules, sensors, jobs, checks.

`dg`/`dagster` read this module (`[tool.dagster] module_name = "orchestration.definitions"`).
It is the composition root: the ONLY module that imports every subsystem, so its
import list is the checklist for what the code location actually contains.

Cross-domain assets are composed from each serving module's `ASSETS` list rather
than one monolithic list, so a new view lands in one place and shows up here.
"""

import os

from dagster import Definitions
from dagster_duckdb import DuckDBResource
from dotenv import load_dotenv

from .defs.engine.harvest import enrichment_harvest_job
from .defs.engine.sensor import enrichment_harvest_sensor, enrichment_submit_sensor
from .defs.engine.silver_asset import silver_enrichment
from .defs.engine.submit import enrichment_submit_job
from .defs.ig_core.bnz.roster import ig_roster_raw
from .defs.ig_core.bnz.scrape import (
    ig_posts_local_raw,
    ig_posts_raw,
    ig_profile_details_raw,
)
from .defs.ig_core.slv.checks import ig_checks
from .defs.ig_core.slv.comments import ig_comments_slv
from .defs.ig_core.slv.labels import ig_post_labels
from .defs.ig_core.slv.posts import ig_posts_slv
from .defs.ig_core.slv.profiles import ig_profiles_slv
from .defs.ig_core.slv.roster import ig_roster_slv
from .defs.ig_enriched.slv import checks as enrichment_checks
from .defs.platform import paths
from .defs.platform.details_sweep import details_sweep
from .defs.platform.resources import (
    ApifyResource,
    PolarsIOManager,
    SQLiteResource,
)
from .defs.platform.schedules import core_refresh, daily_medallion
from .defs.serving import checks as serving_checks_mod
from .defs.serving import dims, marts, metrics, views

load_dotenv()

# ── Resources ─────────────────────────────────────────────────────────────────

all_resources = {
    # Root follows the ONE configured data root rather than a cwd-relative
    # literal: `"data/lake"` survives every IG_* override, so in a container
    # (where the mount is /data) it silently pointed at a nonexistent
    # /app/data/lake. With IG_DATA_DIR unset on a host this resolves to exactly
    # the same <repo>/data/lake as before.
    "io_manager": PolarsIOManager(lake_root=str(paths.DATA_DIR / "lake")),
    "duckdb": DuckDBResource(
        database=os.environ.get("IG_DB_PATH", "data/state.duckdb"),
    ),
    "ops": SQLiteResource(),
    "apify": ApifyResource(),
}

# ── Assets ────────────────────────────────────────────────────────────────────
#
# Order is not load-bearing: every serving asset declares its `deps=`, so the
# execution graph comes from those edges. The list is grouped by layer for a
# reader, not for the scheduler.

all_assets = [
    # Instagram core
    ig_posts_raw,
    ig_posts_local_raw,
    ig_posts_slv,
    ig_post_labels,
    ig_profiles_slv,
    ig_comments_slv,
    # Roster: landed from the dashboard API, then published for the pipeline
    ig_roster_raw,
    ig_roster_slv,
    # Details scrapes, reconciled from the roster by the sweep schedule
    ig_profile_details_raw,
    # Enrichment: submit discovers and materializes its own partitions
    # (ADR-0016) — there is no drain asset.
    silver_enrichment,
    # Serving: dimensions, then the canonical metrics, then the marts, then views
    *dims.ASSETS,
    *metrics.ASSETS,
    *marts.ASSETS,
    *views.ASSETS,
]

# ── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=all_assets,
    asset_checks=[
        *ig_checks,
        *enrichment_checks.ENRICHMENT_CHECKS,
        *enrichment_checks.ENRICHMENT_DQ_CHECKS,
        *serving_checks_mod.serving_checks,
    ],
    resources=all_resources,
    schedules=[daily_medallion, core_refresh, details_sweep],
    jobs=[enrichment_harvest_job, enrichment_submit_job],
    sensors=[enrichment_harvest_sensor, enrichment_submit_sensor],
)
