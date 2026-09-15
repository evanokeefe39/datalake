"""The silver publisher asset — the ONE Dagster-aware piece of the conform path.

`engine/silver_rt.py` is the pure conform runtime: `conform()` is a function of
bronze, and its import graph is guarded to contain no provider client, no seam
module and no Dagster resource
(`test_zero_api_calls_enforced_structurally`). That guard is what makes "zero
API calls on replay" a structural property rather than a promise.

This module is separate for exactly that reason: an `@asset` needs
`DuckDBResource`, and importing a resource into the runtime would put Dagster
back into conform's transitive imports. Keeping the asset here lets the
runtime stay pure and the guard stay meaningful.
"""

import logging

from dagster import AssetKey, asset

from orchestration.defs.engine import silver_rt as conform
from orchestration.defs.platform import paths as lake
from orchestration.defs.platform.resources import DuckDBResource

logger = logging.getLogger("enrichment.silver")


@asset(
    name="silver_enrichment",
    group_name="enrichment",
    deps=[AssetKey(["bronze_enrichment_raw"])],
    description=(
        "Conform + validate bronze_enrichment_raw into the six silver_* tables "
        "(plus the loud quarantine surface), publish each as an atomic Parquet "
        "snapshot and register them in state DuckDB so the gold marts' view SQL "
        "compiles. Zero API calls; a re-run is an idempotent replay."
    ),
)
def silver_enrichment(duckdb: DuckDBResource) -> None:
    """Deterministic bronze → silver build, orchestrated by Dagster."""
    with duckdb.get_connection() as conn:
        conform.conform(
            root=lake.BRONZE_LAKE,
            silver_root=lake.SILVER_LAKE,
            conn=conn,
        )


__all__ = ["silver_enrichment"]
