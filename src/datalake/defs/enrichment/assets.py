"""Enrichment assets — gold_analyses AssetSpec, health checks.

``gold_analyses`` is an AssetSpec, not an @asset. The worker is the only writer
and emits AssetMaterialization events. This is the correct pattern for a
continuously-updated table where no single Dagster run produces the complete
asset.
"""
from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSpec,
    asset_check,
)

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH

# ── Gold analyses table DDL ─────────────────────────────────────────────────

_GOLD_ANALYSES_DDL = """
CREATE TABLE IF NOT EXISTS gold_analyses (
    post_id         TEXT NOT NULL,
    domain          TEXT NOT NULL DEFAULT 'instagram',
    prompt_hash     TEXT,
    result_json     TEXT,
    analysed_at     TEXT NOT NULL,
    PRIMARY KEY (post_id, domain)
);
"""


def ensure_gold_analyses(db: DuckDBResource) -> None:
    """Create gold_analyses table if it doesn't exist (idempotent)."""
    with db.get_connection() as conn:
        conn.execute(_GOLD_ANALYSES_DDL)


# ── AssetSpec ───────────────────────────────────────────────────────────────

gold_analyses = AssetSpec(
    key=AssetKey("gold_analyses"),
    group_name="enrichment",
    description="Enriched social media posts — multi-domain gold layer.",
    deps=[AssetKey("ig_posts_slv"), AssetKey("ig_posts_gld_batches")],
)


# ── Asset checks ────────────────────────────────────────────────────────────

@asset_check(asset=gold_analyses.key)
def check_enrichment_health(
    duckdb: DuckDBResource,
    ops: SQLiteResource,
) -> AssetCheckResult:
    """Warn if batch items are stuck, batches are stalled, or dead_letter grows.

    Does NOT mutate state. Read-only check against batch_jobs/batch_items.
    """
    ensure_gold_analyses(duckdb)

    conn = ops.get_connection()
    try:
        pending = conn.execute(
            "SELECT COUNT(*) FROM batch_items WHERE status = 'pending'"
        ).fetchone()[0]
        processing = conn.execute(
            "SELECT COUNT(*) FROM batch_items WHERE status = 'processing'"
        ).fetchone()[0]
        failed = conn.execute(
            "SELECT COUNT(*) FROM batch_items WHERE status = 'failed'"
        ).fetchone()[0]
        dead = conn.execute(
            "SELECT COUNT(*) FROM dead_letter"
        ).fetchone()[0]
    finally:
        conn.close()

    with duckdb.get_connection() as db_conn:
        enriched = db_conn.execute(
            "SELECT COUNT(*) FROM gold_analyses"
        ).fetchone()[0]

    metadata = {
        "batch_pending": pending,
        "batch_processing": processing,
        "batch_failed": failed,
        "dead_letter_count": dead,
        "gold_analyses_count": enriched,
    }

    if processing > 20:
        return AssetCheckResult(
            passed=False,
            metadata=metadata,
            description=f"{processing} items stuck in processing — check worker health.",
        )

    if dead > 50:
        return AssetCheckResult(
            passed=False,
            metadata=metadata,
            description=f"{dead} items in dead_letter — manual triage needed.",
        )

    return AssetCheckResult(passed=True, metadata=metadata)


@asset_check(asset=gold_analyses.key)
def check_prompt_currency(duckdb: DuckDBResource) -> AssetCheckResult:
    """Detect rows where prompt_hash is stale (prompt or model changed).

    Does NOT trigger re-enrichment (prompt changes cost money — human gate).
    """
    ensure_gold_analyses(duckdb)

    with duckdb.get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM gold_analyses "
            "WHERE prompt_hash IS NULL OR prompt_hash != ?",
            [CURRENT_PROMPT_HASH],
        ).fetchone()

    stale = row[0] if row else 0

    if stale > 0:
        return AssetCheckResult(
            passed=False,
            metadata={"stale_rows": stale, "current_prompt_hash": CURRENT_PROMPT_HASH},
        )
    return AssetCheckResult(passed=True, metadata={"stale_rows": 0})


ENRICHMENT_CHECKS = [check_enrichment_health, check_prompt_currency]
