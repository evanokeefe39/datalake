"""Enrichment assets — gold_analyses AssetSpec, health checks.

``gold_analyses`` is an AssetSpec, not an @asset. The worker is the only writer
and emits AssetMaterialization events. This is the correct pattern for a
continuously-updated table where no single Dagster run produces the complete
asset.
"""
from __future__ import annotations

from datetime import timedelta

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSpec,
    FreshnessPolicy,
    asset_check,
)

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.common.schemas import duckdb_ddl
from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH
from datalake.defs.instagram.labels import LABEL_VERSION

# ── Gold analyses table DDL ─────────────────────────────────────────────────

_GOLD_ANALYSES_DDL = duckdb_ddl("gold_analyses")


def ensure_gold_analyses(db: DuckDBResource) -> None:
    """Create gold_analyses table if it doesn't exist (idempotent).

    Applies additive migrations to pre-existing tables (ALTER ADD COLUMN
    for columns added to the catalog after a table was created).
    """
    with db.get_connection() as conn:
        conn.execute(_GOLD_ANALYSES_DDL)
        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'gold_analyses'"
            ).fetchall()
        }
        if "model" not in cols:
            # Additive migration (ADR-0001): reproducibility version column.
            conn.execute("ALTER TABLE gold_analyses ADD COLUMN model VARCHAR")


# ── AssetSpec ───────────────────────────────────────────────────────────────

gold_analyses = AssetSpec(
    key=AssetKey("gold_analyses"),
    group_name="enrichment",
    description="Enriched social media posts — multi-domain gold layer.",
    deps=[AssetKey("ig_posts_slv")],
    # Freshness SLO (ADR-0001): gold is written by the external worker, which
    # POSTs AssetMaterialization events to the REST endpoint. The OSS
    # FreshnessDaemon (dagster 1.13.11) reads asset_entry.last_materialization
    # from the event log, so worker-POSTed events ARE consumed. OSS runs the
    # FreshnessDaemon when `freshness.enabled` is set in dagster.yaml — it
    # defaults to True in 1.13.11, so the setting is only needed on older
    # versions or to opt out.
    freshness_policy=FreshnessPolicy.time_window(
        fail_window=timedelta(hours=48),
        warn_window=timedelta(hours=24),
    ),
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
        approved_unenriched = db_conn.execute("""
            SELECT COUNT(*) FROM ig_post_labels l
            WHERE l.enrich_decision IN ('standout', 'control', 'floor_filler')
              AND l.label_version = ?
              AND NOT EXISTS (
                  SELECT 1 FROM gold_analyses g
                  WHERE g.post_id = l.post_id AND g.domain = 'instagram'
                    AND g.prompt_hash = ?
              )
        """, [LABEL_VERSION, CURRENT_PROMPT_HASH]).fetchone()[0]

    metadata = {
        "batch_pending": pending,
        "batch_processing": processing,
        "batch_failed": failed,
        "dead_letter_count": dead,
        "gold_analyses_count": enriched,
        "approved_unenriched": approved_unenriched,
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

    if approved_unenriched > 20:
        return AssetCheckResult(
            passed=False,
            metadata=metadata,
            description=(
                f"{approved_unenriched} triage-approved posts are unenriched — "
                "run ig_posts_gen_batches to drain the admission gate."
            ),
        )

    return AssetCheckResult(passed=True, metadata=metadata)


@asset_check(asset=gold_analyses.key)
def check_prompt_currency(duckdb: DuckDBResource, ops: SQLiteResource) -> AssetCheckResult:
    """Detect rows where prompt_hash is stale (prompt or model changed).

    Does NOT trigger re-enrichment (prompt changes cost money — human gate).
    Also verifies the current prompt is registered in the prompt/version
    registry (ADR-0001): an unresolvable current prompt means gold rows are
    being produced without a recoverable prompt definition.
    """
    ensure_gold_analyses(duckdb)

    with duckdb.get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM gold_analyses "
            "WHERE prompt_hash IS NULL OR prompt_hash != ?",
            [CURRENT_PROMPT_HASH],
        ).fetchone()

    stale = row[0] if row else 0

    from datalake.defs.enrichment.registry import is_current_prompt_registered

    registered = is_current_prompt_registered(ops)

    if stale > 0 or not registered:
        return AssetCheckResult(
            passed=False,
            metadata={
                "stale_rows": stale,
                "current_prompt_hash": CURRENT_PROMPT_HASH,
                "current_prompt_registered": registered,
            },
        )
    return AssetCheckResult(passed=True, metadata={"stale_rows": 0, "current_prompt_registered": True})


ENRICHMENT_CHECKS = [check_enrichment_health, check_prompt_currency]
