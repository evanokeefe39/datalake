"""Enrichment assets — the silver conform asset and its health checks.

``gold_analyses`` (the legacy gold layer) was RETIRED per ADR-0011: it is
superseded by ``silver_content_classification`` and must never be recreated
by live code. Its AssetSpec, DDL-bootstrap (``ensure_gold_analyses``) and
table-backed checks were removed 2026-09-15 (W9); the checks with surviving
value were re-pointed at ``silver_content_classification``.
"""
from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetKey,
    asset,
    asset_check,
)

from datalake.defs.common import lake
from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment import conform
from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH
from datalake.defs.instagram.labels import LABEL_VERSION

# ── Silver-backed checks (the gold_analyses retirement, W9) ────────────────



# ── Asset checks ────────────────────────────────────────────────────────────

@asset_check(asset="silver_enrichment_conform")
def check_approved_classification_coverage(
    duckdb: DuckDBResource,
) -> AssetCheckResult:
    """Warn if triage-approved posts lack a silver_content_classification row.

    The v3 replacement for the retired ``check_enrichment_health`` gold
    counter (W9): the admission-gate signal (approved-but-unclassified
    posts) survives, re-pointed from ``gold_analyses`` to the live
    classification table. The retired ops.sqlite queue counters
    (batch_items stuck / dead_letter growth) are NOT carried over — ADR-0012
    retired that queue; ``check_no_silent_loss`` (bronze \\ conformed) is
    the blocking health gate for the conform itself.

    Does NOT mutate state. Read-only.
    """
    with duckdb.get_connection() as db_conn:
        classified = db_conn.execute(
            "SELECT COUNT(*) FROM silver_content_classification "
            "WHERE platform = 'instagram'"
        ).fetchone()[0]
        approved_unenriched = db_conn.execute("""
            SELECT COUNT(*) FROM ig_post_labels l
            WHERE l.enrich_decision IN ('standout', 'control', 'floor_filler')
              AND l.label_version = ?
              AND NOT EXISTS (
                  SELECT 1 FROM silver_content_classification c
                  WHERE c.post_id = l.post_id AND c.platform = 'instagram'
              )
        """, [LABEL_VERSION]).fetchone()[0]

    metadata = {
        "silver_content_classification_count": classified,
        "approved_unenriched": approved_unenriched,
    }
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


@asset_check(asset="silver_enrichment_conform")
def check_prompt_currency(duckdb: DuckDBResource, ops: SQLiteResource) -> AssetCheckResult:
    """Detect rows where prompt_hash is stale (prompt or model changed).

    Does NOT trigger re-enrichment (prompt changes cost money — human gate).
    Also verifies the current prompt is registered in the prompt/version
    registry (ADR-0001): an unresolvable current prompt means gold rows are
    being produced without a recoverable prompt definition.
    """
    with duckdb.get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM silver_content_classification "
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
    return AssetCheckResult(
        passed=True, metadata={"stale_rows": 0, "current_prompt_registered": True}
    )


@asset_check(asset="silver_enrichment_conform")
def check_enrichment_seam_purity() -> AssetCheckResult:
    """ADR-0008 seam guard: silver onward stays hermetic.

    Pure enrichment modules must contain no Gemini API calls, and every op
    in the enrichment domain must carry the ``{adr: 0008, seam:
    enrichment-api}`` tag. A violation means an API call has leaked into (or
    an untagged op is one edit away from leaking into) a pure transform.
    """
    from datalake.defs.enrichment.media_upload import seam_violations

    violations = seam_violations()
    return AssetCheckResult(
        passed=not violations,
        metadata={
            "violations": violations,
            "adr": "0008",
            "seam": "enrichment-api",
        },
    )


@asset(
    name="silver_enrichment_conform",
    group_name="enrichment",
    deps=[AssetKey(["bronze_enrichment_raw"])],
    description=(
        "ADR-0011 Phase 4 caller — conform + validate bronze_enrichment_raw "
        "into the six silver_* tables (plus the loud quarantine surface), "
        "publish each as an atomic Parquet snapshot and register them in "
        "state DuckDB so the gold marts' view SQL compiles. Zero API calls; "
        "a re-run is an idempotent replay."
    ),
)
def silver_enrichment_conform(duckdb: DuckDBResource) -> None:
    """Deterministic bronze → silver conform, orchestrated by Dagster."""
    with duckdb.get_connection() as conn:
        conform.conform(
            root=lake.BRONZE_LAKE,
            silver_root=lake.SILVER_LAKE,
            conn=conn,
        )


ENRICHMENT_CHECKS = [
    check_approved_classification_coverage,
    check_prompt_currency,
    check_enrichment_seam_purity,
]
