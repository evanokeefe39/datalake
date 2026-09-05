"""Runtime asset checks for serving-layer data quality.

Each ``@asset_check`` runs after its target asset materializes and queries
DuckDB for dimensional-model invariants.

Per test-hardening plan Phase 4:
- No overlapping SCD2 intervals
- effective_from ≤ effective_to
- No gaps between consecutive intervals per profile_key
- v_post_detail returns rows
"""

from __future__ import annotations

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

# ── Helpers ────────────────────────────────────────────────────────────────


def _get_connection(resources):
    """Context-manager for the DuckDB connection."""
    return resources.duckdb.get_connection()


# ── dim_profile checks ─────────────────────────────────────────────────────


@asset_check(
    asset="dim_profile",
    name="dim_profile_no_overlapping_intervals",
    required_resource_keys={"duckdb"},
    description="No entity has overlapping SCD2 effective periods.",
)
def _dim_profile_no_overlapping_intervals(context) -> AssetCheckResult:
    """Detect overlapping intervals per ``owner_id`` using windowed self-join.

    Two rows overlap when the second row's ``effective_from`` falls before
    the first row's ``effective_to`` (for the same owner_id).
    """
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        overlaps = conn.execute("""
            SELECT COUNT(*)
            FROM dim_profile a
            JOIN dim_profile b
              ON a.owner_id = b.owner_id
             AND a.profile_key < b.profile_key
             AND a.effective_from < COALESCE(b.effective_to, '9999-12-31'::TIMESTAMP)
             AND COALESCE(b.effective_from, '1970-01-01'::TIMESTAMP)
                 < COALESCE(a.effective_to, '9999-12-31'::TIMESTAMP)
        """).fetchone()[0] or 0
    if overlaps > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"{overlaps} overlapping interval pair(s) detected.",
            metadata={"overlap_pairs": overlaps},
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset="dim_profile",
    name="dim_profile_effective_range_valid",
    required_resource_keys={"duckdb"},
    description="effective_from ≤ effective_to for all rows (or to is NULL).",
)
def _dim_profile_effective_range_valid(context) -> AssetCheckResult:
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        bad = conn.execute("""
            SELECT COUNT(*) FROM dim_profile
            WHERE effective_to IS NOT NULL
              AND effective_from > effective_to
        """).fetchone()[0] or 0
    if bad > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"{bad} row(s) where effective_from > effective_to.",
            metadata={"invalid_rows": bad},
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset="dim_profile",
    name="dim_profile_no_gaps",
    required_resource_keys={"duckdb"},
    description="No gaps between consecutive intervals per profile_key.",
)
def _dim_profile_no_gaps(context) -> AssetCheckResult:
    """Detect gaps where the next row's ``effective_from`` does not equal
    the previous row's ``effective_to`` (per ``owner_id`` ordering)."""
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        gaps = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT
                    owner_id,
                    effective_from,
                    LAG(effective_to) OVER (
                        PARTITION BY owner_id ORDER BY effective_from
                    ) AS prev_effective_to
                FROM dim_profile
            ) sub
            WHERE prev_effective_to IS NOT NULL
              AND effective_from != prev_effective_to
        """).fetchone()[0] or 0
    if gaps > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"{gaps} gap(s) found between consecutive intervals.",
            metadata={"gap_count": gaps},
        )
    return AssetCheckResult(passed=True)


# ── v_post_detail check ──────────────────────────────────────────────────────


@asset_check(
    asset="v_post_detail",
    name="v_post_detail_row_count_positive",
    required_resource_keys={"duckdb"},
    description="Foundational view returns at least one row when data exists.",
)
def _v_post_detail_row_count_positive(context) -> AssetCheckResult:
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM v_post_detail").fetchone()[0] or 0
    if count == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="v_post_detail returned 0 rows.",
            metadata={"row_count": 0},
        )
    return AssetCheckResult(
        passed=True,
        metadata={"row_count": count},
    )


@asset_check(
    asset="v_post_detail",
    name="v_post_detail_gold_attribute_coverage",
    required_resource_keys={"duckdb"},
    description=(
        "Fail when gold_analyses rows store attributes that v_post_detail's "
        "JSON extraction fails to surface; report overall attribute coverage."
    ),
)
def _v_post_detail_gold_attribute_coverage(context) -> AssetCheckResult:
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM v_post_detail").fetchone()[0] or 0
        if total == 0:
            return AssetCheckResult(passed=True, metadata={"row_count": 0})
        missing, unsurfaced = conn.execute("""
            WITH g AS (
                SELECT result_json FROM gold_analyses WHERE domain = 'instagram'
            )
            SELECT
                (SELECT COUNT(*) FROM v_post_detail WHERE gold_topic IS NULL),
                (SELECT COUNT(*) FROM g
                 WHERE json_extract_string(result_json, '$.topic') IS NULL
                   AND json_extract_string(result_json, '$[0].topic') IS NOT NULL)
        """).fetchone()
        no_gold = conn.execute(
            "SELECT COUNT(*) FROM v_post_detail WHERE result_json IS NULL"
        ).fetchone()[0] or 0
    # unsurfaced > 0 = real extraction bug (stored data the view drops).
    # no_gold = never-enriched posts — closing that gap costs Gemini money and
    # is human-gated, so it is reported but does not fail the check.
    metadata = {
        "row_count": total,
        "missing_gold_topic": missing,
        "no_gold_analyses_row": no_gold,
        "coverage_ratio_topic": round(1 - missing / total, 4) if total else 1.0,
        "stored_but_unsurfaced_rows": unsurfaced,
    }
    if unsurfaced > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=(
                f"{unsurfaced} gold_analyses rows store attributes in "
                "result_json that v_post_detail fails to surface (extraction gap)."
            ),
            metadata=metadata,
        )
    return AssetCheckResult(passed=True, metadata=metadata)


serving_checks = [
    _dim_profile_no_overlapping_intervals,
    _dim_profile_effective_range_valid,
    _dim_profile_no_gaps,
    _v_post_detail_row_count_positive,
]
