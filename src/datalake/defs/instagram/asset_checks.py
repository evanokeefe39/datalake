"""Runtime asset checks for Instagram-layer data quality.

Each ``@asset_check`` runs after its target asset materializes, verifying
a specific data-quality invariant. Checks that fail raise warnings
(severity = WARN) so the pipeline still produces downstream data.

Per test-hardening plan Phase 4:

- Bronze checks
- Silver dedup / row-bounding checks
- Gold enrichment validity checks
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from ..common.lake import BRONZE_LAKE

# ── Valid admiralty codes per gold prompt taxonomy ─────────────────────────
# The prompt accepts A1 (authoritative) through C2 (entertainment), so every
# combination A1–A6, B1–B6, C1–C2 is valid.
_VALID_ADMIRALTY: set[str] = {
    f"{letter}{num}"
    for letter in "ABC"
    for num in "123456"
} - {"C3", "C4", "C5", "C6"}

_EXPECTED_SCHEMA_VERSION = 3


# ── Helpers ────────────────────────────────────────────────────────────────


def _latest_bronze_path() -> Path | None:
    """Return the most-recently-written bronze Parquet file, or *None*."""
    files = sorted(BRONZE_LAKE.glob("*.parquet"), key=os.path.getmtime, reverse=True)
    return files[0] if files else None


def _read_bronze_df() -> pl.DataFrame | None:
    """Read the latest bronze Parquet; *None* if nothing has been written."""
    path = _latest_bronze_path()
    if path is None:
        return None
    try:
        return pl.read_parquet(path)
    except Exception:
        return None


# ── Bronze checks ──────────────────────────────────────────────────────────


@asset_check(
    asset="ig_posts_raw",
    name="ig_posts_raw_has_rows",
    description="Bronze row count > 0.",
)
def _ig_posts_raw_has_rows() -> AssetCheckResult:
    df = _read_bronze_df()
    if df is None or df.is_empty():
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="No non-empty bronze Parquet found.",
        )
    return AssetCheckResult(
        passed=True,
        metadata={"row_count": len(df)},
    )


@asset_check(
    asset="ig_posts_raw",
    name="ig_posts_raw_has_meta",
    description=".meta sidecar exists and is valid JSON.",
)
def _ig_posts_raw_has_meta() -> AssetCheckResult:
    path = _latest_bronze_path()
    if path is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="No bronze Parquet file to check.",
        )
    meta_path = path.with_suffix(".parquet.meta")
    if not meta_path.exists():
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Missing .meta sidecar: {meta_path.name}",
        )
    try:
        meta = json.loads(meta_path.read_text())
    except json.JSONDecodeError as exc:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Invalid JSON in .meta: {exc}",
        )
    required = {"run_id", "actor", "item_count", "downloaded_at"}
    missing = required - set(meta)
    if missing:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Missing .meta fields: {', '.join(sorted(missing))}",
        )
    return AssetCheckResult(passed=True, metadata={"fields": list(meta.keys())})


@asset_check(
    asset="ig_posts_raw",
    name="ig_posts_raw_run_id_not_null",
    description="No null post IDs in bronze Parquet rows.",
)
def _ig_posts_raw_run_id_not_null() -> AssetCheckResult:
    """Verify every row has a non-null ``id`` (post identifier).

    Null row IDs in bronze would cascade into downstream join failures.
    """
    df = _read_bronze_df()
    if df is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="No bronze Parquet to check.",
        )
    if "id" not in df.columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Bronze Parquet missing 'id' column.",
        )
    null_count = df["id"].null_count()
    if null_count > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"{null_count} row(s) with null 'id'.",
        )
    return AssetCheckResult(passed=True, metadata={"total_rows": len(df)})


# ── Silver checks ──────────────────────────────────────────────────────────


@asset_check(
    asset="ig_posts_slv",
    name="ig_posts_slv_no_duplicates",
    required_resource_keys={"duckdb"},
    description="DISTINCT post_id count = total row count (no duplicates).",
)
def _ig_posts_slv_no_duplicates(context) -> AssetCheckResult:
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM silver_ig_posts"
        ).fetchone()[0] or 0
        distinct = conn.execute(
            "SELECT COUNT(DISTINCT post_id) FROM silver_ig_posts"
        ).fetchone()[0] or 0
    if total != distinct:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"{total} rows, {distinct} distinct post_ids — duplicates found.",
            metadata={"total_rows": total, "distinct_post_ids": distinct},
        )
    return AssetCheckResult(
        passed=True,
        metadata={"total_rows": total, "distinct_post_ids": distinct},
    )


@asset_check(
    asset="ig_posts_slv",
    name="ig_posts_slv_row_count_bounded",
    required_resource_keys={"duckdb"},
    description="Silver rows ≤ bronze rows (dedup guarantee).",
)
def _ig_posts_slv_row_count_bounded(context) -> AssetCheckResult:
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        silver_count = conn.execute(
            "SELECT COUNT(*) FROM silver_ig_posts"
        ).fetchone()[0] or 0
    bronze_count: int = 0
    for path in BRONZE_LAKE.glob("*.parquet"):
        try:
            df = pl.read_parquet(path)
            bronze_count += len(df)
        except Exception:
            pass
    if silver_count > bronze_count:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=(
                f"Silver ({silver_count}) > bronze ({bronze_count}) "
                f"— unexpected row expansion."
            ),
            metadata={"silver_rows": silver_count, "bronze_rows": bronze_count},
        )
    return AssetCheckResult(
        passed=True,
        metadata={"silver_rows": silver_count, "bronze_rows": bronze_count},
    )


# ── Gold checks ────────────────────────────────────────────────────────────


@asset_check(
    asset="ig_posts_gld",
    name="ig_posts_gld_valid_admiralty",
    required_resource_keys={"duckdb"},
    description="Admiralty code in known set.",
)
def _ig_posts_gld_valid_admiralty(context) -> AssetCheckResult:
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        rows = conn.execute(
            "SELECT post_id, result_json FROM gold_ig_analyses"
        ).fetchall()
    invalid: list[str] = []
    for post_id, result_json in rows:
        if result_json is None:
            invalid.append(post_id)
            continue
        try:
            parsed = json.loads(result_json)
            code = parsed.get("admirality", "")
            if code not in _VALID_ADMIRALTY:
                invalid.append(f"{post_id}: {code!r}")
        except json.JSONDecodeError:
            invalid.append(f"{post_id}: unparseable JSON")
    if invalid:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Invalid admiralty codes: {', '.join(invalid[:5])}",
            metadata={"total_checked": len(rows), "invalid_count": len(invalid)},
        )
    return AssetCheckResult(
        passed=True,
        metadata={"total_checked": len(rows)},
    )


@asset_check(
    asset="ig_posts_gld",
    name="ig_posts_gld_valid_json",
    required_resource_keys={"duckdb"},
    description="educational_json and actionable_json parseable from result_json.",
)
def _ig_posts_gld_valid_json(context) -> AssetCheckResult:
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        rows = conn.execute(
            "SELECT post_id, result_json FROM gold_ig_analyses"
        ).fetchall()
    failed: list[str] = []
    for post_id, result_json in rows:
        if result_json is None:
            failed.append(f"{post_id}: null result_json")
            continue
        try:
            parsed = json.loads(result_json)
        except json.JSONDecodeError:
            failed.append(f"{post_id}: unparseable JSON")
            continue
        for field in ("educational_json", "actionable_json"):
            val = parsed.get(field)
            if val is None:
                failed.append(f"{post_id}: missing {field}")
                continue
            if not isinstance(val, dict):
                failed.append(f"{post_id}: {field} is not an object")
                continue
            if not val.get("summary"):
                failed.append(f"{post_id}: {field}.summary missing or empty")
    if failed:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"JSON validation failures: {', '.join(failed[:5])}",
            metadata={"total_checked": len(rows), "failed_count": len(failed)},
        )
    return AssetCheckResult(
        passed=True,
        metadata={"total_checked": len(rows)},
    )


@asset_check(
    asset="ig_posts_gld",
    name="ig_posts_gld_schema_version_current",
    required_resource_keys={"duckdb"},
    description="All rows have schema_version = {_EXPECTED_SCHEMA_VERSION}.",
)
def _ig_posts_gld_schema_version_current(context) -> AssetCheckResult:
    duckdb = context.resources.duckdb
    with duckdb.get_connection() as conn:
        rows = conn.execute(
            "SELECT post_id, schema_version FROM gold_ig_analyses "
            "WHERE schema_version != ?",
            [_EXPECTED_SCHEMA_VERSION],
        ).fetchall()
    if rows:
        offenders = ", ".join(f"{r[0]}: v{r[1]}" for r in rows[:5])
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=(
                f"{len(rows)} row(s) with non-current schema_version "
                f"(expected {_EXPECTED_SCHEMA_VERSION}): {offenders}"
            ),
            metadata={"expected": _EXPECTED_SCHEMA_VERSION, "offending_rows": len(rows)},
        )
    return AssetCheckResult(
        passed=True,
        metadata={"expected": _EXPECTED_SCHEMA_VERSION},
    )
ig_checks = [
    _ig_posts_raw_has_rows,
    _ig_posts_raw_has_meta,
    _ig_posts_raw_run_id_not_null,
    _ig_posts_slv_no_duplicates,
    _ig_posts_slv_row_count_bounded,
    _ig_posts_gld_valid_admiralty,
    _ig_posts_gld_valid_json,
    _ig_posts_gld_schema_version_current,
]
