"""Phase 5 migration (US-ESA-2): gold_analyses → silver_content_classification.

Write → Audit → Publish, with a loud reconciliation gate:

1. PLAN (default, read-only) — read gold, preview the conform, and report the
   reconciliation: ``gold_total == conformed + quarantined`` with every
   quarantined or key-deviant row NAMED with its reason. Writes nothing.
2. APPLY (``--apply``) — backfill `bronze_enrichment_raw` from
   `gold_analyses.result_json` (verbatim bodies, bulk, idempotent on the
   natural key ``(post_id, platform, workload, prompt_hash, run_id)``),
   conform bronze → `silver_content_classification` deterministically (ZERO
   model/API calls — a pure function of bronze), publish the silver Parquet
   atomically, register the table in state.duckdb, then audit byte identity
   and typed-column parity before declaring success.

This script NEVER drops `gold_analyses` (US-ESA-2 AC10 gates the drop on
AC1–AC9 — a later phase) and makes zero model/API calls. Re-running APPLY is
idempotent: the bronze anti-join appends nothing new and the silver upsert
converges to the same state.

ALWAYS prove on a copy first:

    uv run python scripts/migrate_classification_to_silver.py \
        --db <copy>/state.duckdb \
        --bronze-root <copy>/lake/bronze \
        --silver-root <copy>/lake/silver \
        --report <copy>/reconciliation.json \
        --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from datalake.defs.enrichment import classification, landing

DB_PATH = "data/state.duckdb"

MIGRATION_RUN_ID = "migration/gold-analyses-phase5"
"""Deterministic run_id for every backfilled bronze row — idempotent replay."""

MIGRATION_PROVIDER = "gemini"
"""Provenance: the legacy corpus was enriched by the Gemini batch path."""

MIGRATION_SCHEMA_VERSION = "1"
"""Legacy classification rows were produced under schema_version 1."""

_BACKFILL_PLATFORM = "instagram"

logger = logging.getLogger("migrate_classification_to_silver")


# ── Read gold (legacy key: (post_id, domain) where domain = PLATFORM) ──────


def _read_gold(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Read the legacy gold table — one row per analysis, verbatim columns."""
    return pl.from_arrow(
        con.execute(
            "SELECT post_id, domain, prompt_hash, model, result_json, analysed_at "
            "FROM gold_analyses"
        ).fetch_arrow_table()
    )


def _bronze_frame(gold: pl.DataFrame, *, landing_at: datetime) -> pl.DataFrame:
    """Shape the gold corpus as ``landing.SCHEMA`` bronze rows.

    Preconditions: ``gold`` carries the legacy columns (post_id, domain,
    prompt_hash, model, result_json, analysed_at); ``domain`` is the PLATFORM
    (verified 'instagram' on the live corpus). Postconditions: the frame
    matches ``landing.SCHEMA`` exactly, `response_text` is the verbatim
    `result_json`, and every row has ok=True — a parsed gold row IS the
    evidence the analysis succeeded.
    """
    non_instagram = (
        gold.filter(pl.col("domain") != _BACKFILL_PLATFORM)
        if gold.height and "domain" in gold.columns
        else gold.head(0)
    )
    if non_instagram.height:
        raise ValueError(
            f"gold_analyses.domain must carry the platform {_BACKFILL_PLATFORM!r}; "
            f"found {sorted(set(non_instagram['domain'].to_list()))!r} on "
            f"{non_instagram.height} rows — refusing to mis-key the backfill."
        )

    def _analysed(raw: str | None) -> datetime:
        if raw:
            try:
                return datetime.fromisoformat(raw)
            except ValueError:
                pass
        return landing_at

    null_str = pl.Series([None] * gold.height, dtype=pl.String)
    return pl.DataFrame(
        {
            "post_id": gold["post_id"],
            "platform": pl.Series([_BACKFILL_PLATFORM] * gold.height, dtype=pl.String),
            "workload": pl.Series(
                [classification.WORKLOAD] * gold.height, dtype=pl.String
            ),
            "prompt_hash": gold["prompt_hash"],
            "run_id": pl.Series([MIGRATION_RUN_ID] * gold.height, dtype=pl.String),
            "provider": pl.Series([MIGRATION_PROVIDER] * gold.height, dtype=pl.String),
            # Bronze records the gap as-observed (NULL); silver conforms it
            # to the explicit sentinel (US-ESA-2 AC5).
            "model": gold["model"],
            "schema_version": pl.Series(
                [MIGRATION_SCHEMA_VERSION] * gold.height, dtype=pl.String
            ),
            "landing_at": pl.Series(
                [landing_at] * gold.height, dtype=pl.Datetime("us", "UTC")
            ),
            "analysed_at": gold["analysed_at"].map_elements(
                _analysed, return_dtype=pl.Datetime("us", "UTC")
            ),
            "ok": pl.Series([True] * gold.height, dtype=pl.Boolean),
            "error_message": null_str,
            "response_text": gold["result_json"],
            "request_echo_json": null_str,
            "input_modality": null_str,
            "sampling_params_json": null_str,
        },
        schema=landing.SCHEMA,
    )


# ── Bronze backfill (idempotent, bulk, atomic) ─────────────────────────────


def backfill_bronze(
    gold: pl.DataFrame, *, root: str | os.PathLike[str] | None = None
) -> tuple[pl.DataFrame, int]:
    """Append the gold corpus to bronze; idempotent on the natural key.

    Mirrors ``landing.land_response``'s contract in bulk (one atomic
    temp+rename write instead of a row-at-a-time read-modify-write):
    already-landed keys are a no-op, rows are never updated or overwritten,
    and a crash never leaves a half-written Parquet file. Returns the full
    bronze frame and the number of rows actually appended.
    """
    existing = landing.read_responses(root)
    new = _bronze_frame(gold, landing_at=datetime.now(UTC))
    key_cols = list(landing.KEY_COLUMNS)
    if existing.height and new.height:
        joined = new.join(existing.select(key_cols), on=key_cols, how="anti")
    else:
        joined = new
    if joined.height == 0:
        return existing, 0
    combined = existing.vstack(joined)
    path = landing.response_path(root)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{landing.DATASET_ID}.", suffix=".parquet.tmp", dir=str(path.parent)
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        combined.write_parquet(tmp_path)
        os.replace(tmp_path, path)
    except BaseException:
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    return combined, joined.height


# ── Audit (parity + identity, before declaring success) ────────────────────


def _audit(con: duckdb.DuckDBPyConnection, report: dict) -> None:
    """Audit the published table against the legacy gold extraction.

    - result_json byte identity: silver.result_json == gold.result_json for
      every conformed row (US-ESA-2 AC6) — asserted, not assumed.
    - Typed-column parity (AC4): the silver typed column carries exactly what
      the legacy COALESCE JSON extraction served.
    - Key + provenance parity (AC5); registered-row and duplicate-key checks.
    """
    byte_mismatch = con.execute(
        f"""
        SELECT COUNT(*) FROM {classification.TABLE_ID} s
        JOIN gold_analyses g ON s.post_id = g.post_id
        WHERE s.result_json IS DISTINCT FROM g.result_json
        """
    ).fetchone()[0]
    report["result_json_byte_mismatches"] = int(byte_mismatch)

    parity: dict[str, int] = {}
    for body_col in classification.BODY_KEYS:
        parity[body_col] = int(
            con.execute(
                f"""
                WITH g AS (
                    SELECT post_id,
                        COALESCE(result_json->>'$.{body_col}',
                                 result_json->>'$[0].{body_col}') AS legacy_val
                    FROM gold_analyses
                )
                SELECT COUNT(*) FROM {classification.TABLE_ID} s
                JOIN g USING (post_id)
                WHERE s.{body_col} IS DISTINCT FROM g.legacy_val
                """
            ).fetchone()[0]
        )
    report["typed_parity_mismatches"] = parity

    report["platform_parity_violations"] = int(
        con.execute(
            f"SELECT COUNT(*) FROM {classification.TABLE_ID} WHERE platform "
            "IS NULL OR platform != 'instagram'"
        ).fetchone()[0]
    )
    report["provenance_nulls"] = {
        col: int(
            con.execute(
                f"SELECT COUNT(*) FROM {classification.TABLE_ID} WHERE {col} IS NULL"
            ).fetchone()[0]
        )
        for col in ("prompt_hash", "schema_version", "run_id")
    }
    report["model_gap_rows"] = int(
        con.execute(
            f"SELECT COUNT(*) FROM {classification.TABLE_ID} WHERE model IS NULL "
            f"OR model = '{classification.MODEL_LEGACY_NULL}'"
        ).fetchone()[0]
    )
    registered, dup = con.execute(
        f"""
        SELECT (SELECT COUNT(*) FROM {classification.TABLE_ID}),
               (SELECT COUNT(*) FROM (
                    SELECT post_id, platform FROM {classification.TABLE_ID}
                    GROUP BY post_id, platform HAVING COUNT(*) > 1
               ))
        """
    ).fetchone()
    report["registered_rows"] = int(registered)
    report["duplicate_keys"] = int(dup)


# ── Orchestration ──────────────────────────────────────────────────────────


def run_migration(
    *,
    db_path: str = DB_PATH,
    bronze_root: str | os.PathLike[str] | None = None,
    silver_root: str | os.PathLike[str] | None = None,
    report_path: str | os.PathLike[str] | None = None,
    apply: bool = False,
) -> dict:
    """Run the Plan → (Apply: Write → Audit → Publish) migration.

    Returns the report dict. In plan mode (default) NOTHING is written —
    the reconciliation is computed from a pure-function preview of the
    conform over the gold corpus. In apply mode the reconciliation gate runs
    BEFORE any write: if any row would be unaccounted, nothing lands.

    Reconciliation invariant (US-ESA-2 AC3): ``gold_total == conformed +
    quarantined``, zero unaccounted — asserted via
    ``classification.reconcile``, which raises on any shortfall. Every
    quarantined or key-deviant row is named in the report with its reason.
    """
    con = duckdb.connect(str(db_path), read_only=not apply)
    try:
        gold = _read_gold(con)
        gold_total = gold.height

        report: dict = {
            "migration": "gold_analyses -> silver_content_classification",
            "db": str(db_path),
            "bronze_root": str(
                Path(os.fspath(bronze_root))
                if bronze_root
                else landing.response_path(None).parent
            ),
            "silver_root": str(
                Path(os.fspath(silver_root))
                if silver_root
                else classification._silver_path(None).parent
            ),
            "run_id": MIGRATION_RUN_ID,
            "gold_total": gold_total,
            "api_calls": 0,
            "mode": "apply" if apply else "plan",
        }

        # PLAN: the conform is a deterministic pure function, so previewing it
        # over a synthetic bronze frame of gold gives the exact counts that an
        # apply run would produce — without touching any store.
        preview = classification.conform_classification(
            _bronze_frame(gold, landing_at=datetime.now(UTC)),
            deviance_notes=classification.LEGACY_DEVIANCE_NOTES,
        )
        counts = dict(preview.counts)
        counts.pop("failed_landing", None)  # gold backfill rows are ok=True by construction
        counts["total"] = gold_total
        report["reconciliation"] = counts
        report["deviant_rows"] = preview.deviance
        report["quarantined_rows"] = (
            preview.quarantine.drop("response_excerpt").to_dicts()
            if preview.quarantine.height
            else []
        )
        report["array_form_post_ids"] = _bronze_frame(
            gold, landing_at=datetime.now(UTC)
        ).filter(
            pl.col("response_text").str.strip_chars_start().str.starts_with("[")
        )["post_id"].to_list()
        report["model_null_post_ids"] = (
            gold.filter(pl.col("model").is_null())["post_id"].to_list()
        )

        # THE GATE (AC3): before anything is written, every gold row must be
        # conformed or quarantined with a named reason. A dropped row fails
        # here — loudly, and nothing is published.
        classification.reconcile(gold_total, counts)
        report["unaccounted_rows"] = 0
        if counts["quarantined"]:
            names = ", ".join(r["post_id"] for r in report["quarantined_rows"])
            logger.warning(
                "QUARANTINED %d rows (named): %s", counts["quarantined"], names
            )
        if counts["deviant"]:
            logger.warning(
                "KEY-DEVIANT %d rows (mapped, named): %s",
                counts["deviant"],
                ", ".join(d["post_id"] for d in report["deviant_rows"]),
            )
        if not apply:
            report["mode"] = "plan"
            _write_report(report, report_path)
            return report

        # WRITE: bronze backfill (idempotent), then conform from bronze ONLY.
        bronze_df, added = backfill_bronze(gold, root=bronze_root)
        report["bronze_rows_appended"] = added
        report["bronze_rows_total"] = bronze_df.height
        report["bronze_rows_duplicate_natural_keys"] = (
            bronze_df.height - bronze_df.select(landing.KEY_COLUMNS).unique().height
        )
        bronze_cls = bronze_df.filter(pl.col("workload") == classification.WORKLOAD)
        result = classification.conform_classification(
            bronze_cls, deviance_notes=classification.LEGACY_DEVIANCE_NOTES
        )
        # The conform is a pure function of bronze: the published run MUST
        # reproduce the preview counts exactly, or state diverged.
        _shared = (
            "classification_rows",
            "conformed",
            "quarantined",
            "deviant",
            "array_form",
            "model_gap",
        )
        if {k: result.counts.get(k) for k in _shared} != {
            k: counts.get(k) for k in _shared
        }:
            raise SystemExit(
                "CONFORM DIVERGENCE: counts from the published bronze differ "
                f"from the preview {counts} != {result.counts}"
            )

        report["published_rows"] = int(result.silver.height)
        classification.publish_silver(result.silver, silver_root)
        classification.register_silver(con, silver_root)

        # AUDIT: byte identity + parity, before declaring success.
        _audit(con, report)
        if report["result_json_byte_mismatches"] != 0:
            raise SystemExit(
                f"AUDIT FAILED: {report['result_json_byte_mismatches']} rows "
                "carry a result_json that is NOT byte-identical to gold."
            )
        bad_parity = {k: v for k, v in report["typed_parity_mismatches"].items() if v}
        if bad_parity:
            raise SystemExit(f"AUDIT FAILED: typed parity mismatches {bad_parity}")
        if report["registered_rows"] != result.silver.height:
            raise SystemExit(
                f"AUDIT FAILED: registered table has {report['registered_rows']} "
                f"rows but the conform produced {result.silver.height} — stale "
                "or duplicated rows in state.duckdb."
            )
        if report["duplicate_keys"]:
            raise SystemExit("AUDIT FAILED: duplicate (post_id, platform) keys in silver.")

        _write_report(report, report_path)
        return report
    finally:
        con.close()


def _write_report(report: dict, report_path: str | os.PathLike[str] | None) -> None:
    rendered = json.dumps(report, indent=2, default=str)
    if report_path:
        Path(report_path).write_text(rendered, encoding="utf-8")
    logger.info("Reconciliation report:\n%s", rendered)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--bronze-root", default=None)
    parser.add_argument("--silver-root", default=None)
    parser.add_argument("--report", default=None)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write bronze + silver and register the table (default: read-only plan)",
    )
    args = parser.parse_args()
    run_migration(
        db_path=args.db,
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        report_path=args.report,
        apply=args.apply,
    )


if __name__ == "__main__":
    main()
