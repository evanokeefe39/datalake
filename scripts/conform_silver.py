"""Publish + register the six v3 silver enrichment tables from bronze.

The production caller for ``conform.conform()`` (ADR-0011 Phase 4). Reads
``bronze_enrichment_raw`` verbatim, conforms + validates it into the six
``silver_*`` tables (+ ``silver_enrichment_quarantine``), publishes each as
an atomic Parquet snapshot under the silver lake root, and registers them in
the state DuckDB so serving views and gold marts can reference them by name.

Zero API calls by construction — silver is a deterministic function of
bronze; a schema/mapping change is a replay, never a re-bill.

Mirrors ``scripts/migrate_classification_to_silver.py``:
- plan mode is the default; ``--apply`` writes;
- default roots are resolved EXPLICITLY (to the ``lake.py`` defaults) BEFORE
  any branch, so the bronze read root and the silver write root always name
  real directories — the classification migration shipped a defect where
  ``root=None`` wrote nothing while the conform read from disk;
- the script never opens the live ``data/state.duckdb`` itself: the operator
  passes ``--state-db`` pointing at a writable copy (plan mode only reads
  nothing at all from the DB).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping

import duckdb
import polars as pl

from datalake.defs.common import lake
from datalake.defs.enrichment import conform, landing


def resolve_roots(
    bronze_root: str | None, silver_root: str | None
) -> tuple[str, str]:
    """Resolve None → the lake defaults BEFORE any branch (hard-won lesson:
    a lazily-resolved root is how the write path and the read path end up
    naming different files)."""
    bronze_dir = str(Path(bronze_root)) if bronze_root else str(lake.BRONZE_LAKE)
    silver_dir = str(Path(silver_root)) if silver_root else str(lake.SILVER_LAKE)
    return bronze_dir, silver_dir


def run_conform(
    db_path: str,
    *,
    bronze_root: str | None = None,
    silver_root: str | None = None,
    report_path: str | None = None,
    apply: bool = False,
    now: datetime | None = None,
) -> dict[str, object]:
    """Bronze → six silver tables → DuckDB registration (plan by default).

    Preconditions: ``db_path`` is a WRITABLE COPY of the state database in
    apply mode; the bronze root exists (or holds no landing file — conform
    then publishes typed-empty tables, which is correct: the six tables must
    EXIST even when a workload has no bronze rows).

    Postconditions (apply mode):
    - All six ``silver_*`` tables + ``silver_enrichment_quarantine`` are
      published under the silver root and registered in ``db_path`` —
      queryable by name, so the gold marts' view SQL compiles.
    - Every terminally-failed bronze row is quarantined LOUDLY with an
      attributable reason_code — never a silent NULL, never a dropped row.
    - A re-run over the same bronze (same ``now``) is byte-identical
      (deterministic snapshot; ``CREATE OR REPLACE`` registration) —
      idempotent replay, 0 rows changed.
    """
    now = now or datetime.now(UTC)
    bronze_dir, silver_dir = resolve_roots(bronze_root, silver_root)

    bronze = landing.read_responses(bronze_dir)
    per_workload: dict[str, int] = (
        dict(
            bronze.group_by("workload")
            .len()
            .sort("workload")
            .iter_rows()
        )
        if bronze.height
        else {}
    )
    ok_rows = (
        int(bronze.filter(pl.col("ok")).height) if bronze.height else 0
    )

    plan = {
        "bronze_root": bronze_dir,
        "silver_root": silver_dir,
        "bronze_rows": bronze.height,
        "bronze_rows_ok": ok_rows,
        "bronze_rows_per_workload": per_workload,
        "supported_workloads": sorted(conform.SUPPORTED_CONFORM_WORKLOADS),
        "transcript_workloads": sorted(conform.TRANSCRIPT_WORKLOADS),
    }

    if not apply:
        report: dict[str, object] = {"mode": "plan", **plan}
        _write_report(report, report_path)
        print("PLAN (nothing written): " + json.dumps(plan, sort_keys=True))
        return report

    con = duckdb.connect(db_path)
    try:
        result = conform.conform(
            root=bronze_dir, silver_root=silver_dir, now=now, conn=con
        )
        registered = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        missing = [
            tid for tid in (*conform.SILVER_TABLES, conform.SILVER_QUARANTINE)
            if tid not in registered
        ]
        if missing:
            raise SystemExit(
                f"FAIL: conform published but DuckDB registration is missing "
                f"{missing} — serving SQL would not compile."
            )
        table_rows = {
            tid: con.execute(f"SELECT COUNT(*) FROM {tid}").fetchone()[0]
            for tid in conform.SILVER_TABLES
        }
    finally:
        con.close()

    counts: Mapping[str, int] = result.counts
    report = {
        "mode": "apply",
        **plan,
        "counts": dict(counts),
        "silver_table_rows": table_rows,
        "quarantined_rows": result.quarantine.to_dicts(),
    }
    if counts["conformed"] + counts["quarantined"] != bronze.height:
        raise SystemExit(
            "FAIL: reconciliation broke — conformed + quarantined "
            f"({counts['conformed']} + {counts['quarantined']}) != bronze rows "
            f"({bronze.height}). A row was lost; refusing to report success."
        )
    if result.quarantine.height:
        print(
            "QUARANTINED (loud, never dropped): "
            + json.dumps(result.quarantine.to_dicts(), default=str)
        )
    _write_report(report, report_path)
    print(
        "conform_silver: " + json.dumps({"counts": dict(counts), **table_rows},
                                        sort_keys=True)
    )
    return report


def _write_report(report: Mapping[str, object], report_path: str | None) -> None:
    if report_path is None:
        return
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--state-db", required=True,
        help="Path to a WRITABLE COPY of state.duckdb (apply mode writes here)",
    )
    parser.add_argument(
        "--bronze-root", default=None,
        help="Bronze lake root (default: the bronze lake root per lake.py)",
    )
    parser.add_argument(
        "--silver-root", default=None,
        help="Silver lake root (default: the silver lake root per lake.py)",
    )
    parser.add_argument(
        "--report", default=None, help="Path for the run report JSON"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Publish + register (default is plan-only)",
    )
    args = parser.parse_args()
    run_conform(
        args.state_db,
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        report_path=args.report,
        apply=args.apply,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
