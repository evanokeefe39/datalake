"""Backfill the legacy `gold_analyses` rows through the v3 layered model.

ADR-0011 Phase 5 / ADR-0014 D5 — the live-table migration. `gold_analyses` is
archived and dropped later (W9); this script moves its rows into the layered
lake WITHOUT touching the gold table and WITHOUT any API call.

Two stages, in the ADR-0011 order (bronze first, then a pure silver replay):

1. BACKFILL BRONZE: every `gold_analyses` row is landed VERBATIM into
   `bronze_enrichment_raw` (`landing.DATASET_ID`) as the classification
   workload — `response_text` is the byte-exact `result_json`, `provider` and
   `model` are carried as recorded, `prompt_hash` is carried, `run_id` is the
   deterministic `MIGRATION_RUN_ID`. The legacy key column `domain` carries
   the PLATFORM value ('instagram') — it populates the bronze/silver
   `platform` KEY column; the niche `domain`/`subdomain`/`topic`/`subtopic`
   live in the payload, never in the key. Landing is idempotent by the
   natural key `(post_id, platform, workload, prompt_hash, run_id)` — a
   re-run appends nothing.
2. CONFORM bronze → `silver_content_classification` through
   `classification.conform_classification` — a DETERMINISTIC FUNCTION OF
   BRONZE with ZERO API calls, so a schema/mapping change is a replay, never
   a re-bill. Deviant rows are REPORTED with reasons (never dropped);
   unparseable payloads are quarantined loudly; the reconciliation identity
   `total == conformed + quarantined (+ failed_landing)` is asserted via
   `classification.reconcile` — the run FAILS if any row is unaccounted for.

Write → Audit → Publish: in `--apply` mode the bronze Parquet is appended
atomically, the silver snapshot is published atomically to the lake, and the
table is registered in the state database (INSERT OR REPLACE by primary key —
idempotent, a second run changes 0 rows). Plan mode (default) performs the
full computation and reconciliation and writes NOTHING.

Usage (NEVER against the live DB without a copy):
    uv run python scripts/migrate_classification_to_silver.py \
        --state-db data/state.duckdb.copy \
        --bronze-root data/lake/bronze --silver-root data/lake/silver \
        --apply

Sentinel gate (ADR-0014 D5): the CLI run expects 8 `model IS NULL` rows (the
audited live count) and FAILS LOUDLY with the real number on a mismatch —
reported, never forced to 8. `run_migration()` leaves the gate off by default
(the unit-test fixture corpus is not the live table); pass
`expected_model_null=8` to enforce it programmatically.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping

import duckdb
import polars as pl

from datalake.defs.enrichment import classification, landing

MIGRATION_RUN_ID = "legacy-gold-classification-backfill"
"""Deterministic migration run id (ADR-0014 D5 provenance). Constant so the
natural-key landing of a re-run is a no-op and provenance stays stable."""

LEGACY_SCHEMA_VERSION = "1"
"""The classification schema version current at migration time (ADR-0014 D5)."""

LEGACY_PROVIDER = "gemini"
"""The only provider that ever wrote `gold_analyses` (verified: distinct
model on the live table is a single gemini model)."""

EXPECTED_MODEL_NULL_ROWS = 8
"""ADR-0014 D5 audited count. The CLI gate fails loudly on a different
observed count, reporting the real number — never forcing 8."""

_GOLD_COLUMNS = (
    "post_id, domain, prompt_hash, result_json, analysed_at, model"
)


def read_gold(db_path: str) -> pl.DataFrame:
    """Read every `gold_analyses` row (the legacy table is never mutated)."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        return pl.from_arrow(
            con.execute(
                f"SELECT {_GOLD_COLUMNS} FROM gold_analyses"
            ).fetch_arrow_table()
        )
    finally:
        con.close()


def _parse_analysed_at(value: object) -> datetime:
    """Gold stores `analysed_at` as an ISO VARCHAR; parse or fail loudly."""
    if value is None:
        raise ValueError("gold_analyses.analysed_at is NULL — provenance gap")
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def backfill_bronze(
    gold: pl.DataFrame,
    *,
    root: str | None = None,
    run_id: str = MIGRATION_RUN_ID,
    now: datetime | None = None,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Build (and, with `root`, land) the bronze frame for the gold rows.

    Preconditions: `gold` carries the `gold_analyses` columns read by
    `read_gold`. `response_text` is the verbatim `result_json` — no
    re-encoding at this layer, so newlines/unicode/escaped quotes survive
    byte-identically (US-ESA-2 AC6).

    Postconditions:
    - Returns `(bronze, info)`; `bronze` has the `landing.SCHEMA` shape and
      one row per gold row (plus, when `root` is given, every previously
      landed row of the dataset).
    - With `root`, landing is idempotent by the natural key: rows already
      landed are left untouched and NOT re-appended (`info["appended"]`
      counts only genuinely new rows, so a re-run appends 0).
    - `info["duplicate_natural_keys"]` counts within-corpus natural-key
      collisions; any non-zero value raises — a collision would make the
      landing silently lossy.
    """
    now = now or datetime.now(UTC)
    rows: list[dict[str, object]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    duplicates = 0
    for rec in gold.iter_rows(named=True):
        platform = rec["domain"]  # legacy KEY column carries the PLATFORM value
        if platform == rec["post_id"]:
            raise ValueError(
                f"post {rec['post_id']}: platform equals post_id — domain "
                "mapping is corrupt"
            )
        key = (
            str(rec["post_id"]),
            str(platform),
            landing.WORKLOAD_CONTENT_CLASSIFICATION,
            str(rec["prompt_hash"]),
            run_id,
        )
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        rows.append(
            {
                "post_id": rec["post_id"],
                "platform": platform,
                "workload": landing.WORKLOAD_CONTENT_CLASSIFICATION,
                "prompt_hash": rec["prompt_hash"],
                "run_id": run_id,
                "provider": LEGACY_PROVIDER,
                "model": rec["model"],
                "schema_version": LEGACY_SCHEMA_VERSION,
                "landing_at": now,
                "analysed_at": _parse_analysed_at(rec["analysed_at"]),
                "ok": True,
                "error_message": None,
                "response_text": rec["result_json"],
                "request_echo_json": None,
                "input_modality": None,
                "sampling_params_json": None,
            }
        )
    if duplicates:
        raise ValueError(
            f"gold corpus carries {duplicates} duplicate natural keys "
            f"(post_id, platform, workload, prompt_hash, run_id) — landing "
            "would silently drop rows; audit the corpus first"
        )
    incoming = pl.DataFrame(rows, schema=landing.SCHEMA)

    appended = 0
    if root is not None:
        existing = landing.read_responses(root)
        merged = pl.concat([existing, incoming]).unique(
            subset=list(landing.KEY_COLUMNS), keep="first", maintain_order=True
        )
        appended = merged.height - existing.height
        if appended:
            landing_path = landing.response_path(root)
            fd, tmp_name = tempfile.mkstemp(
                prefix=f".{landing.DATASET_ID}.",
                suffix=".parquet.tmp",
                dir=str(landing_path.parent),
            )
            os.close(fd)
            tmp_path = Path(tmp_name)
            try:
                merged.write_parquet(tmp_path)
                os.replace(tmp_path, landing_path)
            except BaseException:
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
        bronze = merged
    else:
        bronze = incoming

    return bronze, {
        "appended": appended,
        "duplicate_natural_keys": duplicates,
        "total": incoming.height,
    }


def _parity_mismatches(
    silver: pl.DataFrame, gold: pl.DataFrame
) -> dict[str, int]:
    """Per-body-key count of typed-column values deviating from what the
    legacy views served (JSON extraction over the verbatim body, array-unwrap
    `$[0]` first, NULL where the key was absent)."""
    payload_by_key: dict[tuple[str, str], object] = {}
    for rec in gold.iter_rows(named=True):
        payload_by_key[(rec["post_id"], rec["domain"])] = rec["result_json"]

    def unwrap(text: str | None) -> dict:
        if text is None:
            return {}
        parsed = json.loads(text)
        return parsed[0] if isinstance(parsed, list) else parsed

    mismatches = {key: 0 for key in classification.BODY_KEYS}
    for row in silver.iter_rows(named=True):
        body = unwrap(payload_by_key.get((row["post_id"], row["platform"])))
        for key in classification.BODY_KEYS:
            expected = body.get(key)
            if key in ("is_educational", "is_actionable"):
                actual = (
                    None
                    if row[key] is None
                    else classification._as_bool(expected)
                )
                if actual != row[key]:
                    mismatches[key] += 1
            elif expected != row[key]:
                mismatches[key] += 1
    return mismatches


def run_migration(
    db_path: str,
    *,
    bronze_root: str | None = None,
    silver_root: str | None = None,
    report_path: str | None = None,
    apply: bool = False,
    expected_model_null: int | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Gold → bronze → silver classification migration (plan by default).

    Preconditions: `db_path` is a WRITABLE COPY of the state database (this
    script never opens the live `data/state.duckdb` itself — the operator
    copies it); `gold_analyses` exists.

    Postconditions (apply mode):
    - Every gold row is landed verbatim in `bronze_enrichment_raw`
      (idempotent: a re-run appends 0) and accounted by the reconciliation
      identity `total == conformed + quarantined (+ failed_landing)` — the
      run FAILS loudly rather than silently dropping a row.
    - `silver_content_classification` (lake snapshot + state table, via
      `classification.publish_silver` / `register_silver`) holds one row per
      conformed row, keyed `(post_id, platform)`, `result_json` byte-identical
      to the gold body.
    - A re-run changes 0 rows (deterministic bronze keys → identical silver).
    - `expected_model_null` (CLI: the ADR-0014 D5 audited 8) gates the
      observed NULL-model count loudly with the real number.

    - `bronze_root=None` resolves to the lake default root BEFORE the
      landing branch — the apply path never lands to memory while reading
      the default file (the silent 0-row defect observed live).
    Plan mode writes nothing and returns the same reconciliation report.
    """
    now = now or datetime.now(UTC)
    gold = read_gold(db_path)
    gold_count = gold.height

    observed_model_null = gold.filter(pl.col("model").is_null()).height
    if expected_model_null is not None and observed_model_null != expected_model_null:
        raise SystemExit(
            f"FAIL: observed {observed_model_null} gold_analyses rows with "
            f"model IS NULL, expected {expected_model_null} (ADR-0014 D5). "
            "A new un-dispositioned row arrived — audit it before migrating."
        )

    bronze, landing_info = backfill_bronze(gold, root=None, now=now)
    result = classification.conform_classification(
        bronze, deviance_notes=classification.LEGACY_DEVIANCE_NOTES
    )
    counts = result.counts
    total = counts["classification_rows"]
    if total != gold_count:
        raise SystemExit(
            f"FAIL: {total} bronze classification rows for {gold_count} gold "
            "rows — the bronze backfill is lossy."
        )
    try:
        classification.reconcile(total, counts)
    except classification.ReconciliationError as exc:
        raise SystemExit(str(exc)) from exc
    unaccounted = total - (
        counts["conformed"] + counts["quarantined"] + counts["failed_landing"]
    )
    reconciliation = {"total": total, **counts}

    if not apply:
        report: dict[str, object] = {
            "mode": "plan",
            "run_id": MIGRATION_RUN_ID,
            "gold_rows": gold_count,
            "reconciliation": reconciliation,
            "unaccounted_rows": unaccounted,
            "deviant_rows": result.deviance,
            "model_null_observed": observed_model_null,
            "bronze_rows_appended": landing_info["appended"],
            "bronze_rows_duplicate_natural_keys": landing_info[
                "duplicate_natural_keys"
            ],
        }
        _write_report(report, report_path)
        print(
            "PLAN (nothing written): "
            + json.dumps(reconciliation, sort_keys=True)
        )
        for deviant in result.deviance:
            print(f"DEVIANT {deviant['post_id']}: {deviant['reason']}")
        return report

    # ── APPLY: land bronze, publish silver, register in the state copy ────
    # Resolve `None` -> the lake default BEFORE the branch, so the landing
    # write and every later read of the same root name ONE file (the live
    # run observed defect: bronze_root=None wrote nothing while conform
    # read the default file — conformed 9576, materialized 0).
    bronze_dir = str(landing.response_path(bronze_root).parent)
    bronze, landed = backfill_bronze(gold, root=bronze_dir, now=now)
    result = classification.conform_classification(
        bronze,
        deviance_notes=classification.LEGACY_DEVIANCE_NOTES,
    )
    # Andon: the landing MUST be on disk, not memory-only — conform worked
    # over the merged frame above; this proves the file it came from exists
    # with the exact row count a fresh `read_responses` of the same root sees.
    if landing.read_responses(bronze_dir).height != bronze.height:
        raise SystemExit(
            "FAIL: bronze landing did not materialize — "
            f"{landing.response_path(bronze_dir)} does not hold the "
            f"{bronze.height} merged rows; refusing to publish silver."
        )
    if result.counts["conformed"] + result.counts["quarantined"] + result.counts[
        "failed_landing"
    ] != result.counts["classification_rows"]:
        raise SystemExit(
            "FAIL: post-landing reconciliation broke — rows were lost in the "
            "bronze write."
        )

    classification.publish_silver(result.silver, silver_root)
    con = duckdb.connect(db_path)
    try:
        classification.register_silver(con, silver_root)
        silver_in_state = con.execute(
            f"SELECT COUNT(*) FROM {classification.TABLE_ID}"
        ).fetchone()[0]
    finally:
        con.close()

    # ── Audit ─────────────────────────────────────────────────────────────
    mismatches = 0
    gold_pairs = {
        (rec["post_id"], rec["domain"]): rec["result_json"]
        for rec in gold.iter_rows(named=True)
    }
    for row in result.silver.iter_rows(named=True):
        if gold_pairs.get((row["post_id"], row["platform"])) != row["result_json"]:
            mismatches += 1
    duplicate_keys = result.silver.height - result.silver.unique(
        subset=["post_id", "platform"]
    ).height
    parity = _parity_mismatches(result.silver, gold)

    report = {
        "mode": "apply",
        "run_id": MIGRATION_RUN_ID,
        "gold_rows": gold_count,
        "reconciliation": {"total": total, **result.counts},
        "unaccounted_rows": 0,
        "deviant_rows": result.deviance,
        "model_null_observed": observed_model_null,
        "bronze_rows_appended": landed["appended"],
        "bronze_rows_duplicate_natural_keys": landed["duplicate_natural_keys"],
        "result_json_byte_mismatches": mismatches,
        "duplicate_keys": duplicate_keys,
        "typed_parity_mismatches": parity,
        "silver_in_state": silver_in_state,
        # Loud, never dropped: every quarantined row is named in the report
        # with its reason (the state copy carries only the conformed table).
        "quarantined_rows": result.quarantine.to_dicts(),
    }
    if mismatches or duplicate_keys or any(parity.values()) or unaccounted:
        raise SystemExit(f"FAIL: migration audit failed: {json.dumps(report)}")
    _write_report(report, report_path)
    print("classification migration: " + json.dumps(reconciliation, sort_keys=True))
    for deviant in result.deviance:
        print(f"DEVIANT {deviant['post_id']}: {deviant['reason']}")
    if result.quarantine.height:
        print(
            "QUARANTINED (loud, never dropped): "
            + json.dumps(result.quarantine.rows(), default=str)
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
        "--state-db", required=True, help="Path to a WRITABLE COPY of state.duckdb"
    )
    parser.add_argument(
        "--bronze-root",
        default=None,
        help="Bronze lake root (default: the bronze lake root per lake.py)",
    )
    parser.add_argument(
        "--silver-root",
        default=None,
        help="Silver lake root (default: the silver lake root per lake.py)",
    )
    parser.add_argument(
        "--report", default=None, help="Path for the reconciliation report JSON"
    )
    parser.add_argument(
        "--run-id", default=MIGRATION_RUN_ID, help="Migration run id (provenance)"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write bronze + silver (default is plan-only)",
    )
    args = parser.parse_args()
    report = run_migration(
        args.state_db,
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        report_path=args.report,
        apply=args.apply,
        # ADR-0014 D5: the CLI run enforces the audited live sentinel count.
        expected_model_null=EXPECTED_MODEL_NULL_ROWS,
    )
    print("counts:", json.dumps(report, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
