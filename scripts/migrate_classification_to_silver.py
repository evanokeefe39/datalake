"""Backfill the legacy `gold_analyses` rows into `silver_content_classification`.

ADR-0011 Phase 5 / ADR-0014 D5 — the live-table migration. `gold_analyses` is
archived and dropped later (W9); this script reshapes its rows into silver
WITHOUT touching the gold table.

What it does (Write → Audit → Publish):
1. READ every `gold_analyses` row: the key column `domain` carries the
   PLATFORM value ('instagram') — it populates the silver `platform` KEY
   column. The niche `domain`/`subdomain`/`topic`/`subtopic` come from
   `result_json` (the platform string NEVER lands in the body `domain`).
2. CONFORM through `conform._classification_body` (the same mapping the
   bronze conform path uses — one scheme, no parallel code): 10 typed body
   columns, dual-shape (object / single-element array) tolerated, `model IS
   NULL` → the ADR-0014 D5 sentinel `legacy-unknown`, provenance carried
   verbatim. A malformed `result_json` (e.g. trailing-comma JSON) QUARANTINES
   loudly with `parse_error` into `silver_enrichment_quarantine` — never a
   silent NULL, never a dropped row.
3. PUBLISH the silver snapshot atomically to the lake, then register it in
   the state database (the `register_conformed` house pattern) with
   INSERT OR REPLACE on the primary key — idempotent, a second run changes
   0 rows.
4. AUDIT in the same run: `count(silver) + count(quarantine rows for this
   workload) == count(gold_analyses)`. The script FAILS if the identity
   breaks; rows are never silently lost.

Usage (NEVER against the live DB without a copy):
    uv run python scripts/migrate_classification_to_silver.py \
        --state-db data/state.duckdb.copy --lake-root data/lake/silver

Sentinel gate (ADR-0014 D5): the script expects 8 `model IS NULL` rows (the
audited live count). A different observed count FAILS LOUDLY with the real
number — it is reported, never forced to 8.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

import duckdb
import polars as pl

from datalake.defs.enrichment import conform, landing

DEFAULT_RUN_ID = "legacy-gold-classification-backfill"
"""Stable migration run id (ADR-0014 D5: the run_id provenance of the
migration). Kept constant so an INSERT OR REPLACE replay is a no-op."""

LEGACY_SCHEMA_VERSION = "1"
"""The classification schema version current at migration time (ADR-0014 D5)."""

LEGACY_PROVIDER = "gemini"
"""The only provider that ever wrote `gold_analyses` (verified: distinct
model on the live table is a single gemini model)."""

EXPECTED_MODEL_NULL_ROWS = 8
"""ADR-0014 D5 audited count. A different observed count fails loudly."""

# DuckDB type names for the silver schema (state-table DDL generated from the
# conform catalog — never a hand-maintained column list).
_DUCKDB_TYPES: dict[pl.DataType, str] = {
    pl.String: "VARCHAR",
    pl.Boolean: "BOOLEAN",
    pl.Datetime("us", "UTC"): "TIMESTAMP WITH TIME ZONE",
}


def _duckdb_type(dtype: pl.DataType) -> str:
    try:
        return _DUCKDB_TYPES[dtype]
    except KeyError:
        raise ValueError(f"no DuckDB type mapping for {dtype!r}") from None


def _silver_ddl() -> str:
    """CREATE TABLE IF NOT EXISTS for the silver table, from the catalog."""
    cols = ",\n    ".join(
        f"{name} {_duckdb_type(dtype)}"
        + (" NOT NULL" if name in conform.KEY_COLUMNS else "")
        for name, dtype in conform.TABLE_SCHEMAS[
            conform.SILVER_CONTENT_CLASSIFICATION
        ].items()
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {conform.SILVER_CONTENT_CLASSIFICATION} (\n"
        f"    {cols},\n"
        "    PRIMARY KEY (post_id, platform)\n)"
    )


def _quarantine_ddl() -> str:
    cols = ",\n    ".join(
        f"{name} {_duckdb_type(dtype)}"
        for name, dtype in conform.TABLE_SCHEMAS[conform.SILVER_QUARANTINE].items()
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {conform.SILVER_QUARANTINE} (\n"
        f"    {cols},\n"
        "    PRIMARY KEY (post_id, platform, workload)\n)"
    )


def _parse_analysed_at(value: object) -> datetime | None:
    """Gold stores `analysed_at` as an ISO VARCHAR; parse or fail loudly."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _parse_payload(result_json: object) -> object:
    """Strict JSON parse — trailing-comma / truncated payloads raise and are
    quarantined upstream (never silently repaired, never dropped)."""
    return json.loads(result_json) if result_json is not None else None


def _quarantine_row(
    post_id: str,
    platform: str,
    base: dict[str, object],
    result_json: object,
    *,
    reason_code: str,
    reason_detail: str,
    now: datetime,
) -> dict[str, object]:
    """One loud quarantine record (conform's schema, REASON_* vocabulary)."""
    return {
        "post_id": post_id,
        "platform": platform,
        **base,
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "response_excerpt": (result_json or "")[: conform._EXCERPT_CHARS],
        "quarantined_at": now,
        "derivation_version": conform.DERIVATION_VERSION,
    }


def migrate(
    state_db: str,
    *,
    lake_root: str | None = None,
    run_id: str = DEFAULT_RUN_ID,
    now: datetime | None = None,
) -> dict[str, int]:
    """Run the gold → silver classification backfill against `state_db`.

    Preconditions: `state_db` is a WRITABLE COPY of the state database (this
    script never opens the live `data/state.duckdb` itself — the operator
    copies it). `gold_analyses` must exist.

    Postconditions:
    - `silver_content_classification` (lake snapshot + state table) holds one
      row per well-formed gold row, keyed `(post_id, platform)`.
    - Every unparseable payload is in `silver_enrichment_quarantine` with a
      `REASON_*` code.
    - `count(silver) + count(quarantine for this workload) == count(gold)`,
      measured in the same run — asserted, never assumed.
    - A re-run replaces rows with identical content (0 observable changes).
    """
    now = now or datetime.now(UTC)
    workload = landing.WORKLOAD_CONTENT_CLASSIFICATION

    conn = duckdb.connect(state_db)
    try:
        gold = conn.execute(
            "SELECT post_id, domain, prompt_hash, result_json, analysed_at, model "
            "FROM gold_analyses"
        ).fetchall()
    finally:
        conn.close()

    gold_count = len(gold)
    observed_model_null = sum(1 for row in gold if row[5] is None)
    if observed_model_null != EXPECTED_MODEL_NULL_ROWS:
        # ADR-0014 D5: fail loudly and REPORT the real number — never force 8.
        raise SystemExit(
            f"FAIL: observed {observed_model_null} gold_analyses rows with "
            f"model IS NULL, expected {EXPECTED_MODEL_NULL_ROWS} (ADR-0014 D5). "
            "A new un-dispositioned row arrived — audit it before migrating."
        )

    silver_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = []
    for post_id, domain, prompt_hash, result_json, analysed_at, model in gold:
        platform = domain  # the legacy KEY column carries the PLATFORM value
        base = {
            "workload": workload,
            "provider": LEGACY_PROVIDER,
            "model": model if model is not None else conform.MODEL_LEGACY_NULL,
            "prompt_hash": prompt_hash,
            "schema_version": LEGACY_SCHEMA_VERSION,
            "run_id": run_id,
        }
        try:
            payload = _parse_payload(result_json)
        except json.JSONDecodeError as exc:
            quarantine_rows.append(
                _quarantine_row(
                    post_id,
                    platform,
                    base,
                    result_json,
                    reason_code=conform.REASON_PARSE_ERROR,
                    reason_detail=f"invalid JSON: {exc}",
                    now=now,
                )
            )
            continue

        body, errors = conform._classification_body(payload)
        if body is None:
            quarantine_rows.append(
                _quarantine_row(
                    post_id,
                    platform,
                    base,
                    result_json,
                    reason_code=conform.classify_reason(errors),
                    reason_detail="; ".join(errors),
                    now=now,
                )
            )
            continue

        silver_rows.append(
            {
                "post_id": post_id,
                "platform": platform,
                **body,
                "is_educational": conform._coerce_bool(body["is_educational"]),
                "is_actionable": conform._coerce_bool(body["is_actionable"]),
                "provider": base["provider"],
                "model": base["model"],
                "prompt_hash": base["prompt_hash"],
                "schema_version": base["schema_version"],
                "input_modality": None,  # legacy gold rows carry no modality metadata
                "content_mime_type": None,
                "sampling_params_json": None,
                "run_id": base["run_id"],
                "analysed_at": _parse_analysed_at(analysed_at),
                "conformed_at": now,
                "derivation_version": conform.DERIVATION_VERSION,
            }
        )

    silver = pl.DataFrame(
        silver_rows,
        schema=conform.TABLE_SCHEMAS[conform.SILVER_CONTENT_CLASSIFICATION],
    ).unique(subset=["post_id", "platform"], keep="last")
    quarantine = pl.DataFrame(
        quarantine_rows, schema=conform.TABLE_SCHEMAS[conform.SILVER_QUARANTINE]
    )

    if silver.height + quarantine.height != gold_count:
        raise SystemExit(
            f"FAIL: this run's output ({silver.height}+{quarantine.height}) "
            f"does not account for all {gold_count} gold rows."
        )

    # Publish: atomic lake snapshot, then register in the state copy.
    conform._write_atomic(
        silver, conform.table_path(conform.SILVER_CONTENT_CLASSIFICATION, lake_root)
    )

    conn = duckdb.connect(state_db)
    try:
        conn.execute(_silver_ddl())
        # Idempotent replay: replace ONLY rows whose content differs under
        # current derivation. Stamp columns (`conformed_at`) are EXCLUDED
        # from the comparison — unchanged content keeps its original stamp,
        # so a second run changes 0 rows by construction.
        cols = [
            c
            for c in conform.TABLE_SCHEMAS[conform.SILVER_CONTENT_CLASSIFICATION]
            if c != "conformed_at"
        ]
        changed = [f"t.{c} IS DISTINCT FROM i.{c}" for c in cols]
        conn.register("_cls_silver_incoming", silver.to_arrow())
        try:
            conn.execute(
                f"DELETE FROM {conform.SILVER_CONTENT_CLASSIFICATION} t "
                "USING _cls_silver_incoming i "
                "WHERE t.post_id = i.post_id AND t.platform = i.platform "
                f"AND ({' OR '.join(changed)})"
            )
            conn.execute(
                f"INSERT INTO {conform.SILVER_CONTENT_CLASSIFICATION} "
                "SELECT * FROM _cls_silver_incoming i "
                "WHERE NOT EXISTS ("
                f"  SELECT 1 FROM {conform.SILVER_CONTENT_CLASSIFICATION} t "
                "  WHERE t.post_id = i.post_id AND t.platform = i.platform)"
            )
        finally:
            conn.unregister("_cls_silver_incoming")

        if quarantine.height:
            conn.execute(_quarantine_ddl())
            conn.register("_cls_quarantine_incoming", quarantine.to_arrow())
            try:
                conn.execute(
                    f"INSERT OR REPLACE INTO {conform.SILVER_QUARANTINE} "
                    "SELECT * FROM _cls_quarantine_incoming"
                )
            finally:
                conn.unregister("_cls_quarantine_incoming")

        # ── Audit (same run, same connection) ─────────────────────────────
        silver_count = conn.execute(
            f"SELECT COUNT(*) FROM {conform.SILVER_CONTENT_CLASSIFICATION}"
        ).fetchone()[0]
        if quarantine.height:
            quarantine_count = conn.execute(
                f"SELECT COUNT(*) FROM {conform.SILVER_QUARANTINE} WHERE workload = ?",
                [workload],
            ).fetchone()[0]
        else:
            quarantine_count = 0
    finally:
        conn.close()

    if silver_count + quarantine_count != gold_count:
        raise SystemExit(
            f"FAIL: reconciliation identity broken — silver={silver_count} + "
            f"quarantine={quarantine_count} != gold={gold_count}. "
            "Rows were silently lost; investigate before publishing."
        )

    counts = {
        "gold_rows": gold_count,
        "conformed": silver.height,
        "quarantined": quarantine.height,
        "model_null_sentinel": observed_model_null,
        "silver_in_state": silver_count,
        "quarantine_in_state": quarantine_count,
    }
    print(
        "classification migration: " + " ".join(f"{k}={v}" for k, v in counts.items())
    )
    if quarantine.height:
        print(
            "QUARANTINED (loud, never dropped):",
            quarantine.group_by("reason_code").len().sort("reason_code").rows(),
        )
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--state-db", required=True, help="Path to a WRITABLE COPY of state.duckdb"
    )
    parser.add_argument(
        "--lake-root",
        default=None,
        help="Silver lake root for the Parquet snapshot (default: the silver lake root)",
    )
    parser.add_argument(
        "--run-id", default=DEFAULT_RUN_ID, help="Migration run id (provenance)"
    )
    args = parser.parse_args()
    counts = migrate(args.state_db, lake_root=args.lake_root, run_id=args.run_id)
    print("counts:", counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
