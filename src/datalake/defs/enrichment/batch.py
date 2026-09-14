"""Retained ops.sqlite helpers for enrichment (ADR-0012).

DISPOSITION: the ``ops.sqlite`` queue (``batch_jobs``/``batch_items``/
``dead_letter``) is RETIRED (ADR-0012/0013). The queue primitives —
``create_batch``, ``claim_batch``, ``claim_pending_items``,
``set_gemini_batch_name``, ``set_gemini_batch_status``, ``complete_item``,
``fail_item``, ``mark_complete``, ``batch_progress``, ``MAX_ATTEMPTS``, and
the ``_require_legacy_queue_tables`` precondition — are DELETED. Nothing in
``src/`` reads or writes a queue table: orchestration state is
Dagster-native (``datalake.defs.enrichment.partitions``), submit discovers
work from the instance's materialized partitions, and failures surface via
the ``landed(bronze) ∖ conformed(silver)`` anti-join.

What remains here serves the RETAINED tables only: ``_ensure_schema``
(migrates ``prompt_registry``), ``_now_iso`` (shared timestamp helper),
and the prompt-registry DDL constant.
"""

from __future__ import annotations

from datetime import datetime, timezone

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.common.schemas import sqlite_ddl

_PROMPT_REGISTRY_SCHEMA = sqlite_ddl("prompt_registry")


def _now_iso() -> str:
    """Current UTC timestamp as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _add_missing_columns(conn, table: str) -> None:
    """Align a live table with the catalog (idempotent).

    ALTER TABLE ADD COLUMN for catalog columns missing on the live table,
    plus a committed backfill of NULL values in defaulted columns (covers
    rows that predate the column — the ALTER-time UPDATE only helps if it
    runs in the same statement session as the ALTER).
    """
    from datalake.defs.common.schemas import _SQLITE_SPECS

    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    for name, col in _SQLITE_SPECS[table].columns.items():
        if col.primary_key:
            continue
        if name not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col.sql_type}")
        if col.default:
            # Idempotent NULL backfill for pre-existing rows.
            conn.execute(
                f"UPDATE {table} SET {name} = {col.default} WHERE {name} IS NULL"
            )
    # sqlite3 legacy mode: DDL autocommits but DML opens an implicit
    # transaction — without an explicit commit the backfill rolls back on
    # close and legacy rows keep NULL for the new column.
    conn.commit()


def _ensure_schema(ops: SQLiteResource) -> None:
    """Ensure the RETAINED ops.sqlite tables exist (idempotent).

    Post-ADR-0012 this creates/migrates ONLY ``prompt_registry`` (a retained
    table). The queue tables (``batch_jobs``/``batch_items``) are NOT created
    here any more; nothing is DROPped — their removal is a separate, gated
    step. Signature kept for its callers (media_upload.py, registry.py).
    """
    conn = ops.get_connection()
    try:
        conn.executescript(_PROMPT_REGISTRY_SCHEMA)
        _add_missing_columns(conn, "prompt_registry")
    finally:
        conn.close()
