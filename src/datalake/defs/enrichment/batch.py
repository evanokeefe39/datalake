"""Retired batch-queue primitives for enrichment (ADR-0012/0013).

DISPOSITION: the ``ops.sqlite`` queue (``batch_jobs``/``batch_items``/
``dead_letter``) is being retired. The queue DDL is no longer created or
migrated here; the legacy functions below are SHIMS kept only so their
caller-facing signatures stay stable while the consumer slices migrate to
Dagster-native orchestration state in ``datalake.defs.enrichment.partitions``
(instance-injected partition snapshots; failures via the
``landed(bronze) ∖ conformed(silver)`` anti-join). They operate only against
a database that still carries the legacy tables (the live ``ops.sqlite``
pending the separate, gated DROP) and FAIL LOUDLY elsewhere.

Follow-up owners by function:
    create_batch / claim_batch ........ instagram/assets.py + defs/cli + scripts
    set_gemini_batch_name / claim_pending_items ... defs/enrichment/submit.py
    set_gemini_batch_status / batch_progress / mark_complete ... harvest.py
    complete_item / fail_item ......... analysis.py + harvest.py + submit.py
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.common.schemas import sqlite_ddl

_PROMPT_REGISTRY_SCHEMA = sqlite_ddl("prompt_registry")

# ── Constants ────────────────────────────────────────────────────────────────

MAX_ATTEMPTS = 5
"""Max attempts per queue item before it is routed to the legacy
terminal-failure path. RETIRED with the queue (ADR-0012): retries become a
new partition key and failures surface via
``partitions.failure_set(landed, conformed)``. Kept only because
``analysis.py`` and ``harvest.py`` still import it while migrating."""

# ── Helpers ──────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    """Current UTC timestamp as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _now_plus_seconds(seconds: float) -> str:
    """ISO timestamp ``seconds`` from now (UTC)."""
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _require_legacy_queue_tables(conn) -> None:
    """Precondition for every retired queue primitive (ADR-0012).

    The queue DDL is no longer created by this module. These shims only run
    against a database that still carries ``batch_jobs``/``batch_items``
    (the live ``ops.sqlite`` pending the separate, gated DROP). On any other
    database this raises instead of silently operating on missing tables.
    """
    missing = [
        table
        for table in ("batch_jobs", "batch_items")
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table],
        ).fetchone()
        is None
    ]
    if missing:
        raise RuntimeError(
            "ops.sqlite queue retirement (ADR-0012): legacy queue tables "
            f"{missing} do not exist. The queue primitives in "
            "datalake.defs.enrichment.batch are retired; use "
            "datalake.defs.enrichment.partitions for orchestration state."
        )


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
    step. Signature kept for its callers (submit.py, harvest.py, analysis.py,
    media_upload.py, registry.py).
    """
    conn = ops.get_connection()
    try:
        conn.executescript(_PROMPT_REGISTRY_SCHEMA)
        _add_missing_columns(conn, "prompt_registry")
    finally:
        conn.close()



# ── Batch operations ─────────────────────────────────────────────────────────


# RETIRED QUEUE PATH (ADR-0012). Follow-up slice: instagram/assets.py +
# defs/cli (create_batch callers) migrate submit to partitions.dynamic keys;
# deleted at the gated ops.sqlite queue DROP.
def create_batch(
    ops: SQLiteResource,
    payloads: list[str],
    consumer: str = "gemini",
    mode: str = "interactive",
) -> int:
    """Create a new batch job with payload items. Returns the new job_id.

    Each payload is a JSON string the consumer knows how to interpret.
    ``consumer`` tags the batch so workers only claim their own.
    ``mode`` records the intended worker execution mode
    (``interactive`` | ``gemini-batch``).

    Raises ValueError if payloads is empty.
    Raises RuntimeError if the legacy queue tables are absent (post-ADR-0012
    retirement): the queue DDL is no longer created; target state is
    ``partitions.partition_key``-derived dynamic partitions on the instance.
    """
    if not payloads:
        raise ValueError("payloads must not be empty")

    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    now = _now_iso()
    try:
        cur = conn.execute(
            "INSERT INTO batch_jobs (consumer, mode, status, created_at, total_items) "
            "VALUES (?, ?, 'pending', ?, ?)",
            [consumer, mode, now, len(payloads)],
        )
        job_id = cur.lastrowid

        conn.executemany(
            "INSERT OR IGNORE INTO batch_items "
            "(job_id, payload, status, attempts, created_at, updated_at) "
            "VALUES (?, ?, 'pending', 0, ?, ?)",
            [(job_id, p, now, now) for p in payloads],
        )
        conn.commit()
        return job_id
    finally:
        conn.close()


# RETIRED QUEUE PATH (ADR-0012). Only caller outside this module:
# scripts/smoke_test_media_e2e.py — migrate to partitions (or retire the
# smoke) with the scripts/catalog slice; deleted at the gated DROP.
def claim_batch(
    ops: SQLiteResource,
    consumer: str = "gemini",
    mode: str | None = None,
) -> dict | None:
    """Claim the oldest batch with pending items for the given consumer.

    Reclaims 'processing' batches that still have pending items (e.g. a
    previous run stopped early on quota/backoff), so retries across worker
    runs work. ``mode`` optionally restricts the claim to batches created
    with that execution mode (``interactive`` | ``gemini-batch``).
    Returns None if no such batch exists.

    Raises RuntimeError if the legacy queue tables are absent
    (post-ADR-0012 retirement).
    """
    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    try:
        params = [consumer, mode, mode] if mode else [consumer]
        row = conn.execute(
            "SELECT id, mode, gemini_batch_name, gemini_batch_status FROM batch_jobs "
            "WHERE consumer = ? AND status IN ('pending', 'processing') "
            + ("AND (mode = ? OR (mode IS NULL AND ? = 'interactive')) " if mode else "")
            + "AND EXISTS (SELECT 1 FROM batch_items i "
            "            WHERE i.job_id = batch_jobs.id AND i.status = 'pending') "
            "ORDER BY created_at ASC LIMIT 1",
            params,
        ).fetchone()

        if not row:
            return None

        job_id = row[0]
        conn.execute(
            "UPDATE batch_jobs SET status = 'processing' WHERE id = ?",
            [job_id],
        )

        items = conn.execute(
            "SELECT payload FROM batch_items WHERE job_id = ? ORDER BY id",
            [job_id],
        ).fetchall()

        conn.commit()
        return {
            "id": job_id,
            "consumer": consumer,
            "mode": row[1],
            "gemini_batch_name": row[2],
            "gemini_batch_status": row[3],
            "payloads": [r[0] for r in items],
        }
    finally:
        conn.close()


# RETIRED QUEUE PATH (ADR-0012). Follow-up slice: defs/enrichment/submit.py
# (caller) records chunk names on the adapter/seam instead; deleted at the
# gated DROP.
def set_gemini_batch_name(
    ops: SQLiteResource, job_id: int, gemini_batch_name: str
) -> None:
    """Record (or extend) the Gemini batch API job names on a queue batch job.

    ``gemini_batch_name`` is a '|'-joined list of chunk names;
    ``gemini_batch_status`` stays aligned to it (one status per name).
    Extending an existing name list appends SUBMITTED entries (incremental
    resubmission); replacing it resets all statuses to SUBMITTED.

    Raises RuntimeError if the legacy queue tables are absent
    (post-ADR-0012 retirement).
    """
    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    try:
        row = conn.execute(
            "SELECT gemini_batch_name, gemini_batch_status FROM batch_jobs "
            "WHERE id = ?",
            [job_id],
        ).fetchone()
        old_names = (row[0] or "").split("|") if row and row[0] else []
        new_names = gemini_batch_name.split("|")
        old_status = (row[1] or "").split("|") if row and row[1] else []
        if len(old_status) != len(old_names):
            old_status = ["SUBMITTED"] * len(old_names)
        if new_names[: len(old_names)] == old_names and old_names:
            statuses = old_status + ["SUBMITTED"] * (len(new_names) - len(old_names))
        else:
            statuses = ["SUBMITTED"] * len(new_names)
        conn.execute(
            "UPDATE batch_jobs SET gemini_batch_name = ?, gemini_batch_status = ? "
            "WHERE id = ?",
            [gemini_batch_name, "|".join(statuses), job_id],
        )
        conn.commit()
    finally:
        conn.close()


# RETIRED QUEUE PATH (ADR-0012). Follow-up slice: defs/enrichment/harvest.py
# (caller) reads per-chunk status from the adapter; deleted at the gated DROP.
def set_gemini_batch_status(
    ops: SQLiteResource,
    job_id: int,
    status: str,
    error: str | None = None,
    name_index: int | None = None,
) -> None:
    """Update the Gemini batch API job status for a queue batch job.

    ``name_index`` updates one entry of the per-chunk status list (aligned
    to the '|'-joined gemini_batch_name); ``None`` writes ``status``
    verbatim (single-chunk batches and explicit resets).

    Raises RuntimeError if the legacy queue tables are absent
    (post-ADR-0012 retirement).
    """
    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    try:
        if name_index is None:
            # Caller-provided blob written verbatim: either a single status
            # (single-chunk batches / explicit resets) or an aligned
            # '|'-joined per-chunk list.
            blob = status
        else:
            row = conn.execute(
                "SELECT gemini_batch_status FROM batch_jobs WHERE id = ?", [job_id]
            ).fetchone()
            entries = (row[0] or "").split("|") if row and row[0] else []
            while len(entries) <= name_index:
                entries.append("SUBMITTED")
            entries[name_index] = status
            blob = "|".join(entries)
        conn.execute(
            "UPDATE batch_jobs SET gemini_batch_status = ?, gemini_batch_error = ? "
            "WHERE id = ?",
            [blob, error, job_id],
        )
        conn.commit()
    finally:
        conn.close()


# RETIRED QUEUE PATH (ADR-0012). Follow-up slice: defs/enrichment/submit.py
# (caller) claims work as partition keys instead; deleted at the gated DROP.
def claim_pending_items(
    ops: SQLiteResource, job_id: int, limit: int = 5
) -> list[dict]:
    """Claim up to ``limit`` pending items from a processing batch.

    Returns list of dicts with keys: id, payload.
    Sets item status to 'processing'.

    Raises RuntimeError if the legacy queue tables are absent
    (post-ADR-0012 retirement); target state is instance dynamic partitions.
    """
    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    try:
        rows = conn.execute(
            "SELECT id, payload FROM batch_items "
            "WHERE job_id = ? AND status = 'pending' "
            "AND (scheduled_for IS NULL OR scheduled_for <= ?) "
            "ORDER BY id LIMIT ?",
            [job_id, _now_iso(), limit],
        ).fetchall()

        if not rows:
            return []

        ids = [r[0] for r in rows]
        now = _now_iso()
        conn.executemany(
            "UPDATE batch_items SET status = 'processing', updated_at = ? "
            "WHERE id = ?",
            [(now, rid) for rid in ids],
        )
        conn.commit()

        return [
            {"id": r[0], "payload": r[1]}
            for r in rows
        ]
    finally:
        conn.close()


# RETIRED QUEUE PATH (ADR-0012). Follow-up slices: defs/enrichment/analysis.py
# and harvest.py (callers) replace item-level completion with the materialized
# partition itself; deleted at the gated DROP.
def complete_item(ops: SQLiteResource, item_id: int) -> None:
    """Mark a batch item as successfully processed.

    Raises RuntimeError if the legacy queue tables are absent
    (post-ADR-0012 retirement).
    """
    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    try:
        conn.execute(
            "UPDATE batch_items SET status = 'complete', updated_at = ? "
            "WHERE id = ?",
            [_now_iso(), item_id],
        )
        conn.execute(
            "UPDATE batch_jobs SET "
            "processed_items = processed_items + 1 "
            "WHERE id = (SELECT job_id FROM batch_items WHERE id = ?)",
            [item_id],
        )
        conn.commit()
    finally:
        conn.close()


# RETIRED QUEUE PATH (ADR-0012). Follow-up slices: defs/enrichment/analysis.py,
# harvest.py, submit.py (callers) replace retries with a new partition key
# (``partitions.partition_key``) and terminal-failure surfacing with
# ``partitions.failure_set(landed, conformed)``; deleted at the gated DROP.
def fail_item(
    ops: SQLiteResource,
    item_id: int,
    error: str,
    backoff: float = 0,
    *,
    preserve_attempts: bool = False,
) -> int:
    """Mark an item as failed/rescheduled. Returns the attempt count.

    Default (preserve_attempts=False): attempts is incremented; at
    MAX_ATTEMPTS the item becomes terminal ('failed'), otherwise it is
    rescheduled as 'pending' and claimable again once scheduled_for passes.

    preserve_attempts=True: attempts is left untouched; used for global
    conditions (quota exhaustion) that are not the item's fault, so innocent
    items never burn an attempt or dead-letter.

    Raises RuntimeError if the legacy queue tables are absent
    (post-ADR-0012 retirement); a missing item returns 0.
    """
    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    try:
        row = conn.execute(
            "SELECT attempts FROM batch_items WHERE id = ?",
            [item_id],
        ).fetchone()

        if not row:
            return 0

        attempts = row[0]
        new_attempts = attempts if preserve_attempts else attempts + 1
        now = _now_iso()
        scheduled_for = _now_plus_seconds(backoff) if backoff > 0 else None

        if not preserve_attempts and new_attempts >= MAX_ATTEMPTS:
            conn.execute(
                "UPDATE batch_items SET status = 'failed', attempts = ?, "
                "error = ?, updated_at = ?, scheduled_for = NULL WHERE id = ?",
                [new_attempts, error, now, item_id],
            )
            conn.execute(
                "UPDATE batch_jobs SET failed_items = failed_items + 1 "
                "WHERE id = (SELECT job_id FROM batch_items WHERE id = ?)",
                [item_id],
            )
        else:
            conn.execute(
                "UPDATE batch_items SET status = 'pending', attempts = ?, "
                "error = ?, updated_at = ?, scheduled_for = ? WHERE id = ?",
                [new_attempts, error, now, scheduled_for, item_id],
            )

        conn.commit()
        return new_attempts
    finally:
        conn.close()


# RETIRED QUEUE PATH (ADR-0012). Follow-up slice: defs/enrichment/harvest.py
# (caller) derives completion from the instance/harvested partitions;
# deleted at the gated DROP.
def mark_complete(ops: SQLiteResource, job_id: int) -> None:
    """Mark a batch job as complete.

    Raises RuntimeError if the legacy queue tables are absent
    (post-ADR-0012 retirement).
    """
    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    try:
        conn.execute(
            "UPDATE batch_jobs SET status = 'complete', completed_at = ? "
            "WHERE id = ?",
            [_now_iso(), job_id],
        )
        conn.commit()
    finally:
        conn.close()


# RETIRED QUEUE PATH (ADR-0012). Follow-up slice: defs/enrichment/harvest.py
# (caller) moves to ``partitions.account(instance, ...)`` for progress;
# deleted at the gated DROP.
def batch_progress(ops: SQLiteResource, job_id: int) -> dict:
    """Return batch progress summary: total, processed, failed, pending, processing.

    Raises RuntimeError if the legacy queue tables are absent
    (post-ADR-0012 retirement); target state is ``partitions.account``.
    """
    conn = ops.get_connection()
    _require_legacy_queue_tables(conn)
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM batch_items WHERE job_id = ?", [job_id]
        ).fetchone()[0]
        processed = conn.execute(
            "SELECT COUNT(*) FROM batch_items WHERE job_id = ? AND status = 'complete'",
            [job_id],
        ).fetchone()[0]
        failed = conn.execute(
            "SELECT COUNT(*) FROM batch_items WHERE job_id = ? AND status = 'failed'",
            [job_id],
        ).fetchone()[0]
        pending = conn.execute(
            "SELECT COUNT(*) FROM batch_items WHERE job_id = ? AND status = 'pending'",
            [job_id],
        ).fetchone()[0]
        processing = conn.execute(
            "SELECT COUNT(*) FROM batch_items WHERE job_id = ? AND status = 'processing'",
            [job_id],
        ).fetchone()[0]
        return {
            "total": total,
            "processed": processed,
            "failed": failed,
            "pending": pending,
            "processing": processing,
        }
    finally:
        conn.close()
