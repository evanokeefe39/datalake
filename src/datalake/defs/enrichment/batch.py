"""Batch abstraction for enrichment processing — generic work queue.

batch_items stores consumer-agnostic JSON payloads. Each consumer
defines its own payload schema. The Gemini consumer uses
``{"post_id": "...", "domain": "instagram"}``; a transcription
consumer might use ``{"video_id": "...", "language": "en"}``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from datalake.defs.common.resources import SQLiteResource

# ── Schema ───────────────────────────────────────────────────────────────────

_BATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS batch_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    consumer        TEXT NOT NULL DEFAULT 'gemini',
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TEXT NOT NULL,
    completed_at    TEXT,
    total_items     INTEGER NOT NULL DEFAULT 0,
    processed_items INTEGER NOT NULL DEFAULT 0,
    failed_items    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS batch_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER NOT NULL REFERENCES batch_jobs(id),
    payload     TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    scheduled_for TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    UNIQUE(job_id, payload)
);

CREATE INDEX IF NOT EXISTS idx_batch_items_job_status
    ON batch_items(job_id, status);
"""

# ── Constants ────────────────────────────────────────────────────────────────

MAX_ATTEMPTS = 5
"""Max retry attempts before an item is routed to dead_letter."""

# ── Helpers ──────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    """Current UTC timestamp as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _now_plus_seconds(seconds: float) -> str:
    """ISO timestamp ``seconds`` from now (UTC)."""
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _ensure_schema(ops: SQLiteResource) -> None:
    """Create batch tables if they don't exist (idempotent)."""
    conn = ops.get_connection()
    try:
        conn.executescript(_BATCH_SCHEMA)
        # Migration: add scheduled_for to pre-existing DBs (idempotent).
        cols = {row[1] for row in conn.execute("PRAGMA table_info(batch_items)")}
        if "scheduled_for" not in cols:
            conn.execute("ALTER TABLE batch_items ADD COLUMN scheduled_for TEXT")
    finally:
        conn.close()


# ── Batch operations ─────────────────────────────────────────────────────────


def create_batch(
    ops: SQLiteResource,
    payloads: list[str],
    consumer: str = "gemini",
) -> int:
    """Create a new batch job with payload items. Returns the new job_id.

    Each payload is a JSON string the consumer knows how to interpret.
    ``consumer`` tags the batch so workers only claim their own.

    Raises ValueError if payloads is empty.
    """
    if not payloads:
        raise ValueError("payloads must not be empty")

    _ensure_schema(ops)
    now = _now_iso()
    conn = ops.get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO batch_jobs (consumer, status, created_at, total_items) "
            "VALUES (?, 'pending', ?, ?)",
            [consumer, now, len(payloads)],
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


def claim_batch(ops: SQLiteResource, consumer: str = "gemini") -> dict | None:
    """Claim the oldest batch with pending items for the given consumer.

    Reclaims 'processing' batches that still have pending items (e.g. a
    previous run stopped early on quota/backoff), so retries across worker
    runs work. Returns None if no such batch exists.
    """
    _ensure_schema(ops)
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM batch_jobs "
            "WHERE consumer = ? AND status IN ('pending', 'processing') "
            "AND EXISTS (SELECT 1 FROM batch_items i "
            "            WHERE i.job_id = batch_jobs.id AND i.status = 'pending') "
            "ORDER BY created_at ASC LIMIT 1",
            [consumer],
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
            "payloads": [r[0] for r in items],
        }
    finally:
        conn.close()


def claim_pending_items(
    ops: SQLiteResource, job_id: int, limit: int = 5
) -> list[dict]:
    """Claim up to ``limit`` pending items from a processing batch.

    Returns list of dicts with keys: id, payload.
    Sets item status to 'processing'.
    """
    _ensure_schema(ops)
    conn = ops.get_connection()
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


def complete_item(ops: SQLiteResource, item_id: int) -> None:
    """Mark a batch item as successfully processed."""
    conn = ops.get_connection()
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


def fail_item(
    ops: SQLiteResource,
    item_id: int,
    error: str,
    backoff: float = 0,
    *,
    preserve_attempts: bool = False,
) -> int:
    """Mark an item as failed/rescheduled. Returns the (possibly unchanged)
    attempt count.

    Default (``preserve_attempts=False``): attempts is incremented; at
    MAX_ATTEMPTS the item becomes terminal ('failed'), otherwise it is
    rescheduled as 'pending' and claimable again once ``scheduled_for``
    passes.

    ``preserve_attempts=True``: attempts is left untouched — used for global
    conditions (quota exhaustion) that are not the item's fault, so innocent
    items never burn an attempt or dead-letter.
    """
    conn = ops.get_connection()
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


def mark_complete(ops: SQLiteResource, job_id: int) -> None:
    """Mark a batch job as complete."""
    conn = ops.get_connection()
    try:
        conn.execute(
            "UPDATE batch_jobs SET status = 'complete', completed_at = ? "
            "WHERE id = ?",
            [_now_iso(), job_id],
        )
        conn.commit()
    finally:
        conn.close()


def batch_progress(ops: SQLiteResource, job_id: int) -> dict:
    """Return batch progress summary: total, processed, failed, pending, processing."""
    conn = ops.get_connection()
    try:
        job = conn.execute(
            "SELECT total_items, processed_items, failed_items "
            "FROM batch_jobs WHERE id = ?",
            [job_id],
        ).fetchone()

        if not job:
            return {"total": 0, "processed": 0, "failed": 0, "pending": 0, "processing": 0}

        pending = conn.execute(
            "SELECT COUNT(*) FROM batch_items "
            "WHERE job_id = ? AND status = 'pending'",
            [job_id],
        ).fetchone()[0]

        processing = conn.execute(
            "SELECT COUNT(*) FROM batch_items "
            "WHERE job_id = ? AND status = 'processing'",
            [job_id],
        ).fetchone()[0]

        return {
            "total": job[0],
            "processed": job[1],
            "failed": job[2],
            "pending": pending,
            "processing": processing,
        }
    finally:
        conn.close()
