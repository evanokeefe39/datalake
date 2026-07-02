"""Batch abstraction for enrichment processing.

Replaces the old enrichment_queue table with a batch-level model:
  - ``batch_jobs`` — one row per batch submission
  - ``batch_items`` — one row per item within a batch

The enqueue asset creates a batch. The standalone worker claims it, processes
each item, and POSTs materialization events to Dagster when done.
"""

from __future__ import annotations

from datetime import datetime, timezone

from datalake.defs.common.resources import SQLiteResource

# ── Schema ───────────────────────────────────────────────────────────────────

_BATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS batch_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
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
    post_id     TEXT NOT NULL,
    domain      TEXT NOT NULL DEFAULT 'instagram',
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    UNIQUE(job_id, post_id, domain)
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


def _ensure_schema(ops: SQLiteResource) -> None:
    """Create batch tables if they don't exist (idempotent)."""
    conn = ops.get_connection()
    try:
        conn.executescript(_BATCH_SCHEMA)
    finally:
        conn.close()


# ── Batch operations ─────────────────────────────────────────────────────────


def create_batch(
    ops: SQLiteResource,
    post_ids: list[str],
    domains: list[str] | None = None,
) -> int:
    """Create a new batch job with items. Returns the new job_id.

    Each (post_id, domain) pair becomes a batch_items row.
    Raises ValueError if post_ids and domains lengths mismatch.
    """
    if not post_ids:
        raise ValueError("post_ids must not be empty")

    if domains is None:
        domains = ["instagram"] * len(post_ids)

    if len(post_ids) != len(domains):
        raise ValueError(
            f"post_ids ({len(post_ids)}) and domains ({len(domains)}) length mismatch"
        )

    _ensure_schema(ops)
    now = _now_iso()
    conn = ops.get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO batch_jobs (status, created_at, total_items) "
            "VALUES ('pending', ?, ?)",
            [now, len(post_ids)],
        )
        job_id = cur.lastrowid

        conn.executemany(
            "INSERT OR IGNORE INTO batch_items "
            "(job_id, post_id, domain, status, attempts, created_at, updated_at) "
            "VALUES (?, ?, ?, 'pending', 0, ?, ?)",
            [(job_id, pid, dom, now, now) for pid, dom in zip(post_ids, domains)],
        )
        conn.commit()
        return job_id
    finally:
        conn.close()


def claim_batch(ops: SQLiteResource) -> dict | None:
    """Claim the oldest pending batch. Returns None if no pending batches.

    Returns dict with keys: id, post_ids, domains
    Sets batch status to 'processing'.
    """
    _ensure_schema(ops)
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM batch_jobs "
            "WHERE status = 'pending' "
            "ORDER BY created_at ASC LIMIT 1"
        ).fetchone()

        if not row:
            return None

        job_id = row[0]
        conn.execute(
            "UPDATE batch_jobs SET status = 'processing' WHERE id = ?",
            [job_id],
        )

        items = conn.execute(
            "SELECT post_id, domain FROM batch_items WHERE job_id = ? ORDER BY id",
            [job_id],
        ).fetchall()

        conn.commit()
        return {
            "id": job_id,
            "post_ids": [r[0] for r in items],
            "domains": [r[1] for r in items],
        }
    finally:
        conn.close()


def claim_pending_items(
    ops: SQLiteResource, job_id: int, limit: int = 5
) -> list[dict]:
    """Claim up to ``limit`` pending items from a processing batch.

    Returns list of dicts with keys: id, post_id, domain.
    Sets item status to 'processing'.
    """
    _ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT id, post_id, domain FROM batch_items "
            "WHERE job_id = ? AND status = 'pending' "
            "ORDER BY id LIMIT ?",
            [job_id, limit],
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
            {"id": r[0], "post_id": r[1], "domain": r[2]}
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
    backoff: int = 0,
) -> int:
    """Mark an item as failed. Returns new attempt count.

    If attempts >= MAX_ATTEMPTS, item stays 'failed' — caller should dead-letter it.
    """
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT attempts FROM batch_items WHERE id = ?",
            [item_id],
        ).fetchone()

        if not row:
            return 0

        new_attempts = row[0] + 1
        now = _now_iso()

        if new_attempts >= MAX_ATTEMPTS:
            conn.execute(
                "UPDATE batch_items SET status = 'failed', attempts = ?, "
                "error = ?, updated_at = ? WHERE id = ?",
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
                "error = ?, updated_at = ? WHERE id = ?",
                [new_attempts, error, now, item_id],
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
