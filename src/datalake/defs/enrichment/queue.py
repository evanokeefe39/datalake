"""SQLite work queue for enrichment processing.

Five functions, no classes, no backend abstraction. All timestamps are
Python-generated ISO 8601 UTC — no ``DEFAULT (datetime('now'))`` in any DDL.
"""

from __future__ import annotations

from datetime import datetime, timezone

from datalake.defs.common.resources import SQLiteResource

# ── Schema ──────────────────────────────────────────────────────────────────

_OPS_SCHEMA = """
CREATE TABLE IF NOT EXISTS enrichment_queue (
    post_id       TEXT NOT NULL,
    domain        TEXT NOT NULL DEFAULT 'instagram',
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    scheduled_for TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (post_id, domain)
);

CREATE TABLE IF NOT EXISTS media_metadata (
    media_url_hash TEXT PRIMARY KEY,
    media_url      TEXT NOT NULL,
    file_api_uri   TEXT,
    mime_type      TEXT,
    file_size      INTEGER,
    upload_state   TEXT DEFAULT 'pending',
    created_at     TEXT NOT NULL,
    uploaded_at    TEXT
);

CREATE TABLE IF NOT EXISTS dead_letter (
    post_id     TEXT NOT NULL,
    domain      TEXT NOT NULL DEFAULT 'instagram',
    error       TEXT,
    attempts    INTEGER NOT NULL DEFAULT 0,
    failed_at   TEXT NOT NULL,
    PRIMARY KEY (post_id, domain)
);
"""

# ── Constants ───────────────────────────────────────────────────────────────

MAX_ATTEMPTS = 5
"""Max retry attempts before a post is moved to dead_letter."""

_STALE_THRESHOLD_SECS = 600
"""Items in 'processing' for longer than this are considered orphaned."""


def _ensure_schema(ops: SQLiteResource) -> None:
    """Create ops.sqlite tables if they don't exist (idempotent)."""
    conn = ops.get_connection()
    try:
        conn.executescript(_OPS_SCHEMA)
    finally:
        conn.close()


def _now_iso() -> str:
    """Current UTC timestamp as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


# ── Queue operations ────────────────────────────────────────────────────────


def enqueue(ops: SQLiteResource, post_id: str, domain: str = "instagram") -> None:
    """Enqueue a post for enrichment (idempotent — resets existing row).

    Contract: only call for new posts (watermark-gated) or re-enrichment
    targets (stale prompt_hash). Never call for items currently being processed.
    """
    _ensure_schema(ops)
    now = _now_iso()
    conn = ops.get_connection()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO enrichment_queue
               (post_id, domain, status, attempts, scheduled_for, created_at, updated_at)
               VALUES (?, ?, 'pending', 0, ?, ?, ?)""",
            [post_id, domain, now, now, now],
        )
        conn.commit()
    finally:
        conn.close()


def claim(
    ops: SQLiteResource,
    limit: int = 5,
) -> list[dict]:
    """Atomically claim pending items for processing.

    Single transaction: stale reaper → SELECT pending → UPDATE to processing.
    Uses BEGIN IMMEDIATE for explicit write-lock acquisition.

    Returns list of rows with keys: post_id, domain.
    """
    _ensure_schema(ops)
    now = _now_iso()
    stale_cutoff = datetime.now(timezone.utc).timestamp() - _STALE_THRESHOLD_SECS
    stale_iso = datetime.fromtimestamp(stale_cutoff, tz=timezone.utc).isoformat()

    conn = ops.get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")

        # Reaper: reset orphaned items
        conn.execute(
            "UPDATE enrichment_queue "
            "SET status = 'pending', updated_at = ? "
            "WHERE status = 'processing' AND updated_at < ?",
            [now, stale_iso],
        )

        # Claim pending items
        rows = conn.execute(
            "SELECT post_id, domain FROM enrichment_queue "
            "WHERE status = 'pending' "
            "AND (scheduled_for IS NULL OR scheduled_for <= ?) "
            "ORDER BY created_at ASC "
            "LIMIT ?",
            [now, limit],
        ).fetchall()

        if rows:
            post_ids = [r["post_id"] for r in rows]
            domains = [r["domain"] for r in rows]
            placeholders = ",".join(["(?, ?)"] * len(rows))
            params: list[str] = []
            for pid, dom in zip(post_ids, domains):
                params.extend([pid, dom])
            conn.execute(
                f"UPDATE enrichment_queue "
                f"SET status = 'processing', attempts = attempts + 1, updated_at = ? "
                f"WHERE (post_id, domain) IN ({placeholders})",
                [now, *params],
            )

        conn.commit()
        return [dict(r) for r in rows]
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def complete(ops: SQLiteResource, post_id: str, domain: str = "instagram") -> None:
    """Mark an item as successfully processed."""
    now = _now_iso()
    conn = ops.get_connection()
    try:
        conn.execute(
            "UPDATE enrichment_queue "
            "SET status = 'complete', last_error = NULL, updated_at = ? "
            "WHERE post_id = ? AND domain = ?",
            [now, post_id, domain],
        )
        conn.commit()
    finally:
        conn.close()


def fail(
    ops: SQLiteResource,
    post_id: str,
    domain: str,
    error: str,
    backoff_seconds: int = 0,
) -> int:
    """Mark an item as failed (retryable). Reschedules with backoff.

    Increments ``attempts``, sets ``scheduled_for`` to ``now + backoff_seconds``.

    Returns the new ``attempts`` count so the caller can check against
    ``_MAX_ATTEMPTS`` and move to dead_letter if exhausted.
    """
    now = _now_iso()
    scheduled = datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() + backoff_seconds,
        tz=timezone.utc,
    ).isoformat()

    conn = ops.get_connection()
    try:
        conn.execute(
            "UPDATE enrichment_queue "
            "SET status = 'pending', last_error = ?, scheduled_for = ?, "
            "attempts = attempts + 1, updated_at = ? "
            "WHERE post_id = ? AND domain = ?",
            [error, scheduled, now, post_id, domain],
        )
        conn.commit()

        row = conn.execute(
            "SELECT attempts FROM enrichment_queue WHERE post_id = ? AND domain = ?",
            [post_id, domain],
        ).fetchone()
        return row["attempts"] if row else 0
    finally:
        conn.close()


def reschedule(
    ops: SQLiteResource,
    post_id: str,
    domain: str,
    error: str,
    backoff_seconds: int = 0,
) -> None:
    """Reschedule an item without incrementing attempts (for global conditions).

    Used for quota exhaustion — the item did not fail, the system is globally
    rate-limited. ``attempts`` is preserved.
    """
    now = _now_iso()
    scheduled = datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() + backoff_seconds,
        tz=timezone.utc,
    ).isoformat()

    conn = ops.get_connection()
    try:
        conn.execute(
            "UPDATE enrichment_queue "
            "SET status = 'pending', last_error = ?, scheduled_for = ?, updated_at = ? "
            "WHERE post_id = ? AND domain = ?",
            [error, scheduled, now, post_id, domain],
        )
        conn.commit()
    finally:
        conn.close()


def delete(ops: SQLiteResource, post_id: str, domain: str = "instagram") -> None:
    """Remove an item from the queue (caller moves to dead_letter)."""
    conn = ops.get_connection()
    try:
        conn.execute(
            "DELETE FROM enrichment_queue WHERE post_id = ? AND domain = ?",
            [post_id, domain],
        )
        conn.commit()
    finally:
        conn.close()


def depth(ops: SQLiteResource) -> int:
    """Return the count of pending + processing items in the queue."""
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM enrichment_queue "
            "WHERE status IN ('pending', 'processing')"
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()
