"""Migrate legacy enrichment_queue rows to the new batch_jobs/batch_items model.

Usage::

    uv run python scripts/migrate_enrichment_queue.py          # Migrate all pending
    uv run python scripts/migrate_enrichment_queue.py --dry-run  # Show counts only
    uv run python scripts/migrate_enrichment_queue.py --drop     # Drop old table after

Safe to run repeatedly — idempotent.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger("migrate_enrichment_queue")

OPS_PATH = "data/ops.sqlite"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_batch_schema(conn: sqlite3.Connection) -> None:
    """Create batch tables if they don't exist (idempotent)."""
    conn.executescript("""
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
    """)


def show_counts(conn: sqlite3.Connection) -> dict:
    """Show current state of enrichment_queue and batch tables."""
    result = {}

    # Check if enrichment_queue exists
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='enrichment_queue'"
    ).fetchall()
    if tables:
        pending = conn.execute(
            "SELECT COUNT(*) FROM enrichment_queue WHERE status = 'pending'"
        ).fetchone()[0]
        processing = conn.execute(
            "SELECT COUNT(*) FROM enrichment_queue WHERE status = 'processing'"
        ).fetchone()[0]
        failed = conn.execute(
            "SELECT COUNT(*) FROM enrichment_queue WHERE status = 'failed'"
        ).fetchone()[0]
        total = conn.execute(
            "SELECT COUNT(*) FROM enrichment_queue"
        ).fetchone()[0]
        result["enrichment_queue"] = {
            "total": total, "pending": pending,
            "processing": processing, "failed": failed,
        }
    else:
        result["enrichment_queue"] = None

    # Check batch tables
    batch_tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='batch_jobs'"
    ).fetchall()
    if batch_tables:
        jobs = conn.execute("SELECT COUNT(*) FROM batch_jobs").fetchone()[0]
        items = conn.execute("SELECT COUNT(*) FROM batch_items").fetchone()[0]
        result["batch"] = {"jobs": jobs, "items": items}
    else:
        result["batch"] = None

    return result


def migrate(conn: sqlite3.Connection) -> int:
    """Migrate pending enrichment_queue rows into a single batch.

    Returns the number of migrated rows.
    """
    _ensure_batch_schema(conn)

    # Read all pending + processing items
    rows = conn.execute(
        "SELECT post_id, domain FROM enrichment_queue "
        "WHERE status IN ('pending', 'processing') "
        "ORDER BY post_id"
    ).fetchall()

    if not rows:
        logger.info("No pending enrichment_queue rows to migrate.")
        return 0

    now = _now_iso()
    post_ids = [r[0] for r in rows]
    domains = [r[1] for r in rows]

    # Create one batch with all items
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

    logger.info(
        "Created batch %d with %d items from enrichment_queue",
        job_id, len(post_ids),
    )
    return len(post_ids)


def drop_enrichment_queue(conn: sqlite3.Connection) -> None:
    """Drop the legacy enrichment_queue table."""
    conn.execute("DROP TABLE IF EXISTS enrichment_queue")
    conn.commit()
    logger.info("Dropped enrichment_queue table.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Migrate enrichment_queue to batch_jobs/batch_items."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show current state without migrating",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop enrichment_queue table after migration",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(OPS_PATH)
    conn.row_factory = sqlite3.Row

    try:
        counts = show_counts(conn)

        if counts["enrichment_queue"]:
            eq = counts["enrichment_queue"]
            logger.info(
                "enrichment_queue: %d total (%d pending, %d processing, %d failed)",
                eq["total"], eq["pending"], eq["processing"], eq["failed"],
            )
        else:
            logger.info("enrichment_queue: table does not exist")

        if counts["batch"]:
            logger.info(
                "batch: %d jobs, %d items",
                counts["batch"]["jobs"], counts["batch"]["items"],
            )
        else:
            logger.info("batch: tables do not exist yet")

        if args.dry_run:
            return

        if counts["enrichment_queue"] and counts["enrichment_queue"]["total"] > 0:
            migrated = migrate(conn)
            logger.info("Migrated %d rows.", migrated)
        else:
            logger.info("Nothing to migrate.")

        if args.drop:
            drop_enrichment_queue(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
