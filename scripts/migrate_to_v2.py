"""Standalone migration: domain-scoped tables, watermarks, dead_letter.

Run once to migrate from the Phase 1-4 schema to the v2 architecture:
- silver_posts → silver_ig_posts (column silvered_at → processed_on)
- gold_analyses → gold_ig_analyses (drop status/error/attempts)
- silver_progress → silver_ig_progress
- silver_watermark → dropped
- New: watermarks, dead_letter tables
- Existing data is preserved; idempotent (safe to re-run).

Usage:
    uv run python scripts/migrate_to_v2.py [--db-path data/state.duckdb]
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger("migrate_v2")

DEFAULT_DB = Path("data/state.duckdb")


def _backup(db_path: Path) -> Path:
    """Copy the DuckDB file as a backup before migration."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_suffix(f".backup_{ts}.duckdb")
    shutil.copy2(db_path, backup)
    logger.info("Backup created: %s", backup)
    return backup


def _table_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Check if a table exists in the main schema."""
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        [name],
    ).fetchone()
    return row[0] > 0


def _column_exists(conn: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = ? AND column_name = ?",
        [table, column],
    ).fetchone()
    return row[0] > 0


def migrate(db_path: Path) -> None:
    """Apply v2 schema migration. Idempotent — safe to re-run."""
    conn = duckdb.connect(str(db_path))

    # ── 1. Create watermarks table ────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watermarks (
            name        TEXT PRIMARY KEY,
            timestamp   TIMESTAMP NOT NULL
        )
    """)
    logger.info("Ensured watermarks table exists")

    # ── 2. Create dead_letter table ───────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dead_letter (
            post_id     TEXT NOT NULL,
            domain      TEXT NOT NULL DEFAULT 'instagram',
            error       TEXT,
            attempts    INTEGER NOT NULL DEFAULT 0,
            failed_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status      TEXT NOT NULL DEFAULT 'pending',
            PRIMARY KEY (post_id, domain)
        )
    """)
    logger.info("Ensured dead_letter table exists")

    # ── 3. Create silver_ig_posts from silver_posts (if source exists) ────
    if _table_exists(conn, "silver_posts") and not _table_exists(conn, "silver_ig_posts"):
        conn.execute("""
            CREATE TABLE silver_ig_posts AS
            SELECT
                post_id, shortcode, url, caption, owner_id, owner_username,
                likes_count, comments_count, video_play_count, video_view_count,
                timestamp, hashtags, meta_data, has_engagement_bait,
                media_files, media_count, source_dataset,
                silvered_at AS processed_on
            FROM silver_posts
        """)
        logger.info("Created silver_ig_posts (silvered_at → processed_on)")

    # ── 4. Create gold_ig_analyses from gold_analyses (if source exists) ──
    if _table_exists(conn, "gold_analyses") and not _table_exists(conn, "gold_ig_analyses"):
        conn.execute("""
            CREATE TABLE gold_ig_analyses AS
            SELECT post_id, schema_version, result_json, analysed_at
            FROM gold_analyses
        """)
        logger.info("Created gold_ig_analyses (dropped status/error/attempts)")

    # ── 5. Create silver_ig_progress from silver_progress (if source exists) ─
    if _table_exists(conn, "silver_progress") and not _table_exists(conn, "silver_ig_progress"):
        conn.execute("""
            CREATE TABLE silver_ig_progress AS SELECT * FROM silver_progress
        """)
        logger.info("Created silver_ig_progress")

    # ── 6. Drop old tables (dependency order: gold → silver → progress) ───
    for old_tbl in ("gold_analyses", "silver_posts", "silver_progress"):
        if _table_exists(conn, old_tbl):
            conn.execute(f"DROP TABLE IF EXISTS \"{old_tbl}\"")
            logger.info("Dropped old table: %s", old_tbl)

    if _table_exists(conn, "silver_watermark"):
        conn.execute("DROP TABLE IF EXISTS silver_watermark")
        logger.info("Dropped silver_watermark table")

    # ── 7. Seed watermarks from existing data ─────────────────────────────
    now = datetime.now(timezone.utc)

    # silver_ig: use MAX(completed_at) from silver_ig_progress if available
    if _table_exists(conn, "silver_ig_progress"):
        row = conn.execute(
            "SELECT MAX(completed_at) FROM silver_ig_progress"
        ).fetchone()[0]
        ts = row if row is not None else now
        if isinstance(ts, datetime):
            conn.execute(
                "INSERT OR REPLACE INTO watermarks (name, timestamp) VALUES (?, ?)",
                ["silver_ig", ts],
            )
            logger.info("Seeded watermarks.silver_ig from silver_ig_progress: %s", ts)

    # gold_ig: use MAX(analysed_at) from gold_ig_analyses if available
    if _table_exists(conn, "gold_ig_analyses"):
        row = conn.execute(
            "SELECT MAX(analysed_at) FROM gold_ig_analyses"
        ).fetchone()[0]
        ts = row if row is not None else now
        if isinstance(ts, datetime):
            conn.execute(
                "INSERT OR REPLACE INTO watermarks (name, timestamp) VALUES (?, ?)",
                ["gold_ig", ts],
            )
            logger.info("Seeded watermarks.gold_ig from gold_ig_analyses: %s", ts)

    # ── 8. Verify ─────────────────────────────────────────────────────────
    # Note: FK gold_ig_analyses → silver_ig_posts is not restored here
    # because DuckDB's ALTER TABLE ADD FOREIGN KEY is not yet implemented.
    # The gold asset CREATE TABLE statement recreates it at runtime.

    # ── 9. Tables and watermarks verification ──────────────────────────────
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    ).fetchall()
    logger.info("Post-migration tables: %s", [t[0] for t in tables])

    wm = conn.execute("SELECT * FROM watermarks").fetchall()
    logger.info("Watermarks: %s", wm)

    conn.close()
    logger.info("Migration complete.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Migrate DuckDB schema to v2")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB,
        help=f"Path to DuckDB state file (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip backup (dangerous — only for CI/test copies)",
    )
    args = parser.parse_args()

    db_path = args.db_path.resolve()
    if not db_path.exists():
        logger.error("Database not found: %s", db_path)
        sys.exit(1)

    if not args.no_backup:
        _backup(db_path)

    migrate(db_path)


if __name__ == "__main__":
    main()
