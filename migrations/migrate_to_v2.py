"""Standalone migration: domain-scoped tables, watermarks (LARGELY RETIRED).

Historical role (Phase 1-4 → v2) with the retired steps marked:
- silver_posts → silver_ig_posts (column silvered_at → processed_on)
- gold_analyses → gold_ig_analyses                    [RETIRED, W9]
- silver_progress → silver_ig_progress
- silver_watermark → dropped                          [RETIRED, vestigial]
- New: watermarks; dead_letter                        [dead_letter RETIRED, ADR-0012]

The gold_analyses / gold_ig_analyses / dead_letter steps were DELETED
2026-09-15 (W9): those tables are dropped and archived. This script is
historical — it has already run — and must not be used to recreate them.
See ISSUES.md #34.

Usage:
    uv run python scripts/migrate_to_v2.py [--db-path data/state.duckdb]
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import UTC, datetime
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
            timestamp   TIMESTAMP NOT NULL,
            config_hash TEXT
        )
    """)
    logger.info("Ensured watermarks table exists")

    # ── 2. dead_letter: RETIRED 2026-09-15 (W9) ─────────────────────────
    # This step created `dead_letter`, which the W9 retirement DROPPED
    # (ADR-0012 retired the ops.sqlite queue). The CREATE is deleted, not
    # renamed — a renamed table would create junk on every run.
    # See ISSUES.md #34.

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

    # ── 4. RETIRED 2026-09-15 (W9) — the gold_ig_analyses step is DELETED. ──
    #
    # This created `gold_ig_analyses` from `gold_analyses`. BOTH names are now
    # retired: `gold_analyses` was dropped by the W9 retirement and superseded
    # by `silver_content_classification`, and `gold_ig_analyses` was itself
    # renamed away in the schema-drift migration. It only appeared inert
    # because of the `_table_exists` guard — a guard is not a retirement.
    #
    # The statement is DELETED, not renamed: a rename would create junk tables.
    # See ISSUES.md #34.

    # ── 5. Create silver_ig_progress from silver_progress (if source exists) ─
    if _table_exists(conn, "silver_progress") and not _table_exists(conn, "silver_ig_progress"):
        conn.execute("""
            CREATE TABLE silver_ig_progress AS SELECT * FROM silver_progress
        """)
        logger.info("Created silver_ig_progress")

    # ── 6. Drop old tables (dependency order: gold → silver → progress) ───
    # `gold_analyses` was dropped by the W9 retirement, so its entry here is
    # inert (guarded by _table_exists) and only kept so the historical drop
    # order stays documented. Do NOT read it as a live target.
    for old_tbl in ("gold_analyses", "silver_posts", "silver_progress"):
        if _table_exists(conn, old_tbl):
            conn.execute(f"DROP TABLE IF EXISTS \"{old_tbl}\"")
            logger.info("Dropped old table: %s", old_tbl)

    if _table_exists(conn, "silver_watermark"):
        conn.execute("DROP TABLE IF EXISTS silver_watermark")
        logger.info("Dropped silver_watermark table")

    # ── 7. Seed watermarks from existing data ─────────────────────────────
    now = datetime.now(UTC)

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

    # gold_ig watermark: RETIRED 2026-09-15 (W9) — this step is DELETED.
    #
    # It read MAX(analysed_at) FROM gold_ig_analyses (both the table and the
    # `gold_ig` watermark are retired — the table was renamed away then dropped
    # by W9, and Epic 3 retired the watermark in favour of the label-driven
    # drain). It appeared inert only because of the `_table_exists` guard.
    # See ISSUES.md #34.

    # ── 8. Verify ─────────────────────────────────────────────────────────
    # Note: the FK gold_ig_analyses → silver_ig_posts is not restored here
    # (DuckDB's ALTER TABLE ADD FOREIGN KEY is not implemented). This used to
    # say "the gold asset CREATE TABLE recreates it at runtime" — that producer
    # was RETIRED by W9, so no such recreation happens any more.

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
