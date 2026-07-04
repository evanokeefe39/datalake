"""Schema drift migration: gold_ig_analyses → gold_analyses, dead_letter move, cleanup.

Detected drift between live state.duckdb and the current schema catalog:

  gold_ig_analyses → gold_analyses  (rename, add domain+prompt_hash, drop schema_version)
  silver_ig_progress                (drop — vestigial; watermarks replaced it)
  dead_letter in state.duckdb       (move to ops.sqlite where the queue architecture expects it)

Idempotent — safe to re-run. Existing data is preserved.
Backs up state.duckdb to data/state.duckdb.bak before modifying.

Usage:
    uv run python scripts/migrate_schema_drift.py [--no-backup]
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

import duckdb

logger = logging.getLogger("migrate_drift")

DEFAULT_DUCKDB = Path("data/state.duckdb")
DEFAULT_OPS_DB = Path("data/ops.sqlite")


# ── Helpers ────────────────────────────────────────────────────────────────


def _backup(db_path: Path) -> Path:
    """Copy the DuckDB file as a backup before migration."""
    import shutil

    backup = db_path.with_suffix(".duckdb.bak")
    shutil.copy2(db_path, backup)
    logger.info("Backed up %s → %s", db_path, backup)
    return backup


def _table_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        [name],
    ).fetchone()
    return row[0] > 0


def _sqlite_ensure_tables(ops_path: Path) -> None:
    """Create ops.sqlite tables if they don't exist (idempotent)."""
    conn = sqlite3.connect(str(ops_path))
    conn.executescript("""
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
    """)
    conn.commit()
    conn.close()
    logger.info("Ensured ops.sqlite tables: enrichment_queue, media_metadata, dead_letter")


# ── Migration ───────────────────────────────────────────────────────────────


def migrate(duckdb_path: Path, ops_path: Path, backup: bool = True) -> None:
    if backup and duckdb_path.exists():
        _backup(duckdb_path)

    conn = duckdb.connect(str(duckdb_path))

    # ── 1. gold_ig_analyses → gold_analyses ──────────────────────────────
    if _table_exists(conn, "gold_ig_analyses") and not _table_exists(conn, "gold_analyses"):
        conn.execute("""
            CREATE TABLE gold_analyses (
                post_id     VARCHAR,
                domain      VARCHAR NOT NULL DEFAULT 'instagram',
                prompt_hash VARCHAR,
                result_json VARCHAR,
                analysed_at VARCHAR NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)
        conn.execute("""
            INSERT INTO gold_analyses
            SELECT
                post_id,
                'instagram',
                NULL,
                result_json,
                CAST(analysed_at AS VARCHAR)
            FROM gold_ig_analyses
        """)
        count = conn.execute("SELECT COUNT(*) FROM gold_analyses").fetchone()[0]
        logger.info("Created gold_analyses from gold_ig_analyses (%d rows)", count)

    if _table_exists(conn, "gold_ig_analyses") and _table_exists(conn, "gold_analyses"):
        conn.execute("DROP TABLE IF EXISTS gold_ig_analyses")
        logger.info("Dropped gold_ig_analyses")

    # ── 1b. Recreate analytics_views (references gold_analyses, not gold_ig_analyses) ─
    conn.execute("DROP VIEW IF EXISTS analytics_views")
    conn.execute("""
        CREATE VIEW analytics_views AS
        SELECT
            sp.post_id, sp.shortcode, sp.url, sp.caption,
            sp.owner_id, sp.owner_username,
            sp.likes_count, sp.comments_count, sp.video_view_count,
            sp.timestamp, sp.hashtags, sp.source_dataset, sp.processed_on,
            ga.result_json,
            ga.analysed_at AS gold_analysed_at,
            dp.profile_key, dp.channel,
            dp.effective_from, dp.effective_to, dp.is_current
        FROM silver_ig_posts AS sp
        LEFT JOIN gold_analyses AS ga ON sp.post_id = ga.post_id
        LEFT JOIN dim_profile AS dp
            ON sp.owner_id = dp.owner_id AND dp.is_current = true
    """)
    logger.info("Recreated analytics_views (gold_ig_analyses → gold_analyses)")

    # ── 2. Drop silver_ig_progress ───────────────────────────────────────
    if _table_exists(conn, "silver_ig_progress"):
        conn.execute("DROP TABLE IF EXISTS silver_ig_progress")
        logger.info("Dropped silver_ig_progress (vestigial)")

    # ── 3. Move dead_letter from DuckDB → SQLite ─────────────────────────
    if _table_exists(conn, "dead_letter"):
        rows = conn.execute(
            "SELECT post_id, domain, error, attempts, failed_at FROM dead_letter"
        ).fetchall()
        if rows:
            ops = sqlite3.connect(str(ops_path))
            _sqlite_ensure_tables(ops_path)  # ensure table exists
            for row in rows:
                try:
                    ops.execute(
                        "INSERT OR IGNORE INTO dead_letter "
                        "(post_id, domain, error, attempts, failed_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        row,
                    )
                except sqlite3.IntegrityError:
                    pass
            ops.commit()
            ops.close()
            logger.info("Moved %d dead_letter rows to ops.sqlite", len(rows))

        conn.execute("DROP TABLE IF EXISTS dead_letter")
        logger.info("Dropped dead_letter from state.duckdb")

    # ── 4. Ensure ops.sqlite schema ──────────────────────────────────────
    _sqlite_ensure_tables(ops_path)

    # ── 5. Verify ────────────────────────────────────────────────────────
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    ).fetchall()
    logger.info("Post-migration DuckDB tables: %s", [t[0] for t in tables])

    wm = conn.execute("SELECT * FROM watermarks").fetchall()
    logger.info("Watermarks: %s", wm)

    # Verify ops.sqlite
    ops = sqlite3.connect(str(ops_path))
    ops_tables = ops.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    logger.info("Ops DB tables: %s", [t[0] for t in ops_tables])
    ops.close()

    conn.close()
    logger.info("Migration complete.")


# ── CLI ─────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Migrate schema drift: gold_ig_analyses → gold_analyses, "
            "dead_letter move, cleanup."
        ),
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DUCKDB),
        help=f"Path to state.duckdb (default: {DEFAULT_DUCKDB})",
    )
    parser.add_argument(
        "--ops-path",
        default=str(DEFAULT_OPS_DB),
        help=f"Path to ops.sqlite (default: {DEFAULT_OPS_DB})",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip backup before migration",
    )
    args = parser.parse_args()

    duckdb_path = Path(args.db_path)
    ops_path = Path(args.ops_path)

    if not duckdb_path.exists():
        logger.error("DuckDB file not found: %s", duckdb_path)
        sys.exit(1)

    migrate(duckdb_path, ops_path, backup=not args.no_backup)


if __name__ == "__main__":
    main()
