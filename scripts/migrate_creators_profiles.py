"""Migrate ``scrape_targets`` → ``creators`` + ``profiles``; recreate lost ops tables.

The creators/profiles split replaces the single ``scrape_targets`` control
table. This one-shot script:

1. Creates the ``creators`` and ``profiles`` tables.
2. Backfills each existing ``scrape_targets`` row into a 1:1 creator + profile
   (creator name = the profile's ``full_name`` when known, else its handle).
3. Recreates the batch tables (``batch_jobs``/``batch_items``/``media_metadata``/
   ``dead_letter``) that were lost when ``ops.sqlite`` was recreated externally.
4. Drops ``scrape_targets``.

Idempotent — safe to re-run. Existing creators/profiles are preserved via the
``profiles`` (platform, handle) primary key.

Usage:
    uv run python scripts/migrate_creators_profiles.py
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger("migrate_creators_profiles")

DEFAULT_OPS = Path("data/ops.sqlite")
DEFAULT_DUCKDB = Path("data/state.duckdb")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", [name]
    ).fetchone()
    return row is not None


def _full_name_for(duckdb_con: duckdb.DuckDBPyConnection, username: str) -> str | None:
    """Look up a profile's ``full_name`` from silver_ig_profiles, if present."""
    try:
        row = duckdb_con.execute(
            "SELECT full_name FROM silver_ig_profiles WHERE owner_username = ?",
            [username],
        ).fetchone()
    except Exception:
        return None  # table absent or not yet populated
    if row is None:
        return None
    value = row[0]
    return value if value else None


def _ensure_batch_tables(con: sqlite3.Connection) -> None:
    """Recreate the batch tables lost when ops.sqlite was recreated externally."""
    con.executescript(
        """
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
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL,
            UNIQUE(job_id, payload)
        );
        CREATE INDEX IF NOT EXISTS idx_batch_items_job_status
            ON batch_items(job_id, status);
        CREATE TABLE IF NOT EXISTS media_metadata (
            media_url_hash         TEXT PRIMARY KEY,
            media_url              TEXT NOT NULL,
            file_api_uri           TEXT,
            mime_type              TEXT,
            file_size              INTEGER,
            video_duration_seconds REAL,
            upload_state           TEXT DEFAULT 'pending',
            expires_at             TEXT,
            created_at             TEXT NOT NULL,
            uploaded_at            TEXT
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
    )


def migrate(ops_path: Path, duckdb_path: Path) -> None:
    ops = sqlite3.connect(str(ops_path))
    ops.row_factory = sqlite3.Row
    try:
        _ensure_batch_tables(ops)

        # creators + profiles schema (idempotent)
        ops.execute("""
            CREATE TABLE IF NOT EXISTS creators (
                id         INTEGER PRIMARY KEY,
                name       TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        ops.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                platform      TEXT NOT NULL,
                handle        TEXT NOT NULL,
                profile_url   TEXT NOT NULL,
                results_type  TEXT NOT NULL DEFAULT 'details',
                results_limit INTEGER NOT NULL DEFAULT 1,
                enabled       INTEGER NOT NULL DEFAULT 1,
                tier          TEXT NOT NULL DEFAULT 'tier1',
                creator_id    INTEGER NOT NULL REFERENCES creators(id) ON DELETE CASCADE,
                updated_at    TEXT NOT NULL,
                PRIMARY KEY (platform, handle)
            )
        """)
        ops.commit()

        if not _table_exists(ops, "scrape_targets"):
            logger.info("scrape_targets absent — nothing to backfill")
            return

        targets = ops.execute(
            "SELECT username, profile_url, results_type, results_limit, "
            "enabled, tier, updated_at FROM scrape_targets ORDER BY username"
        ).fetchall()

        duckdb_con = (
            duckdb.connect(str(duckdb_path), read_only=True)
            if duckdb_path.exists()
            else None
        )
        try:
            created_creators = 0
            created_profiles = 0
            for t in targets:
                username = t["username"]
                existing = ops.execute(
                    "SELECT 1 FROM profiles WHERE platform = 'instagram' AND handle = ?",
                    [username],
                ).fetchone()
                if existing is not None:
                    continue  # already migrated

                full_name = _full_name_for(duckdb_con, username) if duckdb_con else None
                creator_name = full_name or username
                now = _now_iso()
                cur = ops.execute(
                    "INSERT INTO creators (name, created_at, updated_at) VALUES (?, ?, ?)",
                    [creator_name, now, now],
                )
                creator_id = cur.lastrowid
                ops.execute(
                    "INSERT OR IGNORE INTO profiles "
                    "(platform, handle, profile_url, results_type, results_limit, "
                    " enabled, tier, creator_id, updated_at) "
                    "VALUES ('instagram', ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        username,
                        t["profile_url"],
                        t["results_type"] or "details",
                        t["results_limit"] or 1,
                        t["enabled"] if t["enabled"] is not None else 1,
                        t["tier"] or "tier1",
                        creator_id,
                        t["updated_at"] or now,
                    ],
                )
                created_creators += 1
                created_profiles += 1

            ops.execute("DROP TABLE scrape_targets")
            ops.commit()
        finally:
            if duckdb_con:
                duckdb_con.close()

        logger.info(
            "Backfilled %d creators and %d profiles; dropped scrape_targets",
            created_creators,
            created_profiles,
        )
    finally:
        ops.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ops", default=str(DEFAULT_OPS), help="Path to ops.sqlite")
    parser.add_argument(
        "--duckdb", default=str(DEFAULT_DUCKDB), help="Path to state.duckdb"
    )
    args = parser.parse_args()
    migrate(Path(args.ops), Path(args.duckdb))
    logger.info("Migration complete.")


if __name__ == "__main__":
    main()
