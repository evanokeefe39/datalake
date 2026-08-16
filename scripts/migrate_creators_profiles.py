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

from datalake.defs.common.schemas import sqlite_ddl, sqlite_ddl_for

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
        sqlite_ddl_for("batch_jobs", "batch_items", "media_metadata", "dead_letter")
    )


def migrate(ops_path: Path, duckdb_path: Path) -> None:
    ops = sqlite3.connect(str(ops_path))
    ops.row_factory = sqlite3.Row
    try:
        _ensure_batch_tables(ops)

        # creators + profiles schema (idempotent, derived from the catalog)
        ops.execute(sqlite_ddl("creators"))
        ops.execute(sqlite_ddl("profiles"))
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
