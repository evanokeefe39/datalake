"""Schema drift migration (LARGELY RETIRED 2026-09-15, W9): cleanup only.

Historical role: gold_ig_analyses → gold_analyses rename, dead_letter move.
BOTH are retired — gold_analyses was dropped (superseded by
silver_content_classification) and dead_letter went with the ops.sqlite queue
(ADR-0012). What remains live is the vestigial-table cleanup below.

Detected drift between live state.duckdb and the current schema catalog:

  gold_ig_analyses → gold_analyses  (rename, add domain+prompt_hash, drop schema_version)
  silver_ig_progress                (drop — vestigial; watermarks replaced it)
  dead_letter move                  (RETIRED — the table was dropped, ADR-0012)

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
    """RETIRED 2026-09-15 (W9) — creates NOTHING.

    This used to ``CREATE TABLE IF NOT EXISTS`` enrichment_queue,
    ``media_metadata`` and ``dead_letter``. All are retired (ADR-0012 for the
    queue; ISSUES.md #32 for ``media_metadata``, a Gemini File-API upload cache
    for a permanently retired provider).

    Retired — not deleted — because this file is the ONE migration the README
    marks "RUN against live", so an operator may reach it. Deleting the call
    site would hide that; a no-op body tells them why nothing happens.

    This was one of SIX paths that recreated retired tables after the W9 drop.
    One surviving creator makes the drop non-durable, so all six had to go.
    """
    return None


# ── Migration ───────────────────────────────────────────────────────────────


def migrate(duckdb_path: Path, ops_path: Path, backup: bool = True) -> None:
    if backup and duckdb_path.exists():
        _backup(duckdb_path)

    conn = duckdb.connect(str(duckdb_path))

    # ── 1. RETIRED 2026-09-15 (W9) — the gold_analyses steps are DELETED. ──
    #
    # This section used to rename gold_ig_analyses -> gold_analyses, INSERT the
    # rows across, then recreate `analytics_views` with a LEFT JOIN onto
    # gold_analyses. All of that is now wrong:
    #
    #   * gold_analyses was superseded by silver_content_classification and
    #     DROPPED by the W9 retirement (archived at
    #     data/lake/archive/gold_analyses/). ISSUES.md #34.
    #   * `analytics_views` is BANNED: serving/assets.py:204 explicitly drops it
    #     "so no serving view can keep referencing the retired table", and
    #     test_state_compatibility.py flags its presence as stale drift.
    #     Recreating it here would resurrect both the view AND the table.
    #
    # The statements are DELETED rather than renamed. A rename (e.g. to
    # `retired_..._NEVER`) would CREATE junk tables on every run — the same
    # defect class as the raw DDL it was meant to replace.
    #
    # This file is the one migration the README marks "RUN against live", so an
    # operator may reach it: the section is kept as this explicit no-op record
    # rather than removed silently.
    logger.info(
        "Skipping the gold_analyses steps — RETIRED by the W9 retirement "
        "(2026-09-15). See ISSUES.md #34."
    )


    # ── 2. Drop silver_ig_progress ───────────────────────────────────────
    if _table_exists(conn, "silver_ig_progress"):
        conn.execute("DROP TABLE IF EXISTS silver_ig_progress")
        logger.info("Dropped silver_ig_progress (vestigial)")

    # ── 3. RETIRED 2026-09-15 (W9) — the dead_letter move is DELETED. ──────
    #
    # This step moved dead_letter rows from state.duckdb into ops.sqlite, then
    # dropped the DuckDB copy. Both halves are wrong now: the ops.sqlite
    # dead_letter table was DROPPED (ADR-0012 retired the queue), so the
    # `INSERT OR IGNORE INTO dead_letter` that stood here would either error or
    # resurrect a retired table. The WRITERS are removed together with the
    # creator — leaving them is a half-cut, and a half-cut is what made the W9
    # drop non-durable in the first place.
    #
    # The DuckDB-side DROP is gone too: nothing creates that table now, so it
    # is a no-op at best and a silent hazard at worst.
    #
    # See ISSUES.md #34 (W9 reconciliation).
    if _table_exists(conn, "dead_letter"):
        logger.info(
            "Found a dead_letter table in state.duckdb but LEFT IT ALONE: the "
            "move is retired (W9, 2026-09-15 - ADR-0012). See ISSUES.md #34."
        )

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
            "cleanup (gold_analyses + dead_letter steps retired by W9)."
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
