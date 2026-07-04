"""Quarantine the o44ZGN3WOEuMzCgcf profile-detail dataset and reset silver.

The o44ZGN3WOEuMzCgcf dataset contains 365 rows of profile-page scrapes (null
shortcode, null caption, profile URLs like /username instead of /p/CODE/).
These are not real posts — they are Instagram profile detail records that
pollute ``silver_ig_posts`` and all downstream views.

This script:

1. Moves the bad Parquet to ``data/lake/bronze/.quarantine/``
2. Truncates ``silver_ig_posts`` (DuckDB)
3. Resets the silver and gold watermarks so the next asset materialization
   sees all remaining bronze files as "new"

After running this script, regenerate silver with::

    uv run dagster asset materialize -m datalake -a ig_posts_slv

This rebuilds silver from all remaining (clean) bronze files. Views
(analytics_views, v_post_detail, etc.) are regular SQL views and reflect
the new silver automatically.  ``gold_analyses`` is untouched (0 rows from
the bad dataset; all 6 enrichments are from other datasets).

Usage::

    uv run python scripts/migrate_quarantine_o44.py [--no-backup]
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger("migrate_quarantine_o44")

BAD_DATASET = "o44ZGN3WOEuMzCgcf"
BRONZE_DIR = Path("data/lake/bronze")
QUARANTINE_DIR = BRONZE_DIR / ".quarantine"
DUCKDB_PATH = Path("data/state.duckdb")


def _backup(db_path: Path) -> Path:
    """Copy the DuckDB file as a backup before migration."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_suffix(f".quarantine_o44_backup_{ts}.duckdb")
    shutil.copy2(db_path, backup)
    logger.info("Backed up %s → %s", db_path, backup)
    return backup


def migrate(duckdb_path: Path, backup: bool = True) -> None:
    # ── 0. Backup ──────────────────────────────────────────────────────
    if backup and duckdb_path.exists():
        _backup(duckdb_path)

    # ── 1. Quarantine the bad Parquet ──────────────────────────────────
    bad_parquet = BRONZE_DIR / f"{BAD_DATASET}.parquet"
    bad_meta = BRONZE_DIR / f"{BAD_DATASET}.parquet.meta"

    if not bad_parquet.exists():
        logger.error("Bad Parquet not found: %s", bad_parquet)
        logger.info("Nothing to quarantine. Run aborted.")
        sys.exit(1)

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    dest = QUARANTINE_DIR / bad_parquet.name
    shutil.move(str(bad_parquet), str(dest))
    logger.info("Quarantined %s → %s", bad_parquet.name, dest)

    if bad_meta.exists():
        dest_meta = QUARANTINE_DIR / bad_meta.name
        shutil.move(str(bad_meta), str(dest_meta))
        logger.info("Quarantined %s → %s", bad_meta.name, dest_meta)

    # ── 2. Truncate silver ─────────────────────────────────────────────
    db = duckdb.connect(str(duckdb_path))
    try:
        deleted_count = db.execute(
            "SELECT COUNT(*) FROM silver_ig_posts"
        ).fetchone()[0]
        db.execute("TRUNCATE TABLE silver_ig_posts")
        logger.info(
            "Truncated silver_ig_posts (%s rows removed)", deleted_count
        )
    finally:
        db.close()

    # ── 3. Reset watermarks ────────────────────────────────────────────
    reset_ts = datetime(1970, 1, 2, tzinfo=timezone.utc)
    db = duckdb.connect(str(duckdb_path))
    try:
        db.execute("DELETE FROM watermarks")
        db.execute(
            "INSERT INTO watermarks (name, timestamp) VALUES ('silver_ig', ?)",
            [reset_ts],
        )
        db.execute(
            "INSERT INTO watermarks (name, timestamp) VALUES ('gold_ig', ?)",
            [reset_ts],
        )
        logger.info(
            "Watermarks reset to %s (silver + gold)", reset_ts.isoformat()
        )
    finally:
        db.close()

    # ── 4. Summary ─────────────────────────────────────────────────────
    remaining = sorted(BRONZE_DIR.glob("*.parquet"))
    print(f"\nQuarantine complete. {len(remaining)} bronze files remain:")
    for f in remaining:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name:40s} {size_mb:7.1f} MB")
    print("\nNext step: regenerate silver —")
    print("  uv run dagster asset materialize -m datalake --select ig_posts_slv")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Quarantine o44 profile-detail dataset and reset silver"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip DuckDB backup before migration",
    )
    args = parser.parse_args()
    migrate(DUCKDB_PATH, backup=not args.no_backup)


if __name__ == "__main__":
    main()
