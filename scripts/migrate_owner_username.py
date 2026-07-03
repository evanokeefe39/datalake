"""One-shot migration: backfill null owner_username in silver from bronze data.

Profile-scraped bronze files lack the ``ownerUsername`` column but have
``username``. Before the coalesce fix (2026-07-03), these rows entered
silver with null ``owner_username``. This script reads all bronze Parquet
files, finds the fallback username for each post_id, and updates silver.

Idempotent — safe to run multiple times. Only touches rows where
owner_username IS NULL.

Usage:
    uv run python scripts/migrate_owner_username.py          # backfill
    uv run python scripts/migrate_owner_username.py --dry-run  # show counts only
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
import polars as pl

BRONZE_DIR = Path("data/lake/bronze")
DB_PATH = "data/state.duckdb"

logger = logging.getLogger("migrate_owner_username")


def collect_owner_usernames() -> dict[str, str]:
    """Scan all bronze Parquet files for (post_id → owner_username) mappings.

    For each row, if ``ownerUsername`` is present and non-null, uses it.
    Otherwise falls back to ``username`` if available.
    """
    mapping: dict[str, str] = {}
    files = sorted(BRONZE_DIR.glob("*.parquet"))
    if not files:
        logger.info("No bronze files found.")
        return mapping

    for fp in files:
        df = pl.read_parquet(fp)
        if "id" not in df.columns:
            continue

        has_owner_uname = "ownerUsername" in df.columns
        has_uname = "username" in df.columns

        if not has_owner_uname and not has_uname:
            continue

        for row in df.iter_rows(named=True):
            post_id = row.get("id")
            if post_id is None:
                continue

            owner_username = None
            if has_owner_uname and row.get("ownerUsername"):
                owner_username = row["ownerUsername"]
            elif has_uname and row.get("username"):
                owner_username = row["username"]

            if owner_username:
                mapping[post_id] = owner_username

    return mapping


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Backfill null owner_username in silver from bronze data"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show affected row counts without making changes",
    )
    args = parser.parse_args()

    # Gather mappings from bronze
    logger.info("Scanning bronze files for owner_username mappings...")
    mapping = collect_owner_usernames()
    logger.info("Found %d post_id → owner_username mappings", len(mapping))

    # Check silver for nulls
    db = duckdb.connect(DB_PATH)
    null_rows = db.execute(
        "SELECT post_id FROM silver_ig_posts WHERE owner_username IS NULL"
    ).fetchall()
    null_ids = {r[0] for r in null_rows}
    logger.info("Silver has %d rows with null owner_username", len(null_ids))

    fixable = {pid: uname for pid, uname in mapping.items() if pid in null_ids}
    unfixable = null_ids - set(mapping.keys())

    print(f"\n  Fixable (found in bronze):     {len(fixable)}")
    print(f"  Unfixable (not in any bronze): {len(unfixable)}")
    if unfixable:
        sample = list(unfixable)[:5]
        print(f"  Sample unfixable ids: {sample}")

    if args.dry_run:
        db.close()
        return

    if not fixable:
        logger.info("Nothing to fix.")
        db.close()
        return

    # Apply fixes
    logger.info("Applying %d fixes...", len(fixable))
    fixed = 0
    for post_id, owner_username in fixable.items():
        db.execute(
            "UPDATE silver_ig_posts SET owner_username = ? WHERE post_id = ?",
            [owner_username, post_id],
        )
        fixed += 1

    db.close()
    logger.info("Done. Fixed %d rows.", fixed)
    print(f"\n  Fixed: {fixed} rows")


if __name__ == "__main__":
    main()
