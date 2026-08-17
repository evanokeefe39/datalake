"""One-shot migration: backfill null owner_id in silver from bronze data.

Legacy bronze datasets carry the author's ``ownerId`` on every post row,
but rows ingested before the ``ownerId`` → ``owner_id`` mapping fix entered
silver with null ``owner_id``. Those posts cannot be attributed to a
creator via ``dim_profile`` (keyed on ``owner_id``), so per-creator views
(``v_creator_quality``, ``v_rising_creators``) silently exclude them while
owner-username-keyed surfaces (``/posts``) include them — the "ACT=0% vs
ACT=YES" mismatch (e.g. evolving.ai's two actionable posts).

This script reads all bronze Parquet files, finds the owner_id for each
post_id, and updates silver.

Idempotent — safe to run multiple times. Only touches rows where
owner_id IS NULL.

Usage:
    uv run python scripts/migrate_backfill_owner_id.py           # backfill
    uv run python scripts/migrate_backfill_owner_id.py --dry-run # show counts only
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
import polars as pl

BRONZE_DIR = Path("data/lake/bronze")
DB_PATH = "data/state.duckdb"

logger = logging.getLogger("migrate_backfill_owner_id")


def collect_owner_ids() -> dict[str, str]:
    """Scan all bronze Parquet files for (post_id → owner_id) mappings.

    Uses ``ownerId`` when present, falling back to ``owner_id`` (older
    schema variant). Both map to the same Instagram account id.
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

        for row in df.iter_rows(named=True):
            post_id = row.get("id")
            if post_id is None:
                continue
            owner_id = None
            if "ownerId" in df.columns and row.get("ownerId"):
                owner_id = row["ownerId"]
            elif "owner_id" in df.columns and row.get("owner_id"):
                owner_id = row["owner_id"]
            if owner_id:
                mapping[str(post_id)] = str(owner_id)

    return mapping

def apply_fixes(db: duckdb.DuckDBPyConnection, fixable: dict[str, str]) -> int:
    """Apply post_id → owner_id updates; returns the number of rows fixed."""
    fixed = 0
    for post_id, owner_id in fixable.items():
        db.execute(
            "UPDATE silver_ig_posts SET owner_id = ? WHERE post_id = ?",
            [owner_id, post_id],
        )
        fixed += 1
    return fixed


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Backfill null owner_id in silver from bronze data"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show affected row counts without making changes",
    )
    args = parser.parse_args()

    # Gather mappings from bronze
    logger.info("Scanning bronze files for owner_id mappings...")
    mapping = collect_owner_ids()
    logger.info("Found %d post_id → owner_id mappings", len(mapping))

    # Check silver for nulls
    db = duckdb.connect(DB_PATH)
    null_rows = db.execute(
        "SELECT post_id FROM silver_ig_posts WHERE owner_id IS NULL"
    ).fetchall()
    null_ids = {r[0] for r in null_rows}
    logger.info("Silver has %d rows with null owner_id", len(null_ids))

    fixable = {pid: oid for pid, oid in mapping.items() if pid in null_ids}
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
    fixed = apply_fixes(db, fixable)

    db.close()
    logger.info("Done. Fixed %d rows.", fixed)
    print(f"\n  Fixed: {fixed} rows")


if __name__ == "__main__":
    main()
