"""One-shot migration: backfill follower observations from ORIGINAL bronze.

Builds the ``silver_ig_profile_observations`` rows for every profile visible
in the existing lake by reading the ORIGINAL bronze Parquet files (immutable).
The ``observed_at`` provenance chain is: ``.meta`` sidecar ``downloaded_at``
→ bronze file mtime — the ORIGINAL scrape time, NEVER the backfill run time.

Reads bronze, never current silver. The follower-count gate is the SAME code
path as the live writer (``_profile_observations``): details files always
qualify (they always carry ``followersCount``); posts files only when the
scrape actor embedded the owner object — a defaulted 0 is never recorded as
an observation.

Idempotent — ``INSERT OR IGNORE`` on PK ``(owner_id, observed_at,
source_dataset)`` means re-runs add zero rows. Additive-only; never drops or
alters tables.

Usage:
    uv run python scripts/migrate_backfill_profile_observations.py           # backfill
    uv run python scripts/migrate_backfill_profile_observations.py --dry-run # counts only
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl

from datalake.defs.common.schemas import duckdb_ddl
from datalake.defs.instagram.assets import (
    _PROFILE_OBS_COLUMNS,
    _classify_bronze,
    _profile_observations,
    _read_downloaded_at,
)

BRONZE_DIR = Path("data/lake/bronze")
DB_PATH = "data/state.duckdb"

logger = logging.getLogger("migrate_backfill_profile_observations")


def collect_observations(bronze_dir: Path = BRONZE_DIR) -> list[tuple]:
    """Scan bronze Parquet files for follower-observation tuples.

    Row order matches ``_PROFILE_OBS_COLUMNS`` so each tuple maps 1:1 onto
    ``INSERT INTO silver_ig_profile_observations``.
    """
    rows: list[tuple] = []
    files = sorted(bronze_dir.glob("*.parquet"))
    if not files:
        logger.info("No bronze files found.")
        return rows

    n_details = n_posts = 0
    for fp in files:
        try:
            df = pl.read_parquet(fp)
        except Exception as exc:
            logger.warning("Skipping %s — unreadable: %s", fp.name, exc)
            continue
        if len(df) == 0:
            continue

        meta_path = fp.with_suffix(".parquet.meta")
        entity_type = _classify_bronze(df, meta_path)
        if entity_type not in ("details", "posts"):
            logger.info("Skipping %s — entity type '%s'", fp.name, entity_type)
            continue

        observed_at = _read_downloaded_at(meta_path) or datetime.fromtimestamp(
            fp.stat().st_mtime, tz=timezone.utc
        )
        obs = _profile_observations(df, entity_type, fp.stem, observed_at)
        if obs is None:
            logger.info("Skipping %s — no genuine followersCount (gate)", fp.name)
            continue
        if entity_type == "details":
            n_details += 1
        else:
            n_posts += 1
        rows.extend(
            tuple(row[c] for c in _PROFILE_OBS_COLUMNS)
            for row in obs.iter_rows(named=True)
        )
    logger.info(
        "Files with observations: %d details, %d posts", n_details, n_posts
    )
    return rows


def apply_backfill(db: duckdb.DuckDBPyConnection, rows: list[tuple]) -> int:
    """Insert observation rows with PK dedup; returns rows actually added."""
    before = db.execute(
        "SELECT COUNT(*) FROM silver_ig_profile_observations"
    ).fetchone()[0]
    db.executemany(
        "INSERT OR IGNORE INTO silver_ig_profile_observations "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    after = db.execute(
        "SELECT COUNT(*) FROM silver_ig_profile_observations"
    ).fetchone()[0]
    return after - before


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="show what would be backfilled"
    )
    args = parser.parse_args()

    db = duckdb.connect(DB_PATH)
    db.execute(duckdb_ddl("silver_ig_profile_observations"))

    rows = collect_observations()
    print(f"\n  Bronze observations: {len(rows)} (unique owner_id × file)")

    if args.dry_run:
        print("  Dry run — no rows written.")
        db.close()
        return

    added = apply_backfill(db, rows)
    total = db.execute(
        "SELECT COUNT(*) FROM silver_ig_profile_observations"
    ).fetchone()[0]
    db.close()
    print(f"\n  Added: {added} rows (table total: {total})")


if __name__ == "__main__":
    main()
