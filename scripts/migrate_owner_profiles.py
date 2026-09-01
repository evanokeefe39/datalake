"""Backfill creators + profiles from silver post owners (ad-hoc, -1 sentinel).

Every distinct ``owner_username`` in ``silver_ig_posts`` gets a ``creators``
row (named after the owner) and an Instagram ``profiles`` row associated with
it, tagged ``results_limit = -1`` (``AD_HOC_LIMIT``) — meaning "ad hoc,
already ingested from disk, never schedule a continuous scrape."

Idempotent: ``create_creator`` and ``add_profile`` are upserts, so re-running
is a no-op for owners that already have a profile. Owners who already exist
as a profile are left untouched (their existing depth/sentinel is preserved).

Usage:
    uv run python scripts/migrate_owner_profiles.py
    uv run python scripts/migrate_owner_profiles.py --ops data/ops.sqlite --duckdb data/state.duckdb
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.instagram.creators import AD_HOC_LIMIT, add_profile, create_creator

logger = logging.getLogger("migrate_owner_profiles")

DEFAULT_OPS = Path("data/ops.sqlite")
DEFAULT_DUCKDB = Path("data/state.duckdb")


def _distinct_owners(duckdb_path: Path) -> list[str]:
    """Return every distinct non-null ``owner_username`` in silver_ig_posts."""
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT DISTINCT owner_username FROM silver_ig_posts "
            "WHERE owner_username IS NOT NULL AND owner_username != '' "
            "ORDER BY owner_username"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def migrate(ops_path: Path, duckdb_path: Path) -> dict:
    """Backfill creators+profiles for every silver owner. Returns counts."""
    ops = SQLiteResource(database=str(ops_path))
    owners = _distinct_owners(duckdb_path)

    created_creators = 0
    created_profiles = 0
    skipped_existing = 0
    for owner in owners:
        # Create-or-get the creator (upsert by name).
        creator = create_creator(ops, owner)
        # Add an ad-hoc Instagram profile linked to the creator. add_profile
        # is an upsert on (platform, handle), so existing profiles are reused.
        add_profile(
            ops,
            creator_id=creator["id"],
            platform="instagram",
            handle=owner,
            results_type="posts",
            results_limit=AD_HOC_LIMIT,
            enabled=True,
            tier="tier1",
        )
        created_profiles += 1
        created_creators += 1

    logger.info(
        "Backfilled %d owners -> %d creators + %d profiles (idempotent upsert; "
        "existing profiles preserved).",
        len(owners),
        created_creators,
        created_profiles,
    )
    return {
        "owners": len(owners),
        "created_creators": created_creators,
        "created_profiles": created_profiles,
        "skipped_existing": skipped_existing,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ops",
        type=Path,
        default=DEFAULT_OPS,
        help="Path to ops.sqlite (default: data/ops.sqlite)",
    )
    parser.add_argument(
        "--duckdb",
        type=Path,
        default=DEFAULT_DUCKDB,
        help="Path to state.duckdb (default: data/state.duckdb)",
    )
    args = parser.parse_args()
    migrate(args.ops, args.duckdb)
    logger.info("Backfill complete.")


if __name__ == "__main__":
    sys.exit(main())
