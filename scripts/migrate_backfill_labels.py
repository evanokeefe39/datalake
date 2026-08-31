"""One-shot migration: bootstrap ig_post_labels for existing silver posts.

Stamps every post already in silver_ig_posts as day0_heuristic/provisional
per the rule table (implementation plan §4). CORRECTNESS GUARD (US-L3):
every existing post predates the multimodal workstream, so the bootstrap
NEVER writes day7_matched — the daily ig_post_labels pass performs the
single day0→day7 upgrade when it runs against mature data. Baselines
(Q3/IQR/n) are still recorded so the bootstrap is analytically complete.

Reads the live profiles table (ops.sqlite) for tier1 core handles.

Idempotent — day0 labels are re-derived and re-upserted with PK dedup on
post_id; re-runs converge to the same state. Additive-only; never drops.

Usage:
    uv run python scripts/migrate_backfill_labels.py           # execute
    uv run python scripts/migrate_backfill_labels.py --dry-run # counts only
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

import duckdb

from datalake.defs.common.schemas import duckdb_ddl
from datalake.defs.instagram.labels import LABEL_VERSION, run_label_pass

DB_PATH = "data/state.duckdb"
OPS_PATH = "data/ops.sqlite"

logger = logging.getLogger("migrate_backfill_labels")


def core_handles() -> set[str]:
    """Tier1 enabled instagram handles from the ops profiles table."""
    import sqlite3

    con = sqlite3.connect(OPS_PATH)
    try:
        rows = con.execute(
            "SELECT handle FROM profiles "
            "WHERE platform = 'instagram' AND tier = 'tier1' AND enabled = 1"
        ).fetchall()
    except sqlite3.OperationalError:
        logger.warning("profiles table missing in %s — treating all as tail", OPS_PATH)
        return set()
    finally:
        con.close()
    return {h.lower().lstrip("@") for (h,) in rows}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report counts only")
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--ops", default=OPS_PATH)
    args = ap.parse_args()

    db = duckdb.connect(args.db)
    try:
        db.execute(duckdb_ddl("ig_post_labels"))
        before = db.execute("SELECT COUNT(*) FROM ig_post_labels").fetchone()[0]
        posts = db.execute("SELECT COUNT(*) FROM silver_ig_posts").fetchone()[0]
        print(f"\n  Posts in silver: {posts}")
        print(f"  Existing labels: {before}")
        print(f"  LABEL_VERSION:   {LABEL_VERSION}")

        if args.dry_run:
            print("\n  DRY RUN — no rows written.")
            return

        handles = core_handles()
        print(f"  Core (tier1) handles: {len(handles)}")
        stats = run_label_pass(
            db, core_handles=handles,
            bootstrap=True, now=datetime.now(timezone.utc),
        )
        after = db.execute("SELECT COUNT(*) FROM ig_post_labels").fetchone()[0]
        day7 = db.execute(
            "SELECT COUNT(*) FROM ig_post_labels WHERE method = 'day7_matched'"
        ).fetchone()[0]
        print(f"\n  Stamped: {stats['stamped']} (table total: {after})")
        print(f"  by_method:   {stats['by_method']}")
        print(f"  by_decision: {stats['by_decision']}")
        if day7:
            raise SystemExit(
                f"CORRECTNESS GUARD FAILED: bootstrap wrote {day7} day7_matched rows"
            )
        print(f"  day7_matched rows: {day7} (guard passed)")
    finally:
        db.close()


if __name__ == "__main__":
    main()
