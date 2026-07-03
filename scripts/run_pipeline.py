"""Run medallion pipeline: bronze-derived silver → enqueue → serving.

Operational script — runs assets directly via build_asset_context. No dagster
daemon required. Useful for testing schema changes, verifying the pipeline
end-to-end without touching Dagster UI.

Usage:
    uv run python scripts/run_pipeline.py                           # incremental run
    uv run python scripts/run_pipeline.py --reset                   # full re-scan
    uv run python scripts/run_pipeline.py --reset 2026-06-15T00:00:00Z
    uv run python scripts/run_pipeline.py --update-stale-analyses   # re-enqueue stale
    uv run python scripts/run_pipeline.py --dry-run                 # state only
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from dagster import build_asset_context

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.instagram.assets import ig_posts_gld_batches, ig_posts_slv
from datalake.defs.instagram.config import GoldConfig
from datalake.defs.serving.assets import dim_date, profile_dimension, v_post_detail

logger = logging.getLogger("run_pipeline")

DB_PATH = "data/state.duckdb"
OPS_PATH = "data/ops.sqlite"


# ── State reporting ──────────────────────────────────────────────────────


def _print_state(phase: str) -> None:
    import sqlite3

    import duckdb

    print(f"\n{'='*60}")
    print(f"  {phase}")
    print(f"{'='*60}")

    db = duckdb.connect(DB_PATH, read_only=True)
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    ).fetchall()
    for (t,) in tables:
        cnt = db.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f"  {t:25s} {cnt:>6} rows")

    wm = db.execute(
        "SELECT name, timestamp FROM watermarks ORDER BY name"
    ).fetchall()
    if wm:
        print("  --- watermarks ---")
        for name, ts in wm:
            print(f"  {name:25s} {ts}")
    db.close()

    ops = sqlite3.connect(f"file:{OPS_PATH}?mode=ro", uri=True)
    ops_tables = ops.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    for (t,) in ops_tables:
        cnt = ops.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  ops.{t:20s} {cnt:>6} rows")
    ops.close()


# ── Watermarks ───────────────────────────────────────────────────────────


def _parse_datetime(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _reset_watermarks(since: datetime) -> None:
    import duckdb

    db = duckdb.connect(DB_PATH)
    db.execute("DELETE FROM watermarks")
    db.execute(
        "INSERT INTO watermarks (name, timestamp) VALUES ('silver_ig', ?)",
        [since],
    )
    db.execute(
        "INSERT INTO watermarks (name, timestamp) VALUES ('gold_ig', ?)",
        [since],
    )
    db.close()
    logger.info("Reset watermarks: silver_ig + gold_ig set to %s", since)


# ── Stale analysis update ────────────────────────────────────────────────


def run_update_stale(ops: SQLiteResource) -> int:
    """Create a batch for posts whose prompt_hash is stale or missing."""
    import duckdb as _duckdb

    from datalake.defs.enrichment.batch import create_batch
    from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH

    print("\n--- Update stale analyses ---")
    db = _duckdb.connect(DB_PATH, read_only=True)
    stale_rows = db.execute(
        "SELECT post_id, domain FROM gold_analyses "
        "WHERE prompt_hash IS NULL OR prompt_hash != ?",
        [CURRENT_PROMPT_HASH],
    ).fetchall()
    db.close()

    if not stale_rows:
        print("  No stale analyses found.")
        return 0

    post_ids = [r[0] for r in stale_rows]
    domains = [r[1] for r in stale_rows]
    create_batch(ops, post_ids, domains)

    print(f"  Created batch with {len(stale_rows)} stale analyses for re-processing")
    return len(stale_rows)


# ── Pipeline steps ───────────────────────────────────────────────────────


def run_silver(duckdb: DuckDBResource) -> int:
    print("\n--- Silver (ig_posts_slv) ---")
    ctx = build_asset_context(resources={"duckdb": duckdb})
    result = ig_posts_slv(ctx)
    n = len(result)
    print(f"  Output: {n} rows, {result['post_id'].n_unique()} unique post_ids")
    return n


def run_enqueue(duckdb: DuckDBResource, ops: SQLiteResource) -> int:
    print("\n--- Enqueue (ig_posts_gld_batches) ---")
    result = ig_posts_gld_batches(
        config=GoldConfig(),
        duckdb=duckdb,
        ops=ops,
    )
    n = result["enqueued"][0] if len(result) > 0 else 0
    print(f"  Enqueued: {n} posts")
    return n


def run_serving(duckdb: DuckDBResource) -> None:
    print("\n--- Serving (dim_date + dim_profile + v_post_detail) ---")
    ctx = build_asset_context(resources={"duckdb": duckdb})
    dim_date(ctx)
    print("  dim_date done")
    profile_dimension(ctx)
    print("  dim_profile done")
    v_post_detail(ctx)
    print("  v_post_detail done (cascades to all downstream views)")


# ── Main ─────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Run medallion pipeline: silver → enqueue → serving"
    )
    parser.add_argument(
        "--reset",
        nargs="?",
        const="1901-01-01T00:00:00Z",
        default=None,
        metavar="DATETIME",
        help=(
            "Reset watermarks to given datetime "
            "(default: 1901-01-01T00:00:00Z, full re-scan)"
        ),
    )
    parser.add_argument(
        "--update-stale-analyses",
        action="store_true",
        help="Enqueue analyses with stale/missing prompt_hash for re-processing",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show current state only, don't run pipeline",
    )
    args = parser.parse_args()

    if args.dry_run:
        _print_state("Current state")
        return

    ops = SQLiteResource()

    if args.update_stale_analyses:
        run_update_stale(ops)
        _print_state("After stale update")
        print("\nDone. Run `python scripts/enrichment_worker.py` to re-process stale analyses.")
        return

    duckdb = DuckDBResource(database=DB_PATH)

    if args.reset:
        since = _parse_datetime(args.reset)
        _reset_watermarks(since)

    _print_state("Before")

    run_silver(duckdb)
    enqueued = run_enqueue(duckdb, ops)
    run_serving(duckdb)

    _print_state("After")

    print(f"\nDone. {enqueued} posts enqueued. Run `python scripts/enrichment_worker.py` to process.")


if __name__ == "__main__":
    main()
