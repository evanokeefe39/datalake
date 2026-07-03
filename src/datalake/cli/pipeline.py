"""Pipeline commands: run, batches, watermarks."""

from __future__ import annotations

import logging

import typer
from dagster import build_asset_context

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.instagram.assets import ig_posts_gld_batches, ig_posts_slv
from datalake.defs.instagram.config import GoldConfig
from datalake.defs.serving.assets import dim_date, profile_dimension, v_post_detail

from ._state import (
    DEFAULT_RESET_DATE,
    parse_datetime,
    print_batches,
    print_full_state,
    print_watermarks,
    reset_batches,
    reset_watermarks,
)

app = typer.Typer(
    name="pipeline",
    help="Run medallion pipeline: silver → batches → serving.",
    no_args_is_help=True,
)

DB_PATH = "data/state.duckdb"


# ── Stale update ───────────────────────────────────────────────────────────


def _run_update_stale(ops: SQLiteResource) -> int:
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


# ── Pipeline steps ─────────────────────────────────────────────────────────


def _run_silver(duckdb: DuckDBResource) -> int:
    print("\n--- Silver (ig_posts_slv) ---")
    ctx = build_asset_context(resources={"duckdb": duckdb})
    result = ig_posts_slv(ctx)
    n = len(result)
    print(f"  Output: {n} rows, {result['post_id'].n_unique()} unique post_ids")
    return n


def _run_enqueue(duckdb: DuckDBResource, ops: SQLiteResource) -> int:
    print("\n--- Enqueue (ig_posts_gld_batches) ---")
    result = ig_posts_gld_batches(
        config=GoldConfig(), duckdb=duckdb, ops=ops
    )
    n = result["enqueued"][0] if len(result) > 0 else 0
    print(f"  Enqueued: {n} posts")
    return n


def _run_serving(duckdb: DuckDBResource) -> None:
    print("\n--- Serving (dim_date + dim_profile + v_post_detail) ---")
    ctx = build_asset_context(resources={"duckdb": duckdb})
    dim_date(ctx)
    print("  dim_date done")
    profile_dimension(ctx)
    print("  dim_profile done")
    v_post_detail(ctx)
    print("  v_post_detail done (cascades to all downstream views)")


# ── Commands ───────────────────────────────────────────────────────────────


@app.command()
def run(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show current state only, don't run pipeline"
    ),
    reset_watermarks_flag: bool = typer.Option(
        False,
        "--reset-watermarks",
        help=f"Reset watermarks before running (default date: {DEFAULT_RESET_DATE})",
    ),
    date: str = typer.Option(
        DEFAULT_RESET_DATE,
        "--date",
        help="Date for watermark reset (ISO 8601)",
    ),
    update_stale: bool = typer.Option(
        False,
        "--update-stale",
        help="Re-enqueue analyses with stale/missing prompt_hash",
    ),
) -> None:
    """Run the full pipeline: silver → batches → serving.

    Without flags, runs incrementally — only new bronze files are processed,
    and only unenriched silver posts are batched.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if dry_run:
        print_full_state("Current state")
        raise typer.Exit()

    ops = SQLiteResource()

    if update_stale:
        _run_update_stale(ops)
        print_full_state("After stale update")
        print(
            "\nDone. Run `python scripts/enrichment_worker.py` "
            "to re-process stale analyses."
        )
        raise typer.Exit()

    duckdb = DuckDBResource(database=DB_PATH)

    if reset_watermarks_flag:
        since = parse_datetime(date)
        reset_watermarks(since)

    print_full_state("Before")

    _run_silver(duckdb)
    enqueued = _run_enqueue(duckdb, ops)
    _run_serving(duckdb)

    print_full_state("After")

    print(
        f"\nDone. {enqueued} posts enqueued. "
        "Run `python scripts/enrichment_worker.py` to process."
    )


@app.command()
def batches(
    reset: bool = typer.Option(
        False, "--reset", help="Delete all batch_jobs and batch_items"
    ),
) -> None:
    """Inspect or reset batch state."""
    logging.basicConfig(level=logging.WARNING)
    if reset:
        reset_batches()
    print_batches()


@app.command()
def watermarks(
    reset: bool = typer.Option(
        False, "--reset", help="Reset watermarks to --date (default: epoch-safe)"
    ),
    date: str = typer.Option(
        DEFAULT_RESET_DATE, "--date", help="Date for watermark reset (ISO 8601)"
    ),
) -> None:
    """Inspect or reset pipeline watermarks."""
    logging.basicConfig(level=logging.WARNING)
    if reset:
        since = parse_datetime(date)
        reset_watermarks(since)
    print_watermarks()
