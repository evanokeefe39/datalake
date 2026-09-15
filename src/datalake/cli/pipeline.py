"""Pipeline commands: run, watermarks."""

from __future__ import annotations

import logging

import typer
from dagster import build_asset_context

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.instagram.assets import ig_posts_slv
from datalake.defs.serving.assets import dim_date, profile_dimension, v_post_detail

from ._state import (
    DEFAULT_RESET_DATE,
    parse_datetime,
    print_full_state,
    print_watermarks,
    reset_watermarks,
)

app = typer.Typer(
    name="pipeline",
    help="Run medallion pipeline: silver → serving.",
    no_args_is_help=True,
)

DB_PATH = "data/state.duckdb"

# ── Pipeline steps ─────────────────────────────────────────────────────────

def _run_silver(duckdb: DuckDBResource) -> int:
    print("\n--- Silver (ig_posts_slv) ---")
    ctx = build_asset_context(resources={"duckdb": duckdb})
    result = ig_posts_slv(ctx)
    n = len(result)
    print(f"  Output: {n} rows, {result['post_id'].n_unique()} unique post_ids")
    return n



def _run_serving(duckdb: DuckDBResource, ops: SQLiteResource) -> None:
    print("\n--- Serving (dim_date + dim_profile + v_post_detail) ---")
    ctx = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
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
) -> None:
    """Run the full pipeline: silver → serving.

    Without flags, runs incrementally — only new bronze files are processed.
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
    duckdb = DuckDBResource(database=DB_PATH)

    if reset_watermarks_flag:
        since = parse_datetime(date)
        reset_watermarks(since)

    print_full_state("Before")

    _run_silver(duckdb)
    _run_serving(duckdb, ops)

    print_full_state("After")


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
