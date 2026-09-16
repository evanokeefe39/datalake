"""`silver_ig_comments` — comment capture from the details scrape."""

import logging

import polars as pl
from dagster import asset

from orchestration.defs.ig_core.slv.posts import ensure_state_tables as _ensure_state_tables
from orchestration.defs.platform.resources import (
    DuckDBResource,
)

logger = logging.getLogger(__name__)

@asset(
    name="silver_ig_comments",
    group_name="instagram",
    description="Comment scrapes from bronze → silver (STUB — not yet implemented).",
    deps=["bronze_ig_posts"],
)
def silver_ig_comments(duckdb: DuckDBResource) -> pl.DataFrame:
    """Stub for comment-type bronze processing.

    No comment-type bronze datasets exist yet. Full implementation deferred
    until real comment data arrives (modeling against non-existent data is
    a confirmed anti-pattern from Phase 2 false start). Ensures the table
    exists so the schema contract holds, but reads no bronze files.
    """
    _ensure_state_tables(duckdb)
    logger.warning("silver_ig_comments: not yet implemented — returning empty")
    return pl.DataFrame(schema={"comment_id": pl.Utf8})
