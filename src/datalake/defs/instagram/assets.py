"""Instagram assets — bronze (Phase 1), silver/gold to follow.

Bronze asset (``ig_posts_raw``) is manual-trigger via the launchpad.
It calls Apify, downloads NDJSON, converts to typed Parquet via Polars,
and writes a ``.meta`` JSON sidecar for lineage.

Apify client functions are temporarily imported from the old ig_pipeline
repo via ``sys.path``. They will be extracted into the datalake package
in a future phase.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dagster import asset

from ..common.lake import BRONZE_LAKE, bronze_path
from ..common.resources import (
    ApifyResource,
    DuckDBResource,
    SQLiteResource,
)
from ..common.schemas import SILVER_COLUMNS
from .config import GoldConfig, ScrapeConfig

# ── Apify client (from old ig_pipeline) ───────────────────────────────────
_OLD_IG_SRC = Path("C:/Users/evano/repos/ig-pipeline/src")
if str(_OLD_IG_SRC) not in sys.path:
    sys.path.insert(0, str(_OLD_IG_SRC))

from ig_pipeline.apify import poll_run, stream_dataset, trigger_run  # noqa: E402

logger = logging.getLogger(__name__)

# ── Metadata sidecar ──────────────────────────────────────────────────────

def _write_meta(
    parquet_path: Path,
    run_id: str,
    dataset_id: str,
    actor: str,
    item_count: int,
    urls: list[str],
    results_limit: int,
    results_type: str,
) -> None:
    """Write a ``.meta`` JSON sidecar alongside the Parquet file."""
    meta = {
        "run_id": run_id,
        "dataset_id": dataset_id,
        "actor": actor,
        "item_count": item_count,
        "input": {
            "urls": urls,
            "results_limit": results_limit,
            "results_type": results_type,
        },
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    meta_path = parquet_path.with_suffix(".parquet.meta")
    meta_path.write_text(json.dumps(meta, indent=2))

# ── Asset ─────────────────────────────────────────────────────────────────

@asset(
    name="ig_posts_raw",
    group_name="instagram",
    description="Apify Instagram scrape → typed Parquet in bronze lake.",
)
def ig_posts_raw(config: ScrapeConfig, apify: ApifyResource) -> pl.DataFrame:
    """Scrape Instagram profiles via Apify, store as typed Parquet.

    Idempotent: if the Parquet file already exists for the dataset_id,
    re-reads and returns it without re-downloading.
    """
    if not apify.token:
        raise RuntimeError("Apify API token is empty — set APIFY_API_TOKEN")

    # 1. Trigger + poll Apify
    run = trigger_run(
        "apify~instagram-scraper",
        config.urls,
        token=apify.token,
        results_limit=config.results_limit,
        results_type=config.results_type,
    )
    dataset_id = poll_run(run.run_id, token=apify.token)

    # 2. Idempotency check
    dest = bronze_path(dataset_id)
    if dest.exists():
        return pl.read_parquet(dest)

    # 3. Download NDJSON, load with Polars, write Parquet
    ndjson_path = BRONZE_LAKE / f"{dataset_id}.jsonl"
    item_count = stream_dataset(dataset_id, dest=ndjson_path, token=apify.token)

    if item_count == 0:
        # Empty dataset — write empty Parquet with no rows
        df = pl.DataFrame()
        df.write_parquet(dest)
    else:
        df = pl.read_ndjson(ndjson_path)
        df.write_parquet(dest)

    # 4. Cleanup + metadata
    if ndjson_path.exists():
        ndjson_path.unlink()
    _write_meta(dest, run.run_id, dataset_id, run.actor, item_count,
                config.urls, config.results_limit, config.results_type)

    return df


# ── Silver asset ──────────────────────────────────────────────────────────

# Column mapping from Apify bronze schema → silver schema.
# Bronze Parquet comes from the raw Apify NDJSON dump (camelCase).
# Silver normalizes to snake_case with derived columns.
_BRONZE_TO_SILVER: dict[str, str] = {
    "id": "post_id",
    "shortCode": "shortcode",
    "caption": "caption",
    "ownerUsername": "owner_username",
    "likesCount": "likes_count",
    "commentsCount": "comments_count",
    "videoViewCount": "video_view_count",
    "videoPlayCount": "video_play_count",
    "ownerId": "owner_id",
    "ownerFullName": "owner_full_name",
    "url": "url",
    "hashtags": "hashtags",
    "mentions": "mentions",
    "taggedUsers": "tagged_users",
    "latestComments": "latest_comments",
    "username": "username",
    "timestamp": "timestamp",
}

# List-type columns that must be serialized to JSON strings
# before Arrow → DuckDB insertion (DuckDB TEXT cannot store Polars List).
_LIST_COLUMNS: set[str] = {"hashtags"}



@asset(
    name="ig_posts_slv",
    group_name="instagram",
    description="Dedup bronze posts → silver Parquet + DuckDB state.",
    deps=["ig_posts_raw"],
)
def ig_posts_slv(duckdb: DuckDBResource) -> pl.DataFrame:
    """Read unprocessed bronze files, dedup via DuckDB DISTINCT ON, persist.

    Idempotent: re-running with no new bronze files is a no-op (returns
    the existing silver DataFrame).
    """

    # ── 1. Ensure state tables exist ──────────────────────────────────────
    db = duckdb
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_ig_posts (
                post_id        TEXT PRIMARY KEY,
                shortcode      TEXT,
                url            TEXT,
                caption        TEXT,
                owner_id       TEXT,
                owner_username  TEXT,
                likes_count    INTEGER,
                comments_count INTEGER,
                video_play_count  INTEGER,
                video_view_count  INTEGER,
                timestamp      TIMESTAMP,
                hashtags       TEXT NOT NULL DEFAULT '[]',
                meta_data      TEXT,
                has_engagement_bait BOOLEAN NOT NULL DEFAULT FALSE,
                media_files    TEXT NOT NULL DEFAULT '[]',
                media_count    INTEGER NOT NULL DEFAULT 0,
                source_dataset TEXT NOT NULL,
                processed_on   TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watermarks (
                name        TEXT PRIMARY KEY,
                timestamp   TIMESTAMP NOT NULL,
                config_hash TEXT
            )
        """)
    # ── 2. Find new bronze files (mtime > last watermark) ──────────────────
    import os as _os

    bronze_files = sorted(BRONZE_LAKE.glob("*.parquet"))
    if not bronze_files:
        return pl.DataFrame(schema={c: pl.Utf8 for c in SILVER_COLUMNS})

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT timestamp FROM watermarks WHERE name = 'silver_ig'"
        ).fetchone()
    if row and row[0] is not None:
        dt = row[0]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        watermark_ts = dt.timestamp()
    else:
        watermark_ts = 0.0

    new_files = [f for f in bronze_files if _os.path.getmtime(f) > watermark_ts]

    if not new_files:
        with db.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM silver_ig_posts"
            ).fetchone()[0]
            if count == 0:
                return pl.DataFrame(
                    schema={c: pl.Utf8 for c in SILVER_COLUMNS}
                )
            reader = conn.execute(
                "SELECT * FROM silver_ig_posts ORDER BY timestamp DESC"
            ).arrow()
        return pl.from_arrow(reader.read_all())
    frames = []
    for f in new_files:
        try:
            df = pl.read_parquet(f)
        except Exception as exc:
            logger.warning("Skipping %s — unreadable: %s", f.name, exc)
            continue

        if len(df) == 0:
            logger.info("Skipping %s — 0 rows", f.name)
            continue

        # Rename known columns, but skip if the target name already exists
        # (some bronze files already have the silver column name).
        to_rename = {
            old: new
            for old, new in _BRONZE_TO_SILVER.items()
            if old in df.columns and new not in df.columns
        }
        df = df.rename(to_rename)

        # Derive missing columns
        dataset_id = f.stem
        for col, default in [
            ("url", None),
            ("owner_id", None),
            ("video_play_count", 0),
            ("video_view_count", 0),
            ("hashtags", "[]"),
            ("meta_data", None),
            ("has_engagement_bait", False),
            ("media_files", "[]"),
            ("media_count", 0),
            ("processed_on", None),
            ("source_dataset", dataset_id),
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(default).alias(col))

        # Coalesce owner_username from username when ownerUsername is null or missing.
        # Profile-scraped rows have the author's handle in username, not ownerUsername
        # (and some profile-scraped files lack ownerUsername entirely).
        if "username" in df.columns:
            if "owner_username" in df.columns:
                df = df.with_columns(
                    pl.when(pl.col("owner_username").is_null())
                    .then(pl.col("username"))
                    .otherwise(pl.col("owner_username"))
                    .alias("owner_username")
                )
            else:
                df = df.with_columns(
                    pl.col("username").alias("owner_username")
                )

        # Serialize list-type columns to JSON strings for DuckDB TEXT columns.
        # map_elements on a List column passes each inner list as a Series.
        for col in _LIST_COLUMNS:
            if col in df.columns and "list" in str(df[col].dtype).lower():
                df = df.with_columns(
                    pl.col(col).map_elements(
                        lambda s: json.dumps(s.to_list() if s is not None else []),
                        return_dtype=pl.Utf8,
                    )
                )

        # Derive URL from shortcode if missing
        if "url" in df.columns and df["url"].null_count() > 0:
            df = df.with_columns(
                pl.when(pl.col("url").is_null())
                .then(pl.lit("https://instagram.com/p/") + pl.col("shortcode") + pl.lit("/"))
                .otherwise(pl.col("url"))
                .alias("url")
            )


        # Cast timestamp column to ensure it's parseable.
        # Strip trailing Z (UTC) then parse — Polars 1.42 rejects timezone
        # suffixes on str.to_datetime() / str.strptime() without a format.
        if "timestamp" in df.columns and df["timestamp"].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col("timestamp")
                .str.replace(r"Z$", "")
                .str.strptime(pl.Datetime, strict=False)
                .alias("timestamp"),
            )

        # Keep only silver columns (drop any Apify extras)
        df = df.select(
            [c for c in SILVER_COLUMNS if c in df.columns]
        )

        # Drop rows without a valid post_id (failed Apify requests)
        df = df.filter(pl.col("post_id").is_not_null())


        frames.append(df)
    # ── 4. Load existing silver from DuckDB ───────────────────────────────
    existing_count = 0
    with db.get_connection() as conn:
        existing_count = conn.execute(
            "SELECT COUNT(*) FROM silver_ig_posts"
        ).fetchone()[0]

    if existing_count > 0:
        with db.get_connection() as conn:
            existing_reader = conn.execute(
                "SELECT * FROM silver_ig_posts"
            ).arrow()
        existing_df = pl.from_arrow(existing_reader.read_all())



        # Keep existing processed_on — new posts get NULL, stamped below
        frames.insert(0, existing_df)

    # ── 5. Union + dedup via DuckDB ───────────────────────────────────────
    if not frames:
        # All bronze files were empty or had only null-id rows
        return pl.DataFrame(schema={c: pl.Utf8 for c in SILVER_COLUMNS})
    unified = pl.concat(frames, how="diagonal_relaxed")
    if unified.is_empty():
        return pl.DataFrame(schema={c: pl.Utf8 for c in SILVER_COLUMNS})
    unified_arrow = unified.to_arrow()
    with db.get_connection() as conn:
        conn.register("unified", unified_arrow)

        deduped_arrow = conn.execute("""
            SELECT DISTINCT ON(post_id) *
            FROM unified
            ORDER BY post_id, timestamp DESC NULLS LAST, source_dataset DESC
        """).arrow()

    deduped = pl.from_arrow(deduped_arrow)

    # Only stamp processed_on on genuinely new posts (existing keep their value)
    now_iso = datetime.now(timezone.utc).isoformat()
    deduped = deduped.with_columns(
        pl.when(pl.col("processed_on").is_null())
        .then(pl.lit(now_iso))
        .otherwise(pl.col("processed_on"))
        .alias("processed_on")
    )

    # ── 6. Upsert into state tables ───────────────────────────────────────
    with db.get_connection() as conn:
        conn.register("to_upsert", deduped.to_arrow())
        conn.execute(
            "INSERT OR REPLACE INTO silver_ig_posts SELECT * FROM to_upsert"
        )


    return deduped




@asset(
    name="ig_posts_ext_api_batches",
    group_name="instagram",
    description="Enqueue unenriched silver posts for async Gemini enrichment.",
    deps=["ig_posts_slv"],
)
def ig_posts_ext_api_batches(
    config: GoldConfig,
    duckdb: DuckDBResource,
    ops: SQLiteResource,
) -> pl.DataFrame:
    """Read unenriched silver posts, create a batch, advance watermark.

    Does NOT call Gemini — the enrichment worker handles that async.
    Watermark advances once after all posts are batched.
    """
    from datalake.defs.enrichment.batch import create_batch

    db = duckdb
    _ensure_state_tables(db)

    # Find pending posts via watermark, excluding already-enriched posts
    with db.get_connection() as conn:
        pending = conn.execute("""
            SELECT sp.post_id, sp.caption, sp.processed_on
            FROM silver_ig_posts sp
            WHERE sp.processed_on > COALESCE(
                (SELECT timestamp FROM watermarks WHERE name = 'gold_ig'),
                '1970-01-01'::TIMESTAMP
            )
            AND NOT EXISTS (
                SELECT 1 FROM gold_analyses
                WHERE post_id = sp.post_id AND domain = 'instagram'
            )
        """).fetchall()

    if not pending:
        # All posts enriched — return empty summary
        return pl.DataFrame({"enqueued": pl.Series([], dtype=pl.Int32)})

    # Collect post_ids with non-empty captions
    post_ids = []
    max_processed = None
    for post_id, caption, processed_on in pending:
        if not (caption or "").strip():
            continue  # Empty caption — skip; worker will skip these
        post_ids.append(post_id)
        if max_processed is None or processed_on > max_processed:
            max_processed = processed_on

    # Create single batch for all posts
    if post_ids:
        create_batch(ops, post_ids)

    # Advance watermark once after all batched
    if max_processed is not None:
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO watermarks (name, timestamp) "
                "VALUES ('gold_ig', ?)",
                [max_processed],
            )

    return pl.DataFrame({"enqueued": pl.Series([len(post_ids)], dtype=pl.Int32)})


# ── Shared helpers ────────────────────────────────────────────────────────


def _ensure_state_tables(db: DuckDBResource) -> None:
    """Create shared state tables if they don't exist."""
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id         TEXT NOT NULL,
                domain          TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash     TEXT,
                result_json     TEXT,
                analysed_at     TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watermarks (
                name        TEXT PRIMARY KEY,
                timestamp   TIMESTAMP NOT NULL,
                config_hash TEXT
            )
        """)


