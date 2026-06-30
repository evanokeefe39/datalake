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
from ..common.resources import ApifyResource, DuckDBResource, GeminiResource
from .config import ScrapeConfig

# ── Apify client (from old ig_pipeline) ───────────────────────────────────
_OLD_IG_SRC = Path("C:/Users/evano/repos/ig-pipeline/src")
if str(_OLD_IG_SRC) not in sys.path:
    sys.path.insert(0, str(_OLD_IG_SRC))

from ig_pipeline.apify import poll_run, stream_dataset, trigger_run  # noqa: E402

logger = logging.getLogger(__name__)

# ── Metadata sidecar ──────────────────────────────────────────────────────

def _write_meta(parquet_path: Path, run_id: str, actor: str, item_count: int) -> None:
    """Write a ``.meta`` JSON sidecar alongside the Parquet file."""
    meta = {
        "run_id": run_id,
        "actor": actor,
        "item_count": item_count,
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
    _write_meta(dest, run.run_id, run.actor, item_count)

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
    "timestamp": "timestamp",
}

# List-type columns that must be serialized to JSON strings
# before Arrow → DuckDB insertion (DuckDB TEXT cannot store Polars List).
_LIST_COLUMNS: set[str] = {"hashtags"}

_SILVER_COLUMNS = [
    "post_id", "shortcode", "url", "caption", "owner_id", "owner_username",
    "likes_count", "comments_count", "video_play_count", "video_view_count",
    "timestamp", "hashtags", "meta_data", "has_engagement_bait",
    "media_files", "media_count", "source_dataset", "silvered_at",
]


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
            CREATE TABLE IF NOT EXISTS silver_posts (
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
                silvered_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_progress (
                source_dataset TEXT PRIMARY KEY,
                post_count     INTEGER NOT NULL DEFAULT 0,
                completed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    # ── 2. Find unprocessed bronze files ──────────────────────────────────
    bronze_files = sorted(BRONZE_LAKE.glob("*.parquet"))
    if not bronze_files:
        # No bronze data at all — return empty
        return pl.DataFrame(schema={c: pl.Utf8 for c in _SILVER_COLUMNS})

    with db.get_connection() as conn:
        tracked = {
            row[0]
            for row in conn.execute(
                "SELECT source_dataset FROM silver_progress"
            ).fetchall()
        }

    new_files = [
        f for f in bronze_files
        if f.stem not in tracked
    ]

    if not new_files:
        # No new files — return existing silver so the I/O manager
        # doesn't overwrite with an empty DataFrame.
        with db.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM silver_posts"
            ).fetchone()[0]
            if count == 0:
                return pl.DataFrame(
                    schema={c: pl.Utf8 for c in _SILVER_COLUMNS}
                )
            reader = conn.execute(
                "SELECT * FROM silver_posts ORDER BY timestamp DESC"
            ).arrow()
        return pl.from_arrow(reader.read_all())

    # ── 3. Read new bronze → map to silver schema ─────────────────────────
    frames: list[pl.DataFrame] = []
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
            ("source_dataset", dataset_id),
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(default).alias(col))

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
            [c for c in _SILVER_COLUMNS if c in df.columns]
        )

        # Drop rows without a valid post_id (failed Apify requests)
        df = df.filter(pl.col("post_id").is_not_null())


        frames.append(df)
    # ── 4. Load existing silver from DuckDB ───────────────────────────────
    existing_count = 0
    with db.get_connection() as conn:
        existing_count = conn.execute(
            "SELECT COUNT(*) FROM silver_posts"
        ).fetchone()[0]

    if existing_count > 0:
        with db.get_connection() as conn:
            existing_reader = conn.execute(
                "SELECT * FROM silver_posts"
            ).arrow()
        existing_df = pl.from_arrow(existing_reader.read_all())
        # Drop silvered_at from existing so the unified DF gets a fresh
        # timestamp from the dedup result.
        if "silvered_at" in existing_df.columns:
            existing_df = existing_df.drop("silvered_at")
        frames.insert(0, existing_df)

    # ── 5. Union + dedup via DuckDB ───────────────────────────────────────
    if not frames:
        # All bronze files were empty or had only null-id rows
        return pl.DataFrame(schema={c: pl.Utf8 for c in _SILVER_COLUMNS})
    unified = pl.concat(frames, how="diagonal_relaxed")
    unified_arrow = unified.to_arrow()

    with db.get_connection() as conn:
        conn.register("unified", unified_arrow)

        deduped_arrow = conn.execute("""
            SELECT DISTINCT ON(post_id) *
            FROM unified
            ORDER BY post_id, timestamp DESC NULLS LAST, source_dataset DESC
        """).arrow()

    deduped = pl.from_arrow(deduped_arrow)

    # Add fresh silvered_at timestamp
    now_iso = datetime.now(timezone.utc).isoformat()
    deduped = deduped.with_columns(pl.lit(now_iso).alias("silvered_at"))

    # ── 6. Upsert into state tables ───────────────────────────────────────
    with db.get_connection() as conn:
        conn.register("to_upsert", deduped.to_arrow())
        conn.execute(
            "INSERT OR REPLACE INTO silver_posts SELECT * FROM to_upsert"
        )

        # Record progress for each processed dataset
        for f in new_files:
            dataset_id = f.stem
            src_count = len(deduped.filter(
                pl.col("source_dataset") == dataset_id
            ))
            conn.execute(
                "INSERT OR REPLACE INTO silver_progress "
                "(source_dataset, post_count, completed_at) "
                "VALUES (?, ?, ?)",
                [dataset_id, src_count, now_iso],
            )

    return deduped


# ── Gold enrichment prompt ────────────────────────────────────────────────

_GOLD_PROMPT = """\
Analyze this Instagram post caption and classify it into the following taxonomy.
Return ONLY valid JSON with no markdown fencing, no explanation.

Taxonomy:
- is_educational (bool): does the post teach something?
- is_actionable (bool): can the viewer do something after watching?
- admirality (str): A1 (authoritative) through C2 (entertainment)
- domain (str): e.g. "Business", "Marketing", "Design", "Web Dev", "AI"
- subdomain (str): within domain
- topic (str): specific topic
- subtopic (str, optional): narrower still
- content_type (str): "tutorial", "listicle", "opinion", "case_study"
  or "storytelling", "thought_leadership", "news", "entertainment"
- style (str): e.g. "casual", "professional", "educational"
- format (str): e.g. "talking head", "screen recording", "carousel"
If is_educational:
- educational_json.summary (str): TL;DR of what's taught
- educational_json.workflow (list of {step, tool, detail}): actionable steps
- educational_json.concepts (list of {term, explanation}): key concepts introduced
- educational_json.principles (list of str): lessons/principles
- educational_json.techniques (list of str): specific techniques

If is_actionable:
- actionable_json.summary (str): what the viewer can do
- actionable_json.resources (list of {name, url, type, purpose}): tools/links mentioned
- actionable_json.tools (list of str): tools mentioned
- actionable_json.guides (list of str): step-by-step guides
- actionable_json.downloads (list of str): any downloads offered

Caption:"""  # no trailing whitespace needed, prompt below feeds the caption


@asset(
    name="ig_posts_gld",
    group_name="instagram",
    description="Enrich silver posts via Gemini classification.",
    deps=["ig_posts_slv"],
)
def ig_posts_gld(duckdb: DuckDBResource, gemini: GeminiResource) -> pl.DataFrame:
    """Read unenriched silver posts, classify each caption via Gemini.

    Records results in ``gold_analyses`` state table. Failed API calls
    are recorded as errors (status='failed') and processing continues.
    """
    import json as _json
    import time

    from google.genai import Client as GeminiClient
    from google.genai.types import GenerateContentConfig

    # ── 1. Ensure state table exists ──────────────────────────────────────
    db = duckdb
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id         TEXT PRIMARY KEY REFERENCES silver_posts(post_id),
                schema_version  INTEGER NOT NULL DEFAULT 3,
                status          TEXT NOT NULL DEFAULT 'pending',
                result_json     TEXT,
                error           TEXT,
                attempts        INTEGER NOT NULL DEFAULT 0,
                analysed_at     TIMESTAMP
            )
        """)

    # ── 2. Find unenriched posts ──────────────────────────────────────────
    with db.get_connection() as conn:
        pending = conn.execute("""
            SELECT sp.post_id, sp.caption
            FROM silver_posts sp
            LEFT JOIN gold_analyses ga ON sp.post_id = ga.post_id
            WHERE ga.post_id IS NULL OR ga.status != 'completed'
            LIMIT 10
        """).fetchall()

    if not pending:
        # All posts enriched — return existing gold
        with db.get_connection() as conn:
            reader = conn.execute(
                "SELECT * FROM gold_analyses WHERE status = 'completed'"
            ).arrow()
            table = reader.read_all()
            if table.num_rows == 0:
                return pl.DataFrame({
                    "post_id": [],
                    "schema_version": pl.Series([], dtype=pl.Int32),
                    "status": pl.Series([], dtype=pl.Utf8),
                    "result_json": pl.Series([], dtype=pl.Utf8),
                    "error": pl.Series([], dtype=pl.Utf8),
                    "attempts": pl.Series([], dtype=pl.Int32),
                    "analysed_at": pl.Series([], dtype=pl.Utf8),
                })
            return pl.from_arrow(table)

    # ── 3. Enrich via Gemini ──────────────────────────────────────────────
    client = GeminiClient(api_key=gemini.api_key)
    model = "gemini-2.0-flash-lite"
    results = []

    for post_id, caption in pending:
        caption_text = caption or ""
        if not caption_text.strip():
            logger.info("Skipping %s — empty caption", post_id)
            results.append({
                "post_id": post_id,
                "status": "skipped",
                "result_json": None,
                "error": "Empty caption",
                "attempts": 0,
            })
            continue

        attempt = 0
        error_text = None
        result_json = None
        status = "completed"

        while attempt < 3:
            attempt += 1
            try:
                prompt = _GOLD_PROMPT + "\n" + caption_text
                response = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config=GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.2,
                        max_output_tokens=2048,
                    ),
                )
                result_json = response.text
                # Validate it's parseable JSON
                _json.loads(result_json)
                break
            except Exception as exc:
                error_text = str(exc)
                logger.warning(
                    "Gemini call failed for %s (attempt %d/3): %s",
                    post_id, attempt, error_text,
                )
                if attempt < 3:
                    time.sleep(2 ** attempt)  # exponential backoff

        if error_text and result_json is None:
            status = "failed"

        results.append({
            "post_id": post_id,
            "status": status,
            "result_json": result_json,
            "error": error_text,
            "attempts": attempt,
        })

    # ── 4. Upsert into state table ────────────────────────────────────────
    now_iso = datetime.now(timezone.utc).isoformat()
    with db.get_connection() as conn:
        for r in results:
            conn.execute("""
                INSERT OR REPLACE INTO gold_analyses
                    (post_id, schema_version, status, result_json, error, attempts, analysed_at)
                VALUES (?, 3, ?, ?, ?, ?, ?)
            """, [
                r["post_id"],
                r["status"],
                r["result_json"],
                r["error"],
                r["attempts"],
                now_iso if r["status"] != "skipped" else None,
            ])

    # ── 5. Return gold DataFrame for I/O manager ──────────────────────────
    with db.get_connection() as conn:
        reader = conn.execute(
            "SELECT * FROM gold_analyses WHERE status = 'completed'"
        ).arrow()
        table = reader.read_all()
        if table.num_rows == 0:
            return pl.DataFrame({
                "post_id": pl.Series([], dtype=pl.Utf8),
                "schema_version": pl.Series([], dtype=pl.Int32),
                "status": pl.Series([], dtype=pl.Utf8),
                "result_json": pl.Series([], dtype=pl.Utf8),
                "error": pl.Series([], dtype=pl.Utf8),
                "attempts": pl.Series([], dtype=pl.Int32),
                "analysed_at": pl.Series([], dtype=pl.Utf8),
            })
        return pl.from_arrow(table)
