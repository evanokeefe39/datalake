"""Instagram assets — bronze (Phase 1), silver/gold to follow.

Bronze asset (``ig_posts_raw``) is manual-trigger via the launchpad.
It calls Apify, downloads NDJSON, converts to typed Parquet via Polars,
and writes a ``.meta`` JSON sidecar for lineage.

Apify client functions are temporarily imported from the old ig_pipeline
repo via ``sys.path``. They will be extracted into the datalake package
in a future phase.
"""

import json
import random
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
    "media_files", "media_count", "source_dataset", "processed_on",
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
            CREATE TABLE IF NOT EXISTS silver_ig_progress (
                source_dataset TEXT PRIMARY KEY,
                post_count     INTEGER NOT NULL DEFAULT 0,
                completed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        return pl.DataFrame(schema={c: pl.Utf8 for c in _SILVER_COLUMNS})

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT timestamp FROM watermarks WHERE name = 'silver_ig'"
        ).fetchone()
    watermark_ts = row[0].timestamp() if row and row[0] is not None else 0.0

    new_files = [f for f in bronze_files if _os.path.getmtime(f) > watermark_ts]

    if not new_files:
        with db.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM silver_ig_posts"
            ).fetchone()[0]
            if count == 0:
                return pl.DataFrame(
                    schema={c: pl.Utf8 for c in _SILVER_COLUMNS}
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
        return pl.DataFrame(schema={c: pl.Utf8 for c in _SILVER_COLUMNS})
    unified = pl.concat(frames, how="diagonal_relaxed")
    if unified.is_empty():
        return pl.DataFrame(schema={c: pl.Utf8 for c in _SILVER_COLUMNS})
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

        # Record progress for each processed dataset
        for f in new_files:
            dataset_id = f.stem
            src_count = len(deduped.filter(
                pl.col("source_dataset") == dataset_id
            ))
            conn.execute(
                "INSERT OR REPLACE INTO silver_ig_progress "
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



def _is_quota_exhausted(exc: Exception, error_text: str) -> bool:
    """Return True if the exception indicates daily quota (RPD) exhaustion.

    Distinguishes ``insufficient_quota`` (daily RPD spent — stop retrying,
    wait until midnight PT) from ``rate_limit_exceeded`` (RPM/TPM burst —
    retry with jitter).

    Inspects google-genai ``APIError`` attributes where available,
    falling back to substring matching on the stringized error.
    """
    # google-genai SDK raises ClientError(APIError) for 4xx responses.
    # Inspect the structured error details when available.
    if hasattr(exc, "code") and getattr(exc, "code", None) == 429:
        details = getattr(exc, "details", {}) or {}
        # The Gemini API may include quota info in error.details or
        # error.message. Look for quota-exhaustion signals.
        msg = str(getattr(exc, "message", "")).lower()
        if any(kw in msg for kw in ("quota", "insufficient", "depleted")):
            return True
        # Also check nested error.details array for quota violations
        if isinstance(details, dict):
            inner = details.get("error", {})
            inner_msg = str(inner.get("message", "")).lower()
            if any(kw in inner_msg for kw in ("quota", "insufficient", "depleted")):
                return True
    # Fallback: substring match on stringized error.
    # Only quota-specific terms — "RESOURCE_EXHAUSTED" is the generic
    # 429 status string and does NOT indicate which subtype.
    lower = error_text.lower()
    quota_keywords = (
        "insufficient_quota", "quota exceeded", "quota exhausted",
    )
    return any(kw in lower for kw in quota_keywords)


def _is_rate_limited(exc: Exception, error_text: str) -> bool:
    """Return True if the exception is a rate-limit burst (RPM/TPM).

    These are transient — retry with jittered backoff should succeed.
    """
    if hasattr(exc, "code") and getattr(exc, "code", None) == 429:
        return not _is_quota_exhausted(exc, error_text)
    return False


@asset(
    name="ig_posts_gld",
    group_name="instagram",
    description="Enrich silver posts via Gemini classification.",
    deps=["ig_posts_slv"],
)
def ig_posts_gld(duckdb: DuckDBResource, gemini: GeminiResource) -> pl.DataFrame:
    """Read unenriched silver posts, classify each caption via Gemini.

    Finds pending posts via watermark-based discovery on silver_ig_posts.
    Successful results land in gold_ig_analyses; failures (empty captions,
    API errors) go to dead_letter. Advances the gold_ig watermark after
    each batch.
    """
    import json as _json
    import time


    # ── 1. Ensure state tables exist ──────────────────────────────────────
    db = duckdb
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_ig_analyses (
                post_id         TEXT PRIMARY KEY REFERENCES silver_ig_posts(post_id),
                schema_version  INTEGER NOT NULL DEFAULT 3,
                result_json     TEXT,
                analysed_at     TIMESTAMP
            )
        """)
        # Ensure watermarks and dead_letter exist (shared tables)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watermarks (
                name        TEXT PRIMARY KEY,
                timestamp   TIMESTAMP NOT NULL,
                config_hash TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dead_letter (
                post_id     TEXT NOT NULL,
                domain      TEXT NOT NULL DEFAULT 'instagram',
                error       TEXT,
                attempts    INTEGER NOT NULL DEFAULT 0,
                failed_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                status      TEXT NOT NULL DEFAULT 'pending',
                PRIMARY KEY (post_id, domain)
            )
        """)

    # ── 2. Find pending posts via watermark ────────────────────────────────
    with db.get_connection() as conn:
        pending = conn.execute("""
            SELECT sp.post_id, sp.caption
            FROM silver_ig_posts sp
            WHERE sp.processed_on > COALESCE(
                (SELECT timestamp FROM watermarks WHERE name = 'gold_ig'),
                '1970-01-01'::TIMESTAMP
            )
            LIMIT 10
        """).fetchall()

    if not pending:
        # All posts enriched — return existing completed rows
        with db.get_connection() as conn:
            reader = conn.execute(
                "SELECT post_id, schema_version, result_json, analysed_at "
                "FROM gold_ig_analyses"
            ).arrow()
            table = reader.read_all()
            if table.num_rows == 0:
                return pl.DataFrame({
                    "post_id": pl.Series([], dtype=pl.Utf8),
                    "schema_version": pl.Series([], dtype=pl.Int32),
                    "result_json": pl.Series([], dtype=pl.Utf8),
                    "analysed_at": pl.Series([], dtype=pl.Utf8),
                })
            return pl.from_arrow(table)

    # ── 3. Enrich via Gemini ──────────────────────────────────────────────
    successes = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for post_id, caption in pending:
        caption_text = caption or ""
        if not caption_text.strip():
            logger.info("Skipping %s — empty caption", post_id)
            with db.get_connection() as conn:
                conn.execute(
                    "INSERT INTO dead_letter (post_id, domain, error, attempts, status) "
                    "VALUES (?, 'instagram', 'Empty caption', 0, 'skipped')",
                    [post_id],
                )
            continue

        attempt = 0
        error_text = None
        result_json = None
        quota_exhausted = False

        while attempt < 3:
            attempt += 1
            try:
                prompt = _GOLD_PROMPT + "\n" + caption_text
                result_json = gemini.analyze(prompt)
                # Validate it's parseable JSON
                _json.loads(result_json)
                break
            except Exception as exc:
                error_text = str(exc)
                # Classify 429 errors: rate_limit_exceeded (retry) vs
                # insufficient_quota (stop — daily RPD exhausted)
                if _is_quota_exhausted(exc, error_text):
                    quota_exhausted = True
                    logger.warning(
                        "Quota exhausted for %s — stopping retries: %s",
                        post_id, error_text,
                    )
                    break
                if _is_rate_limited(exc, error_text):
                    logger.warning(
                        "Rate-limited for %s (attempt %d/3): %s",
                        post_id, attempt, error_text,
                    )
                else:
                    logger.warning(
                        "Gemini call failed for %s (attempt %d/3): %s",
                        post_id, attempt, error_text,
                    )
                if attempt < 3:
                    # Exponential backoff with jitter: 2^N + random(0,1) seconds.
                    # Jitter prevents harmonic lockstep with the rate limiter
                    # when multiple requests hit the RPM/TPM wall simultaneously.
                    delay = (2 ** attempt) + random.uniform(0, 1)
                    logger.debug("Backing off %.1fs before retry", delay)
                    time.sleep(delay)

        with db.get_connection() as conn:
            if result_json is not None:
                # Success — write to gold_ig_analyses
                successes.append(post_id)
                conn.execute(
                    "INSERT OR REPLACE INTO gold_ig_analyses "
                    "(post_id, schema_version, result_json, analysed_at) "
                    "VALUES (?, 3, ?, ?)",
                    [post_id, result_json, now_iso],
                )
            else:
                # Failure — write to dead_letter
                conn.execute(
                    "INSERT INTO dead_letter (post_id, domain, error, attempts, status) "
                    "VALUES (?, 'instagram', ?, ?, 'pending')",
                    [post_id, error_text, attempt],
                )

    # ── 4. Advance watermark ──────────────────────────────────────────────
    # Compute prompt hash for deferred auto-reset (Phase B1)
    prompt_hash = str(hash(_GOLD_PROMPT + "gemini-2.0-flash-lite"))
    with db.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO watermarks (name, timestamp, config_hash) "
            "VALUES ('gold_ig', ?, ?)",
            [now_iso, prompt_hash],
        )

    # ── 5. Return completed gold DataFrame ────────────────────────────────
    with db.get_connection() as conn:
        reader = conn.execute(
            "SELECT post_id, schema_version, result_json, analysed_at "
            "FROM gold_ig_analyses"
        ).arrow()
        table = reader.read_all()
        if table.num_rows == 0:
            return pl.DataFrame({
                "post_id": pl.Series([], dtype=pl.Utf8),
                "schema_version": pl.Series([], dtype=pl.Int32),
                "result_json": pl.Series([], dtype=pl.Utf8),
                "analysed_at": pl.Series([], dtype=pl.Utf8),
            })
        return pl.from_arrow(table)
