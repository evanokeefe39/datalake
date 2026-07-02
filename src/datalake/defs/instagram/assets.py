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
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dagster import asset

from ..common.lake import BRONZE_LAKE, bronze_path
from ..common.resources import (
    _DEFAULT_GEMINI_MODEL,
    ApifyResource,
    DuckDBResource,
    GeminiResource,
)
from .config import GoldConfig, ScrapeConfig

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
def ig_posts_gld(
    config: GoldConfig,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
) -> pl.DataFrame:
    """Read unenriched silver posts, classify each caption via Gemini.

    Finds pending posts via watermark-based discovery on silver_ig_posts.
    Successful results land in gold_ig_analyses; failures (empty captions,
    API errors) go to dead_letter. Advances the gold_ig watermark after
    each successful post for crash recovery.

    Tier-aware: free tier limits to 10 posts/batch; Tier 1+ processes
    all pending with RPM pacing.
    """
    import json as _json
    import time

    from .config import GeminiTierConfig

    tier_cfg = GeminiTierConfig.detect()

    db = duckdb

    _ensure_state_tables(db)

    # ── 2. Find pending posts via watermark ────────────────────────────────
    limit_clause = ""
    if tier_cfg.max_posts_per_run > 0:
        limit_clause = f"LIMIT {tier_cfg.max_posts_per_run}"

    with db.get_connection() as conn:
        pending = conn.execute(f"""
            SELECT sp.post_id, sp.caption
            FROM silver_ig_posts sp
            WHERE sp.processed_on > COALESCE(
                (SELECT timestamp FROM watermarks WHERE name = 'gold_ig'),
                '1970-01-01'::TIMESTAMP
            )
            {limit_clause}
        """).fetchall()

    if not pending:
        # All posts enriched — return existing completed rows
        return _get_current_gold(db)

    # ── 3. Enrich via Gemini ──────────────────────────────────────────────
    successes = []
    prompt_hash = str(hash(_GOLD_PROMPT + _DEFAULT_GEMINI_MODEL))

    # RPM pacing: enforce minimum interval between requests
    rpm = tier_cfg.default_rpm
    min_interval = 60.0 / rpm if rpm > 0 else 0.0
    last_request_time = 0.0

    for post_id, caption in pending:
        caption_text = caption or ""
        if not caption_text.strip():
            logger.info(
                "Skipping %s — empty caption (video enrichment deferred)", post_id
            )
            continue

        attempt = 0
        error_text = None
        error_subtype = "api_error"
        result_json = None

        while attempt < 3:
            attempt += 1
            try:
                # RPM pacing: wait if we're sending requests too fast
                if min_interval > 0 and last_request_time > 0:
                    elapsed = time.time() - last_request_time
                    if elapsed < min_interval:
                        time.sleep(min_interval - elapsed)

                prompt = _GOLD_PROMPT + "\n" + caption_text
                result_json = gemini.analyze(prompt)
                # Validate it's parseable JSON
                _json.loads(result_json)
                break
            except Exception as exc:
                error_text = str(exc)
                # Classify 429 errors: rate_limit_exceeded (retry) vs
                if _is_quota_exhausted(exc, error_text):
                    error_subtype = "insufficient_quota"
                    logger.warning(
                        "Quota exhausted for %s — stopping retries: %s",
                        post_id, error_text,
                    )
                    break
                if _is_rate_limited(exc, error_text):
                    error_subtype = "rate_limit_exceeded"
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

        last_request_time = time.time()

        with db.get_connection() as conn:
            if result_json is not None:
                # Success — write to gold_ig_analyses
                successes.append(post_id)
                now_iso = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    "INSERT OR REPLACE INTO gold_ig_analyses "
                    "(post_id, schema_version, result_json, analysed_at) "
                    "VALUES (?, 3, ?, ?)",
                    [post_id, result_json, now_iso],
                )
            else:
                # Failure — write to dead_letter with error subtype
                subtype_tag = f"[{error_subtype}]"
                conn.execute(
                    "INSERT INTO dead_letter (post_id, domain, error, attempts, status) "
                    "VALUES (?, 'instagram', ?, ?, ?)",
                    [post_id, f"{subtype_tag} {error_text}", attempt, error_subtype],
                )

        # ── Advance watermark per-post (crash recovery) ───────────────────
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO watermarks (name, timestamp, config_hash) "
                "VALUES ('gold_ig', CURRENT_TIMESTAMP, ?)",
                [prompt_hash],
            )

    # ── 4. Return completed gold DataFrame ────────────────────────────────
    return _get_current_gold(db)


# ── Shared helpers ────────────────────────────────────────────────────────

_GOLD_SCHEMA = {
    "post_id": pl.Utf8,
    "schema_version": pl.Int32,
    "result_json": pl.Utf8,
    "analysed_at": pl.Utf8,
}
"""Reusable gold DataFrame schema used by both interactive and batch assets."""


def _empty_gold_df() -> pl.DataFrame:
    """Return an empty gold-analyses DataFrame with the canonical schema."""
    return pl.DataFrame({
        name: pl.Series([], dtype=dt)
        for name, dt in _GOLD_SCHEMA.items()
    })


def _get_current_gold(db: DuckDBResource) -> pl.DataFrame:
    """Fetch all rows from gold_ig_analyses as a Polars DataFrame."""
    with db.get_connection() as conn:
        reader = conn.execute(
            "SELECT post_id, schema_version, result_json, analysed_at "
            "FROM gold_ig_analyses"
        ).arrow()
        table = reader.read_all()
        if table.num_rows == 0:
            return _empty_gold_df()
        return pl.from_arrow(table)


def _ensure_state_tables(db: DuckDBResource) -> None:
    """Create shared state tables if they don't exist."""
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_ig_analyses (
                post_id         TEXT PRIMARY KEY REFERENCES silver_ig_posts(post_id),
                schema_version  INTEGER NOT NULL DEFAULT 3,
                result_json     TEXT,
                analysed_at     TIMESTAMP
            )
        """)
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


def _ensure_batch_jobs_table(db: DuckDBResource) -> None:
    """Create batch job tracking table if it doesn't exist."""
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ig_batch_jobs (
                job_name    TEXT PRIMARY KEY,
                state       TEXT NOT NULL,
                input_file  TEXT NOT NULL,
                output_file TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                poll_count  INTEGER DEFAULT 0
            )
        """)


def _build_batch_jsonl(
    pending: list[tuple[str, str | None]],
) -> str:
    """Build a JSONL string for Gemini batch API.

    Each line: ``{"custom_id": "<post_id>", "request": {"contents": …}}``
    """
    lines: list[str] = []
    for post_id, caption in pending:
        caption_text = caption or ""
        if not caption_text.strip():
            continue
        prompt = _GOLD_PROMPT + "\n" + caption_text
        request = {
            "custom_id": post_id,
            "request": {
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            },
        }
        lines.append(json.dumps(request, ensure_ascii=False))
    return "\n".join(lines)


def _write_batch_failures(
    db: DuckDBResource,
    post_id: str,
    error_text: str,
    status: str = "batch_failed",
) -> None:
    """Write a per-item batch failure to dead_letter."""
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO dead_letter (post_id, domain, error, attempts, status) "
            "VALUES (?, 'instagram', ?, 1, ?)",
            [post_id, error_text, status],
        )


# ── Batch backfill asset ───────────────────────────────────────────────────

_BATCH_POLL_TIMEOUT = 3600  # 1 hour — max wall time before giving up
_BATCH_POLL_INTERVAL_SECS = 60  # seconds between sensor ticks


@asset(
    name="ig_posts_gld_backfill",
    group_name="instagram",
    description=(
        "Batch backfill enrichment via Gemini batch API. "
        "Tier 1+ only. Starts, polls, then processes a batch job "
        "across sequential materializations."
    ),
    deps=["ig_posts_slv"],
)
def ig_posts_gld_backfill(
    duckdb: DuckDBResource,
    gemini: GeminiResource,
) -> pl.DataFrame:
    """Backfill all unenriched posts via Gemini batch API.

    Each materialization makes incremental progress:
    1. Pre-batch — builds JSONL from unenriched posts, uploads to File API,
       submits a Gemini batch job.
    2. In-flight — polls the batch job for completion.
    3. Post-batch — downloads results, joins by ``post_id``,
       writes successes to ``gold_ig_analyses``, failures to ``dead_letter``.

    Returns the current ``gold_ig_analyses`` snapshot.
    """
    from google.genai import Client as GeminiClient

    from .config import GeminiTierConfig

    tier_cfg = GeminiTierConfig.detect()
    if not tier_cfg.supports_batch:
        logger.warning("Batch backfill requires Tier 1+ (GEMINI_TIER=tier1|tier2)")
        return _get_current_gold(duckdb)

    db = duckdb
    _ensure_state_tables(db)
    _ensure_batch_jobs_table(db)

    client = GeminiClient(api_key=gemini.api_key)

    # ── Check for an active batch job ──────────────────────────────────
    with db.get_connection() as conn:
        active = conn.execute("""
            SELECT job_name, state, input_file, poll_count
            FROM ig_batch_jobs
            WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELLED','EXPIRED')
            ORDER BY created_at DESC
            LIMIT 1
        """).fetchone()

    if active:
        # ── Poll existing job ──────────────────────────────────────────
        job_name, _old_state, _input_file, poll_count = active

        if poll_count * _BATCH_POLL_INTERVAL_SECS >= _BATCH_POLL_TIMEOUT:
            # Timed out — dead-letter all items associated with this job,
            # then start fresh.
            logger.warning("Batch job %s timed out after %d polls", job_name, poll_count)
            try:
                client.batches.cancel(name=job_name)
            except Exception:
                pass
            with db.get_connection() as conn:
                conn.execute(
                    "UPDATE ig_batch_jobs SET state = 'TIMEOUT' WHERE job_name = ?",
                    [job_name],
                )
            # Fall through to start a new job below

        else:
            try:
                batch = client.batches.get(name=job_name)
                new_state = batch.state.name if batch.state else "JOB_STATE_UNSPECIFIED"
            except Exception as exc:
                logger.warning("Failed to poll batch job %s: %s", job_name, exc)
                new_state = "POLL_ERROR"

            with db.get_connection() as conn:
                conn.execute(
                    "UPDATE ig_batch_jobs SET state = ?, poll_count = poll_count + 1 "
                    "WHERE job_name = ?",
                    [new_state, job_name],
                )

            if new_state in ("JOB_STATE_SUCCEEDED", "JOB_STATE_PARTIALLY_SUCCEEDED"):
                # ── Process completed batch ────────────────────────────
                if batch.dest and batch.dest.file_name:
                    try:
                        result = client.files.download(name=batch.dest.file_name)
                        result_lines = result.decode("utf-8").strip().split("\n")
                    except Exception as exc:
                        logger.error("Failed to download batch results: %s", exc)
                        return _get_current_gold(db)

                    successes = 0
                    failures = 0
                    for line_str in result_lines:
                        if not line_str.strip():
                            continue
                        try:
                            item = json.loads(line_str)
                        except json.JSONDecodeError:
                            continue

                        custom_id = item.get("custom_id", "")
                        if not custom_id:
                            continue

                        # Check for per-item error
                        response = item.get("response", {})
                        status_code = response.get("status_code", 200)
                        if status_code != 200 or "error" in response:
                            err = str(response.get("error", f"HTTP {status_code}"))
                            _write_batch_failures(db, custom_id, err, "batch_item_error")
                            failures += 1
                            continue

                        # Extract result text from response body
                        resp_body = response.get("body", {}) if isinstance(response, dict) else {}
                        candidates = resp_body.get("candidates", [])
                        if not candidates:
                            _write_batch_failures(db, custom_id,
                                                  "No candidates in batch response",
                                                  "batch_item_empty")
                            failures += 1
                            continue

                        try:
                            result_json = candidates[0]["content"]["parts"][0]["text"]
                            # Validate JSON
                            json.loads(result_json)
                        except (KeyError, IndexError, json.JSONDecodeError) as exc:
                            _write_batch_failures(
                                db, custom_id, f"Parse error: {exc}",
                                "batch_item_parse",
                            )
                            failures += 1
                            continue

                        # Write to gold_ig_analyses
                        now_iso = datetime.now(timezone.utc).isoformat()
                        with db.get_connection() as conn:
                            conn.execute(
                                "INSERT OR REPLACE INTO gold_ig_analyses "
                                "(post_id, schema_version, result_json, analysed_at) "
                                "VALUES (?, 3, ?, ?)",
                                [custom_id, result_json, now_iso],
                            )
                        successes += 1

                    logger.info(
                        "Batch %s complete: %d succeeded, %d failed",
                        job_name, successes, failures,
                    )
                else:
                    logger.warning(
                        "Batch %s has no output file. Marking as completed.", job_name
                    )

                with db.get_connection() as conn:
                    conn.execute(
                        "UPDATE ig_batch_jobs SET state = 'PROCESSED' "
                        "WHERE job_name = ?",
                        [job_name],
                    )
                return _get_current_gold(db)

            elif new_state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"):
                # Terminal failure — get error details
                err_msg = (
                    batch.error.message if batch.error else "Unknown batch failure"
                )
                logger.error("Batch job %s failed: %s", job_name, err_msg)
                return _get_current_gold(db)

            else:
                # Still running — return current results
                return _get_current_gold(db)

    # ── Start a new batch job ──────────────────────────────────────────
    # Find unenriched posts (no watermark — reads everything)
    with db.get_connection() as conn:
        unenriched = conn.execute("""
            SELECT sp.post_id, sp.caption
            FROM silver_ig_posts sp
            LEFT JOIN gold_ig_analyses ga ON sp.post_id = ga.post_id
            WHERE ga.post_id IS NULL
            ORDER BY sp.processed_on ASC
        """).fetchall()

    if not unenriched:
        logger.info("No unenriched posts — nothing to backfill.")
        return _get_current_gold(db)

    # Build JSONL
    jsonl_content = _build_batch_jsonl(unenriched)
    if not jsonl_content.strip():
        logger.info("All posts have empty captions — nothing to backfill.")
        return _get_current_gold(db)

    # Check token count and split if needed
    total_tokens = gemini.count_tokens(
        jsonl_content,
        model=f"models/{_DEFAULT_GEMINI_MODEL}",
    )
    max_tokens = tier_cfg.max_batch_tokens
    if total_tokens > max_tokens:
        logger.info(
            "Batch JSONL (%d tokens) exceeds tier limit (%d). "
            "Splitting into sub-jobs.",
            total_tokens, max_tokens,
        )
        # Split into chunks that fit within the token limit
        chunk_lines = _split_jsonl_by_token_limit(
            unenriched, max_tokens, gemini,
        )
        # Process each chunk
        for chunk_idx, chunk in enumerate(chunk_lines):
            _submit_single_batch(db, client, chunk, gemini, chunk_idx)
        return _get_current_gold(db)

    # Single job
    _submit_single_batch(db, client, unenriched, gemini)
    return _get_current_gold(db)


def _submit_single_batch(
    db: DuckDBResource,
    client: object,  # google.genai.Client — imported lazily inside
    pending: list[tuple[str, str | None]],
    gemini: GeminiResource,
    chunk_idx: int | None = None,
) -> None:
    """Build JSONL, upload to File API, submit batch job, record in state table."""
    from google.genai.types import CreateBatchJobConfig, UploadFileConfig

    jsonl_content = _build_batch_jsonl(pending)
    if not jsonl_content.strip():
        return

    # Temp file for upload
    import tempfile

    display_name = "ig_posts_gld_backfill"
    if chunk_idx is not None:
        display_name += f"_chunk{chunk_idx}"

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False,
    ) as tmp:
        tmp.write(jsonl_content)
        tmp_path = tmp.name

    try:
        # Upload to File API
        uploaded = client.files.upload(
            file=tmp_path,
            config=UploadFileConfig(display_name=display_name),
        )
        if not uploaded.name:
            raise RuntimeError("File API upload returned no name")

        logger.info("Uploaded JSONL to File API: %s", uploaded.name)

        # Submit batch job
        job = client.batches.create(
            model=f"models/{_DEFAULT_GEMINI_MODEL}",
            src=uploaded.name,
            config=CreateBatchJobConfig(display_name=display_name),
        )

        job_name = job.name if job.name else "unknown"
        job_state = job.state.name if job.state else "JOB_STATE_UNSPECIFIED"
        logger.info("Batch job submitted: %s (state: %s)", job_name, job_state)

        # Record in state table
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ig_batch_jobs "
                "(job_name, state, input_file) VALUES (?, ?, ?)",
                [job_name, job_state, uploaded.name],
            )

    finally:
        # Clean up temp file
        Path(tmp_path).unlink(missing_ok=True)


def _split_jsonl_by_token_limit(
    pending: list[tuple[str, str | None]],
    max_tokens: int,
    gemini: GeminiResource,
) -> list[list[tuple[str, str | None]]]:
    """Split pending posts into chunks each under ``max_tokens`` tokens."""
    chunks: list[list[tuple[str, str | None]]] = []
    current_chunk: list[tuple[str, str | None]] = []
    current_tokens = 0

    for post_id, caption in pending:
        caption_text = caption or ""
        if not caption_text.strip():
            continue
        prompt = _GOLD_PROMPT + "\n" + caption_text
        token_count = gemini.count_tokens(
            prompt, model=f"models/{_DEFAULT_GEMINI_MODEL}"
        )

        # Start a new chunk if adding this post would exceed the limit
        if current_chunk and current_tokens + token_count > max_tokens:
            chunks.append(current_chunk)
            current_chunk = []
            current_tokens = 0

        current_chunk.append((post_id, caption))
        current_tokens += token_count

    if current_chunk:
        chunks.append(current_chunk)

    logger.info(
        "Split %d posts into %d chunks (max %d tokens each)",
        len(pending), len(chunks), max_tokens,
    )
    return chunks
