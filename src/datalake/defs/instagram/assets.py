"""Instagram assets — bronze (Phase 1), silver/gold to follow.

Bronze asset (``ig_posts_raw``) is manual-trigger via the launchpad.
It calls Apify, downloads NDJSON, converts to typed Parquet via Polars,
and writes a ``.meta`` JSON sidecar for lineage.

Apify client functions live in ``common/apify.py`` (extracted from the
legacy ig_pipeline repo to remove the local-checkout dependency).
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dagster import asset

from ..common.apify import poll_run, stream_dataset, trigger_run
from ..common.lake import BRONZE_LAKE, bronze_path
from ..common.resources import (
    ApifyResource,
    DuckDBResource,
    SQLiteResource,
)
from ..common.schemas import SILVER_COLUMNS, duckdb_ddl
from ..enrichment.media_cache import cache_media_bytes, seed_media_from_file
from .config import LOCAL_INGEST_DIR, GoldConfig, ScrapeConfig
from ..enrichment.prompts import CURRENT_PROMPT_HASH
from .creators import AD_HOC_LIMIT, enabled_profiles
from .labels import APPROVED_DECISIONS, LABEL_VERSION, run_label_pass

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
    estimated_cost_usd: float = 0.0,
) -> None:
    """Write a ``.meta`` JSON sidecar alongside the Parquet file."""
    meta = {
        "run_id": run_id,
        "dataset_id": dataset_id,
        "actor": actor,
        "item_count": item_count,
        "estimated_cost_usd": estimated_cost_usd,
        "input": {
            "urls": urls,
            "results_limit": results_limit,
            "results_type": results_type,
        },
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    meta_path = parquet_path.with_suffix(".parquet.meta")
    meta_path.write_text(json.dumps(meta, indent=2))


def _classify_bronze(df: pl.DataFrame, meta_path: Path | None) -> str:
    """Classify bronze dataset by entity type.

    Priority: meta sidecar ``input.results_type`` > schema sniffing.
    Returns ``"posts"``, ``"details"``, ``"comments"``, or ``"unknown"``.
    """
    # 1. Meta sidecar takes priority when present
    if meta_path and meta_path.exists():
        try:
            meta_text = meta_path.read_text(encoding="utf-8")
            meta = json.loads(meta_text)
            rt = meta.get("input", {}).get("results_type")
            if rt in ("posts", "details", "comments"):
                return rt
        except (json.JSONDecodeError, KeyError, OSError):
            pass

    # 2. Schema sniffing fallback for files without meta
    if len(df) == 0 or df.schema is None:
        return "unknown"

    cols = set(df.columns)

    # Posts have id + shortCode (the primary Apify identifiers)
    if "id" in cols and "shortCode" in cols:
        return "posts"
    # Details/profiles have biography + either followersCount or profilePicUrlHD
    if "biography" in cols or "followersCount" in cols:
        return "details"
    # Comments have commentId
    if "commentId" in cols:
        return "comments"

    return "unknown"


def _read_downloaded_at(meta_path: Path | None) -> datetime | None:
    """Read the scrape time from a bronze ``.meta`` sidecar, if present.

    Returns an aware UTC datetime or None when the sidecar is missing or
    carries no parseable ``downloaded_at``.
    """
    if not meta_path or not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        raw = meta.get("downloaded_at")
        if not raw:
            return None
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (json.JSONDecodeError, ValueError, OSError):
        return None


# ── Asset ─────────────────────────────────────────────────────────────────


@asset(
    name="ig_posts_raw",
    group_name="instagram",
    description="Apify Instagram scrape → typed Parquet in bronze lake.",
)
def ig_posts_raw(config: ScrapeConfig, apify: ApifyResource, ops: SQLiteResource) -> pl.DataFrame:
    """Scrape Instagram profiles via Apify, store as typed Parquet.

    Media bytes are cached into ``media_cache`` at scrape time (ingestion),
    while the CDN URLs are still fresh — the enrichment worker later uploads
    from those local bytes. Silver never caches; producers own ingestion-time
    caching. Idempotent: if the Parquet file already exists for the dataset_id,
    re-reads and returns it without re-downloading or re-caching.
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
        max_charge_usd=config.max_charge_usd,
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

    # 4. Cache media bytes at ingestion while the CDN URLs are fresh.
    #    Silver is a pure transform (no network); producers own caching.
    if len(df) > 0:
        media_df = _derive_media(df)
        seen_urls: set[str] = set()
        for media_files_json in media_df["media_files"].to_list():
            for url in json.loads(media_files_json or "[]"):
                if url not in seen_urls:
                    seen_urls.add(url)
                    cache_media_bytes(ops, url)

    # 5. Cleanup + metadata
    if ndjson_path.exists():
        ndjson_path.unlink()
    _write_meta(
        dest,
        run.run_id,
        dataset_id,
        run.actor,
        item_count,
        config.urls,
        config.results_limit,
        config.results_type,
        run.estimated_cost_usd,
    )

    return df


# ── Local ad-hoc bronze producer ───────────────────────────────────────────


def _local_post_media_pairs(post: dict, post_dir: Path) -> list[tuple[str, Path]]:
    """Map a post's media URLs to the local files the scrape saved.

    Position/type mapping of the scrape-ig-saved-list layout:
    ``videoUrl`` → ``video.mp4``; ``images[i]`` → ``media_{i:02d}.jpg``;
    ``displayUrl`` → ``media_00.jpg`` when there is no images list.
    Posts without media yield an empty list (null-skip — never an error).

    A video post (``videoUrl`` present, ``images`` empty) maps ONLY to its
    ``video.mp4`` — its ``displayUrl`` is the poster frame, not a separate
    file the scrape downloaded. Mapping it to ``media_00.jpg`` would log a
    spurious "source missing" on every run.
    """
    if post.get("videoUrl"):
        return [(post["videoUrl"], post_dir / "video.mp4")]
    pairs: list[tuple[str, Path]] = []
    images = post.get("images") or []
    if images:
        for i, url in enumerate(images):
            if url:
                pairs.append((url, post_dir / f"media_{i:02d}.jpg"))
    elif post.get("displayUrl"):
        pairs.append((post["displayUrl"], post_dir / "media_00.jpg"))
    return pairs


@asset(
    name="ig_posts_local_raw",
    group_name="instagram",
    description="Local ad-hoc scrape dumps → bronze Parquet (write-once) + media seeding.",
)
def ig_posts_local_raw(ops: SQLiteResource) -> pl.DataFrame:
    """Ingest local ad-hoc scrape dumps as a second bronze producer.

    Reads ``<LOCAL_INGEST_DIR>/<dataset_id>/<post_id>/post_metadata.json``
    (raw Apify camelCase wire format) and writes one bronze Parquet per
    dataset as ``local_<dataset_id>`` — the ``local_`` prefix namespaces
    dataset_ids that overlap with existing Apify bronze files, while
    silver's post_id dedup makes the redundancy harmless.

    WRITE-ONCE: a dataset whose ``local_<id>.parquet`` already exists is
    never rewritten — silver's mtime watermark treats a rewrite as new
    data and would re-ingest stale rows. Re-runs with no new datasets are
    a no-op for bronze.

    Media seeding: a separate idempotent pass (sha256(url)-keyed cache rows)
    copies the already-downloaded media files into the scrape-time byte
    cache instead of re-downloading expiring CDN URLs. It runs over ALL
    datasets every materialization so an interrupted run self-heals; posts
    without media (or with missing local files) are skipped silently.
    """
    frames: list[pl.DataFrame] = []
    if not LOCAL_INGEST_DIR.exists():
        logger.warning("Local ingest dir missing: %s", LOCAL_INGEST_DIR)
        return pl.DataFrame()

    for dataset_dir in sorted(p for p in LOCAL_INGEST_DIR.iterdir() if p.is_dir()):
        dataset_id = f"local_{dataset_dir.name}"
        dest = bronze_path(dataset_id)
        if dest.exists():
            # Write-once: never touch an existing bronze file — silver's
            # mtime watermark would re-ingest it with stale data.
            frames.append(pl.read_parquet(dest))
        else:
            post_files = sorted(dataset_dir.glob("*/post_metadata.json"))
            rows = [json.loads(p.read_text(encoding="utf-8")) for p in post_files]
            if not rows:
                logger.warning("Skipping %s — no post_metadata.json found", dataset_dir.name)
                continue

            # NDJSON roundtrip mirrors ig_posts_raw's proven read path for
            # the same wire format. infer_schema_length=None scans ALL rows:
            # sparse fields (e.g. a caption-like column null for the first
            # N posts) otherwise infer as NULL and a later non-null row
            # raises ComputeError.
            ndjson_path = BRONZE_LAKE / f"{dataset_id}.jsonl"
            ndjson_path.write_text(
                "".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8"
            )
            try:
                df = pl.read_ndjson(ndjson_path, infer_schema_length=None)
                df.write_parquet(dest)
            finally:
                if ndjson_path.exists():
                    ndjson_path.unlink()

            profile_urls = sorted(
                {
                    f"https://www.instagram.com/{row.get('ownerUsername')}/"
                    for row in rows
                    if row.get("ownerUsername")
                }
            ) or [f"file://{dataset_dir.as_posix()}"]
            _write_meta(
                dest,
                run_id="local-adhoc",
                dataset_id=dataset_id,
                actor="local-disk",
                item_count=len(df),
                urls=profile_urls,
                results_limit=AD_HOC_LIMIT,
                results_type="posts",
            )
            logger.info("Ingested local dataset %s: %d posts", dataset_id, len(df))
            frames.append(df)

        # Media seeding pass — idempotent per URL, self-healing on re-runs.
        for post_file in sorted(dataset_dir.glob("*/post_metadata.json")):
            row = json.loads(post_file.read_text(encoding="utf-8"))
            for url, src in _local_post_media_pairs(row, post_file.parent):
                seed_media_from_file(ops, url, src)

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


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


def _derive_media(df: pl.DataFrame) -> pl.DataFrame:
    """Derive ``media_files`` (JSON) + ``media_count`` from bronze media columns.

    Per-post precedence: a ``videoUrl`` (rich media) wins; otherwise the
    carousel ``images`` list; otherwise the single ``displayUrl`` image. The
    result is a JSON array of URLs plus its length, so the worker can resolve
    them against the scrape-time byte cache rather than the expiring CDN.
    """
    empty = pl.lit([], dtype=pl.List(pl.Utf8))

    def _scalar_list(col: str) -> pl.Expr:
        return (
            pl.when(pl.col(col).is_not_null())
            .then(pl.col(col).cast(pl.List(pl.Utf8)))
            .otherwise(empty)
        )

    video = _scalar_list("videoUrl") if "videoUrl" in df.columns else empty
    images = (
        pl.when(pl.col("images").is_not_null())
        .then(pl.col("images").cast(pl.List(pl.Utf8)))
        .otherwise(empty)
        if "images" in df.columns
        else empty
    )
    display = _scalar_list("displayUrl") if "displayUrl" in df.columns else empty

    media_list = pl.when(video.list.len() > 0).then(video).otherwise(
        pl.when(images.list.len() > 0).then(images).otherwise(display)
    )
    return df.with_columns(
        media_list.map_elements(
            lambda s: json.dumps(s.to_list() if s is not None else []),
            return_dtype=pl.Utf8,
        ).alias("media_files"),
        media_list.list.len().alias("media_count"),
    )


@asset(
    name="ig_posts_slv",
    group_name="instagram",
    description="Dedup bronze posts → silver Parquet + DuckDB state.",
    deps=["ig_posts_raw", "ig_posts_local_raw"],
)
def ig_posts_slv(duckdb: DuckDBResource) -> pl.DataFrame:
    """Read unprocessed bronze files, dedup via DuckDB DISTINCT ON, persist.

    PURE TRANSFORM — no network I/O and no media caching. Producers
    (``ig_posts_raw``, ``ig_posts_local_raw``) cache media bytes at ingestion
    while CDN URLs are fresh. Idempotent: re-running with no new bronze files
    is a no-op (returns the existing silver DataFrame).
    """

    # ── 1. Ensure state tables exist ──────────────────────────────────────
    db = duckdb
    with db.get_connection() as conn:
        conn.execute(duckdb_ddl("silver_ig_posts"))
        conn.execute(duckdb_ddl("watermarks"))
        conn.execute(duckdb_ddl("silver_ig_post_observations"))
    # ── 2. Find new bronze files (mtime > last watermark) ──────────────────
    import os as _os

    bronze_files = sorted(BRONZE_LAKE.glob("*.parquet"))
    if not bronze_files:
        return pl.DataFrame(schema={c: pl.Utf8 for c in SILVER_COLUMNS})

    with db.get_connection() as conn:
        row = conn.execute("SELECT timestamp FROM watermarks WHERE name = 'silver_ig'").fetchone()
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
            count = conn.execute("SELECT COUNT(*) FROM silver_ig_posts").fetchone()[0]
            if count == 0:
                return pl.DataFrame(schema={c: pl.Utf8 for c in SILVER_COLUMNS})
            reader = conn.execute("SELECT * FROM silver_ig_posts ORDER BY timestamp DESC").arrow()
        return pl.from_arrow(reader.read_all())
    frames = []
    max_mtime = 0.0
    for f in new_files:
        try:
            df = pl.read_parquet(f)
        except Exception as exc:
            logger.warning("Skipping %s — unreadable: %s", f.name, exc)
            continue
        mtime = _os.path.getmtime(f)
        if mtime > max_mtime:
            max_mtime = mtime

        if len(df) == 0:
            logger.info("Skipping %s — 0 rows", f.name)
            continue

        # Classify entity type; skip non-post files
        meta_path = f.with_suffix(".parquet.meta")
        entity_type = _classify_bronze(df, meta_path)
        if entity_type != "posts":
            logger.info("Skipping %s — entity type '%s' (not 'posts')", f.name, entity_type)
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
            ("processed_on", None),
            ("source_dataset", dataset_id),
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(default).alias(col))

        # Derive media_files/media_count from bronze media columns (video,
        # carousel images, single display image) before dropping the extras.
        df = _derive_media(df)

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
                df = df.with_columns(pl.col("username").alias("owner_username"))

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
        df = df.select([c for c in SILVER_COLUMNS if c in df.columns])

        # Drop rows without a valid post_id (failed Apify requests)
        df = df.filter(pl.col("post_id").is_not_null())

        # ── Transient per-file scrape time (US-S1/S2) ─────────────────────
        # meta.downloaded_at → bronze file mtime. Drives dedup ordering
        # ("newest scrape wins") and the observations fallback chain; it is
        # stamped before the union and dropped after dedup so it never
        # becomes a silver_ig_posts column.
        scraped_at = _read_downloaded_at(meta_path) or datetime.fromtimestamp(
            mtime, tz=timezone.utc
        )
        df = df.with_columns(pl.lit(scraped_at).alias("scraped_at"))

        # ── Observations append (US-S2) ───────────────────────────────────
        # One row per post per bronze file, before dedup. observed_at
        # fallback: meta.downloaded_at → file mtime → processed_on (last
        # resort, logged). INSERT OR IGNORE on PK (post_id, source_dataset)
        if "processed_on" in df.columns:
            obs_at_expr = pl.coalesce(
                pl.lit(scraped_at),
                pl.col("processed_on").cast(pl.Datetime("us", "UTC"), strict=False),
            )
        else:
            obs_at_expr = pl.lit(scraped_at)
        obs = df.select(
            "post_id",
            obs_at_expr.alias("observed_at"),
            *(pl.col(c) if c in df.columns else pl.lit(None).alias(c) for c in
              ("likes_count", "comments_count", "video_view_count", "video_play_count")),
            pl.lit(dataset_id).alias("source_dataset"),
        )
        obs_arrow = obs.to_arrow()
        with db.get_connection() as conn:
            conn.register("obs_new", obs_arrow)
            conn.execute(
                "INSERT OR IGNORE INTO silver_ig_post_observations "
                "SELECT post_id, observed_at, likes_count, comments_count, "
                "video_view_count, video_play_count, source_dataset FROM obs_new"
            )
            appended = conn.execute("SELECT COUNT(*) FROM obs_new").fetchone()[0]
        logger.info(
            "Observation candidates from %s: %d rows (scraped_at=%s)",
            f.name, appended, scraped_at,
        )

        frames.append(df)
    # ── 4. Load existing silver from DuckDB ───────────────────────────────
    existing_count = 0
    with db.get_connection() as conn:
        existing_count = conn.execute("SELECT COUNT(*) FROM silver_ig_posts").fetchone()[0]

    if existing_count > 0:
        with db.get_connection() as conn:
            existing_reader = conn.execute("SELECT * FROM silver_ig_posts").arrow()
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

    # Preserve first-seen processed_on across re-scrapes. The dedup below
    # breaks ties on source_dataset (a random dataset id); carrying the
    # existing non-null processed_on forward to every row of a post_id means
    # the tie-break can never re-stamp it to "now".
    unified = unified.with_columns(
        pl.col("processed_on").fill_null(
            pl.col("processed_on").max().over("post_id")
        )
    )
    unified_arrow = unified.to_arrow()
    with db.get_connection() as conn:
        conn.register("unified", unified_arrow)

        deduped_arrow = conn.execute("""
            SELECT DISTINCT ON(post_id) *
            FROM unified
            ORDER BY post_id, scraped_at DESC NULLS LAST, source_dataset DESC
        """).arrow()

    deduped = pl.from_arrow(deduped_arrow)

    # scraped_at is transient — it drives dedup ordering and observation
    # provenance but must never become a silver_ig_posts column.
    deduped = deduped.drop("scraped_at")

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
        conn.execute("INSERT OR REPLACE INTO silver_ig_posts SELECT * FROM to_upsert")

    # Advance the silver watermark to the newest bronze file examined this
    # run so the next materialization only re-reads genuinely new files.
    if max_mtime > 0:
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO watermarks (name, timestamp) "
                "VALUES ('silver_ig', ?)",
                [datetime.fromtimestamp(max_mtime, tz=timezone.utc).replace(tzinfo=None)],
            )

    return deduped


# ── Entity-specific assets ────────────────────────────────────────────────


@asset(
    name="ig_post_labels",
    group_name="instagram",
    description=(
        "Tukey-fence standout labels + triage decisions per post "
        "(daily; self-versioned via LABEL_VERSION)."
    ),
    deps=["ig_posts_slv"],
)
def ig_post_labels(duckdb: DuckDBResource, ops: SQLiteResource) -> pl.DataFrame:
    """Stamp ``ig_post_labels`` for every silver post (plan §4 rule table).

    Reads the latest non-sentinel observation per post, judges against the
    trailing Tukey baseline, and upserts labels. Provisional day0 labels
    upgrade exactly once to day7 when a core post matures; day7 labels are
    immutable. Idempotent — re-running with no new data is a no-op.
    """
    _ensure_state_tables(duckdb)
    core_handles = {
        (p["handle"] or "").lower().lstrip("@")
        for p in enabled_profiles(ops)
        if p["platform"] == "instagram" and p["tier"] == "tier1"
    }
    with duckdb.get_connection() as conn:
        stats = run_label_pass(conn, core_handles=core_handles)
        labels = pl.from_arrow(
            conn.execute("SELECT * FROM ig_post_labels").arrow().read_all()
        )
    logger.info("ig_post_labels pass: %s", stats)
    return labels


@asset(
    name="ig_profiles_slv",
    group_name="instagram",
    description="Extract profiles + download avatars from post/details scrapes.",
    deps=["ig_posts_raw"],
)
def ig_profiles_slv(duckdb: DuckDBResource, ops: SQLiteResource) -> pl.DataFrame:
    """Extract profiles from post and details scrapes; download avatars.

    Post scrapes carry the author's profile fields (username, profilePicUrlHD,
    owner_id) on every row, so profiles can be built from either entity type.
    Avatars are downloaded at scrape time — CDN URLs expire in ~4-5 days.
    """

    from ..common.lake import avatar_path
    from ..common.schemas import DUCKDB_TABLES
    from .creators import enabled_profiles

    db = duckdb
    _ensure_state_tables(db)

    # Profile list comes from the profiles control table (ops).
    targets = enabled_profiles(ops)
    if targets:
        logger.info(
            "Tracking %d enabled profile(s): %s",
            len(targets),
            sorted(t["handle"] for t in targets),
        )

    # Find details-type bronze files that haven't been processed
    bronze_files = sorted(BRONZE_LAKE.glob("*.parquet"))
    if not bronze_files:
        if targets:
            logger.warning("No bronze files for %d enabled target(s)", len(targets))
        return pl.DataFrame(schema={"owner_id": pl.Utf8})

    with db.get_connection() as conn:
        row = conn.execute("SELECT timestamp FROM watermarks WHERE name = 'profiles_ig'").fetchone()
    if row and row[0] is not None:
        dt = row[0]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        watermark_ts = dt.timestamp()
    else:
        watermark_ts = 0.0

    import os as _os

    new_files = [f for f in bronze_files if _os.path.getmtime(f) > watermark_ts]
    if not new_files:
        with db.get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM silver_ig_profiles").fetchone()[0]
            if count == 0:
                return pl.DataFrame(schema={"owner_id": pl.Utf8})
            reader = conn.execute(
                "SELECT * FROM silver_ig_profiles ORDER BY owner_username"
            ).arrow()
        return pl.from_arrow(reader.read_all())

    frames = []
    max_mtime = 0.0
    for f in new_files:
        try:
            df = pl.read_parquet(f)
        except Exception as exc:
            logger.warning("Skipping %s — unreadable: %s", f.name, exc)
            continue

        if len(df) == 0:
            logger.info("Skipping %s — 0 rows", f.name)
            continue

        # Classify — process both details-type and post-type (both carry
        # profile fields). Comments carry no profile data.
        meta_path = f.with_suffix(".parquet.meta")
        entity_type = _classify_bronze(df, meta_path)
        if entity_type not in ("details", "posts"):
            continue

        mtime = _os.path.getmtime(f)
        if mtime > max_mtime:
            max_mtime = mtime

        # Map camelCase columns → snake_case
        _profile_col_map = {
            "ownerId": "owner_id",
            "username": "owner_username",
            "fullName": "full_name",
            "biography": "biography",
            "followersCount": "followers_count",
            "followsCount": "follows_count",
            "postsCount": "posts_count",
            "isBusinessAccount": "is_business",
            "isVerified": "is_verified",
            "externalUrl": "external_url",
        }
        to_rename = {
            old: new
            for old, new in _profile_col_map.items()
            if old in df.columns and new not in df.columns
        }
        df = df.rename(to_rename)

        # Profile pic: prefer HD, fall back to standard. Handled separately
        # because both source columns map to the same target.
        if "profilePicUrlHD" in df.columns:
            df = df.rename({"profilePicUrlHD": "profile_pic_url"})
        elif "profilePicUrl" in df.columns:
            df = df.rename({"profilePicUrl": "profile_pic_url"})

        dataset_id = f.stem
        for col, default in [
            ("owner_id", None),
            ("owner_username", None),
            ("full_name", None),
            ("biography", None),
            ("followers_count", 0),
            ("follows_count", 0),
            ("posts_count", 0),
            ("is_business", False),
            ("is_verified", False),
            ("profile_pic_url", None),
            ("external_url", None),
            ("source_dataset", dataset_id),
            ("processed_on", None),
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(default).alias(col))

        # Download avatar from fresh CDN URL (they expire in ~4-5 days)
        if "profile_pic_url" in df.columns:
            profile_rows = df.select(["owner_username", "profile_pic_url"]).unique().rows()
            for owner_username, pic_url in profile_rows:
                if not owner_username or not pic_url:
                    continue
                local = avatar_path(owner_username)
                if local.exists() and local.stat().st_size > 0:
                    continue  # Already cached
                try:
                    import urllib.request as _urllib

                    req = _urllib.Request(
                        pic_url,
                        headers={
                            "User-Agent": (
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/120.0.0.0 Safari/537.36"
                            ),
                            "Referer": "https://www.instagram.com/",
                        },
                    )
                    resp = _urllib.urlopen(req, timeout=15)
                    body = resp.read()
                    local.write_bytes(body)
                    logger.info(
                        "Downloaded avatar %s -> %s (%d bytes)",
                        owner_username,
                        local,
                        len(body),
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to download avatar for %s: %s",
                        owner_username,
                        exc,
                    )
                    continue

        # Keep only valid profile columns
        schema_cols = list(DUCKDB_TABLES["silver_ig_profiles"].keys())
        df = df.select([c for c in schema_cols if c in df.columns])

        # Drop rows without owner_id, then collapse post scrapes (one row per
        # post) to a single row per profile.
        df = df.filter(pl.col("owner_id").is_not_null())
        df = df.unique(subset=["owner_id"], keep="first")
        frames.append(df)

    # ── 3. Load existing + dedup via DuckDB ─────────────────────────────
    with db.get_connection() as conn:
        existing_count = conn.execute("SELECT COUNT(*) FROM silver_ig_profiles").fetchone()[0]

    if existing_count > 0:
        with db.get_connection() as conn:
            reader = conn.execute("SELECT * FROM silver_ig_profiles").arrow()
        existing_df = pl.from_arrow(reader.read_all())
        frames.insert(0, existing_df)

    if not frames:
        return pl.DataFrame(schema={"owner_id": pl.Utf8})

    unified = pl.concat(frames, how="diagonal_relaxed")
    if unified.is_empty():
        return pl.DataFrame(schema={"owner_id": pl.Utf8})

    now_iso = datetime.now(timezone.utc).isoformat()
    unified = unified.with_columns(
        pl.when(pl.col("processed_on").is_null())
        .then(pl.lit(now_iso))
        .otherwise(pl.col("processed_on"))
        .alias("processed_on")
    )

    with db.get_connection() as conn:
        conn.register("to_upsert", unified.to_arrow())
        conn.execute("INSERT OR REPLACE INTO silver_ig_profiles SELECT * FROM to_upsert")

    # Advance watermark
    if max_mtime > 0:
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO watermarks (name, timestamp) VALUES ('profiles_ig', ?)",
                [datetime.fromtimestamp(max_mtime, tz=timezone.utc).replace(tzinfo=None)],
            )

    return unified


@asset(
    name="ig_comments_slv",
    group_name="instagram",
    description="Comment scrapes from bronze → silver (STUB — not yet implemented).",
    deps=["ig_posts_raw"],
)
def ig_comments_slv(duckdb: DuckDBResource) -> pl.DataFrame:
    """Stub for comment-type bronze processing.

    No comment-type bronze datasets exist yet. Full implementation deferred
    until real comment data arrives (modeling against non-existent data is
    a confirmed anti-pattern from Phase 2 false start). Ensures the table
    exists so the schema contract holds, but reads no bronze files.
    """
    _ensure_state_tables(duckdb)
    logger.warning("ig_comments_slv: not yet implemented — returning empty")
    return pl.DataFrame(schema={"comment_id": pl.Utf8})


@asset(
    name="ig_posts_gen_batches",
    group_name="instagram",
    description="Drain triage-approved labels into Gemini enrichment batches.",
    deps=["ig_post_labels"],
)
def ig_posts_gen_batches(
    config: GoldConfig,
    duckdb: DuckDBResource,
    ops: SQLiteResource,
) -> pl.DataFrame:
    """Drain label-approved posts into a Gemini batch.

    Dumb drain over ``ig_post_labels`` (US-L4): any post whose label pass
    approved it for enrichment (standout / control / floor_filler) that has
    no current-prompt gold analysis and no open batch item is enqueued.
    The ``gold_ig`` watermark is retired — the labels table is the discovery
    source. Explicit ``post_ids`` re-enrichment bypasses all guards.

    Stale gold rows (prompt_hash != CURRENT_PROMPT_HASH, e.g. pre-multimodal
    text-only analyses) are re-enqueue-eligible (US-L5) — only a current
    prompt_hash blocks. Empty-caption posts never reach this asset: the
    label pass sets enrich_decision='skip' for them (US-L6).

    ``whole_corpus`` (GoldConfig) opts into corpus-wide admission: ALL silver
    posts with non-empty captions (including the label pass's ``skip``
    posts) are enqueued for a text-only pass (ADR-0001). Still excludes
    current-prompt gold rows + open batch items so a re-run never re-pays.
    """
    import json

    from datalake.defs.enrichment.batch import _ensure_schema, create_batch

    db = duckdb
    _ensure_state_tables(db)

    post_ids = list(config.post_ids or [])

    with db.get_connection() as conn:
        if post_ids:
            # Targeted re-enrichment: ad-hoc post_ids bypass labels, the
            # gold guard, and open-batch guards (re-process at will).
            pending = conn.execute(
                """SELECT sp.post_id
                   FROM silver_ig_posts sp
                   WHERE list_contains(?, sp.post_id)""",
                [post_ids],
            ).fetchall()
            candidates = [r[0] for r in pending]
            candidates_seen = len(candidates)
        elif config.whole_corpus:
            # Corpus-wide admission (opt-in): bypass the label gate entirely.
            candidates = [
                r[0]
                for r in conn.execute(
                    """
                    SELECT sp.post_id
                    FROM silver_ig_posts sp
                    WHERE sp.caption IS NOT NULL AND trim(sp.caption) <> ''
                      AND NOT EXISTS (
                          SELECT 1 FROM gold_analyses g
                          WHERE g.post_id = sp.post_id
                            AND g.domain = 'instagram'
                            AND g.prompt_hash = ?
                      )
                    """,
                    [CURRENT_PROMPT_HASH],
                ).fetchall()
            ]
            candidates_seen = len(candidates)
        else:
            candidates = [
                r[0]
                for r in conn.execute(
                    """
                    SELECT l.post_id
                    FROM ig_post_labels l
                    WHERE l.enrich_decision IN ('standout', 'control', 'floor_filler')
                      AND l.label_version = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM gold_analyses g
                          WHERE g.post_id = l.post_id
                            AND g.domain = 'instagram'
                            AND g.prompt_hash = ?
                      )
                    """,
                    [LABEL_VERSION, CURRENT_PROMPT_HASH],
                ).fetchall()
            ]
            candidates_seen = len(candidates)

    _ensure_schema(ops)
    if candidates:
        ops_conn = ops.get_connection()
        try:
            open_ids = {
                (json.loads(r[0]) or {}).get("post_id")
                for r in ops_conn.execute(
                    "SELECT payload FROM batch_items "
                    "WHERE status IN ('pending', 'processing')"
                ).fetchall()
            }
        finally:
            ops_conn.close()
        candidates = [pid for pid in candidates if pid not in open_ids]

    payloads = [
        json.dumps({"post_id": pid, "domain": "instagram"}) for pid in candidates
    ]

    if payloads:
        # Whole-corpus passes run on the Gemini BATCH API (paid tier, ~50%
        # cheaper); tag them so the gemini-batch worker claims them.
        create_batch(
            ops, payloads, consumer="gemini",
            mode="gemini-batch" if config.whole_corpus else "interactive",
        )

    return pl.DataFrame(
        {
            "enqueued": pl.Series([len(payloads)], dtype=pl.Int32),
            "candidates_seen": pl.Series([candidates_seen], dtype=pl.Int32),
        }
    )


# ── Shared helpers ────────────────────────────────────────────────────────


def _ensure_state_tables(db: DuckDBResource) -> None:
    """Create shared state tables if they don't exist."""
    with db.get_connection() as conn:
        for name in (
            "gold_analyses",
            "ig_post_labels",
            "silver_ig_post_observations",
            "watermarks",
            "silver_ig_profiles",
            "silver_ig_comments",
        ):
            conn.execute(duckdb_ddl(name))
