"""Instagram bronze — Apify scrape → typed Parquet in the bronze lake.

Manual-trigger only (no schedule): the operator supplies a `ScrapeConfig` in the
Dagster launchpad. Media bytes are cached into the media cache at scrape time,
while the CDN URLs are still fresh — enrichment runs months later on a backlog,
long after the CDN URLs expire. Producers own ingestion-time caching; silver
never caches.

Two ingestion paths share this module: the Apify scrape (`ig_posts_raw`, remote
CDN) and the local-disk ad-hoc path (`ig_posts_local_raw`, `LOCAL_INGEST_DIR`).
Both land typed Parquet plus a `.meta` JSON sidecar for lineage.
"""
import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import polars as pl
from dagster import Config, asset
from opsdb.roster import AD_HOC_LIMIT

from orchestration.defs.engine.media import cache_media_bytes, seed_media_from_file
from orchestration.defs.ig_core.slv.posts import (
    _derive_media,
)
from orchestration.defs.integration.apify_client import poll_run, stream_dataset, trigger_run
from orchestration.defs.platform.paths import BRONZE_LAKE, bronze_path
from orchestration.defs.platform.resources import (
    ApifyResource,
    SQLiteResource,
)

logger = logging.getLogger(__name__)


# ── Config ──────────────────────────────────────────────────────────────────


class ResultsType(str, Enum):
    """Valid ``resultsType`` values for the Apify Instagram scraper."""

    POSTS = "posts"
    DETAILS = "details"
    COMMENTS = "comments"


class ScrapeConfig(Config):
    """Configuration for triggering an Apify Instagram scrape."""

    urls: list[str]
    results_limit: int = 12
    results_type: ResultsType = ResultsType.POSTS
    max_charge_usd: float | None = None


# ── Local ad-hoc ingestion ─────────────────────────────────────────────────

LOCAL_INGEST_DIR = Path(
    os.environ.get(
        "IG_LOCAL_INGEST_DIR",
        r"C:\Users\evano\repos\scrape-ig-saved-list\data\ingest",
    )
)
"""Root of local ad-hoc scrape dumps: ``<dataset_id>/<post_id>/post_metadata.json``.

Override with ``IG_LOCAL_INGEST_DIR``. Each ``<dataset_id>`` subdirectory is one
ad-hoc (non-continuous) scrape and becomes one bronze ``local_<dataset_id>``
file; media files (``video.mp4``, ``media_NN.jpg``) sit beside each
``post_metadata.json``.
"""


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


# ── Details scrape runner ───────────────────────────────────────────────────


def scrape_details_to_bronze(
    profile_url: str,
    *,
    token: str,
    results_limit: int = 1,
) -> str:
    """Run a details-type Apify scrape for one profile → bronze Parquet.

    Returns the dataset_id. Idempotent: skips if the Parquet already exists.
    """
    import json as _json
    from datetime import datetime

    import polars as pl

    from ..common.apify import poll_run, stream_dataset, trigger_run
    from ..common.lake import BRONZE_LAKE, bronze_path

    run = trigger_run(
        "apify~instagram-scraper",
        [profile_url],
        token=token,
        results_limit=results_limit,
        results_type="details",
    )
    dataset_id = poll_run(run.run_id, token=token)

    dest = bronze_path(dataset_id)
    if dest.exists():
        return dataset_id

    ndjson_path = BRONZE_LAKE / f"{dataset_id}.jsonl"
    item_count = stream_dataset(dataset_id, dest=ndjson_path, token=token)

    if item_count == 0:
        pl.DataFrame().write_parquet(dest)
    else:
        pl.read_ndjson(ndjson_path).write_parquet(dest)

    if ndjson_path.exists():
        ndjson_path.unlink()

    meta = {
        "run_id": run.run_id,
        "dataset_id": dataset_id,
        "actor": run.actor,
        "item_count": item_count,
        "input": {
            "urls": [profile_url],
            "results_limit": results_limit,
            "results_type": "details",
        },
        "downloaded_at": datetime.now().astimezone().isoformat(),
    }
    dest.with_suffix(".parquet.meta").write_text(_json.dumps(meta, indent=2))
    return dataset_id
