"""Recover media for posts whose scrape-time byte cache is missing.

WHY THIS EXISTS
---------------
Media is cached at scrape time, while the CDN URL is still signed. Instagram's URLs
carry an ``oe`` expiry of ~4.5 days (measured), so a post whose download failed during
its scrape can never be cached by that URL again — it is permanently un-enrichable,
because enrichment resolves media by hashing the URL silver holds.

The only cure for an expired URL is to mint a fresh one. Apify can do that by
permalink: one post costs $0.0023 and returns current, downloadable media URLs.

THE KEYING RULE (the whole point of this module)
------------------------------------------------
A recovered post's fresh URLs are DIFFERENT URLs — different sha256 — from the ones
silver stores. Measured on a real recovery: ``stored ∩ fresh == 0``. So caching bytes
under the fresh URL leaves the stored URL still uncached and the post still
un-enrichable, which is the exact failure this module exists to fix.

Therefore bytes are cached under the ORIGINAL, STORED URL's hash. The bytes satisfy
the reference enrichment actually resolves (``media_files`` in silver); the fresh URL
is only the transport that delivered them. Provenance records both.

This is the opposite of a "regenerated URL is a cache miss" case (WATCHDOG.md) —
that rule describes a NEW scrape minting a new URL for a post that was never cached.
Here the stored URL is the durable identity and the fresh URL is disposable.

NOT A ONE-OFF SCRIPT
--------------------
Recovery is a standing mechanism, not a migration: the ~4.5-day window makes media
loss continuous, so every future scrape can lose the same way. This is a bronze
producer that takes a set of permalinks, so it serves the current backlog and any
future one.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from dagster import asset
from dagster_duckdb import DuckDBResource

from orchestration.defs.engine.media import (
    cache_media_bytes,
    local_media_path,
    seed_media_from_file,
)
from orchestration.defs.integration.apify_runs import (
    poll_run,
    stream_dataset,
    trigger_run,
)
from orchestration.defs.platform.paths import BRONZE_LAKE
from orchestration.defs.platform.resources import ApifyResource, SQLiteResource

logger = logging.getLogger(__name__)

#: The Apify actor that can resolve a permalink to current media URLs.
RECOVERY_ACTOR = "apify~instagram-scraper"

#: Charge cap per recovery run, USD. A single-post fetch costs $0.0023 (measured);
#: this is a ceiling well above it so an actor misbehaving on one URL cannot run
#: away with the key's credit.
RECOVERY_CHARGE_CAP_USD = 0.05

#: Consecutive fetch failures before the producer stops.
#:
#: A breaker, not a retry policy: if Apify stops resolving permalinks (auth, quota,
#: schema change) every subsequent post fails identically and the run would burn the
#: whole backlog's budget producing nothing. Tripping early keeps the spend honest
#: and the failure loud.
MAX_CONSECUTIVE_FAILURES = 25


@dataclass
class RecoveryReport:
    """Outcome of a recovery pass.

    `recovered` counts POSTS whose stored URLs are now fully cached — the unit the
    operator cares about, since a post is enrichable or it is not. `urls_cached`
    counts individual files, which can exceed posts (a carousel has several).
    """

    attempted: int = 0
    recovered: int = 0
    urls_cached: int = 0
    failed: int = 0
    skipped_cached: int = 0
    failed_permalinks: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.failed == 0

    def summary(self) -> str:
        return (
            f"recovery: {self.recovered}/{self.attempted} posts recovered "
            f"({self.urls_cached} files), {self.skipped_cached} already cached, "
            f"{self.failed} failed"
        )


def posts_missing_media(duckdb: DuckDBResource, ops: SQLiteResource) -> list[dict]:
    """Posts whose stored media URLs are not all cached, newest first.

    Only posts that HAVE stored URLs are returned: a post with an empty
    ``media_files`` has no hash to key recovered bytes under, so a paid fetch
    could not make it enrichable (that population is out of scope here and is
    ~1,189 posts — it needs its own decision, not this mechanism).

    Returns dicts of ``post_id``, ``url`` (the permalink), and ``missing`` (the
    stored URLs with no cached bytes).
    """
    conn = duckdb.get_connection()
    try:
        rows = conn.execute(
            """
            SELECT post_id, url, media_files
            FROM silver_ig_posts
            WHERE media_files IS NOT NULL AND media_files != '' AND media_files != '[]'
            """
        ).fetchall()
    finally:
        conn.close()

    out: list[dict] = []
    for post_id, permalink, media_files in rows:
        try:
            urls = [u for u in json.loads(media_files) if u]
        except (TypeError, ValueError):
            continue
        if not urls:
            continue
        missing = [u for u in urls if local_media_path(ops, u) is None]
        if missing:
            out.append(
                {
                    "post_id": post_id,
                    "url": permalink,
                    "stored": urls,
                    "missing": missing,
                }
            )
    return out


def _fresh_media_for_post(item: dict) -> list[str]:
    """The post's media URLs in the SAME ORDER the scrape recorded them.

    This is the pairing rule, and it is load-bearing.

    A Sidecar's ``childPosts[]`` mirrors the scraped ``media_files[]`` 1:1 by index
    (verified: a 7-URL carousel returns 7 children in the same order). The item's
    OWN ``displayUrl`` must be EXCLUDED for a Sidecar — it is the post's cover frame,
    not one of the carousel entries, and including it shifts every pairing by one
    (measured: prepending it produced 8 URLs against 7 stored).

    So:
      - ``childPosts`` present  -> the children, in order.
      - otherwise              -> the item's own displayUrl/videoUrl (single-media post).

    Caching wrong-position bytes under a stored URL's hash is worse than a miss:
    enrichment would send the wrong image to the model and nothing re-fetches it.
    """
    children = item.get("childPosts") or []
    if children:
        return [u for ch in children for u in _own_media(ch)]
    return _own_media(item)


def _own_media(item: dict) -> list[str]:
    """An item's or child's own media URLs, deduped, display frame before video.

    Deduped because a video child commonly carries BOTH ``displayUrl`` (its poster
    frame) and ``videoUrl`` pointing at the SAME underlying file — falling back to
    the poster when they differ. Emitting both would produce two entries for one
    media slot and shift every later pairing.
    """
    display = item.get("displayUrl")
    video = item.get("videoUrl")
    if display and video:
        return [video] if display == video else [display, video]
    return [u for u in (display, video) if u]


def recover_one(
    ops: SQLiteResource,
    apify: ApifyResource,
    *,
    post_id: str,
    permalink: str,
    stored: list[str],
    media_dir: Path | None = None,
) -> tuple[int, str | None]:
    """Fetch one post by permalink and cache its media under the STORED url hashes.

    Returns ``(files_cached, error)`` — error is None on success.

    `stored` is the post's FULL ``media_files`` list, not just the uncached subset:
    the fresh item mirrors it 1:1 by index, so pairing must use the same indices.
    URLs already cached are skipped inside the loop, which makes a re-run of a
    partially-recovered post a no-op.

    The pairing rule: ``childPosts[i]`` corresponds to ``stored[i]`` (verified on a
    7-URL carousel, 1:1 in order). A count mismatch aborts the post rather than
    guessing. Cached bytes are keyed by the STORED url's hash, because that is what
    ``media_urls_to_local_paths`` — and therefore enrichment — resolves.
    """
    run = trigger_run(
        RECOVERY_ACTOR,
        [permalink],
        token=apify.token,
        results_limit=1,
        results_type="posts",
        max_charge_usd=RECOVERY_CHARGE_CAP_USD,
    )
    outcome = poll_run(run.run_id, token=apify.token, timeout=600)

    tmp = BRONZE_LAKE / f"_recover_{post_id}.ndjson"
    try:
        count = stream_dataset(outcome.dataset_id, tmp, token=apify.token)
        if count == 0:
            return 0, "Apify returned no item for the permalink"
        item = json.loads(tmp.read_text(encoding="utf-8").splitlines()[0])
    finally:
        if tmp.exists():
            tmp.unlink()

    fresh = _fresh_media_for_post(item)
    if not fresh:
        return 0, "Apify item carried no media URLs"

    # Pair against the FULL stored list by index: fresh[i] mirrors stored[i]. Pairing
    # against `missing` (the uncached subset) would shift every entry after the first
    # cached one — the exact mispairing this module exists to avoid.
    if len(fresh) != len(stored):
        # A count mismatch means the mapping is not understood for this post, so
        # NOTHING is cached. Partially caching on a guessed pairing would put bytes
        # under the wrong hashes with no miss to trigger a re-fetch.
        return 0, (
            f"media count mismatch: {len(stored)} stored vs {len(fresh)} fresh — "
            "refusing to guess the pairing"
        )

    cached = 0
    for stored_url, fresh_url in zip(stored, fresh, strict=True):
        if local_media_path(ops, stored_url) is not None:
            continue  # already cached; a partial recovery re-run is a no-op
        if _cache_under(ops, fresh_url, stored_url, media_dir=media_dir):
            cached += 1
    if cached == 0:
        return 0, "fetched media but cached none of the stored URLs"
    return cached, None


def _cache_under(
    ops: SQLiteResource,
    fresh_url: str,
    stored_url: str,
    *,
    media_dir: Path | None = None,
) -> bool:
    """Download ``fresh_url``'s bytes and record them under ``stored_url``'s hash.

    The stored URL is the durable identity — it is what silver holds and what
    ``media_urls_to_local_paths`` hashes when enrichment resolves a post — while the
    fresh URL is disposable transport that expires again in ~4.5 days. Caching under
    the fresh hash would leave the post un-enrichable, which is the bug being fixed.

    Idempotent: a stored URL that already resolves is left alone.
    """
    if local_media_path(ops, stored_url) is not None:
        return True
    tmp_path = cache_media_bytes(ops, fresh_url, media_dir=media_dir)
    if not tmp_path:
        return False
    src = local_media_path(ops, fresh_url)
    if src is None:
        return False
    return (
        seed_media_from_file(ops, stored_url, Path(src), media_dir=media_dir)
        is not None
    )


@asset(
    name="bronze_ig_media_recovery",
    group_name="instagram",
    description=(
        "Recover missing post media by permalink fetch, caching bytes under the "
        "STORED url hashes that enrichment resolves. Paid: ~$0.0023/post."
    ),
    deps=["silver_ig_posts"],
)
def bronze_ig_media_recovery(
    duckdb: DuckDBResource,
    ops: SQLiteResource,
    apify: ApifyResource,
) -> pl.DataFrame:
    """Recover media for every post whose stored media URLs are not all cached.

    Not scheduled. This spends money, so it runs when an operator launches it — the
    same "schedules ship stopped" rule the rest of the platform follows.
    """
    if not apify.token:
        raise RuntimeError("Apify API token is empty — set APIFY_API_TOKEN")

    candidates = posts_missing_media(duckdb, ops)
    report = RecoveryReport()
    if not candidates:
        logger.info("media recovery: nothing missing")
        return pl.DataFrame()

    logger.info(
        "media recovery: %d post(s) missing media, est. cost $%.2f at $0.0023/post",
        len(candidates),
        len(candidates) * 0.0023,
    )

    consecutive = 0
    for cand in candidates:
        report.attempted += 1
        cached, error = recover_one(
            ops,
            apify,
            post_id=cand["post_id"],
            permalink=cand["url"],
            stored=cand["stored"],
        )
        if error is None:
            report.recovered += 1
            report.urls_cached += cached
            consecutive = 0
        else:
            report.failed += 1
            report.failed_permalinks.append(cand["url"])
            consecutive += 1
            logger.error(
                "media recovery FAILED for %s: %s", cand["url"], error
            )
            if consecutive >= MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    "media recovery: %d consecutive failures — stopping early "
                    "rather than spending the remaining budget on a broken fetch",
                    consecutive,
                )
                break

    logger.info(report.summary())
    if report.failed_permalinks:
        logger.error(
            "media recovery: %d post(s) still missing media: %s",
            len(report.failed_permalinks),
            report.failed_permalinks[:10],
        )

    return pl.DataFrame(
        {
            "post_id": [c["post_id"] for c in candidates[: report.attempted]],
            "permalink": [c["url"] for c in candidates[: report.attempted]],
        }
    )
