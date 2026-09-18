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
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from dagster import asset
from dagster_duckdb import DuckDBResource
from opsdb.media_cache import cache_keys_for

from orchestration.defs.engine.media import (
    _EXT_BY_MIME,
    POST_MEDIA_DIR,
    _atomic_write,
    _download_bytes,
    _PermanentFetchError,
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
    with duckdb.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT post_id, url, media_files
            FROM silver_ig_posts
            WHERE media_files IS NOT NULL AND media_files != '' AND media_files != '[]'
            """
        ).fetchall()

    # ONE read of the cache key set, not one lookup per URL. `local_media_path`
    # opens a fresh SQLite connection each call (~23 ms on Windows, dominated by
    # the WAL pragma), and a corpus-wide scan makes ~30k such calls — measured at
    # 320s before this change, which dominated the whole run.
    #
    # BOTH key forms are held verbatim, and membership is tested against BOTH.
    # Neither normalization works alone:
    #   - Legacy rows hold `sha256(the original scrape URL)`. That string is not
    #     recoverable from silver's `url` (it carries its own signature), so
    #     re-deriving `media_key(source_url)` produces a key that matches nothing
    #     and silently relists every legacy post — measured at 8,848 candidates
    #     ($20) when the real backlog was 233.
    #   - `mid:` rows hold only the stable key.
    # So the set keeps raw keys, and each silver URL is tested via
    # `cache_keys_for(u)` (stable first, then legacy) — matching how
    # `local_media_path` resolves at enrichment time.
    with ops.get_connection() as cache_conn:
        cached_keys = {
            r[0] for r in cache_conn.execute("SELECT cache_key FROM media_cache")
        }

    candidates: list[tuple[str, str, list[str], list[str]]] = []
    for post_id, permalink, media_files in rows:
        try:
            urls = [u for u in json.loads(media_files) if u]
        except (TypeError, ValueError):
            continue
        if not urls:
            continue
        missing = [
            u for u in urls if not any(k in cached_keys for k in cache_keys_for(u))
        ]
        if missing:
            candidates.append((post_id, permalink, urls, missing))

    # Confirm the few candidates whose stored URLs all appear cached but whose
    # bytes may have gone missing from disk under a real existence check.
    out: list[dict] = []
    for post_id, permalink, urls, missing in candidates:
        if not missing and not any(local_media_path(ops, u) is None for u in urls):
            continue
        out.append(
            {
                "post_id": post_id,
                "url": permalink,
                "stored": urls,
                "missing": missing,
            }
        )
    return out


def _first_item(ndjson_path: Path) -> dict:
    """Parse the first item from a streamed NDJSON file.

    Splits on ``"\\n"``, NOT ``str.splitlines()``. Python's ``splitlines`` also
    breaks on U+2028 LINE SEPARATOR and U+2029 PARAGRAPH SEPARATOR, which are
    VALID characters inside a JSON string — and Instagram captions contain them
    (a real caption broke the parse at char 143 with "Unterminated string").
    ``json.dumps`` only ever escapes ``\\n``, so ``"\\n"`` is the only separator
    that appears between items.
    """
    text = ndjson_path.read_text(encoding="utf-8")
    for line in text.split("\n"):
        if line.strip():
            return json.loads(line)
    raise ValueError(f"no JSON item in {ndjson_path.name}")


def _fresh_media_for_post(item: dict) -> list[str]:
    """The post's media URLs in the SAME ORDER the scrape recorded them.

    This is the pairing rule, and it is load-bearing. Three shapes, each verified
    against a real fetch:

    - **Sidecar** (``childPosts`` present): ``childPosts[i]`` corresponds to
      ``stored[i]`` 1:1 in order (verified on a 7-URL carousel, 7 vs 7). The
      item's OWN ``displayUrl`` is the post's cover frame, NOT a carousel entry —
      including it shifts every pairing by one (measured: prepending it gave 8
      URLs against 7).
    - **Video** (``videoUrl`` present, no children): silver stores ONE url, the
      video. Apify returns TWO — the poster ``displayUrl`` and the ``videoUrl`` —
      so the video is selected and the poster skipped (measured: 1 stored vs 2
      fresh, and the stored url is the ``.mp4``).
    - **Image**: the single ``displayUrl``, 1:1.

    Caching wrong-position bytes under a stored URL's hash is worse than a miss:
    enrichment would send the wrong image to the model and nothing re-fetches it.
    """
    children = item.get("childPosts") or []
    if children:
        return [u for ch in children for u in _own_media(ch)]
    video = item.get("videoUrl")
    if video:
        # A video post: its media is the video, not the poster frame.
        return [video]
    return _own_media(item)


def _own_media(child: dict) -> list[str]:
    """A CHILD's media URLs, deduped, video preferred over its poster frame.

    A video child carries both ``displayUrl`` (its poster) and ``videoUrl`` for the
    same media. Emitting both would produce two entries for one carousel slot and
    shift every later pairing, so the video is preferred and the poster used only
    when there is no video. Verified: a 7-child carousel pairs to 7 stored URLs.
    """
    video = child.get("videoUrl")
    if video:
        return [video]
    display = child.get("displayUrl")
    return [display] if display else []


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
    # settle_cost=False: this fan-out runs ONE run per post and never records the
    # billable cost, so waiting up to 30s for Apify to publish it would be pure
    # dead time — and at one run per post it dominates the pass.
    outcome = poll_run(
        run.run_id, token=apify.token, timeout=600, settle_cost=False
    )

    tmp = BRONZE_LAKE / f"_recover_{post_id}.ndjson"
    try:
        count = stream_dataset(outcome.dataset_id, tmp, token=apify.token)
        if count == 0:
            return 0, "Apify returned no item for the permalink"
        item = _first_item(tmp)
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
    """Download ``fresh_url``'s bytes and record them under ``stored_url``'s key.

    The stored URL is the durable identity — it is what silver holds and what
    ``media_urls_to_local_paths`` resolves when enrichment processes a post —
    while the fresh URL is disposable transport that expires again in ~4.5 days.

    BOTH IDENTITIES NOW DERIVE FROM THE SAME MEDIA ID, so the obvious
    implementation is silently wrong. ``media_key(fresh)`` and
    ``media_key(stored)`` both yield ``mid:<id>``, which means
    ``cache_media_bytes(fresh)`` returns EARLY (its idempotence check) whenever
    that key already resolves — and on a partially-cached carousel the thing it
    resolves to is a DIFFERENT item's file. Seeding from it would copy the wrong
    bytes under the stored key: a silent mispair, worse than a miss, because
    enrichment would then ship the wrong image to the model and nothing would
    re-fetch it.

    So the download is done directly, with no cache-key consultation: fetch the
    bytes to a temp file, then seed under the stored key. Whether the target
    already resolves is checked first (that skip is genuine idempotence); nothing
    else about the fresh URL's cache state is consulted.
    """
    if local_media_path(ops, stored_url) is not None:
        return True

    # No cache lookup on the fresh URL — deliberately. See the docstring.
    try:
        downloaded = _download_bytes(fresh_url)
    except _PermanentFetchError as exc:
        # An expired signature never revives; a re-scrape minting a fresh URL is
        # the only cure, and the caller's next run gets one.
        logger.error(
            "recovery fetch PERMANENTLY failed (HTTP %s) for %s — %s",
            exc.code,
            fresh_url[:80],
            exc.diagnosis(),
        )
        return False
    if downloaded is None:
        return False
    data, content_type = downloaded

    # The temp file must carry the media's real extension: `seed_media_from_file`
    # derives content_type from the suffix, and a bare `.tmp` would be recorded as
    # application/octet-stream, losing the image/video distinction downstream.
    base_type = content_type.split(";")[0].strip().lower()
    ext = _EXT_BY_MIME.get(base_type, ".bin")
    tmp_dir = media_dir or POST_MEDIA_DIR
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp = tmp_dir / f".recover-{uuid.uuid4().hex}{ext}"
    try:
        _atomic_write(tmp, data)
        seeded = seed_media_from_file(ops, stored_url, tmp, media_dir=media_dir)
    finally:
        tmp.unlink(missing_ok=True)

    if seeded is None:
        return False

    # Proving the STORED url resolves is the only evidence that matters: the
    # caller's whole purpose is making the post enrichable, and a write that
    # landed under some other key would report success while achieving nothing.
    return local_media_path(ops, stored_url) is not None


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
