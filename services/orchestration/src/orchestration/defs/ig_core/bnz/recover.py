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
The stored URL is the durable reference and the fresh URL is disposable
transport, so bytes must land where enrichment will look: under the key derived
from the URL silver holds.

Since the cache key became the STABLE media id (``mid:<id>``), the stored and
fresh URLs of one media resolve to the SAME key — they are different strings but
the same media. That equivalence is a trap as well as the point:

  * It is the POINT, because enrichment's lookup needs the stored URL's key and
    a fresh fetch can now be keyed directly, without reconstructing which expired
    URL it once corresponded to.
  * It is a TRAP, because any code that looks up the fresh URL in the cache
    before downloading will find the STORED entry's file — and on a partially
    cached carousel that is a DIFFERENT item's bytes. Seeding from it is a
    mispair: silently wrong, with no miss to trigger a re-fetch.

So the download here is unconditional (:func:`_cache_under`) and the write is
verified against the stored URL before it counts as success.

This is not the "regenerated URL is a cache miss" case (WATCHDOG.md) — that rule
describes a NEW scrape minting a new URL for a post that was never cached.

NOT A ONE-OFF SCRIPT
--------------------
Recovery is a standing mechanism, not a migration: the ~4.5-day window makes media
loss continuous, so every future scrape can lose the same way. This is a bronze
producer that takes a set of permalinks, so it serves the current backlog and any
future one.

A STANDING MECHANISM MUST REMEMBER
----------------------------------
Because it runs repeatedly, a failure verdict that is only logged is a verdict
forgotten: an unrecoverable post (deleted or private, so no media comes back) would
be re-selected and RE-PAID on every pass. Verdicts are therefore persisted in
``opsdb.media_recovery`` and the candidate scan excludes them.
"""

from __future__ import annotations

import json
import logging
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from dagster import asset
from dagster_duckdb import DuckDBResource
from opsdb.media_cache import cache_keys_for
from opsdb.media_recovery import (
    UNRECOVERABLE_APIFY_ERRORS,
    UNRECOVERABLE_POST_GONE,
    exhausted_post_ids,
    record_exhausted,
)

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


#: Posts per actor run. The actor takes a LIST of direct URLs, so one run recovers
#: this many posts instead of one. Each run carries its own startup, polling and
#: scheduling overhead, and the measured ~8s/post was dominated by that overhead
#: rather than by transferring the media — so batching is the main lever on the
#: pass's wall clock.
#:
#: Sized well under the actor's practical limits: a batch that is too large risks
#: the whole chunk failing together, and the per-post cost is unchanged either way
#: (the actor bills per result, not per run).
POSTS_PER_RECOVERY_RUN = 20


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

    # Posts a prior run proved unrecoverable. Excluding them here is what makes
    # the mechanism's memory real: without it every deleted/private post is
    # re-listed and RE-PAID on every run, because no future fetch will ever
    # succeed for it.
    exhausted = exhausted_post_ids(ops)

    candidates: list[tuple[str, str, list[str], list[str]]] = []
    for post_id, permalink, media_files in rows:
        if post_id in exhausted:
            continue
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


def _all_items(ndjson_path: Path) -> list[dict]:
    """Parse EVERY item from a streamed NDJSON file.

    Same separator rule as :func:`_first_item` (``"\\n"`` only — see there for why
    ``splitlines`` corrupts a real caption), but a batched run returns many items
    and each must reach its own post.

    A malformed line is skipped rather than aborting the batch: one bad item must
    not discard the media recovered for every other post in the chunk.
    """
    text = ndjson_path.read_text(encoding="utf-8")
    items: list[dict] = []
    for line in text.split("\n"):
        if not line.strip():
            continue
        try:
            items.append(json.loads(line))
        except ValueError:
            logger.warning("media recovery: skipping unparseable NDJSON line")
    return items


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


def _cache_item(
    ops: SQLiteResource,
    *,
    post_id: str,
    stored: list[str],
    item: dict,
    media_dir: Path | None = None,
) -> tuple[int, str | None]:
    """Cache one post's media from an already-fetched Apify item.

    Returns ``(files_cached, error)`` — error is None on success.

    `stored` is the post's FULL ``media_files`` list, not just the uncached subset:
    the fresh item mirrors it 1:1 by index, so pairing must use the same indices.
    URLs already cached are skipped inside the loop, which makes a re-run of a
    partially-recovered post a no-op.

    The pairing rule: ``childPosts[i]`` corresponds to ``stored[i]`` (verified on a
    7-URL carousel, 1:1 in order). A count mismatch aborts the post rather than
    guessing. Cached bytes are keyed by the STORED url's key, because that is what
    ``media_urls_to_local_paths`` — and therefore enrichment — resolves.
    """
    fresh = _fresh_media_for_post(item)
    if not fresh:
        # The item carries no media. WHY matters, because the verdict is one-way:
        # an Apify ERROR item has this shape, and its reason decides whether the
        # post is gone or merely restricted right now.
        error = str(item.get("error") or "")
        description = str(item.get("errorDescription") or "")
        if error in UNRECOVERABLE_APIFY_ERRORS:
            record_exhausted(ops, post_id, UNRECOVERABLE_POST_GONE)
            return 0, f"Apify reports the post is gone ({error}): {description}"
        # Anything else — 'restricted_page' ("Restricted access, only partial data
        # available"), a rate limit, an unrecognized error shape — is NOT permanent.
        # Recording it would permanently exclude a post a later run could recover,
        # which is worse than a failed attempt: nothing ever revisits it.
        return 0, (
            f"Apify returned no media and no permanent verdict ({error or 'no error field'}"
            f"): {description or 'item carried no media'}"
        )

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


def _shortcode(permalink: str) -> str:
    """The post's shortcode — Instagram's per-post identifier in the permalink.

    Used to attribute a batched fetch's items back to the post that asked for
    them: a multi-URL run returns items in no guaranteed order, so matching by
    position would silently cache one post's media under another's keys.

    Reduced defensively rather than by a bare ``rsplit``. A naive split returns
    ``?utm_source=ig_web`` for a query-suffixed permalink and ``CBL8httj7aK`` for a
    clean one, so the two keys would not match — every item in the chunk would drop
    as unmatched, and the post would be re-paid on every run while appearing in no
    report. Measured today: all 10,038 silver urls are plain ``/p/<code>/`` with no
    query strings, so this is a guard against a shape change, not a live bug.

    Accepts a full permalink, a path, or a bare code, in all cases returning the
    trailing path segment with any query/fragment stripped.
    """
    # Strip query/fragment FIRST: a bare code has no "/", so splitting on the path
    # separator before removing the query would return a fragment of the query.
    code = permalink.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    return code.rsplit("/", 1)[-1]


def _item_shortcode(item: dict) -> str:
    """The shortcode of a fetched item, reduced the SAME way as a candidate's.

    `shortCode` is the actor's own identity for the item, but it is passed through
    `_shortcode` rather than returned raw so that BOTH sides of attribution reduce
    by identical rules. If an item ever arrived with a query-suffixed or
    path-prefixed `shortCode`, a raw return would stop matching the candidate key
    (`_shortcode(candidate["url"])`) and every item in the chunk would drop as
    unmatched — the post would then be re-paid on every run, invisibly.

    The URL is the fallback, so a schema change that drops `shortCode` cannot
    break attribution either.
    """
    code = item.get("shortCode")
    if isinstance(code, str) and code:
        return _shortcode(code)
    return _shortcode(str(item.get("url") or ""))


def recover_batch(
    ops: SQLiteResource,
    apify: ApifyResource,
    *,
    candidates: list[dict],
    media_dir: Path | None = None,
    per_run: int = POSTS_PER_RECOVERY_RUN,
) -> tuple[int, int, list[str]]:
    """Recover many posts with FAR fewer actor runs. Returns (recovered, cached, failed_permalinks).

    ONE run takes a LIST of direct URLs, so N posts cost one actor run rather than
    N. That matters: each run carries its own startup, polling and scheduling
    overhead, and the per-post cost measured at ~8s was dominated by that overhead,
    not by the media transfer.

    Items come back in no guaranteed order, so each is attributed to its post by
    SHORTCODE (parsed from the item's ``url``). A post whose item never arrives is a
    failure for that post only — a batch never loses the others.

    Reliability: a single dead permalink inside a batch does not fail the batch.
    (Measured: a run over a a mixed list returns items for the resolvable ones.)
    """
    recovered = cached_total = 0
    failed: list[str] = []
    consecutive_failed_runs = 0

    for start in range(0, len(candidates), per_run):
        chunk = candidates[start : start + per_run]
        # BOTH sides are reduced through `_shortcode`, so candidate keying and item
        # keying cannot drift apart. Measured today: every silver url is a plain
        # `/p/<code>/` (10,038 of 10,038, zero query strings, zero duplicates), so
        # the two agree. Keying the candidate side naively while the item side
        # prefers `shortCode` would make them agree only for that one shape — a
        # future `/reel/<code>/` permalink would drop every item as unmatched and
        # re-pay the post forever, invisibly.
        by_code = {_shortcode(c["url"]): c for c in chunk}
        if len(by_code) != len(chunk):
            # A collapsed duplicate would silently remove a candidate from `seen`
            # AND from `failed`, so it would vanish from the report entirely.
            # Cannot happen while permalinks are distinct, but say so rather than
            # letting it be discovered as missing spend.
            logger.error(
                "media recovery: %d candidate(s) collapsed to %d shortcode key(s) "
                "in a chunk — duplicates would be invisible in this run's report",
                len(chunk),
                len(by_code),
            )

        run = trigger_run(
            RECOVERY_ACTOR,
            [c["url"] for c in chunk],
            token=apify.token,
            results_limit=len(chunk),
            results_type="posts",
            # Scale the cap with the number of posts: the per-post cap would
            # otherwise stop a multi-post run early and silently truncate it.
            max_charge_usd=RECOVERY_CHARGE_CAP_USD * len(chunk),
        )
        outcome = poll_run(run.run_id, token=apify.token, timeout=1800, settle_cost=False)

        tmp = tempfile.NamedTemporaryFile(prefix="_recover_batch_", suffix=".ndjson", delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        try:
            count = stream_dataset(outcome.dataset_id, tmp_path, token=apify.token)
            items = _all_items(tmp_path) if count else []
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        seen: set[str] = set()
        chunk_recovered = 0
        for item in items:
            code = _item_shortcode(item)
            cand = by_code.get(code)
            if cand is None:
                # LOUD, because this is a paid item that reached no post. A silent
                # drop here (an Apify URL shape `_shortcode` cannot parse — a
                # trailing query, a /reel/ redirect) means the post is retried and
                # RE-PAID on every run without ever being recorded or reported.
                # The log line names both, so the shape is diagnosable.
                logger.error(
                    "media recovery: fetched item matched no candidate "
                    "(shortcode %r, url %r) — its media was paid for and discarded",
                    code,
                    str(item.get("url") or "")[:120],
                )
                continue
            seen.add(code)
            try:
                n, err = _cache_item(
                    ops,
                    post_id=cand["post_id"],
                    stored=cand["stored"],
                    item=item,
                    media_dir=media_dir,
                )
            except Exception as exc:  # noqa: BLE001 — one bad post must not end the pass
                # The actor run is already paid for, and this loop may be 100 posts
                # in. `_cache_under` catches _PermanentFetchError specifically, so
                # anything else (timeout, TLS, unexpected HTTP, a schema surprise)
                # would otherwise propagate out of the batch and ABANDON the rest
                # of the backlog — after spending on the whole run, with no `failed`
                # entry for this post. Contained here so the failure is per-post.
                #
                # The exception type is surfaced in the message: a systematic cause
                # (every post raising the same class) must still be diagnosable.
                failed.append(cand["url"])
                logger.exception(
                    "media recovery: unexpected error for %s (%s) — contained; "
                    "the rest of the batch continues",
                    cand["url"],
                    type(exc).__name__,
                )
                continue
            if err is None:
                recovered += 1
                chunk_recovered += 1
                cached_total += n
            else:
                failed.append(cand["url"])
                logger.error("media recovery FAILED for %s: %s", cand["url"], err)

        # A post in the chunk whose item never came back: one fetch failed, but
        # the batch still delivered the rest. Recorded per post, not as a batch loss.
        for code, cand in by_code.items():
            if code not in seen:
                failed.append(cand["url"])
                logger.error(
                    "media recovery: no item returned for %s (deleted/private, "
                    "or filtered by Apify)",
                    cand["url"],
                )

        # BREAKER — a whole chunk returning nothing means the fetch itself is
        # broken (auth, quota, schema change), not that these particular posts are
        # gone. Measured cost of continuing would be the entire remaining budget
        # producing nothing, so stop loudly instead.
        if chunk_recovered == 0:
            consecutive_failed_runs += 1
            if consecutive_failed_runs >= MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    "media recovery: %d consecutive empty runs — stopping rather "
                    "than spending the remaining budget on a broken fetch",
                    consecutive_failed_runs,
                )
                break
        else:
            consecutive_failed_runs = 0
    return recovered, cached_total, failed


def recover_one(
    ops: SQLiteResource,
    apify: ApifyResource,
    *,
    post_id: str,
    permalink: str,
    stored: list[str],
    media_dir: Path | None = None,
) -> tuple[int, str | None]:
    """Fetch ONE post by permalink and cache its media. Returns (files_cached, error).

    Kept for a single-post probe or an explicit retry; the standing pass uses
    :func:`recover_batch`, which costs one actor run per batch instead of one per
    post.
    """
    run = trigger_run(
        RECOVERY_ACTOR,
        [permalink],
        token=apify.token,
        results_limit=1,
        results_type="posts",
        max_charge_usd=RECOVERY_CHARGE_CAP_USD,
    )
    outcome = poll_run(run.run_id, token=apify.token, timeout=600, settle_cost=False)

    tmp = tempfile.NamedTemporaryFile(
        prefix=f"_recover_{post_id}_", suffix=".ndjson", delete=False
    )
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        count = stream_dataset(outcome.dataset_id, tmp_path, token=apify.token)
        if count == 0:
            return 0, "Apify returned no item for the permalink"
        item = _first_item(tmp_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return _cache_item(
        ops, post_id=post_id, stored=stored, item=item, media_dir=media_dir
    )


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
        "media recovery: %d post(s) missing media, est. cost $%.2f at $0.0023/post, "
        "%d actor run(s) at %d URLs each",
        len(candidates),
        len(candidates) * 0.0023,
        -(-len(candidates) // POSTS_PER_RECOVERY_RUN),
        POSTS_PER_RECOVERY_RUN,
    )

    recovered, urls_cached, failed = recover_batch(
        ops, apify, candidates=candidates
    )
    report.attempted = len(candidates)
    report.recovered = recovered
    report.urls_cached = urls_cached
    report.failed = len(failed)
    report.failed_permalinks = failed

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
