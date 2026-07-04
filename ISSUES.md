# Issues & deferred work

Issue tracking is local — this file, not GitHub Issues.

## Resolved

### 1. Comprehensive medallion testing strategy ✅ (2026-07-01)

Resolved by test hardening plan (`tasks/plans/test-hardening.md`). 87 tests across
unit, integration, E2E layers. Full pipeline coverage: bronze→silver→gold→serving.

### 2. End-to-end operational test coverage gaps ✅ (2026-07-01)

All E2E definition-of-done items complete:
- `tests/e2e/test_full_pipeline.py` — full pipeline on tmp_path + :memory: DuckDB
- Watermark chain verified (silver_ig → gold_ig cascade)
- Cross-layer post_id audit (every bronze post_id traceable through all layers)
- Dead_letter routing (empty caption + API failure paths)
- Schedule validation (`weekly_medallion` loads, targets match asset keys)
- Ad-hoc run sequence verified
- Golden-dataset snapshot (`tests/e2e/test_snapshot.py` + `tests/data/bronze_sample.parquet`)

### 3. State readiness validation layer ✅ (2026-07-01)

Resolved by `tasks/plans/state-readiness-impl.md`. Schema contract catalog
(`tests/operational/expected_schema.py`) with 6 tables + 1 view, 8 state
readiness tests, absent-DB handling. Drift detection proven against missing
column, type mismatch, and missing table scenarios.

## Active

### 4. S3 / R2 storage backend for GitHub Actions


### 5. Investigate null engagement data in silver_ig_posts

**Observed:** 365 out of 2,628 rows (13.9%) in `silver_ig_posts` have NULL values
for `likes_count`, `comments_count`, `owner_username`, `timestamp`, and several
other columns simultaneously. These rows all have `hashtags=[]`, `media_files=[]`,
`media_count=0`, and `has_engagement_bait=False` — they appear to be failed or
incomplete Apify scraper results that were not filtered out.

**Dataset breakdown (null rows per source):**
```
source_dataset         null_rows    total    null_pct
o44ZGN3WOEuMzCgcf      365         365      100.0%
(all other datasets)     0       2,263        0.0%
```

The entire `o44ZGN3WOEuMzCgcf` dataset (365 rows) is all nulls — every single
row. This strongly suggests a systemic problem with that specific Apify actor run,
not one-off scraper failures. Likely causes:
- Apify actor returned a different response shape (profile-only, no post data)
- Rate limiting produced empty pages for every profile in that batch
- Actor config changed between runs (missing `resultsType=posts` or similar)

**Suggested fix:** Investigate what `o44ZGN3WOEuMzCgcf` was scraping vs other
datasets. If it was a profile-list scrape vs individual posts, the silver asset
may need to handle both shapes. Alternatively, filter rows where `likes_count IS
NULL` AND `owner_id IS NULL` at the silver layer and route to dead_letter.

### 6. Dead letter backlog from Gemini rate limiting

**Observed:** 10 rows in ``dead_letter``:
- 4 ``status=skipped`` — "Empty caption" (legitimate — profile pages without posts)
- 6 ``status=pending`` — "429 RESOURCE_EXHAUSTED" (Gemini API rate limiting)

**Diagnosis (2026-07-01):** The 429 error type is ambiguous — could be
``rate_limit_exceeded`` (RPM/TPM burst, fix with jitter) or
``insufficient_quota`` (daily RPD exhausted, must wait until 08:00 UTC).
Without the error subtype in the dead_letter message, we can't tell which.

**Code fix applied:** ``ig_posts_gld`` retry loop now uses jittered backoff
(``(2^N) + random(0,1)`` seconds instead of deterministic ``2^N``) and
classifies 429 errors via ``_is_quota_exhausted()`` / ``_is_rate_limited()``
helpers. Quota exhaustion stops retries immediately; rate limits retry with
jitter. See ``AGENTS.md#gemini-api-rate-limits`` for full context.

**Impact:** ``gold_ig_analyses`` has 0 rows — no enrichment happened. The
pending dead_letter entries will need re-processing once rate limits reset or
a paid tier project is available. The skipped entries should be reviewed: are
empty-caption profile URLs expected data, or should they be filtered earlier
in the pipeline?

**Next step:** Re-run the gold asset when rate limits allow and observe
whether the improved retry loop resolves the 429s. If errors persist, check
the error subtype in the updated dead_letter messages to distinguish burst
from quota exhaustion.

### 7. Multimodal processing — video, image, text enrichment

The enrichment worker currently discards `lookup_or_upload` return values — file
URIs never reach `gemini.analyze()`. All 56 gold_analyses rows are text-only.
This tracks the full implementation: media download, Gemini File API upload with
state polling, cache with TTL, tier gating (FREE skips video), and token budget
checks. Plan: `tasks/plans/multimodal-processing.md`.

The failing `test_worker_passes_media_uri_to_gemini` is the canary — once media
URIs flow through the worker, it passes.

#### Phase 1: Foundation

- [x] `GeminiResource.analyze()` accepts optional `media_files: list[MediaFile]` parameter using a `MediaFile` TypedDict `{uri: str, mime_type: str}`
- [x] `analyze()` constructs `contents = [Part.from_uri(file_uri=mf["uri"], mime_type=mf["mime_type"]), Part.from_text(text=prompt)]` when media_files provided
- [x] `analyze()` wires `media_resolution='low'` on `GenerateContentConfig` when media_files present
- [x] `test_worker_passes_media_uri_to_gemini` rewritten: captures `analyze()` kwargs, asserts `media_files` contains the URI

#### Phase 2: Media pipeline

- [x] `lookup_or_upload` renamed to `lookup_or_upload_all`, returns `list[MediaFile]` instead of `str | None`
- [x] `lookup_or_upload_all` downloads URLs to temp files before `client.files.upload(file=<local_path>)`
- [x] `lookup_or_upload_all` polls `file.state == ACTIVE` after upload (30s timeout, 2s interval)
- [x] `lookup_or_upload_all` stores `expires_at` column in media_metadata (now + 24h); cache hits check expiry
- [x] `lookup_or_upload_all` processes ALL URLs in media_files (deduplicated), not just first
- [x] `lookup_or_upload_all` returns stored `mime_type` from media_metadata on cache hit
- [x] `lookup_or_upload_all` uses INSERT OR IGNORE with placeholder row to prevent TOCTOU duplicate uploads
- [x] `lookup_or_upload_all` adds INFO logging for cache hits, cache misses, upload start/complete
- [x] Schema migration: add `expires_at TEXT` column to `media_metadata` table (IF NOT EXISTS)
- [x] Unit tests: cache hit, cache miss, expiry, dedup, polling, ACTIVE state transition

#### Phase 3: Worker integration

- [x] `process_item()` captures media_files from `lookup_or_upload_all` and passes to `gemini.analyze()`
- [x] `process_item()` gates video processing on tier: FREE tier skips video (text-only), Tier 1+ processes
- [x] `process_item()` estimates tokens from `video_metadata.duration_seconds` and skips video if over tier budget
- [x] `process_item()` classifies File API errors separately from generation errors (no batch abort on upload 429s)
- [x] `IG_GOLD_PROMPT` updated: "Analyze the Instagram post and any attached media below..."
- [x] Integration tests: text-only, single video, multiple files, free tier skip, upload failure, token budget skip

#### Phase 4: Cleanup

- [x] Existing 20 tests still pass (106 total, 2 multimodal pass)
- [x] Ruff zero warnings on changed files
- [x] Logging + cost tracking per batch

### 8. Instagram CDN media URLs expire — profile pics and thumbnails

**Observed (2026-07-03):** The dashboard needs real Instagram profile pictures and
post thumbnails. These are available in Apify bronze data (``profilePicUrlHD``,
``profilePicUrl``, ``displayUrl``), but Instagram CDN URLs
(``scontent-*.cdninstagram.com``) contain expiry parameters (``&oe=`` timestamp).
Once they expire, the cached URLs return 403. All existing bronze CDN URLs
expired on 2026-06-29 (~4 days ago as of 2026-07-03).

**Investigation findings (2026-07-03):**

*Post thumbnails — runtime solution found:*
- ``https://www.instagram.com/p/{shortcode}/media/?size=m`` returns a 302
  redirect to a **fresh** CDN URL valid for ~4-5 days.
- Standard ``urllib`` with browser User-Agent + Referer header fetches image
  bytes directly (follows the redirect). 50/50 success at 4 req/sec, zero rate
  limits. No authentication, no ``curl_cffi``, no pipeline dependency.
- Thumbnails can be fetched on demand by the dashboard server, not just at
  pipeline time. Works for any post shortcode at any time.

*Profile pics — pipeline-time download still required:*
- ``i.instagram.com/api/v1/users/web_profile_info/`` returns profile data
  including ``profile_pic_url``, but requires TLS fingerprinting via
  ``curl_cffi`` (Python's ``urllib``/``requests`` are blocked at the TLS
  handshake level). Even with ``curl_cffi``, Instagram enforces ~200 req/hr
  per IP. For 401 profiles that's a ~2-hour trickle — doable but adds a
  dependency and maintenance burden (doc_id rotates every 2-4 weeks).
- ``unavatar.io/instagram/{username}`` — third-party avatar proxy, tested
  and returns 403 for all usernames (service appears non-functional).
- Recommendation: download profile pics from Apify's fresh CDN URLs at
  pipeline time, store to filesystem, serve from FastAPI. Apify handles the
  anti-bot infrastructure. Profile pics change monthly at most — pipeline
  cadence covers it. DiceBear fallback for uncached profiles.

*Redis is a dead end for this problem:*
- The ``lakehouse-redis`` container caches CDN **URLs**, not bytes. All 2,264
  cached URLs have expired ``oe=`` timestamps and return 403.
- URL caching cannot solve the CDN expiry problem — the bytes must be
  downloaded and stored locally. Redis should be removed from the media path.

*Dashboard server.py issues:*
- ``_fetch_og_image`` scrapes Instagram pages for ``og:image`` — dead code.
  Instagram no longer serves og:image meta tags to unauthenticated requests.
- ``instagram_media_cache`` SQLite table has only 28 rows, all DiceBear
  fallbacks — no real images were ever cached through this path.
- ``scripts/cache_instagram_media.py`` (Playwright-based) is redundant —
  Instagram's bot detection blocks headless browsers from rendering post pages.

**Resolved by:** Issue 9 (media cache architecture) and Issue 10 (multi-entity
Apify scraper shapes for profile data extraction).

### 9. Media cache — permanent local image storage

**Intent:** Store Instagram thumbnails and profile pictures as permanent local
files so the dashboard never depends on Instagram CDN availability or URL
expiry. Images persist indefinitely once fetched — no TTL, no re-scrape, no
external service dependency at view time.

**Design:**

*Storage layout:*
- Filesystem: ``data/media/thumbnails/{shortcode}.jpg``,
  ``data/media/avatars/{username}.jpg``
- SQLite metadata table ``media_cache`` in ``ops.sqlite``::

    CREATE TABLE IF NOT EXISTS media_cache (
        cache_key    TEXT PRIMARY KEY,  -- "thumb:{shortcode}" or "avatar:{username}"
        local_path   TEXT NOT NULL,     -- relative path within data/media/
        content_type TEXT NOT NULL,     -- image/jpeg, image/webp, etc.
        size_bytes   INTEGER NOT NULL,
        fetched_at   TEXT NOT NULL,     -- ISO timestamp
        source_url   TEXT               -- original CDN URL (for debugging)
    )

- Estimated footprint: ~2,400 thumbnails × ~30KB + ~400 avatars × ~50KB ≈ 92MB

*Thumbnail population (dashboard runtime):*
- Dashboard server fetches on first request via Instagram's public
  ``/p/{shortcode}/media/?size=m`` endpoint (issues a 302 → fresh CDN URL →
  image bytes).
- Server writes bytes to ``data/media/thumbnails/{shortcode}.jpg``, inserts
  metadata row in ``media_cache``, serves via ``FileResponse``.
- Subsequent requests hit disk directly — zero Instagram calls.
- The Instagram endpoint generates a fresh CDN URL on every request, so
  there's no race against URL expiry.

*Avatar population (pipeline time):*
- During silver processing (or a dedicated downstream asset), read
  ``profilePicUrlHD`` from bronze Parquet, download bytes, write to
  ``data/media/avatars/{owner_username}.jpg``.
- Apify provides fresh CDN URLs at scrape time — download must happen
  immediately, in the same pipeline run.
- ``dim_profile`` should carry a ``profile_pic_path`` column pointing to the
  local file so the dashboard joins directly.
- Dashboard fallback: DiceBear identicon when no local file exists.

*Dashboard server changes:*
- Replace current ``/api/media/thumbnail/{shortcode}``: check disk →
  fetch from Instagram ``/media/`` endpoint → cache → serve.
- Replace current ``/api/media/avatar/{username}``: check disk →
  serve or DiceBear fallback.
- Use FastAPI ``FileResponse`` — no Redis, no external CDN dependency.
- Remove ``_fetch_og_image``, ``instagram_media_cache`` SQLite table,
  ``_ensure_media_cache``, ``_get_cached_media``, ``_cache_media``.

*Cleanup:*
- Remove ``lakehouse-redis`` Docker container and all Redis references.
- Delete or archive ``scripts/cache_instagram_media.py`` (Playwright scraper —
  Instagram no longer serves crawlable image URLs).

*Revert half-applied pipeline changes:*
- The previous session added ``profile_pic_url`` and ``display_url`` to the
  silver DDL and ``_BRONZE_TO_SILVER`` mapping in ``assets.py``, but NOT to
  the canonical schema catalog (``schemas.py``). The actual DuckDB table
  doesn't have these columns (no migration was run). This is schema drift.
- Revert the DDL change (remove ``profile_pic_url``/``display_url``, restore
  ``meta_data``/``has_engagement_bait``) and remove the two lines from
  ``_BRONZE_TO_SILVER``. Silver doesn't need CDN URL columns — images are
  downloaded to filesystem.

### 10. Multi-entity Apify scraper shapes — entity-aware bronze → silver routing

**Observed:** The Apify Instagram scraper outputs different JSON shapes
depending on ``ScrapeConfig.results_type`` (``"posts"``, ``"comments"``,
``"details"``, etc.). The current pipeline treats all bronze rows as posts,
which produces null-filled garbage when non-post data arrives. The
``o44ZGN3WOEuMzCgcf`` dataset (365 rows, all nulls in silver, issue #5)
is the canonical example — likely a ``results_type="details"`` or profile-only
scrape that produced zero post-shaped rows.

The bronze ``.meta`` sidecar already records the scrape configuration:

    {
        "input": {
            "results_type": "posts",
            "results_limit": 12,
            "urls": ["..."]
        }
    }

**What needs to happen:**

*Entity-aware routing at the silver layer:*
- ``ig_posts_slv`` should read ``results_type`` from the ``.meta`` sidecar
  and skip bronze files where ``results_type != "posts"``. This prevents
  null rows from profile-only or comment-only scrapes.
- New assets for non-post entity types (deferred — out of scope for this
  branch):
  - ``ig_profiles_slv`` — ``results_type="details"`` → profile metadata
    (owner_id, owner_username, full_name, biography, followers_count,
    follows_count, posts_count, is_business, is_verified, profile_pic_url,
    external_url).
  - ``ig_comments_slv`` — ``results_type="comments"`` → comment data
    (comment_id, post_id, post_shortcode, text, owner_username, owner_id,
    likes_count, timestamp, reply_to_id).

*``ig_profiles_slv`` schema (indicative — verify against real Apify output):*

    CREATE TABLE ig_profiles_slv (
        owner_id         TEXT PRIMARY KEY,
        owner_username   TEXT NOT NULL,
        full_name        TEXT,
        biography        TEXT,
        followers_count  INTEGER,
        follows_count    INTEGER,
        posts_count      INTEGER,
        is_business      BOOLEAN,
        is_verified      BOOLEAN,
        profile_pic_url  TEXT,        -- CDN URL captured at scrape time
        external_url     TEXT,
        source_dataset   TEXT NOT NULL,
        processed_on     TIMESTAMP
    )

*``ig_comments_slv`` schema (indicative — verify against real Apify output):*

    CREATE TABLE ig_comments_slv (
        comment_id       TEXT PRIMARY KEY,
        post_id          TEXT NOT NULL,
        post_shortcode   TEXT,
        text             TEXT,
        owner_username   TEXT,
        owner_id         TEXT,
        likes_count      INTEGER,
        timestamp        TIMESTAMP,
        reply_to_id      TEXT,
        source_dataset   TEXT NOT NULL,
        processed_on     TIMESTAMP
    )

*``dim_profile`` update:*
- Add ``profile_pic_path TEXT`` column — points to
  ``data/media/avatars/{owner_username}.jpg``.
- Populated by a downstream asset that reads ``ig_profiles_slv`` and writes
  the local avatar path into the dimension table.

*ScrapeConfig hardening:*
- ``results_type`` currently defaults to ``"posts"``. The Apify actor
  supports ``"posts"``, ``"comments"``, ``"details"``, and possibly others.
  Add an ``Enum`` validation so mismatched types are caught at config time
  rather than producing silent null rows.

*Implementation strategy:*
- This issue is multi-branch — the immediate need (this branch) is only the
  ``ig_posts_slv`` guard: skip bronze files where ``results_type != "posts"``.
- ``ig_profiles_slv`` and ``ig_comments_slv`` are deferred to follow-up
  branches. Create stub assets that log "not yet implemented" if they
  encounter non-post data, so the DAG doesn't silently drop rows.