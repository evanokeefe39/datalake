# Issues & deferred work

Issue tracking is local — this file, not GitHub Issues.

## Complete — `feat/media-and-entity-routing` (2026-08-12)

### 8, 9, 10: Media cache + entity-aware bronze routing

**Branch:** `feat/media-and-entity-routing`
**Plan:** `tasks/plans/media-and-entity-routing.md`
**Status:** Complete (2026-08-12) — merged, see PR

Resolves three issues:
- **#8** — Instagram CDN media URLs expire (profile pics and thumbnails)
- **#9** — Media cache: permanent local image storage
- **#10** — Multi-entity Apify scraper shapes: entity-aware bronze → silver routing

Full design and behavioral contracts in plan. User stories below map to
acceptance criteria and tests.

## User Stories (`feat/media-and-entity-routing`)

Each story has acceptance criteria. Every criterion maps to a test.
Stories are numbered for traceability in commit messages and test names.

### US-01: Entity classifier identifies bronze dataset type

**As a** pipeline operator
**I want** bronze ingestion to identify whether a Parquet file contains posts,
profiles, or comments
**so that** each entity type routes to the correct silver table instead of
producing null-filled garbage rows.

*Acceptance criteria:*
- [ ] `_classify_bronze(df, meta_path)` returns "posts" when columns
      `id`+`shortCode`+`caption` are present
- [ ] Returns "details" when `biography`+`followersCount` are present and
      `id`+`shortCode` are absent
- [ ] Returns "comments" when `commentId` is present
- [ ] Returns "unknown" for unrecognized schemas
- [ ] Meta sidecar `input.results_type` takes priority over schema-sniffing
      when present (falls back when absent or missing the field)
- [ ] Empty (0-row) Parquet returns "unknown"

*Tests:* `test_classifier_posts`, `test_classifier_details`,
`test_classifier_comments`, `test_classifier_unknown`,
`test_classifier_meta_priority`, `test_classifier_empty_file`

### US-02: Silver posts asset skips non-post bronze files

**As a** pipeline operator
**I want** `ig_posts_slv` to skip bronze files that don't contain post-shaped
data
**so that** `silver_ig_posts` never receives profile or comment rows (the
`o44ZGN3WOEuMzCgcf` 365-null-row problem doesn't recur).

*Acceptance criteria:*
- [ ] Post-shaped bronze files load into `silver_ig_posts` as before
- [ ] Non-post files are skipped with an INFO log including file name and
      detected entity type
- [ ] The `o44ZGN3WOEuMzCgcf` file (all-null rows) is classified and skipped
- [ ] Existing dedup/upsert behavior for real post data is unchanged

*Tests:* `test_slv_skips_profile_bronze`, `test_slv_skips_comment_bronze`,
`test_slv_processes_post_bronze`, `test_slv_skips_o44_dataset`

### US-03: Profile silver table from details-type scrapes

**As a** data analyst
**I want** profile metadata from `results_type="details"` scrapes stored in a
`silver_ig_profiles` DuckDB table
**so that** `dim_profile` can use real biography, follower counts, and profile
picture URLs instead of the minimal data available from post scrapes.

*Acceptance criteria:*
- [ ] `silver_ig_profiles` table created with `owner_id` TEXT PRIMARY KEY
- [ ] Columns: `owner_username`, `full_name`, `biography`, `followers_count`,
      `follows_count`, `posts_count`, `is_business`, `is_verified`,
      `profile_pic_url`, `external_url`, `source_dataset`, `processed_on`
- [ ] `ig_profiles_slv` asset reads details-type bronze files, renames columns
      (camelCase → snake_case), upserts via INSERT OR REPLACE
- [ ] Downloads `profilePicUrlHD` bytes to `data/media/avatars/{username}.jpg`
      during processing (CDN URLs expire in ~4-5 days)
- [ ] Inserts `media_cache` row for downloaded avatar
- [ ] Uses own watermark (`name = 'profiles_ig'`) for incremental processing
- [ ] `group_name="instagram"`, `deps=["ig_posts_raw"]`

*Tests:* `test_profiles_slv_upsert`, `test_profiles_slv_incremental`,
`test_profiles_slv_no_bronze`, `test_profiles_slv_avatar_download`,
`test_profiles_slv_column_mapping`

### US-04: Comment silver stub

**As a** pipeline operator
**I want** the DAG to recognize comment-type bronze files instead of silently
dropping rows
**so that** when comment scrapes arrive, there's a registered asset to extend
rather than an invisible data path.

*Acceptance criteria:*
- [ ] `silver_ig_comments` table created with `comment_id` TEXT PRIMARY KEY,
      `post_id`, `post_shortcode`, `text`, `owner_username`, `owner_id`,
      `likes_count`, `timestamp`, `reply_to_id`, `source_dataset`, `processed_on`
- [ ] `ig_comments_slv` asset registered with `group_name="instagram"`,
      `deps=["ig_posts_raw"]`
- [ ] Asset logs "not yet implemented" at WARNING level and returns empty
      DataFrame (no-op)
- [ ] Does not read bronze files (no data exists to model against)

*Tests:* `test_comments_slv_returns_empty`, `test_comments_slv_logs_warning`

### US-05: Profile picture path in dim_profile

**As a** dashboard user
**I want** `dim_profile` to carry a `profile_pic_path` column
**so that** the dashboard can serve real profile pictures from disk without
depending on Instagram CDN URLs.

*Acceptance criteria:*
- [ ] `dim_profile` column `profile_pic_path TEXT` added (NULL-able)
- [ ] Column is NULL for existing rows until repopulated
- [ ] Migration via `ALTER TABLE … ADD COLUMN IF NOT EXISTS` with try-except
      (DuckDB doesn't support `IF NOT EXISTS` on ALTER TABLE)
- [ ] `schemas.py` catalog updated with the new column
- [ ] State readiness test passes

*Tests:* `test_dim_profile_has_pic_path_column`,
`test_dim_profile_pic_path_nullable`

### US-06: Thumbnail byte-cache endpoint

**As a** dashboard user
**I want** `/api/media/thumbnail/{shortcode}` to return real post thumbnail images
**so that** the dashboard shows actual Instagram post thumbnails.

*Acceptance criteria:*
- [ ] First request: fetches `https://www.instagram.com/p/{shortcode}/media/?size=m`,
      follows 302, writes bytes to `data/media/thumbnails/{shortcode}.jpg`,
      inserts `media_cache` row, returns `FileResponse` with correct Content-Type
- [ ] Subsequent requests: serves from disk, zero Instagram API calls
- [ ] Instagram returns 404 or non-200: returns HTTP 404 to frontend
- [ ] Instagram returns non-image Content-Type: does not cache, returns 404
- [ ] Empty response body: does not cache, returns 404
- [ ] `data/media/thumbnails/` directory created on first write if absent
- [ ] Uses browser User-Agent + Referer headers for Instagram request
- [ ] Atomic write (temp file + rename) to prevent partial reads

*Tests:* `test_thumbnail_cache_miss`, `test_thumbnail_cache_hit`,
`test_thumbnail_instagram_404`, `test_thumbnail_non_image_content_type`,
`test_thumbnail_empty_body`

### US-07: Avatar serve-from-disk endpoint

**As a** dashboard user
**I want** `/api/media/avatar/{username}` to serve real profile pictures
**so that** the dashboard shows actual avatars when available.

*Acceptance criteria:*
- [ ] File exists at `data/media/avatars/{username}.jpg`: returns `FileResponse`
      with correct Content-Type
- [ ] File exists but is 0 bytes: treats as uncached, returns DiceBear redirect
- [ ] No local file: returns 302 redirect to DiceBear identicon
- [ ] Does NOT make any Instagram API call (avatars are pipeline-populated)
- [ ] `data/media/avatars/` directory created on server startup if absent

*Tests:* `test_avatar_from_disk`, `test_avatar_empty_file_fallback`,
`test_avatar_no_file_dicebear`, `test_avatar_no_instagram_call`

### US-08: Dead code removal

**As a** maintainer
**I want** the broken media-cache infrastructure removed
**so that** nobody wastes time debugging Playwright scrapers or og:image
extraction that Instagram blocks.

*Acceptance criteria:*
- [ ] `_fetch_og_image` function removed from `server.py`
- [ ] `_get_cached_media` function removed from `server.py`
- [ ] `_cache_media` function removed from `server.py`
- [ ] `_ensure_media_cache` function removed from `server.py`
- [ ] `instagram_media_cache` table dropped from ops.sqlite
- [ ] `scripts/cache_instagram_media.py` deleted
- [ ] `import re` removed from `server.py` if only used by `_fetch_og_image`
- [ ] `import urllib.request` removed from `server.py` if only used by `_fetch_og_image`

*Tests:* Verify server module imports without dead function references

### US-09: ScrapeConfig results_type validation

**As a** pipeline operator
**I want** `ScrapeConfig.results_type` to reject invalid values at config time
**so that** a typo like "stories" doesn't produce a silent failed scrape.

*Acceptance criteria:*
- [ ] `results_type` field uses `ResultsType(str, Enum)` with members
      `POSTS = "posts"`, `COMMENTS = "comments"`, `DETAILS = "details"`
- [ ] `ScrapeConfig` default remains `results_type="posts"`
- [ ] Invalid value raises a Dagster config validation error at launch time
- [ ] Existing callers (tests, `ig_posts_raw` asset signature) unchanged

*Tests:* `test_scrape_config_valid_types`,
`test_scrape_config_invalid_type_rejected`

### US-10: Media cache and lake path helpers

**As a** developer
**I want** shared path helpers for the media cache and schema catalog entries
**so that** the dashboard server and pipeline assets agree on file locations
and the schema drift detector catches table mismatches.

*Acceptance criteria:*
- [ ] `MEDIA_ROOT`, `thumbnail_path(shortcode)`, `avatar_path(username)` in
      `lake.py`
- [ ] `media_cache` table in `SQLITE_TABLES` in `schemas.py`
- [ ] `silver_ig_profiles` and `silver_ig_comments` in `DUCKDB_TABLES`
- [ ] `dim_profile` gains `profile_pic_path TEXT` in `DUCKDB_TABLES`
- [ ] State readiness test updated and passing

## Active

### 14. Creator growth analysis — baseline cohort + follower history (Q9-Q11)

**Status:** Proposed (2026-08-31) — design discussion in
`docs/creator-growth-analysis.md`

Goal: answer research questions about how successful creators start and grow
(Q1-Q11 in the ref doc), and ultimately produce per-creator channel audits
benchmarked against their domain. This is a data-acquisition + analysis-design
issue, not code yet.

**Reference:** `docs/creator-growth-analysis.md` (full design context).

#### What's needed
- **Follower-count time series** (`profile_observations` table + scheduled
  profile re-scrape) — the #1 gap; unblocks Q5 and most of Q11.
- **Wayback CDX smoke test** — confirm/deny Wayback as the free past-backfill
  source for follower history (sparse coverage + UI drift are the risks).
- **Domain / sub-domain taxonomy** — consistent creator-level labels derived from per-post
  gold classifications (Q10/Q11 bucketing key).
- **Baseline cohort** — matched case-control "ladder" (fail/flat/slow/medium)
  per domain-platform-era cell; hundreds total, not thousands; outcome spread;
  controls selected by a principled frame, not opportunism. 20 IG-only beats 20
  split across platforms for IG questions.
- **Success metric + failure definition** — pin down before building (followers
  vs engagement rate vs growth velocity; persisted-but-stalled vs abandoned).
- **Multi-platform coverage** — TikTok/YouTube sources + cross-platform identity
  for Q3 (separate creator cohort design).

#### Expert panel gap analysis (2026-08-31) — see ref doc §7-8
- **6 enabling changes, in leverage order:** GAP-1 `profile_observations` +
  scheduled re-scrape (highest leverage, unblocks Q5/8/9/10/11), GAP-2
  early-history backfill, GAP-3 cohort_labels + baseline recruitment, GAP-4
  `gold_creator_domain`, GAP-5 structured CTA fields in result_json, GAP-6
  second-platform sources (deferred).
- **Merge clusters:** Q5+Q11-velocity; Q9+Q10; Q1+Q4+Q6 (one early_history
  build serves all three); Q2+Q7 (same format/CTA assets).
- **Drop/deprioritize:** Q3 (blocked on unbuilt sources, near-dup of Q1),
  Q11 as independent effort (re-scope as a view over Q5+Q10), Q9 standalone
  recruitment (the matched-ladder embeds it).
- **Cost verdict:** all questions CHEAP except Q3 (new connectors) and
  retrospective Q5 (feasibility-limited, NOT dollar-limited — historical
  0→100→1k curves for small accounts are largely unbuyable). One-time
  ~$40-160 + steady-state ~$5-10/mo; Gemini video is the dominant cost
  driver (subsample it); Gemini text on free tier.

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

The entire `o44ZGN3WOEuMzCgcf` dataset (365 rows) is all nulls. Likely a
systemic problem with that specific Apify actor run. Likely causes:
- Apify actor returned a different response shape (profile-only, no post data)
- Rate limiting produced empty pages for every profile in that batch
- Actor config changed between runs (missing `resultsType=posts` or similar)

**Suggested fix:** Entity classifier from issue #10 fixes this — detects
non-post shapes and skips them. Tracked in `feat/media-and-entity-routing`.

### 6. Dead letter backlog from Gemini rate limiting

**Observed:** 10 rows in ``dead_letter``:
- 4 ``status=skipped`` — "Empty caption" (legitimate — profile pages without posts)
- 6 ``status=pending`` — "429 RESOURCE_EXHAUSTED" (Gemini API rate limiting)

**Code fix applied:** ``ig_posts_gld`` retry loop now uses jittered backoff
(``(2^N) + random(0,1)`` seconds instead of deterministic ``2^N``) and
classifies 429 errors via ``_is_quota_exhausted()`` / ``_is_rate_limited()``
helpers. Quota exhaustion stops retries immediately; rate limits retry with
jitter. See ``AGENTS.md#gemini-api-rate-limits`` for full context.

**Next step:** Re-run the gold asset when rate limits allow and observe
whether the improved retry loop resolves the 429s.

### 7. Multimodal processing — video, image, text enrichment

The enrichment worker currently discards `lookup_or_upload` return values — file
URIs never reach `gemini.analyze()`. All 56 gold_analyses rows are text-only.
This tracks the full implementation: media download, Gemini File API upload with
state polling, cache with TTL, tier gating (FREE skips video), and token budget
checks. Plan: `tasks/plans/multimodal-processing.md`.

#### Phase 1: Foundation
- [x] `GeminiResource.analyze()` accepts optional `media_files: list[MediaFile]` parameter
- [x] `analyze()` constructs `contents = [Part.from_uri(...), Part.from_text(...)]` when media_files provided
- [x] `analyze()` wires `media_resolution='low'` on `GenerateContentConfig` when media_files present
- [x] `test_worker_passes_media_uri_to_gemini` rewritten

#### Phase 2: Media pipeline
- [x] `lookup_or_upload_all` returns `list[MediaFile]`, downloads URLs to temp files
- [x] `lookup_or_upload_all` polls `file.state == ACTIVE` after upload
- [x] `lookup_or_upload_all` stores `expires_at`, checks expiry on cache hits
- [x] `lookup_or_upload_all` processes ALL URLs, uses INSERT OR IGNORE for TOCTOU
- [x] Schema migration: add `expires_at TEXT` to `media_metadata`

#### Phase 3: Worker integration
- [x] `process_item()` captures media_files and passes to `gemini.analyze()`
- [x] `process_item()` gates video processing on tier, estimates tokens
- [x] `process_item()` classifies File API errors separately

#### Phase 4: Cleanup
- [x] Existing 20 tests still pass (106 total)
- [x] Ruff zero warnings

### 11. Profile management + metadata scrape (frontend CRUD + ops control)

**Status:** Proposed (2026-08-12)

Users need a durable, first-class place to manage the list of profiles the
pipeline tracks, at per-profile depth, instead of ad-hoc ``ScrapeConfig.urls``
typed into the launchpad. This is the input side of the triage idea in
``AGENTS.md`` that was never implemented.

Scope:
- **ops.sqlite ``scrape_targets`` control table** — source of truth for the
  profile list: ``username``, ``profile_url``, ``results_type``,
  ``results_limit`` (per-profile depth), ``enabled``, ``tier``, ``updated_at``.
- **Frontend CRUD page** — add/remove/edit profiles, set per-profile depth,
  manage the list.
- **Profile metadata scrape on save** — when a profile is accepted, run the
  Apify actor with ``results_type="details"`` to fetch metadata (biography,
  followers, ``profilePicUrlHD``) WITHOUT posts, and write it to ops.sqlite.
  Cost ~1 result per profile (~$0.18 for 66 at free tier).
- **Datalake pulls from ops** — ``ig_profiles_slv`` (or a new asset) reads
  profile metadata from ops as its source, so profile rendering (avatars,
  biography, follower counts) no longer depends on post scrapes.

Motivation: profile rendering is currently blocked because avatars only arrive
via post/details scrapes whose CDN URLs expire in ~4-5 days. A details scrape
at add-time gives fresh ``profilePicUrlHD`` immediately, and the ops table makes
the list durable and independently manageable.

### 12. Serving-layer test breakage (analytics_views staleness)

**Status:** Open

The serving layer was refactored from a single ``analytics_views`` asset into
``v_post_detail`` + 7 downstream views, but 5 test files still reference the
old name and fail at collection or assertion:

- ``tests/unit/serving/test_serving.py`` — imports ``analytics_views`` (collection error)
- ``tests/integration/serving/test_gold_to_serving.py`` — imports ``analytics_views`` (collection error)
- ``tests/e2e/test_full_pipeline.py`` — 2 tests query ``analytics_views``
- ``tests/e2e/test_snapshot.py`` — runs ``v_post_detail`` without ``dim_date`` (DuckDB replacement-scan error)
- ``tests/unit/serving/test_serving_asset_checks.py`` — references ``analytics_views_row_count_positive`` (KeyError)

Fix: migrate these tests to ``v_post_detail`` + the individual view names, and
add a ``dim_date`` setup in the snapshot test. This predates
``feat/media-and-entity-routing`` and is unrelated to it.

### 13. Stale code + doc references

**Status:** Open

- ``README.md`` still shows the pre-refactor architecture: ``analytics_views``,
  ``ig_posts_gld_enqueue``, and ``enrichment/ # Queue, worker, sensor``.
- Default Gemini model string ``gemini-3.1-flash-lite`` (``common/resources.py``,
  ``enrichment/prompts.py``) is stale — current models are ``gemini-3.5-flash-lite``
  and ``gemini-3.6-flash``. Verify before the next enrichment run.
- ``GoldConfig`` docstring references ``gold_ig_analyses`` (renamed ``gold_analyses``).
- ``defs/serving/asset_checks.py`` docstring references ``analytics_views returns rows``.

### 15. Full test suite runtime is long

**Status:** Tracked (2026-08-31) — not a priority, just tracking.

The full suite (`uv run pytest tests/ -q`) now takes **~15-20 minutes**
(measured 450s / 7.5 min on a clean run, plus warmup; it has exceeded a 600s
command timeout). Slowest areas are the migrated labels-driven admission tests
and the enrichment/media-cache tests. Root causes are not yet investigated;
candidate levers: pytest-xdist parallelization, marking slow E2E tests, or
splitting unit vs integration/e2e into separate CI jobs.

This is a tracking note only — no action planned until runtime becomes a
bottleneck. If optimizing, verify that assertion coverage is preserved (do not
cut tests to save time).


### 16. Local-disk ad-hoc ingestion as first-class bronze source

**Status:** Current actionable workstream (2026-09-01).
**Branch:** `feat/ig-local-ingestion`
**Plan:** `tasks/plans/ig-local-ingestion.md`
**Origin:** #14 — the creator-growth baseline cohort needs the ad-hoc
saved-list posts; this issue makes them a first-class bronze producer instead
of a one-off import.

#### Intent

The repo has 10 local dataset ids (9,465 posts; 9,413 with media, 52 without)
on local disk at
`C:/Users/evano/repos/scrape-ig-saved-list/data/ingest/<dataset_id>/<post_id>/post_metadata.json`.
Make local-disk ingestion a **second bronze producer** under the existing
producer-agnostic bronze contract (`docs/BRONZE_SCHEMA.md`) — not a bootstrap
or one-off migration script. Silver already classifies by
`input.results_type`, globs all `*.parquet` against the `silver_ig` watermark,
and dedups via `DISTINCT ON(post_id) ... ORDER BY scraped_at DESC,
source_dataset DESC`, so a `local_`-prefixed Parquet with
`results_type="posts"` is picked up with **zero silver changes**.

This is a NEW SOURCE RULE case: a new scrape source is a candidate second
bronze producer on the existing contract. A bootstrap script would duplicate
the producer and bypass watermark/dedup.

#### Locked design decisions

1. **`local_<dataset_id>` file naming.** The local producer namespaces every
   file `local_<dataset_id>` (e.g. `local_abc123.parquet`). This preserves
   provenance for the 3 dataset ids that overlap existing Apify ids; silver
   dedup makes the redundancy harmless. Used consistently for ALL local
   files.
2. **Media seeding, not re-downloading.** Seed `media_cache` rows pointing at
   the EXISTING local media files (copy bytes into `POST_MEDIA_DIR`, keyed
   `sha256(url)`), never re-download from the Instagram CDN (those URLs are
   expired). The 52 no-media posts land with nullable media fields and skip
   seeding cleanly.
3. **`results_limit = -1` ad-hoc sentinel.** `profiles.results_limit = -1`
   means "ad hoc" — data already ingested, not a continuous scrape target.
   `creators.py` currently rejects limits < 1 (relaxed to accept -1);
   `enabled_profiles` treats -1 as don't-schedule.

#### Acceptance criteria

- [ ] Local producer writes `local_<dataset_id>.parquet` + `.parquet.meta`
      sidecar with `input.results_type="posts"` for all 10 dataset ids
- [ ] Silver `ig_posts_slv` picks up local files with no code change (mtime >
      watermark `silver_ig`)
- [ ] Dedup keeps one row per `post_id`; `source_dataset = local_<dataset_id>`
      preserved; the 3 overlapping Apify ids resolve deterministically
- [ ] `media_cache` seeded from local bytes for the 9,413 posts with media;
      the 52 no-media posts handled without error
- [ ] `profiles.results_limit = -1` accepted by `creators.py` and never
      scheduled by `enabled_profiles`
- [ ] Producer is write-once and idempotent: a re-run on unchanged data adds
      no new bronze files and does not re-trigger silver
- [ ] `ig_posts_raw` (Apify producer) untouched

#### Non-goals

- **No change to `ig_posts_raw`** — its config, code, and file naming stay
  untouched.
- No new silver/gold tables — the local source flows through the existing
  medallion path.
- No re-download of Instagram media (URLs expired; local bytes are canonical).
- No parallel pipeline or migration script (NEW SOURCE RULE: producer on the
  existing contract, not a one-off bootstrap).

### 17. Upgrade to Gemini Tier 2 for batch enrichment (+ cost estimates)

**Status:** Proposed (2026-09-01).
**Origin:** #16 — the local-ad-hoc ingestion backlog (1,016 posts) is pending
enrichment; Tier 1 interactive processing is the bottleneck and video is
gated behind Tier 2.

#### Intent

Enrich the 1,016-post backlog (652 image, 364 text-only, 0 video — video is
Tier-2 gated and skipped on free/Tier 1) via Gemini **batch API**. Tier 2
lifts the 10M batch-token cap (→ 500M), raises RPD, and is required for video
enrichment at any scale. This issue tracks the upgrade decision + the batch
cost/feasibility estimate.

#### Cost estimate (measured backlog)

Model: `gemini-3.5-flash-lite` (`_DEFAULT_GEMINI_MODEL`). Measured 1,016
pending posts → 1,704 media items, 0 video.

| Line | Value |
|---|---|
| Input tokens | ~1.05M (images 0.44M @ ~258/img + text 0.61M) |
| Output tokens | ~0.81M |
| Est. cost, standard rates ($0.30 in / $2.50 out per M) | **~$2.35** |
| Est. cost, batch (50% discount) | **~$1.17** |

The current **image/text backlog is trivially cheap (~$1-3)**. The dominant
cost driver is **video** (10-100× all scraping; a 10-min reel ≈ 174K tokens).
If video enrichment is added, the one-off full-reel estimate from
`docs/creator-growth-analysis.md` is **$50-300+** — mitigate via stratified
subsampling (top/bottom ~10 posts/creator, $10-60).

#### Tier 1 → Tier 2 escalation triggers (numeric, from AGENTS.md)

- [ ] Weekly post volume ≥ 1,000/week for 2 consecutive weeks
- [ ] Any batch job projected > 10M tokens (Tier 1 flash-lite batch cap)
- [ ] Adding video enrichment (immediate Tier 2 trigger regardless of metrics)
- [ ] Rolling 30-day Gemini spend ≥ $200 (80% of Tier 1 $250/mo cap)

#### Acceptance criteria

- [ ] Confirm live Tier 1/2 rates at `https://aistudio.google.com/rate-limit`
      and update the estimates above
- [ ] Decide: batch-enrich current backlog on Tier 1 (interactive, ~$1-3) vs
      upgrade to Tier 2 first (required for batch API + video)
- [ ] Once decided, re-introduce the batch-API worker variant (Tier 2)
- [ ] Clear the 1,016 pending `batch_items`

#### Non-goals

- No video enrichment at scale until Tier 2 (cost + upload-time bottleneck).
- No change to the local-ingestion producer; this is an enrichment-tier decision.

### 18. Post detail page — first-party view of full post context + source links

**Status:** Proposed idea (2026-09-02) — awaiting full Epic: user stories,
validation, discussion, analysis, plan. Queued as the next orchestration after
the metrics-centralization refactor (PR #27).
**Origin:** Dashboard UX gap. Hot-posts cards link out directly to the source
Instagram post with nothing identifying the post in-platform besides the
thumbnail, so a post is hard to trace back to a creator's post list. There is no
first-party "post detail" surface.

#### Intent

Give any post (from hot-posts cards, standout feeds, or a creator-detail post
list) a first-party **post detail page** in this platform that aggregates
everything we hold about it — post metadata, caption/transcript, gold
enrichment (domain/topic/educational/actionable/admiralty), engagement metrics
(now warehouse-canonical per the metrics-centralization refactor) — and links
back to the original source post (Instagram/other platform). Hot-posts cards and
creator-detail post rows link into these detail pages.

#### Sketch (for discussion — NOT a locked design)

- **Route** e.g. `/posts/{post_id}` backed by a read-only endpoint selecting
  from canonical views (`v_post_detail`/`v_post_metrics` + enrichment) — thin
  projector only, consistent with the metrics-centralization rule.
- **Fields:** full metadata, media/thumbnail, caption, transcript where present,
  enrichment summary, point-in-time breakout context (hot/standout, z vs the
  post's own trailing baseline), and a **link to the original source post**.
- **Entry points:** hot-posts card → detail; standout feeds → detail;
  creator-detail post list → detail. Keep the existing direct outbound
  source link too.

#### Open (validation needed — surface in the Epic process)

- Scope: is "post detail" read-only analytics surface, or does it need write/
  re-enrich actions? (Lean read-only first.)
- Transcript source/availability (video transcripts, caption text) and where it
  is stored.
- Which enrichment fields are meaningful at post grain vs already on the card.
- Navigation/back behavior; whether creator pages deep-link into it.

### 19. Batch-multimodal enrichment (wire media into the gemini-batch path)

**Status:** Resolved (2026-09-04) — merged in PR #43 (feat/19-batch-multimodal-20-mime). The
interactive multimodal path is wired + proven, batch is the durable vehicle
for video-at-scale.
**Origin:** First live multimodal runs (2026-09-04) confirmed interactive
media enrichment works and materially changes classification (93.6% of
media-bearing posts vs text-only); the `gemini-batch` execution mode remains
**text-only** and is the scaling gap.

#### Intent

Media reaches Gemini in **interactive** mode end-to-end (`process_item` reads
`media_files`, routes through `lookup_or_upload_all` → File API, applies the
FREE-tier video gate + per-item video-token cap, calls `gemini.analyze(...)` at
`MEDIA_RESOLUTION_LOW`). The **batch** path is text-only: `build_requests_for_items`
selects caption only, and `_to_inlined_request` serializes `contents` as a bare
string with no file `Part`. Batch-multimodal would make a scalable video-at-scale
corpus pass possible.

#### Locked design direction (from ADR-0001 scope + 2026-09-04 run)

1. Batch requests must carry File-API-referenced media: `build_requests_for_items`
   reads `media_files` + calls `lookup_or_upload_all`, applies the same tier/video
   token gates, and attaches the media list to each request.
2. `_to_inlined_request` builds `contents` as text `Part` + per-file `Part.from_uri`,
   mirroring `GeminiResource.analyze`'s multimodal branch.
3. Chunk/token accounting must include media tokens (video ~98 tok/s low-res), not
   just prompt text — batch in-flight caps bound enqueued INPUT tokens.
4. **External Integration Gate first:** submit → poll → retrieve a tiny multimodal
   batch (1 real image + 1 short video) before any scale run. Verify the Batch API
   accepts file URIs in `InlinedRequest` — unproven and the top risk.

#### Non-goals

- No change to interactive mode (works; leave as-is).
- No change to `ig_post_labels` / the label pass.
- Batch-multimodal is NOT required for sub-~700-post runs — interactive suffices.

### 20. media_cache File-API mime-detection gap (intermittent dead-letters)

**Status:** Resolved (2026-09-04) — merged in PR #43. Diagnosis history below.
**Origin:** First multimodal runs dead-lettered ~3% of items with
`Unknown mime type: Could not determine the mimetype for your file — set the mime_type argument`
from `google.genai` File API uploads. Recurring, per-item, not systemic — but it
caps recovered counts on every multimodal pass.

#### Intent

`lookup_or_upload_all` (and `_download_bytes`/`cached_local_path`) can fail to
classify a downloaded media file's MIME type for certain URLs (observed on image
URLs whose served `Content-Type` / extension mapping falls through the
`_EXT_BY_MIME` detector), routing otherwise-valid posts to `dead_letter` after 5
attempts.

#### Root-cause candidates (needs confirmation)

- MIME inferred from URL extension or served `Content-Type` misses some CDN image
  variants (no/obscured extension; octet-stream fallback not mapped).
- The resolved `mime_type` is `application/octet-stream`, which Gemini's File API
  rejects for content it can't sniff, and no file-extension fallback is applied.

#### Acceptance criteria

- [ ] Reproduce on a dead-lettered post's media URL; identify the exact fallthrough.
- [ ] Add a robust mime fallback (sniff magic bytes via `python-magic`/`file`, or map
      from extension when `Content-Type` is generic) so image/video posts upload.
- [ ] Re-enqueue the 25 dead-lettered posts (5 residue + 20 slice) on the fix and
      confirm they enrich.

#### Non-goals

- No behavior change to the byte-cache-first upload path (CDN fallback stays a
  fallback — do not re-introduce the expiry race as the primary path).
- No change to the batch job dead-letter routing semantics.

### 21. Posts table lags at ~10k rows — client-side-everything + eager per-row network images

**Status:** Resolved (2026-09-04) — merged in PR #41 (feat/dashboard-posts-paging). See the #41 body + PR diff for the implemented fix. History of the diagnosis retained below.
**Origin:** Dashboard /posts visibly lags as the dataset grew (~10k rows). Root-cause
diagnosed from source (dashboard + dash-api); not yet implemented.

#### Root cause (diagnosed, evidence-backed)

Not buffering — the whole dataset is loaded into the client with no server paging,
and every grid row renders eager network images:

1. **Full-client row model.** `/posts` fetches the ENTIRE dataset (no server
   LIMIT: `server.py` applies `LIMIT only if limit>0`; the UI calls
   `fetchPosts(0,0)`) into `useState` and feeds ~10k rows to AG Grid's CLIENT
   row model — re-sorting/re-filtering all rows client-side on every filter change
   (`doesExternalFilterPass` over every node, no debounce on filter/quick-filter).
2. **Eager per-row network images, no lazy-load.** Each row mounts a `<Thumbnail
   size=48>` and an `<Avatar>` `<img>` (`posts-table.tsx`), with NO
   `loading=lazy`/IntersectionObserver (`thumbnail.tsx`), so ~10k thumbnails
   are requested as rows are created. Refetches (search/username change) recreate
   the row DOM and re-request every image — no `getRowId`/`immutableData`.
3. **Backend media-fetch bottleneck.** Uncached thumbnail requests cold-fetch from
   Instagram via synchronous `urllib` with a 10s timeout inside the sync FastAPI
   threadpool (`server.py`), so scrolling across thousands of uncached shortcodes
   saturates the threadpool and stalls JSON + thumbnail endpoints alike.
4. **Duplicate initial full fetch** (two mount effects fire when search is empty);
   coarse re-renders on sidebar toggle.

#### Recommended fix direction (deltas; not yet implemented)

- **True server-side / virtual-paged row model** for /posts (LIMIT/OFFSET against
  the same DuckDB serving view) with client `paginationPageSize` — stop shipping
  10k rows per request.
- **`getRowId` + immutable row-data updates** so refetches don't recreate row DOM.
- **Stop eager grid thumbnails**: serve media only on the post-detail page, or gate
  grid images behind an IntersectionObserver + `decoding=async`/`fetchpriority=low`
  and only for the visible page.
- **Keep media serving async** on the backend so uncached thumbnail fetches never
  block the endpoint threadpool (real `await`/HTTPX, not sync `urllib`).
- **Debounce filter + quick-filter**; de-duplicate the mount fetch; move pure
  value cells to `valueFormatter` and memo reused cell renderers.

#### Non-goals

- No change to canonical serving views (dashboard stays a thin projector).
- No schema/warehouse change — this is a data-delivery (paging + image delivery)
  concern.

### 22. Account discovery + crawling — compile niche account lists by profile type

**Status:** Proposed (2026-09-05). Ad-hoc discovery tooling (not yet a scheduled
pipeline). Companion to the growth-report work (Q3/Q9 sub-100k gaps).

#### Why / the gap
The lake is 94.5% accounts ≥10k followers and ~73% Tech/Business. To answer
"what do *small* accounts in our niches do" (Q3) and widen the niche map (Q9) we
need NEW accounts we don't already track, across follower sizes, topics, and
success levels. Discovery must be automated and budget-tracked, and must NOT put
the user's IG account at risk (no logged-in browser bots / no user-session
cookies on the user's account).

#### Profile types we want to collect (classification target)
Each discovered account is tagged with a profile-type label so it can be routed
to the right future cohort. Type = size tier × (success/engagement signal) ×
niche. Examples of the taxonomy we want output:
- **small_creator_successful** — low followers (<~10k) but strong relative
  engagement (the "what a nobody did right" cases the report lacks).
- **small_creator_domain** — low followers, specific niche/domain (bio/topic),
  regardless of success — fills the per-topic small-account gap.
- **mid_creator_* / big_creator_*** — same success/domain dimensions at
  10k–100k / 100k+.
- **unsuccessful_100k** — large follower count but weak/declining engagement or
  stalled growth (the control/anti-pattern cohort — partial Q4 proxy).
- **successful_100k** — large + strong, the imitation-reference cohort.

Success/engagement is scored from a public no-login profile scrape (followers,
posts count, avg recent-post likes / engagement rate, bio, join-date if
available), NOT from the gold lake (these are new, un-enriched accounts).

#### Discovery methods (validate reliability first, then build)
All ban-free on the user's account (Apify actor infra / search engines; never a
logged-in user browser bot — rejected as high ban risk, per research):
1. **Niche keyword / account search** (`data-slayer/instagram-search-users`,
   `seemuapps/instagram-niche-finder`) — keyword → accounts of all sizes; best
   for surfacing SMALL accounts in a niche. → validate.
2. **No-login follower/following graph** (`scraping_solutions/
   instagram-scraper-followers-following-no-cookies`) — who niche leaders follow
   ≈ niche adjacency, no session. → validate.
3. **Related/similar-accounts rail** (`thenetaji/instagram-related-user-scraper`,
   `elliotpadfield/instagram-related-profiles` [BFS+follower-filter+budget]) —
   recursive niche widening. CAVEAT (verified): the rail skews to same/larger
   tier; useful to map the niche above target size, not to find small accounts.
4. (Fallback/adjacent) SERP discovery — Google-indexed IG posts by niche term
   (proven working; IG posts indexed since 2025-07-10).

#### Deliverables (in order)
- Validate which actors reliably return account handles + follower counts (cheap
  ~$0.01 runs, budget-tracked; under $5 total per session).
- A basic Python script (ad hoc run, not yet Dagster) that: runs the validated
  discovery method(s) against our desired niches → compiles candidate account
  handles → no-login profile-scrapes each → classifies into the profile-type
  taxonomy above → dedupes against the tracked roster (ops.sqlite `profiles`) →
  outputs ~20+ new accounts with their type + reason for interest.
- Log the crawl budget and spend per run (tracked, so sessions stay under cap).
- Later: productionize as a scheduled Dagster ingestion pipeline (separate
  issue/plan).

#### Non-goals (this issue)
- No scheduled pipeline yet (manual/ad-hoc script only).
- No enrichment of discovered accounts yet (that's the normal gold path once
  ingested).
- No scraping of the user's logged-in IG account or follower lists under their
  session.
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
