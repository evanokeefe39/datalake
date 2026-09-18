# Issues & deferred work

Issue tracking is local — this file, not GitHub Issues.

> **Currency note, 2026-09-15.** ADR-0011 is now **LIVE**: `bronze_enrichment_raw`
> → six `silver_*` tables → four gold marts all materialize, and `gold_analyses`
> parity is verified (9,576 rows, 0 both-non-null conflicts). ADR-0012's
> Dagster-native state is live for the classification workload; what remains is the
> queue-table DROP (staged in `scripts/retire_queue_tables.py`, awaiting human
> approval) and promoting the qwen facets path off the hand-rolled CLI.
> **Older entries below still describe the pre-migration world** — read them as
> history unless the entry says otherwise. Current state: `AGENTS.md`
> ("Enrichment v3 — verified state, 2026-09-15") and `WATCHDOG.md`.

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
### 38. Compose acceptance (V5/V6) — EXECUTED 2026-09-16, both passed

**Status:** RESOLVED. Docker Desktop was started and the acceptance was run for real against
the containers, not simulated.

**V5 — the platform comes up. All criteria met.**

- `docker compose build` built all three images; `up -d` brought up orchestration, jobs and
  dashboard.
- `jobs` `/health` → 200 `{"status":"ok","model":"qwen/qwen3.7-flash","version":"0.1.0"}`;
  the container is `healthy` per its healthcheck.
- Dagster webserver → 200; all six daemons running (`SensorDaemon` among them), and the log
  shows `Checking for new runs for sensor: enrichment_harvest_sensor` on its 30 s interval.
- Sensor policy confirmed against the instance (GraphQL, i.e. what the UI shows):
  `enrichment_harvest_sensor` **RUNNING**, `enrichment_submit_sensor` **STOPPED**.
- Negative control PASSED: with `jobs` stopped, `ServiceBackedAdapter().require_health()`
  raised `ProviderError: inference service http://jobs:8462 is DOWN (/health: [Errno -2] Name
  or service not known); refusing to submit` — a loud failure, never a quiet nothing-to-do.
- `DAGSTER_HOME` correctly resolves to `/data/dagster_home` (the compose `environment:`
  override beats `.env`'s Windows path), which is ISSUES #36 (`.env` `DAGSTER_HOME` beats a shell export) defeated in the container.

**V6 — one real enrichment cycle. Passed, destination-verified.**

Run against live as the owner directed, with `data/backups/v6-pre/` snapshotted first.

|Assertion|Result|
|---|---|
|submit|`2 submitted, 0 failed, 2 candidate(s) of 21 discovered`; real service job handle|
|media reached the model|items carried **14 and 16 images**; both `completed` with 3–4 KB of output|
|harvest|sensor fired **autonomously**, ran `enrichment_harvest` → `RUN_SUCCESS`|
|bronze destination|9,576 → **9,580** rows, new rows `provider='service_backed'`|
|silver destination|`silver_visual_annotations` 0 → **2**; `silver_visual_summaries` 0 → **2**|
|failures recorded, not dropped|2 pre-mount items quarantined with `reason_code='provider_error'`|

**Two container-only defects found by actually running it** (neither visible to any test):

1. **A stale host dev server squatted port 3002.** An earlier `uvicorn` on
   `127.0.0.1:3002` held the port, so the dashboard container started *unpublished*
   (`3002/tcp` with no host mapping) and every `curl localhost:3002` hit the stale process,
   which served an older `server.py` without the SPA mount. This is why `/` 404'd while
   `/api/roster` 200'd. Killed the process; `up -d --force-recreate dashboard` bound the
   port. **Lesson:** a container that starts without its declared port mapping is silent —
   check `docker ps` for `0.0.0.0:N->N/tcp`, not just `Up`.
2. **The launchpad configs in `data/smoke/*.json` are host-relative.** `submit-live.json`
   says `database: "data/smoke/state.duckdb"`, which resolves against the container WORKDIR
   `/app` and fails `Cannot open file "/app/data/smoke/state.duckdb"`. Added
   `data/smoke/submit-container.json` with absolute `/data/...` paths. The smoke configs are
   dev artifacts (gitignored) so this is documented rather than "fixed" in-repo.

**A third finding — the smoke slice does not isolate the bronze landing.** The smoke config
swaps only the `duckdb` and `ops` resources; `harvest.py` and `silver_rt.py` call
`land_response`/`read_responses` with `root=None`, which defaults to the LIVE lake root. So
a smoke-configured run still appends to live bronze. Recorded rather than papered over: the
bronze landing is replaceable by locked decision, so a live run is recoverable, but the
isolation the smoke slice appears to offer is not there. Threading a root through harvest +
silver is the fix if isolation is wanted.

**Port note:** Dagster publishes host **7642** → container 3000 (compose default), chosen so it
does not collide with the dev-server ports (3000/3001/8000/8080) that this machine's other
stacks occupy. Override with `DAGSTER_HOST_PORT`.

**Follow-on observed (now ADR-0018):** the harvest landed bronze while
`silver_visual_annotations` sat at 0 — the lineage was correct but nothing acted on it.
The chain stopped at bronze until silver was materialized by hand. The owner's decision —
auto-materialize the replay path, while the submit edge stays a **determinism boundary** so
`silver_*` remains a pure replay of bronze (cost sits on the API credential as
defense-in-depth) — settles the design; see
`docs/architecture/adr/0018-pipeline-automation-and-the-cost-boundary.md`. The
auto-materialization implementation is separate, open work.

### 37. `media_cache` rows are written in the writer's path vocabulary

**Status:** RESOLVED for the container path (ADR-0017); the mixed-runtime caveat remains.

**Symptom.** 8,425 of 8,431 items in the live job store carry media paths, all
Windows-absolute (`C:\Users\evano\repos\datalake\data\media\posts\<sha>.jpg`). Into a
Linux container those are unopenable, so every item would fail `_missing_images` even
though the bytes are mounted at `/data/media/posts/`.

**Root cause.** The stored path is whatever the FETCHING process had, and
`opsdb.media_cache.cached_local_path` tested it with `os.path.exists` before any
translation — so the cache read as empty rather than misconfigured.

**Fix.** `stored_local_path` (no filesystem check) + `engine.media.local_media_path`
(translate via `platform.paths.runtime_path`, then test). Both containers mount
`./data` at `/data`, so one vocabulary works at the wire.

**Remaining caveat (accepted, not a defect):** a row written FROM a container holds
`/data/media/...` and will not resolve on the host without the reverse mapping. Compose is
the runtime of record; a host run after a container run needs a re-seed.

### 36. `.env` `DAGSTER_HOME` beats a shell export for the `dagster` CLI

**Status:** OPEN (unchanged) — the Compose orchestration service works around it by setting
`DAGSTER_HOME=/data/dagster_home` in `environment:`, which overrides `env_file`. The trap
remains for host CLI runs.

### 35. Enrichment harvest could not land anything (`KNOWN_PROVIDERS` drift)

**Status:** RESOLVED — found by the first real paid run through the seam.

**Symptom.** `enrichment_harvest` failed at the landing step:

```
ValueError: refusing to land provider 'service_backed' into the LIVE default lake
root: not in KNOWN_PROVIDERS ['gemini', 'none', 'qwen'] — pass an explicit tmp/test root
```

Submit and poll both succeeded and the provider call returned 200; only landing failed.
No row could reach bronze from any live adapter.

**Root cause.** `landing.py`'s `KNOWN_PROVIDERS` allowlist was written when providers were
named after the model vendor (`gemini`, `qwen`). The Gemini retirement left
`ServiceBackedAdapter` as the only registered adapter, and it names the **seam**
(`service_backed`, `service_backed.py:46`). The allowlist was never updated — the
docstring already said "a new real provider is ADDED here explicitly, alongside its
producer", and that step was simply missed when the vendor was retired.

**Why every gate missed it.** `dagster definitions validate` checks graph structure only.
The suite never lands with `root=None` (the production path), so no test could observe it.
243 green tests and a valid graph, and the paid path was still terminal. This is the
repo's verification-plane lesson in its purest form: it took one real run to see it.

**Fix.** `service_backed` added to `KNOWN_PROVIDERS` (`qwen` kept for rows already landed
under that name); the docstring now states the value is the adapter's registered name,
not the vendor. New test `test_every_registered_adapter_may_land_in_the_live_root` pins
the allowlist to `provider.ADAPTER_REGISTRY`, so the next rename fails in CI instead of
in production. Commit `e1c38b0`.

**Not changed, deliberately:** the `root=None` guard itself is correct — it is what stops
`provider='fake'` fixtures reaching the live lake.

---

### 39. Smoke E2E wrote to the live lake and the live Dagster instance

**Status:** RESOLVED (state restored) — recorded because a future session will see
cleared instance history and a backup trail, and would otherwise assume corruption.

**Symptom.** `enrichment_submit` / `enrichment_harvest` run against the smoke slice wrote
2 rows into the LIVE `data/lake/bronze/bronze_enrichment_raw.parquet` and materialized
runs and partitions into the LIVE Dagster instance. Nothing errored; the runs reported
success.

**Root cause — three independent mechanisms, all of which must be overridden.** This is
the part worth reading twice:

1. **Lake roots are import-time module constants** in `platform/paths.py`
   (`IG_DATA_DIR` / `IG_BRONZE_DIR` / `IG_SILVER_DIR`). Setting them **does** work — but
   the smoke config JSON overrode only the two DB resources, so bronze landed live.
2. **`PolarsIOManager(lake_root="data/lake")` is a hardcoded literal** at
   `definitions.py:42` that **survives every environment variable**. Asset outputs routed
   through the io manager ignore the `IG_*` exports entirely.
3. **`DAGSTER_HOME` does not work as a shell export for the `dagster` CLI.** The CLI reads
   `.env` and ignores the export — verified directly: `dagster instance info` reports the
   `.env` home while `DagsterInstance.get()` in a Python process honours the export. So
   every `dagster job execute` wrote its run records and partition materializations to the
   **live** instance while the lake stayed correctly on smoke.

**Sequence.** The first E2E contaminated live bronze and the live instance; that was
remediated. Awaiting a second E2E that was *believed* isolated, mechanism 3 was still
unknown — so it repeated the instance contamination. The lake isolation did hold on the
second attempt (proven by live bronze's size+mtime being byte-identical across the run,
while smoke bronze went 228 → 230).

**Consequence.** `data/smoke/dagster_home` is **empty (0 runs / 0 partitions)** — the
smoke instance was never actually used. Do not read that as evidence of successful
isolation; it is the opposite.

**Fix / remediation.** (a) The 2 contaminated rows were excised from live bronze by
`run_id`, restoring it to exactly **9,576 rows, provider `gemini` only**. (b) The live
instance history was cleared twice, restoring **0 runs / 0 partitions**. (c) The gap
plan's smoke recipe now carries a pre-run anchor assertion (assert every resolved root is
under `data/smoke`) and a post-run size+mtime check on the live bronze parquet — a row
count can match while bytes change.

**Backups** (all verified present before each wipe):
- `data/backups/contaminated-by-smoke-e2e-20260915T205450Z/` — 12 files (bronze parquet + history)
- `data/backups/smoke-instance-partitions-20260915T211054Z/` — 6 files (history)
- `data/backups/dagster-history.pre-legacy-key-wipe-20260915T164612Z/` — 25 files (the earlier legacy-key wipe)

**Which data is irreplaceable — the distinction that made this remediation safe.**
`bronze_enrichment_raw` is model output: reproducible from media + prompt, so excising
rows costs money, not information. `data/media/posts/` and the Apify bronze are **not**
reproducible (scraped bytes behind ~4–5 day CDN URLs; as-observed history). The same
excision applied to those would have been permanent data loss.

**STILL LIVE — not fixed, and this is the landmine for the next E2E.** The state is
remediated; the **trap is not**. Mechanism 3 above still holds: exporting `DAGSTER_HOME`
does nothing for the `dagster` CLI, so anyone who runs the smoke recipe as originally
written will contaminate the live instance again. The plan's recipe now says so and adds
assertions, but the underlying `.env`-beats-the-export behaviour is unfixed and needs
either a `--env-file`-style override or a documented "edit `.env`, run, restore"
procedure. Treat any future smoke run's instance state as UNVERIFIED until measured.

---

### 40. Retry-budget guard: was unreachable, now over-broad (2026-09-15)

**Status:** the unreachable half is FIXED; the over-broad half is **OPEN**.

**Symptom (fixed).** `_guard_round` in `engine/submit.py` was supposed to refuse a
re-submission once a post's retry budget was exhausted. It tested
`state.next_round >= MAX_ROUNDS and state.suppressed` — but the caller `continue`s past
suppressed posts *before* reaching the guard, so `suppressed` was always `False` and the
raise could never fire. A partition stuck at the ceiling was therefore re-submitted
forever, silently, at cost.

**Fix.** The guard now keys on the round alone. Regression test
`test_guard_raises_when_retry_budget_is_exhausted` in
`tests/unit/enrichment/test_partitions_retry.py`, verified discriminating (it fails on
the old condition). Landed in `3b4b410`.

**Open — the fix is now over-broad.** Keyed on `next_round` alone, a post that has
reached the ceiling raises on **any** future eligibility. That includes the legitimate
case: a post whose earlier rounds failed under an OLD prompt hash, which under ADR-0011
/ADR-0016 is supposed to be *re-enrichable* when the prompt changes. Today it raises
instead of being re-submitted.

**Required fix.** Scope the budget to the **current prompt hash** — the retry budget is
per (post, workload, prompt_hash), not per post for all time. `compute_round` already
has the key material; the guard needs the prompt hash passed in and compared, so a
prompt change resets the budget while a genuine retry loop still terminates.

**Why it matters.** This is the difference between "a retry loop cannot burn money
forever" and "a stale-prompt post can never be refreshed" — the exact failure mode
US-L5 exists to prevent. It is currently masked because no prompt has changed since the
guard landed.

---

### Dagster event log reset — repo-reorganization branches 1-3 (2026-09-15)

**Status:** RESOLVED (instance state) — recorded because it is destructive and
a future session will see an empty run history.

**Symptom.** The plan's gate 8 (the facet dry run) failed on the live instance:

```
RuntimeError: unparseable in-flight partition key '0e0bfcb3009b42c3':
  partition key must be '<workload>\x00r<N>\x00<post_id>'
```

**Root cause.** The pre-refactor layout wrote sha256-digest partition keys into
`data/dagster_home`. Eight such keys remained in the event log, and
`in_flight_partitions` derives from `get_materialized_partitions` — the EVENT
log, not the partition definitions — so `_in_flight_by_post` raised on every
submit run. `SqliteEventLogStorage` supports neither partition-scoped wipe
(`wipe_asset_partitions` → `NotImplementedError`) nor asset-wide
(`wipe_assets` → `False`), so the events could not be purged selectively;
deleting the 8 partition *definitions* alone would have left the events behind
and the guard still raising.

**Fix.** Cleared `data/dagster_home/history/` entirely. All 8 keys were verified
to name no workload, have zero harvested counterparts, and be unharvestable —
no real completed work was affected. Backup (verified 25 files / 9.42 MB) at
`data/backups/dagster-history.pre-legacy-key-wipe-20260915T164612Z/`.

**Consequence:** the live instance has NO run history before 2026-09-15. Prior
run records exist only in that backup.

**Not fixed (deliberate):** `_in_flight_by_post` still RAISES on an unparseable
key. That is the strict grammar guard ADR-0014 D1 specifies, and it is correct
once the instance matches the grammar. A future migration introducing a third
key grammar should reconcile via `migrations/` rather than loosening it.

### Enrichment ops reported through the I/O manager (found by gate 8)

**Status:** RESOLVED (2026-09-15).

`submit_enrichment_op` and `harvest_enrichment_op` returned a report dict.
Neither job declares an asset, so Dagster routed the value through the
job-level `PolarsIOManager`, which resolved an asset key for an op that has none
and failed the run at execution time. `definitions validate` passed on this and
always would — the graph is structurally sound. Both now declare
`out=Out(Nothing)`.

### The test suite does not finish clean

**Status:** OPEN — deferred by the owner 2026-09-15 ("focus on finishing the
scaffold first").

`uv run pytest tests/` is not a gate for the reorganization. State at the end of
branch 3: 235 enrichment tests pass, and the wider tree collects without errors
but has not been fully re-executed since the module split (instagram / serving /
operational were relinted, not rerun). The enrichment directory is the replanned
one and is green.

**Measured 2026-09-16 on the reorg stack** (`refactor/facets-native` @ `b1922a6`), from
a CI run against the branch tip — the first execution of the full suite against the
post-reorg tree. Ruff passes; the suite fails in five named classes (~40 tests):

1. **`ModuleNotFoundError: orchestration.defs.ig_core.slv.creators`** — 10 failures, and
   NOT a test artifact: `ig_core/slv/profiles.py` imported `enabled_profiles` from a
   module the move never created (the roster lives in `opsdb.roster`). `ig_profiles_slv`
   would have raised the next time it ran. FIXED (2026-09-16); an AST sweep of all 175
   modules confirmed it was the tree's only unresolved relative import.
2. **Tests monkeypatching relocated symbols** (`posts.bronze_path`,
   `posts.ig_post_labels`) — 8 failures. The move re-homed those names; the tests still
   patch them on `posts`.
3. **`test_submit_sensor.py` hardcodes `data/smoke/state.duckdb`** — a gitignored local
   artifact, so 5 failures that can never pass in CI. Every other data-dependent test
   uses a `skipif` guard (`test_smoke_slice.py`); this one is the outlier.
4. **Serving baseline resolves every asset from `serving.views`** — 6 errors. The reorg
   split serving into five modules (`dims`/`metrics`/`marts`/`views`/`checks`), so
   `getattr(serving, "dim_date")` no longer resolves.
5. **Bronze→silver integration + silver unit tests produce zero rows** — 9 failures
   (`assert 0 == 1`). Cause not established.

**Intermediate tips are red in a different way:** slice 1 at `51aee30` has 143 lint
errors (73 `F821 undefined-name`) and slice 2 at `3b4b410` has 130 (72 `F821`) — the
suite re-target is a slice-3 commit, so slices 1 and 2 are not independently runnable.
Merging the stack in order would put two red states on `main`.

---

### Retired tables kept coming back (retirement was not durable)

**Status:** RESOLVED (2026-09-15) — W9 follow-up.

**Symptom.** The W9 retirement dropped seven tables (`gold_analyses`,
`gold_growth_facets`, `dead_letter`, `batch_jobs`, `batch_items`,
`facets_batch_jobs`, `media_metadata`) with export-count == live-count verified
in the same run. The drop was real. It was not DURABLE: live code and six
migration paths still created those tables, so any run of them would bring a
retired table back.

**Root cause.** "Dropped" was treated as a property of the database rather than
a property of the system. Nothing checked that the SET OF CREATORS was empty.
The `retire_queue_tables.py` script asserted the KEEP set after the drop, but
that only proves the tables were gone at that instant — not that they would
stay gone.

**Why it was invisible.** The readiness test
(`tests/operational/test_state_compatibility.py`) compares the live DB against
the catalog. With the tables dropped and their specs still in the catalog, it
reported three failures — which read as a test that needed updating, when it was
in fact correctly reporting a half-finished migration.

**The defect class, named:** removing a producer without removing its writers,
or vice versa. A half-cut converts a resurrection bug into a runtime failure.
The rule adopted: retire a cluster WHOLE — creator + writers + readers together
— or not at all.

**Fix.** Producers retired in `src/` (`ensure_gold_analyses`,
`_GOLD_ANALYSES_DDL`, `_GOLD_FACETS_DDL`, `_GOLD_FACETS_UPSERT`, the five
`gold_analyses`-reading asset checks, the AssetSpec, the exports); six migration
paths retired; the three catalog specs removed.

**Statements are DELETED, never renamed.** An earlier attempt renamed retired
tables to a `retired_*_NEVER` suffix — that CREATES a junk table on every run,
the same defect class as the raw DDL it was meant to replace. Both attempts were
reverted.

**Verification (this is the part that makes it a fix rather than a claim).**
1. `migrate_schema_drift.migrate()` — the one migration the README marks "RUN
   against live" — was executed against a COPY of live `state.duckdb` +
   `ops.sqlite`. Afterwards no retired table and no `analytics_views` existed in
   either database.
2. `read_gold` was executed against live `state.duckdb`; its guard fired with
   the intended message instead of a raw `CatalogException`.
3. `tests/operational/test_state_compatibility.py`: 96 passed, 0 failed. The
   three W9 failures resolved because the producers are gone — NOT by weakening
   the assertions.

**Lesson.** A drop is not done until the set of creators is empty, and emptiness
must be demonstrated by RUNNING the paths, not by grepping for the names. Greps
also miss half-cuts: a sweep for `CREATE TABLE` finds creators but not the
surviving `INSERT`/`UPDATE`/`SELECT` writers.


### 14. Creator growth analysis — baseline cohort + follower history (Q9-Q11)

**Status:** Proposed (2026-08-31) — design discussion in
`docs/research/creator-growth-analysis.md`

Goal: answer research questions about how successful creators start and grow
(Q1-Q11 in the ref doc), and ultimately produce per-creator channel audits
benchmarked against their domain. This is a data-acquisition + analysis-design
issue, not code yet.

**Reference:** `docs/research/creator-growth-analysis.md` (full design context).

#### What's needed
- **Follower-count time series** (`profile_observations` table + scheduled
  profile re-scrape) — the top-priority gap; unblocks Q5 and most of Q11.
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
producer-agnostic bronze contract (`docs/architecture/bronze-schema.md`) — not a bootstrap
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

**Status:** Proposed (2026-09-01). **Update (2026-09-08):** superseded —
batch enrichment is now the only path and runs on Tier 1; the interactive
"bottleneck" framing no longer applies. Kept for cost-estimate history.
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
`docs/research/creator-growth-analysis.md` is **$50-300+** — mitigate via stratified
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

**Status:** Resolved (2026-09-04) — merged in PR #43 (feat/19-batch-multimodal-20-mime).
Batch multimodal is wired + proven and is the durable vehicle for video-at-scale.
**Origin:** First live multimodal runs (2026-09-04) confirmed interactive
media enrichment works and materially changes classification (93.6% of
media-bearing posts vs text-only); at the time the `gemini-batch` execution
mode was text-only — since closed out (batch multimodal shipped, interactive
removed).

**status (2026-09-08):** batch IS multimodal and is the ONLY enrichment
vehicle — the entire synchronous interactive path (process_item,
scripts/enrich_interactive.py, GeminiResource.analyze) was removed; batch
growth facets run via scripts/enrich_facets_batch.py (see AGENTS.md
multimodal status).

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
   accepts file URIs in `InlinedRequest` — unproven and the top risk. (PASSED
   2026-09-08 on the facets-batch branch.)

#### Non-goals

- No change to `ig_post_labels` / the label pass.
- Batch-multimodal is NOT required for sub-~700-post runs (historical note).

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
**Update (2026-09-05):** `scripts/discover_accounts.py` built + validated (PR #51
feat/account-discovery) and the sized roster-seeded crawl added **50 quality small
niche creators** (<10k; niche-vocab + >=100 flw gate) to tracking at depth
results_limit=30 — IG roster 625 -> 675. Curation-group behavior is documented
in the script docstring. No pipeline yet.

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

### 23. Model each IG/social profile as a Dagster producer/source — compare to job-board scraping

**Status:** Idea / design note (2026-09-05). No code changes — investigate, then
route to an ADR.

#### Why
Onboarding 50 new IG profiles (issue #22) exposed that **adding a profile to the
roster does not produce a scrape by itself**: `ig_posts_raw` is a manual,
config-driven asset that scrapes exactly `config.urls`; it never auto-discovers
new/enabled profiles. `profiles`/`creators` (ops.sqlite) act as an operational
*control* list for downstream silver/label/batch scoping, but not as the thing
that drives ingestion. We had to run an explicit `ig_posts_raw` with the 50
handles to get their posts into bronze.

The idea: treat **each IG profile — and any future social profile — as a
first-class Dagster source/producer**, so a tracked profile is something the
pipeline discovers and pulls from, rather than a URL we feed a manual run.

#### What to compare (this issue is the comparison)
1. **How job-board scraping was done** — source representation, what drove the
   discovery/enumeration of what to scrape, scheduling/fan-out, state. Document
   the pattern we used there.
2. **Current IG ingestion** — ops.sqlite `profiles`/`creators` as control tables;
   `ig_posts_raw` config-driven over `config.urls`; silver ingests any new bronze
   (mtime watermark); `ig_profiles_slv`/labels scope to `enabled_profiles`.
3. **The proposed producer/source model** — per-profile (or per-platform-source)
   Dagster source that the medallion pulls from; the creators/profiles registry
   already generalizes (a creator owns 1..N profiles across platforms), so
   multi-platform additivity is a design target, not an afterthought.

#### Open questions
- What exactly was the job-board source/discovery/scheduling design to pattern-match?
- Does per-source Dagster fan-out justify the complexity vs the current
  config-driven batch scrape — especially given the Apify scrape-actor reliability
  constraints found during discovery (login walls)?
- Should the source registry live in ops.sqlite (profiles/creators) and be read by
  a Dagster sensor/schedule that enqueues scrapes for enabled profiles without a
  tracked bronze file?

#### Non-goals (this issue)
- No code/asset changes here — investigation + comparison + ADR decision only.

### 24. Enrichment appears stuck overnight — no persistent poller for async Gemini batches

**Status:** Resolved (2026-09-07) — Dagster-native harvest (migration branch
`migration/batch-native-enrichment`). The fix replaces the one-shot manual worker
re-run with a Dagster `gemini_batch_harvest_sensor` (cursor, persisted-job
rediscovery) + short `gemini_batch_harvest` run, plus a `submit_gemini_batches_job`,
so a finished async Gemini batch is always harvested with no manual step. Live-proven
on the original job 6 (593 posts): our harvest path applied the stranded terminal
tail (gold 9,570→9,576; items 584→590 complete; remote status RETRIEVED) with no human.
The external worker is removed. The 50-account ingestion (issue #22/#23 work)
enriched on the old manual worker re-run, which exposed the gap.

#### What happened
Job 6 (593 posts of the 50 scraped accounts, gemini-batch mode) sat at
**543 'processing' for ~12 hours overnight** with gold flat — looked broken.
Re-running the worker (`scripts/enrichment_worker.py --mode gemini-batch`) once
immediately retrieved **535 + failed 8**, and gold grew 9,035 → 9,570. Not a bug —
the batch had finished on Google's side; nothing was harvesting it.

#### Five Whys
1. **Why not done?** 543 items stuck 'processing'; only 49 complete; gold not growing.
2. **Why processing?** Items were claimed + submitted to the Gemini batch API; gold
   is written only at retrieval-apply, which requires polling a *terminal* batch.
3. **Why no retrieval?** A worker run does ONE submit-then-poll cycle and EXITS.
   After a bounded loop stopped at ~00:05 (batch still RUNNING), no worker process
   ran for hours, so nothing polled/retrieved the (by then finished) Google batch.
4. **Why no worker running?** The enrichment worker is a one-shot CLI, not a
   persistent daemon / scheduled sensor; nothing keeps it alive to poll async
   Gemini batch jobs to completion.
5. **Root cause:** there is no persistent or scheduled enrichment-consume loop, so
   an async Gemini batch's completion is only harvested on a manual re-run — making
   long batches look "stuck" and delaying gold indefinitely. No alerting/monitoring
   surfaced the "no worker polling for N hours" condition either.

#### Observations (benign, not bugs — verified)
- Worker DuckDBResource is `data/state.duckdb` — same file queries read; no path split.
- "49 complete, no gold" was a mid-flight read; gold landed correctly on retrieval.
- "Failed to POST to Dagster: HTTP 308" = redirect in materialization notify (non-fatal).

#### Suggested fix (not yet done)
- A scheduled/looping consume path that keeps polling `--mode gemini-batch` until
  jobs drain (Dagster sensor or a resilient looping runner), plus completion-latency
  visibility. Related to issue #23 (producer/source + pipeline productionization).

#### Corrective lesson — Dagster source-consumption patterns
#24 is the canonical failure of treating an **async external job** as a one-shot
synchronous fetch. The corrective discipline is a documented three-shape taxonomy
(global rule in the `dlc-worker` agent definition under `SOURCE-CONSUMPTION
RULES`; also written into the orchestration HTML post-mortem + the refactor
plan):
- **Synchronous request-response** (answer returns in the call — most REST, a
  bounded external-DB read): fetch in-run with keyset pagination + chunked
  batches, per-request retry/backoff, idempotent by key. No sensor — nothing to
  wait on between calls.
- **Async external job** (the system holds the result until it is done — Apify
  actor, `gemini-batch`, BigQuery/Snowflake/Databricks async-query jobs, sync
  tools): **submit → persist the handle (`run_id`/`job_id`/`dataset_id`) → END
  the run**; a cursor sensor polls terminal state and issues a `RunRequest` to a
  HARVEST run that streams the finished dataset into landing/bronze. **Never
  block a Dagster run on `poll_run`/waiting**, and never make a one-shot CLI the
  sole poller — both are this bug.
- **Unbounded stream** (Kafka & co.): micro-batch drain (interval sensor,
  cursor = committed offset) or a stream engine owns the hot path with Dagster
  orchestrating the landed table. Not an asset materialization.

Decision rule: *does the system give the answer in the call (sync), a handle to
something it finishes later (async), or an unbounded flow (stream)?* If it hands
you a token to poll, it is async even when the submit returns immediately.

**Applies to both producers here:** `gemini-batch` (this issue) and the Apify
actor, which today blocks the bronze run on `poll_run` while the actor runs — both
should move to submit + sensor-harvest. Landed bytes stay durable (bronze +
media cache) before any hermetic transform (ADR-0003).

### 25. Broken media coverage — partial/zero byte-cache carousels + live-CDN fallback (rescrape backlog) — DEFERRED 2026-09-08

**Deferred** (2026-09-08, user): park the broken-media remediation so the
enrichment-facets PR series can proceed. Full analysis is done and captured
below; resume by resolving the no-CDN-fallback correctness fix + the ingestion
coverage gap, then re-run the census and pick the Apify rescrape set.

**Problem (measured 2026-09-08):** of 8,849 media-bearing posts, 790 have ≥1
media URL missing from the scrape-time byte cache (`media_cache` in ops.sqlite) —
303 partial + 487 zero — 1,892 missing slide-URLs total. On enrichment, a cache
miss triggers a **live CDN download fallback** in `media_cache.py`
(`_upload_one` / `_try_inline_payload` / `lookup_or_upload_all`); those Instagram
CDN URLs are ~always expired by enrich time → HTTP 403 → the whole post
dead-letters after 5 wasted retries (all-or-nothing per post by design). The
byte cache is otherwise healthy (26,657 post-media keys, files present).

**Affected posts by age (silvered):** 0-4d=204 (suspicious — recent scrape should
have been cached → ingestion seeding gap, not expiry), 5-14d=556 (expired-CDN
set), 45d+=30 (predates the cache ~Aug 14). Whole-profile wipeouts (30/30 zero
cached: collective_career_lab, andrewwarner, empowered.nyu, hasewingroom,
theking_of_africa, kimbeauty_...) indicate per-scrape-run seeding failures, not
random expiry. `girsta` 78/94 mostly partial carousels.

**Census + candidate CSVs (committed):** `analysis/output/rescrape_candidates_2026-09-08.csv`
(790 posts: post_id/owner/permalink/shortcode/missing/severity/age, grouped by
owner) and `analysis/output/rescrape_owners_2026-09-08.csv` (owner rollup).

**To resume — three work items (do in order):**
1. **No live-CDN fallback at enrich/upload time** — cache miss ⇒ terminal,
   accurate "media unavailable (not byte-cached)" error, dead-letter on attempt 1
   (not 5). Only `cache_media_bytes` (scrape-time fill, fresh URLs) may download
   live. Add/update tests (`test_media_cache.py`, `test_batch_inline_media.py`).
2. **Ingestion coverage gap (root cause)** — why specific carousel slides +
   whole recent scrape batches never seeded (`seed_media_from_file` /
   `_local_post_media_pairs` in `instagram/assets.py`); fix so every persisted
   slide is byte-cached at scrape. By-design exception: a video's `displayUrl` is
   not cached when a real video file exists (`test_local_ingestion.py`).
3. **Re-run the census + pick the Apify rescrape set** — posts whose missing
   bytes aren't recoverable from surviving bronze local files genuinely need a
   rescrape (Apify, by profile); recoverable ones just need a re-seed (no Apify).

**Re-verified 2026-09-18 (still open, unchanged numbers).** The census holds
exactly: 790 posts, 556 / 204 / 30 by age tier, 487 zero-cached + 303 partial,
1,892 missing URL entries. Two additions from this pass:

- **The silent-failure mechanism, named.** `engine/media.py::cache_media_bytes`
  is documented "best-effort" and returns `None` when a download fails, logging
  at **WARNING** (`_download_bytes`, "media download failed for …") among a
  scrape run's thousands of lines. Its one caller, `scrape.py:256`, discards the
  return value entirely: `cache_media_bytes(ops, url)`. So a media URL that fails
  to download during the scrape produces **no row, no ERROR, no record** — the
  post reads as successfully scraped, and the loss only surfaces days later as an
  unbuildable enrichment item. This is the repo's silent-failure anti-pattern at
  the exact boundary work item 1 targets. A failed cache write at scrape time
  should be loud (ERROR + a count in the run's sidecar), because the bytes are
  unrecoverable once the CDN URL expires.
- **It is per-BATCH, not per-post.** Uncached rate by producing dataset:
  `OENbim5qyFy5UFalA` 23% (204/901), `local_g0h9S6SZAyuf2Pye2` 19% (129/686),
  `local_Hd5zaIqJ6HFTREg4X` 11%, down to 3% for others. A single scrape run
  loses a fraction of its media — the signature of transient CDN failures being
  swallowed, not of a structural derivation bug. Consistent with the whole-
  profile wipeouts (30/30) being runs where the failure was near-total.
- **Free re-seed recovers nothing here.** Checked every one of the 1,892 missing
  entries against the 27,594 byte files in `POST_MEDIA_DIR` (by sha256 stem,
  extension-agnostic): **0 have surviving bytes**. The re-seed path in work item
  3 applies to local-ingest cases, not this set — so all 790 genuinely need a
  rescrape, and option 2's cost is real, not overstated.
- **Not positional, not the video-precedence rule.** Missing slides are spread
  across indices [0..9] (declining with the far smaller population at high
  indices), and 0 of the 504 partial-carousel posts found in bronze carry BOTH
  `videoUrl` and `images`, so `_derive_media`'s "video wins" precedence is not
  the cause.

**CORRECTION to work item 1's premise (2026-09-18).** The description above says
a cache miss "triggers a live CDN download fallback in `media_cache.py`
(`_upload_one` / `_try_inline_payload` / `lookup_or_upload_all`); those URLs are
~always expired → HTTP 403 → dead-letters after 5 wasted retries". **Those
symbols no longer exist** — they were the retired Gemini File-API path, removed
per ADR-0009. Media resolution is cache-only now. The current behaviour is
simpler and worse: a miss simply returns None, with no fallback, no accounting,
and (until the fix below) a retry that could never succeed. The 403 is real; it
happens at **scrape** time, not at enrich time.

**FIXED 2026-09-18** (commit `2bbecb8`, ISSUES #25 work items 1 + 2, the retry
half of 3):

|Was|Now|
|---|---|
|`cache_media_bytes` made ONE attempt|3 attempts, exponential backoff. Verified live against an expired URL: `HTTP 403` ×3 over 4.4s, then a loud ERROR.|
|The scrape loop discarded every result; the function self-described as "best-effort … falls back to the CDN" (a rationale that died with the Gemini path)|`cache_media_urls` returns a `MediaCacheReport` (attempted/cached/failed + the failed URLs); the scrape logs it at ERROR when anything failed and lands it in the `.meta` sidecar under `media_cache`.|
|`seed_media_from_file`'s None was discarded too (the local_* branch)|Same accounting added to the local-ingest seeding pass.|
|A media miss raised `retryable=True` ("the media may arrive later")|`retryable=False`. It cannot arrive: the only writers of a cache row are the two scrape/ingest-time functions, and `core_refresh` re-fetches only posts newer than the profile watermark.|

**Still open:** the dead-letter queue is deliberately DEFERRED (user,
2026-09-18) — the landed `ok=False` row plus the anti-join check is the current
substitute.

**ANTIBOT RULED OUT — tested at scrape scale, not with one download
(2026-09-18).** A single fetch proves nothing (ordinary browsing does that), so
the load pattern was tested directly: a fresh profile scrape yielded **123 unique
media URLs**, all fetched → **123/123 HTTP 200**. Then the same URLs were
re-fetched with no delay → **861 consecutive requests, 861/861 HTTP 200**, zero
403 / 429 / timeouts, before the 900s cap ended the test. That is ~7 profiles of
media in one continuous burst from this machine with no proxy and no rate
limiting. (The real cache loop is fully sequential — one URL at a time, no
thread pool — so it is gentler still.) A 403 therefore means the URL expired, not
that we were blocked: same status either way, which is why volume had to be
measured separately. This removes the case for residential proxies — they would
solve a problem that does not exist, and bandwidth-metered proxy download is
expensive. Caveat: one machine, one network, ~15 minutes; it does not rule out a
longer-window or datacenter-IP-specific limit, and the new fetch logging (status
code + permanent/transient classification) is what would surface that.

**Still open:** the underlying **ingestion coverage question** — why a URL that
was reachable at scrape time fails to download at all. The retry now makes a
transient failure survivable and a permanent one visible, but the 0-4d bucket
(204 posts, exact-30 whole-profile clusters) still says some runs lose media
wholesale, and that cause is not yet identified. The leading hypothesis is the
~4.5-day signed-URL window (`oe` parameter) colliding with scrape timing, not
blocking.

### 26. Sentinel literal diverged across sibling silver producers — 8 live rows carry the REJECTED value

**Found 2026-09-15** by the W10 conformance panel (DataArchitect lens), then
verified against the live store — this is a published-data defect, not a style nit.

Two modules define `MODEL_LEGACY_NULL` with different values:

| File | Value |
|---|---|
| `src/datalake/defs/enrichment/classification.py:181` | `unrecorded-legacy-null` |
| `src/datalake/defs/enrichment/conform.py:654` | `legacy-unknown` |

ADR-0014:214 records the owner **selecting `unrecorded-legacy-null` over the
original `legacy-unknown`**, and names `classification.MODEL_LEGACY_NULL` as "the
single definition". So `conform.py`'s copy is the stale superseded value — and
`conform.py` is the live silver publisher that actually wrote the rows.

Live state confirms the divergence reached the warehouse:

```
silver_content_classification: gemini-3.5-flash-lite × 9568, legacy-unknown × 8
```

All 8 rows carry `run_id='legacy-gold-classification-backfill'`,
`provider='gemini'`, `prompt_hash='24c8e291fdfc28ed'`. ADR-0014:322 already flags
that the sentinel "leaks into serving".

**Fix — three parts, in order:**
1. Define the constant ONCE in the shared schema module
   (`src/datalake/defs/common/schemas.py`) and have BOTH producers import it. Do
   NOT have one producer import from the other: they are sibling producers on the
   same target table, and coupling them recreates the silent-divergence trap this
   bug came from.
2. Correct `conform.py`'s literal (the cause — it makes the next replay right).
3. **Re-publish silver via `scripts/conform_silver.py`** (deterministic replay from
   bronze). NEVER a hand `UPDATE`: bronze holds all 9,576 classification rows
   including the 8 NULL-model ones, so the sentinel is a pure function of bronze,
   and an UPDATE would leave bronze and silver disagreeing on a table whose entire
   contract is "silver is a pure function of bronze".

Verify the other 9,568 rows are byte-identical after the replay — a replay that
silently changed anything else would be a worse defect than the one being fixed.
Update the two tests asserting mutually exclusive sentinels
(`test_classification.py:307`, `test_conform_classification.py:145`).

**Blast radius**: contained. The sentinel appears in no serving view
(`v_post_detail`/`v_post_metrics` carry no model column), so there is no consumer
to migrate. The 8 rows are silent, mislabelled provenance.

### 27. `conform_silver.py` defaults `--silver-root` to the LIVE silver lake

**Found 2026-09-15** (panel, PlatformEngineer lens; surfaced while planning the
sentinel republish in #26).

`resolve_roots` defaults `silver_root` to `lake.SILVER_LAKE`, and the plan
documented `--state-db` as the writable-copy lever without naming `--silver-root`.
So a plain `--apply` run writes **new Parquet snapshots into the live silver lake**
— the artifact serving and the gold marts read — while registering them into
whatever copy `--state-db` points at. The copy's registration and the live parquet
then disagree.

**Fix**: make the pairing explicit. Either (a) require both roots to be passed
together and refuse a mixed live/scratch pair, or (b) refuse `--apply` when
`--silver-root` is left at its default while `--state-db` is overridden. Also
document the rule: plan-first (default mode writes nothing) at the exact live roots
to capture the "before" tuple, then apply deliberately in one shot.

**Related hazard (same class)**: `state.duckdb` is single-writer. Run this when no
Dagster daemon/dashboard holds the file, or the write fails or interleaves
(ADR-0012 decision 8 records the observed `Cannot open file … being used by
another process`).

### 28. Retired Gemini modules are inert but LIVE-imported — removal has 4 blockers

**Found 2026-09-15** (panel, DataArchitect lens). My initial triage called
`classification.py` dead. It is not — and the same audit found the Gemini set is
safe-with-changes, not free.

**`classification.py` is LIVE**: `defs/instagram/assets.py:1174-1176` imports it and
executes `_cls.CLASSIFICATION_DDL` on **every** `ig_posts_gen_batches` drain. It is
also NOT superseded by `conform.py` — `conform.SUPPORTED_CONFORM_WORKLOADS`
explicitly skips the classification workload. They are sibling producers. Deleting
it raises `ImportError` at runtime *inside the function body*, so `dg dev` stays
green and the drain crashes only when it runs.

**Removing `gemini_batch.py`/`submit.py`/`harvest.py`/`media_upload.py`/`batch.py`/
`registry.py` requires 4 rewires first:**
1. `definitions.py:17-22` imports + `:69-74` (`jobs=[...]`, `sensors=[...]`,
   `*ENRICHMENT_CHECKS`).
2. `defs/enrichment/__init__.py:16-32` re-export hub.
3. `defs/enrichment/assets.py:174` (registry), `:201` (media_upload), and the
   `check_enrichment_health` body at `:80-146`.
4. `defs/enrichment/analysis.py:23` and `registry.py:12` both import `_now_iso` from
   `batch.py` — a pair that must be severed together.

**Contradiction to resolve**: `check_enrichment_health` (`assets.py:90-99`) reads
`batch_items` and `dead_letter` at runtime, while `__init__.py:5-7` claims the
retired queue "has no read or write on any live path". The code says the docstring
is wrong. Resolve as part of W8/W9.

**Ordering**: promoting facets to Dagster (W11) is a PRECONDITION of removing
submit/harvest — `sensor.py:27-30` drives `harvest_enrichment_job`, so deleting them
without a replacement loses all orchestration.

### 29. `--plan` (offline cost projection) has no Dagster equivalent

**Found 2026-09-15** (panel, DagsterExpert lens). Dagster has no dry-run primitive,
so the pre-spend cost gate does not come for free when the CLI is retired.

`scripts/enrich_facets_batch.py --plan` → `facets_batch.estimate_facets_cost` is the
ONLY pre-spend gate in the facets path. It is also a real dependency:
`scripts/make_smoke_slice.py:448-478` shells out to it and asserts visual submittable
> 0 — deleting the CLI breaks the smoke slice's usability check.

**Fix**: re-express as an explicit dry-run job (`facets_plan_job`) whose op runs
enumerate + build + estimate and emits `AssetObservation`/`AssetCheckResult` metadata
`{targets, submittable, est_input_tokens, cost_usd}` while submitting nothing. Do not
retire the `--plan` arm until that exists.

**Related (same lens)**: `DRAIN_WORKLOAD` is a hardcoded module constant
(`instagram/assets.py:1024`), so facets posts can never enter the existing
single-workload drain. Generalizing it is small — every helper it calls already
derives workload from the partition key — but it is a prerequisite for W11.
### 30. No test constructs the asset graph — `materialize` / `execute_in_process` are absent

**Found 2026-09-15** while auditing test coverage against Dagster's own testing
model. Logged for after the migration; do not chase it mid-close-out.

**The gap.** `grep -rn "materialize(\|execute_in_process" tests/` returns
**nothing**. No test anywhere constructs the asset graph. Every layer below that
exists and works — the unit tests are real, and the e2e files genuinely exercise
live v3 assets (`ig_posts_gen_batches`, `ig_posts_slv`, `v_post_detail`,
`daily_medallion`, `dim_profile`) with `DagsterInstance.ephemeral()` +
`build_asset_context(instance=...)`. What no test does is ask Dagster to *assemble
and run the graph*.

**Why that matters — the falsifier.** An `instance: "PartitionSnapshot | None"`
signature shipped an unloadable graph past a fully green suite. Unit tests call
functions directly; nothing ever asked Dagster to resolve the graph's
dependencies, so a signature Dagster cannot load went unnoticed. `dg dev` and
`dagster definitions validate` catch load-time breakage, but neither is run by
`pytest`, so a green suite is not evidence the graph loads.

**The four layers Dagster prescribes** (docs.dagster.io/guides/test):

| Layer | Mechanism | Status here |
|---|---|---|
| Unit | call the asset fn directly | present, extensive |
| Integration | `dg.materialize(assets=[...], resources={...})` | **ABSENT** |
| Job | `job.execute_in_process(instance=DagsterInstance.ephemeral())` | **ABSENT** |
| Runtime DQ | `@asset_check` | present (W8) |

**Fix — two tests, not a suite.** Both are small and high-value:
1. One `dg.materialize(...)` over the **enrichment cycle** (bronze landing →
   conform → silver) against tmp roots, asserting `result.success` and reading a
   real conformed row back. This is the integration layer the manual smoke-slice
   runs have been standing in for.
2. One `execute_in_process(...)` over the **enqueue → submit → harvest** graph on
   an ephemeral instance, so the graph is actually constructed.

**Also add pytest markers.** None are configured (`pyproject.toml`
`[tool.pytest.ini_options]` has only `asyncio_mode` and `testpaths`), so
`pytest tests/` is an undifferentiated ~15-minute monolith with no way to scope
unit vs integration vs e2e. Add `integration`/`e2e`/`slow` markers and default
`addopts = "-m 'not slow'"`.

**Framing note.** This is NOT "add instance testing" — instance-based testing is
already present. The missing layer is specifically **graph assembly**: proving
Dagster can construct and run the graph, which is the only thing that would have
caught the unloadable signature.

### 31. `data/media` has no verified off-repo copy — the one asset nothing can regenerate
>
Listed FIRST among the post-W9 items because it is the only one whose loss is
permanent, and because the queue retirement does NOT close it.

**Status: UNVERIFIED. Open. Carried forward deliberately — not resolved by W9.**

`data/media/` is 27,806 files / **55.36 GB** and is not a cache in the disposable
sense: it is the **only copy** of the scraped bytes. CDN URLs expire in ~4-5 days,
so no re-enrichment and no downstream action recovers it. The plan says this
plainly (`remediation-plan.md:565-567`).

The §3.0 gate records it as **"UNVERIFIED — do not treat as satisfied."** The
owner's belief was *"i think we have the media backed up in a google storage
bucket rn"* — a hypothesis, not a read-back.

**Why W9 was still safe to run.** The drop's blast radius (§3.2,
`remediation-plan.md:612`) is queue + enrichment tables only. `data/media` and
`media_cache` are on the KEEP list, and the retirement verified `media_cache`
intact at 27,748 rows after the drops. The unverified item is genuinely outside
that drop's reach — which is why proceeding on owner approval was defensible, and
also exactly why the retirement must not be read as clearing it.

**What would close it.** List the bucket and spot-read N objects — an actual
read-back, not a recollection. A local file count is not evidence of an off-repo
copy; the count above only proves the bytes are still on this disk, which is the
disk the backup exists to survive.

**Recorded automatically.** `scripts/retire_queue_tables.py` now records §3.0 gate
evidence into the W9 log on every `--apply`/`--rehearse` (step 0), and prints
UNVERIFIED items as carried-forward. See
`data/logs/w9-retirement-20260915T110342Z.json` for the drop that ran before this
recording was added, and subsequent runs for the full record.

### 32. `media_metadata` is dropped but still recreates itself — retirement not durable

**Found 2026-09-15** by the catalog-reconciliation worker, which correctly STOPPED
rather than deleting the spec.

W9 dropped `media_metadata` from live `ops.sqlite`, but the drop is not durable:

- `defs/enrichment/media_cache.py:63` `_ensure_schema()` still executes
  `sqlite_ddl("media_metadata")` — plus two `ALTER TABLE` migrations (`:68`, `:70`)
  — on the live path, so the next call recreates it.
- `tests/unit/instagram/test_migrate_creators_profiles.py:111-113` asserts it
  survives retirement, contradicting the drop.

**Evidence it is dead weight (checked, not assumed):**
- It only ever cached **Gemini File-API uploads** — `file_api_uri`,
  `upload_state = 'uploaded'` (`media_cache.py:583-593`). Gemini batch is
  permanently retired (ADR-0009).
- Its **only** producer is `lookup_or_upload_all` (`media_cache.py:626`), and the
  sole caller is `scripts/experiments/facet_experiment.py:211` — a scratch
  experiment, not a pipeline path.
- It has **no reader** anywhere: the live path now resolves media to scrape-time
  cached local paths (`media_paths.media_urls_to_local_paths`).

**Decision (owner principle: "stalled jobs from the queue we are retiring don't
matter, can delete safely" — applied by analogy):** retire it fully. Remove the
`_ensure_schema` creation and the surviving-table assertion, drop the spec from
`schemas.py`, and add it to `_STALE_SQLITE_TABLES` with a DROPPED hint pointing at
`data/lake/archive/media_metadata/`.

**Acceptance falsifier:** after the change, nothing in `src/` executes DDL or
DML for `media_metadata`, and a fresh ops.sqlite never grows the table.

**Same defect class as the "Retired tables kept coming back (retirement was not durable)" entry (2026-09-15)** (`gold_analyses`/`gold_growth_facets` recreated by
`ensure_gold_analyses` and `_GOLD_FACETS_DDL`): W9 dropped tables whose producers
survived. The "starve, don't drop" control (**C4**, `remediation-plan.md:513`) was
not satisfied
before the drop.

### 33. Full `pytest tests/` run does not finish clean — cause UNVERIFIED

**Observed, 2026-09-15. Cause is NOT established — nothing below is a diagnosis.**

Three full-suite runs on essentially the same tree produced three different shapes:

| Run | Result | Duration |
|---|---|---|
| 1st | collection error — 2 errors, no tests ran (stale `scripts/` paths; since fixed) | 3.85s |
| 2nd | **34 failed, 746 passed, 4 skipped, 4 errors** | 323.72s |
| 3rd | ended in `sqlite3.ProgrammingError: Cannot operate on a closed database` during teardown; **no clean summary line produced** | 996.74s |

The 3rd run's traceback pointed at SQLAlchemy pool teardown
(`pool._dialect.do_rollback` → `dbapi_connection.rollback()`). **That is where the
traceback surfaced, not a verified cause.** No test in `tests/` calls `dispose(`,
configures pool settings, or constructs an engine — `grep` for all three returns
nothing — so the owning connection is unattributed. A plausible-sounding root
cause was deliberately NOT recorded.

**Why its own entry rather than absorption into #30.** `Cannot operate on a closed
database` under parallel/teardown execution is the *shared-connection* failure
shape this branch has already been burned by — the DuckDB single-writer
constraint (ADR-0012 decision 8: "Cannot open file … being used by another
process, observed") and the concurrent-agents-on-one-checkout incident are in that
family. Whether this is the same family is unknown.

**Needed before closing:**
1. Identify the 34 failures and 4 errors **by name** — scoped per-directory runs,
   not another full run (which is what #30's markers exist to make cheap).
2. Test for order dependence (same tests, fixed order vs single-file) — order
   dependence is what would connect it to the shared-connection family.
3. Only then attribute a cause.

**Note on run-to-run variance:** 323.72s vs 996.74s for the same suite is itself a
signal and is unexplained. Both figures are recorded because the variance is part
of the observation.

**Not blocking the migration close-out.** The branch's acceptance evidence is the
verified destination state, not this suite. Recorded so a failing suite is never
mistaken for a green gate.

### 34. W9 must reconcile the 4 `facets_batch_jobs` rows BEFORE the drop

**Found 2026-09-15** reviewing `scripts/retire_queue_tables.py` against the plan
(`remediation-plan.md:485`).

**The gap.** The plan requires the 4 `facets_batch_jobs` rows be reconciled into
the service's job store *before* the drop. The script archives the table to Parquet
then drops it — which preserves the handles in an archive file, but leaves the live
service holding job `2214532df4d3` (1,339 completed / 5 failed / 6,830 pending,
still `processing`) with **no local record pointing at it**.

Job ids are minted service-side, so this ledger is the only local index of which
service jobs matter. After the drop, nothing on this host says which ids are live.
That is the loss of a pointer, not merely of history.

**Fix.** Make reconciliation a *durable output of `--apply`*, never a manual step
that can be skipped. On `--apply`: resolve each of the 4 job ids against the service
store (`~/.qwen-batch/state.sqlite`, table `jobs`, column **`id`** — not `job_id`),
write the mapping `ledger_id → service_state → n_completed/n_failed/n_pending →
disposition` into the W9 log, AND record open-handle ids in ISSUES.md before the
drop proceeds. A Parquet file nobody reads is not a pointer.

**Reconciliation as observed 2026-09-15** (the service store is authoritative and
mutable — re-read at apply time):

| ledger job_id | ledger status | n_req | service state | completed | failed |
|---|---|---|---|---|---|
| `4de3befc10e7…` | JOB_FAILED | 5 | failed | 0 | 5 |
| `58dbe69c5828…` | RETRIEVED | 5 | completed | 5 | 0 |
| `c28c34d0c387…` | RETRIEVED | 98 | completed | 98 | 0 |
| `2214532df4d3…` | SUBMITTED | 8178 | **processing** | 1339 | 5 |

The first three are terminal and need no action. **The fourth is open.**

**Stalled job `2214532df4d34f828adfb90fbf87f253` — OPEN DECISION.** 1,339 completed
/ 5 failed / 6,830 pending / 4 processing; `updated` is ~5.9 days after `created`,
so it is not progressing at the expected rate. **Re-read 2026-09-15 at rehearsal
time: 6,693 pending** — the count moved from 6,830, so the worker is crawling
rather than frozen. That makes "harvest-and-continue" more viable than a stalled
job would; re-check the count immediately before deciding. It landed **zero** bronze rows (live bronze
carries only the 9,576 legacy classification rows). Disposition is one of:
harvest-and-continue (~$2-3 for the remaining ~83%), harvest-and-abandon (keeps the
already-paid 1,339 for free), or declare it a dead pilot explicitly. **Must be
decided before W9 drops the table.**

The script's archive-verify gate and KEEP-set assertion are correct as written —
only the reconciliation output is missing.

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

---

## E-DISCOVERY follow-ons (2026-09-17)

Filed from the E-DISCOVERY epic work (`tasks/epics/creator-discovery/`). All
three are carried by the owner-approved next branch
(`feat/us-disc-7-ingestion-upgrade`) — none is deferred: **#41** (SDK migration)
ships as US-DISC-8, **#43** (the date filter) as US-DISC-7. **#42** is resolved
as NOT a truncation bug (the item endpoint is uncapped; the 1,000 cap is on the
datasets *listing* endpoint) — its residual is a memory concern to fold into #41.

### 41. Replace the hand-rolled Apify client with the official `apify-client` SDK — SCHEDULED (US-DISC-8, 2026-09-17)

**Status:** Scheduled on the `feat/us-disc-7-ingestion-upgrade` branch as **US-DISC-8** (`tasks/epics/creator-discovery/user-stories/US-DISC-8-apify-sdk-migration.md`); US-DISC-6 is retained only as the original statement of intent.

`defs/integration/apify_client.py` is a hand-rolled client (~150 lines: auth,
tenacity retry, three functions) while the official `apify-client` (v3.2.0) is
**not a dependency**. Verified by installing and inspecting the SDK:

| Capability | Hand-rolled | Official SDK |
|---|---|---|
| Trigger / poll | ✅ | ✅ `actor.call()` |
| Retries | ✅ tenacity | ✅ built in |
| `maxTotalChargeUsd` | ✅ (as a **query** param) | ✅ `call(max_total_charge_usd=Decimal)` |
| Dataset fetch | ❌ single blocking GET, **fully buffered in memory**, no pagination | ✅ `dataset.iterate_items()` / `stream_items()` |
| **Actor input schema** | ❌ **none** | ✅ `actor.get()` |
| **Input validation before spending** | ❌ | ✅ `actor.validate_input()` |
| Maintained by | us | Apify |

**This caused a real defect.** The absent schema access is why
`onlyPostsNewerThan` (the actor's date filter, see #43) went unnoticed: our
wrapper could not reveal it, and it was found only by querying the API
directly. A client exposing the input schema makes the next such gap
discoverable.

**Name collision (fix regardless):** the module occupies the exact import name
of the PyPI package (`apify_client`), so adding the official client creates an
import shadow or a confusing two-name space. Rename to something like
`apify_transport.py` / `apify_runs.py` even if the swap is deferred.

**Migration scope is NOT a thin import change — FOUR contracts must move:**

1. **Call shape.** `scrape.py:28` imports three *functions*
   (`trigger_run`, `poll_run`, `stream_dataset`), while the SDK is OO
   (`ApifyClient(token).actor(id).call()`, `.dataset(id).iterate_items()`).
   It is a rewrite of call shapes, not an import swap.
2. **Return contract.** `RunInfo` and `stream_dataset(...) -> int` (item count)
   must be preserved or every caller updated.
3. **Patch target.** `tests/unit/instagram/test_core_refresh.py:42` patches
   `orchestration.defs.integration.apify_client._post` **by module path**. If the
   module is renamed or removed, that patch must move with it — a patch on a
   recreated shim would silently stop intercepting.
4. **Idempotency + streaming must survive.** `bronze_path(dataset_id)` plus the
   exists-check give write-once idempotency keyed on dataset_id; `iterate_items()`
   yields in memory, so the swap must preserve the write-to-Parquet behaviour,
   not just the HTTP calls.

**Do not lose:** `stream_dataset` deliberately uses `format=json` (a JSON
**array**) to *"avoid Apify's NDJSON newline bug"*. That workaround encodes
hard-won knowledge; re-verify it against the current SDK or retain it
explicitly, with evidence either way.

### 42. `stream_dataset` buffers the whole dataset in memory — NOT a truncation bug

**Status:** RESOLVED as not-a-truncation (verified 2026-09-17). Residual: a memory
concern at large item counts.

**Originally filed as** "latent silent truncation" on the theory that
`/datasets/{id}/items` caps its response at 1,000 elements. **That theory is
wrong, and the cap belongs to a different endpoint:**

- **`GET /v2/datasets/:id/items`** — what `stream_dataset` calls. Docs:
  *"No limit exists to how many items can be returned in one response"*;
  `limit` — *"By default there is no limit."*
- **`GET /v2/datasets`** — the dataset *listing*. *"will not return more than
  1000 array elements"*; `limit` default **and maximum** `1000`.

The 1,000 cap is on listing datasets, not retrieving items. **Empirically
confirmed:** the largest live API dataset (`OENbim5qyFy5UFalA`, 912 stored rows)
returned **912** items from `/items?format=json` with no limit param — matching
the stored count exactly. (Datasets above 1,000 could not be tested — three
others returned HTTP errors, likely server-side retention expiry — but the
item-level docs are explicit, so no cap is expected.)

**So: no data loss.** `stream_dataset` is not truncating.

**The real residual (low severity, not a defect):** despite its name,
`stream_dataset` does **not** stream. It does a single blocking GET, then
`json.loads(resp.text)` on the whole response, then writes lines. So peak memory
is proportional to dataset size — fine at ~1,000 items, but a 10,000-item
scrape would buffer the entire payload. `iterate_items()` in the official SDK
addresses this; fold the fix into #41 if the SDK swap happens.

**One caveat to keep if `clean=true` is ever added:** the docs note `clean`
skips empty items and hidden fields, so the response "might contain less items
than the `limit` value". We currently pass only `format=json`, so this does not
apply today.

### 43. Incremental refresh via `onlyPostsNewerThan` (US-DISC-5 → US-DISC-7)

Not yet an issue — recorded here so it is not lost. The actor exposes a date
filter the client does not send; using it makes the weekly refresh genuinely
incremental (1 result for a once-weekly creator vs 7). Carries an explicit
precondition: a creator posting less often than the window is never re-observed
and their metrics freeze silently — mitigate by overlapping the window.
