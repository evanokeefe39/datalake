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

### 6. Dead letter backlog from Gemini quota exhaustion

**Observed:** 10 rows in `dead_letter`:
- 4 `status=skipped` — "Empty caption" (legitimate — profile pages without posts)
- 6 `status=pending` — "429 RESOURCE_EXHAUSTED" (Gemini API free tier quota)

**Impact:** `gold_ig_analyses` has 0 rows — no enrichment happened. The pending
dead_letter entries will need re-processing once a Gemini API key with quota is
available. The skipped entries should be reviewed: are empty-caption profile URLs
expected data, or should they be filtered earlier in the pipeline?