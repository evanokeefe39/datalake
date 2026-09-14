# Audit — Phase 5 (content-classification migration) & Phase 6 (gold marts)
Branch: `feat/enrichment-v3-phase-1-seam-and-landing` · 2026-09-13 · Auditor: AuditP5P6 · READ-ONLY
Verified against the **live** `data/state.duckdb` (`duckdb.connect(..., read_only=True)`), not fixtures.

## Headline
**The orchestrator's "Phase 5 and Phase 6 complete" claims do NOT survive contact with the live database.**
`silver_content_classification` and all four gold marts exist only as code. The live DB still serves
everything from `gold_analyses` (9,576 rows) via `v_post_detail`/`v_overview`. The migration script
(`scripts/migrate_classification_to_silver.py`, commit 4e9cf4b) and the mart/view definitions
(`src/datalake/defs/serving/assets.py`, commit 04ea11d) were **written but never materialized** on
`data/state.duckdb`. The prior architecture review (`review-architecture-soundness.md` L24-25) marks
both "complete" — that claim is false against the live DB.

### Exists / populated — the table (highest-value output)

| Object | Live `data/state.duckdb` status |
|---|---|
| `silver_content_classification` | **DEFINED-IN-CODE-ONLY** (DDL in `classification.py`; migration script never run) |
| `silver_visual_summaries` | **DEFINED-IN-CODE-ONLY** |
| `silver_text_summaries` | **DEFINED-IN-CODE-ONLY** |
| `silver_audio_transcripts` | **DEFINED-IN-CODE-ONLY** |
| `silver_text_annotations` | **DEFINED-IN-CODE-ONLY** (also: mart deps reference it) |
| `silver_visual_annotations` | **DEFINED-IN-CODE-ONLY** (mart deps reference it) |
| `silver_enrichment_quarantine` | **DEFINED-IN-CODE-ONLY** |
| `silver_classification_incoming` | **DEFINED-IN-CODE-ONLY** |
| `gold_post_enrichment` | **DEFINED-IN-CODE-ONLY** |
| `gold_creator_performance` | **DEFINED-IN-CODE-ONLY** |
| `gold_content_shape_performance` | **DEFINED-IN-CODE-ONLY** |
| `gold_top_posts` | **DEFINED-IN-CODE-ONLY** |
| `gold_analyses` (legacy) | **EXISTS-AND-POPULATED — 9,576 rows** (still the live source of truth) |
| `gold_growth_facets` (legacy) | **EXISTS-AND-POPULATED — 205 rows** |
| `silver_ig_post_observations` | EXISTS-AND-POPULATED — 12,701 rows (context, Phase 4) |
| `ig_post_labels` | EXISTS-AND-POPULATED — 10,038 rows (context, Phase 4) |

Additional proof of non-migration:
- `duckdb_views()` shows **3 views read `gold_analyses`** (`v_post_detail`, `v_overview`,
  `analytics_views`) and **0 views read `silver_content_classification`**. Transitive closure over
  the 69 live views that depend on `gold_analyses`: **22 views** — none rebound.
- The schema catalog (`src/datalake/defs/common/schemas.py` → `DUCKDB_TABLES`, consumed by
  `tests/operational/expected_schema.py`) still lists `gold_analyses` and **does not** list
  `silver_content_classification` or any of the four marts. If the migration had run, the catalog
  and the DB would disagree loudly.
- The live `v_post_detail` definition is verbatim `... ga.result_json, ga.analysed_at AS
  gold_analysed_at, ga.prompt_hash ... FROM gold_analyses` — code asset (`serving/assets.py:185+`)
  has the silver-bound version; it was never executed against the live DB.
- The four marts are `CREATE OR REPLACE VIEW ...` in the code — i.e. **not materialized tables even
  in intent**; "row-count reconciliation per mart" is not just unmet, it is inapplicable to the
  written definition (a view has no materialized row count to reconcile).

---

## Phase 5 — Content-classification migration

### C5.1 "`v_post_detail` + `v_overview` serve the same columns from silver."
**UNMET.** Live DB: both views read `gold_analyses` directly (definitions quoted above; 0 views
reference `silver_content_classification`). The silver-bound definitions exist only as Dagster asset
bodies in `serving/assets.py` and were never materialized. Evidence: `duckdb_views()` SQL dump,
2026-09-13.

### C5.2 "The 19 transitive views are unchanged (asserted, not assumed)."
**UNMET (and the "asserted" part is VACUOUS in practice).** Two independent failures:
1. Live transitive closure of `gold_analyses` readers is **22 views**, not 19 — the plan's count is
   itself unverified. All 22 still read the legacy table, so the premise (rebinding happened) is false.
2. No assertion of *unchanged-ness* exists anywhere: `test_state_compatibility.py` only checks that
   views are **SELECT-able** (`SELECT * FROM {view} LIMIT 1`), never that their SQL matches a
   definition baseline or the code asset. A definition could drift arbitrarily and the suite stays
   green. There is no definition-equality or snapshot test.

### C5.3 "`test_state_compatibility.py` green — the no-regression gate."
**MET but VACUOUS as a Phase 5 gate.** The suite passes live (77 passed, verified this audit). But it
passes **because nothing changed**: the catalog it tests against (`DUCKDB_TABLES`) still expects
`gold_analyses` and does not expect `silver_content_classification` or the marts. If the migration
*had* been run, the extra silver/mart objects would merely trigger a non-failing warning and the
rebound views would still query fine against silver — i.e. the gate cannot fail on this migration.
Green here is evidence of *status quo*, not of *no-regression-after-migration*.

### C5.4 "Row-count reconciliation: `gold_analyses` rows accounted for in `silver_content_classification` or explicitly quarantined."
**UNMET.** `gold_analyses`: 9,576 rows (live count). `silver_content_classification`: table does not
exist — 0 accounted, 0 quarantined (`silver_enrichment_quarantine` also absent). The migration script
and its quarantine schema exist in code only. The "9,576 accounted" figure in the plan is a plan
claim, not a live fact; the true reconciliation ledger is: **9,576 in legacy, 0 migrated.**

### C5.5 "The dashboard is unaffected (verified — it reads views only, never the table by name)."
**MET.** `dashboard/server.py` queries only views (`v_overview`, `v_signal`, `v_post_detail`,
`v_post_metrics`, `v_standout_calendar`, `v_recent_hot_posts`, `v_creator_*`, …); zero references to
`gold_analyses` or `silver_content_classification` by name (grep-verified). Caveat: the dashboard is
unaffected precisely because it reads views — but those views still serve legacy `gold_analyses`, so
"unaffected" today means "still on the legacy source".

---

## Phase 6 — Gold marts

The four marts in code (`serving/assets.py`): `gold_post_enrichment` (L1223),
`gold_creator_performance` (L1351), `gold_content_shape_performance` (L1441), `gold_top_posts` (L1601).

### C6.1 "Four marts materialized; a row-count reconciliation per mart."
**UNMET.** All four are **absent from the live DB** (`Catalog Error` on each). They are defined as
`CREATE OR REPLACE VIEW`, not tables — so even the code's notion is "view, recomputed on read",
which is a legitimate choice but does not satisfy "materialized", and no reconciliation report
exists. Additionally, `gold_content_shape_performance` depends on `silver_visual_annotations` /
`silver_text_annotations` — which are also code-only — so it *could not* materialize even if run.

### C6.2 "No mart restates a canonical-view metric (checked, not asserted)."
**PARTIALLY MET (by code inspection only; unverifiable live).** Reading the definitions:
`avg_engagement_z` = `AVG(v_post_metrics.engagement_score)` — canonical, not re-derived ✓.
`standout_rate` = `AVG(is_standout)` ✓. `gold_top_posts` flows `engagement_score` straight through;
`overall_rank`/`overall_percentile` are new positional projections, not restatements ✓.
`lift_vs_slice_baseline` is a mart-local ratio over canonical values ✓. Constants (tier buckets,
momentum) are not restated ✓. BUT: there is **no automated check** ("checked" is a human reading),
and no live materialization exists to check against. Status: supported by inspection, not asserted.

### Known finding verified — `gold_content_shape_performance` PK/grain omits `platform`
**CONFIRMED against the actual definition.** The facets CTE carries `post_id, platform` and joins
`v_post_detail pd ON pd.post_id = f.post_id AND pd.channel = f.platform`, but the final `cells` grain
is `(gold_domain, gold_topic, follower_tier, facet_name, facet_value)` — **no `platform`, and no
`post_id`** in the grouping. Consequences verified from the SQL: (a) two platforms hitting the same
`(domain, topic, tier, facet_name, facet_value)` collapse into one cell; (b) since facet rows are
per-post and the grain excludes the post, `n_posts` is the only post-level signal and per-platform
drill-down is impossible from this mart. (Compounded by it being a view, so there is no declared PK
at all — the omission is in the logical grain.)

---

## Count by status (9 criteria: 5 × Phase 5, 2 × Phase 6, plus 2 findings)

| Verdict | Count | Criteria |
|---|---|---|
| MET | 2 | C5.3 (vacuously), C5.5 |
| PARTIALLY MET | 1 | C6.2 |
| UNMET | 5 | C5.1, C5.2, C5.4, C6.1, (+C6.1's reconciliation) |
| VACUOUS (gate passes for wrong reason) | 1 | C5.3 as "no-regression gate"; C5.2's "asserted" clause |
| Findings confirmed | 2 | transitive-view count is 22 not 19; `platform` omitted from mart grain |

## Verdict
Phase 5: **UNMET in full against the live DB** (dashboard-only criterion met). Phase 6: **UNMET** —
zero of four marts exist on the live DB. The orchestrator's "phase complete" claims reflect the
*code branch* (commit 04ea11d), not the *system state*. Every `serving`/`conform` asset in commits
4e9cf4b and 04ea11d is write-never-executed against `data/state.duckdb`.
