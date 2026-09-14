# Three-state articulation: Enrichment v3

Source for the architecture diagrams. Every fact below is verified against the repo, the live
databases, and the audits in `data/dev/`. Do not invent nodes or edges.

---

## STATE 1 — OLD (the state before the migration; it WORKED)

This is the pipeline that ran and produced real output. It is not a failure state. It is the
baseline that must not be lost while the new one is built.

**Orchestration:** a SQLite work queue in `data/ops.sqlite`.

**Flow:**

1. **Apify scrape** → `bronze` Parquet (NDJSON → Polars → Parquet)
2. **`ig_posts_raw` (bronze asset)** → writes Parquet under `data/lake/bronze/`
3. **`ig_posts_slv` (silver asset)** → `silver_ig_posts` in `data/state.duckdb` (deduped,
   normalized; watermark in the generic `watermarks` table)
4. **`ig_post_labels`** → the discovery source; every post is labelled with an
   `enrich_decision` (standout / control / floor_filler / skip) and a `label_version`
5. **`ig_posts_gen_batches`** → writes a `batch_jobs` row + `batch_items` rows into
   `data/ops.sqlite`. This is the WORK QUEUE. No API calls; sub-millisecond.
6. **`submit_gemini_batches_job`** → reads `batch_jobs` (status=pending) → uploads media to the
   Gemini File API → submits a Gemini batch
7. **`gemini_batch_harvest`** (driven by `gemini_batch_harvest_sensor` when enabled) → polls
   Gemini, retrieves results
8. **`gold_analyses`** (DuckDB, 9,576 rows live) + **`gold_growth_facets`** (205 rows live)
9. **Serving**: `v_post_detail` and ~21 further views read **`gold_analyses` directly**
   (`ga.result_json`, `ga.prompt_hash`)
10. **Failures** → `dead_letter` table (776 rows live)

**Evidence it worked:** `gold_analyses` holds 9,576 rows; `gold_growth_facets` 205;
`silver_ig_posts` 10,038; `ig_post_labels` 10,038. The dashboard reads the views.

**Its known limitations (what motivated the migration):**
- IG-only. The PK is `(post_id, domain)` and `domain` is hardcoded `'instagram'`.
- Text-only in practice: the silver layer hardcoded `media_files = "[]"`.
- Every batch job's state lives in a bespoke queue that exists for one path.

---

## STATE 2 — CURRENT / INCORRECT (what is on the branch today)

**The defining characteristic: the code branch is green and the system does not run.** Twelve
objects exist as code and are materialized nowhere. The live database is still STATE 1.

**What the code now declares (materialized: NONE of it):**
- `bronze_enrichment_raw` (verbatim landing, keyed `(post_id, platform, workload, prompt_hash, run_id)`)
- Six silver tables: `silver_visual_annotations`, `silver_visual_summaries`,
  `silver_audio_transcripts`, `silver_text_annotations`, `silver_text_summaries`,
  `silver_content_classification`
- `silver_enrichment_quarantine`, `silver_classification_incoming`
- Four gold marts: `gold_post_enrichment`, `gold_creator_performance`,
  `gold_content_shape_performance`, `gold_top_posts` (all `CREATE OR REPLACE VIEW` in code)

**What the live database actually holds:** `gold_analyses` (9,576) and `gold_growth_facets`
(205) — i.e. **still STATE 1**. Zero views read `silver_content_classification`; three still
read `gold_analyses`.

**The four specific breakages — draw these, they are the point:**

1. **DISCONNECTED HALVES.** The drain materializes `enrichment_submitted` Dagster partitions
   (`instagram/assets.py:1289`) that **nothing consumes**. `submit.py:77` discovers work by
   reading `batch_jobs` — a table **nothing writes**. There is no edge between them. Show the
   producer and the consumer NOT touching, with a visible gap.

2. **THE MISSING PRODUCER.** `enrichment_harvested` has **no production writer** anywhere in
   `src/`. The in-flight set is computed as `submitted − harvested`, so it only ever grows:
   after the first cycle the guard suppresses everything and the pipeline **stalls**.

3. **THE DEAD CONTRACT, KEPT ALIVE BY CODE.** `_require_legacy_queue_tables`
   (`batch.py:51`) actively **raises** if the retired `batch_jobs` table is absent. The zombie
   path is preserved by the code that was supposed to retire it.

4. **THE BYPASSED SEAM.** One seam (a `ProviderAdapter` port) was built. **1 of 4 production
   flows uses it.** 12 call sites bypass it: `submit.py:152`, `submit.py:170`, eight sites in
   `harvest.py` (311, 316, 319, 334, 446, 454, 459, 462), `facets_batch.py:318`,
   `facets_batch.py:322`. `seam.run_lifecycle` has **zero** production callers. Draw the port
   with the adapters attached but only ONE wire going out to a production caller, and 12 wires
   going around it.

**Also show, as a small inset:** `bronze_enrichment_raw` has **zero landed rows**, and the test
mocks use a FLAT envelope while real provider responses NEST — so the landing has never met a
real payload shape.

**And the vacuous gate:** `tests/operational/test_state_compatibility.py` passes (77 tests)
because the schema catalog still lists `gold_analyses` and does not expect
`silver_content_classification`. It asserts the status quo, so it cannot detect that the
migration never happened. Draw it as a green gate whose input is the OLD catalog and whose
output is "PASS" — it is certifying a world that no longer matches intent.

**Root cause, for a caption:** integration was treated as an emergent property of component
completeness rather than an owned, tested deliverable. Per-component dispatch under
per-component acceptance cannot detect inter-component drift. 7 of 36 exit criteria are genuinely
met.

---

## STATE 3 — TARGET (ADR-0011 / ADR-0012 / ADR-0013; not yet built)

The layered model. Clean, one direction, no queue.

**Flow (top to bottom):**

1. **Providers** (Gemini batch, qwen-batch-service) — reached ONLY through the seam
2. **SEAM** — `ProviderAdapter` port; `build_adapter(name)` is the ONLY place a provider is
   named. Three verbs: `submit` / `poll-to-terminal` / `retrieve`. One canonical state
   vocabulary: `pending` / `processing` / `completed` / `failed`.
3. **`bronze_enrichment_raw`** — EVERY external model response landed VERBATIM, append-only,
   immutable. Keyed `(post_id, platform, workload, prompt_hash, run_id)`. **The paid and
   stochastic part ENDS here.**
4. **Conform** — deterministic, **ZERO API calls**. A pure function of bronze. A schema or
   mapping change is a REPLAY, never a re-bill.
5. **Six silver tables** — keyed `(post_id, platform)` — never `domain` — each carrying its own
   provenance (`provider`, `model`, `prompt_hash`, `schema_version`, `run_id`):
   `silver_visual_annotations`, `silver_visual_summaries`, `silver_audio_transcripts`,
   `silver_text_annotations`, `silver_text_summaries`, `silver_content_classification`.
   Validation lives here, with LOUD quarantine on terminal failure.
6. **Four gold marts** — compose the canonical metric views; never re-derive a metric.
   `gold_post_enrichment`, `gold_creator_performance`, `gold_content_shape_performance`,
   `gold_top_posts`.
7. **Serving** — the ~22 views read silver/marts, never a legacy table.
8. **Orchestration (ADR-0012)** — state lives in the Dagster instance and the lake. The
   `ops.sqlite` queue is RETIRED. Retry is a new partition key; failures surface via the
   anti-join `landed(bronze) ∖ conformed(silver)` plus a BLOCKING asset check.
9. **`ops.sqlite` retains only:** `media_cache`, `media_metadata`, `creators`, `profiles`,
   `creator_merges`, `prompt_registry`.

**The load-bearing words to label on the edges:**
- Providers → SEAM: "the only provider-named boundary"
- SEAM → bronze: "verbatim, append-only — the paid part ends here"
- bronze → conform: "deterministic, zero API calls — replay, never re-bill"
- conform → silver: "keyed (post_id, platform), never domain"
- silver → marts: "compose canonical metrics, never re-derive"
- Orchestration → all: "Dagster-native; the queue is retired"

---

## Visual guidance

- Use **`dataflow`** for the pipeline states (1 → 2 → 3 as three separate panels or one figure
  with three columns). Use **`architecture`** if showing the seam/port/adapter structure.
- **STATE 2 is the one that matters most.** It must make the four breakages visually obvious —
  especially the disconnected halves, which should read as two chains that visibly do not meet.
- Colour semantics: state 1 neutral/working, state 2 red/incorrect, state 3 blue/target.
- Keep node counts low (≤12 primary per diagram). Label the meaningful edges.
- Every label must be a fact from this document. Do not invent components.
