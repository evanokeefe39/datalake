---
id: US-EENG-3
epic: E-ENRICH-ENGINE
persona: P1
status: Open
---
# US-EENG-3 — Land verbatim model responses and converge the two lifecycles on one seam

- **Epic:** E-ENRICH-ENGINE
- **Status:** Open
- **Relates to:** US-EENG-1 (the service-backed executor rides this seam),
  US-EENG-2 (the fail-loud health gate the submit path uses), US-EFAC-1/3/4
  (Phase 4 silver conform is impossible without the landing)
- **Source:** `tasks/plans/enrichment-v3-migration-master.md` (Phase 1),
  `tasks/plans/inference-service-seam.md` (§4.1, §4.2 — "the economic point"),
  ADR-0011 (bronze landing), ADR-0012 (Dagster-native orchestration),
  ADR-0013 (the seam keeps no ledger),
  `docs/architecture/pipelines/enrichment.md` §3 (`bronze_enrichment_raw`
  columns/keys)
- **Migration phase:** Phase 1 — the seam and the landing (foundation). Master
  plan: `tasks/plans/enrichment-v3-migration-master.md` §3 "Phase 1".
- **Settled contract (2026-09-10, ADR-0013):** there is NO
  `external_jobs`/ledger table anywhere. The qwen-batch-service owns its job
  store and Dagster polls it over HTTP (`POST /jobs`, `GET /jobs/{id}`,
  `GET /jobs/{id}/results`); Dagster's in-flight set is instance-native
  (`materialized(submitted) − materialized(harvested)`); the lake's record of
  what succeeded is `landed(bronze_enrichment_raw) ∖ conformed(silver)`.

## Story

**As a** pipeline operator, **I want** every external model response to land
verbatim in `bronze_enrichment_raw` (immutable, append-only) and both of today's
independent provider lifecycles to converge on ONE `ProviderAdapter` seam
(one lifecycle: submit → poll-to-terminal → retrieve, one canonical state
vocabulary, `build_adapter(name)` the only place a provider is named),
**so that** a schema or mapping change never re-calls the paid model (the
economic point of ADR-0011), a third workload cannot fork the pattern again,
and swapping providers is a config string.

## Acceptance criteria (binary)

The landing:

- AC1: `bronze_enrichment_raw` exists in the lake (Parquet, append-only) with
      exactly the columns of `docs/architecture/pipelines/enrichment.md` §3 —
      `post_id`, `platform`, `workload`, `provider`, `model`, `prompt_hash`,
      `schema_version`, `run_id`, `analysed_at`, `input_modality`,
      `sampling_params_json`, `response_text` (verbatim, unmodified),
      `request_echo_json` — idempotent on
      `(post_id, platform, workload, prompt_hash, run_id)`; re-harvesting the
      same run writes no duplicate rows.
- AC2: For one real call per workload (`qwen-vision`, `text-LLM`), harvest
      writes the verbatim `response_text` to `bronze_enrichment_raw`; the
      stored bytes are byte-identical to what the provider returned (spot-
      check against a captured envelope).
- AC3: A deliberately terminated/unparseable item lands in
      `bronze_enrichment_raw` with its explicit `ok`/status column set to the
      failure state (plus the error detail), so failure is READ from a column,
      never inferred from a missing conformed row — and the
      `write_conformed`-never-conforms-a-failed-item invariant is asserted by
      a test (the spike S5 recommendation, per ADR-0013 consequences).
- AC4: Re-conforming silver from an existing `bronze_enrichment_raw` makes
      ZERO API calls, proven by a real run with the provider disabled.

The seam:

- AC5: `build_adapter(name)` is the ONLY place in the production codebase a
      provider is named — swapping `service_backed` ↔ `direct_batch` is a
      config string, proven by a test that drives the full lifecycle through
      both without touching any other module (per the seam plan §5).
- AC6: ONE `run_lifecycle` implementation exists (submit → poll-to-terminal →
      retrieve); `submit`/`poll`/`retrieve` exist once in production code, not
      twice — verified by grep/AST assertion in a test.
- AC7: One canonical state vocabulary (`pending`/`processing`/`completed`/
      `failed`): no downstream consumer ever sees a provider-native state
      string; both providers' real envelopes (Gemini's
      `{custom_key: {ok, text, error}}` and the qwen service's `{items: [...]}`)
      parse through the same provider-neutral `Result` type — tested against
      CAPTURED real responses from BOTH providers, not the mock.
- AC8: Consumers branch on `Capabilities`, never on the adapter's name; no
      Gemini-specific code is reachable from the qwen path (the qwen service's
      async job model is the `ServiceBackedAdapter` contract).
- AC9: The Gemini path runs on a `DirectBatchAdapter` and the qwen path on a
      `ServiceBackedAdapter`, both over the same lifecycle; the Gemini path is
      kept as a tested second implementation (the seam plan §4.5 regression
      fixture), not deleted.
- AC10: Submit intent is recorded before the billed POST as the
      `enrichment_submitted` partition materialization (placeholder-before-
      POST semantics per the seam plan §4.1) — a killed submit leaves an
      inspectable record, proven by a test, with no ledger table involved.
- AC11: NO ledger table exists: `external_jobs` is NOT created anywhere in the
      lake or `ops.sqlite` (ADR-0013); a test asserts its absence by name, as
      spike S5 did.
- AC12: `facets_batch_jobs` is dropped ONLY after every one of its 4 rows is
      reconciled into the qwen-batch-service's own job store or explicitly
      accounted for as in-flight work — reconcile-before-drop proven by a
      migration step that fails loudly on an unreconciled row.
- AC13: One submit per pass fans out at conform to every table that pass
      produced (visual → `silver_visual_annotations` + `silver_visual_summaries`
      shape, per the seam plan §4.3); never one submit per table.
- AC14: `facets.py`'s false docstring claiming it submits to the Gemini Batch
      API is corrected (it submits to the qwen service).

## Definition of done

- [ ] One real submit → poll-to-terminal → retrieve → verbatim landing cycle
      per workload, end-to-end, writes `bronze_enrichment_raw` rows with full
      provenance (`provider`, `model`, `prompt_hash`, `schema_version`,
      `run_id`, `analysed_at`).
- [ ] `facets_batch_jobs` retired with its 4 rows accounted for; the seam's
      in-flight set is the Dagster instance's
      `materialized(submitted) − materialized(harvested)`.
- [ ] Scoped tests pass (landing idempotency, dual-provider `Result` parsing,
      swap-by-string, no-ledger negative assertion, failure-is-read); no
      regression to the existing harvest paths.
- [ ] Schema catalog + readiness green for the bronze rows.

## Tests

- Re-harvest of the same job idempotent on the natural key — no duplicate
  `bronze_enrichment_raw` rows.
- A real envelope from BOTH providers parses through the same `Result` type;
  the mock (flat `items[]`) is never the only fixture.
- Re-conform from bronze with the provider unreachable — zero network calls,
  silver output unchanged (the economic-point proof).
- A failed item's `ok`/status is readable in bronze, and a test asserts the
  conform step skips it loudly (never a silent empty error queue behind a
  green pipeline).
- Swap-by-string: both adapters drive the same lifecycle in one test run.
- No-ledger negative assertion: `external_jobs` is absent from the lake and
  `ops.sqlite`, by name (spike S5 pattern).
- Reconcile-before-drop: dropping `facets_batch_jobs` with an unreconciled
  row fails loudly.
