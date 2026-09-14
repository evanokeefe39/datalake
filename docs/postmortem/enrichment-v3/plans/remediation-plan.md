# Enrichment v3 — Remediation Plan

Planning only. Branch `feat/enrichment-v3-phase-1-seam-and-landing`.

**Currency note, 2026-09-14.** The plan was written against HEAD `04ea11d`, the commit the
post-mortem audited. The branch has since advanced to `461f5e3`, which sits on top of `952386d`
("checkpoint the queue-retirement slices"). Three facts below have changed and are corrected
inline where they appear; re-verified against the working tree and the live databases:

- **The queue DDL is gone from code.** `batch.py` was rewritten in `952386d`: it no longer
  creates `batch_jobs`/`batch_items`, and carries an explicit DISPOSITION docstring describing
  the legacy functions as deliberate transitional SHIMS that fail loudly outside a legacy
  database. `_require_legacy_queue_tables` is therefore no longer a zombie contract kept alive
  by the code meant to retire it — it is a fail-loud precondition on transitional code. The
  *dependency* is unchanged: five modules still call those shims, so the queue cannot be
  dropped. Read this as **retired but still called**, a migration-ordering problem.
- **The queue-retirement slices are committed, not uncommitted.** W2's "land or discard the
  uncommitted changes" is done; the tree is clean.
- **CORRECTED 2026-09-14 (this bullet was wrong).** The earlier note here inferred "the suite
  is not 25-red" from running the TWO files W2 happened to name. That was a scope error: those
  two files are green, but they are not the suite. A full `uv run pytest tests/` run gives
  **26 failed, 657 passed, 2 skipped, 31 errors in 422s** — 57 red, spread across 16 files in
  unit, integration, e2e, and operational. The 25-red figure was REAL, not stale; the commit
  message that produced it scoped itself to `tests/unit/instagram/` but the actual red is
  broader and centred on `tests/unit/enrichment/`. The dominant uniform cause is the same one
  the commit message named: tests still construct or require the retired queue tables, so they
  die on `RuntimeError: ops.sqlite queue retirement (ADR-0012): legacy queue tables
  ['batch_jobs', 'batch_items'] do not exist`, `sqlite3.OperationalError: no such table:
  batch_items`, or `KeyError: 'batch_jobs'`. The fail_item harness bug was one red test among
  57; fixing it was necessary and nowhere near sufficient. **Lesson: characterize a suite from
  a full run, never from the files a plan names** — a named-file sample is a hypothesis about
  where the red is, not a measurement of it.

Everything else in this plan was verified against the live databases and still holds: the live
`state.duckdb` contains **zero** new-model objects, `enrichment_harvested` still has no
production writer anywhere in `src/`, the drain still materializes `enrichment_submitted`
(`instagram/assets.py:1087`), `submit.py:77` still reads `batch_jobs`, and the seam bypass
stands (`submit.py:152` and `harvest.py:334` call `gemini_batch` directly; only
`facets_batch.py:292` goes through `build_adapter`). 7/36 exit criteria genuinely met.

Inputs: [`../analysis/three-state-articulation.md`](../analysis/three-state-articulation.md) (target), [`../postmortem.md`](../postmortem.md)
§10, the three phase audits, [`../analysis/learnings-mece.md`](../analysis/learnings-mece.md) (C1–C5 control set), [`../panel/data.md`](../panel/data.md),
[`../panel/adversary.md`](../panel/adversary.md), `docs/architecture/pipelines/enrichment.md`, ADR-0011/0012/0013, master plan §3/§4/§8.

---

## 1. The fork in the road

### The three options, stated plainly

**(a) FINISH the migration as designed.** The code for all of ADR-0011's statics exists (seam,
landing, conform, six silver tables, four marts, silver-bound views) and is green under its own
tests. What is missing is *wiring and dynamics*: the drain→submit handoff, the
`enrichment_harvested` producer, the retry driver, the quarantine consumer, the seam call-site
adoption, and materialization of 12 objects against the live DB. Nothing audited says the
built code is wrong — it says it is unwired, unrun, and unobserved.

**(b) REVERT serving to `gold_analyses` and rebuild silver with process fixes.** The live DB is
already effectively state 1 (`gold_analyses` 9,576 rows, 22 views reading it, dashboard green),
so "revert" is really "abandon the branch". This discards: a correct prompt-identity scheme
(the entire legacy corpus is cleanly distinguishable by hash — audit P3-3), an idempotent
verbatim landing that is the one pattern implemented correctly (`test_harvest_landing.py:162`),
a conform layer that is provably API-free as coded, and the four mart definitions. The old
world's known limitations (hardcoded `domain`, `media_files = "[]"`, bespoke one-path queue)
are exactly what motivated the migration.

**(c) HYBRID — validation spike first, then decide on evidence.** One real bronze row driven
end-to-end (drain → submit → land verbatim → conform → silver → mart → view → drain run 2
suppression), on a subset (`--max-pages 1` / single post), before any further migration work.

### Assessment against the live data

- The live world is intact and serving: `gold_analyses` 9,576 rows, `gold_growth_facets` 205,
  22 transitive views, dashboard reads views only. There is no production outage to recover
  from — option (b)'s urgency is zero.
- Zero bronze rows means the migration has never met reality, but the *code-side* audits found
  no structural flaw in the built modules — the failures are all inter-connection and
  non-materialization (audits P2-1/P2-2, P4 #4/#5, P5/P6: "written but never materialized").
- The asymmetry the brief names is decisive: **ADR-0011 (statics: layering, seam, verbatim
  landing, deterministic conform) is sound and its code exists. ADR-0012 (dynamics: retry
  driver, quarantine consumer, harvested transition) is underspecified — those are design gaps
  that would have surfaced under any dispatch quality** ([`../panel/data.md`](../panel/data.md) §5). You cannot "revert
  your way out of" an underspecified dynamic; rebuilding silver would reproduce the same gaps
  unless ADR-0012 is completed first — which is exactly what option (a) must do anyway.

### Recommendation: **(c), converging to (a)**

Run the validation spike (W1) before any further work, with the ADR-0012 dynamic specs
(retry driver, quarantine disposition, harvested transition) written as part of the spike
preparation — the spike *is* the instrument that resolves the underspecification, because a
driver cannot be chosen on paper without seeing a real cycle. If the spike demonstrates the
round-trip (expected: the code is green and only unwired), proceed to FINISH (option a) with
the unit breakdown below. If the spike exposes structural breakage in the statics (not just
wiring), that is the first evidence ever obtained that would justify a (b)-style rebuild, and
the decision goes to the human with that evidence. Do **not** revert now: (b) destroys
reusable correct work to escape an orchestration problem whose fixes (§2) are independent of
the codebase choice.

**[HUMAN DECISION]**: adopt (c) with convergence to (a); spike budget (~1 real post, paid API
call) and abort criteria must be approved before any spend.

---

## 2. Ordered work breakdown

Every unit states its target files (≤5), declared dependencies, and an acceptance that is a
**demonstrated round-trip**, never an existence claim. Each unit names its MECE control (C1
specification / C2 accountability / C3 interface / C4 sequencing / C5 verification) per the
[`../analysis/learnings-mece.md`](../analysis/learnings-mece.md) §2 map, so every defect row of §2 there is covered exactly once.

### W0 — ADR-0012 dynamic specification (drives the fork decision)
- **What**: Write the three missing dynamic specs as an ADR-0012 addendum: (i) the *driver*
  that materializes `enrichment_harvested` (actor + trigger), (ii) the retry driver (who mints
  round N, when; partition-key round shape that both producer and consumer can derive), (iii)
  the quarantine disposition policy (consumer, triage cadence, retention, redrive rule). Also
  fix the partition-key shape requirement: the round dimension must survive derivation (no
  one-way digest fusing it).
- **Files**: `docs/architecture/adr/0012-dagster-native-orchestration.md`, `docs/architecture/adr/0014-orchestration-dynamics.md`, `tasks/plans/enrichment-v3-migration-master.md` (§3 sequencing note only).
- **Deps**: none. Blocks W1's fork decision.
- **Control**: C1 (three mechanisms existed with no driver/consumer).
- **Acceptance (round-trip)**: each spec answers "name the production consumer of everything
  you write and the production producer of everything you read" for retry, quarantine, and
  harvested — i.e., a worked cycle on paper: a failed submit produces a round-1 partition key,
  the retry driver consumes it, and the drain guard releases the post; a malformed response
  lands in quarantine, is read by its named consumer, and redrives. **[HUMAN APPROVAL: the
  spec sign-off gates the fork decision.]**

### W-FREEZE — Retire the Gemini path and freeze the old write path
**DECIDED 2026-09-14 (owner):** Gemini batch usage is **retired for the foreseeable future** —
not parked, not pending re-admission. *"id rather completely bin gemini batch usage and finish
the migration swiftly… lets assume we aren't gonna use gemini batch for the foreseable future.
if we need to re-enrich everything so be it."*

**The re-admission story is DROPPED, deliberately.** Panel round 2 returned the re-admission half
as *"a promissory note with no scheduled trigger, owner, or unit"* — and the owner has now chosen
retirement over a promise. This retires the panel's §5 conditions by resolving the question the
other way: there is nothing to trigger. Consequences:
- The panel's four interface gaps (handle lifetime, dual `max_tokens`, encoding, HTTP taxonomy)
  stay **unfixed** and are recorded as *known limits of the seam*, not as W5 work. W5 keeps only
  the two live qwen sites. That is a real reduction in scope and it is the point.
- `gemini_batch.py` + `DirectBatchAdapter` are kept as **inert code** (deleting them is optional
  cleanup, not required). No test asserts they construct — `build_adapter("gemini")` was never a
  real key anyway (`direct_batch` is), and the panel classified the adapter **PARTIAL**:
  never run over the wire, every test stubs the SDK.
- **If Gemini returns, it returns as a rewrite against then-current needs, not as a re-admission
  through this seam.** Say that plainly so nobody later mistakes inert code for a capability.

Owner decision: stop maintaining the intermediate stages. Rather than run the old and new
paths side by side through a long migration, **freeze the old write path and cut over.**
Gemini-batch usage is retired; the Gemini code (the client and `DirectBatchAdapter`) stays on
disk as inert code — see the retirement framing above. It is NOT a supported seam provider in
any operational sense: no entry point references it, no test exercises it over the wire, and
the seam's lifecycle was never fixed to accommodate it.

**This is a precondition, ordered BEFORE W1, not a step inside the migration** — the spike and
W6's reconciliation both measure against `gold_analyses`, and a baseline that can still move is
not a baseline.

- **What**: (1) Disable/retire the Dagster entry points that drive the old path —
  `submit_gemini_batches_job` (`submit.py:227`) and `gemini_batch_harvest_sensor`
  (`harvest.py:419`) — so nothing can write `gold_analyses` again. (2) Disposition the stuck
  artifacts: `batch_jobs` id=6 has sat in `processing` since 2026-09-05 and will never complete;
  record it as abandoned. The 776 `dead_letter` rows are terminal history for a retired path —
  leave them, they are W9 archive material. (3) Record the frozen `gold_analyses` count as the
  migration baseline.
- **Why it is safe (measured 2026-09-14, not assumed)**: the old path is **already dormant**.
  Zero instigator records in the Dagster instance (nothing enabled); last `batch_jobs` write
  2026-09-05, last `dead_letter` 2026-09-08, last `facets_batch_jobs` 2026-09-09. This unit
  formalizes an existing fact rather than stopping a running pipeline — which is exactly why it
  is cheap and why leaving it implicit is fragile (nothing but convention stops a manual trigger).
- **Files**: `src/datalake/defs/enrichment/submit.py`, `src/datalake/defs/enrichment/harvest.py`,
  `src/datalake/defs/enrichment/__init__.py`, `data/ops.sqlite` (via script only).
- **Deps**: W0 (so the retirement is recorded against the spec). **Blocks W1** — the freeze
  precedes any measurement.
- **Control**: C4 (sequencing) + C1 (the old path's end state must be declared, not inferred).
- **Acceptance (round-trip)**: the entry points are gone from the Dagster definitions
  (observed by registry listing, not by reading source); a manual attempt to write
  `gold_analyses` fails loudly rather than silently succeeding; the frozen count is recorded
  with the date it was read. Then: `gold_analyses` count is unchanged after 24h of normal
  operation — a cheap empirical proof that nothing writes it.
- **⚠ This unit CANNOT LAND ALONE — it creates the stall by construction.** Established by the
  panel round 2 orchestration seat (see `panel/readmission/SYNTHESIS.md` §1). With the Gemini
  submitter gone and no qwen submitter wired yet, **one drain run materializes the entire
  approved corpus as `enrichment_submitted`**; in-flight (`submitted − harvested`) then never
  shrinks, the drain guard suppresses every future run, and the only signal is a **warning log**
  (`assets.py:1256-1261`; trace `1083-1092,1289` → `partitions.py:230-236` → `1228-1244`). This
  is the same stall mechanism the post-mortem identified as the migration's signature failure.
  Mitigating facts, all verified: `check_enrichment_health` can fire on `approved_unenriched > 20`
  (though it prescribes the wrong remedy and nothing ticks it); the schedule that would trigger it
  (`daily_medallion` → the drain) ships stopped; recovery is automatic once W3/W4 land. But it is
  **one UI toggle away**.
  **Therefore W-FREEZE must be paired with either W4's harvested producer or an explicit
  submitter-quiet guard** (a drain-side refusal to materialize when no submitter is registered).
  Freezing the writer without wiring the reader is not a partial state — it is a stall.
- **Owner decision supersedes the panel's trigger requirement (2026-09-14):** retirement was
  chosen over re-admission, so there is no trigger to name. The panel's finding stands as the
  reason the choice had to be made explicitly rather than left as "temporarily" — and the panel
  is the reason the choice was cheap to make, having established that the delete half was sound
  and the re-admission half was not yet real.

**Consequence 1 — W5 shrinks by ~80%.** Measured against the working tree: of the bypass call
sites the plan enumerated, **9 are Gemini and only 2 are qwen**
(`facets_batch.py:318,322`). Binning Gemini usage converts nine *migrations* into nine
*deletions*, because those sites are the bypass being removed anyway:

| Site | Was (W5) | Becomes |
|---|---|---|
| `submit.py:152` (`gemini_batch.submit`) | migrate to the seam | **delete** with the retired entry point |
| `harvest.py:311,316,319,334` (poll/job_state/is_terminal/retrieve) | migrate | **delete** |
| `harvest.py:446,454,459,462` (second poll path) | migrate | **delete** |
| `facets_batch.py:292` (`qwen_client`, via `build_adapter`) | migrate to the seam | **migrate** (unchanged — this is the live path) |

The seam's `DirectBatchAdapter` already wraps Gemini, so the *capability* is not lost — only the
direct, un-seamed calls. That is the same thing W5 was trying to achieve, reached by deletion
instead of adaptation. W5 keeps its CI grep (provider names only inside adapter modules) and
its qwen transport consolidation; it loses the nine-site migration.

**Consequence 2 — the coexistence window closes.** W2/W3 no longer have to keep a live old write
path green while building the new one, and the "6 live `batch_jobs` rows reconciled before the
read-path dies" item in §3 becomes a static reconciliation against a frozen set rather than a
race. W6's reconciliation identity now has a stable right-hand side.

**Consequence 3 — the Gemini code stays on disk, inert, with NO test guarding it.**
**SUPERSEDED 2026-09-14 by the owner's retirement decision.** Two corrections to the draft this
replaces, both from panel round 2:

- The registry name is **`direct_batch`**, not `"gemini"` — `seam.register_adapter("service_backed", …)`
  is the only registration in `seam.py` (:113,:119). An earlier draft specified a test for
  `build_adapter("gemini")`: a key that does not exist, which would have failed on a *correct*
  implementation. That draft is withdrawn.
- The required test is **withdrawn too.** It existed to preserve a re-admission path the owner has
  now retired. Asserting that inert code constructs would be maintaining a capability claim nobody
  holds — exactly the "dead weight that looks like a capability" the adversary seat warned about.
  `DirectBatchAdapter` is inert; leave it inert; a future Gemini effort starts from the ADRs and
  the client library, not from an unused adapter.

Deleting `gemini_batch.py` / `DirectBatchAdapter` outright is **optional cleanup**, not required by
this unit and not a blocker for any other. If deleted, note it in the W9 log so the removal is
discoverable; if kept, note *why* (inert, unguarded, unreferenced) so it is not mistaken for a
supported path.

### W1 — Validation spike (the fork instrument)
- **What**: One real post (multi-image, one workload) driven end-to-end against the real
  providers: drain enqueue → submit through the seam → verbatim bronze landing → conform (zero
  API calls on re-run) → one silver table → mart/view → drain run 2 suppression. Subset only;
  no full corpus. Capture the real nested envelope as a test fixture (mocks are flat today).
- **Files**: `src/datalake/defs/enrichment/landing.py`, `src/datalake/defs/enrichment/conform.py`, `tests/fixtures/real_envelope_gemini.json` (new), [`../analysis/spike-evidence.md`](../analysis/spike-evidence.md) (new, evidence for the fork decision), `tasks/plans/enrichment-v3-migration-master.md` (result note).
- **Deps**: W0 (spec exists to check against), **W-FREEZE** (the spike's completion guard reads
  `gold_analyses`, and measures against the frozen baseline — running it against a table that
  can still grow makes the observed behaviour ambiguous: a post suppressed because it is
  already-enriched is indistinguishable from one suppressed because a concurrent run just wrote
  it). **Blocks everything else** — its verdict selects the path.
- **Control**: C5 (walking skeleton / tracer bullet — the most damning miss in the audits).
- **Edge contract** (required by §6 — this unit IS the per-edge test the migration never had):
  - *reads from:* the real provider (qwen-batch service or Gemini) over the seam's HTTP
    contract; the drain's `enrichment_submitted` partitions; the real bronze root on disk.
  - *read by:* the fork decision (its verdict selects finish vs revert); every later unit relies
    on the envelope shape it captures as `tests/fixtures/real_envelope_gemini.json`.
  - *verifiable without a fake on either side?* **NO — deliberately.** That is the entire point
    of the unit: every other test in this repo mocks this boundary, and the flat-vs-nested
    envelope mismatch is the failure mode the migration is named for. The spike's value is
    precisely that it does not fake the boundary. State this as the unit's residual risk
    (a real call can fail for real reasons), not as a coverage claim.
- **Acceptance (round-trip)**: `landing.read_responses()` height ≥ 1 with the real captured
  envelope; **conform re-run over the same bronze row executes with a call counter reading
  `0`**. Build the counter by *wrapping* the adapter in a pass-through decorator that increments
  and delegates — do NOT `monkeypatch.setattr` the provider, which would both fake the very
  boundary under test and trip `hooks/pre/boundary-gate.ts` (§6). If the code shape forces a
  patch instead, the test file MUST carry `boundary-mock-ok: <reason>` and the unit must record
  the mock as a declared residual. Also: the silver row exists keyed `(post_id, platform)`; a
  second drain run does not re-enqueue the post. Evidence file records each observed
  materialization, including the real envelope's actual nesting.

### W2 — Branch stabilization: finish the queue-retirement slices
- **What**: The slices are COMMITTED (tree clean) — the "land or discard" half is done. But the
  unit is NOT one test, and closing it on the two named files repeats the scope error that
  produced the wrong currency note above. The measured baseline is **57 red** (26 failed +
  31 errors) across 16 files, and Pytest reports 26 as the count it would `--maxfail` on, which
  is why the number is easy to misread as smaller than it is. Red by file:
  `test_enrichment_exec.py` 11F, `test_harvest_sensor.py` 8E, `test_harvest_landing.py` 8E,
  `test_submit_job.py` 6E+1F, `test_media_upload_op.py` 5E, `test_batch_media_resilience.py` 4E,
  `test_migrate_creators_profiles.py` 3F, `test_batch_inline_media.py` 2F, `test_snapshot.py`
  (e2e) 2F, `test_full_pipeline.py` (e2e) 2F, `test_silver_observations.py` 1F,
  `test_ddl_builder.py` (operational) 1F, `test_gold_to_serving.py` (integration) 1F,
  `test_silver_to_gold.py` (integration) 1F, `test_operational.py` (e2e) 1F.
  The bulk are one failure mode — fixtures still building the retired queue tables — and are
  therefore a single mechanical fix applied across files, not 57 independent bugs. The
  `fail_item` harness bug (`TypeError: fail_item() got multiple values for argument 'item_id'`)
  is FIXED and committed (`2280e2c`). Note the contract those tests now assert: these primitives
  "have NO behaviour beyond the loud refusal" — the old behaviour tests (claim routing,
  attempts/backoff, failed_items counts) were replaced deliberately, so do not restore them.
  `_require_legacy_queue_tables` (`batch.py:51`) is a fail-loud precondition on transitional
  shims, NOT a zombie the retirer preserves; it is deleted only when W3 has replaced its last
  of five callers (submit, harvest, media_upload, analysis, registry).
- **Files**: the 16 red files above (fixtures that still build the retired queue tables),
  plus `src/datalake/defs/enrichment/batch.py`, `src/datalake/defs/instagram/assets.py`,
  `tests/operational/test_state_compatibility.py` (unblock only; rewrite is W7). Note
  `test_ddl_builder.py` and `test_state_compatibility.py` share a root cause with defect 9 (raw
  `CLASSIFICATION_DDL` literal bypassing `duckdb_ddl`), so that red clears with W6/W7 rather
  than here — do not force it green in W2 by weakening the assertion.
- **Deps**: none (parallel with W0). Blocks W3.
- **Control**: C4 (retirement ordering: producers retired before readers move — the inverse of
  expand-contract, which is exactly what the five-caller shim dependency exposes).
- **Premise corrected 2026-09-14: "the suite was green except one harness test" is FALSIFIED.**
  W2's acceptance was written on that premise, so it is unachievable as stated and would force
  double work. Every red file exercises the primitives W3 deletes (`batch.py` + its five
  callers) or the seam sites W5 rewires: measured by occurrence, `create_batch`,
  `claim_pending_items`, `set_gemini_batch_name`, `batch_items`, `batch_jobs`. Making those
  green inside W2 means re-implementing the queue W3 is removing — and it is the exact inverse
  of this unit's own rule ("never a green test resting on deletion").
- **Acceptance (round-trip), restated**: W2 exits when the suite's red is **partitioned and
  owned**, not when it is zero. Three parts:
  1. **DELETE (W2's own work)** — tests asserting retired behaviour, per this unit's note that
     the old behaviour tests "were replaced deliberately, so do not restore them".
     `test_enrichment_exec.py` is the measured case: its 11 red are the claim-routing
     (`test_claim_batch_mode_filter`, `test_claim_batch_interactive_skips_gemini_batch`),
     batch-mode-column (`test_create_batch_defaults_to_interactive`,
     `test_create_batch_with_gemini_batch_mode`,
     `test_migration_backfills_mode_on_legacy_rows`,
     `test_migration_adds_columns_to_preexisting_tables`), status-setter
     (`test_set_gemini_batch_name_and_status`,
     `test_set_name_extending_appends_submitted_statuses`) and whole-corpus-admission
     (`test_default_stays_label_gated`, `test_whole_corpus_includes_skip_posts`,
     `test_whole_corpus_excludes_current_prompt_gold`) cases. The SAME FILE's green tests
     (chunking, request building, token estimation, retrieve state machine) cover live
     behaviour and stay — delete by test, not by file. Each deletion is recorded with the
     retired behaviour it asserted, so the coverage is not silently lost.
  2. **W3/W5 ENTRY DEBT** — green-able only once the queue is truly gone and discovery is
     partition-based: `test_harvest_sensor.py` (8E), `test_harvest_landing.py` (8E),
     `test_submit_job.py` (6E+1F), `test_media_upload_op.py` (5E),
     `test_batch_media_resilience.py` (4E), `test_batch_inline_media.py` (2F),
     `test_silver_observations.py` (1F).
     `test_migrate_creators_profiles.py` — **FIXED 2026-09-14, not debt.** It was not fixture
     fallout: `scripts/migrate_creators_profiles.py:67` called
     `sqlite_ddl_for("batch_jobs", "batch_items", "media_metadata", "dead_letter")`, which
     raised KeyError on the three retired names (spec lookup). The script was not trying to
     resurrect them opportunistically — it was written before retirement and still listed them,
     so "fixing" the KeyError by re-adding specs would have resurrected the queue on live
     `ops.sqlite`. Dropped the three retired names, kept `media_metadata`. Two test assertions
     encoding the same retired expectation were corrected. 3 passed. NB the genuinely dangerous
     script is `migrate_enrichment_queue.py` (raw `CREATE TABLE IF NOT EXISTS`), recorded in the
     master plan's consumer register — it cannot fail loudly, it just recreates the tables.
  3. **W6/W7 ENTRY DEBT** — green-able only once silver/gold exist: `test_snapshot.py` (2F),
     `test_full_pipeline.py` (2F), `test_gold_to_serving.py` (1F), `test_silver_to_gold.py`
     (1F), `test_operational.py` (1F), `test_ddl_builder.py` (1F — defect 9, the raw
     `CLASSIFICATION_DDL` literal bypassing `duckdb_ddl`).
  Then demonstrate the queue path: with `batch_jobs` present, the legacy read path executes;
  with it absent, the *replacement* discovery path (W3) serves submit — never a green test
  resting on deletion.
- **W2 exit state:** the suite is NOT expected green at W2 exit. It is expected to have a
  documented partition (deleted / W3-W5 debt / W6-W7 debt) with counts, so W3 and W5 open
  against a known debt rather than rediscovering it. Re-running the two named files and
  declaring victory is the failure this note exists to prevent.

### W3 — Reconnect the halves: drain→submit handoff + retire the queue read-path
- **What**: `submit.py` discovers work from the Dagster instance's `enrichment_submitted`
  partitions (what the drain materializes at `assets.py:1289`), not from `batch_jobs`
  (`submit.py:77`). Delete `_require_legacy_queue_tables`. Multi-chunk handle round-trip test
  fixing the `"|"` vs `","` encoding divergence (submit.py:170 vs adapters.py:280) — one
  serialization, one test with a multi-chunk case.
- **Files**: `src/datalake/defs/enrichment/submit.py`, `src/datalake/defs/enrichment/batch.py` (delete), `src/datalake/defs/enrichment/partitions.py`, `src/datalake/defs/enrichment/adapters.py`, `tests/unit/enrichment/test_submit_discovery.py` (new).
- **Deps**: W1 verdict = finish (a); W2 (tree stable). Blocks W4, W5.
- **Control**: C2 (runtime producer/consumer pairing unowned) + C3 (handle encoding).
- **Acceptance (round-trip)**: on one shared instance: drain run materializes N partitions →
  submit discovers exactly N work items and submits them → the accounting identity
  `done + failed + in_flight + backlog == candidates` holds over the *real corpus*, not a
  constructed example. A test joins the two real halves and asserts a **nonempty handoff**.

### W4 — `enrichment_harvested` producer + retry driver
- **What**: Implement the driver specified in W0: an asset/job that materializes
  `enrichment_harvested` partitions when harvest reaches a terminal state (the missing writer
  that makes `in_flight = submitted ∖ harvested` shrink). Implement the retry driver: failed
  submits/terminal failures mint round-N partition keys per W0's shape; remove the round-0
  hardcode (`DRAIN_ATTEMPT_ROUND = 0`).
- **Files**: `src/datalake/defs/enrichment/partitions.py`, `src/datalake/defs/instagram/assets.py`, `src/datalake/defs/enrichment/harvest.py`, `tests/unit/enrichment/test_partitions_retry.py` (new).
- **Deps**: W3 (drain→submit exists to observe), W0 (spec).
- **Control**: C1 + C2 (mechanism with no actor; state transition with no named event).
- **Acceptance (round-trip)**: a real cycle: submit → harvest-terminal → `enrichment_harvested`
  partition observed materialized → drain run 2 re-enqueue eligible for that post while
  in-flight posts stay suppressed (the existing `test_two_consecutive_runs_enqueue_no_post_twice`
  rewritten so completion genuinely releases work — it must FAIL if the writer is removed).
  Retry: a forced terminal failure produces a round-1 key, distinct from round-0, and the guard
  admits it without double-submitting round 0.

### W5 — Seam call-site migration + one transport per service  *(shrunk by W-FREEZE)*
- **What**: Measured against the working tree, the "12 bypasses" are **9 Gemini and 2 qwen**.
  W-FREEZE retires Gemini *usage*, so the nine Gemini sites are **deleted with their entry
  points, not migrated** (§ W-FREEZE, Consequence 1): `submit.py:152`;
  `harvest.py:311,316,319,334`; `harvest.py:446,454,459,462`. What remains to *migrate* is the
  live qwen path: `facets_batch.py:318,322` routed through the seam, plus fixing the
  `max_tokens` interface defect by adding `JobSpec` to the seam's submit verb (the documented
  bypass reason), and consolidating `qwen_client`'s direct HTTP path so `ServiceBackedAdapter`
  is the only qwen transport with one terminal predicate.
- **Note the ordering interaction**: W-FREEZE deletes the Gemini entry points; W5 deletes what
  remains of their call sites. Doing W5 first would mean migrating nine sites that W-FREEZE then
  deletes — the double work this unit exists to avoid. **W-FREEZE precedes W5.**
- **Files**: `src/datalake/defs/enrichment/submit.py`, `src/datalake/defs/enrichment/harvest.py`, `src/datalake/defs/enrichment/facets_batch.py`, `src/datalake/defs/enrichment/seam.py`, `src/datalake/defs/enrichment/qwen_client.py` (delete/retire).
- **Deps**: W-FREEZE (**hard predecessor** — it deletes the 9 Gemini sites this unit would
  otherwise migrate; running W5 first is the double work the freeze exists to prevent), W3
  (submit already rewired; avoids conflicting edits to `submit.py`). Blocks W1-grade
  real runs of the converged path; parallel with W4 only if W4's `harvest.py` edits are
  coordinated (both own `harvest.py` — see §5).
- **Control**: C2 + C3 (composition root never wired; two clients, divergent predicates;
  per-call parameter inexpressible).
- **Acceptance (round-trip)**: CI grep hook: provider names (`gemini_batch\.|qwen_client\.`)
  appear only inside adapter modules — zero matches elsewhere in `src/`; AND a seam-adoption
  test runs the real production entry point and records `run_lifecycle`/adapter calls on the
  stack trace. Existence of the registry test is explicitly not acceptance.

### W6 — Materialize the twelve objects (bronze→silver→marts) with the backfill
- **What**: Execute, for the first time, against `data/state.duckdb` and the real bronze root:
  the Phase-5 classification migration from `gold_analyses.result_json` (idempotent by natural
  key — `INSERT OR REPLACE` on `(post_id, platform)`; the backfill currently has **no**
  idempotency/reconciliation instrument — add one), the conform registration (give `conform`
  its production caller as a Dagster asset), and the four gold marts as views. Fix
  `gold_content_shape_performance` grain to include `platform` (and post-level signal) before
  materializing.
- **Files**: `scripts/migrate_classification_to_silver.py`, `src/datalake/defs/enrichment/conform.py`, `src/datalake/defs/instagram/assets.py` (conform caller + remove cross-domain DDL at :1170), `src/datalake/defs/serving/assets.py`, `tests/operational/test_backfill_idempotency.py` (new).
- **Deps**: W1 (real envelope shapes proven — conform has never met real data), W4 (terminal
  states exist to drive conform), W5 (all flows on the seam). Blocks W7.
- **Control**: C5 (zero real-run gate; twelve code-only objects; unproven replay guarantee).
- **Acceptance (round-trip)**: every `gold_analyses` row accounted for —
  `count(silver_content_classification) + count(silver_enrichment_quarantine)
   == count(gold_analyses)` **measured in the same run**, each
  migrated row carrying provenance (`provider, model, prompt_hash, schema_version, run_id`);
  re-running the backfill changes 0 rows (idempotency demonstrated by double-run + diff);
  delete a conformed row and re-conform from bronze with the SDK counter at **0** (replay,
  never re-bill — the keystone claim, now demonstrated on a real row).
  **Amended by ADR-0014 (W0, 2026-09-14):** the 8 `model IS NULL` legacy rows are
  NOT a re-hash/quarantine fork — verified live, all 8 carry `prompt_hash 24c8e291`
  and valid classification JSON, so quarantining would misrepresent valid data and
  re-hashing `prompt_identity_v1` is void (the model is unknowable by construction).
  The disposition is **migrate with sentinel provenance**: `model='legacy-unknown'`,
  `provider='gemini'`, `prompt_hash` unchanged, and the migration asserts
  `count(model IS NULL rows dispositioned) == 8`, failing loudly if a ninth appears
  between audit and migration. The reconciliation identity becomes genuinely
  checkable: `count(silver_content_classification where model='legacy-unknown') == 8`
  AND `count(gold_analyses where model IS NULL) == 0` after migration, asserted in
  `tests/operational/test_backfill_idempotency.py`.

### W7 — Serving rebind + schema-catalog/target reconciliation
- **What**: Rebind the 22 transitive views from `gold_analyses` to silver/marts (the silver-bound
  definitions already exist in `serving/assets.py` but have never executed). Reconcile the
  catalog to the TARGET world: `DUCKDB_TABLES` in `schemas.py` must expect
  `silver_content_classification` + the marts and stop listing `gold_analyses` as authoritative.
  Rewrite `test_state_compatibility.py` to assert view *definitions* against a baseline snapshot
  (not SELECT-ability) and to fail on the old world. **Additive-first**: new objects created and
  parity-verified before any view is rebound.
- **Files**: `src/datalake/defs/serving/assets.py`, `src/datalake/defs/common/schemas.py`, `tests/operational/test_state_compatibility.py`, `tests/operational/test_view_definition_baseline.py` (new), `dashboard/server.py` (read-only check only).
- **Deps**: W6 (silver populated — a rebind over empty silver serves nothing).
- **Control**: C4 (expand-contract: readers move before stores retire) + C5 (the vacuous gate).
- **Acceptance (round-trip)**: a parity job diffs the old and new worlds side-by-side for the
  migration window: for a sample of posts, `v_post_detail`-from-silver returns the same
  `result_json`-derived values as from `gold_analyses` (column-level equality on a real sample);
  view-definition snapshots of all 22 views captured BEFORE rebind, asserted unchanged-after
  except for their source table. Only after parity passes are the views rebound.

### W8 — Quarantine consumer + DQ/freshness gates (the last DQ layer)
- **What**: Give `silver_enrichment_quarantine` its named consumer: a triage view + a **blocking**
  Dagster asset check on quarantine growth and on `landed(bronze) ∖ conformed(silver)` (the
  anti-join ADR-0012 mandates). Repair the vacuous checks (asset checks that pass on unread
  parquet; `classify_error` defaulting unknown exceptions to TERMINAL; unbounded
  warn-and-continue poll loops). Declare freshness/volume expectations per asset.
- **Files**: `src/datalake/defs/enrichment/conform.py` (reason codes unchanged), `src/datalake/defs/enrichment/checks.py` (new), `src/datalake/defs/enrichment/harvest.py` (bound the poll loop), `src/datalake/defs/enrichment/classification.py` (`classify_error`), `tests/unit/enrichment/test_checks_fire.py` (new).
- **Deps**: W4 (terminal states), W6 (quarantine table real). Independent of W7.
- **Control**: C1 + C5 (control surface with no reader; gates that cannot fail).
- **Acceptance (round-trip)**: inject a deliberately malformed real-shaped response → the check
  fires (observed failure, not a log line); restore → check green. The anti-join check
  demonstrably fires when a bronze row has no conformed counterpart. Each check has a stated
  "what would make this fail" — the vacuity test of the audits.

### W9 — Retirement (Phase 7, last)
- **What**: Only after W3/W4/W7 prove the new world: drop `batch_jobs`, `batch_items`,
  `dead_letter`, `gold_growth_facets` from `ops.sqlite`/`state.duckdb`, **and `gold_analyses`
  from `state.duckdb`** (decided retired 2026-09-14 — superseded by
  `silver_content_classification`); reconcile `facets_batch_jobs`' 4 live ledger rows
  (1 JOB_FAILED, 2 RETRIEVABLE…) into the service's job store BEFORE the drop (audit P1-5a:
  currently nothing accounts for them); update docs.
- **Archive is a precondition of the drop, and it is verified, not asserted.** For every table
  dropped — including the six-figure `gold_analyses` — export the full table to Parquet under
  `data/lake/archive/<table>/` (`COPY <table> TO '…' (FORMAT PARQUET)`), then compare the
  export's row count against the live count **in the same run** and fail the retirement if they
  differ. The owner's instruction was "take a copy and archive it so it's safe" — an unchecked
  export is not a copy, and a copy that was never counted is not safe. Record each export path
  + measured count in the W9 log.
- **The gate is SELF-REFERENTIAL: export count == live count at archive time.** It is
  deliberately NOT a comparison against the numbers below. **W-FREEZE removes the growth source**
  (`analysis.py:179` does `INSERT INTO gold_analyses` — that path is retired by W-FREEZE), so
  the count should be static from the freeze to W9. The self-referential form is kept anyway as
  defence in depth: the freeze is a convention enforced by removing entry points, and a gate that
  depends on a convention holding for weeks is a gate with a hidden assumption. A frozen
  `== 9,576` would fail on any drift — a manual trigger, a restored job, a backfilled row — and
  report it as an archive error rather than as what it is. Use these only as a **drift signal** —
  a dated baseline snapshot:
  `gold_analyses` 9,576 · `dead_letter` 776 · `gold_growth_facets` 205 · `facets_batch_jobs` 4
  (read from the live DBs 2026-09-14). If the live count differs at W9, it means something wrote
  the table after W-FREEZE, which contradicts that unit's acceptance. Treat it as a W-FREEZE regression
  first and a reconciliation problem second: re-check W6 before archiving.
  Note the ordering that makes this safe: `gold_analyses` must NOT be archived-and-dropped
  before W6 migrates its rows and W7 proves parity against it — W7's acceptance is a side-by-side
  diff with the live table. Sequence is W6 migrate → W7 parity → archive → approval → drop.
- **Files**: `scripts/retire_queue_tables.py` (new), `data/ops.sqlite` (via script only), `src/datalake/defs/enrichment/facets.py` (docstring fix, P1-6), `docs/architecture/pipelines/enrichment.md`.
- **Deps**: W7 (readers moved — expand-contract contract satisfied), W3/W4 (new state sources proven).
- **Control**: C4 (retire only after readers move; starve, don't drop).
- **Acceptance (round-trip)**: four steps, each observable. (1) **Archive verified** — every
  dropped table exported to Parquet and its export row count measured equal to its live count
  **in the same run** (self-referential, per above — never against a frozen constant);
  a mismatch fails the retirement. A live count that differs from the 2026-09-14 baseline is a
  drift signal to investigate, not automatically a failure. (2) **Reconciliation ledger** for the 4 `facets_batch_jobs`
  rows (each mapped to a service job-store state or explicitly dispositioned). (3) **The drop
  script runs.** (4) A full drain→submit→harvest cycle succeeds **without** any legacy table —
  demonstrating the retirement, not asserting it.
  **[HUMAN APPROVAL required: any DROP against live data.]**
  Note the archive is not "the drop was fine" — it is the only thing standing between
  `gold_analyses`' rows and permanent loss, so it is gated on a measured count match
  rather than on the script having run.

**MECE coverage check** against [`../analysis/learnings-mece.md`](../analysis/learnings-mece.md) §2: rows map to W0 (C1 rows: retry driver,
quarantine consumer, harvested producer — no duplication: spec in W0, implementation in W4/W8),
W2+W3 (drain/submit pairing, zombie queue), W5 (**2 surviving qwen bypass sites** after W-FREEZE
deletes the 9 Gemini ones, dual clients, handle encoding), W-FREEZE (Gemini usage retired, old
write path frozen — the unit that makes the 9 deletions deletions rather than migrations),
W4 (partition-key shape, retry), W6 (mart grain, materialization, replay proof), W7 (catalog
reconciliation, vacuous gate, expand-contract), W8 (quarantine consumer, silent failures,
vacuous checks), W9 (sequencing/retirement). Every defect row covered at least once; no row
implemented twice (spec-once, build-once, verify-once per unit).

---

## 3. Blast radius & invalidation plan

### 3.0 THE BACKUP GATE — must pass before ANY destructive step

Owner instruction 2026-09-14: *"i definitely dont want to lose apify scraped data."* The
enrichment layer is re-derivable (re-run qwen; if the whole corpus needs re-enriching, so be it —
that is the accepted cost of finishing swiftly). The **scraped** layer is not. This gate protects
the difference.

**Audit result (measured 2026-09-14 — the gap is real):**

| Asset | Size | In git? | Backed up? |
|---|---|---|---|
| `data/lake/bronze/*.parquet` — Apify scraped raw | 42 files, 69 MB | **No** (gitignored) | **NO** |
| `data/media/posts/` — scraped media BYTES | 27,806 files, **55.4 GB** | **No** (gitignored) | **NO** |
| `ops.sqlite` `media_cache` — URL→file mapping for the above | 27,748 rows | No | only via a 09-10 snapshot |
| `ops.sqlite` `creators`/`profiles`/`creator_merges` — human curation | 675/675/2 | No | only via a 09-10 snapshot |
| `ops.sqlite` `prompt_registry` | 1 | No | only via a 09-10 snapshot |
| `ops.sqlite` / `state.duckdb` | 42.7 / 80.5 MB | No | `~/backups/datalake/2026-09-10-spike-baseline` (4 days stale) |

`~/backups/datalake/` contains **two files** — `ops.sqlite` and `state.duckdb`, dated 2026-09-10.
It contains **zero parquet files and zero media files**. The scraped layer exists **only on this
laptop**.

**Why the media bytes are the sharpest edge:** Instagram CDN URLs die in ~4-5 days (WATCHDOG).
The byte cache exists *precisely* because re-fetching is impossible — so `data/media/` is not a
cache in the disposable sense, it is the **only copy** of the scraped media. Losing it means
losing the media permanently, and no amount of re-enrichment recovers it. This is the one asset
in the repo that no downstream action can regenerate.

**Gate (all three required, before the first destructive step):**
1. **Bronze + media bytes copied off this machine.** Destination: R2 (profiles `r2-handoff` /
   `r2-sessions` already configured; endpoint in `~/.aws/config`). 55.4 GB — a long upload, so it
   is a background process (`rule://offload-long-runs`), not a subagent unit.
2. **Identity tables exported** (`media_cache`, `creators`, `profiles`, `creator_merges`,
   `prompt_registry`) — small, and they are the mapping that makes the media bytes *usable*. A
   byte cache with no URL→file index is 55 GB of unlabelled files.
3. **Restore verified, not asserted** — spot-check N files read back from R2, byte-compare
   against local, and record the counts. An upload nobody read back is a hope, not a backup.

**Integrity verified 2026-09-14 — every indexed byte is present.** The apparent mismatch between
`media_cache` (27,748 rows) and `data/media/posts/` (26,657 files) was checked rather than
assumed, because a row pointing at missing bytes would be unrecoverable data loss on the exact
asset this gate protects. Result — the gap is **benign and arithmetically exact**:

- `media_cache`: 27,748 rows, 27,748 distinct `cache_key`, 27,748 distinct `local_path`, **0 NULL paths**
- Rows whose file **exists: 27,748**. Rows whose file is **missing: 0**. Zero-byte files: **0**
- The 1,091-row difference is `data/media/thumbnails/` (1,099 files), not posts: posts 26,657 +
  thumbnails 1,091 = **27,748**, matching the cache exactly
- Total bytes across all indexed files: **55.36 GB**

So `media_cache` is a **complete and accurate index** of the scraped media bytes — no orphans, no
dangling pointers. This is what makes the gate's step (2) meaningful: restoring the bytes *and*
the index restores a usable asset; restoring either alone does not.

**Not claimed by this gate:** that R2 is durable, versioned, or lifecycle-managed. Those are
decisions for whoever owns the bucket, not for this plan. The gate asserts only that a second
copy exists and was read back.

**Note on `media_metadata` (5,613 rows):** it is **not** in the preservation set — it caches
Gemini File API URIs, 5,419 of which have expired (max `expires_at` 2026-09-11). It is dead
weight, safe to drop.

| Change | Invalidates | Consumers to signal | Refresh class |
|---|---|---|---|
| W3/W4 discovery + in-flight switch (`batch_jobs` → Dagster partitions) | `ops.sqlite` `batch_jobs`/`batch_items` become read-dead; in-flight derivation changes | Drain, submit, harvest, accounting identity, any operator tooling reading the queue | Self-correcting for serving; **full** for pipeline state — first run under new discovery must be on a subset; 6 live `batch_jobs` rows reconciled before the read-path dies (W2/W9) |
| W4 retry/round keys | Partition keys minted before this change are round-0-shaped and unrecoverable by the new driver | Drain guard, accounting identity, in-flight dashboards | **Versioned backfill**: partition keys self-version by round suffix; legacy round-0 rows grandfathered and marked; identity re-checked on real corpus |
| W6 classification migration (9,576 rows) | Creates `silver_content_classification`; `gold_analyses` stays intact as read-only history — **9,576 rows must not be lost; nothing is dropped in this unit** | Serving views (via W7), dashboard, drain completion guard (now reads silver) | **Versioned backfill** — idempotent by natural key, proven by double-run; `gold_analyses` retained until W9 decision |
| W6 conform caller + marts | Creates 5 silver tables + quarantine + 4 marts (views); bronze becomes a live layer | Marts' consumers, asset checks | **Full** first materialization (nothing exists to refresh); thereafter self-correcting views |
| W7 serving rebind (22 transitive views) | Every view's source changes: `v_post_detail`, `v_overview`, and the 20 transitive views | **Dashboard (`dashboard/server.py`)** — reads views only, but the KPI values underneath change source; re-verify rendered KPI numbers post-rebind; also any notebook/ad-hoc consumer of `gold_analyses` by name | **Parity-gated cutover**: old and new coexist during migration window; views rebound atomically after sample parity passes; `gold_analyses` retained (never dropped without human approval) |
| W7 catalog reconciliation | `DUCKDB_TABLES`/`expected_schema.py` change meaning | `test_state_compatibility.py`, schema docs | Self-correcting (test suite re-reads catalog) |
| W8 checks/freshness | Adds blocking checks — new failure surface | On-call/operator workflows | Additive only |
| **W9 drops — DROP list** (`batch_jobs` 6, `batch_items` 10,285, `dead_letter` 776, `facets_batch_jobs` 4, `media_metadata` 5,613, `gold_growth_facets` 205, `gold_analyses` 9,576) | Irreversible removal of queue history + the enrichment layer | Serving (must already be off gold by W7), dashboard | **Destructive — §3.0 backup gate MUST pass first, then human approval, then archive** (export every dropped table to Parquet under `data/lake/archive/`, verify export count == live count in the same run). Owner 2026-09-14: *"i dont care about the queues and batches in ops.sqlite we can confidently drop them"* — the queue drops are AUTHORIZED; sequencing is the only question. `gold_analyses` is decided retired but drops LAST, only after W6 migrates its rows and W7 proves parity |
| **W9 — KEEP list (irreplaceable or curated)** `media_cache` 27,748 · `creators` 675 · `profiles` 675 · `creator_merges` 2 · `prompt_registry` 1 | — | None — these are the mapping that makes the 55.4 GB of scraped media bytes *usable*, plus human curation that cannot be regenerated | **NEVER DROPPED.** `media_cache` is the URL→file index for the scraped bytes (verified complete: all 27,748 rows resolve to existing files, 0 missing — §3.0); without it the cached bytes are 55 GB of unlabelled files. `creators`/`profiles` encode human identity decisions (WATCHDOG: "Creator identity is a human decision"). Dropping any of these is data loss even though the queue drops are authorized |

**⚠ THE DROP MUST BE PER-TABLE — never a database-level operation.** The KEEP and DROP sets live in
the **same file**, `ops.sqlite`. `media_cache` (27,748 rows, the index to 55.36 GB of unrecoverable
scraped media), `creators`, `profiles`, `creator_merges` and `prompt_registry` share that file with
the queue tables that are cleared to drop. Therefore:
- `scripts/retire_queue_tables.py` issues **per-table `DROP TABLE` statements**, one named table at a
  time, from an explicit allow-list. No `DROP DATABASE`, no file deletion, no "recreate ops.sqlite
  clean", no `VACUUM INTO`-and-swap, no temp-file rename, no wholesale rewrite.
- The script **asserts the KEEP list is still present and non-empty after the drops** — if
  `media_cache` or `creators` is missing when the script finishes, it failed loudly and the
  promotion does not proceed.
- The KEEP set is **backed up independently** (§3.0 step 2) *before* the script runs, so a botched
  per-table drop is recoverable and not a best-effort.
- Deleting `ops.sqlite` to "start clean" would destroy the media index and the human curation while
  leaving the 55 GB of bytes intact and unlabelled — the worst possible outcome, since it looks like
  a successful cleanup. Say so in the script's docstring.

Nothing in this plan writes to `gold_analyses` or deletes any row before W9, and W9's drops are
gated, archived, and human-approved. All new DDL is additive (`CREATE OR REPLACE` for views,
new tables only).

---

## 4. Whole-remediation acceptance gate

**One end-to-end round-trip criterion:**

> One real, already-enriched post and one real never-enriched post, driven through the live
> pipeline: drain run 1 enqueues only the never-enriched post (completion guard reads silver,
> respects the legacy rows present at run time); submit goes through the seam; the response lands verbatim in
> bronze (`ok` populated); conform materializes the silver row with zero additional API calls on
> re-run; the marts and serving views expose it; harvest reaches terminal state and the
> `enrichment_harvested` producer materializes; drain run 2 re-enqueues nothing in-flight and
> nothing already-conformed. Meanwhile `gold_analyses` is still intact — it holds every row it
> held when the run started, and is never smaller (measure before and after; assert
> non-shrinkage, never equality — after W-FREEZE it should in fact be *unchanged*, and a change
> is a W-FREEZE regression rather than expected corpus growth) — and the dashboard serves
> coherently throughout.

**Mechanical non-vacuity checks** (each can fail, each targets a specific vacuity found in the
audits):
1. **Reconciliation identity**: `count(silver_content_classification) + count(silver_enrichment_quarantine) == count(gold_analyses)` at migration time, every legacy row accounted (C5 — replaces the unmet C5.4). **Self-referential, not `== 9,576`**: W-FREEZE makes the right-hand side static, but a frozen constant still asserts a convention rather than a measurement — and it would report a W-FREEZE regression as a reconciliation error.
2. **Provider-name grep in CI**: `gemini_batch\.|qwen_client\.` matches only adapter modules (C2/C3). After W-FREEZE this checks a smaller surface than it was written for — 9 of the original bypass sites no longer exist — but it still guards the 2 surviving qwen sites and any future re-introduction, which is the point. (It was previously PAIRED with a `build_adapter("gemini")` constructibility check; that pairing is **withdrawn** — the key was wrong (`direct_batch` is real) and the owner's retirement decision removed the capability it was preserving. See W-FREEZE Consequence 3.)
3. **Zero-row gate**: any new object referenced by a consumer with zero rows fails the merge (C5 — "every new object has rows").
4. **Catalog-vs-target reconciliation**: `DUCKDB_TABLES` names the TARGET world and the live DB matches — not the status quo (C5 — the vacuous-gate fix; reconciliation is against the *target* schema, per postmortem §10.2's correction).
5. **View-definition baseline**: all 22 transitive views' SQL snapshotted; rebind PRs must diff against it (C5 — replaces the never-written "asserted, not assumed").
6. **Zero-API-call replay counter**: a monkeypatched SDK-call counter reads 0 on a conform re-run over real bronze (C5 — replaces the AST scan).
7. **Falsifiability review**: every acceptance test in W2–W9 answers "what plausible bug makes it fail?" — a test that cannot fail is rejected at review (the audits' vacuity standard).

---

## 5. Sequencing & risk

### Declared DAG

```
[§3.0 BACKUP GATE] ──► W0 ──► W-FREEZE ──► W1 ──► [FORK DECISION — human] ──► W2 ──► W3 ──► W5 ──► W6 ──► W7 ──► W9
                                                                                     │        ▲       ▲
                                                                                     └──► W4 ─┘       │
                                                                                          W8 ────────┘ (independent of W7)
```

- **§3.0 BACKUP GATE runs FIRST — before W0, before anything.** It is not a unit and has no
  acceptance document; it is a precondition with one observable: the scraped layer
  (`data/lake/bronze` 69 MB + `data/media` 55.4 GB) exists in a second location, read back and
  byte-verified. It gates only the DESTRUCTIVE steps in principle, but it runs first because it
  is currently **unmet** (see §3.0: zero parquet, zero media in `~/backups/datalake/`) and a
  backup is precisely the kind of task that gets deferred to just-before-the-drop and then
  shipped without being read back.

- **W-FREEZE precedes W1**, and precedes W5 specifically. It freezes `gold_analyses` so the
  baseline W1 measures and W6 reconciles against cannot move mid-migration, and it deletes the
  Gemini entry points so W5 deletes their call sites rather than migrating nine of them into
  code that is about to disappear. Ordered wrong, both units do double work.

- **W0, W2** start immediately and in parallel (no deps). W1 after W0.
- **W3 → W5 serial**: both edit `submit.py`; shared-file edits are one ownership boundary.
- **W3 → W4 serial**: the harvested producer and retry driver need the handoff to exist.
- **W4 and W5 both touch `harvest.py`** (W4 adds terminal-state materialization; W5 migrates 8
  call sites) — they MUST be either serialized or coordinated via `hub` with one owner named
  for `harvest.py`. Recommended: W4 first (dynamics before rewiring), then W5.
- **W6 → W7 serial**: rebind over empty silver serves nothing.
- **W8** parallelizable with W7 (different files; both depend on W6/W4 outputs).
- **W9** strictly last (expand-contract: nothing retires until its readers moved).

### Independent / parallelizable
W0 ∥ W2; W8 ∥ W7; W5 ∥ W8 if `harvest.py` ownership is settled first.

### Risky / destructive / human-gated
1. **Fork decision (after W1)** — human.
2. **W6 backfill** — first writes to `state.duckdb` under new logic; idempotency must be
   demonstrated (double-run) before it is trusted; mitigate by running on a copy of
   `state.duckdb` first (single-writer discipline).
3. **W7 view rebind** — the only step that can break the serving surface; parity-gated,
   snapshot-before, and the dashboard's rendered KPIs re-verified visually afterward.
4. **W9 drops** — irreversible; archive-first; explicit human approval per table; `gold_analyses`
   retention decided separately (recommend: keep read-only ≥ 1 full cycle).
5. **W1 spike spend** — real paid API calls; budget approved by human.
6. **The W2 red set** — the fail_item harness bug is FIXED (`2280e2c`) and was never a product
   defect, but it was 1 red of 57. The real W2 risk is the size of the remainder: 56 red across
   15 further files, mostly one mechanical failure mode (fixtures still building the retired
   queue tables) but including red that cannot clear until W3/W6/W7 and must be assigned rather
   than forced green. The queue-retirement slices are already committed, so no commit-or-discard
   decision remains, and no history rewrite. **Do not re-close W2 on a named-file subset** —
   that is exactly the error the corrected currency note records.

### Process controls binding every unit (from §10 of the postmortem — promotion, not prose)
- Each dispatch names the **owner** of the contract and the **production consumer** of
  everything it writes / producer of everything it reads (C2).
- Acceptance is a demonstrated round-trip with evidence of materialization, never an existence
  claim; the reviewer gate is run by a fresh-context `dlc-reviewer` per unit, verifying data
  coherence (rows exist? derived data stale? consumer contract changed?) not just code (C5).
- Mechanically checkable items ship as hooks/tests, not prose: the CI grep (W5), the
  reconciliation and definition-baseline tests (W6/W7), the replay counter (W6/W8).

---

## 6. Enforcement plane (agent config, added 2026-09-14)

The plan was written before the harness gained a rules + hooks enforcement plane. Three of
those rules and both hooks bear directly on this migration, and the plan referenced none of
them. Binding them here so a unit does not have to rediscover them at implementation time.

### The edge contract is now a required per-unit artifact

`rule://edge-contract` was written *from this migration* — it quotes "the 36 exit criteria in
the enrichment-v3 migration" as its worked example, and names the exact failure: **every
criterion was checkable without any other component existing.** Per-node completeness was easy
and per-edge completeness was invisible. That is why the migration landed complete and
non-functional with a green suite.

Every unit W0–W9 must therefore carry three lines on top of its existing What/Files/Deps:

```
- reads from:      <the counterparties whose real output this consumes>
- read by:         <the counterparties that consume what this produces>
- verifiable without a fake on either side?  <yes | no — and if no, say so plainly>
```

A `no` is not a failure. It is a **module, not a component**, and must be reported as such and
recorded as a declared residual — never presented as coverage. A mock at the boundary encodes
the author's belief about the other side's shape, and *the belief* is what gets tested; when
the real envelope differs (nested not flat, field renamed, key under a different name) the
suite stays green and integration fails on first contact.

### `boundary-gate` hook — will block this plan's own acceptance tests

`~/.omp/agent/hooks/pre/boundary-gate.ts` blocks a write to a **test file** whose text contains
**both** a mocking construct (`monkeypatch.setattr`, `MagicMock`, `mock.patch`, `AsyncMock`, …)
**and** a boundary word (`client|adapter|api|endpoint|provider|db|sqlite|duckdb|session|queue|…`),
case-insensitive. Escape: a file carrying `boundary-mock-ok: <reason>` is not blocked, and the
suppression is itself recorded so the exemption is auditable.

This collides with three units as currently written:

| Unit | The acceptance line | Why it trips |
|---|---|---|
| W1 | "a monkeypatched provider-SDK call counter reading **0**" | `monkeypatch.setattr` + `provider` |
| W6 | "re-conform from bronze with the SDK counter at **0**" | same |
| W8 | "inject a deliberately malformed real-shaped response" | mock at the adapter/endpoint boundary |

**Required of each:** carry `boundary-mock-ok: <reason>` naming the boundary and why it cannot
be exercised for real here, AND record in that unit's acceptance that the mock is a declared
residual rather than coverage. Where the counter can be built by *wrapping* the adapter instead
of *patching* it — a pass-through decorator that increments and delegates — prefer that: it
counts the real call path and needs no suppression at all. W1 and W6 should be written the
wrapping way if the code shape allows; only fall back to a declared suppression if it does not.

Measured blast radius on this plan's files: of the plan-touched test files, only
`tests/unit/enrichment/test_media_cache.py` currently contains blocked lines (3) and **zero**
files carry the marker — so WATCHDOG's "keep the File API upload path exercised there" cannot
be honoured by an edit until that file declares its suppression. Repo-wide the pattern occurs
on 31 lines across the suite; the gate will surface each when first touched, which is the
intended behaviour, not a bug.

### `migration-guard` hook + `migration-ships-with-code` — constrains W9's mechanism

`hooks/pre/migration-guard.ts` blocks destructive DDL (`DROP TABLE|COLUMN|SCHEMA|DATABASE|INDEX`,
`TRUNCATE TABLE`) written to a path matching `migrations/`, `alembic/`, `versions/`, or any
`.sql` file; the attempt is recorded as a guard-block entry. Additive DDL passes untouched.

**Two mechanisms, two different behaviours — do not conflate them.** The hook and the rule are
scoped differently, and an implementer who expects only one will misread the other:

- **The hook (`migration-guard.ts`) is PATH-scoped.** It matches only `migrations/`, `alembic/`,
  `versions/`, or `*.sql`. `scripts/retire_queue_tables.py` matches none of those, so **the hook
  will not block W9's script.** It blocks, records, and stops; additive DDL passes untouched.
- **The rule (`migration-ships-with-code`) is PATH-AGNOSTIC.** Its condition is a bare
  `DROP TABLE|COLUMN|SCHEMA|INDEX` / `TRUNCATE TABLE` across `tool:edit(*)` and `tool:write(*)`
  — any file, any extension, including this plan document. It was demonstrated during this
  review: editing this very section fired it, because the prose contains the phrase. It is
  configured **non-interrupting**, so it advises rather than blocks.

Consequence for W9, stated so nobody weakens the script to silence a signal:

- The DROP statements belong **in the script**, never in a `.sql` file or under a `migrations/`
  path — that is what keeps them past the blocking hook.
- W9 will nonetheless **see `migration-ships-with-code` fire when the script is written.** That
  is the expected signal, not a defect. The correct response is to confirm the ceremony is
  present — archive-first export to `data/lake/archive/`, explicit human approval, non-numbered
  standalone script — and proceed. The WRONG response is to silence it by removing the DROP,
  splitting it across statements to dodge the pattern, or renaming the tables out of the
  statement. A suppressed guard on the one operation that can destroy live data is worse than
  the guard firing.
- W9 remains an archived, human-gated, standalone script — explicitly **not** a numbered
  migration, which is what the rule forbids ("never DROP against real data in a numbered
  migration"). The archive-first + explicit-approval ceremony W9 already specifies IS the
  compliant form; this note only says why, and what the expected signal looks like.

### `dormant-vs-broken` — W8 must declare each source's intended state, and one is unresolved

`rule://dormant-vs-broken`: "Before building a presence/freshness check, declare each source's
intended state: **live** (must stay current) or **retired/dormant** (healthy as-is) … If a
source's intended state is unknown, **ask** — never guess." W8 currently says only "declare
freshness/volume expectations per asset", which is about *thresholds*, not about *intended
state* — and a threshold on a deliberately-retired source is a false andon.

States that must be declared explicitly in W8:

| Source | Intended state after this migration | Note |
|---|---|---|
| `batch_jobs`, `batch_items`, `dead_letter` | **retired/dormant** | W9 drops them; a presence check must not fire during the W3–W9 window |
| `facets_batch_jobs` | **retired/dormant** | 4 live rows reconciled at W9 before the drop |
| `gold_analyses` | **retired/dormant — DECIDED 2026-09-14** | superseded by `silver_content_classification`; **archived before any drop** (see below) |
| `bronze_enrichment_raw`, `silver_*`, the four marts | **live** | must stay current; staleness is a defect |
| `gold_growth_facets` | **retired/dormant** | 205 rows, dropped at W9 |

**RESOLVED 2026-09-14 — `gold_analyses` is retired/dormant, archived before drop.** Owner's
call: it is superseded by `silver_content_classification`, so it is not live history and gets
no freshness gate. Two consequences, both mandatory:

1. **Archive is the precondition, not a nicety.** Before any drop, export the full table to
   Parquet under `data/lake/archive/gold_analyses/` (`COPY … TO … (FORMAT PARQUET)`), verify the
   row count on the export equals the live count **at that moment**, and record the export path
   + count in the W9 log. The archive is what makes the drop reversible in substance even though
   it is not reversible in place — without it, "superseded" is an assertion and the rows are
   simply gone. (Baseline for drift: 9,576 as of 2026-09-14, read live. Do not assert against
   it — W-FREEZE should hold it static, so any change is a freeze regression worth investigating
   before the archive.)
2. **Retirement is TARGET state, not current state.** `gold_analyses` stays fully live and
   readable through W6's backfill and W7's parity gate — W7's whole acceptance is a side-by-side
   diff against it. It becomes dormant only once the parity proof passes and no view reads it.
   Sequenced: W6 migrates the rows → W7 proves parity → archive → human approval → W9 drop.

Checks treat it as dormant from the W7 cutover: no freshness gate, no non-shrinkage gate on a
table nobody writes. What replaces that signal is the W6 reconciliation identity
(`count(silver_content_classification) + count(quarantine) == count(gold_analyses)` at migration
time) — the archive proves retention, the reconciliation proves migration.

### `decompose-lean-units` — W6 (and W4/W8) exceed the cap

`rule://decompose-lean-units`: a unit is TOO BIG if it "spans multiple deliverables/contracts,
is expected to run >= ~30-45 min, or would hold a long materialization in-runtime" — SPLIT it.
Target is "one coherent deliverable with ONE observable acceptance criterion, ≤5 explicit files".

W6 as written bundles **four** deliverables — (a) the 9,576-row classification backfill,
(b) the conform production caller as a Dagster asset, (c) the four gold marts as views,
(d) the `gold_content_shape_performance` grain fix — and its single acceptance only covers ONE
of them (the backfill reconciliation), leaving the marts, the grain fix and the conform caller
un-accepted by the unit that builds them. That is the defect: not speed, but an acceptance that
does not reach everything the unit does.

**Corrected — the runtime claim above was overstated.** Measured: `gold_analyses` is 9,576 rows
at ~1,787 bytes of `result_json` each, ~17 MB of payload total. Parsing that into local DuckDB
is seconds of work, not a long materialization, and it does not threaten the 2h cap. It should
still run as a background process for a different reason — so main keeps working and a fresh
agent verifies the result — but the sizing argument for splitting W6 is the four-deliverables /
one-acceptance mismatch, not runtime. Suggested split, by contract boundary rather than phase:

- **W6a** — grain fix + four marts as views (additive, no data movement, verifiable on an empty
  silver). Deps: none beyond W1.
- **W6b** — conform production caller registered as a Dagster asset. Deps: W4 (terminal states).
- **W6c** — the 9,576-row backfill + idempotency instrument. Deps: W6a/W6b. **This one is a
  background process, not a subagent unit** — `rule://offload-long-runs`: launch it, checkpoint,
  and let main dispatch a fresh bounded agent to verify the result.

W4 (harvested producer + retry driver) and W8 (quarantine consumer + DQ/freshness gates) each
bundle two deliverables; each is defensible as ONE unit only because both halves share a single
contract (W4: the same harvest run owns both per ADR-0014 D2/D3; W8: the same quarantine table).
State that justification explicitly in those units, or split them.