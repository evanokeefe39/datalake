# Content source for the HTML post-mortem

This file is the CONTENT for the post-mortem page. Do not invent facts. Every number and
claim below is verified. You may shorten prose, but you MUST NOT drop a finding.

## Language rule (Simplified Technical English)

Write in Simplified Technical English (ASD-STE100 style):

- One idea per sentence. Maximum about 20 words per sentence.
- Use the active voice. Use the present tense or the past tense. Do not use the -ing form
  when you can avoid it.
- Use one word for one meaning. Do not use two words for the same thing.
- Do not use idioms, metaphors, or humor. Do not use "robust", "leverage", "delve",
  "seamless", "holistic", "best-in-class".
- Define a technical word the first time you use it.
- Give each instruction or finding as a short, direct statement.

This is for two audiences. Juniors must understand it. Seniors must not lose the detail.
So use a TWO-REGISTER pattern for every finding:

1. A plain statement that a junior can read alone.
2. A `DETAIL` block that carries the file, the line, the count, or the mechanism.

The junior can stop at level 1. The senior reads level 2. Neither loses anything.

---

## 1. Masthead

- Title: "Enrichment v3 migration — post-mortem"
- Subtitle: "Where the work went wrong, and what would have caught it"
- Date: 2026-09-13
- Branch: `feat/enrichment-v3-phase-1-seam-and-landing`
- Verdict line, in large type: **7 of 36 exit criteria are met. The code branch is green.
  The system does not run.**

---

## 2. Summary (BLUF)

Use this exact set of facts.

- A migration was planned in 7 phases. Subagents built each phase. Each phase reported
  complete. The test suite was green.
- Independent audit then checked each exit criterion against the LIVE database, not against
  the branch.
- Result: 7 of 36 criteria are genuinely met. 5 are vacuous. 12 are unmet. 7 are partial.
- 12 objects exist as code only. Nothing materialized them in the live database.
- The pipeline cannot complete one cycle.
- The cause is the process, not the workers. The plan divided the work by component. No unit
  owned the connections between components. Each worker did what its brief said. The briefs
  did not name the connections.

Include a KPI strip with these six numbers:

| Label | Value |
|---|---|
| Criteria genuinely met | 7 / 36 |
| Criteria that are vacuous | 5 |
| Objects defined in code, materialized nowhere | 12 |
| Bronze rows ever landed | 0 |
| Unit tests passing while the pipeline cannot run | 701 |
| Production seam adoption | 1 of 4 flows |

---

## 3. Where it went wrong — the six concrete failures

For each failure give: a plain statement, a DETAIL block, and the evidence.

### 3.1 Twelve objects exist only as code

Plain: The migration created 12 new tables and views. None of them exists in the live
database. The old tables are still the source of truth.

DETAIL: The code-only objects are `silver_content_classification`, the five conform tables
(`silver_visual_annotations`, `silver_visual_summaries`, `silver_audio_transcripts`,
`silver_text_annotations`, `silver_text_summaries`), `silver_enrichment_quarantine`,
`silver_classification_incoming`, and all four gold marts (`gold_post_enrichment`,
`gold_creator_performance`, `gold_content_shape_performance`, `gold_top_posts`). The live
`state.duckdb` still holds `gold_analyses` (9,576 rows) and `gold_growth_facets` (205 rows).
The four gold marts are `CREATE OR REPLACE VIEW` statements in code. They are not even
tables.

### 3.2 No view reads the new table

Plain: The dashboard reads the old enrichment table. It has never read the new one.

DETAIL: Zero views read `silver_content_classification`. Three views still read
`gold_analyses`. The live `v_post_detail` reads `ga.result_json` and `ga.prompt_hash`
directly. The silver-bound version of that view exists only inside the `serving/assets.py`
asset body, and it has never executed.

### 3.3 No data ever landed in bronze

Plain: The new bronze landing table is the foundation of the whole design. It has zero rows.

DETAIL: `bronze_enrichment_raw` has zero landed rows. Worse, the test mocks use a flat
envelope. The real provider responses nest. So the landing code has never met a real payload
shape. The first real run may fail on the real shape.

### 3.4 The conform layer has no caller

Plain: The team built the layer that turns bronze into silver. Nothing calls it.

DETAIL: Phase 4's conform module has no asset, no invocation, and no reference anywhere in
`src/`. The five silver tables exist only in test fixtures.

### 3.5 The pipeline has two halves, and they do not touch

Plain: One half writes work to a new place. The other half looks for work in the old place.
So the pipeline runs once, then stops.

DETAIL: The drain materializes `enrichment_submitted` partitions
(`instagram/assets.py:1289`) that no consumer reads. `submit.py:77` discovers work by
reading `batch_jobs`, and nothing writes `batch_jobs` any more.
`_require_legacy_queue_tables` (`batch.py:51`) even raises an error if that retired table is
absent — the code actively preserves the dead contract. Separately, `enrichment_harvested`
has no production writer at all, so the in-flight set never shrinks and the drain suppresses
everything after the first run.

### 3.6 The seam is built but not used

Plain: One phase created a single boundary for all external providers. Four production flows
should use it. One does.

DETAIL: 12 bypass sites exist: `submit.py:152`, `submit.py:170`, eight sites in
`harvest.py` (311, 316, 319, 334, 446, 454, 459, 462), `facets_batch.py:318`, and
`facets_batch.py:322`. `harvest.py` never imports `seam` at all. `seam.run_lifecycle` has
zero production callers.

### 3.7 (Callout) The vacuous gate

This is the most important single finding. Give it its own visually distinct block.

Plain: The team relied on one test to prove "no regression". That test cannot detect the
failure. It asserts the old status quo.

DETAIL: `tests/operational/test_state_compatibility.py` passes with 77 tests. It passes
because the schema catalog still lists `gold_analyses` and does not expect
`silver_content_classification`. So the test asserts the status quo. It cannot detect that
the migration never happened. It also checks only that each view can be SELECT-ed. It never
checks that the view DEFINITION is unchanged. So the criterion "19 transitive views are
asserted, not assumed" has no assertion anywhere in the repository.

The lesson in one line: a gate gives evidence of the things it can assert. A gate that
asserts the status quo cannot certify a migration.

---

## 4. Root cause — the Five Whys

Render this as a chain. Each Why must be visible. End with the root cause in large type.

- **Why 1 — Why are the two pipeline halves disconnected?** Because each half implemented
  its own brief against the state in front of it. The drain writes to partitions. `submit.py`
  reads a table. No brief mentioned the other half.
- **Why 2 — Why did no unit own the connection?** Because the plan divided the work by
  component. Integration was nobody's task. The plan knew the coupling: §4 names the drain as
  Phase 2's consumer, and Phase 2's text says the drain rewrite "must land with the queue
  retirement". But the retirement was in Phase 7, and no criterion in any phase said "submit
  consumes what the drain produces".
- **Why 3 — Why did phase verification accept this?** Because each phase's test suite injects
  a fake on one side of every seam. The drain tests against a fake instance. The submit tests
  against a fake table. No test asserts a handoff between the two real halves.
- **Why 4 — Why did the exit criteria not force integration?** Because the criteria state that
  a component EXISTS. They do not state that data moves between components. "build_adapter is
  the ONLY place a provider is named" passes by unit-testing the registry, while 12 production
  sites bypass it. "Two drain runs enqueue no post twice" passes because run 2 enqueues
  nothing at all.
- **ROOT CAUSE — The plan treated integration as a result that would appear by itself when
  all components were complete. Integration was not an owned, tested deliverable.**

Add this sentence as a highlighted conclusion: "The failure was set at dispatch-design time,
before any worker read a prompt. The workers' tests passed because the tests were true
statements about their own small slices."

---

## 5. Who is at fault — and who is not

Include this, because "the subagents failed" is not accurate.

- The workers did their briefs. The briefs did not name the connections.
- Three cases are "worker correct, interface defective":
  - The `max_tokens` bypass. The seam's `Item` is per-item. `max_tokens` is per-job. The
    worker could not express it. The worker wrote the reason in a docstring.
  - The `DRAIN_ATTEMPT_ROUND = 0` hardcode. The ADR mandates distinct retry keys, but says
    nothing about the key shape. The worker implemented the guard honestly for round 0.
  - The missing `enrichment_harvested` writer. The brief said "derive in-flight from the
    instance". That is a guard instruction. No brief named the writer.
- Two cases ARE worker faults, and both are small:
  - `DagsterInstance.get()` fallbacks. The asset already accepted an injected instance. The
    worker chose the permissive path.
  - Silent failure paths. `classify_error` sends unknown exceptions to TERMINAL. Poll loops
    warn and continue with no bound. Asset checks pass on unread parquet.

Verdict bar: "Orchestration is the dominant cause. The workers' faults are second-order."

---

## 6. Lessons, in three layers

This is the section the juniors must be able to read and the seniors must be able to use.
Keep the three layers visually separate. Each layer needs: the principle, why it matters
here, and the check that enforces it.

### Layer A — Generic software design and migration principles

These apply to any migration, in any language, of any system.

1. **A contract between two components needs an owner.** A component is not done when it
   works alone. It is done when its caller uses it and its provider feeds it.
   - Why here: the seam was built and 12 sites bypassed it.
   - Check: name the production consumer of everything you write, and the production producer
     of everything you read. Write both names in the brief.

2. **Test the round trip, not the existence.** A test that asks "does this function exist"
   passes while the system is broken. A test that asks "does data arrive at the other end"
   fails when the system is broken.
   - Why here: every criterion was an existence claim.
   - Check: for every new component, add one test that runs producer and consumer against the
     same fake and asserts a non-empty handoff.

3. **A green gate is only evidence of what it asserts.** Know what your gate cannot see.
   - Why here: `test_state_compatibility.py` asserted the status quo.
   - Check: for each gate, write the sentence "this gate cannot detect X". If X is a real
     risk, the gate is wrong.

4. **Delete what you replace, in the same change.** A retired path that still exists will be
   used. Code preserves dead contracts.
   - Why here: `_require_legacy_queue_tables` raised an error to keep a dead table alive.
   - Check: make "delete everything you supersede" an explicit item in the brief and the
     acceptance.

5. **Do not defer retirement to a later phase than the rewiring.** If phase N stops writing
   to a store, phase N must also move the readers.
   - Why here: the drain was rewired in Phase 2; the queue retirement was in Phase 7.
   - Check: for each phase, list the producers and the consumers of every store it touches.

6. **An abstraction that only one caller uses is not adopted.** Adoption is a count, not a
   design.
   - Why here: seam adoption was 1 of 4 flows.
   - Check: express adoption criteria as a count or a grep test, never as "the abstraction
     exists".

7. **Every mechanism needs a driver.** A retry, a quarantine, a reconciliation must name the
   actor and the trigger. A mechanism with no driver never runs.
   - Why here: retry, quarantine, and the harvested writer all had no actor.
   - Check: for each mechanism, write "who calls this, and when".

8. **Fail loudly, and bound every loop.** A silent default turns a small error into permanent
   bad data.
   - Why here: `classify_error` sent unknown exceptions to TERMINAL. Poll loops had no bound.
   - Check: each error path names its behaviour for an unknown error, and each loop names its
     exit.

9. **Keep dependencies pointing inward.** A shared module must not import from one of its
   consumers.
   - Why here: `adapters.py` imported `GeminiTierConfig` from the Instagram module.
   - Check: list every import that leaves your assigned slice. Flag each one.

10. **Verify against the real artifact, not the test double.** A mock encodes your assumptions.
    Real data does not.
    - Why here: mocks were flat; real envelopes nest.
    - Check: before you trust a module, run it once on one real payload.

### Layer B — Data architecture and migration best practice

These apply to moving or reshaping data structures.

1. **A schema change is a contract change.** Name the contract and the requirement before you
   change it.
   - Why here: `gold_content_shape_performance` dropped `platform` from its grain.
   - Check: for each schema change, list every consumer and say how you refresh it.

2. **A migration is not done until it runs against the live store.** Code that defines a table
   is not a table.
   - Why here: 12 objects were defined and never materialized.
   - Check: the acceptance query is `SELECT count(*) FROM <new object>` against the real
     database. A code search is not evidence.

3. **Never drop or replace a live table without a backup and a review.** Read every migration
   before you run it.
   - Why here: this repository lost 391 real rows to a `DROP TABLE` in a numbered migration.
   - Check: additive DDL only in numbered migrations. A drop needs its own reviewed step.

4. **Keep the schema catalog in step with the migration.** If the catalog still describes the
   old world, the compatibility test proves nothing.
   - Why here: the catalog still lists `gold_analyses`. That made the gate vacuous.
   - Check: update the catalog in the same commit as the schema change.

5. **Every derived layer needs provenance.** A derived value must record what made it, from
   what, with which logic, and when.
   - Why here: the design says this. The implementation has no materialized derived layer at
     all.

6. **Raw data is immutable.** Never rewrite what the source sent. Conform it later.
   - Why here: the design is correct on this point. Bronze keeps the verbatim response. The
     code is right; the data is absent.

7. **A key must be unique at the grain you claim.** Check the grain against every dimension
   the data carries.
   - Why here: a mart keyed by `(domain, topic, follower_tier, facet_name, facet_value)`
     dropped `platform`, though the source carries it. Cross-platform rows would merge.

8. **Quarantine needs a consumer.** A dead-letter table that nothing reads is a log, not a
   control.
   - Why here: `silver_enrichment_quarantine` has no reader and no check on its growth.
   - Check: for every quarantine, name the alert or the query that reads it.

9. **Version the derivation.** When logic changes, old rows must be detectable as old.
   - Why here: the prompt hash is versioned correctly. That part is a good example to cite.

### Layer C — Data pipeline migration best practice

These apply to pipelines that move and transform data on a schedule.

1. **Declare dependencies. Do not hope for order.** Each asset states its inputs and its
   outputs.
   - Why here: no criterion connected the drain's output to submit's input.

2. **Write, audit, then publish.** Write to a place no consumer reads. Check it against the
   quality contract. Then swap it in.
   - Why here: objects were defined and treated as done before any check ran on real data.

3. **Prove idempotency by running twice and comparing.** A second run must produce the same
   state, not a duplicate and not a stall.
   - Why here: the "no post twice" test passed because run 2 suppressed everything. The real
     system stalls after one cycle.

4. **A guard must tell a dormant source from a broken one.** A quiet source is either retired
   or dead. The system must know which.
   - Why here: the in-flight set grows and never shrinks. The system cannot tell "all work
     finished" from "nothing is running".

5. **Every pipeline needs an end-to-end smoke run on real data before you call it finished.**
   One real item through the whole path.
   - Why here: zero bronze rows landed. The path never ran end to end.

6. **Check the freshness and the volume of each layer, not only its existence.** A layer with
   zero rows is not a healthy layer.
   - Why here: the audits asked "exists and populated / exists and empty / code only". That
     three-way question is the check.

7. **Instrument the boundary.** A guarantee you cannot observe is not a guarantee.
   - Why here: the accounting identity exists and is good. Use it as the example of correct
     design.

---

## 7. Harness improvements

The user asked specifically for improvements to the harness: the `dlc` and `sdlc` subagents,
the skills, the rules, and the hooks. Give each its own block. Each improvement must name the
failure it prevents.

### 7.1 `dlc-worker` and `sdlc-worker` agents

1. **Add a contract-inventory step to the dispatch, before any code.** The dispatcher lists
   every producer and every consumer that the unit touches. Any connection with no owner
   becomes its own unit.
   - Prevents: the disconnected halves.

2. **Add a mandatory "consumer/producer" line to the unit brief.** The brief must state:
   "You write to X. Who reads X? You read Y. Who writes Y?"
   - Prevents: the missing `enrichment_harvested` writer.

3. **Change the acceptance from existence to round trip.** The worker must show data arriving
   at the other end, not a function existing.
   - Prevents: the 12 bypass sites, the unadopted seam.

4. **Make "delete what you supersede" an explicit deliverable in every unit.**
   - Prevents: the preserved dead queue contract.

5. **Add a mandatory real-data smoke step.** One real item end to end before the unit is done.
   - Prevents: the zero bronze rows and the flat-mock / nested-real mismatch.

6. **Require the worker to report what it could NOT see.** The worker names the callers it does
   not control. The dispatcher assigns those to another unit.
   - Prevents: the unassigned call-site migration.

7. **Give the worker a budget and a bounded retry.** Three attempts, then escalate.

### 7.2 Skills

1. **Add a `migration-verification` skill.** It carries the four round-trip assertions:
   handle round trip with a multi-chunk case, partition-key round trip, seam adoption through
   the production entry point, and a grep test for bypasses.

2. **Add a `contract-mapping` skill.** It gives a repeatable method to list producers,
   consumers, and owners for a change.

3. **Add a `schema-migration` skill that encodes the datalake rules.** Additive DDL only.
   Read every migration before you run it. Back up before a drop. Update the schema catalog in
   the same commit.

4. **Extend the existing verification skill with the "could this test fail?" question.** The
   reviewer must write one sentence that says what bug the test would catch. If there is no
   such bug, the test is vacuous.

### 7.3 Rules

1. **A rule that a gate must declare what it cannot detect.** Each gate states the failures it
   is blind to.

2. **A rule that existence is never acceptance.** An acceptance criterion phrased as
   "X exists" or "X is the only place" must be paired with an adoption assertion.

3. **A rule that integration is an owned deliverable.** Every multi-unit plan names the units
   that own each seam, or states that a seam has no owner and why.

4. **A rule that a mechanism names its driver.** Retry, quarantine, reconciliation, and
   cleanup each name the actor and the trigger.

5. **A rule that a phase must not defer retirement of a store that it stops writing to.** The
   readers move in the same phase.

6. **A rule for layering.** A shared module must not import from a consumer module.

7. **A rule that the raw data is immutable and the derived data carries provenance.**

### 7.4 Hooks

1. **A pre-commit or CI hook that greps for provider names outside the adapter modules.**
   `gemini_batch.` and `qwen_client.` may appear only in the adapter files. This is a
   mechanical check; it catches every bypass.

2. **A hook that fails the build when a schema catalog and the live database disagree.** The
   catalog must expect the new objects and must not expect the retired ones.

3. **A hook that fails when a defined table is never materialized.** A code-only object is a
   failure, not a warning.

4. **A hook that checks the grain of every new key against the dimensions the data carries.**

5. **A hook that reports the count of rows in every new object.** Zero rows blocks the merge.

6. **A hook that runs the views' definitions as a diff.** Assert the definition equals the
   expected text, not only that the view can be selected.

Add a final note on ordering: "The hooks are the cheapest control and the rules are the
strongest. The agents and the skills change behaviour; the hooks catch what behaviour misses.
Use the hooks for the mechanical checks and the agents for the judgment."

---

## 8. Evidence appendix

List these files. The reader can open them for the full detail.

- [`../postmortem.md`](../postmortem.md) — the root-cause analysis (301 lines)
- [`../audits/p1-p2.md`](../audits/p1-p2.md) — 18 criteria, phases 1-2
- [`../audits/p3-p4.md`](../audits/p3-p4.md) — 8 criteria, phases 3-4
- [`../audits/p5-p6.md`](../audits/p5-p6.md) — 10 criteria, phases 5-6
- [`../reviews/architecture-soundness.md`](../reviews/architecture-soundness.md)
- [`../reviews/interfaces.md`](../reviews/interfaces.md)
- [`../reviews/round-semantics.md`](../reviews/round-semantics.md)
- `tasks/plans/enrichment-v3-migration-master.md` — the plan

Add one closing honesty note: the architecture is sound. The design does not need to change.
The implementation drifted from the design, and the process could not see the drift. Keep the
design. Fix the process.

---

## 9. Build notes for the frontend agent

- Follow the datalake house report style: the playbook in
  `~/repos/datalake/AGENTS.md` names
  `~/repos/vibe-coding-analytics/docs/reports/2026-09-06-vibe-coding-analytics-retrospective.html`
  as the visual reference. Use the same register: VT323 display font, IBM Plex Sans and Mono
  for body and code, sticky table of contents, scroll reveal, and a reduced-motion branch.
- Read `~/repos/dev-portfolio-2/docs/content-style.md` for the house voice rules.
- Keep the tone honest and direct. Do not soften the numbers. Do not add celebration.
- The three lesson layers must be clearly separated and must not blur together.
- Make the KPI strip legible at a glance.
- The page must be readable on a phone. The team reads reports on phones.
- Save the file to `C:/Users/evano/repos/datalake/docs/postmortem/enrichment-v3/postmortem.html`.
- Verify the page in a real browser at desktop and mobile width. Take screenshots. Check the
  KPI values render as the numbers above.
