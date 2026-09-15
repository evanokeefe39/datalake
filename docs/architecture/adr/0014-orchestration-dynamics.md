# ADR-0014: Orchestration dynamics — the three drivers (harvested, retry, quarantine) and the partition-key shape that survives derivation

- Status: Accepted (2026-09-14). Ratified under delegated decision
  authority while the owner was unavailable; panel round 2 had already returned
  the retirement half of this decision set as sound (SYNTHESIS §3). This ADR
  supplies the dynamics ADR-0012 described without actors.
- Decided: 2026-09-14
- Related: ADR-0012 (Dagster-native orchestration — this ADR completes its
  **dynamics**: it names the actors ADR-0012 only described), ADR-0013 (the
  seam keeps no ledger — every mechanism below re-derives state from the
  instance and the lake; **no new ledger is introduced**), ADR-0011 (layered
  model: bronze landing → silver conform → gold), ADR-0008 (one API seam),
  ADR-0009 (qwen batch service)
- Evidence: live `data/state.duckdb` (8 `gold_analyses` rows with
  `model IS NULL`, all `prompt_hash 24c8e291`, all valid classification JSON);
  `src/datalake/defs/enrichment/partitions.py`; `instagram/assets.py`
  drain guard (`ig_posts_gen_batches`); `defs/enrichment/conform.py` reason
  codes; remediation plan W0/W4/W6/W8.

## Context

ADR-0012 decided the statics of Dagster-native orchestration — in-flight is a
set difference, retry is a new partition key, failure is an anti-join check,
dead_letter is retired — but never named the **drivers**: which actor
materializes `enrichment_harvested`, which actor mints a round-N key and when,
and which actor reads `silver_enrichment_quarantine`. The remediation plan's
audit (P2/P4/P5) found the consequence: `enrichment_harvested` has **no
production writer anywhere in `src/`**, so
`in_flight = materialized(submitted) − materialized(harvested)` only ever
grows, the drain guard suppresses those posts forever, and the pipeline stalls
after one cycle. A mechanism with no named actor is precisely the defect: a
spec that says "retry is a new partition key" with no writer of that key is a
spec nobody can implement.

Three dynamics are missing. Each is specified below with **ACTOR / TRIGGER /
STATE TRANSITION**, and each answers the production-consumer /
production-producer question for everything it writes and reads. Alongside
them, one supporting defect is fixed: the current
`partition_key(workload, round, [pid])` **digests the round away** — it
returns `sha256(workload ∥ round ∥ pid)[:16]`, so the round is one-way and
cannot be recovered from a materialized key. A retry driver that must answer
"what round is this in-flight key at?" cannot. The fix is the key shape itself.

## Decision

### D1. The partition key is a self-describing composite, not a digest

**ACTOR (writer):** the drain (`ig_posts_gen_batches`) for round 0; the retry
driver (D2) for round N. **ACTOR (reader):** the drain guard
(`drain_suppressed_post_ids`) and the retry driver.

> **Amended by [ADR-0016](0016-discovery-and-guard-share-one-derivation.md).**
> The key grammar above is unchanged. The WRITER is not: the drain is deleted and
> `engine/submit.py` is the sole discovery actor, so the submit stage writes its
> own round-0 placeholder and reads the in-flight set in the same run. The guard
> is no longer a second derivation — see ADR-0016.

Today's key hides every dimension behind a sha256 slice. Replace it with a
**readable, delimiter-disambiguated composite**:

```
<workload>\x00r<N>\x00<post_id>          # e.g. "content_classification\x00r0\x00Cabc123"
```

- **Derivable by both sides, both directions.** The drain guard derives the
  key it expects for a candidate (`workload, round, pid` → key, membership
  test). The retry driver derives the **dimensions back out of** an observed
  key: split the key, read `r<N>` as the round, read the pid, read the
  workload. Neither side consults any table to do this — the key IS the
  record, which is why ADR-0013's no-ledger rule holds.
- Round 0 keys keep the same shape (`r0`), so there is one grammar and no
  special case for first attempts.
- A 64-bit digest is **not needed**: the dimensions are short and enumerable,
  and dynamic partitions are compared by exact string. The digest bought
  collision-resistance for inputs that never vary beyond its components.
- **Migration:** keys materialized before this change are opaque round-0
  digests. Grandfather them as round 0 (they were), and let the drain guard
  treat an unparseable key as round 0 for suppression only. New keys are
  composites from the first write. This is the versioned-backfill row of the
  remediation plan §3.

This one change is what makes D2 possible at all.

### D2. The harvested driver: the harvest asset itself reports terminal partitions

**ACTOR:** the *harvest* stage — the same run that polls the seam to terminal
and lands responses verbatim in bronze. This is a real, existing Dagster
construct: the harvest **run** reports a partition materialization of the
`enrichment_harvested` asset when `run_lifecycle` returns and every item for
that partition is terminal, i.e. `instance.report_runless_asset_event(
AssetMaterialization(asset_key=AssetKey("enrichment_harvested"), partition=key))`
— exactly the mechanism the drain already uses for `enrichment_submitted`
(`_materialize_submitted_partitions`), mirrored on the other side of the seam.

- **TRIGGER:** terminal state on the seam (`run_lifecycle` returns
  `(handles, results)` with every item `completed` or `failed`) — not a timer,
  not a sensor on bronze. The transition *submit → terminal* is the event.
- **STATE TRANSITION:** the partition's key moves from the in-flight set to
  the harvested set: `materialized(submitted) − materialized(harvested)`
  shrinks by that key. That is the ONLY bookkeeping — there is no status
  column, no queue row (ADR-0012 decision 1, ADR-0013).
- **Grain:** one harvested partition per submitted partition (per post, per
  round), so the set difference is exact and the drain guard's per-post
  membership test stays correct.
- **How the drain guard observes it:** it already does —
  `drain_in_flight_keys` reads `in_flight_partitions(instance)` on every
  drain run, so once the harvest run reports the event, the next drain run's
  guard stops suppressing the post. No new observer code; the missing piece
  was never the reader, it was the writer.
- **Producer / consumer accounting:** the harvest run is the production
  *producer* of the `enrichment_harvested` event and the *consumer* of the
  seam's terminal states (which the qwen/gemini service produces over the
  HTTP contract). The drain guard is the production *consumer* of the
  resulting instance state. The bronze rows it lands are produced by the
  harvest run's verbatim landing and consumed by conform (D4).

### D3. The retry driver: the harvest run mints round N+1 when a partition completes with failures

**ACTOR:** the harvest run again — the same run that reports the harvested
partition (D2). This is deliberate: the harvest run is the ONLY actor that
observes the seam's terminal verdict for a partition at the moment it becomes
known. A separate "retry sensor" would have to re-derive the failure set
later, which duplicates judgment and widens the window for drift. (It is also
why `DRAIN_ATTEMPT_ROUND = 0` must go — the drain no longer owns rounds.)

- **TRIGGER:** the harvest run's terminal accounting for partition key K:
  any item in K's results is `failed`, or K's bronze landing has no conformed
  silver counterpart. Success-only partitions mint nothing.
- **STATE TRANSITION (mint, not mutate):** the harvest run materializes a NEW
  round-N+1 partition key for each failed post, derived with the D1 grammar:
  `retry_key = partition_key(workload, N+1, [pid])`. The round-0 (or round-N)
  key stays harvested — it is never re-materialized (ADR-0012 decision 5 and
  its S3 evidence stand: re-materializing a harvested partition is invisible
  to the set difference and orphans provider work). The next drain run sees
  the round-N+1 submitted partition as *not in flight* (only
  `enrichment_submitted` participates in the drain guard's in-flight set for
  the candidate's CURRENT round), re-enqueues the post, submit materializes
  `enrichment_submitted` at the round-N+1 key, and the cycle repeats.
- **Budget replaces `scheduled_for` backoff and `MAX_ATTEMPTS`:** there is no
  timer and no attempt column. The attempt count for a post is **the round
  number read straight out of the highest-round key for that post**, which
  both sides derive from the D1 composite. "Give up" is the derived condition
  `round >= MAX_ROUNDS` (constant in `partitions.py`, replacing
  `MAX_ATTEMPTS=5`): a partition at `MAX_ROUNDS` whose post is still failed
  mints NO further key — the failure check (below) owns its visibility
  permanently. No jitter/backoff is specified deliberately: rounds advance
  only when a real harvest cycle completes, which already spaces attempts by
  the natural cadence of a full poll-to-terminal cycle. `scheduled_for` had a
  job the queue is retiring.
- **`dead_letter` is replaced by the anti-join, enforced as a BLOCKING asset
  check.** The failure set is
  `landed(bronze_enrichment_raw) − conformed(silver_*)` —
  `partitions.failure_set` already exists and is specified (not implemented)
  as the source. A Dagster **asset check** on the conform asset evaluates it
  every conform run and FAILS the check — with the stuck post ids and counts
  in the check metadata — whenever it is non-empty. It is blocking: a red
  check blocks downstream materialization, so a post whose provider work
  failed can never fall out of the pipeline silently. This is exactly
  ADR-0012 decision 7; the dynamic adds the named actor (conform's check) and
  the cadence (every conform run).
- **Producer / consumer accounting:** the harvest run is the producer of the
  round-N+1 key (it writes only Dagster instance events — no table, no
  ledger). The drain is the consumer (its guard admits the round-N+1
  candidate). The bronze rows the check reads are produced by the harvest
  landing and by conform; the silver conformance it anti-joins is produced by

### D4. The quarantine disposition: triage is a human-cadenced read over a redrivable table

**ACTOR (writer):** conform (`conform.py`), which already writes
`silver_enrichment_quarantine` with reason codes from
`REASON_PROVIDER_ERROR` through `REASON_COMPLETENESS` and a verbatim 500-char
excerpt. **ACTOR (consumer):** the operator — via the triage surface W8
builds: a `v_quarantine_triage` view plus a NON-blocking (advisory) asset
check whose failure text says "quarantine grew: go triage". This is the named
consumer the audits found missing; the post-mortem's phrase was "a control
surface with no reader".

- **TRIGGER:** for the writer, any conform validation failure or
  `ok=False` bronze row. For the consumer, the triage cadence: **weekly**, at
  the same cadence as the content-strategy review, plus any time the advisory
  check fires. Quarantine is not an on-call surface — a growing quarantine
  under MAX_ROUNDS exhaustion is expected provider-pathology, not a paging
  event; the BLOCKING check (D3's anti-join) is the loud surface, this one is
  the reading surface.
- **Retention:** quarantine rows are retained until **resolved, never
  aged out**. The table is small by construction (a fraction of the corpus,
  keyed `(post_id, platform)` with provenance and excerpt). Deleting a
  quarantine row destroys the only record of why a post is not enriched —
  the anti-join check would fire instead. Resolution means either redrive or
  an explicit `disposition` note; the table grows a `disposition` /
  `resolved_at` annotation at W8 time (additive, self-versioned).
- **Redrive rule:** a quarantined post redrives by **re-landing, not by
  re-conforming**: after the operator fixes the upstream cause (prompt
  change → new `prompt_hash`, schema change → new `schema_version`, provider
  fix → new attempt), the post is re-submitted at a fresh partition key (D1
  grammar, next round), the new response lands verbatim in bronze, and
  conform runs over it. The old quarantine row is marked resolved and its
  bronze counterpart stays as history. Conform is deterministic and
  idempotent, so redrive never re-bills a provider call for already-landed
  responses — only the genuinely re-run posts hit the seam again.
- **Producer / consumer accounting:** bronze rows are produced by the
  harvest landing; the quarantine table is produced by conform; the triage
  view and advisory check consume it; the redrive loop's consumer is the
  drain (via the fresh partition key).

### D5. The 8-row disposition: re-provenance, do NOT quarantine

Live-database verification (2026-09-14, `data/state.duckdb`): exactly **8
rows** of `gold_analyses` have `model IS NULL`; all 8 share
`prompt_hash = 24c8e291`; all 8 have non-empty `result_json`; all 8 parse as
valid classification JSON. Their content is intact and migratable; the only
defect is missing **model provenance**, which is metadata, not data.

**Rule:** the 8 rows are migrated to `silver_content_classification` with
their verified JSON payload, and their provenance block is filled as:

| Field | Value for the 8 rows |
|---|---|
| `provider` | `gemini` (the only provider that ever wrote `gold_analyses`) |
| `model` | `unrecorded-legacy-null` — a sentinel, NOT `NULL` and NOT a fabricated model name. AMENDED 2026-09-14: the owner selected this literal over the original `legacy-unknown`, keeping the code constant (`classification.MODEL_LEGACY_NULL`) as the single definition |
| `prompt_hash` | `24c8e291` (unchanged — the legacy prompt identity is real and recorded) |
| `schema_version` | the classification schema version current at migration time |
| `run_id` | the backfill's migration run id (provenance of the *migration*) |

The sentinel `unrecorded-legacy-null` is honest: it says "this result exists and was
verified, but the producing model name was never recorded" — which is true.
Quarantining would be a lie (the data is valid); backfilling a plausible
model name would be a worse lie (fabricated provenance). The migration
script asserts `count(dispositioned) == 8` and fails loudly otherwise, so a
ninth `model IS NULL` row arriving between audit and migration cannot slip
through un-dispositioned.

**This amends W6's acceptance** (see the remediation plan): the 8 rows are
NOT a "re-hash or quarantine" fork — the re-hash branch is void, because the
model is unknowable by construction and re-hashing
`prompt_identity_v1(prompt, model)` requires the model. The only honest
disposition is migrate-with-sentinel-provenance, verified by the count
assertion above.

### D6. Worked cycles (paper, end to end)

**Cycle 1 — failed submit retries and then releases.**
1. Drain run 1: post `P` is a candidate; guard finds no in-flight key for
   `P`; submit materializes `enrichment_submitted` at key
   `content_classification\x00r0\x00P` (D1).
2. Harvest run polls the seam; item for `P` returns `failed`
   (e.g. 429-quota). Harvest reports `enrichment_harvested` at the r0 key
   (D2) and, per D3, mints
   `content_classification\x00r1\x00P` as an `enrichment_submitted`-side
   pending round key (registered dynamic partition + runless event, same
   mechanism the drain uses).
3. Drain run 2: guard derives `P`'s current-round key (`r1`), tests the
   in-flight set — `r1` is not yet submitted, `r0` is harvested — so `P` is
   NOT suppressed. Submit materializes `enrichment_submitted` at `r1`.
4. Harvest run 2: item succeeds; response lands verbatim in bronze; conform
   writes the silver row. In-flight shrinks to empty for `P`. The anti-join
   check is green.
5. If instead the retry also failed, step 2 mints `r2`; at
   `r >= MAX_ROUNDS` no further key is minted, and `P`'s bronze-landed row
   (if any) with no silver counterpart holds the anti-join check red — the
   loud, derived replacement for `dead_letter`.

**Cycle 2 — malformed response lands in quarantine and redrives.**
1. Harvest lands `P`'s response verbatim in bronze (valid envelope, garbage
   body — truncation mid-JSON). Harvest reports the harvested partition (D2).
2. Conform run: parse fails → row lands in `silver_enrichment_quarantine`
   with reason `parse_error`, bronze provenance, and a 500-char excerpt.
   No silver row is written for `P`.
3. The BLOCKING anti-join check fires on the same conform run:
   `landed(bronze) − conformed(silver)` contains `P` — observed check
   failure, not a log line. The advisory quarantine-growth check names the
   triage surface.
4. Triage (weekly cadence, or immediately on the advisory check): the
   operator reads the excerpt, sees truncation, fixes the upstream cause
   (e.g. the provider token limit), and marks the row's disposition.
5. Redrive: `P` is re-submitted at a fresh key (D1 grammar, next round) —
   by explicit `post_ids` bypass or the next drain cycle — the corrected
   response lands verbatim, conform writes the silver row, the anti-join
   empties for `P`, and the old quarantine row stays resolved (retained) as
   the audit record. Conform's replay over the already-landed corrected row
   makes zero provider calls.

## Alternatives considered

- **A harvest `@asset_sensor` on bronze landing materializations.** Rejected —
  ADR-0012 decision 2 already showed an asset sensor fires only on NEW
  materialization events, so items non-terminal at the sensor's instant are
  never re-checked. The trigger must be terminal state, which only the
  harvest run observes directly.
- **A separate `@sensor`/job deriving retries from the anti-join.** Rejected —
  it re-derives failure judgment the harvest run already made, on a delay,
  and its schedule is a hidden dependency. Also it drifts: two actors
  deciding "what failed" is the two-ledger shape ADR-0012 retired.
- **Keep the sha256 digest key and store `(round, pid)` in a side table.**
  Rejected — that table is the ledger ADR-0013 prohibits, and a key that
  cannot be read back is the one-way-digest defect this ADR exists to fix.
- **Quarantine the 8 `model IS NULL` rows.** Rejected — all 8 are verified
  valid classification JSON; quarantine would misrepresent valid data as
  malformed and lose it from serving (W6's acceptance explicitly forbids
  "silently skipped").
- **Backfill a plausible model name for the 8 rows.** Rejected — fabricated
  provenance is worse than an honest sentinel; provenance-on-derived rules
  forbid invented origin.

## Consequences

Positive: every ADR-0012 dynamic now has a named production actor; in-flight
actually shrinks (the stall defect is specified dead); the round survives in
the partition key so both producer and consumer derive it with no shared
table; `scheduled_for`, `MAX_ATTEMPTS`, and `dead_letter` each have a derived
replacement; the 8 legacy rows keep serving with honest provenance.

Negative / work this commits us to:

- **The harvest run grows a second responsibility** (report harvested
  partitions + mint retries). W4 implements it; a harvest run that crashes
  between landing bronze and reporting the event leaves the partition
  in-flight — safe (drain suppresses; a re-run re-derives), but the window
  must be tested.
- **Partition-key shape is a contract change**: keys minted before adoption
  are opaque digests grandfathered as round 0 (D1 migration row). Anything
  comparing keys across the boundary must parse defensively.
- **`MAX_ROUNDS` is a policy constant, not a column**: changing the retry
  budget is a code/config change, never data.
- **Quarantine triage is a human obligation**, not an automated one. If
  nobody triages weekly, quarantine grows silently behind the advisory
  check — the blocking anti-join is the only hard stop.
- **The 8-row sentinel leaks into serving** as model `unrecorded-legacy-null`; any
  consumer that filters or groups on `model` must tolerate it. This is
  deliberate: it makes the legacy gap visible rather than laundered.

## Supersedes / Superseded by

- **Completes (does not supersede) ADR-0012's dynamics**: ADR-0012's
  decisions 1, 2, 5, 6, 7 all stand; this ADR names their actors, triggers,
  and transitions, and fixes the one shape defect (the one-way digest key)
  that decision 5's "retry is a NEW partition key" implied but never
  specified.
- **Holds ADR-0013**: no mechanism here creates a ledger table; every state
  transition is a Dagster instance event or a derived query.
- **Amends W6's acceptance** in the remediation plan (the 8-row disposition
  is migrate-with-sentinel, with a count assertion, not a re-hash/quarantine
  fork).
- Implementation units: W4 (D1–D3), W6 (D5), W8 (D3's check + D4's triage
  surface).
