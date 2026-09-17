# US-DISC-4 — Keep label maturity independent of tier/refresh state

- **Epic:** E-DISCOVERY
- **Persona:** P7 (Owner / Principal), P5 (Reviewer / Auditor)
- **Status:** Open
- **Depends on:** E-ENRICH-LABELS

## Story

**As** the owner,
**I want** the day-7 label pass to behave identically whether or not I know a
re-scrape of a young post will happen,
**So that** label correctness is a function of observed data alone, never of a
human-editable roster table.

## Context — as-is: the coupling is LIVE, not hypothetical

Owner ruling (2026-09-17): *"I don't think we should couple the labels 7-day
maturity to anything in `ops.sqlite`."* This ruling **retires** the
maturity-clock fork previously put to the owner. If maturity cannot consult the
roster, then `is_core` cannot gate `pending`, so maturity becomes a pure function
of post age and available observations.

**The coupling exists today** (verified in `ig_core/slv/labels.py`):

- `ig_post_labels` takes `ops: SQLiteResource` (`:363`).
- It imports `enabled_profiles` at call time (`:372`) and builds `core_handles`
  from it, filtered by **`tier == "tier1"`** (`:375-379`).
- `is_core` (`:163`) gates `pending` vs `day7_matched` (`:211-214`).

So this story is **undoing a live dependency**, not adding a guard. The
`tier == "tier1"` filter is also dead code in practice — all 675 profiles are
`tier1`, so it selects the entire roster.

**Why the coupling is harmful (three concrete failures):**

1. The same post with identical observations can receive a **different label**
   depending on who is tier 1 this week.
2. Promoting a creator **retroactively changes historical labels**.
3. The pass stops being reproducible from the lake — breaking the property that
   identical inputs yield identical output.

**Consequence a decision must cover:** the immutability guard
(`labels.py:246-253`) skips rows already `day7_matched` with a matching version,
so the **307 live `day7_matched` rows are RETAINED unchanged and never
recomputed** — they do **not** flip to `pending` (frozen, and possibly resting on
a superseded baseline). What changes is *future* judgments: a post judged after
the decoupling, from a creator no longer in `core_handles`, lands `pending`
instead of maturing to a day-7 verdict.

So the open decision is **not** "do we let the 307 flip to pending" — they don't
flip. It is: **do we force-recompute the 307, and under which gate?** (AC 5.)

**Note on the guard that would NOT catch this:** `enabled_profiles` reads
**DuckDB `silver_ig_roster`**, not SQLite — and per ADR-0017 that roster is
*ops-authored* (the dashboard writes ops, ops publishes to bronze, bronze
becomes silver). So an import-graph assertion for a SQLite accessor would
**pass while the coupling persists**. The only AC that proves the property is
behavioral: differing ops state must not change labels.

## Acceptance criteria (binary)

1. **GIVEN** two runs of the label pass over identical observation state,
   **WHEN** `ops.sqlite` differs between the runs (different tiers, different
   `enabled` flags),
   **THEN** every label row is identical. ← *the AC that proves the property*
2. **GIVEN** an `ops.sqlite` that is absent or unreadable,
   **WHEN** the label pass runs,
   **THEN** it completes successfully (no dependency on ops being available).
3. **GIVEN** a post first observed below the maturity threshold and never
   re-observed,
   **WHEN** it is judged,
   **THEN** it carries a provisional method — never a maturity-matched verdict
   from immature data.
4. **GIVEN** the label pass module's source,
   **WHEN** it is inspected,
   **THEN** no `enabled_profiles` / `core_handles` / `tier` roster read remains
   (a source-level assertion, second to AC 1's behavioral proof — **not** the
   primary guard).
5. **GIVEN** the 307 existing `day7_matched` rows,
   **WHEN** the decoupling lands,
   **THEN** their post-change state is the one the recorded decision specifies
   (recomputed, retained, or reclassified — decided explicitly, verified after).

## Definition of done

- [ ] AC 1 test: same observations, differing ops state → identical labels.
- [ ] AC 2 test: pass runs with ops unavailable.
- [ ] AC 3 covered, and it requires settling the maturity rule below.
- [ ] AC 4 source-level assertion added as a *secondary* guard.
- [ ] AC 5: the fate of the 307 rows is decided, recorded, and verified against
      the live store after the change.
- [ ] The maturity rule is recorded in the label module docstring (see open
      question).
- [ ] Reasoning trace + assumption log.

## The maturity rule to settle (owner ruling constrains it)

With roster state removed, maturity is a function of **post age and available
observations** only:

- A post whose best observation is at age ≥ 7d → `day7_matched` (maturity-matched).
- A post whose only observation is at age < 7d → provisional (`day0_heuristic`),
  **never** `day7_matched` — the observations plan's *"never stamp a day-7 label
  from day-0 data"*.
- A post with no usable observation → `unjudgeable` / `insufficient_baseline`.

The remaining choice is which **clock** measures age: the labelling clock
(`now − timestamp`, today's behaviour) or the observation clock
(`observed_at − timestamp`). The owner's ruling favours the **observation
clock**, since it makes maturity a property of the data rather than of when the
pass happened to run — and it closes the defect where a post first observed at
day 3 is stamped `day7_matched` at day 40. The consequence — posts never
re-observed stay provisional permanently — is the intended behaviour under
"labels must not know whether re-scraping will happen."
