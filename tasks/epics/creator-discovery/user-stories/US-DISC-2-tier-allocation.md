# US-DISC-2 — Allocate the core set via script; dashboard reads the suggestion

- **Epic:** E-DISCOVERY
- **Persona:** P7 (Owner / Principal), P1 (Pipeline Operator)
- **Status:** Open
- **Depends on:** US-DISC-1

## Story

**As** the owner,
**I want** to review the warehouse's `suggested_core` signal and allocate
creators to the refresh set from the command line,
**So that** I can act on the signal now without waiting for dashboard work — and
keep the scripts as DB-admin utilities afterwards.

## Owner decision (2026-09-17)

- **The dashboard READS `suggested_core` from the warehouse and displays it.**
  The suggestion is **warehouse-only** and is never written into ops — so nothing
  crosses the boundary and no ADR-0017 exception exists or is needed.
- **Allocation is managed by scripts.** This is the **sanctioned** near-term
  mechanism, not a tolerated deviation — the owner's focus is the data platform,
  and the scripts are expected to persist as DB-admin utilities.
- **Long term:** the dashboard's creator list shows the allocation, and an
  **admin view** enables batch selection/enablement of core creators ("sync
  enabled" in the UI). Deferred; recorded so the script path does not become the
  permanent interface by accident.

## Context — the mechanism that shapes this design

**Allocation IS the depth write (`results_limit`).** Epic Q1 is settled: the
core axis is `results_limit`, and `tier` stays vestigial. Consequences:

- `-1` (`AD_HOC_LIMIT`) already means "not scheduled" — the sentinel filter in
  `enabled_profiles` is the existing mechanism, so no new flag is needed.
- Promotion must **set depth explicitly**: `results_limit` defaults to
  `DEFAULT_DEPTH = 1` (`opsdb/roster.py:42`, schema default `"1"`). A creator
  promoted but left at `-1` is in the suggestion yet **never scheduled**; left at
  the default it scrapes **one post**.

**Why the suggestion must not be a `profiles` column (verified):** `add_profile`
(`opsdb/roster.py:218-221`) is

```sql
INSERT OR REPLACE INTO profiles
  (platform, handle, profile_url, results_type, results_limit,
   enabled, tier, creator_id, updated_at)
```

— an **explicit column list**. SQLite's `REPLACE` is delete-then-insert, so any
column outside that list **resets to its schema default** on every dashboard
add/edit. A `suggested_core` column would be silently **wiped by ordinary owner
activity**, and its "override preservation" guarantee could not hold.

**Use the single-column UPDATE pattern, not `add_profile`.** `edit_depth`
(`opsdb/roster.py:278-301`) is the precedent:

```sql
UPDATE profiles SET results_limit = ?, updated_at = ?
WHERE platform = ? AND handle = ?
```

A single-column `UPDATE` never resets siblings, unlike `INSERT OR REPLACE`. A
`set_profile_depth` allocation path should mirror `edit_depth`, and any future
dashboard admin action should call the same function — so the write semantics
stay defined in one place.

## Acceptance criteria (binary)

1. **GIVEN** the warehouse `suggested_core` signal and the current ops
   allocation,
   **WHEN** the review command runs,
   **THEN** it reports, per creator: *suggested and allocated*, *suggested only*,
   or *allocated only* — plus the signal values that drove the suggestion.
2. **GIVEN** the review command in read-only mode (the default),
   **WHEN** it completes,
   **THEN** no database row has changed (asserted, not assumed).
3. **GIVEN** an owner allocation,
   **WHEN** the review command runs again,
   **THEN** the owner's allocation is reported as authoritative and is not
   overwritten by the suggestion.
4. **GIVEN** a promotion is applied for specific handles at a specified depth,
   **WHEN** it completes,
   **THEN** each profile's `results_limit` is set to that depth via a
   single-column `UPDATE`-style call, and **no other column is reset**.
5. **GIVEN** a promotion at depth `d`,
   **WHEN** the profile is re-read,
   **THEN** `results_limit = d` (not `-1`, not the default `1`) and the profile
   is now returned by `enabled_profiles`.
6. **GIVEN** a demotion,
   **WHEN** it completes,
   **THEN** `results_limit` becomes `-1` and the profile is no longer returned by
   `enabled_profiles`.
7. **GIVEN** the allocation command is run twice with the same input,
   **WHEN** the second run completes,
   **THEN** the resulting state is identical (idempotent).
8. **GIVEN** a creator in the warehouse with no ops profile or no matching
   handle,
   **WHEN** the command runs,
   **THEN** the unmatched creator is reported rather than silently skipped.
9. **GIVEN** any allocation change,
   **WHEN** it completes,
   **THEN** the refresh set size (enabled, non-sentinel profiles) and its
   `Σ results_limit` are reported before and after in the same run.

**NOTE on AC 6 — demotion is LOSSY and indistinguishability is by design.** `-1`
is `AD_HOC_LIMIT`, the same value carried by the 624 profiles that were ingested
once from disk and never scheduled. So after a demotion there is **no way to tell
"was core, deliberately demoted" from "never scheduled"**, and `profiles` has no
history column (unlike `creator_merges`, which has a ledger with `--undo`). The
prior depth is **unrecoverable**; re-promotion must supply a new depth.

Owner accepts this tradeoff (2026-09-17) — the refresh set is a current statement
of intent, not an audited history. Two mitigations are in scope and cheap:

- the command **prints the prior depth for each demoted handle** before writing,
  so the value is captured in the operator's terminal/log at the moment of the
  change;
- `updated_at` is stamped (as `edit_depth` already does), so *when* a profile
  changed is recoverable, even though *what it was* is not.

If an audited tier history is ever needed (for the future RBAC/admin view), that
is a **new ledger table**, mirroring `creator_merges` — explicitly out of scope.

## Definition of done

- [ ] Review command exists (read-only default); writes require an explicit flag.
- [ ] AC 2 proven by a test asserting zero writes in read-only mode.
- [ ] AC 4 proven: a test asserts no unrelated `profiles` column is reset by an
      allocation change (the `INSERT OR REPLACE` hazard).
- [ ] AC 5/6 proven against `enabled_profiles` (the actual scheduling gate), not
      just the stored value.
- [ ] Demotion prints the prior `results_limit` for each affected handle before
      writing, so the lossy value is captured at the moment of change (AC 6 note).
- [ ] AC 3 (override preservation) is the load-bearing AC.
- [ ] AC 9: before/after refresh-set size and depth-sum reported in one run.
- [ ] Allocation goes through an `opsdb.roster` function mirroring `edit_depth`,
      so upsert/UPDATE semantics live in one place.
- [ ] Unmatched creators reported, not swallowed.
- [ ] `enabled_profiles` sentinel filtering unchanged; guard asserted.
- [ ] Epic records that near-term script management is a **deliberate deferral**
      of the admin view, not the end state.
- [ ] Reasoning trace + assumption log.

## Open questions

- Does the allocation write `tier` as well as `results_limit`? `tier` is
  vestigial (it gates nothing) and uniformly `tier1` today, so writing it would
  create a second, unenforced source of truth for "is core". Recommendation:
  **don't** — allocation is the depth write alone. Revisit if the dashboard ever
  needs to distinguish *why* a profile has depth.
- Promotion depth: is there a single default (e.g. 30, matching the current
  refreshable set) or per-creator choice? Cost scales with `Σ depth`, so this is
  the second cost lever after set membership.
