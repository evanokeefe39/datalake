# Epic E-DISCOVERY — Cost-governed creator discovery & tiering

- **Theme:** Collection strategy & cost governance
- **Owner:** dlc-worker (warehouse/serving) / sdlc-worker (dashboard)
- **Status:** Active — governance complete; **US-DISC-7 and US-DISC-8 implemented** (PR #89, 2026-09-17). Remaining: US-DISC-1..4 (US-DISC-5 was absorbed by US-DISC-7, US-DISC-6 superseded by US-DISC-8).
- **Depends on:** E-IDENTITY (creators/profiles), E-ENRICH-LABELS (standout/hot signal), E-SERVING-ANALYTICS (metric views)
- **Feeds:** E-DASHBOARD (admin/cost surface)

## Outcome

A deliberate, cost-governed **collection strategy**: we do not analyze every
creator on Instagram. We want creators discovered at defined audience levels
(a wider banding — `<100` through `1M+` — is the aspiration) across niches we
also discover, and we pay to **continuously refresh only an approved core set**.
Everything else is ingested once and left alone.

**Scope note on bands:** the *canonical, implemented* audience bucket is
`follower_tier` with **four** buckets (`0-100`, `100-1k`, `1k-10k`, `10k+`,
`views.py:502-503`). The wider `<100k`/`<1M`/`1M+` bands are the discovery
aspiration and **do not exist in any view** — that is new work needing its own
story, not an extension of the canonical definition (owner decision 2026-09-17:
stay with the canonical four for now).

The tier mechanism exists **to control cost**, not to model the creators. Its
job is to answer: *which creators do we spend Apify money on, at what depth,
and what does that cost per month?*

## Intent (owner-authored 2026-09-17)

1. **Discovery is a funnel by audience level across niches.** We want creators
   found at defined follower bands, spanning several niches, and we want the
   *niches themselves* discovered rather than enumerated up front.
2. **Today the core list is manually curated.** The owner populates creators
   via the dashboard (or `ops.sqlite` directly) and the warehouse ingests that
   list. This is a manual process and is expected to remain manual in the near
   term.
3. **The long-term target is algorithmic / programmatic / semi-automatic**
   discovery — explicitly deferred, not designed here.
4. **Tiering is suggested by data, allocated by human.** A creator becomes a
   candidate for core status based on their **standout and hot post counts**
   (already computed by the label pass and serving views). The owner approves
   who is actually in tier 1. So the computed signal and the allocation are
   **separate facts**: a *suggested* tier and an *allocated* tier.
5. **Cost is a first-class reported figure.** Monthly cost of the core list is
   estimated from the Apify rate × posts-per-creator × tier membership, and is
   surfaced on the dashboard overview. The dashboard is becoming an **admin
   surface** for the platform (not only an analytics view); a distinct admin
   view with RBAC is future work, not this epic.
6. **Override is required.** Automatic classification must never be the final
   word — the owner can promote/demote regardless of what the data suggests.

## Scope highlights

- **`suggested_core` (warehouse) vs core allocation (ops), in separate stores.**
  The suggestion is a **warehouse-only** fact: `standout_count >= 2 AND
  hot_count >= 1`, per US-DISC-1. The allocation is the depth write in **ops**
  (see "Promotion is TWO writes"). They must be independently storable so a
  re-run of the suggestion never silently changes what we spend.
- **Owner decision (2026-09-17): the dashboard READS the suggested tier from the
  warehouse and displays it; ops stays the allocated store.** Near term
  allocation is managed by **scripts** (the owner's focus is the data platform,
  not the dashboard); those scripts may persist as DB-admin utilities. Long term
  the creator list shows tier and an **admin view enables batch
  selection/enablement of core creators** ("sync enabled" in the UI).
- **A `suggested_core`-style column on `profiles` is UNSAFE (verified
  mechanism).**
  `add_profile` (`opsdb/roster.py:218-221`) is `INSERT OR REPLACE` with an
  explicit column list; SQLite `REPLACE` is delete-then-insert, so any column
  outside that list resets to its schema default on every dashboard edit. So the
  suggestion must **never** be stored on `profiles`, and any **new** allocation
  column requires updating `add_profile`'s list and its call sites
  (`server.py:764, 775, 845, 1195, 1212`) **in the same change**. Repurposing the
  existing `tier` column is safe — it is in the list.
- **Audience size reuses the existing canonical `follower_tier`**, defined once
  in `v_post_follower_context` (`views.py:502-503`) and cited as canonical by the
  Q1/Q2 marts (`marts.py:167-168, 260`). **No `follower_band`** — a third name
  for audience size would collide with `tier` (allocation) and `follower_tier`
  (audience).
- **Cost estimate as a derived, reported figure** — Apify publishes a $/1k
  results rate for the Instagram actor; with per-creator depth
  (`profiles.results_limit`) and tier membership, monthly cost is computable.
  This is an **estimate** — the rate is **measured and verified** (2026-09-17):
  actor `apify~instagram-scraper` bills the `result` event at **$0.0023/result =
  $2.30/1k on our BRONZE tier** (STARTER plan, $39/mo). **Exact linearity across
  run size is confirmed from the run records**, including a 912-result run billed
  $2.0976 = 912 × $0.0023 to the cent. Re-measure if the plan tier changes.

  **Do NOT blend in the separately-billed components.** Apify reports compute and
  proxy as *separate* line items, never folded into a per-result figure:
  `ACTOR_COMPUTE_UNITS` (@ $0.30/CU = memory_GB × wall_s / 3600),
  `PROXY_RESIDENTIAL_TRANSFER_GBYTES`, storage/transfer. Plan the scrape spend on
  the per-result term and treat these as a small additive overhead — at the last
  full cycle compute was $0.496 against $37.44 of per-event charges.

  **Trap when auditing rates:** an earlier draft derived "$0.0031/result, ~33%
  above list" from two runs that turned out to belong to a **different actor**
  (billed `apify-default-dataset-item` @ $0.003 plus a one-time $0.004
  `apify-actor-start`). Attribute a rate to an actor before deriving a unit cost
  from it. **Verified basis: runs of 1, 3 and 912 results all bill exactly
  n × $0.0023 (the 912-result run at $2.0976) — see US-DISC-7's rate section.**
  *(`cross-check-2026-09-17.md` §3b is retained as dated history: its 80-item
  `$0.2440` row is the different-actor trap above, and its §3 Free-tier framing
  is superseded by §3b.)*

  **Re-measure when the rate changes, not only when the tier changes.** The
  actor's `pricingInfos` history shows the rate moved with **no tier change**:
  $0.0023 (2024-08-14) → briefly $0.0007 (2024-12-12, superseded 86 minutes
  later) → back to $0.0023, and the *model* itself changed from
  `PRICE_PER_DATASET_ITEM` to `PAY_PER_EVENT` on 2025-12-12. A tier-change-only
  warning would not have caught any of that, so treat the per-result rate as a
  value to re-read from `pricingInfos` periodically rather than a constant.
- **Cadence is a staleness decision, not a cost decision** — and this is now
  measured, not merely asserted. **Owner's cadence (2026-09-17, superseded the
  same day):** the first design was "scrape once, then fetch only the last 7
  days every 7 days" using the actor's `onlyPostsNewerThan` date filter. That is
  **superseded by the per-creator watermark** (item 2 below): a flat window
  either re-fetches a prolific creator repeatedly or under-covers a slow one, and
  the measured corpus is 12 days dry, so a flat 10-day window currently returns
  nothing while reporting success. `onlyPostsNewerThan` remains the transport;
  the watermark is the selection rule. Measured: 100
  profiles = $6.90 one-off then **$1.00–$6.97/month**; all 675 = **$46.57**
  one-off (675 × 30 × $0.0023 = $46.575 — the cross-check's table rounds the same
  figure *up* to **$46.58** at its own two-decimal precision, so the two
  documents agree on sight: one number, two roundings) then
  **$6.72–$47.03/month**. The $150/month trigger needs **~2,150 profiles
  refreshing at the full 30-result depth each month** — so the filter controls
  *attention*, not spend. *(Corrected 2026-09-17: an earlier range ended at
  "3,200 profiles posting daily", which does not follow — 3,200 × 30 × $0.0023
  is $220.80, not $150, and the endpoints were inverted: genuinely-incremental
  refetching means FEWER results per creator, hence MORE profiles needed before
  the trigger, not fewer. The dated `cross-check §3b` retains the original
  wording.)*

  **Two populations, two figures — do not conflate them:** the cost-model
  figure above ranges over **all 675 profiles**. The sync runs over the
  **sentinel-filtered 51** ($3.52). `disabled = 0` and all 675 are `enabled`, so
  **`results_limit != AD_HOC_LIMIT` is the ONLY thing separating the 51
  refreshable profiles from the 624 never-schedule ad-hoc ones** — a change to
  that predicate would silently resubmit the entire ad-hoc corpus to Apify.
  That is a test, not just prose (US-DISC-7 AC 15).
- **The refreshable set is the sentinel-filtered set.** `results_limit = -1`
  (`AD_HOC_LIMIT`) means "ingested once from disk; never schedule." The
  refreshable population is `enabled AND results_limit != -1`.

## As-is: the coupling already exists (verified 2026-09-17)

This is **not** a constraint to preserve — it is a **live dependency that must be
undone**. Verified in `ig_core/slv/labels.py`:

- `ig_post_labels` takes `ops: SQLiteResource` (`:363`).
- It imports `enabled_profiles` at call time (`:372`) and builds `core_handles`
  from it, **filtered by `tier == "tier1"`** (`:375-379`).
- `is_core` (`:163`) gates `pending` vs `day7_matched` (`:211-214`).

So **label maturity today depends on roster state**, through a
`tier == "tier1"` filter — and since every profile is `tier1`, that filter is
currently dead code that matches the whole roster.

**Consequence of decoupling, which a decision must cover:** the immutability
guard (`labels.py:246-253`) skips rows already `day7_matched` with a matching
version, so the **307 live `day7_matched` rows are RETAINED unchanged and never
recomputed** (frozen, possibly resting on a superseded baseline) — they do not
flip to `pending`. What changes is *future* judgments: a post judged after the
decoupling, from a creator no longer in `core_handles`, lands `pending` instead
of maturing. The open decision is therefore **whether to force-recompute the
307, and under which gate** — not whether to let them flip.

## Explicit non-goals

- **Label maturity MUST NOT depend on `ops.sqlite`** (owner ruling 2026-09-17).
  Maturity is a property of the *observation* (`observed_at`), not of roster
  intent — see the as-is block above for what this undoes.
- **No automatic promotion/demotion side effects.** A suggestion is a
  suggestion; it must not change what gets scraped without approval.
- **Algorithmic discovery** (the long-term target) is out of scope for this
  epic's implementation; the epic records the intent so the manual path is
  designed to be replaced, not rebuilt.
- **RBAC / multi-user admin** is out of scope; the admin surface is single-owner
  for now.

## What we must not break

- `enabled_profiles` filters `results_limit != AD_HOC_LIMIT` and reads
  `silver_ig_roster` (not `ops.sqlite`). This is load-bearing: without the
  sentinel filter a refresh fan-out would resubmit the entire ad-hoc corpus to
  Apify. Any tiering change MUST preserve this guard.
- The ad-hoc corpus (624 of 675 profiles at `results_limit = -1`) is
  deliberately never refreshed. Tiering work must not sweep it into a schedule.

## Source of truth

- Intent: this epic (owner-authored 2026-09-17).
- Cost model + rate assumption: `tasks/plans/post-performance-observations-investigation.md`
  ("Apify cost model" — explicitly *"rate assumption, not fact"*).
- Observations/maturity design: same plan, decisions 1–5 (parked: peer-cohort
  baselines, EWMA, shrinkage, bitemporal metric tables).
- Roster ownership boundary: ADR-0017 (dashboard owns `creators`/`profiles`/
  `creator_merges`; the pipeline lands the roster as bronze `ig_roster_raw`).
- Implementation layout: ADR-0015.

## Status: governance complete; US-DISC-7/8 implemented

**Epic state (2026-09-17):** intent captured, cross-checked against design docs
and the live implementation, decisions recorded, and eight user stories written
(US-DISC-1..8)
with binary AC + DoD.

**Update 2026-09-17:** **US-DISC-7 (per-creator watermark sync) and US-DISC-8
(SDK migration) are implemented** on `feat/us-disc-7-ingestion-upgrade`
(PR #89) — the hand-rolled Apify client is replaced by the official
`apify-client` SDK, and `core_refresh` now syncs per creator. The acceptance run
landed 270 items across 7 successful runs. US-DISC-1..6 remain unbuilt, and
US-DISC-7 AC 13 (the run-memory measurement) is open.

**Authoring note for whoever edits this epic next — the five defect classes
this doc set was swept for eight times.** Each was introduced during authoring
and none was visible from a single-file read, so a re-read will not catch them.
Before publishing an edit to any file under `tasks/epics/creator-discovery/`:

1. **Stale counts.** The epic's stated story count was left at its old value a
   revision after the eighth story was written. If you add or remove a story,
   grep the epic for every count word and fix them together.
2. **Mis-cited issue numbers.** One cross-reference named an issue number that
   belonged to an unrelated entry (the DAGSTER_HOME issue) while the intended
   target was the date-filter issue. Open the issue — do not match on the
   number's apparent plausibility.
3. **Off-target `file:LINE` citations.** Line references drift as files change.
   Re-check the content at the line, never just that the file exists.
4. **Dangling `AC n` references after renumbering.** Renumbering an AC list
   silently invalidates every reference above the edit point.
5. **Scripted-edit artifacts.** A scripted edit glued a heading onto the end of
   the preceding sentence, which renders the heading as body text and silently
   swallows the following paragraph; others produced a backslash-dollar escape
   rendering literally and CRLF contamination. After any scripted edit, check
   line endings, that every heading starts its own line, and that no backslash
   precedes a dollar sign.

Also: `cross-check-2026-09-17.md` is **dated history and is never edited** —
corrections of record go in the living docs as dated errata, per AGENTS.md.
A test guard for these classes was written and deliberately **not** kept: the
doc set is stable, `tasks/` is working content (`.gitignore` excludes all but
`tasks/epics/`), and no other test in the repo reads it — coupling the
production suite to governance prose is a poor trade for a frozen surface. The
checks that remain mechanically valuable (AC contiguity, `AC n` range) are
listed above for a human to run on edit.

**Carried by the approved next branch (owner decision 2026-09-17; originally deferred, now scheduled) — US-DISC-8's item is DONE:**

- **US-DISC-6** (replace the hand-rolled Apify client with `apify-client`) —
  `ISSUES.md` #41. **Now scheduled, not deferred:** carried by **US-DISC-8** on
  the `feat/us-disc-7-ingestion-upgrade` branch; US-DISC-6 is retained only as
  the original statement of intent. The **module rename** (it shadows the package name)
  is worth doing whenever that branch opens, independent of the SDK swap.
- **`stream_dataset` memory buffering** — `ISSUES.md` #42. **Not a truncation
  bug** (verified: the item endpoint is uncapped; the 1,000-element cap belongs
  to the datasets *listing* endpoint). Residual: it buffers the whole response,
  so peak memory scales with dataset size. Fold into #41.

**NEXT BRANCH: US-DISC-7 + US-DISC-8 — ingestion upgrade (owner-approved
2026-09-17).** The branch absorbs **US-DISC-5** (date filter) and `ISSUES.md`
**#42** (memory), carried by **US-DISC-7** (the sync half), plus `ISSUES.md`
**#41** (SDK), carried by **US-DISC-8** (the migration half — the two are
accepted separately; see the split note below). It adds concurrency
investigation:

1. **Initial scrape by result limit** — predictable cost per creator added.
2. **Ongoing sync by a PER-CREATOR WATERMARK, not a flat window** (owner design,
   2026-09-17) — the boundary comes from each creator's own `MAX(timestamp)` in
   `silver_ig_posts`, read from the LAKE. A flat `"10 days"` either re-fetches a
   prolific creator's recent posts repeatedly or under-covers a slow one;
   grouping profiles into **date buckets** (7–30d, 30–180d, >180d) lets one run
   per bucket carry many profile URLs. **Measured live: zero creators are inside
   a 7-day window and 14 have never been scraped**, and the corpus is 12 days
   dry (newest post 2026-09-05). The first sync therefore needs a wide
   staleness pass rather than a 10-day incremental one. **Cost ≈ $3.52** over
   the refreshable set (51 profiles; `results_limit != -1` — the only real
   gate, since all 675 are `enabled`), or $46.57 if the owner deliberately
   elects to sync the 624 ad-hoc profiles too.
3. **Official `apify-client`** instead of the hand-rolled module — now its own
   story, **US-DISC-8**, on the same branch (independently acceptable; the
   migration changes transport, not behaviour).
4. **Concurrency** — 32 runs / 64 GB combined on STARTER; per-result cost is
   invariant, so the trade is latency vs compute. **The fan-out unit is N
   PROFILES PER RUN, not N slices of one profile:** `trigger_run` takes
   `urls: list[str]` and `resultsLimit` applies per URL, so one run covering
   several profiles is safe by construction. Slicing a *single* profile by count
   is not expressible (no offset input; repeated URLs return the same newest N
   and bill N times) — so the proposed "8 jobs × 125 posts of one profile" is
   unnecessary for the wall-clock win and is the double-billing shape. The
   fan-out carries its own **named cap** (`DEFAULT_MAX_PARALLEL_SCRAPE_RUNS`,
   mirroring `DEFAULT_MAX_PROFILES_PER_SWEEP`) so the bound is ours, not Apify's
   ceiling.

**Sequencing note:** US-DISC-5, the SDK migration (now US-DISC-8), and #42 all touch the same code path
(`apify_client.py` + `scrape.py` + `test_core_refresh.py`), which is why they
collapse into one branch rather than three.

## Open questions

1. ~~Which axis marks a profile as core?~~ **SETTLED (2026-09-17):
   `results_limit` is the core axis, and `tier` stays vestigial.**
   Rationale: `-1` (`AD_HOC_LIMIT`) already means "not scheduled", the
   `enabled_profiles` sentinel filter is the existing mechanism, and promotion
   becomes a single-column `edit_depth`-style write. Repurposing `tier` would be
   safe (it is in the upsert list) but adds a second source of truth for the same
   question.
2. ~~What is the promotion threshold?~~ **SETTLED (2026-09-17):
   `standout_count >= 2 AND hot_count >= 1`** — 94 creators; cost is the sum of
   their ACTUAL allocated depths (≈ 1,840 results, back-derived from the $4.23 figure → ~19.6 per creator), NOT 94 × 30
   (which would be $6.49) — consistent with US-DISC-3's rule to sum real
   `results_limit` per profile, never assume a constant depth. **Absolute counts, not a rate** (owner decision): with 535 of 661
   creators holding fewer than 10 posts, a rate's denominator carries no
   information. Tunable; see US-DISC-1.
3. **Which follower bands are the target discovery bands, and how are niches
   discovered/represented?** Canonical `follower_tier` currently has **four**
   buckets (`0-100`, `100-1k`, `1k-10k`, `10k+`); the epic's `<100k`/`<1M`/`1M+`
   intent implies **wider bands that do not exist in any view** — that is new
   work needing its own story, not an extension of the canonical definition.
   **Owner decision (2026-09-17): stay with the canonical four for now.**
4. **Calibrate the Apify $/1k rate** from `RunInfo.estimated_cost_usd` before the
   cost figure is presented as more than an estimate.

**Settled (2026-09-17):** the suggestion is warehouse-only and read by the
dashboard; allocation is managed by scripts near term (sanctioned, expected to
persist as DB-admin utilities) and an admin batch-select view long term;
`suggested_tier` must not be a `profiles` column (upsert wipes it); audience size
reuses `follower_tier` with its canonical four buckets.

## Promotion is TWO writes, not one (verified)

`results_limit` is `-1` (`AD_HOC_LIMIT`) on 624 profiles and defaults to
`DEFAULT_DEPTH = 1` (`opsdb/roster.py:42`; schema default `"1"`). So promoting a
creator must set depth explicitly:

- Left at `-1` → the creator is in the suggestion but **never scheduled**
  (`enabled_profiles` filters it out; cost $0).
- Left at the default `1` → the refresh scrapes **one post**, and the cost
  formula's "depth 30" is false.

So **promotion = set `results_limit`** (and `tier` only if the owner wants the
label). This is the concrete requirement US-DISC-2's allocation path carries,
and it is why the cost figure must read the allocated depth rather than assume
one.

## Epic DoD

- [ ] Intent captured and cross-checked against design docs and implementation
      (this epic + `cross-check-2026-09-17.md`).
- [ ] `suggested_core` derived in the serving layer from the accepted rule,
      reusing `v_creator_profile`; `follower_tier` exposed alongside (not
      conflated), canonical four buckets only.
- [ ] Owner allocation manageable via script (near term) — **promotion sets
      depth**, not just a tier label.
- [ ] Monthly core-list cost estimated from *allocated depth* and surfaced on the
      dashboard overview.
- [ ] Override path proven: a promoted/demoted creator changes the refresh set
      and the cost figure without re-running the suggestion.
- [ ] Label pass proven independent of ops tier state (US-DISC-4).
- [ ] Actor capabilities discoverable from code — the hand-rolled client
      replaced (or renamed) so `onlyPostsNewerThan`-style gaps are findable
      without querying the API by hand (US-DISC-8).
- [ ] (Long term) admin view: creator list with tier + batch enablement of core
      creators — out of scope until the data platform work lands.
