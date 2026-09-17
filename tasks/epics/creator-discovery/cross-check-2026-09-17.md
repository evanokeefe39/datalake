# E-DISCOVERY — cross-check: intent vs design docs vs implementation

Recorded 2026-09-17, following the owner's intent statement. Purpose: establish
where intent, documentation, and running code agree, where they diverge, and
which gaps are real. Every claim below was verified against the repo or the live
data, not inferred.

## 0. Findings that change the framing (verified after the first pass)

Findings A–C below supersede earlier phrasing in this document where they
conflict.

**A. The label/ops coupling is LIVE — the owner's ruling undoes existing code.**
`ig_core/slv/labels.py`: `ig_post_labels` takes `ops: SQLiteResource` (`:363`),
imports `enabled_profiles` at call time (`:372`), builds `core_handles` filtered
by **`tier == "tier1"`** (`:375-379`), and `is_core` (`:163`) gates `pending` vs
`day7_matched` (`:211-214`).

So the owner's ruling — *"don't couple the labels 7-day maturity to anything in
`ops.sqlite`"* — **retires a live dependency**, and it **resolves the
maturity-clock fork** that was previously put to the owner: if maturity cannot
consult the roster, `is_core` cannot gate `pending`, so maturity becomes a pure
function of post age + available observations. The `tier == "tier1"` filter is
also dead code — all 675 profiles are `tier1`, so it selects the whole roster.

**Consequence needing a decision:** removing it **changes existing labels, but
not by flipping them.** The immutability guard (`labels.py:246-253`) skips rows
already `day7_matched` with a matching version — so the **307 existing
`day7_matched` rows are RETAINED unchanged and never recomputed** (frozen, and
possibly resting on a superseded baseline). What changes is *future* judgments:
a post judged after the decoupling, from a creator no longer in `core_handles`,
lands `pending` (or `day0_heuristic`) instead of maturing to a day-7 verdict.

So the open decision is **not** "do we let the 307 flip to pending" — they don't
flip. It is: **do we force-recompute the 307, and under which gate?** (US-DISC-4
AC 5.)

**B. The dashboard is already the owner's tier-editing surface, and ops is the
authoritative store.** `server.py:1195` calls
`add_profile(_ops_resource(), ..., tier=payload.tier)`, and ADR-0017 publishes
that ops state → `GET /api/roster` → bronze `ig_roster_raw` → silver
`ig_roster` → `enabled_profiles`. `add_profile` upserts on `(platform, handle)`.

**Owner decision (2026-09-17):** the **dashboard READS the suggested tier from
the warehouse and displays it**; ops remains the allocated store. Near term the
owner manages allocation via **scripts** (focus is the data platform, not the
dashboard); those scripts may survive as DB-admin utilities. Long term the
creator list shows tier and an **admin view allows batch selection/enablement of
core creators** ("sync enabled" in the UI). So the suggested value is
**warehouse-only** — it is never written into ops, which removes the
second-writer risk entirely rather than managing it.

**Mechanism that makes a `profiles`-column suggestion UNSAFE (verified):**
`add_profile` (`opsdb/roster.py:218-221`) does
`INSERT OR REPLACE INTO profiles (platform, handle, profile_url, results_type,
results_limit, enabled, tier, creator_id, updated_at)` — an **explicit column
list**. SQLite's `REPLACE` is delete-then-insert, so **any column outside that
list is reset to its schema default on every dashboard add/edit**. A
`suggested_tier` column on `profiles` would therefore be silently **wiped by
ordinary owner activity**, and its "override preservation" AC could not hold.

Consequences, both mechanical rather than preferential:

- **The suggestion lives in the warehouse (a view), never on `profiles`.**
- **Allocation is the `results_limit` write** (settled — see below), so no new
  `profiles` column is required at all; adding one would be unsafe unless
  `add_profile`'s list and all its call sites (`server.py:764, 775, 845, 1195,
  1212`) were updated **in the same change**.

**C. The tier vocabulary was ambiguous — three axes, not two.** The epic
described tiers as follower *bands* and as core/refreshable *allocation*, while
the suggestion is derived from *performance*. Resolved:

| Axis | Meaning | Home | Status |
|---|---|---|---|
| `follower_tier` | Audience-size bucket — **4 canonical buckets** (`0-100`, `100-1k`, `1k-10k`, `10k+`) | `v_post_follower_context` (`views.py:502-503`); marts cite it as canonical (`marts.py:167-168, 260`) | Reuse as-is |
| `suggested_core` | **Performance signal only**: `standout_count >= 2 AND hot_count >= 1` | warehouse, from `v_creator_profile` | New (US-DISC-1) |
| core allocation | The purchase decision | ops `results_limit` | Settled axis |

**Do NOT introduce a `follower_band`** — `follower_tier` already exists and is
the single definition. A third name for audience size would collide three ways
(`follower_tier` = audience, `follower_band` = audience again, `results_limit` =
allocation), which is exactly the duplicate-definition failure the repo forbids.

**Settled (2026-09-17):**

- **Core axis = `results_limit`**, not `tier`. `-1` (`AD_HOC_LIMIT`) already
  means "not scheduled", the sentinel filter is the existing mechanism, and
  promotion is a single-column `edit_depth`-style write. `tier` stays vestigial.
- **Threshold = absolute counts** (`standout_count >= 2 AND hot_count >= 1`),
  chosen over a *rate* (owner decision 2026-09-17): with **535 of 661 creators
  holding fewer than 10 posts**, a rate's denominator carries no information —
  a single standout reads as 20%. Measured: 94 creators, ~$4.23/mo at depth 30.
  All absolute-count variants land in a narrow band (89–105 creators,
  $4.00–$4.72), so the threshold is not where the cost decision lives.
  Alternatives measured and rejected: `standout_count >= 1` → 105;
  `is_rising` → 6. All figures measured against **`v_creator_profile`** so they
  are comparable (earlier mixed-view figures were not).
- **Bands stay the canonical four.** Wider `<100k`/`<1M`/`1M+` bands exist in no
  view — new work, not an extension of a canonical definition.
- **Promotion sets depth explicitly.** `results_limit` defaults to
  `DEFAULT_DEPTH = 1`; a promoted profile left at the default scrapes **one
  post**, and one left at `-1` is never scheduled at all. Cost must sum actual
  `results_limit` per profile, never assume a fixed depth.

## 1. Intent vs design docs — three divergences

| Intent (2026-09-17) | Design docs say | Verdict |
|---|---|---|
| Tiering exists to **control cost**; the core set is owner-approved | Decision 3 of the observations plan: *"Monthly (default) refresh of a **tier-selected** core set, using existing `ops.profiles` columns (`tier`, `depth`, `enabled`)"* | **Divergence.** `tier` never selects the core set in practice — all 675 profiles are `tier1`. And the plan names `ops.profiles.depth`, which **does not exist** (the column is `results_limit`). |
| Cost is a **reported figure** surfaced on the dashboard | Plan has a cost model table, explicitly labelled *"Rate assumption, not fact"*; no reporting requirement | **Gap.** The cost model was analysis; no doc or code commits to *reporting* it. |
| Tiering is **suggested by data, allocated by human** (override required) | Not addressed anywhere; plan asserts "tier-selected" without saying who selects | **Gap.** The suggested/allocation split is new intent with no prior representation. |
| Discovery by **audience band across niches**, algorithmic long-term | Not present in any design doc | **New.** No prior art in the repo; recorded here so the manual path is designed to be replaced. |

**Cadence, correctly documented (and better than expected):** the plan's
conclusion 3 — *"Core-refresh cadence is a staleness decision, not a cost
decision. Monthly vs quarterly on a 20-creator core differs by ~$40/year"* — is
consistent with the measured reality, and the measured cost is even lower (§3).

## 2. Intent vs implementation — what actually runs

Verified against `data/ops.sqlite` (675 profiles) and the code:

| Fact | Evidence |
|---|---|
| **624 of 675 profiles carry `results_limit = -1`** (`AD_HOC_LIMIT`) — "ingested once from disk; never schedule" | `opsdb/roster.py:31-33` |
| **The refreshable set is 51 profiles** (enabled, real limit): 50 at depth 30, 1 `details` | live query |
| **All 675 profiles are `tier1`** — `tier` does not discriminate a core set | live query |
| **The sentinel filter is real and load-bearing**: `enabled_profiles` filters `results_limit != AD_HOC_LIMIT` and reads `silver_ig_roster`, not `ops.sqlite` | `ig_core/slv/roster.py:126-128` |
| **`tier` is carried but never used as a scrape selector** | grep over roster + dashboard: written/displayed, never selected on |
| **Label maturity is gated on the labelling clock** (`now − timestamp`), not the observation clock | `labels.py:213` |
| **94.3% of posts were first observed at ≥7d old**; only 15 posts ever reached ≥7d via a later scrape | live query over `silver_ig_post_observations` |
| **574 posts were first observed at <7d old**; 554 remained `day0_heuristic`, 20 `pending` | live query + `ig_post_labels` |
| **The ad-hoc corpus is never refreshed** — and must not be swept into a schedule | WATCHDOG + the sentinel filter above |

**Consequence for the tier discussion:** the axis that actually governs spend is
**`results_limit`**, not `tier`. Any tiering design must either use depth as the
axis or give `tier` real meaning — it currently has none.

## 3. The cost figure, corrected

An earlier pass of this analysis multiplied 675 profiles by ~150 posts and
reached ~$152/month. **That was wrong** — it treated `-1` as "unlimited" when it
is a never-schedule sentinel. The corrected figure:

- Refreshable posts-type profiles: **50**, each `results_limit = 30`
- Declared results per refresh cycle: **1,500**
- Monthly cost at the plan's assumed $1.50/1k: **~$2.25 — inside the Free tier's
  $5 cap**

So the cost-optimisation concern is currently **not urgent**, and cadence can be
chosen on freshness grounds without cost pressure. If the core set grows by an
order of magnitude the picture changes (500 profiles × 30 posts = 15,000
results ≈ $22.50/mo), which is why the *reported* cost figure (US-DISC-3) is the
right control — it makes growth visible before it matters.

**Caveat carried forward:** the rate is an uncalibrated assumption. The pipeline
records `RunInfo.estimated_cost_usd` per run, so replacing it with a measured
value is a query, and should be done before the figure is presented as more than
an estimate.

## 3b. Cadence cost model — initial backfill + weekly incremental (2026-09-17)

### The rate is MEASURED (2026-09-17) — $2.30/1,000 results

Queried from the Apify API for actor **`apify~instagram-scraper`**:

- **Billing model:** `PAY_PER_EVENT`, event `"result"` — *"Each result written
  to the dataset"*. Per-result billing at list rate on the 20- and 30-item runs
  (exactly $0.0023/result). **Scope note:** the 80-item run billed
  $0.2440/80 = **$0.00305** and the 35-item run $0.1090/35 = **$0.00311**, both
  ~33% above list, and the cycle separately bills `ACTOR_COMPUTE_UNITS`
  ($0.4958). So compute exists; these three run shapes are not enough to say
  whether it is folded into those totals. **Plan at the list rate and treat any
  excess as compute** — do not blend the two terms into one constant.
- **Rate is plan-tier dependent** (the plan's flat "$1.50" was a GOLD-tier
  figure, never ours):

  | Plan tier | $/result | $/1,000 results |
  |---|---|---|
  | FREE | 0.0027 | $2.70 |
  | **BRONZE (ours)** | **0.0023** | **$2.30** |
  | SILVER | 0.0019 | $1.90 |
  | GOLD | 0.0015 | $1.50 |
  | PLATINUM | 0.0009 | $0.90 |
  | DIAMOND | 0.0005 | $0.50 |

- **Our plan:** `STARTER`, tier `BRONZE`, $39/mo base with $39 usage credits.
  (STARTER is a **paid** plan, so the FREE tier's $5 hard cap does **not** apply
  here — there is no hard-stop risk at this tier.)
- **Measured rate: exactly $0.0023/result = $2.30 per 1,000 results.** Verified
  across 22 comparable runs: every 30-item run cost **$0.0690**, every 80-item
  run **$0.2440**, every 20-item run **$0.0460** — exact linearity at the BRONZE
  list rate, to the cent. **This is the planning figure.**
- **Actual current spend** (usage cycle 2026-08-12 → 2026-09-11):
  `PAID_ACTORS_PER_EVENT` **$37.44**, `PROXY_RESIDENTIAL_TRANSFER` $1.97, other
  services $0.84 → **total $40.25**, against the $39 credit line. **The account
  is at its included-credit ceiling already** — a real constraint on growth.
- **Segment by call type before deriving any unit cost.** The `details` runs cost
  a flat $0.0023-ish each (single-profile) while post scrapes scale with items;
  a blended figure across both is not a unit cost. (An earlier "$1.105/1k" was
  derived from ONE run's `usageTotalUsd`, which does not cover all billed
  components — **retracted**; see below.)

### The incremental mechanism — `onlyPostsNewerThan` EXISTS and is NOT USED

**Corrected 2026-09-17.** The actor's input schema
(`GET /v2/acts/apify~instagram-scraper/builds/default` → `actorDefinition.input`)
contains **`onlyPostsNewerThan`**:

> *"Filter by date. Limit how far back to scrape. Enter a date in `YYYY-MM-DD`,
> ISO format, or as a relative value, e.g. `1 day`, `2 months`, `3 years`."*
> `dateType: absoluteOrRelative` — the pattern accepts `7 days`.

**Our client does not send it.** `integration/apify_client.py:111-115` sends only
`directUrls`, `resultsType`, `resultsLimit`, and `proxy`. So the pipeline
currently fetches the **newest N posts by count**, not by date.

This is the crux of the incremental design, and the two options differ:

| | `resultsLimit: N` (runs today) | `onlyPostsNewerThan: "7 days"` (available) |
|---|---|---|
| Low-frequency creator (1 post/wk) | returns **7**, 6 already seen → **billed again** | returns **1** |
| High-frequency creator (>N/wk) | older posts roll off **silently** | returns all in the window |
| Cost per run | fixed at N | proportional to actual new posts |
| Blind spot | prolific posters' middle posts | none (date-complete) |

**Recommendation for the weekly incremental: use `onlyPostsNewerThan`.** It makes
the refresh genuinely incremental — a creator posting once a week costs **1**
result, not 7 — and it removes the prolific-poster blind spot that a count
filter cannot. The "last 7 days" intent should be expressed as the date filter,
with `resultsLimit` retained as an upper safety bound.

### Cost on the owner's cadence

**Cadence:** scrape a creator fully once (last 30 posts), then fetch **only the
last 7 days every 7 days** via `onlyPostsNewerThan`. Assumed actual new posts:
1–7 per creator per week (×4.33 weeks/month).

| Profiles | One-off (30 each) | Weekly refresh, monthly |
|---|---|---|
| 50 | $3.45 | $0.50 – $3.48 |
| **100** | **$6.90** | **$1.00 – $6.97** |
| 150 | $10.35 | $1.49 – $10.45 |
| 300 | $20.70 | $2.99 – $20.90 |
| 675 (all) | $46.58 | $6.72 – $47.03 |
| 1,000 | $69.00 | $9.96 – $69.67 |

**Owner's scenario (100 profiles fully, then weekly 7-day incrementals):**
one-off **$6.90**; steady-state **$1.00–$6.97/month**; first month **$7.90–$13.87**.

**The $150/month trigger needs ~2,150–3,200 profiles posting daily** (at the LOW
end if the date filter delivers only genuinely-new posts; at the HIGH end if the
window returns 7 results per creator regardless). So on this cadence a
`suggested_core` filter is **not needed to control spend** — only to control
*attention*. That materially lowers the stakes of the threshold rule (US-DISC-1).

### Structural properties worth keeping

1. **Depth, not cadence, is the cost driver.** The initial 30-post backfill
   (30 × $0.0023 = $0.069/creator) dominates because billing is per result.
2. **The date filter is load-bearing for cost.** Re-scraping the full last-30
   every week would be 30 × 100 × 4.33 ≈ 13,000 results ≈ **$29.90/month** —
   4–30× the incremental cost.
3. **Always set `maxTotalChargeUsd`** (the client already supports
   `max_charge_usd`). With the account at its credit ceiling, a per-run cap makes
   a runaway run terminate gracefully instead of stalling mid-cycle.

### One residual caveat

**Confirm the date filter's per-result behaviour once implemented.** With
`onlyPostsNewerThan`, cost should scale with *genuinely new* posts. If the actor
instead returns the window's full contents each run (re-billing seen posts), cost
tracks the upper bound. The range above covers both, but the mechanism should be
verified on the first real incremental run — measure `itemCount` against the
number of posts actually new since the prior run.

### The date filter's SILENT DATA-LOSS risk (must be an explicit precondition)

**This caveat is specific to `onlyPostsNewerThan`** — it did not apply under the
count filter, and it applies *harder* now that the date filter is the
recommendation.

With `onlyPostsNewerThan: "7 days"`, a creator who posts **less often than
weekly** has **no post inside any 7-day window**. They are **never re-observed**,
so their metrics silently **freeze while every run reports success** — precisely
the silent-failure class this repo guards against. The pipeline would report a
healthy refresh while the creator's data quietly rots.

Consequences and mitigation:

- The owner's assumption **"at least 1 post per week"** is what makes a 7-day
  window safe, so it must be an **explicit precondition**, not an implicit one.
  If it cannot be guaranteed for every profile, the window must be widened.
- **Mitigation: overlap the window** — use `onlyPostsNewerThan: "10 days"` on a
  7-day cadence. That costs ~43% more results but removes the gap for anyone
  posting at least every 10 days.
- **The window is not perfectly bounded either way:** the actor's own docs state
  *"Pinned posts may still appear even with this filter set"*, so a pinned old
  post can appear regardless of the cutoff. Harmless (the pipeline dedupes) but
  it means cost is not exactly `new posts × rate`.
- **The count filter has the mirror-image flaw** (a prolific poster's older
  posts roll off unobserved), so neither is complete. Date + overlap is the
  better combination; note the limitation rather than claiming it is solved.

US-DISC-5 carries this as an explicit precondition and an open question on
window length.

### On the signal distribution

The measured "94 of 105 signal-bearing creators pass `standout_count >= 2 AND
hot_count >= 1`" is **not** evidence that the rule fails to discriminate. The
roster was **manually curated** (owner, 2026-09-17), so it is skewed toward
quality by construction — a curated list *should* pass a quality filter.
Recorded so a later reader does not misread the distribution as a defect.

## 4. Constraints that must survive any tiering work

1. **`enabled_profiles`' sentinel filter.** Without it, a refresh fan-out
   resubmits the entire 624-profile ad-hoc corpus to Apify. This is the
   WATCHDOG hazard and the filter is the guard.
2. **Label pass independence from `ops.sqlite`** (owner-stated, US-DISC-4).
   Maturity must be a function of observations alone; a roster table must never
   change a label.
3. **No aggregation in `dashboard/server.py`** — the cost figure is a projector
   over a serving value (`test_no_aggregation_in_server.py`).
4. **Schedules ship stopped.** Any new refresh schedule follows ADR-0018 and the
   owner enables it deliberately.

## 5. Tooling gap — the hand-rolled Apify client (US-DISC-6)

`defs/integration/apify_client.py` is a hand-rolled client (~150 lines) while the
official `apify-client` (v3.2.0) is **not a dependency**. Verified comparison:

| Capability | Hand-rolled | Official SDK |
|---|---|---|
| Trigger / poll | ✅ | ✅ `actor.call()` |
| Retries | ✅ tenacity | ✅ built in |
| `maxTotalChargeUsd` | ✅ (as a **query** param) | ✅ `call(max_total_charge_usd=Decimal)` |
| Dataset fetch | ❌ single blocking GET, **fully buffered in memory**, no pagination | ✅ `iterate_items()` / `stream_items()` |
| **Actor input schema** | ❌ **none** | ✅ `actor.get()` |
| **Input validation before spending** | ❌ | ✅ `actor.validate_input()` |
| Maintained by | us | Apify |

**Consequence: it caused a real defect.** The absent schema access is exactly why
`onlyPostsNewerThan` went unnoticed — the actor supports a date filter, our
wrapper could not reveal it, and it was found only by querying the API directly.
A client exposing the input schema makes the next such gap discoverable.

**Naming:** the module shadows the official `apify_client` package, so importing
the real library inside it would read confusingly — rename or delete.

**Do not lose:** `stream_dataset` deliberately uses `format=json` to *"avoid
Apify's NDJSON newline bug"*. That workaround must be re-verified against the
current SDK or explicitly retained, with evidence either way.

## 5b. Doc gaps this cross-check identifies

- **No written scheduling/cadence policy.**
  `docs/architecture/pipelines/core.md` describes the label pass and the drain;
  it documents no scrape cadence, no tier semantics, and no cost model. The only
  cadence material in
  `docs/architecture/` is `bronze-schema.md` (the `-1` sentinel). Per AGENTS.md
  the home for this is `docs/architecture/pipelines/`, not `tasks/plans/`.
- **The plan's "`ops.profiles.depth`" is wrong** — the column is
  `results_limit`. Any doc written from the plan must correct this.
- **`tier` has no defined semantics** despite existing in the schema, the
  dashboard, and decision 3.
- **Open item 2 of the observations plan remains open:** *"validation that day 7
  is the right maturity constant (empirical check on refreshed core data)."*
  Record 7 as locked-but-unvalidated.
- **The label/ops coupling is undocumented as a dependency.** `labels.py` reads
  the roster (via `enabled_profiles`, filtered `tier == "tier1"`) to decide
  `pending` vs `day7_matched`, and this appears in no design doc. It is the
  live dependency US-DISC-4 removes.
- **No doc states that the dashboard is the tier-editing surface.** The write
  path (dashboard → ops → roster bronze → silver → `enabled_profiles`) is
  implied by ADR-0017 but not stated as the tier workflow, which is why a
  warehouse→ops seed script would have looked reasonable.

## 6. Follow-on work this cross-check implies

| Item | Home | Status |
|---|---|---|
| Scheduling/cadence + tier-semantics policy doc | `docs/architecture/pipelines/core.md` | Not started — doc gap |
| Suggested-tier derivation (US-DISC-1) | E-DISCOVERY / E-SERVING-ANALYTICS | Open |
| Suggested→allocated seed script (US-DISC-2) | E-DISCOVERY | Open |
| Reported cost figure (US-DISC-3) | E-DISCOVERY / E-DASHBOARD | Open |
| Label/ops independence guard + maturity-clock decision (US-DISC-4) | E-DISCOVERY / E-ENRICH-LABELS | Open |
| Apify rate calibration | E-DISCOVERY | **Done** — measured $0.0023/result (BRONZE list) |
| Incremental refresh via `onlyPostsNewerThan` (US-DISC-5) | E-DISCOVERY | Open — capability exists, client does not send it |
| Replace hand-rolled client with `apify-client` (US-DISC-6) | E-DISCOVERY | Open |
