# US-DISC-7 — Ingestion sync: result-limit backfill, per-creator watermark, concurrency

- **Epic:** E-DISCOVERY
- **Persona:** P1 (Pipeline Operator), P2 (Platform Engineer)
- **Status:** **Implemented** (PR #89, 2026-09-17) — built with US-DISC-8 on
  `feat/us-disc-7-ingestion-upgrade`; acceptance run landed 270 items across 7
  successful runs. AC 13 (memory measurement) is open — `SCRAPE_RUN_MEMORY_MB`
  is provisional.
- **Depends on:** —
- **Absorbs:** US-DISC-5 (date filter)
- **Related (same branch):** US-DISC-8 — the SDK migration (ISSUES #41, #42)

## Story

**As** the pipeline operator,
**I want** the ingestion path to (a) backfill by result limit, (b) sync by
"posts newer than" via a per-creator watermark, and (c) exploit concurrency,
**So that** adding a creator has a predictable cost, and ongoing sync is
incremental per creator rather than re-billing or silently missing a slow one.

## Owner decisions (2026-09-17)

1. **Initial scrape: by result limit** — so the cost of adding an IG profile is
   easily determined (`resultsLimit × $0.0023`).
2. **Ongoing sync: by a PER-CREATOR WATERMARK** — each creator's own
   `MAX(timestamp)` from the lake, bucketed by staleness. *(Supersedes an earlier
   "flat 10-day window on a 7-day cadence": a flat window either re-fetches a
   prolific creator repeatedly or under-covers a slow one, and the measured
   12-day-dry corpus means it would currently return nothing while reporting
   success. `onlyPostsNewerThan` remains the transport mechanism; the watermark
   is the selection rule.)*
3. **Fan-out cap: 16 concurrent runs** — half the account's 32-run ceiling,
   leaving headroom for the details sweep.
4. **Concurrency is worth exploring** — the owner suspects 8 × 125 / 16 / 32 jobs
   could scrape 1,000 posts faster.
5. **The official `apify-client`** — carried by **US-DISC-8** on this same
   branch, not by this story.

**Scope note (split, 2026-09-17):** this story covers the **sync** half —
result-limit backfill, per-creator watermark, bucketing, and concurrency. The
**SDK migration** is its own story, **US-DISC-8**, on the same branch: it is
self-contained (four contracts, its own patch target and smoke test) and does not
depend on the watermark design, so it is accepted separately. Both ship on the
`feat/us-disc-7-ingestion-upgrade` branch.

## Backfill by result limit, then sync by per-creator watermark

- **Backfill** sends `resultsLimit: N` (today's behaviour) — make the depth
  explicit and cost-derivable in the review surface.
- **Sync** sends `onlyPostsNewerThan` **derived per creator from that profile's
  own watermark** (`MAX(timestamp)` in the lake), NOT a flat literal. The
  boundary is per-profile; profiles are grouped into staleness buckets because
  the parameter is top-level (AC 2, AC 9).
- Keep `resultsLimit` on the sync path as an **upper safety bound**.
- **Params must be transmitted verbatim** — the actor accepts relative values
  (`10 days`) and `YYYY-MM-DD`/ISO; do not convert client-side.

**Verified capability:** `onlyPostsNewerThan` exists in the actor's input schema
(`GET /v2/acts/apify~instagram-scraper/builds/default` → `actorDefinition.input`),
`dateType: absoluteOrRelative`, and is a **top-level** input. Our client does not
send it.

**SUPERSEDED — the flat-window hazard, retained as rationale.** An earlier draft
specified a flat `onlyPostsNewerThan: "10 days"` on a 7-day cadence. Its failure
mode is worth recording because it is what motivated the watermark: a creator
posting less often than the window has no post inside it, is never re-observed,
and their metrics stop advancing **while every run reports success** — and the
measured corpus is 12 days dry, so a flat 10-day window would currently return
nothing at all. **The per-creator watermark removes this**: there is no fixed
window to fall outside of, the boundary simply ages and the request widens (a
capacity question — see open questions 3 and 4). Note also the actor's docs:
*"Pinned posts may still appear even with this filter set"*, so any window is not
perfectly bounded.

## Concurrency (investigate, do not assume it helps)

**Plan limits (STARTER, from `docs.apify.com/account/limits`):**

| Limit | Value |
|---|---|
| Max concurrent actor runs | **32** |
| Max combined memory of all running jobs | **65,536 MB (64 GB)** |
| Max memory per single run | 32,768 MB (32 GB) |
| Memory per CPU core | 4,096 MB (4 GB) |

**Config arithmetic for 1,000 posts:**

| Config | Runs | Memory | Verdict |
|---|---|---|---|
| 8 × 125 @ 2 GB | 8 | 16 GB | ✅ comfortable |
| 16 × 62 @ 2 GB | 16 | 32 GB | ✅ |
| 32 × 31 @ 1 GB | 32 | 32 GB | ✅ at run-count cap |
| 16 @ 4 GB / 32 @ 2 GB | — | 64 GB | ⚠️ at memory ceiling |

**Three constraints, in priority order:**

1. **The 32-run cap binds before memory** for small actors (32 × 1 GB = 32 GB,
   half the memory ceiling).
2. **The actor's own default memory is 1,024 MB (1 GB)** — verified from its
   `defaultRunOptions` (`{"memoryMbytes": 1024, "timeoutSecs": 604800}`). So
   1 GB is the author's chosen default, **not** an under-resourced config; my
   earlier "32 × 1 GB may be too small" caution was speculation and is retracted.
   Memory-per-core (4 GB) still means a 1 GB run gets ~0.25 CPU, so whether more
   RAM helps is an empirical question for the Instagram actor — measure it.
3. **Per-result cost is INVARIANT to parallelism** — billing is per result
   ($0.0023), so 1,000 posts costs $2.30 whether in 1 run or 8. What changes is
   **wall-clock** (the real win), **compute cost** (`ACTOR_COMPUTE_UNITS`,
   billed separately — $0.4958 last cycle; each run carries fixed startup
   overhead, so 8 runs cost more compute than 1), and **blast radius** (a failed
   run loses 125 posts, not 1,000).

**Therefore: measure before designing.** Run one 125-post job and one 1,000-post
job; compare `usageTotalUsd` per result and wall-clock. The trade is latency vs
compute, and the account is already at its credit ceiling.

### The fan-out must carry its own NAMED CAP (repo convention)

ADR-0018 establishes bounded fan-out ops keep an **explicit, chosen-once,
reviewable cap** — the precedent is `DEFAULT_MAX_PROFILES_PER_SWEEP = 10`
(`platform/details_sweep.py:50`). A concurrency fan-out that *could* spawn 32
runs needs the same treatment:

- a **named constant** (e.g. `DEFAULT_MAX_PARALLEL_SCRAPE_RUNS`) chosen once and
  reviewable, **not** "whatever the plan allows";
- a test asserting the cap, in the same spirit as the asset-graph-integrity
  guards;
- the bound enforced **in our code**, not delegated to Apify's ceiling — the plan
  limit is a backstop, not the design.

Without this, a first tick fans out to the plan maximum with zero headroom for
other workloads (the 32-run cap is account-wide, shared with the details sweep).

### Decision: fan-out cap = 16 concurrent runs (owner, 2026-09-17)

`DEFAULT_MAX_PARALLEL_SCRAPE_RUNS = 16` — half the account's 32-run ceiling,
leaving headroom for the details sweep and any other actor run on the same
account. 16 x 1,024 MB (the actor's own default memory) = 16,384 MB of the
65,536 MB combined budget. NOTE: live runs currently request 4096 MB, so this
assumes the memory is set explicitly — see the memory finding below. Named, chosen once, reviewable, with a test asserting it
(AC 6) — the ADR-0018 bounded-fan-out convention.

### Dormancy, metric drift, and observations — MEASURED, not assumed

**Three corrections to earlier claims in this story, each now grounded in code
and data.**

**(1) A re-scrape DOES update engagement.** `posts.py:405-408` dedups with
`DISTINCT ON (post_id) ... ORDER BY post_id, scraped_at DESC NULLS LAST,
source_dataset DESC` — the **newest scrape wins** for likes/comments/views. Only
`processed_on` is carried forward (`:394-398`). So the pipeline is not
first-value-wins.

**(2) `scraped_at` is deliberately dropped** (`posts.py:412-414`, comment:
*"transient — it drives dedup ordering and observation provenance but must never
become a silver_ig_posts column"*). So bronze file **mtime** and the observations
table are the only records that a post was re-observed.

**(3) `silver_ig_post_observations` DOES record engagement drift — measured.**
Live query over posts with >1 observation:

| Measure | Value |
|---|---|
| Posts with multiple observations | 2,521 |
| **Posts showing real drift (`max(likes) > min(likes)`)** | **542** — the figure **+21,162 is the single LARGEST per-post gain**, not a sum |
| Posts with byte-identical re-captures | 1,979 |
| Posts whose observations span >7 days | 1,074 |

So observations *do* evidence growth — for **542 of 2,521** multi-obs posts. The
other 1,979 are re-captures of the same window (consistent with the colliding
local `dataset_id`s WATCHDOG flags). **Claim scope: "observations exist and are
appended per scrape, and 542 posts show measured drift" — NOT "the table tracks
drift broadly."**

**Keyed-column trap (from the schema):** PK is `(post_id, source_dataset)`
(`schemas.py:129`), written `INSERT OR IGNORE` (`posts.py:359`). A re-scrape
creates a new `dataset_id` so it appends; a **replay of the same dataset_id
appends nothing**. Verified: for all 2,521 multi-obs posts,
`COUNT(DISTINCT source_dataset) == COUNT(DISTINCT observed_at)` — datasets and
timestamps are 1:1, so counting distinct datasets is the safe freshness measure.
**Compare `max(observed_at)` against the sync watermark so a replay does not read
as fresh observation.**

**(4) The two consumers take OPPOSITE ends of the series — do not "unify" them.**

| Consumer | Pick | Why |
|---|---|---|
| `labels.py:82-90` | `observed_at DESC` → **newest** non-null | current engagement |
| `v_post_detail` (`views.py:478-484`) | `observed_at ASC` + own-dataset tie-break, filtered `observed_at >= p.timestamp` → **earliest at-or-after** | point-in-time attribution |

Both take a **single** row, so **no consumer reads multiplicity** — the
truncation cause is benign today. But a future refactor that "deduplicates these
two queries" would silently invert point-in-time attribution. **Stated
explicitly so it does not look safe.**

**Net:** a dormant creator is not mis-recorded; the last observation stands as
accurate for its timestamp. The real gap is that **a post only refreshes if a
later scrape re-returns it — and a fixed window is what stops re-returning it.**
That is exactly what the per-creator watermark design below addresses.

### Per-creator watermark sync — the owner's design (2026-09-17)

**Replace the flat `onlyPostsNewerThan: "10 days"` with a per-profile watermark
derived from that creator's own most-recent post.** Rationale: a flat window
either re-fetches a prolific creator's recent posts repeatedly (waste) or
under-covers a slow creator (a 3-week poster is caught only sometimes). A
per-profile boundary is exact for both.

**Watermark source:** the lake, not `ops.sqlite` — the same boundary the roster
already crossed (ADR-0017). Candidate expression:

```sql
-- per profile: latest post timestamp straight from bronze/silver
SELECT owner_username, MAX(timestamp) AS last_post_at
FROM silver_ig_posts GROUP BY 1
```

Then each run requests posts newer than that per-profile value. Preferred
mechanism, in order:

1. **Per-URL date boundary** if the actor accepts a per-URL `onlyPostsNewerThan`
   — not established; the schema documents it as a **top-level** input, so this
   likely requires one run per date-bucket (see batching below).
2. **Keep `resultsLimit` as the fetch guard, with the watermark as the
   *selection* rule** — i.e. the watermark decides *which* profiles to refresh
   and how deep; the actor still returns newest-N.

**Loud-failure requirement:** a profile whose watermark cannot be resolved (no
posts in silver, i.e. never scraped) MUST be treated as "full backfill needed"
and surfaced, never silently skipped — a skipped new profile is the silent-loss
shape this repo has been burned by.

### Cold start, run warmth, and the real cost driver (MEASURED)

The owner's question: does each run pay a cold start, or does an actor stay warm?

**What the historical record CAN and CANNOT show.** These runs are from
2026-09-10 (a week old), so warm-pool state is **not observable retroactively** —
"does it stay warm after the first run of the day" cannot be answered from
history at all. It needs a **live paired test** (N runs back-to-back vs N runs
separated by hours), which is an explicit task for this branch.

**What the record DOES show — compute scales with work, and there is no fixed
startup term:**

| Run | `inputBodyLen` | wall | computeUnits |
|---|---|---|---|
| `9ZdO7hRN` | 80 B | 7.1 s | 0.0079 |
| `YBqZ9AV` | 36 B | 123 s | 0.1367 |
| `WtKeSF8Q` | 969 B | 156.7 s | 0.0109 |

`inputBodyLen` is the discriminator: a 969-byte input is a multi-URL run, a
36-byte one is not. **The long runs differ by INPUT, not by temperature** — a
466 s run and a 7 s run are doing different amounts of work. There is no visible
fixed cold-start charge; the short runs are short because they did less.

**Compute units = memory x time — EXACT, not a fit. Apify bills 1 CU per
GB-hour.** Fitted over all three runs (residual shown, so the constant is
defensible rather than single-point):

| Run | memo MB | wall s | GB-s | observed CU | predicted (K x GB-s) | residual |
|---|---|---|---|---|---|---|
| `9ZdO7hRN` | 4096 | 7.100 | 28.40 | 0.00789 | 0.00789 | **0.00%** |
| `YBqZ9AV` | 4096 | 123.015 | 492.06 | 0.13668 | 0.13668 | **0.00%** |
| `WtKeSF8Q` | 256 | 156.729 | 39.18 | 0.01088 | 0.01088 | **0.00%** |

**K = 0.000277778 CU per GB-second = 1/3600** — so this is not a fitted
estimate, it is **the billing formula itself: 1 CU = 1 GB-hour of
memory × runtime.** *(Evidence scope: extrapolated from three runs of ONE actor
(2026-09-10) — it is a measured identity, not a published rate card, so treat it
as verified for this actor and re-check if compute ever bills oddly.)* The 0.00% residual on all three runs (identical to six
decimals) is what distinguishes a reverse-engineered identity from a model that
merely correlates. All three pairwise GB-s ratios match their CU ratios
(0.06 / 0.72 / 12.56).

**Consequence: compute cost is now EXACTLY computable, not estimated.**
`CU = (mem_MB / 1024) × wall_s / 3600`. At 4096 MB that is **1/900 CU per
second**; at 1024 MB, **1/3600 CU per second** — a 4x lever that is purely the
memory setting, and arithmetic rather than inference.

**Budgeting consequence:** cost scales with **memory x wall-clock**, so halving
memory halves compute at equal throughput. This is what makes the memory
question below worth measuring rather than assuming.

**What IS established:** compute units are exactly `memory x wall-clock`, so
memory is the cost multiplier. Runs report **`memoryMbytes: 4096`** while the
actor declares `defaultRunOptions.memoryMbytes = 1024`, and **our code passes no
memory argument at all** (`grep` over `scrape.py` and `apify_client.py` is
empty) — so 4096 is applied platform-side, not by us.

**What is NOT established (hypothesis, requires measurement):** that passing
`1024` explicitly reduces billing. Our grep proves we do not *set* it; it does
not prove Apify honours a lower value rather than clamping to its own floor. The
party to confirm is a run that explicitly passes 1024; until that lands, the
"4x saving" is a hypothesis, not a finding. AC 13 requires exactly that
measurement.

**Caveat on this table:** `itemCount` and `usage` are `None` on the run objects,
so **per-result cost cannot be derived from these records.** Quote the measured
$0.0023/result from the billing cycle, not from these runs.

**Consequence for the fan-out:** task-runner start time is not the bottleneck the
owner feared — but confirm warmth with the live paired test before relying on it.
`16 x 1,024 MB = 16,384 MB` of the 65,536 MB budget, and setting memory explicitly
is the practical saving.

### Reuse the `details_sweep` contract — do NOT invent a parallel mechanism

`platform/details_sweep.py` already implements this exact shape and its four
properties are the contract this design must inherit:

| Property | Implementation | Why it matters here |
|---|---|---|
| Watermark row | `WATERMARK_NAME`, one name per sweep | one row, not per-profile state |
| Named cap | `DEFAULT_MAX_PROFILES_PER_SWEEP = 10` | the ADR-0018 bound, chosen once |
| **Staleness ordering** | `ORDER BY updated_at ASC` | a capped tick drains the **OLDEST first** |
| **Advance AFTER success** | called by the asset, never the schedule | see below |

**The ordering property is load-bearing for a 16-run cap.** `details_sweep`
sorts by `updated_at ASC` so *"a backlog drains in order instead of starving
whichever profile happens to sort last."* A per-creator watermark sync must sort
by **staleness** (oldest `MAX(timestamp)` first) for the same reason — sorting by
`handle` with a 16-run cap would permanently starve the alphabetically-last
profiles.

**The advance-after-success property prevents silent retirement.** The docstring
is explicit: *"Advancing at evaluation time would move the watermark past runs
that had not executed yet: an Apify error or a cap would then never be retried,
and the profile would be silently skipped forever."* A per-creator `last_synced`
MUST be written **after a successful scrape lands in bronze**, never when the run
is merely requested. This is the same silent-loss shape as the never-scraped
profiles.

**Band boundaries vs depth:** the staleness bands (7/30/180 days) are a
*different axis* from `resultsLimit` depth — sync cadence vs backfill depth. Keep
them as separate fields; do not conflate them.

### Cadence batching by last-post age (owner's proposal)

Because `onlyPostsNewerThan` is a **top-level** input, one run can only filter to
one boundary — so profiles are **grouped into date buckets** and each bucket is a
run (or a run carrying many profile URLs):

Buckets are **staleness tiers** — an axis distinct from `resultsLimit` depth
(backfill depth vs sync cadence are different fields; do not conflate them).

Measured populations (whole roster / refreshable subset), staleness bands:

| Band | Roster (675) | Refreshable (51) |
|---|---|---|
| never scraped | 14 | 14 |
| within 7 days | **0** | **0** |
| 7–30 days | 136 | 9 |
| 30–180 days | 346 | 19 |
| >180 days | 179 | 9 |

**Note the >180d band: 179 on the roster versus 9 refreshable.** The long-dormant
population is overwhelmingly the **ad-hoc corpus (`results_limit = -1`) which
must not be scheduled.** Bucketing over the roster would sweep in 624 profiles
the design deliberately excludes.

**This composes with the fan-out cap:** a bucket is a bounded unit of work, so
"16 concurrent runs" bounds a bucket's parallelism rather than the whole corpus.
**Bucket membership must be computed from the watermarks at run time**, not
stored — a stored field on `profiles` would be wiped by the `INSERT OR REPLACE`
upsert (same trap as `suggested_core`, US-DISC-1).

**MEASURED — live watermark coverage (2026-09-17).** Join is verified real:
661 of 675 profiles match `silver_ig_posts.owner_username`; silver holds 692
distinct usernames. The 14 non-matching are genuine profile names.

Whole roster (675):

| Bucket | Profiles |
|---|---|
| never scraped (no silver posts) | 14 |
| posted within 7 days | **0** |
| 7–30 days | 136 |
| 30–180 days | 346 |
| >180 days | 179 |

Refreshable subset (the 51 with `results_limit != -1`): 14 never scraped, **0
within 7 days**, 9 at 7–30d, 19 at 30–180d, 9 at >180d.

**Three consequences that reshape the design:**

1. **The 7-day bucket is EMPTY** — and every profile therefore sits in the
   7–30d band or older. The newest post in silver is **2026-09-05** while today
   is **2026-09-17**: a 12-day corpus gap. Buckets must be computed at run time
   and never assumed static; on a freshly-caught-up corpus the distribution
   inverts.
2. **A 12-day-dry corpus against a 7-day cadence means a flat
   `onlyPostsNewerThan: "10 days"` would return NOTHING** for any profile
   whose last post predates 2026-09-07 — which is all of them. This is the
   concrete failure the per-creator watermark fixes. It is also **current
   state, not a design property**: the first run after any resume looks wide,
   and the corpus being dry is a scheduling gap.
3. **The 14 never-scraped profiles are the silent-loss risk** — they must enter
   the backfill bucket explicitly (the loud-failure requirement above), not be
   filtered out by a watermark join that finds no row.

### The $0.0023/result rate — VERIFIED exact by run size (2026-09-17)

Settled from the run records and the actor's `pricingInfos`:

| Actor | results | billed event | per-result |
|---|---|---|---|
| `apify~instagram-scraper` | 1, 3, **912** | `result` @ $0.0023 | **exactly $0.0023** |
| `apify~instagram-profile-scraper` (details) | 10, 20, 30 | `profile` @ $0.0023 | exactly $0.0023 |

The 912-result run billed **$2.0976 = 912 × $0.0023** to the cent, so linearity
holds across three orders of magnitude. Current billing model is `PAY_PER_EVENT`
(from the actor's `pricingInfos`, effective 2026-02-20).

**Trap:** two runs that appeared to bill ~33% above list ($0.00305, $0.00311)
belonged to a **different actor**, billed `apify-default-dataset-item` @ $0.003
plus a one-time $0.004 `apify-actor-start`. Always attribute a rate to its actor
before deriving a unit cost — this is the same class of error as inferring
capability from our own wrapper.

**Compute/proxy are SEPARATE lines, never blended.** `ACTOR_COMPUTE_UNITS`
(@ $0.30/CU) and `PROXY_RESIDENTIAL_TRANSFER_GBYTES` are reported independently;
at the last full cycle compute was $0.496 against $37.44 per-event. So the
per-result term is the right planning basis, and **$3.52 / $46.57 stand.**

**A 200-item run that billed $0.1798 is NOT a counterexample** (it looks like one at
$0.46 of per-result). Its cost decomposes as `ACTOR_COMPUTE_UNITS` 0.1367 CU ×
$0.30 = $0.0410 plus `PROXY_RESIDENTIAL_TRANSFER` ≈ $0.1232 → ≈ $0.164 of the
$0.1798, and its `platformUsageBillingModel` was `USER` (compute-billed), not the
developer per-event path. **Different billing model, not a different rate** — the
same trap as the different-actor rows, caught here for the record.

**Source of each figure** (they are not the same kind of evidence): the **unit
price $0.0023** is measured from the billing cycle across 22 runs, not from a
single run's `usageTotalUsd`; the **CU identity (1 CU = 1 GB-hour)** is
extrapolated from three runs of one actor; **$0.30/CU** comes from the cycle
($0.4958 / 1.6527 CU).

**Re-read the rate periodically, not just on tier change.** `pricingInfos`
shows the rate moving with no tier change ($0.0023 → $0.0007 → $0.0023, then a
model switch to `PAY_PER_EVENT` on 2025-12-12), so the figure is a value to
re-fetch, not a constant.

**Cost of the first run — DENOMINATOR MATTERS.** Verified against
`ops.sqlite`: 675 profiles total, **all 675 `enabled = 1`**, of which:

| Filter | Count | First-run cost (x30 x $0.0023) |
|---|---|---|
| `enabled AND results_limit != -1` (**the refreshable set**) | **51** | **$3.52** |
| all enabled / whole roster | 675 | $46.57 |

**The `enabled` flag does no filtering** (all 675 are enabled) — the
**`results_limit != -1` sentinel is the only real gate**, and it is the same
contract as `ig_core/slv/roster.py:126-128` and `opsdb.roster.enabled_profiles`.

**Therefore: if the sync ranges over the refreshable set — which is what the
refresh contract selects — the first run costs ≈ $3.52, not $45.61.** An earlier
draft of this story said $45.61 by ranging over 661 matched profiles; that
denominator is wrong and has been corrected. **$45.61 is the number only if the
661 matched profiles (refreshable + ad-hoc together) are synced** — and syncing
**only the 624 ad-hoc profiles** would be $43.06 (624 × 30 × $0.0023). Neither is
the design, which explicitly does not schedule the ad-hoc corpus.

**This is the figure the owner is being asked to sign off on: $3.52.**
Corpus freshness is a separate matter (below) and does not change the
denominator.

### The fan-out unit: N PROFILES PER RUN (verified from the schema)

**The well-behaved parallelism is N *different* profiles per run, not N slices of
one profile.** `trigger_run` takes `urls: list[str]` (`apify_client.py:99`) and
`resultsLimit` is documented *"per Instagram URL"*, so:

```python
trigger_run(urls=[p1, p2, ..., p8], results_limit=30)
```

returns the last 30 posts of **each of the 8 profiles**, with **zero overlap** and
**no disjointness proof required**. Eight runs each covering 30 profiles is
8 x 30 profile-results, billed exactly once each.

**Why slicing ONE profile by count is not expressible:** `resultsLimit` is per
URL and the input schema has **no offset parameter**. Passing the same profile
URL to N runs gives N runs each independently returning the **same** newest N —
the same items, billed N times.

**Consequence for the owner's proposal:** *"8 jobs x 125 posts of one profile"*
is **not required to get the parallelism benefit** — and is the shape that
double-bills. Queueing **many profiles** across bounded runs achieves the same
wall-clock win with no double-billing exposure. The only way to slice a single
profile is disjoint **`onlyPostsNewerThan` date windows** (e.g. month-by-month),
which multiplies runs and startup overhead and is likely not worth it.

**This dissolves open question 1:** count-based in-profile slicing is not
expressible, and profile-partitioned fan-out is safe by construction.

### Pipeline facts the fan-out design must respect (verified)

- **Idempotency is keyed on `dataset_id`** (`bronze_path(dataset_id)` + an
  exists-check), and silver discovery keys on bronze file **mtime**. So N
  parallel jobs producing N bronze files is **compatible** with the write-once
  model and adds no locking risk.
- **The fan-out unit is the profile** — one run may carry several profile URLs,
  each getting `resultsLimit` applied independently.

**Do not infer mechanism from our wrapper** — that is the exact mistake that hid
`onlyPostsNewerThan`. Every mechanism asserted here comes from the actor's input
schema or the API docs.

## Acceptance criteria (binary)
1. **GIVEN** a backfill config with depth N,
   **WHEN** triggered,
   **THEN** the body sends `resultsLimit: N` and the projected cost
   (`N × $0.0023`) is derivable and surfaced.
2. **GIVEN** a sync config,
   **WHEN** triggered,
   **THEN** the boundary is derived per profile from that creator's
   `MAX(timestamp)` in the lake — a hard-coded flat `"10 days"` is not
   acceptable — **and the run transmits the bucket's OLDEST watermark as the
   single `onlyPostsNewerThan` value** (`onlyPostsNewerThan` is top-level: one
   date per RUN, so a multi-profile run must transmit the widest boundary its
   members need, letting the per-profile filter happen at selection time).
   **Division of labour, stated so run granularity is not left to taste:**
   - the **per-profile watermark decides WHICH profiles refresh and how deep**
     (selection) — that is where the per-creator boundary lives;
   - the **transmitted date is bucket-level** (the oldest watermark in the
     bucket), because one run carries one date;
   - `resultsLimit` remains the per-URL upper bound.

   **Runs over the OLDEST-watermark-ordered queue are the unit of parallelism**
   (see the fan-out cap, AC 6) — not one-run-per-profile, which the 16-run cap
   would make pathological for a 51-profile sync.
3. **GIVEN** a creator with 1 post newer than THAT CREATOR'S watermark (the
   window is their own boundary-to-now span, not a fixed 10 days — the fixture
   must construct the per-profile boundary, or a green test would prove the
   wrong thing), **WHEN** sync runs, **THEN** the dataset contains 1 item (not
   N) — proving the filter is applied server-side.
4. **GIVEN** a scrape is requested through the transport (whichever client is in
   use), **WHEN** the payload is asserted, **THEN** it carries
   `directUrls`/`resultsType`/`resultsLimit`/proxy **plus** the per-run values
   this story adds (`onlyPostsNewerThan`, and an explicit `memoryMbytes`).
   *(This is a SYNC-PATH requirement — payload shape for the sync inputs. The SDK
   migration asserts the same body key-by-key in **US-DISC-8 AC 3**
   (payload equivalence), with module removal and schema access at ACs 1 and 2.)*
5. **GIVEN** concurrency is enabled,
   **WHEN** the config is applied,
   **THEN** it respects both the 32-run cap and the 64 GB combined-memory cap,
   and the per-result cost is unchanged.
6. **GIVEN** the fan-out cap is a **named, chosen-once constant**
   (`DEFAULT_MAX_PARALLEL_SCRAPE_RUNS`, mirroring
   `DEFAULT_MAX_PROFILES_PER_SWEEP`),
   **WHEN** a tick would emit more runs than the cap,
   **THEN** it emits at most the cap — asserted by a test, so the bound lives in
   our code rather than being inherited from Apify's plan ceiling.
7. **GIVEN** the operator sets a fan-out above the account's concurrent-run
    limit,
    **WHEN** the task starts,
    **THEN** it fails LOUDLY at `pre_execute` with the limit named — not by
    launching runs that are rejected server-side.
8. **GIVEN** parallelism is applied,
    **WHEN** N runs are launched,
    **THEN** each covers a **disjoint set of profiles** (or disjoint
    `onlyPostsNewerThan` date windows), and a test asserts that no profile URL
    appears in more than one concurrent run — since a profile URL repeated
    across runs returns the same newest-N and bills N times.
9. **GIVEN** a sync run,
    **WHEN** it selects profiles,
    **THEN** each profile's boundary comes from `MAX(timestamp)` in
    `silver_ig_posts` for that handle — the watermark is read from the LAKE, not
    `ops.sqlite`, and its expression is exercised by a test.
10. **GIVEN** the selection ranges over the **refreshable set**
    (`enabled AND results_limit != -1`; 51 profiles live, of which **14 have no
    posts in silver**), **WHEN** the selection runs, **THEN** each never-scraped
    profile is placed in the full-backfill bucket and surfaced in the run log —
    never silently excluded by the watermark join. **The population is the
    refreshable set, not the 675-profile roster**, and a test asserts the
    never-scraped group is not dropped.
11. **GIVEN** bucket membership,
    **WHEN** computed,
    **THEN** it is derived at run time from the watermarks and is NOT stored on
    `profiles` (the `INSERT OR REPLACE` upsert would wipe it).
12. **GIVEN** an engagement observation,
    **WHEN** freshness is assessed,
    **THEN** it counts **distinct `source_dataset`**, not distinct `observed_at`
    — a dataset replay appends nothing (`INSERT OR IGNORE`) and must not read as
    fresh observation.
13. **GIVEN** runs are launched,
    **WHEN** `memoryMbytes` is chosen,
    **THEN** the request specifies it **explicitly**, and the value is selected
    by comparing **`mem_GB × wall_s` (GB-seconds), not wall-clock alone**, at
    1024 MB vs 4096 MB for the same input — **lower GB-s wins**. Because Apify
    bills exactly 1 CU per GB-hour, this is arithmetic, not preference: at
    4096 MB a 300 s run costs 0.3333 CU against 0.0833 CU at 1024 MB, so 4096 is
    justified only if it demonstrably cuts wall-clock by more than the 4x it
    costs — precisely: **more than a 75% runtime cut favours 4096 MB, exactly
    75% is a wash, and less than 75% favours 1024 MB.** At exactly 75%,
    4 GB × 75 s = 300 GB-s equals 1 GB × 300 s = 300 GB-s; a run that merely
    halves costs 2x MORE (4 GB × 150 s = 600 GB-s vs 300 GB-s).
14. **GIVEN** one run carries several profile URLs,
    **WHEN** it completes,
    **THEN** it returns `resultsLimit` results for **each** URL (per-URL
    semantics), so a single run already provides the fan-out unit without any
    in-profile slicing.

15. **GIVEN** the roster sentinel predicate
    (`enabled AND results_limit != AD_HOC_LIMIT` — the ONLY thing separating
    the 51 refreshable profiles from the 624 never-schedule ad-hoc ones, since
    `disabled = 0` and all 675 are `enabled`),
    **WHEN** the selection is exercised,
    **THEN** a test asserts that ad-hoc profiles (`results_limit = -1`) are
    **excluded** and that the returned count matches the refreshable population
    — so a refactor that drops, reorders, or re-values that predicate fails
    loudly instead of silently resubmitting the entire ad-hoc corpus to Apify.

## Definition of done

- [ ] Blast radius enumerated before editing: the scrape path (`scrape.py`),
      `ScrapeConfig`, the transport's `trigger_run` call site. *(The SDK's own
      blast radius — patch target, module rename — is US-DISC-8's.)*
- [ ] A **real smoke run** over the smoke slice: the changed sync path executed
      end-to-end and the destination verified — not just a green suite.
- [ ] Concurrency **measured**, not assumed: cost/result and wall-clock for
      1 vs 8 vs 32 runs, recorded.
- [ ] **Named fan-out cap constant** + a test asserting it (AC 6), mirroring
      `DEFAULT_MAX_PROFILES_PER_SWEEP`; a pre-execute loud failure above the
      account limit (AC 7).
- [ ] Fan-out partitions by **profile** (AC 14); no profile URL appears in two
      concurrent runs, asserted by a test.
- [ ] Per-creator watermark sync implemented, read from the LAKE (AC 9);
      never-scraped profiles routed to the backfill bucket loudly (AC 10).
- [ ] Bucket membership computed at run time, nothing stored on `profiles`
      (AC 11).
- [ ] Freshness counted as distinct `source_dataset` (AC 12).
- [ ] `memoryMbytes` set explicitly and measured at 1024 vs 4096 MB (AC 13) —
      runs currently report 4096 MB though our code passes no memory argument and
      the actor declares 1024, and compute units are memory x time.
- [ ] Watermark state is advanced by the ASSET after a successful scrape, never
      at evaluation time, and the selection orders by STALENESS (`details_sweep`
      contract) so a capped tick cannot starve the tail.
- [ ] Live paired test for run warmth (N back-to-back vs N separated) — warmth
      is not observable from historical runs.
- [ ] Cost claims state their DENOMINATOR (refreshable 51 vs roster 675); no
      figure is quoted without it.
- [ ] Sentinel-predicate test (AC 15): ad-hoc profiles (`results_limit = -1`)
      proven excluded, so the roster filter cannot silently resurface the 624.
- [ ] Memory saving verified by an EXPLICIT 1024 MB run before the 4x figure is
      written anywhere as a finding.
- [ ] The dormancy/staleness behaviour is stated in the module docstring and
      in this story's open questions.
- [ ] **Capability questions answered from the actor schema/docs, never from our
      wrapper** — the `onlyPostsNewerThan` miss is the precedent.
- [ ] No `pip`; `uv` only. Conventional commits; branch per repo conventions.
- [ ] Reasoning trace + assumption log.

## Open questions

**DECISION NEEDED NOW: approve the first-run spend — $3.52 (recommended: the
51-profile refreshable set) or $46.57 (the full 675-profile roster, i.e. the 624
ad-hoc profiles synced IN ADDITION to the 51). See the last question below.
Everything else here is a measurement task, not a blocker.**

1. **Not a blocker — DISSOLVED:** a count-based split of one profile is not
   expressible (`resultsLimit` is per URL, no offset input), and
   profile-partitioned fan-out is safe by construction. Optional refinement
   only: is an `onlyPostsNewerThan` date-window split ever worth extra runs and
   startup overhead for one very deep profile?
2. **ONE measurement run set answers BOTH 1024-vs-4096 and warmth.** These are
   separable experiments but can share a batch: run N back-to-back at 1024 MB and
   N at 4096 MB. That yields the two comparisons AC 13 needs —
   `mem_GB × wall_s` per setting (the RULE is settled; only the run is open) —
   AND reveals an interaction AC 13 cannot see alone: whether a 1024 MB run is
   slower only when the pool is cold. Do not run the two tests as separate
   batches.
3. **Long-dormant creators: a COST question, not a data-loss one.** Under a
   per-creator watermark there is no fixed window to fall outside of, so no
   creator is "missed" and nothing goes silent — the boundary simply ages, and
   the selection keeps picking them up. **The real residual is that a very old
   boundary means the run asks for a very wide window**, i.e. a larger fetch and
   longer runtime, not lost data. Open question: for the `>180d` band (179 on
   the roster, 9 refreshable), is bucketed re-entry sufficient, or is an
   explicit deep-recovery pass warranted for cost/latency control? The
   flat-window dormancy concern is DISSOLVED, and the truncation-is-benign
   finding above still holds.

   **The concrete failure mode, stated precisely:** a very old boundary asks for
   a very wide window, and there is **no way to page** — `resultsLimit` is per
   URL and the actor's input schema offers **no offset parameter** (both
   verified from the schema). **What is NOT yet verified:** whether a
   date-windowed fetch **truncates at `resultsLimit`** or returns the whole
   window. That half is inference, not evidence, and it is load-bearing for the
   deep-recovery design — **confirm it from the schema or the first real run.**
   Either way the recovery is bounded by capacity (possibly several sequential
   runs), not by posts we silently miss.
4. **Confirm whether a date-windowed fetch truncates at `resultsLimit`.**
   Verified from the schema: `resultsLimit` is titled **"Results limit per URL"**
   (*"Set how many posts or comments to scrape per Instagram URL"*) and there is
   **no offset input**; `onlyPostsNewerThan` is a separate top-level string.
   **Unverified: whether the combination truncates the date window or returns it
   whole.** The field name says "limit", which reads as an output cap and
   supports truncation — but that is inference, so confirm from the schema or
   the first real run before relying on it. This bounds the deep-recovery design
   (question 3).
5. **Promotion depth default** (from US-DISC-2): one default (e.g. 30) or
   per-creator?
6. **DECISION REQUIRED — approve the first-run spend.** The first sync costs
   **≈ $3.52** (51 refreshable profiles × 30 results × $0.0023), and its scope is
   the sentinel-filtered set. **Alternative, at ~13x:** syncing the **full 675
   profile roster** — the 624 ad-hoc profiles (`results_limit = -1`) *in
   addition to* the 51 refreshable — costs **$46.57** (675 × 30 × $0.0023), and
   would mean scheduling work the design currently declares never-scheduled.
   **Answer needed: $3.52 (recommended) or $46.57 — and if $46.57, confirm the
   624 ad-hoc profiles are deliberately in scope.**
