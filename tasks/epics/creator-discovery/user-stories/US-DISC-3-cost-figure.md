# US-DISC-3 — Report the monthly cost of the allocated core list

- **Epic:** E-DISCOVERY
- **Persona:** P7 (Owner / Principal)
- **Status:** Open
- **Depends on:** US-DISC-2

## Story

**As** the owner,
**I want** the projected monthly Apify cost of the allocated core list reported
on the dashboard overview,
**So that** I can see the cost consequence of my allocation decisions before
committing to them.

## Context

Cost is computable from facts we already hold: the Apify Instagram actor's
$/1k results rate, each profile's **allocated depth** (`profiles.results_limit`),
and which profiles are in the core set.

**The rate is measured (2026-09-17), not assumed.** Actor
`apify~instagram-scraper` bills `PAY_PER_EVENT` per result — *"Each result
written to the dataset"* — at a **plan-tier-dependent** rate:

| Plan tier | $/1,000 results |
|---|---|
| FREE | $2.70 |
| **BRONZE (ours)** | **$2.30** |
| SILVER | $1.90 |
| GOLD | $1.50 |
| PLATINUM | $0.90 |
| DIAMOND | $0.50 |

Our plan is **STARTER / BRONZE** ($39/mo, $39 credits). **Measured rate: exactly
$0.0023/result = $2.30 per 1,000 results** on the `apify~instagram-scraper` actor
— **verified from the run records, invariant to run size**: runs of 1, 3 and 912
results all bill exactly *n* × $0.0023, the 912-result run at $2.0976 to the
cent. **Use $2.30/1k**; the rate MUST be a single named constant carrying its
source, actor, tier, and measurement date.

**Two corrections to an earlier draft of this story (2026-09-17):**

- **Billing is NOT "pure per-result with no compute component."** Compute and
  proxy are billed as **separate line items** — `ACTOR_COMPUTE_UNITS`
  (@ $0.30/CU; 1 CU = 1 GB-hour = memory_GB × wall_s / 3600) and
  `PROXY_RESIDENTIAL_TRANSFER_GBYTES`. At the last full cycle compute was $0.496
  against $37.44 of per-event charges. Plan on the per-result term; account for
  compute separately.
- **Figures from 80- and 35-item runs do not belong to this actor.** They billed
  $0.2440 and $0.1090 — ~33% above list — because they were runs of a
  **different actor** (`apify-default-dataset-item` @ $0.003 plus a one-time
  $0.004 `apify-actor-start`). Attribute a rate to its actor before deriving a
  unit cost from it.

**Re-read the rate periodically, not just on tier change.** The actor's
`pricingInfos` shows the rate moving with no tier change ($0.0023 → $0.0007 →
$0.0023) and the model switching to `PAY_PER_EVENT` on 2025-12-12, so the figure
is a value to re-fetch rather than a constant.

**Segment by call type before deriving any unit cost.** `details` runs are
near-flat per profile while post scrapes scale with items; a blended figure
across both is not a unit cost. (An earlier "$1.105/1k" came from one run's
`usageTotalUsd`, which does not cover all billed components — **retracted**.)

Current spend for reference (cycle 2026-08-12 → 2026-09-11):
`PAID_ACTORS_PER_EVENT` $37.44 + $2.81 other = **$40.25**, against the $39 credit
line — the account is **already at its included-credit ceiling**, so a per-run
`maxTotalChargeUsd` cap matters.

**D must come from the allocated depth, never a constant.** `results_limit` is
`-1` (`AD_HOC_LIMIT`, never scheduled) on 624 profiles and defaults to
`DEFAULT_DEPTH = 1` (`opsdb/roster.py:42`). So:

- a profile at `-1` contributes **zero** (the sentinel filter excludes it);
- a profile at `1` contributes one result, not thirty.

Assuming a fixed depth of 30 would overstate cost for sentinel profiles and
understate it for shallow ones. The figure must sum each profile's actual
`results_limit`.

The figure MUST be labelled an estimate wherever it is shown. A number presented
as fact when it rests on an uncalibrated rate is exactly the silent-failure
class this repo guards against.

## Acceptance criteria (binary)

1. **GIVEN** an allocated core set of profiles each with depth D_i,
   **WHEN** the cost is computed,
   **THEN** it equals `Σ(D_i) × rate / 1000`, with `rate` a single named,
   documented constant.
2. **GIVEN** the rate constant,
   **WHEN** the figure is displayed,
   **THEN** it is marked as an **estimate**, and the constant carries its
   source, tier, and measurement date — not presented as actual spend.
3. **GIVEN** the Apify plan tier changes (e.g. BRONZE → SILVER),
   **WHEN** the rate constant is inspected,
   **THEN** it is a single named constant whose provenance names the tier, so a
   tier change is a one-line update rather than a silent error.
4. **GIVEN** a profile with `results_limit = -1` (ad-hoc sentinel),
   **WHEN** the cost is computed,
   **THEN** it contributes zero (sentinel profiles are never refreshed).
5. **GIVEN** a profile whose depth is the schema default `1`,
   **WHEN** the cost is computed,
   **THEN** it contributes exactly one result — the figure does not assume a
   larger depth.
6. **GIVEN** the owner promotes one creator into the core set at depth 30,
   **WHEN** the cost is recomputed,
   **THEN** the figure increases by `30 × rate / 1000`.
7. **GIVEN** no profiles are allocated to the core set,
   **WHEN** the cost is shown,
   **THEN** the figure reads a defined zero (not NULL, not an error).
8. **GIVEN** the cost computation,
   **WHEN** it is inspected,
   **THEN** it performs no aggregation in the dashboard server (it is a
      projector over a serving value).

## Definition of done

- [ ] Cost surfaced on the dashboard overview with an explicit "estimate"
      treatment.
- [ ] Cost sums **actual `results_limit` per profile** — no assumed constant
      depth (AC 4 is the load-bearing test).
- [ ] The rate constant is named, documented with its source, **tier**, and
      measurement date (AC 2/3).
- [ ] ACs 1–8 each have a test; ACs 4 (sentinel → zero) and 5 (default depth is
      one, not thirty) are load-bearing.
- [ ] Re-measure the effective rate from Apify's usage API (or a real run's
      `usageTotalUsd` ÷ `itemCount`) if the plan tier changes — the constant is
      tier-dependent, so a tier change silently invalidates it.
- [ ] No aggregation in `dashboard/server.py` (guard:
      `test_no_aggregation_in_server.py`).
- [ ] Reasoning trace + assumption log.

## Open questions

- Should the figure show allocated-cost only, or also the *counterfactual*
  (what it would cost if every `suggested_core` creator were promoted at depth
  30)? The counterfactual is what makes the allocation decision legible, and it
  is the more useful number for sizing — but it is an estimate of an estimate, so
  it must be labelled as such.
