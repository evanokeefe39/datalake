# US-DISC-1 — Surface a data-derived suggested-core signal per creator

- **Epic:** E-DISCOVERY
- **Persona:** P7 (Owner / Principal), P3 (Growth Analyst)
- **Status:** Open
- **Depends on:** E-SERVING-ANALYTICS (`v_creator_profile`, `v_post_metrics`)
- **Owner decision (2026-09-17):** threshold rule accepted as proposed; tunable later.

## Story

**As** the owner curating a creator list,
**I want** the warehouse to compute which creators are *obviously popular and
interesting* from their standout performance,
**So that** I review a populated candidate list of creators worth keeping
up to date — while the final allocation stays mine.

## The rule (accepted 2026-09-17; tunable)

**`suggested_core` = `standout_count >= 2` AND `hot_count >= 1`**

Both predicates reuse existing canonical definitions:

- **`standout_count`** comes from the Tukey fence (`likes > Q3 + 1.5·IQR`
  against the creator's own trailing baseline) — robust to outliers, already
  computed, already canonical.
- **`hot_count`** is the exceptional subset: `is_hot = is_standout AND
  likes_zscore >= 2`. Requiring one hot post means at least one *clearly*
  exceptional post, not just two marginal ones.
- **Requiring 2 standouts** (rather than 1) protects against a single lucky post
  being the entire evidence for a creator.

**Why an ABSOLUTE COUNT, not a rate (owner decision 2026-09-17).** A rate needs a
stable denominator, and we often ingest only 10–30 posts for a creator — and for
most creators far fewer. The live distribution:

| Posts per creator | Creators |
|---|---|
| < 10 | **535** |
| 10–29 | 27 |
| 30–99 | 50 |
| 100+ | 49 |

**535 of 661 creators have fewer than 10 posts.** A rate over a 5-post
denominator is noise — a single standout reads as 20%. So the rate would be
computed mostly on creators where the denominator carries no information. The
absolute count is the honest measure of the evidence available.

**Measured effect (live data, 661 creators, all rules against `v_creator_profile`):**

| Rule | Creators | Est. cost/mo at ACTUAL depths |
|---|---|---|
| `standout_count >= 1` | 105 | $4.72 |
| **`standout_count >= 2 AND hot_count >= 1`** | **94** | **$4.23** |
| `standout_count >= 3 AND hot_count >= 1` | 89 | $4.00 |

*(The column is cost at each profile's ACTUAL allocated depth — summing real
`results_limit`, per US-DISC-3's rule never to assume a constant depth. It is
**not** `creators × 30 × $0.0023`: 94 × 30 × $0.0023 would be $6.49. The 94-creator
row implies ≈ 1,840 results total, ≈ 19.6 per creator.)*

*(Dated-history erratum: `cross-check-2026-09-17.md` states "94 creators,
~$4.23/mo **at depth 30**". The figure is right, the **label** is wrong — at depth
30 it would be $6.49. The cross-check is retained unmodified as point-in-time
history; this note is the correction of record, as with its §3b trigger wording.)*

Every absolute-count variant lands in a narrow band (89–105 creators,
$4.00–$4.72), so the threshold is not where the cost decision lives — pick on
meaning, not arithmetic. Both the loose and strict variants fit the Free tier's
$5 cap; the `>= 2` form is preferred because it requires corroborating evidence.

**Rejected alternatives, for the record:**

- **A rate (`standout_rate >= 0.10`)** — rejected on the denominator problem
  above (owner decision). It also would have measured inconsistently: the
  `total_posts >= 3` floor excludes creators while others enter on 10-post
  denominators.
- **`is_rising` alone** — 6 creators, too narrow; momentum answers a different
  question (recent-vs-baseline trend, not absolute quality).

`standout_rate` is still exposed for **display only**, composed from the
**existing definition** in `gold_creator_performance` (`marts.py:217-218`) — do
NOT re-derive it in a new view (see the DoD). `is_rising`, `momentum_ratio`, and
`dominant_domain` are likewise display context, not thresholds.

## Premise: three distinct axes, kept separate

| Axis | Meaning | Where it lives | This story? |
|---|---|---|---|
| **`follower_tier`** | Audience-size bucket — **4 canonical buckets: `0-100`, `100-1k`, `1k-10k`, `10k+`** | `v_post_follower_context` (`views.py:502-503`); marts cite it as canonical (`marts.py:167-168, 260`) | **No** — reuse as-is |
| **`suggested_core`** | **Performance signal only** (the rule above) | warehouse (derived, this story) | **Yes** |
| **core allocation** | The purchase decision | ops, via `results_limit` (see epic Q1) | **No** — US-DISC-2 |

**Do NOT introduce a `follower_band`.** `follower_tier` already exists with its
four buckets and is the single definition; a third name for audience size would
collide three ways (`tier` = allocation, `follower_tier` = audience,
`follower_band` = audience again).

**Wider bands are NOT this story.** The epic's audience-band discovery intent
(`<100k`, `<1M`, `1M+`) is implied by discovery, but canonical `follower_tier`
**tops out at `10k+`** — so those wider bands do not exist in any view and
would be a **new definition** requiring its own story, not a silent extension of
a canonical one. **Owner decision (2026-09-17): stay with the canonical four.**

**Also settled:** the suggestion is **warehouse-only**. It must not become a
`profiles` column — `add_profile` is `INSERT OR REPLACE` with an explicit column
list (`opsdb/roster.py:218-221`), so SQLite's delete-then-insert semantics would
**wipe** any unlisted column on every dashboard edit.

## Context

**No new aggregation is required.** `v_creator_profile`
(`serving/metrics.py:309-379`) is a one-row-per-creator canonical rollup
carrying `creator_name`, `total_posts`, **`standout_count`, `hot_count`**,
`avg_likes`, `max_likes`, `avg_engagement_score`, `dominant_domain`,
`recent_avg`/`recent_posts`, `baseline_avg`/`baseline_posts`, `momentum_ratio`,
and `is_rising`. The work is to expose the rule as a serving fact plus a review
surface — not to aggregate.

**Reference query (verified to run, single source view):**

```sql
SELECT creator_id, creator_name, dominant_domain,
       total_posts, standout_count, hot_count,
       is_rising, momentum_ratio,
       standout_count >= 2 AND hot_count >= 1 AS suggested_core
FROM v_creator_profile
```

**For display, compose the canonical ratio — do not redefine it.**
`gold_creator_performance` already defines `standout_rate` as
`standout_count / NULLIF(total_posts, 0)` (`marts.py:217-218`) and already joins
the canonical `follower_tier` (`marts.py:167-168`). Read it from that mart rather
than recomputing it, so the metric has one definition.

**Note:** `v_creator_metrics` carries `creator_id`, `total_posts`,
`standout_count`, `hot_count`, `avg_likes`, `max_likes` — no `creator_name`. Use
`v_creator_profile` (or join on `creator_id`). All thresholds in this story are
measured against `v_creator_profile` so the numbers are comparable.

This story produces the **suggestion only**. It must not write to `ops.sqlite`,
must not change what gets scraped, and must not alter any label.

## Acceptance criteria (binary)

1. **GIVEN** a creator with at least one post in `v_creator_profile`,
   **WHEN** the suggestion is queried,
   **THEN** that creator appears with exactly one row.
2. **GIVEN** a creator with zero standouts,
   **WHEN** the suggestion is queried,
   **THEN** `suggested_core` is false (not NULL, not absent).
3. **GIVEN** two creators with different standout/hot counts,
   **WHEN** both are queried,
   **THEN** `suggested_core` is consistent with the declared rule
   (`standout_count >= 2 AND hot_count >= 1`).
4. **GIVEN** a creator with 1 standout and 1 hot post,
   **WHEN** the suggestion is queried,
   **THEN** `suggested_core` is **false** — one standout is not sufficient.
5. **GIVEN** a creator with 2 standouts and **0** hot posts,
   **WHEN** the suggestion is queried,
   **THEN** `suggested_core` is **false** — the hot predicate is required.
6. **GIVEN** a creator with few total posts but 2 standouts and 1 hot post,
   **WHEN** the suggestion is queried,
   **THEN** `suggested_core` is **true** — the rule is count-based and must not
   be silently rate-normalized by `total_posts`.
7. **GIVEN** the suggestion is recomputed,
   **WHEN** outputs are compared with unchanged inputs,
   **THEN** every row is identical (deterministic).
8. **GIVEN** a creator with a `follower_tier` value,
   **WHEN** the suggestion is computed,
   **THEN** the value comes from the canonical four-bucket `follower_tier`
   (reused, not redefined) and `suggested_core` is **unchanged by it** — proving
   the axes are not conflated.
9. **GIVEN** the rule's constants (`2`, `1`),
   **WHEN** they are inspected,
   **THEN** they are named constants documented as **tunable** (owner: fine-tune
   later), not inline literals.

## Definition of done

- [ ] `suggested_core` exposed as a serving fact derived from
      `v_creator_profile`; catalogued in `schemas.py` if it becomes a table.
- [ ] Threshold constants named and documented as tunable (AC 9).
- [ ] `standout_rate`, if displayed, is **composed from
      `gold_creator_performance`** — NOT re-derived in a new view (single
      definition of the metric).
- [ ] `follower_tier` reused from `v_post_follower_context`; **no
      `follower_band`**, and no wider bands (AC 8).
- [ ] No write to `ops.sqlite`; no label mutation; proven by test inspection.
- [ ] Guard: derivable from the existing serving views alone (no new source of
      truth, no new aggregation).
- [ ] ACs 1–9 each have a test; ACs 4, 5 (both predicates required) and 6
      (count, not rate) are load-bearing.
- [ ] Cross-checked against `docs/architecture/pipelines/core.md` and the ADR
      set; discrepancies surfaced, not silently resolved.
- [ ] Reasoning trace + assumption log.

## Open questions

- Lifetime counts vs windowed (e.g. standouts in the last 90 days)? Lifetime is
  the default; a window would stop promoting dormant creators and is the first
  tuning knob if this proves too inclusive.
- Should the review surface also show `follower_tier` and `total_posts` as
  context for the owner's judgment? (Recommended: yes, display-only.)
