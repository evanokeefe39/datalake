# Creator Growth Analysis — Reference

Reference for the analytical comparison / audit workstream: what questions we
want answered, what data we have vs what we need, how to build a valid baseline
cohort, and the candidate sources for follower-count history.

Issue: `ISSUES.md` #14. This document is the design context; the issue is the
tracking shell.

---

## 1. Research questions

Q1. For creators successful *now*, how did they start?
Q2. What content and formats did they post — reels/TikToks vs carousels vs single image?
Q3. Were they multi-platform initially? If so, how did each platform perform early on?
Q4. What was their initial posting cadence, and how consistent were they?
Q5. How long did it take to go 0 → 100 → 1,000 → 10,000 followers (etc.)?
Q6. What course corrections did they make along the journey?
Q7. Did they use CTAs from the beginning? If not, when did they start?
Q8. What elements may have hindered growth?
Q9. Do we need a sample of creators that never made it, as a baseline?
Q10. For a given domain/topic, what should we replicate from successful creators vs not?
Q11. End-state: for any creator, produce an audit of their channel compared to
     others in their domain.

### Current answerability (as of 2026-08-31)

| Q | Answerable today? | Why |
|---|---|---|
| Q1 | Partially | "How they started" is mostly outside scrape reach (see §2) |
| Q2 | Yes, within window | `media_count > 1` ≈ carousel; video + `video_play/view_count` ≈ reel |
| Q3 | No | No TikTok/YouTube source yet; no cross-platform identity or follower series |
| Q4 | Yes, within window | Post timestamps → cadence over scraped window |
| Q5 | No | Needs follower-count **time series**, which we don't store (§2) |
| Q6 | Partially | Drift detection (domain/format/cadence/CTA change) within held window |
| Q7 | Yes, within window | Caption classification (note `has_engagement_bait` ≠ CTA) |
| Q8 | Inference-heavy | Partly from post trajectories / engagement outliers; mostly audit-style |
| Q9 | Yes — required | Survivorship-bias control group (§3) |
| Q10 | Needs taxonomy | Requires consistent creator-level domain labels + baseline cohort |
| Q11 | No | Needs follower series + cohort + domain taxonomy + benchmarks |

---

## 2. Data we have vs data we need

### What supports this today (`schemas.py`)
- **Per-post** (`silver_ig_posts`): post timestamp, caption, `hashtags`,
  `has_engagement_bait`, `media_files`, `media_count`, likes/comments/views.
  → cadence, format mix, content signals over the scraped window.
- **Per-post trajectory** (`silver_ig_post_observations`): likes/comments/views
  at observation points → which posts took off fast.
- **Profile snapshot** (`silver_ig_profiles`): `followers_count` — **single
  point in time, PK overwrites on each scrape**. No history.
- **Gold content analysis** (`gold_analyses` / `result_json`): per-post domain,
  educational/actionable classification → content-type drift over time.

### Critical gaps
1. **Follower-count time series (the #1 gap).** `silver_ig_profiles` holds one
   snapshot per `owner_id`. Q5 and the entire growth-velocity side of Q11 are
   unanswerable without a series: a `profile_observations` table (mirroring
   `post_observations`) + a scheduled profile re-scrape. Sources in §4.
2. **Full / early post history.** IG scrapers surface recent posts (bounded by
   `results_limit`). The first year of a successful account is usually
   unreachable → early cadence/format/origin domain are typically gone. Q1/Q6
   are partially outside scraping's reach; may need dedicated "oldest posts"
   scrapes (depth-limited) or external knowledge (biography, `external_url`).
3. **Multi-platform coverage + identity.** `creators`/`profiles` split and
   `domain` dispatch already model one creator owning N profiles. But no
   TikTok/YouTube source yet, and no cross-platform matching + per-platform
   follower series → Q3 is empty until then.
4. **Domain / sub-domain taxonomy.** Gold classifies per-post domain/sub-domain. A consistent
   *creator-level* domain label (standardized, not free-form from Gemini) is the
   bucketing key for Q10/Q11.
5. **Baseline cohort.** No control group exists (§3).

---

## 3. Baseline cohort design (Q9, Q10, Q11)

### Purpose
The baseline must be a **fair comparison group**: matched to the success arm on
everything that predicts growth *except* the behaviors under study, so outcome
differences are attributable to behavior, not to who the creators are.

### Fit-for-purpose criteria
- **Matched on the confounding set, blind to the studied behavior:** domain,
  platform, start-era (algorithm regimes change), region/language, account age.
- **Outcome defined a priori + control strata defined.** "Never made it" splits
  into *persisted-but-stalled* vs *abandoned/quit* — different questions, not
  pooled blindly.
- **Controls selected by a principled frame, not opportunism.** The success arm
  is, by construction, scraping-worthy. Controls must be drawn from a defined
  universe (hashtag/domain-keyword profiles, follower-network neighbors,
  "similar accounts" off exemplars) then matched — otherwise you get a second
  success cohort.
- **Comparable longitudinal data completeness across arms** (measurement
  requirement applied identically to both arms).
- **Outcome spread in the baseline** — not all floor-dwellers, or there's
  nothing to model time-to-threshold against.

### Structure: matched ladder, not strict 1-to-1 pairs
- One successful "anchor" + several comparison creators at **different outcome
  levels** — fail, flat, slow growth, medium — all matched on the same
  domain/platform/era.
- Matching is on *characteristics*, not outcome. The spread is what lets us
  answer "how much does X predict growth," not just "did winners do X."
- Roughly **one success per several comparisons** across the outcome ladder.

### Sample size (orders of magnitude, not exact N)
- **Hundreds, not thousands or tens of thousands.** A few hundred matched
  accounts with outcome spread is enough to see large, general patterns
  reliably. Thousands add redundant information for the broad questions.
- Analyze at **post/observation granularity with mixed-effects models**, not
  just creator level — repeated measurements give power from post count, so
  modest creator N works.
- **~20+ matched per domain-platform cell** is a defensible floor for
  trajectory-shape and large-effect (d≈0.9) work; fewer than ~15/arm is hard
  to defend even for trajectories.
- **Time-to-event is the weakest at low N**: if most controls never reach a
  threshold they're censored, so you need enough *events*, not enough accounts.
- 20 IG-only beats 20 split across platforms for IG questions (don't fragment
  the design); multi-platform questions are a **separate creator cohort**
  design.

### Practical shape
- Per-domain-platform-era **matched case-control cells**; start with 1–2
  high-value domains rather than 10 thin ones; grow by adding cells over time.

---

## 4. Follower-count history: candidate sources

Goal: a first-class **time series** keyed by (profile, platform, observed_at),
merging past backfill + present/future observations.

### A. Own scheduled re-scrape (present → future)
- What it is: extend the existing profile-scrape pattern into a recurring
  `profile_observations` series. Accurate, free, matches current architecture.
- Limitation: only ever provides *future* history from when we start.
- **Baseline for the series.** Combine with a past backfill below.

### B. Wayback Machine CDX + snapshot extraction (past backfill) — PROMISING, NEEDS TESTING
- What it is: free; CDX API enumerates archived captures of a profile URL;
  fetch snapshots and extract the follower number (regex / json-ld) into a
  time series.
- Pros: free; genuinely contains historical follower counts for public IG
  profiles at various points; already used in research to reconstruct growth.
- Cons: **sparse and uneven coverage** (only when the crawler hit the page);
  no private accounts; IG UI changes over the years break older extractions.
- **Action required:** a smoke test on a known exemplar — pull its capture
  history, extract counts, and validate against a couple of known values before
  trusting it as the backfill source.

### C. SocialBlade (paid API / Apify scrapers)
- Growth-history charts for IG, TikTok, YouTube, Twitch. Official paid API plus
  Apify scrapers returning subscribers, daily history, ranks, grades.
- Free tier is website-only; API is paid. Daily-ish resolution.

### D. Influencer analytics platforms (HypeAuditor, Modash, etc.)
- Large creator databases (Modash ~200M, HypeAuditor ~137M) with current +
  growth/fraud signals, already segmented by domain/audience.
- Best for the *comparison/audit* question (domain benchmarking out of the box),
  but paid (~$120–300+/mo). Justified only for the curated cohort, not a census.

### Recommended hybrid
- **Own re-scrape** for present→future (accurate, free).
- **Wayback CDX** for free past backfill on the exemplar/curated cohort —
  pending the smoke test above.
- **One paid tracker (SocialBlade API, or HypeAuditor/Modash if domain-audit
  signals are wanted)** for the curated cohort only — bounded cost because we
  deliberately do **not** scrape everyone.
- **Minimum-confidence rule:** sparse Wayback snapshots and third-party
  estimates carry uncertainty; don't over-weight a single noisy point when
  computing time-to-100/1k/10k.

---

## 5. Definitions still to pin down (escalate before building)
- **"Success" metric:** followers vs engagement rate vs growth velocity
  (velocity normalized per domain is closest to "what worked").
- **Domain / sub-domain taxonomy:** creator-level labels standardized from
  per-post gold classifications (not Gemini free-form).
- **Failure definition:** persisted-but-stalled vs abandoned — separate strata.
- **First domain to start with:** determines the first cohort cells to build.

---

## 6. Suggested sequence
1. Follower-count time series (cheapest, unblocks Q5 + most of Q11).
2. Wayback CDX smoke test → confirm/deny as past-backfill source.
3. Define success metric + domain/sub-domain taxonomy.
4. Build first cohort cell(s) (matched ladder, hundreds total, spread of
   outcomes).
5. Multi-platform coverage (TikTok/YouTube sources + cross-platform identity).

---

## 7. Expert panel gap analysis (2026-08-31)

Panel: Data Architect (schema/pipeline), Analytical/ML (value + method),
Cost/Feasibility (Apify/Gemini dollars). Converged on the same priorities.

### Enabling changes (in leverage order) — the "build once, answer many" set

| ID | Change | Enables |
|---|---|---|
| GAP-1 | `silver_ig_profile_observations` (owner_id, observed_at, followers/follows/posts_count, source_dataset) + scheduled profile re-scrape. Keep `silver_ig_profiles` as latest-state snapshot (dim_profile depends on it); add the series as a NEW table. Clone the existing post-observations + watermark pattern. | Q5, Q8, Q9, Q10, Q11 |
| GAP-2 | Early-history (oldest-posts) Apify backfill into `silver_ig_posts` with distinct `source_dataset='ig_early_backfill'` (must NOT mix with recent-window in cadence). One depth-limited pass per curated creator, bounded by cohort not census. | Q1, Q4, Q6, part of Q7 |
| GAP-3 | `cohort_labels` + matched-ladder baseline recruitment. Labels on `ops.sqlite creators` (person-level, platform-agnostic), NOT silver_ig_profiles (account-level, overwritten). | Q9, Q10, Q11, strengthens Q8 |
| GAP-4 | `gold_creator_domain` — standardized creator-level domain rollup (majority/weighted vote over per-post gold_analyses, constrained to a taxonomy enum, not Gemini free-form). | Q10, Q11 |
| GAP-5 | `gold_analyses` result_json extension: structured CTA fields (`cta_present`, `cta_type`) (+ optional explicit format). Rides existing Gemini enrichment. | Q7, Q2 robustness |
| GAP-6 | Second-platform sources + cross-platform identity. Explicitly deferred; ops schema already anticipates it (creators/profiles/domain). | Q3 |

### Value, method, and cohort dependency per question

| Q | Analytical value | Method | Requires baseline? |
|---|---|---|---|
| Q1 How they started | HIGH — launch recipe | Early-window descriptive + time-to-first-viral survival | Counterfactual half yes; descriptive half from success arm |
| Q2 Formats | HIGH — controllable lever | Per-post mixed effects, within-creator FE | Not strictly (interaction nice-to-have) |
| Q3 Multi-platform early | MEDIUM strategic / LOW stat near-term | Panel / diff-in-diff after sources exist | Irrelevant until data exists |
| Q4 Cadence + consistency | HIGH — controllable | Recurring-event / gap-time models | Yes — cross-arm |
| Q5 0→100→1k→10k | HIGH if well-answered | Multi-state interval-censored time-to-threshold | Yes (is the ladder) |
| Q6 Course corrections | MEDIUM — case studies, weak decision rule | Change-point detection (PELT/BOCPD) + interruption analysis | Success arm for shifts; baseline for "did pivot precede inflection" |
| Q7 CTA timing | MEDIUM-HIGH — binary lever | Same mixed-effects as Q2; time-to-adoption; evaluate vs follower growth, NOT likes | Treatment effect yes; timing from success arm |
| Q8 What hindered growth | MEDIUM — "avoid X" | Within-creator event studies, negative controls | HARD requirement — impossible from success arm alone |
| Q9 Baseline cohort | HIGH (enabler) | Outcome ladder defined up front; domain/era/start-window matching | It IS the cohort |
| Q10 Replicate within domain | HIGH if heterogeneity real | Multilevel heterogeneity of effects, shrinkage | Both arms + domain diversity — statistically fragile at low per-domain N |
| Q11 Per-creator audit | HIGH — most reusable output | Descriptive percentile benchmarking + drift (PSI/KS) | Any domain sample suffices, NOT the ladder — cheapest to satisfy |

### Merge / drop recommendation (panel consensus)
- **Merge into clusters (one acquisition serves all):**
  - *Velocity cluster*: Q5 + Q11-velocity → GAP-1.
  - *Cohort cluster*: Q9 + Q10 → GAP-3 + GAP-4.
  - *Longitudinal behavior cluster*: Q1 + Q4 + Q6 → GAP-2 (build early_history once, answer all three from it — do NOT build three pipelines).
  - *Format cluster*: Q2 + Q7 → GAP-5 (format inference + CTA detection on same assets).
- **Drop / deprioritize:**
  - **Q3** — blocked on unbuilt TikTok/YouTube sources; keep identity keys in ops.profiles now (already modeled), defer the question until a second source adapter exists. Near-duplicate of Q1 with an extra platform dimension.
  - **Q11 as an independent effort** — overlaps Q5 (audit) + Q10 (benchmark); re-scope as a VIEW over Q5+Q10 answers, not separate acquisition.
  - **Q9 standalone recruitment** — the matched-ladder design already embeds the baseline; a separate Q9 acquisition pass double-spends. Keep the cohort-frame, drop the independent pass.

### Survivorship-bias risks (apply from day one)
- Success-arm-only descriptives may describe universal behavior, not growth cause → run identical descriptives on the baseline arm.
- Flat creators are hard to discover (search/explore favors visible accounts) → enforce domain-matched sampling, harvest via hashtag back-pages.
- Follower snapshot overwrite retrojects current followers as if always high → use timestamp-relative analysis windows.
- Creators prune failed posts; missing early posts are not missing-at-random → treat "gap with no posts" as censored, not absence.
- Wayback snapshots skew to notable accounts → Q5 reconstruction inherits selection.

### Statistically fragile at low N
- Q10 domain-stratified effects (per-domain cells of 10-30) — use partial pooling, refuse fine-grained domain claims.
- Q5 milestone times — interval-censored, expect wide CIs; only coarse month-scale bands credible.
- Q6 change-point events — few per creator (1-3); treat as case-study generation, not hypothesis testing.
- Q8 hindrance-specific effects — rare events, multiplicity risk; pre-specify a short hindrance list.
- Cap any per-format×domain×era interaction at 2-way (3-way cells collapse fast).

---

## 8. Cost feasibility (verified Apify/Gemini pricing)

Assumes curated cohort (~60 success + ~150 baseline), NOT a census. **All questions
CHEAP except Q3 (needs new connectors) and retrospective Q5 (feasibility-limited,
not dollar-limited).**

### Per-asset costs
- **Full post history:** ~$30-125 one-time (18k-46k results @ $1.50-2.70/1k). Comments are the multiplier — sample top ~20-50 per post if needed (+$25-100). IG rate limits make >1,000-post depth unreliable; budget ~1.5× for retries.
- **Follower history (the feasibility gap):**
  - Own re-scrape: prospective only, ~$3-5/mo for daily tracking of ~60 creators. No retrospective value.
  - SocialBlade: ~$0.21-2.50 one-off for the cohort, but only 15-31d daily OR 90-365d coarse deltas — **0→100→1k timelines for accounts already past them are NOT recoverable**.
  - Wayback CDX: free, sparse (<30% hit for small creators), brittle per snapshot era — opportunistic bonus, not a plan; smoke test mandatory.
  - **Verdict:** retrospective Q5 is largely unbuyable. Reframe to: start daily re-scrape NOW (forward-instrumented) + SocialBlade coarse deltas + Wayback luck. Budget ~$5 total, expect partial answers.
- **Profile metadata:** ~$0.30-0.60 one-off (210 accounts). Negligible; grab linked-account fields free to seed Q3 identity.
- **Gemini enrichment:** text-only is ~$0 (free tier 500 RPD = 36-90 days) or ~$5-20 paid. **Video is the dominant cost driver by 10-100× all scraping** (18M-70M tokens one-off for full reel history, or $50-300+). Mitigate: enrich video only for a stratified subsample (top/bottom ~10 posts/creator, $10-60); classify format/CTA from caption+metadata for the rest.

### Biggest cost drivers
1. Gemini VIDEO enrichment — cap via stratified subsample.
2. IG pagination depth / retries (~1.5× soft multiplier).
3. Unsampled comment scraping.

### Free vs paid strategy
- Apify free $5/mo covers profiles + SocialBlade + baseline scrape each month; do the one-time bulk post-history burst on paid (~$30-125 once).
- Gemini free tier: text-only enrichment over 2-3 months, reserve RPD for new data, never spend free RPD on video.
- No real build cost anywhere except TikTok/YouTube connectors (Q3) — that's engineering time, not dollars.

### Bottom line
Whole acquisition for Q1-Q11 minus Q5's retrospective gap: **one-time ~$40-160 + steady-state ~$5-10/mo**, Gemini text on free tier, video subsampled ($10-60 optional). The recurring ~$5-10/mo operating cost IS the whole system's steady-state bill.
