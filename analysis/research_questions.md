# Research Questions → Data Verdict Map

Epic R / US-R1 companion to `tasks/plans/follower-observations-underperformer-eda.md`.
Every question below is tagged with one verdict:

- **data-answerable-now** — the current DuckDB surfaces (as of the A1–A4 build) contain the fields
  needed; an EDA script under `analysis/` addresses it.
- **data-gap-blocked** — the field/mechanism does not exist yet (or is too sparse); the gap and the
  enabler are named. No amount of clever SQL fixes a missing observation.
- **web-required** — the evidence lives outside the lake (external research, platform docs, competitor
  scraping strategy decisions); flag for the human.

---

## Q1. What content should a small faceless dev/ai/data-eng account post to grow — what should we replicate?

**Verdict: data-answerable-now** (with caveats) — `analysis/eda_content_axis.py`.

Data supports: standout posts' distributions over `topic` / `subtopic` / `content_type` / `format` /
`style` / `admiralty` / `domain` vs the corpus average, held at a single `label_version`.
Decision supported: content-portfolio choices for OUR account (what to make more of).

Caveats recorded inline by the script:
- Label maturity is mixed: rows are split `is_provisional` (day-0-ish) vs day-7 judgments; the script
  reports both but treats day-7 (non-provisional) as the trusted subset.
- Single-label-version filter = results only speak for one prompt generation; re-run after any label
  prompt change to re-base.
- `format` labels derive from text-only signals (caption/hashtags/media metadata), not visual
  inspection — treat format claims as provisional.

## Q2. What content bombs relative to a creator's own baseline — what should we avoid?

**Verdict: data-answerable-now** — `analysis/eda_content_axis.py` (underperformer segment) +
`v_underperformer_posts` / `v_engagement_outliers` with the `-1σ/-2σ/-3σ` magnitude split.

Decision supported: explicit avoid-list (topics/formats that reliably underperform the same creator's
trailing Tukey baseline — a genuinely self-relative signal, not vanity-metric).

Caveats: underperformers are the sparsest segment (baseline spread must be > 0 and z ≤ −1); thin cells
are flagged, not silently aggregated.

## Q3. Does strategy differ by follower stage (0–100 / 100–1k / 1k–10k / 10k–100k / 100k+)?

**Verdict: data-gap-blocked (partially answerable as a snapshot)** — `analysis/eda_follower_tier.py`.

What IS answerable: stratify the Q1/Q2 axis by the owner's follower level around the post time,
computed directly from `silver_ig_profile_observations` (nearest at-or-after the post timestamp, with
`owner_username` fallback). Coverage and thin cells are reported, never hidden.

What is NOT answerable: **follower GROWTH over time.** The backfill recovers ~58 observations over
~50 owners across 6 files — most owners have exactly ONE observation. There is no 0→100→1k trajectory
for anyone, so the *mechanics of crossing thresholds* (what changed when an account went from small to
mid) remain **web-required** until forward scrapes accumulate a real series. The script states this in
its output; do not over-read tier tables as causal.

## Q4. What does a "failed" small account look like — is there a failed-creator baseline we should study?

**Verdict: data-gap-blocked.** We observe only accounts that were worth scraping (survivorship); the
corpus has no deliberate sample of dead/abandoned accounts, and `silver_ig_profiles` rows are not
linked to any activity/abandonment signal. A useful version would need a new (web or scrape) sampling
decision — flagged for the human, not automatable in this pass.

## Q5. What posting cadence / timing works in this niche?

**Verdict: data-answerable-now (descriptive only)** — `v_post_detail` timestamps + `dim_date`
(`is_weekend`, `day_of_week`) support descriptive cadence/timing tables for standouts vs baseline.
Decision supported: posting schedule priors. NOT answerable causally (no experiment, heavy confounding
by creator activity level — `v_creator_metrics` activity rates differ wildly).

## Q6. Do CTAs / engagement bait / educational framing move outcomes?

**Verdict: data-answerable-now (partially)** — `silver_ig_posts.has_engagement_bait`,
`is_educational`, `is_actionable` from gold analyses are on `v_post_detail`; the content-axis script
includes admiralty/education splits. Decision supported: whether "educational" and "actionable"
labels correlate with standout (or underperformer) status within one label_version.

## Q7. Which creators in the niche have the best standout/underperformer rates — who to imitate?

**Verdict: data-answerable-now** — `v_creator_outlier_rate`, `v_creator_underperformer_rate`,
`v_creator_quality` (gated). Note the rollups pool across label versions; per-version recomputation is
in the content-axis script. Decision supported: shortlist of imitation targets.

## Q8. How has OUR candidate niche's content mix evolved over time?

**Verdict: data-gap-blocked (mostly).** `v_quality_trend` gives weekly aggregates, but gold coverage is
recent and label_version non-stationarity means week-over-week "topic mix" shifts may be prompt artifacts,
not real shifts. Honest verdict: descriptive trends only; any strategy conclusion needs the caveat.

## Q9. What growth tactics work on Instagram in 2025/26 for faceless tech accounts (reels weighting,
   SEO captions, collabs, trial reels)?

**Verdict: web-required.** Platform-mechanics knowledge (distribution changes, trial reels, keyword
SEO) is not in the lake and changes faster than our scrape cadence. Human web research; the EDA
outputs supply the "what worked historically in THIS corpus" half that web research cannot.

## Q10. How big is the opportunity — how do niche accounts' like counts compare across follower tiers?

**Verdict: data-gap-blocked.** We only observe OUR scraped corpus (biased sample, unknown selection
rule). Any market-size or benchmark-per-tier claim needs external data. Web-required for benchmarks.

---

## Assumptions & Blindspots (recorded honestly)

1. **Survivorship bias.** Every account in the lake was scraped because it looked worth scraping.
   Failed accounts that look like our successes are absent; "what works" is conditioned on "survived".
2. **Engagement-as-proxy.** Likes/comments/views stand in for strategy success. Reach, saves, shares,
   follows-per-post — the numbers that actually gate follower growth — are NOT collected by the
   scraper. A post can underperform on likes yet drive follows; we cannot see that.
3. **Label-version non-stationarity.** `ig_post_labels.baseline_*` and gold enrichment are products of
   specific prompt generations. The content-axis script pins a single `label_version` for internal
   validity, which means results do NOT automatically extend to a new labeling pass; cross-version
   comparisons are not meaningful.
4. **Sparse follower data.** Backfill yields ~58 observations / ~50 owners / 6 files; most owners have
   a single observation, so follower tier is a SNAPSHOT attribute, not a trajectory. Fabricated-0
   protection exists upstream (observations are gated on the source genuinely carrying
   `followersCount`), but tiers derived from one observation inherit that observation's timing error.
5. **Text-only historical format labels.** Gold `format`/`style`/`content_type` were inferred from
   captions/metadata, not frames. Historical format claims (e.g. "carousel vs reel") may misclassify;
   treat format-axis findings as hypotheses to verify visually.
6. **Small-n everywhere.** The whole corpus is a few hundred labeled posts. Every percentage in the
   outputs carries wide uncertainty; the scripts flag cells below n=5 instead of rounding them away.
7. **Single-writer, single-snapshot.** Scripts read `data/state.duckdb` read-only at run time; results
   are deterministic for a given DB file, but the DB itself evolves — outputs are point-in-time.
