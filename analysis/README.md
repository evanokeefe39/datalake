# analysis/ — Recreatable EDA for the growth-strategy work

Scripts implementing **Epic R** of `tasks/plans/follower-observations-underperformer-eda.md`.
Everything here is read-only against the lake: nothing in `analysis/` writes to
`data/state.duckdb` or `data/ops.sqlite`.

## How to run

```bash
uv run python analysis/eda_follower_tier.py   # follower-tier stratification (Q3)
uv run python analysis/eda_cadence_timing.py  # posting cadence / timing (Q5)
uv run python analysis/eda_cta_education.py   # CTA / educational framing (Q6)
uv run python analysis/eda_creator_benchmark.py  # per-creator benchmark (Q7)
```

All five scripts accept the same flags:

| flag | default | meaning |
|---|---|---|
| `--db` | `$DATALAKE_DUCKDB` or `data/state.duckdb` | DuckDB path, opened **read-only** |
| `--out` | `analysis/output` | scratch directory for CSV + markdown output |

Requirements: `uv` environment of this repo (stdlib + `duckdb` + `polars`; no new
dependencies). Scripts must run AFTER the orchestrator materializes the DB
(`pipeline run` through the serving layer so all views exist).

## What each script reads and emits

### `eda_content_axis.py`

- **Reads:** `v_post_detail` (content attrs: `gold_topic/subtopic/domain/subdomain`,
  `content_type`, `format`, `style`, `admiralty`, `is_educational/actionable`,
  `has_engagement_bait`), `v_post_metrics` (`is_standout`), `v_engagement_outliers`
  semantics via `sigma_tier` on `v_post_metrics`, `ig_post_labels`
  (`label_version`, `is_provisional`).
- **Holds constant:** `label_version` = MAX in `ig_post_labels`; provisional vs
  day-7 label counts reported separately.
- **Emits:** `output/content_axis.md` + one CSV per axis
  (`content_axis__<column>.csv`) with a COMPARATIVE over-index table:
  `value | n_total | standout_n | standout_rate | underperf_n | underperf_rate
  | over_index | underperf_over_index`, where the global rates are computed
  from the same frame (`standout_n/labeled_posts`, `underperf_n/labeled_posts`)
  and `over_index = standout_rate / global_standout_rate`,
  `underperf_over_index = underperf_rate / global_underperf_rate`. Primary
  table lists values with n ≥ 10 sorted by over_index DESC; a capped long-tail
  section (top 15, 5 ≤ n < 10) keeps emerging labels visible; thin cells
  (n < 5) flagged. Each axis also gets explicit **Decision rows**:
  best-replicate candidates (n ≥ 10, over_index ≥ 1.25, underperf_over_index
  ≤ 1.0) and avoid candidates (n ≥ 10, underperf_over_index ≥ 1.25).

### `eda_follower_tier.py`

- **Reads:** everything above PLUS `silver_ig_profile_observations`. Computes the
  post→owner follower attribution DIRECTLY (nearest observation at-or-after the
  post timestamp; `owner_id` join with `owner_username` fallback) — it does NOT
  depend on any A2.2 attribution view.
- **Emits:** `output/follower_tier.md`, `follower_tier__segment_counts.csv`
  (tier-level over-index table, same schema as above),
  `follower_tier__content_axis.csv` (axis × tier over-index rows). Reports
  attribution coverage and flags cells with n < 5. Per axis × tier: primary
  table (n ≥ 10, over_index DESC), capped long tail (top 10), and
  best-replicate / avoid decision rows — over-index computed WITHIN each
  tier's attributed posts against the whole-population global rates.
- **States in its output:** follower GROWTH over time is not yet observable
  (sparse backfill, ~58 obs / ~50 owners / 6 files) — tiers are snapshots.

### `eda_cadence_timing.py` (Q5 — descriptive only)

- **Reads:** `v_post_detail` (`timestamp`), `dim_date` (`day_of_week`,
  `is_weekend` — joined via `v_post_detail.dim_date`), `v_post_metrics`
  (`is_standout`, `sigma_tier`), `ig_post_labels` (`label_version`).
- **Emits:** `output/eda_cadence_timing.md` +
  `eda_cadence_timing_day_of_week.csv` / `_hour_of_day.csv` / `_weekend.csv`
  — over-index tables in calendar/clock order (day of week) and over-index
  DESC order (hour of day, weekend), same schema as the content-axis tables.
- **States in its output:** descriptive only — no experiment, heavily
  confounded by creator activity level; timestamps carry the scraper's
  capture timezone, so hour-of-day is a relative prior, not an absolute
  schedule. Posts without a `dim_date` join are counted and shown as
  `(missing)`.

### `eda_cta_education.py` (Q6 — partially answerable)

- **Reads:** `v_post_detail` (`is_educational`, `is_actionable` — gold;
  `has_engagement_bait` — silver_ig_posts; `content_type` — gold),
  `v_post_metrics`, `ig_post_labels`.
- **Emits:** `output/eda_cta_education.md` + one CSV per attribute
  (`eda_cta_education_<attribute>.csv`) — boolean flags as
  `true`/`false`/`(missing)` rows, `content_type` over-index DESC; decision
  rows exclude `(missing)`.
- **States in its output:** column coverage on ALL posts (gold flags are
  ~88% populated); the `(missing)` segment over-indexes standout, so it is
  shown, never dropped. Honest data gap: the lake has NO CTA-type taxonomy
  (comment-bait / save-CTA / follow-CTA) and `has_engagement_bait` never
  fires in the current corpus (0 `true` rows) — the bait axis is reported
  as degenerate rather than fabricated.

### `eda_creator_benchmark.py` (Q7)

- **Reads:** `v_creator_outlier_rate` ⨝ `v_creator_underperformer_rate`
  (per-creator totals + segment counts, pooled across label versions),
  `v_post_detail` (population baseline).
- **Emits:** `output/eda_creator_benchmark.md` + `eda_creator_benchmark.csv`
  — `creator | creator_id | n_posts | standout_n | standout_rate |
  underperf_n | underperf_rate | over_index | underperf_over_index`.
  Primary table (n ≥ 10, over_index DESC), capped long tail (top 15,
  5 ≤ n < 10), thin cells flagged (517/624 creators have n < 5), and
  imitation-shortlist / underperformance-prone decision rows.
- **States in its output:** rates are unadjusted for follower tier (mechanical
  follower advantage, not skill) and pooled across label versions; survivorship
  caveat — a shortlist within the niche, not a market census.


## Determinism

- Read-only DuckDB connection; no writes anywhere.
- Every query ends in a deterministic `ORDER BY` (`post_id`, then value); every
  Polars aggregation is sorted before serialization.
- No timestamps, RNG, or wall-clock values in outputs: re-running against an
  unchanged DB produces byte-identical files.
- Outputs are a point-in-time snapshot of the DB — a re-materialized DB can
  legitimately change results.

## `output/` is scratch

`output/.gitignore` ignores everything in `analysis/output/`. Committed
artifacts are the scripts and this contract, never generated tables.
