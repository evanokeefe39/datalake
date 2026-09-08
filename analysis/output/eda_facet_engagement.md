# US-EFAC-2 — Engagement-utility validation of V3 growth facets

Does each V3 facet value separate standout/hot posts from
underperformers? Grounded in the 95-post `facet_menu` V3 run joined to
lake engagement state. Reproduce with
`uv run python analysis/eda_facet_engagement.py`.

## Method

- Source: `data/facet_menu.duckdb`, `variant='V3'`, **run 0**,
  `95` rows, all `ok`; run 1 (`95` rows)
  is used only to count inter-run value flips (agreement itself was
  validated in the design doc §4, so it is not re-measured here).
- Outcome (`label_version=1`): **high** =
  `ig_post_labels.enrich_decision='standout'`; **low** =
  `v_post_metrics.sigma_tier='-1σ'`; everything else is classed `other`
  (kept for coverage, not for the discrimination rates).
- Media type is **derived** (silver has no `media_type` column):
  `video_view_count>0` → video; `media_count>1` → carousel; else image.
- `over_index` = value's high-rate ÷ global high-rate over classed
  posts (n≥10 cells are the primary signal; smaller cells are
  marked `*` and never drive a verdict).
- Verdict rule: **keep** = a value with n≥10 at over_index ≥ 1.5
  (either direction); **refine** = signal between 1.25 and 1.5, or all
  cells n<10; **drop** = zero variance or max over_index < 1.25.

> **Decision — keep-bias (supersedes the drop labels below).** The classed
> signal here is thin (8 high / 26 low of 94), so no facet is pruned on this
> evidence. Adding a facet to the schema is near-free (same universal video
> call); re-adding a pruned one later means a full, expensive re-enrich.
> Therefore the per-facet `drop` labels below mean **"monitor — low current
> discrimination," NOT "remove."** Default is KEEP. The only prune candidates
> are genuinely degenerate fields (zero variance / single value with no
> descriptive use), and even those are cheap to keep. Re-validate discrimination
> against a larger-n facet dataset after the universal video call ships
> (additive — no re-enrich needed to re-check).

## Coverage (honesty first)

| segment | n | high | low | other |
|---|---|---|---|---|
| pooled | 94 | 8 | 26 | 60 |
| video | 51 | 5 | 19 | 27 |
| carousel | 29 | 2 | 6 | 21 |
| image | 14 | 1 | 1 | 12 |

- Label decisions on the facet posts: `control`=23, `skip`=63, `standout`=8.
- Unparseable V3 result_json rows (run 0): 1.
- Niche split: gold covers 94/95 facet posts but
  yields 93 distinct subtopics (~1 post
  per niche) — **a per-niche facet table would be all thin cells**, so
  it is not computed. Niche-level facet utility needs a larger corpus
  or coarser niche grouping (e.g. `gold_topic`, still ~1/subtopic).
- Classed posts per media type: video 5 high / 19 low, carousel
  2 high / 6 low, image 1 high / 1 low — per-media rates are
  indicative only; nothing per-media-type clears a keep-verdict bar.

## Per-facet discrimination (pooled)

### `face_present` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| true | 79 | 7 | 0.089 | 24 | 0.304 | 0.38 | 0.40 | 1.84 |
| false | 15 | 1 | 0.067 | 2 | 0.133 | 0.28 | 0.17 | 1.80 |

### `audience_named` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 85 | 7 | 0.082 | 25 | 0.294 | 0.35 | 0.38 | 2.01 |
| true * | 9 | 1 | 0.111 | 1 | 0.111 | 0.47 | 0.15 | 0.04 |

### `is_sponsored` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 85 | 8 | 0.094 | 22 | 0.259 | 0.40 | 0.34 | 2.07 |
| true * | 9 | 0 | 0.000 | 4 | 0.444 | 0.00 | 0.58 | -1.02 |

### `has_brand_tag` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 69 | 8 | 0.116 | 15 | 0.217 | 0.49 | 0.28 | 2.83 |
| true | 25 | 0 | 0.000 | 11 | 0.440 | 0.00 | 0.58 | -0.73 |

### `original_audio` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| true | 56 | 6 | 0.107 | 18 | 0.321 | 0.46 | 0.42 | 3.09 |
| false | 38 | 2 | 0.053 | 8 | 0.211 | 0.22 | 0.28 | -0.45 |

### `value_depth` — refine (thin-cell signal only)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| practical | 67 | 4 | 0.060 | 21 | 0.313 | 0.25 | 0.41 | 1.68 |
| shallow | 19 | 1 | 0.053 | 3 | 0.158 | 0.22 | 0.21 | -0.20 |
| deep * | 8 | 3 | 0.375 | 2 | 0.250 | 1.59 | 0.33 | 5.54 |

### `hook_type` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| bold_claim | 25 | 6 | 0.240 | 5 | 0.200 | 1.02 | 0.26 | 8.07 |
| direct_value_promise | 15 | 0 | 0.000 | 2 | 0.133 | 0.00 | 0.17 | -0.46 |
| contrarian_take | 13 | 2 | 0.154 | 5 | 0.385 | 0.65 | 0.50 | -0.44 |
| curiosity_gap | 10 | 0 | 0.000 | 3 | 0.300 | 0.00 | 0.39 | -0.55 |
| question * | 9 | 0 | 0.000 | 6 | 0.667 | 0.00 | 0.87 | -0.81 |
| personal_story * | 7 | 0 | 0.000 | 2 | 0.286 | 0.00 | 0.37 | -0.62 |
| other * | 6 | 0 | 0.000 | 1 | 0.167 | 0.00 | 0.22 | -0.66 |
| shocking_stat * | 4 | 0 | 0.000 | 2 | 0.500 | 0.00 | 0.65 | -0.59 |
| comparison * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.86 |
| other:announcement * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| point_of_view * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.59 |
| quote * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| social_proof * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### `format_structure` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| curated_collection | 28 | 2 | 0.071 | 6 | 0.214 | 0.30 | 0.28 | 1.79 |
| tutorial | 22 | 5 | 0.227 | 3 | 0.136 | 0.97 | 0.18 | 6.20 |
| personal_story | 11 | 0 | 0.000 | 3 | 0.273 | 0.00 | 0.36 | -0.73 |
| behind_the_scenes * | 8 | 0 | 0.000 | 4 | 0.500 | 0.00 | 0.65 | -0.74 |
| news_update * | 8 | 1 | 0.125 | 3 | 0.375 | 0.53 | 0.49 | -0.24 |
| comparison * | 4 | 0 | 0.000 | 1 | 0.250 | 0.00 | 0.33 | -0.28 |
| case_study * | 3 | 0 | 0.000 | 1 | 0.333 | 0.00 | 0.44 | -0.77 |
| demo * | 3 | 0 | 0.000 | 2 | 0.667 | 0.00 | 0.87 | -0.96 |
| day_in_life * | 2 | 0 | 0.000 | 2 | 1.000 | 0.00 | 1.31 | -1.03 |
| other * | 2 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| review * | 2 | 0 | 0.000 | 1 | 0.500 | 0.00 | 0.65 | -1.09 |
| other:brand_teaser * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### `value_medium` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| text_overlay_slides | 27 | 2 | 0.074 | 4 | 0.148 | 0.31 | 0.19 | 0.66 |
| talking_head | 23 | 2 | 0.087 | 9 | 0.391 | 0.37 | 0.51 | -0.24 |
| broll_voiceover | 17 | 1 | 0.059 | 6 | 0.353 | 0.25 | 0.46 | 3.08 |
| screen_recording | 14 | 2 | 0.143 | 4 | 0.286 | 0.61 | 0.37 | 7.35 |
| static_illustration_photo | 13 | 1 | 0.077 | 3 | 0.231 | 0.33 | 0.30 | -0.08 |

### `text_overlay` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| burned_in_captions | 43 | 5 | 0.116 | 16 | 0.372 | 0.49 | 0.49 | 3.24 |
| dense_on_screen_text | 38 | 3 | 0.079 | 8 | 0.211 | 0.34 | 0.28 | 0.33 |
| none * | 8 | 0 | 0.000 | 2 | 0.250 | 0.00 | 0.33 | -0.58 |
| title_only * | 5 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | 0.93 |

### `cta_type` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| comment | 44 | 5 | 0.114 | 12 | 0.273 | 0.48 | 0.36 | 2.02 |
| none | 17 | 1 | 0.059 | 3 | 0.176 | 0.25 | 0.23 | -0.45 |
| link_in_bio | 15 | 0 | 0.000 | 7 | 0.467 | 0.00 | 0.61 | -0.73 |
| follow * | 9 | 1 | 0.111 | 3 | 0.333 | 0.47 | 0.44 | 7.76 |
| save * | 5 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.56 |
| dm * | 3 | 1 | 0.333 | 1 | 0.333 | 1.42 | 0.44 | 3.35 |
| question * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | 1.17 |

### `sponsorship_signal` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| (missing) | 85 | 8 | 0.094 | 22 | 0.259 | 0.40 | 0.34 | 2.07 |
| disclosed #ad * | 6 | 0 | 0.000 | 4 | 0.667 | 0.00 | 0.87 | -1.02 |
| product placement/name-drop * | 3 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.99 |

### `brand_safety_profanity` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 91 | 7 | 0.077 | 25 | 0.275 | 0.33 | 0.36 | 1.88 |
| true * | 3 | 1 | 0.333 | 1 | 0.333 | 1.42 | 0.44 | 0.20 |

### `brand_safety_sensitive_adjacency` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 80 | 8 | 0.100 | 23 | 0.287 | 0.43 | 0.38 | 2.22 |
| true | 14 | 0 | 0.000 | 3 | 0.214 | 0.00 | 0.28 | -0.81 |

### `any_brand_logos` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 53 | 4 | 0.075 | 11 | 0.208 | 0.32 | 0.27 | 0.37 |
| true | 41 | 4 | 0.098 | 15 | 0.366 | 0.41 | 0.48 | 3.42 |

### `hashtag_count_bucket` — drop (no discrimination)

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| 5+ | 40 | 5 | 0.125 | 8 | 0.200 | 0.53 | 0.26 | 3.17 |
| 0 | 36 | 2 | 0.056 | 11 | 0.306 | 0.24 | 0.40 | 1.57 |
| 3-4 | 10 | 1 | 0.100 | 4 | 0.400 | 0.43 | 0.52 | -0.57 |
| 1-2 * | 8 | 0 | 0.000 | 3 | 0.375 | 0.00 | 0.49 | -0.90 |

*(n < 10 marked `*`.)*

## Inter-run stability of each facet (run 0 vs run 1)

| facet | flipped posts / 94 |
|---|---|
| face_present | 0 |
| audience_named | 0 |
| is_sponsored | 2 |
| has_brand_tag | 12 |
| original_audio | 9 |
| value_depth | 2 |
| hook_type | 13 |
| format_structure | 11 |
| value_medium | 13 |
| text_overlay | 5 |
| cta_type | 2 |
| sponsorship_signal | 2 |
| brand_safety_profanity | 0 |
| brand_safety_sensitive_adjacency | 6 |
| any_brand_logos | 8 |
| hashtag_count_bucket | 0 |

## Per-media-type tables

Only the facets with any classable signal per media type are shown;
full CSVs carry every facet × value row.
### video (n=51)

### video — `face_present`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| true | 47 | 5 | 0.106 | 18 | 0.383 | 0.51 | 0.48 | 2.85 |
| false * | 4 | 0 | 0.000 | 1 | 0.250 | 0.00 | 0.32 | -0.47 |

### video — `audience_named`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 48 | 5 | 0.104 | 19 | 0.396 | 0.50 | 0.50 | 2.81 |
| true * | 3 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | 0.15 |

### video — `is_sponsored`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 45 | 5 | 0.111 | 15 | 0.333 | 0.53 | 0.42 | 3.15 |
| true * | 6 | 0 | 0.000 | 4 | 0.667 | 0.00 | 0.84 | -1.02 |

### video — `has_brand_tag`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 34 | 5 | 0.147 | 12 | 0.353 | 0.71 | 0.45 | 4.23 |
| true | 17 | 0 | 0.000 | 7 | 0.412 | 0.00 | 0.52 | -0.70 |

### video — `original_audio`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| true | 40 | 5 | 0.125 | 16 | 0.400 | 0.60 | 0.51 | 3.45 |
| false | 11 | 0 | 0.000 | 3 | 0.273 | 0.00 | 0.34 | -0.53 |

### video — `value_depth`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| practical | 41 | 3 | 0.073 | 17 | 0.415 | 0.35 | 0.52 | 2.80 |
| deep * | 6 | 2 | 0.333 | 1 | 0.167 | 1.60 | 0.21 | 3.23 |
| shallow * | 4 | 0 | 0.000 | 1 | 0.250 | 0.00 | 0.32 | -0.49 |

### video — `hook_type`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| bold_claim | 13 | 4 | 0.308 | 3 | 0.231 | 1.48 | 0.29 | 11.82 |
| contrarian_take * | 8 | 1 | 0.125 | 4 | 0.500 | 0.60 | 0.63 | -0.54 |
| direct_value_promise * | 8 | 0 | 0.000 | 1 | 0.125 | 0.00 | 0.16 | -0.36 |
| question * | 8 | 0 | 0.000 | 6 | 0.750 | 0.00 | 0.95 | -0.81 |
| curiosity_gap * | 7 | 0 | 0.000 | 3 | 0.429 | 0.00 | 0.54 | -0.56 |
| shocking_stat * | 3 | 0 | 0.000 | 1 | 0.333 | 0.00 | 0.42 | -0.44 |
| personal_story * | 2 | 0 | 0.000 | 1 | 0.500 | 0.00 | 0.63 | -0.77 |
| other * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| point_of_view * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.59 |

### video — `format_structure`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| tutorial | 18 | 4 | 0.222 | 3 | 0.167 | 1.07 | 0.21 | 5.44 |
| curated_collection * | 7 | 1 | 0.143 | 2 | 0.286 | 0.69 | 0.36 | 7.66 |
| news_update * | 6 | 0 | 0.000 | 3 | 0.500 | 0.00 | 0.63 | -0.81 |
| personal_story * | 6 | 0 | 0.000 | 3 | 0.500 | 0.00 | 0.63 | -0.69 |
| behind_the_scenes * | 5 | 0 | 0.000 | 3 | 0.600 | 0.00 | 0.76 | -0.97 |
| demo * | 3 | 0 | 0.000 | 2 | 0.667 | 0.00 | 0.84 | -0.96 |
| case_study * | 2 | 0 | 0.000 | 1 | 0.500 | 0.00 | 0.63 | -0.96 |
| comparison * | 2 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | 0.40 |
| day_in_life * | 1 | 0 | 0.000 | 1 | 1.000 | 0.00 | 1.26 | -1.05 |
| review * | 1 | 0 | 0.000 | 1 | 1.000 | 0.00 | 1.26 | -1.09 |

### video — `value_medium`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| talking_head | 23 | 2 | 0.087 | 9 | 0.391 | 0.42 | 0.49 | -0.24 |
| broll_voiceover | 16 | 1 | 0.062 | 6 | 0.375 | 0.30 | 0.47 | 3.34 |
| screen_recording | 12 | 2 | 0.167 | 4 | 0.333 | 0.80 | 0.42 | 7.35 |

### video — `text_overlay`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| burned_in_captions | 42 | 5 | 0.119 | 16 | 0.381 | 0.57 | 0.48 | 3.34 |
| dense_on_screen_text * | 8 | 0 | 0.000 | 3 | 0.375 | 0.00 | 0.47 | -0.63 |
| title_only * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | 0.93 |

### video — `cta_type`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| comment | 25 | 2 | 0.080 | 9 | 0.360 | 0.38 | 0.45 | 2.44 |
| link_in_bio | 10 | 0 | 0.000 | 6 | 0.600 | 0.00 | 0.76 | -0.65 |
| follow * | 9 | 1 | 0.111 | 3 | 0.333 | 0.53 | 0.42 | 7.76 |
| none * | 4 | 1 | 0.250 | 1 | 0.250 | 1.20 | 0.32 | -0.18 |
| dm * | 2 | 1 | 0.500 | 0 | 0.000 | 2.40 | 0.00 | 7.82 |
| question * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | 1.17 |

### video — `sponsorship_signal`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| (missing) | 45 | 5 | 0.111 | 15 | 0.333 | 0.53 | 0.42 | 3.15 |
| disclosed #ad * | 5 | 0 | 0.000 | 4 | 0.800 | 0.00 | 1.01 | -1.02 |
| product placement/name-drop * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.99 |

### video — `brand_safety_profanity`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 49 | 4 | 0.082 | 18 | 0.367 | 0.39 | 0.46 | 2.75 |
| true * | 2 | 1 | 0.500 | 1 | 0.500 | 2.40 | 0.63 | 0.20 |

### video — `brand_safety_sensitive_adjacency`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 43 | 5 | 0.116 | 16 | 0.372 | 0.56 | 0.47 | 3.30 |
| true * | 8 | 0 | 0.000 | 3 | 0.375 | 0.00 | 0.47 | -0.77 |

### video — `any_brand_logos`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| true | 29 | 4 | 0.138 | 11 | 0.379 | 0.66 | 0.48 | 4.72 |
| false | 22 | 1 | 0.045 | 8 | 0.364 | 0.22 | 0.46 | -0.21 |

### video — `hashtag_count_bucket`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| 5+ | 23 | 3 | 0.130 | 4 | 0.174 | 0.63 | 0.22 | 4.32 |
| 0 | 19 | 1 | 0.053 | 11 | 0.579 | 0.25 | 0.73 | 2.28 |
| 3-4 * | 6 | 1 | 0.167 | 3 | 0.500 | 0.80 | 0.63 | -0.54 |
| 1-2 * | 3 | 0 | 0.000 | 1 | 0.333 | 0.00 | 0.42 | -0.82 |

### carousel (n=29)

### carousel — `face_present`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| true | 21 | 1 | 0.048 | 5 | 0.238 | 0.19 | 0.32 | -0.65 |
| false * | 8 | 1 | 0.125 | 1 | 0.125 | 0.50 | 0.17 | 2.65 |

### carousel — `audience_named`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 24 | 1 | 0.042 | 5 | 0.208 | 0.17 | 0.28 | 0.55 |
| true * | 5 | 1 | 0.200 | 1 | 0.200 | 0.80 | 0.27 | -0.05 |

### carousel — `is_sponsored`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 28 | 2 | 0.071 | 6 | 0.214 | 0.29 | 0.29 | 0.45 |
| true * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### carousel — `has_brand_tag`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 25 | 2 | 0.080 | 3 | 0.120 | 0.32 | 0.16 | 0.74 |
| true * | 4 | 0 | 0.000 | 3 | 0.750 | 0.00 | 1.00 | -0.98 |

### carousel — `original_audio`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 19 | 1 | 0.053 | 4 | 0.211 | 0.21 | 0.28 | -0.63 |
| true | 10 | 1 | 0.100 | 2 | 0.200 | 0.40 | 0.27 | 2.62 |

### carousel — `value_depth`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| practical | 22 | 1 | 0.045 | 4 | 0.182 | 0.18 | 0.24 | -0.59 |
| shallow * | 6 | 0 | 0.000 | 2 | 0.333 | 0.00 | 0.44 | -0.98 |
| deep * | 1 | 1 | 1.000 | 0 | 0.000 | 4.00 | 0.00 | 25.95 |

### carousel — `hook_type`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| bold_claim * | 9 | 1 | 0.111 | 2 | 0.222 | 0.44 | 0.30 | 2.52 |
| contrarian_take * | 5 | 1 | 0.200 | 1 | 0.200 | 0.80 | 0.27 | -0.23 |
| personal_story * | 4 | 0 | 0.000 | 1 | 0.250 | 0.00 | 0.33 | -0.82 |
| curiosity_gap * | 3 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.55 |
| direct_value_promise * | 3 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.44 |
| comparison * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.86 |
| other * | 1 | 0 | 0.000 | 1 | 1.000 | 0.00 | 1.33 | -1.01 |
| question * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.83 |
| quote * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| shocking_stat * | 1 | 0 | 0.000 | 1 | 1.000 | 0.00 | 1.33 | -1.06 |

### carousel — `format_structure`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| curated_collection | 18 | 1 | 0.056 | 4 | 0.222 | 0.22 | 0.30 | -0.62 |
| personal_story * | 3 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.98 |
| tutorial * | 3 | 1 | 0.333 | 0 | 0.000 | 1.33 | 0.00 | 12.96 |
| behind_the_scenes * | 1 | 0 | 0.000 | 1 | 1.000 | 0.00 | 1.33 | -1.07 |
| case_study * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.41 |
| comparison * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.92 |
| day_in_life * | 1 | 0 | 0.000 | 1 | 1.000 | 0.00 | 1.33 | -1.01 |
| news_update * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.75 |

### carousel — `value_medium`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| text_overlay_slides | 25 | 2 | 0.080 | 4 | 0.160 | 0.32 | 0.21 | 0.66 |
| static_illustration_photo * | 4 | 0 | 0.000 | 2 | 0.500 | 0.00 | 0.67 | -0.98 |

### carousel — `text_overlay`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| dense_on_screen_text | 26 | 2 | 0.077 | 4 | 0.154 | 0.31 | 0.21 | 0.59 |
| none * | 3 | 0 | 0.000 | 2 | 0.667 | 0.00 | 0.89 | -1.04 |

### carousel — `cta_type`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| comment | 14 | 2 | 0.143 | 2 | 0.143 | 0.57 | 0.19 | 1.53 |
| none * | 6 | 0 | 0.000 | 2 | 0.333 | 0.00 | 0.44 | -0.88 |
| save * | 5 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.56 |
| link_in_bio * | 3 | 0 | 0.000 | 1 | 0.333 | 0.00 | 0.44 | -1.12 |
| dm * | 1 | 0 | 0.000 | 1 | 1.000 | 0.00 | 1.33 | -1.12 |

### carousel — `sponsorship_signal`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| (missing) | 28 | 2 | 0.071 | 6 | 0.214 | 0.29 | 0.29 | 0.45 |
| product placement/name-drop * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### carousel — `brand_safety_profanity`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 28 | 2 | 0.071 | 6 | 0.214 | 0.29 | 0.29 | 0.45 |
| true * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### carousel — `brand_safety_sensitive_adjacency`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 25 | 2 | 0.080 | 6 | 0.240 | 0.32 | 0.32 | 0.58 |
| true * | 4 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.98 |

### carousel — `any_brand_logos`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 20 | 2 | 0.100 | 2 | 0.100 | 0.40 | 0.13 | 1.19 |
| true * | 9 | 0 | 0.000 | 4 | 0.444 | 0.00 | 0.59 | -0.78 |

### carousel — `hashtag_count_bucket`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| 5+ | 16 | 2 | 0.125 | 4 | 0.250 | 0.50 | 0.33 | 1.35 |
| 0 * | 9 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.72 |
| 1-2 * | 3 | 0 | 0.000 | 2 | 0.667 | 0.00 | 0.89 | -0.98 |
| 3-4 * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.83 |

### image (n=14)

### image — `face_present`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| true | 11 | 1 | 0.091 | 1 | 0.091 | 0.18 | 0.18 | 0.33 |
| false * | 3 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### image — `audience_named`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 13 | 1 | 0.077 | 1 | 0.077 | 0.15 | 0.15 | 0.33 |
| true * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### image — `is_sponsored`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 12 | 1 | 0.083 | 1 | 0.083 | 0.17 | 0.17 | 0.33 |
| true * | 2 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### image — `has_brand_tag`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 10 | 1 | 0.100 | 0 | 0.000 | 0.20 | 0.00 | 0.86 |
| true * | 4 | 0 | 0.000 | 1 | 0.250 | 0.00 | 0.50 | -0.47 |

### image — `original_audio`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false * | 8 | 1 | 0.125 | 1 | 0.125 | 0.25 | 0.25 | 1.33 |
| true * | 6 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.33 |

### image — `value_depth`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| shallow * | 9 | 1 | 0.111 | 0 | 0.000 | 0.22 | 0.00 | 1.14 |
| practical * | 4 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.76 |
| deep * | 1 | 0 | 0.000 | 1 | 1.000 | 0.00 | 2.00 | -1.01 |

### image — `value_medium`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| static_illustration_photo * | 9 | 1 | 0.111 | 1 | 0.111 | 0.22 | 0.22 | 0.60 |
| screen_recording * | 2 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| text_overlay_slides * | 2 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| broll_voiceover * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.76 |

### image — `cta_type`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| none * | 7 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | -0.12 |
| comment * | 5 | 1 | 0.200 | 1 | 0.200 | 0.40 | 0.40 | 0.63 |
| link_in_bio * | 2 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### image — `sponsorship_signal`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| (missing) | 12 | 1 | 0.083 | 1 | 0.083 | 0.17 | 0.17 | 0.33 |
| disclosed #ad * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| product placement/name-drop * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### image — `brand_safety_sensitive_adjacency`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 12 | 1 | 0.083 | 1 | 0.083 | 0.17 | 0.17 | 0.33 |
| true * | 2 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### image — `any_brand_logos`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| false | 11 | 1 | 0.091 | 1 | 0.091 | 0.18 | 0.18 | 0.33 |
| true * | 3 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

### image — `hashtag_count_bucket`

| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |
|---|---|---|---|---|---|---|---|---|
| 0 * | 8 | 1 | 0.125 | 0 | 0.000 | 0.25 | 0.00 | 1.68 |
| 3-4 * | 3 | 0 | 0.000 | 1 | 0.333 | 0.00 | 0.67 | -0.57 |
| 1-2 * | 2 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |
| 5+ * | 1 | 0 | 0.000 | 0 | 0.000 | 0.00 | 0.00 | — |

## Free-text keys (not facets)

V3 JSON also carries `hook_content`, `claimed_results`,
`replicable_tactic`, `evidence` — free text with no enum, so they
cannot be validated by this screen and are excluded. They serve
summaries/search, not decision-gated facets (design §6).

## Provenance & caveats

- Script: `analysis/eda_facet_engagement.py` (deterministic,
  read-only `duckdb.connect(..., read_only=True)`; `--seed` accepted
  but unused — no sampling).
- Data: `data/facet_menu.duckdb` V3 run 0/1 (95 posts);
  `data/state.duckdb` `silver_ig_posts`, `ig_post_labels`
  (label_version=1), `v_post_metrics`,
  `v_post_detail`. No writes to any database.
- n=95 with 9 high / 26 low classed posts is a thin base: pooled
  cells with n<10 are indicative, not conclusive. Verdicts marked
  'refine (thin cells)' should be re-tested after the facet pass
  ships lake-wide.
- Standout labels are label-pass output on the current label
  version, not human gold; re-materializing the lake can legitimately
  change these numbers.

