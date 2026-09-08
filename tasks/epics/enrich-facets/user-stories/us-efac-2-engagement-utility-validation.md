---
id: US-EFAC-2
epic: E-ENRICH-FACETS
persona: P3
status: Open
---
# US-EFAC-2 — Engagement-utility validation of facets

- **Epic:** E-ENRICH-FACETS
- **Status:** Open
- **Relates to:** E-ENRICH-LABELS (standout/underperformer labels as the
  criterion), E-SERVING-ANALYTICS (engagement views)
- **Source:** `docs/enrichment-enhancement-design.md` §4 (validity revision)

## Story
**As a** growth analyst, **I want** each facet validated as actually
discriminating standout/hot vs underperforming posts (per media type / niche),
**so that** we know which facets carry real engagement signal — replacing human
gold with an objective, free, lake-data criterion — WITHOUT auto-pruning facets
on thin evidence.

## Acceptance criteria (binary)
- AC1: Join V3 facet values (95-post `facet_menu` + caption/menu sets) to the
      lake's standout/hot + engagement fields (via `v_post_metrics` /
      `ig_post_labels` semantics, held on `label_version`).
- AC2: Report per-facet discrimination per media type (video/carousel/image) and
      per niche — does the facet separate high from low performers?
- AC3: Facets with zero variance OR zero discrimination are flagged **monitor**,
      NOT removed (keep-bias overrides any prune reading).
- AC4: Output is a committed artifact (report + numbers grounded in lake state),
      not an ad-hoc shell query.
- AC5 (keep-bias rationale): adding a facet to the schema is near-free (same
      universal call); re-adding a pruned one means a full, expensive re-enrich.
      So low/zero discrimination on thin evidence never prunes a facet — only a
      genuinely degenerate field (zero variance / single value, no descriptive
      use) is a prune candidate. Re-validate discrimination at larger n after
      the universal call ships (additive — no re-enrich needed to re-check).

## Definition of done
- [ ] Utility report artifact produced with honest coverage/thin-cell flags.
- [ ] Decision recorded per facet: keep / monitor / refine (no drop on thin
      evidence; only a truly degenerate field may drop), with the number shown.

## Tests
- Re-running the report over the same `label_version` is deterministic.
- Every reported discrimination number is reproducible from the referenced
  DuckDB view, not hard-coded.
