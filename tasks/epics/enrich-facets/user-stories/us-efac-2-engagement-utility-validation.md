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
**so that** facets that carry no engagement signal are dropped and only
decision-useful facets ship — replacing human gold with an objective,
free, lake-data criterion.

## Acceptance criteria (binary)
- AC1: Join V3 facet values (95-post `facet_menu` + caption/menu sets) to the
      lake's standout/hot + engagement fields (via `v_post_metrics` /
      `ig_post_labels` semantics, held on `label_version`).
- AC2: Report per-facet discrimination per media type (video/carousel/image) and
      per niche — does the facet separate high from low performers?
- AC3: Facets with zero variance OR zero discrimination are flagged for removal
      (matches the manifest-over-latent + drop-degenerate rule).
- AC4: Output is a committed artifact (report + numbers grounded in lake state),
      not an ad-hoc shell query.

## Definition of done
- [ ] Utility report artifact produced with honest coverage/thin-cell flags.
- [ ] Decision recorded per facet: keep / refine / drop, with the number shown.
- [ ] Anything dropped is documented in `tasks/lessons.md` as a spec gap if the
      reason is a schema issue, not just low signal.

## Tests
- Re-running the report over the same `label_version` is deterministic.
- Every reported discrimination number is reproducible from the referenced
  DuckDB view, not hard-coded.
