---
id: US-EFAC-3
epic: E-ENRICH-FACETS
persona: P1
status: Open
---
# US-EFAC-3 — Universal visual-facet pass (single additive media call)

- **Epic:** E-ENRICH-FACETS
- **Status:** Open
- **Relates to:** E-ENRICH-SUMMARIES (same universal video call), E-ENRICH-ENGINE
- **Source:** `docs/enrichment-enhancement-design.md` §3

## Story
**As a** pipeline operator, **I want** one universal media call per post that
extracts the visual-necessary facets into an additive `growth_facets_json`
(own column/hash, prompt_versioned), **so that** cross-tab analytics work
without touching the gold schema and without re-running universal multimodal
for each new feature.

## Acceptance criteria (binary)
- AC1: One media call per post (media resolved once, File API URIs reused across
      passes), output `max_output_tokens=4096`, `MEDIA_RESOLUTION_LOW`.
- AC2: Output additive — new column/table + own `prompt_hash`; gold table and
      reserved keys untouched.
- AC3: Post-parse validation passes: valid JSON, no reserved keys, all required
      fields present; failures route to a terminal surface, never silent.
- AC4: `seam_violations()==[]` holds after wiring.
- AC5: Cost accounting printed at run start (est. tokens + $) — triage-free only
      if re-enriching a bounded, high-value set.

## Definition of done
- [ ] Pass runs over a sample and writes validated facets to a temp/isolated DB
      first (smoke), then production wiring is reviewed before enablement.
- [ ] Schema catalog + readiness green for the new column/table.
- [ ] Full-suite scoped tests pass; no regression to the gold path.

## Tests
- A crafted bad request (dead CDN URL) drops only that post and is dead-lettered,
      not the whole job.
- Adding a facet to the schema re-runs only the cheap text call, never the video
      input (regression guard on the single-pass contract).
