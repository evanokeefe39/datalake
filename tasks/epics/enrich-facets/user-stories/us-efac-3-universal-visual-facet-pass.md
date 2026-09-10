---
id: US-EFAC-3
epic: E-ENRICH-FACETS
persona: P1
status: Open
---
# US-EFAC-3 — Universal visual-facet pass (single additive media call)

- **Epic:** E-ENRICH-FACETS
- **Status:** Open
- **Relates to:** E-ENRICH-SUMMARIES (ONE visual submit fans out at harvest to
  `silver_visual_annotations` AND `silver_visual_summaries` — same request, not two
  submits), E-ENRICH-ENGINE (backend = qwen batch service, ADR-0009)
- **Source:** `docs/architecture/enrichment-design-v1-superseded.md` §3
- **Backend note (2026-09-10, reconciled to the settled contract):** media
  transport = **client-side ffmpeg frame-sampling** → image parts on the qwen
  service (the pass is "visual" — images AND video frames — not "video").
  Output lands in `silver_visual_annotations`; schema + additive semantics
  unchanged.

## Story
**As a** pipeline operator, **I want** one universal visual call per post that
extracts the visual-necessary facets into `silver_visual_annotations`
(own prompt_hash + metadata, schema_versioned), **so that** cross-tab analytics
work without touching the silver schema and without re-running the multimodal
call for each new feature.

## Acceptance criteria (binary)
- AC1: One visual pass per post (media resolved once — native images + ffmpeg
      frames for reels), sent as image parts to the qwen batch service
      (US-EENG-1 backend). The submit is ONE job; at harvest it fans out to
      `silver_visual_annotations` AND `silver_visual_summaries` (US-ESUM-1) — never
      one submit per table.
- AC2: Output additive — the new `silver_visual_annotations` table keyed
      `(post_id, platform)` with its own `prompt_hash`; existing silver tables
      and reserved classification keys untouched.
- AC3: Post-parse validation passes: valid JSON, no reserved keys, all required
      fields present; enum definitions come from the versioned schema registry
      (`schema_version`), NOT per-row `_bound` flags; failures route to a
      terminal surface, never silent.
- AC4: `seam_violations()==[]` holds after wiring.
- AC5: Cost accounting printed at run start (est. tokens + $) — triage-free only
      if re-enriching a bounded, high-value set.
- AC6: The visual pass records ITS OWN provenance (`prompt_hash` + `model` +
      `schema_version` + `run_id`) on `silver_visual_annotations` — per-pass
      provenance (epic audit P0-4); the text pass
      (`silver_text_annotations`) may not overwrite it.

## Definition of done
- [ ] Pass runs over a sample and writes validated facets to a temp/isolated DB
      first (smoke), then production wiring is reviewed before enablement.
- [ ] Schema catalog + readiness green for `silver_visual_annotations`.
- [ ] Scoped tests pass; no regression to the silver path.

## Tests
- A crafted bad request (dead CDN URL) drops only that post and is dead-lettered,
      not the whole job.
- Adding a facet to the schema re-runs only the cheap text call
      (`silver_text_annotations`), never the visual input (regression guard on
      the single-pass contract).

