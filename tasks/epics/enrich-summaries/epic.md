# Epic E-ENRICH-SUMMARIES — Content summaries by form (visual + transcript)

- **Theme:** Richer media-grounded enrichment (new)
- **Owner:** dlc-worker
- **Depends on:** E-ENRICH-ENGINE, E-MEDIA
- **Relates to:** E-ENRICH-FACETS (ONE visual submit fans out to both visual
  tables — annotations + summaries; ONE text submit fans out to
  `silver_text_annotations` + `silver_text_summaries`), E-ENRICH-TRANSCRIPTS
  (transcript summary input: `silver_audio_transcripts`), E-SERVING-ANALYTICS
  (embeddings/search/qualitative use)

## Product intent — summaries per content form (audit 2026-09-09)

`content_summary` (in `silver_visual_summaries`) is the **OVERALL visual summary
for every content form**; `image_summaries_json` is the per-image layer,
carousels only:

| Content form | content_summary (overall) | image_summaries_json (per image) | transcript_summary (silver_text_summaries) |
| Video / reel | 2–3 sentences over the sampled frames | n/a | what is SAID (US-ESUM-3; rides the text call over caption+transcript) |
| Carousel (n>1) | **overall summary of the images** (NEW requirement — currently the prompt asks per-image only, `defs/enrichment/prompts.py:110-122`, so carousels have no overall) | one short sentence per image, index-aligned | **not applicable — audio-absent**, encoded via `silver_audio_transcripts.transcript_status = no_audio_source` (US-ETR-2), not a silent NULL |
| Single image | the individual summary (overall == individual) | n/a | not applicable — audio-absent (same encoding) |

Every media post carries a visual-first `content_summary` (video frames /
single image / carousel overall) and one summary per carousel image, produced
inside the same universal visual call the facets already pay for — so
embeddings/search and qualitative "what do winning posts show/say" get a
visual narrative with no second visual round-trip. Videos additionally carry a
`transcript_summary` (what is SAID) in `silver_text_summaries`, folded into the
cheap text call (US-ESUM-3); for image/carousel posts transcript summaries are
explicitly audio-absent, not skipped.
## Design decision (spike-backed, 2026-09-08)
Summaries are **folded into the single universal visual call**, NOT a separate
pass. This deliberately reverses triage-first because the visual input is
already spent on the facet core every media post — asking for a bounded
summary is near-free marginal output and eliminates a second visual
round-trip (the exact scope the user is cutting). Output cap 4096; visual
summary 2–3 sentences visual-first; one sentence per carousel image.

**Spike evidence** (`scripts/facet_summary_spike.py`, 22 posts × A/B_F/B_S):
facet fidelity under fold held (is_sponsored/text_overlay 100%, face_present/
sponsorship 95%, value_medium 86%); carousel index-alignment 11/11; A = 1
visual call vs B = 2. **Boundary:** the A-vs-B_S judge is self-referential,
caption-context (rates caption-plausibility, not true visual grounding) — so
visual *faithfulness* of the folded summary is the residual unmeasured risk.

### Unified async-ingest contract (audit 2026-09-09; reconciled 2026-09-10)
Harvesting summaries into bronze (verbatim) then silver (conformed + validated)
obeys the repo's ONE remote-async ingest
pattern (ledger → submit → poll-to-terminal → retrieve → idempotent MERGE
upsert with per-pass provenance and loud per-item failure) — see
`data/dev/dlc-enrichment-audit.md` §2. **One-submit-fans-out (settled
contract):** the visual submit lands its verbatim response in
`bronze_enrichment_raw` and fans out at conform to `silver_visual_annotations`
AND `silver_visual_summaries`; the text submit fans out to
`silver_text_annotations` AND `silver_text_summaries` — NOT one submit per table.
The transcript summary rides the text workload on that same pattern; its
executor (qwen text default, Gemini pluggable) is an E-ENRICH-ENGINE decision,
not this epic's; the provider lives in metadata, never in the table name.

## Source of truth
`docs/enrichment-enhancement-design.md` §6, `scripts/facet_summary_spike.py`,
`data/facet_summary_spike.duckdb`.

## Epic DoD (draft)
- [ ] `content_summary` (overall, ALL forms incl. carousel overall) +
      `image_summaries_json` present in the universal visual call output,
      validated (parse + carousel length == n_media), written to
      `silver_visual_summaries` via the visual job's fan-out.
- [ ] `transcript_summary` present for video posts in `silver_text_summaries`
      via the text call (US-ESUM-3); image/carousel rows carry the
      audio-absent status from `silver_audio_transcripts.transcript_status`,
      never a silent NULL.
- [ ] Re-test visual faithfulness ONLY if a summary becomes decision-grade
      (then spot-check or summary-only on that subset); not for embeddings use.
