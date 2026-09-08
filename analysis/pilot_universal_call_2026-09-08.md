# Universal video→Gemini call — bounded pilot report (US-EFAC-3 + US-ESUM-1)

Date: 2026-09-08 · Branch: `feat/facet-universal-call` · Driver: `scripts/enrich_facets_pilot.py` · Model: `gemini-3.5-flash-lite`, `MEDIA_RESOLUTION_LOW`, `max_output_tokens=4096`

## Method

- 30-item pilot cap (1 External-Integration-Gate smoke + 28 additional media-bearing posts sampled from `silver_ig_posts` via `sample_posts(seed=42)`, balanced video/carousel).
- One universal call per post: visual facets + content summary + folded per-image summaries in a single response; media routed through the existing scrape-time byte cache → File API path (`_resolve_media_for_call`); 429 taxonomy with exponential backoff per item; failures dead-letter into the pilot DB.
- Resume semantics: the driver now skips post_ids already in `data/facets_pilot.duckdb`, so the smoke row is never re-paid or double-counted.
- Scratch DB only (`data/facets_pilot.duckdb`); prod `state.duckdb` untouched; `ops.sqlite` touched only through the additive, idempotent media-cache path.

## Spend & wall time (usage-based, exact)

| Metric | Value |
|---|---|
| Items processed (distinct) | 29 (26 ok w/ visual facets, 3 dead-lettered) |
| Gemini cost, 29-item pilot | **$0.02441** |
| Gemini cost incl. smoke (30-item cap) | **$0.02490** |
| Wall time (sum of per-item elapsed) | **274.3 s ≈ 4.6 min** (269.5 s pilot + 4.8 s smoke) |
| Tokens | 222,866 prompt / 6,526 candidates (incl. smoke: 218,546+4,320 / 6,394+132) |
| Per-item cost (mean, ok items) | ≈ $0.00092 — well under the ~$0.002 design §7 estimate |
| Model / prompt_hash | `gemini-3.5-flash-lite` / `a9c26ea360ebea78` |

## Coverage & outcomes

| media_type | ok | dead |
|---|---|---|
| video | 14 | 0 |
| carousel | 12 | 3 |

Dead letters (all carousel, all `HTTP Error 403: Forbidden` on CDN media fetch after 5 attempts — expired/geo-blocked Instagram CDN URLs, the known media-expiry mode, correctly terminal not retried to waste): `3975109049644486605`, `3704015139327367167`, `3934874316873659538`.

## Schema / contract verification

- Every stored visual-facet JSON (26/26) passes `validate_visual_facets` (re-validated from stored rows post-run, not just at parse time).
- Facet keys observed: exactly `brand_logos, face_present, on_screen_claim, text_overlay_present, value_medium` — the visual-core lock. No text-layer or brand_safety fields emitted.
- `content_summary` and `image_summaries` are stored in their own columns and never appear inside the facet JSON (verified across all rows).
- 12 rows carried folded per-image summaries (carousels); videos carry a single `content_summary`.

## Representative outputs

**Video** `3944679501543145356` (the smoke post):
- facets: `{"brand_logos": ["OpenAI"], "face_present": true, "on_screen_claim": false, "text_overlay_present": true, "value_medium": "talking_head and screenshare"}`
- content_summary: "A creator wearing glasses and a green shirt speaks into a microphone while demonstrating three free AI certification courses from OpenAI and Anthropic on a computer screen. Each certification topic is highlighted with on-screen text overlays as the speaker discusses their value."

**Video** `3858123157218243494`:
- facets: `{"brand_logos": [], "face_present": true, "on_screen_claim": false, "text_overlay_present": true, "value_medium": "talking_head"}`
- content_summary: "A woman speaks directly to the camera from inside a vehicle while text overlays appear above her. She gesticulates with her hands while discussing decision-making and advice."

**Carousel** `3972470401632850804` (folded image summaries):
- facets: `{"brand_logos": ["The North Face"], "face_present": true, "on_screen_claim": true, "text_overlay_present": true, "value_medium": "slideshow_carousel"}`
- image_summaries (per-slide, truncated): `[{"index": 1, "summary": "A woman rests her chin on her hand with text overlay about rating nursing school side hustles."}, {"index": 2, "summary": "A woman hangs from monkey bars at an indoor gym setting with text rating gymnastics assisting."}, …]`

## Mismatches found & fixed

- Resume gap: the pilot driver re-selected (and would have re-paid for) the already-processed smoke post on re-run. Fixed in `scripts/enrich_facets_pilot.py` by excluding post_ids already present in the pilot DB. No prompt/schema mismatch surfaced; prompt unchanged (`prompt_hash` stable across all 30 items).

## Honest caveats

- Cost figures are Tier-1 flash-lite list-price estimates computed from response usage (`_estimate_cost`), not billed amounts.
- 3/15 carousels failed on expired CDN URLs — consistent with the ~20% media-expiry dead-letter rate seen on prior live runs; the byte-cache-at-scrape-time fix (AGENTS.md item 2) is the structural remedy.
