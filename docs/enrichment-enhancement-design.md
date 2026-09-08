# Enrichment enhancement design: facets + transcripts + video/carousel summaries

Date: 2026-09-07. Consolidated from the facet-design work (see `tasks/plans/facet-list-experiment-design.md`) plus this session's transcript and summary additions. Companion: `docs/refactor-research/migration-batch-native-enrichment.md` (the current batch-native baseline this builds on).

## 1. Executive summary

The current enrichment is a single batch-native Gemini pass (ADR-0007/0008) that classifies each post (educational/actionable/admiralty/domain/topic/content_type/format) and is text/caption-only at scale (multimodal only in interactive runs). This design *enhances* enrichment into a small set of complementary passes that extract far richer, media-grounded signal:

- **Universal video→Gemini call** (media posts, ONCE per post): visual-necessary facets + bounded `content_summary` + per-image carousel summaries in a single call — the expensive video input is paid once and never re-sent (§6).
- **Transcript capture**: extract audio from the already-cached video bytes (ffmpeg) → ASR (faster-whisper, local, $0) → text; fully independent of Gemini (§5).
- **Text-derivable facets** (caption + transcript): a cheap, re-runnable text call — the thing that iterates freely as the schema evolves without re-paying video (§7).

Everything is strictly additive (own column/hash), modality-agnostic (a facet reads whatever channel carries its evidence), and validated by downstream engagement-utility rather than human gold. Cost for the universal video call is input-dominated (~$9–18 once); transcripts are ~$0 (local compute); summaries add only modest output tokens because they reuse the already-paid video input — so new features never re-run universal multimodal, they iterate on the cheap text call.

## 2. Current state (measured)

| Item | Value |
|---|---|
| Corpus | silver 10,038; gold 9,576 (single `instagram` domain); raw 12,834 |
| Media-bearing posts | ~8,849 (of 10,038) |
| Media mix | video ~6,101 / carousel ~2,174 / image ~476 (of 8,751 caption-bearing) |
| Cached media bytes | 27,748 rows / 55.36 GB (video 49.86 GB / **7,029 mp4**; image 5.5 GB / 20,719) |
| Cached video avg duration | ~35 s (probed 15.6–47 s); **all 7,029 cached mp4s carry an AAC audio stream** |
| Media type | videos and carousel images cached on disk; File-API URIs expire ~48 h → re-enrich re-uploads unless in GCS |
| Gold | caption-derived (batch text-only); multimodal only in interactive |
| Batch jobs | ~95 requests/job processed in ~4–8 min (observed); Tier-1 Flash-Lite, in-flight token caps |

## 3. Target architecture — evidence bundle + passes

Each post produces an **evidence bundle** that any pass may read:
- caption + hashtags (already in silver)
- content media (already cached; precedence videoUrl → carousel images → displayUrl)
- **ASR transcript** (new; see §5)
- (optional) keyframe OCR text (see §7)

**Passes** (all additive columns/tables; own `prompt_hash`; structural exclusion of reserved keys):

1. **Gold pass** (existing, unchanged): text-derived classification.
2. **Universal video→Gemini call** (new, media posts, ONCE per post): visual-necessary facets (§4 → `growth_facets_json`) + bounded `content_summary` + per-image carousel summaries (§6) in ONE call at 4096 output — the video input is paid once, then never re-sent.
3. **Text facet call** (new): text-derivable facets (§5 list) over caption + transcript — cheap, re-runnable on schema change; never touches video.
4. **Transcript capture** (new, upstream): §5 — independent of Gemini, $0.

Facet model (corrected during design): facets are **modality-agnostic** — a facet is judged across every channel that can carry its evidence (caption, transcript, on-screen text, imagery). Facets differ only in which channels are *necessary* (face_present needs imagery; sponsorship can be satisfied by text). This is the response to the naive "text layer vs visual layer" split, which was wrong (hooks, profanity, CTA, brand-safety are cross-modal).

## 4. Growth-facet schema (V3, winner)

Empirically selected. Across 95 posts × 4 presentations (V0 open-hints / V1 enum / V2 codebook / V3 enum+codebook) × 2 runs, measured by Gwet AC1:

| Field | V3 AC1 | note |
|---|---|---|
| audience_named | 1.00 | bool |
| face_present | 1.00 | bool, visual-necessary |
| value_depth | 0.95 | ordinal shallow/practical/deep |
| text_overlay / cta_type | 0.91 / 0.97 | cross-modal |
| hook_type | 0.84 | open-with-`other`, directional-grade, not decision-grade |
| format_structure / value_medium | 0.86 / 0.82 | value_medium enum list likely too coarse — tweak |
| is_sponsored | 0.88 | audit-capable w/ codebook rules; expect recall>precision |

Degenerate fields culled by value-distribution screen: `originality` (2 values), 3-pt `visual_quality` (88% one bucket), `hook_present` gate (95% true), standalone `profanity` (0/41 → folded into brand-safety), `sensitive_adjacency` (vague grab-bag → decomposed). `competitor_logo` (relative, undefined reference) → reframed to objective `brand_logos`.

**Brand-safety = explicit enumerable set** (modality-agnostic, brand-selectable): `brand_safety_profanity`, `brand_safety_sexualized_content`, `brand_safety_political`, `brand_safety_medical_claims`, `brand_safety_financial_guarantees`, `brand_safety_violence_trauma`. Each bool from any channel; a sponsor picks its own risk set. (Replaces the under-specified `sensitive_adjacency`.)

**Visual-necessary** (imagery required): `face_present`, what's shown (product/scene — left to embeddings), `brand_logos` (visible marks), `value_medium`/format, on-screen-only claims.

**Prompt = V3** (enum-constrained categoricals + codebook decision-rules + boundary exemplars), which beat both ingredients alone. Validity = V3 inter-run agreement gate + downstream engagement-utility (no human gold — user decision; see plan). **Keep-bias decision (2026-09-08):** engagement-utility *informs* the schema but does NOT auto-prune facets. Adding a facet to the universal call is near-free (same video input, marginal output); re-adding a pruned one means a full, expensive re-enrich. So low/zero discrimination on thin evidence flags a facet **monitor** (default KEEP); only a genuinely degenerate field (zero variance / single value, no descriptive use) is a prune candidate. Re-validate at larger n after the universal call ships (additive).

## 5. Transcript capture (new)

Rationale: a large share of what we label is *text-in-media* (spoken words, on-screen words, disclosures). Text is cheaper, more reliable, and verifiable than VLM perception of audio.

**Method — no `audioUrl` dependency:** the scrape-time byte cache already persists the full video, so extract the audio track from the cached mp4 with ffmpeg (`-vn`, decode to 16k wav), then ASR. Confirmed: all 7,029 cached mp4s have AAC; ffmpeg 8.1.2 available. This sidesteps the expiring-CDN problem entirely and makes transcription a run-anytime job, not a scrape-time race.

**ASR choice:** local faster-whisper (`small.en`/`base.en`, int8) — $0/min, MIT, offline. Cloud STT (if ever needed) is ~$0.003–0.007/min **batch** vs ~$0.016/min **online** (not chosen either way: needs a key + sends audio off-machine; batch only makes sense if the audio already lives in GCS). NVIDIA Parakeet is more accurate + ~49× faster but needs an NVIDIA GPU (not on the laptop or cx33).

Transcripts are stored as text (additive column), discarded audio. Spoken audio is the full mix (voiceover + music); music-only clips yield near-empty transcripts cheaply (no pre-filter needed).

**Facets better served by transcript** (evidence channel, not assignment): verbatim `hook_content`, `hook_type` (text-inclusive classification), disclosed `is_sponsored`/`sponsorship_signal` ("use code", "sponsored by"), `profanity`, spoken `claimed_results`, `cta_type`, `audience_named`, `value_depth`, `replicable_tactic`, topical brand-safety. Visual facets still read imagery.

## 6. Summaries — folded into the single universal video call

Whole-video `content_summary` + **per-image carousel summaries** (`image_summaries: [{index, summary}]`, aligned to sent media order; validate length == #images to catch mis-alignment). A summary is free-text — not a facet (no reliability gate) — and serves embeddings/search, qualitative "what do winning posts show/say", and grounding.

**Design decision — summaries ride the one universal video call, NOT a separate pass.** This deliberately **reverses the triage-first stance** (AGENTS: deep-pass only high-value items): that principle applies when video input cost is *incurred per call*, but here the visual-facet core already sends every media post's video once, so the input is spent regardless — asking for a bounded summary alongside is near-free marginal *output* on the already-paid call, and folding it in eliminates a second video round-trip (the exact scope the user is cutting). Trade-off: summary output now rides all ~8,849 media posts (not a ~15% subset), raising universal output modestly — still small next to the paid video input, and it costs no extra upload or encode wall-clock. The universal call runs at **4096 output** to fit visual facets + summaries. Caps: video summary 2–3 sentences visual-first; one sentence per carousel image.

**Spike evidence (2026-09-08, `scripts/facet_summary_spike.py`, 22 posts = 11 video + 11 carousel × 3 modes A/B_F/B_S, media identical across modes):**
- **Facet fidelity under fold** (A folded vs B_F facets-only, per-field agreement) — measures task-*interference*, Gemini-vs-itself: is_sponsored 100%, text_overlay_present 100%, face_present 95%, sponsorship_signal 95%, value_medium 86%, brand_logos 68%, on_screen_claim 73%. Core fields hold; folding does not perturb facet extraction.
- **Carousel index-alignment (A) 11/11** — objective; per-image summaries match slide count.
- **Cost: A = 1 video-input call/post vs B = 2** — folding halves video input.
- **Judge (A vs B_S summaries, blinded, caption-context text-only): A 13/9 better, 13/9 more_grounded, 12/10 more_informative** — DIRECTIONAL ONLY: Gemini judges Gemini against caption-plausibility, not true visual grounding; it proves A and B_S are equally *caption-plausible*, NOT equally *visually faithful*.
- **Methodological boundary:** facet agreement + index-alignment are objective; the judge is self-referential and caption-context. **Visual faithfulness of the folded summary is the residual unmeasured risk** — the one axis a dedicated summary-only call is designed to win. Verdict: fold now (facets untouched, alignment clean, half the video input); re-test visual faithfulness only if a summary ever becomes decision-grade (then spot-check or run summary-only on that subset, triage-style). Intended use (embeddings/search + qualitative themes) is non-decision-grade, so folding is sufficient.

## 7. Cost implications

Operating figures are repo-grounded estimates (~$0.002/media low-res call, caption ~$0.0005; full multimodal re-enrich of 9,576 ≈ $10–19). Tier-1 batch: 50% token discount, in-flight caps 10M (Flash-Lite gen).

| Component | Scope | Est. cost |
|---|---|---|
| Universal video→Gemini call (V3 visual facets + content_summary + per-image summaries) | ~8,849 media posts, ONCE | **~$9–18 total** (input-dominated; summaries add modest output — input already paid) |
| Text facet call (caption + transcript) | ~8,849 posts | pennies; **re-runnable freely** on schema change (never re-pays video) |
| Transcript capture | 7,029 videos (~4,100 min audio) | **$0 compute** if local faster-whisper; ~$12–29 one-time if cloud **batch** STT (~$0.003–0.007/min × ~4,100 min) |
| Media re-upload on re-enrich | if NOT in GCS, every re-enrich re-uploads ~55 GB via File API | recurring + ~48 h expiry risk |

**Rule that protects the scope:** the video call's output is a *fixed, rarely-changing visual contract* (visual facets + summaries). Every future semantic feature derives from caption + transcript in the cheap text call — so adding a facet never re-runs universal multimodal.

**Output tokens** are the lever to watch on the universal call: summaries + per-image carousel arrays raise it, so the call runs at 4096 to fit visual facets + bounded summaries (input still dominates cost).

## 8. Processing-time impacts

Measured + estimated:

| Component | Throughput | Corpus wall time |
|---|---|---|
| Universal video call (facets + summaries) | ~95 posts/job, ~4–8 min/job (~700–1,400 posts/hr); summaries add only output — no extra upload/encode | ~8,849 posts ≈ **~6–13 h of batch**, once (parallelizable across jobs) |
| Transcript capture (local CPU) | faster-whisper small.en int8 ≈ 0.3–0.6× realtime on laptop/cx33 | full 7,029 ≈ **~25–35 h** (small.en) or ~13–20 h (base.en); incremental per scrape = minutes |
| Transcript on NVIDIA GPU (if ever) | Parakeet/Whisper-large | ~1–2 h |
(no separate summary pass — folded into the universal video call above)
| Media upload (if re-enriching w/o GCS) | ~5–15 s/video File API | ~8,849 videos ≈ ~25–40 h upload wall (dominant re-enrich cost) |

Transcript full backfill is an unattended overnight job (1–2 nights) at $0; not a blocker.

## 9. Infrastructure options

The pipeline runs locally today (ETL local; only Apify actor + Gemini batch external).

**Option A — stay local (default).** The single universal video call + text facet call via Gemini batch as now. Transcript capture on the laptop (incremental at scrape; overnight full backfill). Simple, no infra change. Constraint: laptop must be up for long batch/transcript jobs.

**Option B — burst long/unattended jobs to the Hetzner box (cx33, 4 vCPU / 8 GB, no GPU, €8.5/mo).** Ideal for the **full-corpus transcript backfill** (long unattended CPU job) and for headless batch enrichment orchestration. Not faster than the laptop for ASR (CPU-only, similar band); it's an *unattended-run* win, not a speed win. Would require shipping audio/transcript work there (scrape location coupling).

**Option C — move media to GCS + use Google Cloud features (when re-enrich is recurring).** The decisive trigger: **do we re-enrich the corpus repeatedly?** If yes, durable media in GCS pays off — `gs://` refs make re-enrich sub-hour and remove the ~48 h File-API expiry + ~55 GB re-upload each cycle. Once in GCP, leverage:

- **Cloud Storage** (media durable; ~$1/mo for the corpus per repo estimate).
- **Cloud Run / Cloud Functions** for the ffmpeg audio-extract + ASR as serverless jobs (no always-on box).
- **Gemini batch / Vertex AI** — the batch API we already use; Vertex adds quotas/observability at scale.
- **Cloud Speech-to-Text** (STT v2, **online** ~$0.016/min) only if we want serverless ASR over Whisper — but it's ~$66 for the full corpus vs Whisper local $0, so prefer Whisper unless running fully in GCP with no local/box compute (batch STT ~$0.003–0.007/min would be the cheaper cloud path).
- Not (yet) needed: BigQuery, Dataflow — current scale is local-DuckDB fine.

**Recommendation:** run Option A/B now (facet + summary passes, local transcript) and treat Option C as the decision point *when* re-enrich frequency justifies durable media — the trigger is repeated enrichment, not first-time extraction. Do not buy a GPU box for the one-time transcript backfill (cx33 overnight covers it).

## 10. Decision points / open questions

1. Confirm the V3 facet schema + cross-modal model + explicit brand-safety set as the target `growth_facets_json`.
2. Fold `content_summary` + per-image carousel summaries into the single universal video call at 4096 output (prototype ~10 posts first to check quality + index-alignment).
3. Build transcript capture (ffmpeg→faster-whisper) — incremental-at-scrape + overnight backfill; store transcript column.
4. Infrastructure: stay local (A) vs burst to cx33 (B) vs move media to GCS / GCP (C) — C gated on re-enrich frequency.
5. Where the scrape runs (local vs box) determines where transcript capture lives.

## Sources / evidence
`tasks/plans/facet-list-experiment-design.md` (facet experiments + research agenda + literature incl. ContentBench 2602.19467, Variance-Aware LLM Annotation 2601.02370, WASSA 2026 facet/claim papers, PARSE 2510.08623, ScrapeGraphAI-100k 2602.15189, LLMStructBench 2602.14743). Experiment artifacts: `data/facet_menu.duckdb`, `data/facet_experiment.duckdb`. ASR: Self-Hosted Whisper guide (2026-06-29), faster-whisper, Open ASR Leaderboard (Parakeet). Media/transcript facts measured from `ops.sqlite`/`state.duckdb` + ffprobe (2026-09-07).
