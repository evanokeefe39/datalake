# Epics & User Stories registry

> Coordination: **[`ROADMAP.md`](ROADMAP.md)** — new-feature build order
> (facets/summaries/transcripts) with quality gates, plus the deferred
> Epic/PR governance automation (hook + SQLite) with its decisions recorded.
>
> Personas: **[`personas/`](personas/README.md)** — P1–P7 principals + the
> AI-agent transitive actor (`personas/ai-agent.md`). User stories carry
> `persona: P#`.
>
> Plan↔Epic reverse lookup: **[`PLANS-INDEX.md`](PLANS-INDEX.md)** — every
> `tasks/plans/*.md` maps back to its epic(s) (and US where applicable).

Canonical store for Epics and User Stories. **The enrichment spine is the
through-line of this repo**: capture rich media → media-grounded Gemini signal
(labels, facets, summaries) at triage cost → a serving/analytics layer for
creator-growth decisions. Nearly every plan file in `tasks/plans/` is a stage,
cost-lever, or consumer of that engine, not a disjoint feature.

## Conventions

- **Canonical ownership: one User Story → one Epic (many-to-one).** A story may
  be *relevant* to other epics via a `relates-to` link, never a second copy.
  Cross-epic affinity is an epic-level dependency edge.
- **Source plans are immutable history.** This store is the go-forward home for
  new stories and the registry for existing epics. Never edit `tasks/plans/`
  retroactively here; if an old plan needs aligning, publish a *new version*
  that links to its canonical epic.
- **Git tracking:** only `tasks/epics/` is committed (`.gitignore` ignores the
  rest of `tasks/`). Epics/stories are review-and-change governance content, so
  they are versioned; `tasks/plans/` + `tasks/lessons.md` stay untracked.
- **New user stories are authored here**, under their epic's `user-stories/`
  dir, with As-a/I-want/So-that + binary AC + DoD + tests. Existing epics point
  to their source plan until extracted.
- **Canonical enrichment layer model (2026-09-10, strict medallion):** external
  model responses land verbatim in `bronze_enrichment_raw` (immutable,
  append-only); the six channel tables are SILVER
  (`silver_visual_annotations`, `silver_visual_summaries`,
  `silver_audio_transcripts`, `silver_text_annotations`,
  `silver_text_summaries`, `silver_content_classification`) — deterministic
  from bronze, zero API calls, and the validation/quality contract lives
  THERE; gold is analytic marts only (`gold_post_enrichment`,
  `gold_creator_performance`, `gold_content_shape_performance`,
  `gold_top_posts`), shaped to the owner's three questions. Keys are
  `(post_id, platform)` / `(creator_id, platform)`; `domain` means the
  CONTENT niche (what the owner calls "X domain"), never the platform.
- **Consolidation is a deliberate second step.** Similar epics may be merged;
  when they are, record the merge here and in the affected story `relates-to`.

## Registry

| Epic (dir) | ID | Theme | Status | Owner | Depends on | Source of truth |
|---|---|---|---|---|---|---|
| [`ingest-silver`](ingest-silver/epic.md) | E-INGEST | Bronze→silver ingestion, schema catalog, watermarks | Done (foundation) | dlc-worker | — | `tasks/plans/phase-1/2`, `ig-local-ingestion`, `watermark-deadletter-refactor`, `state-readiness-*` |
| [`media-capture`](media-capture/epic.md) | E-MEDIA | Scrape-time media/avatar byte cache, media_files wiring | Active | dlc-worker | E-INGEST | `tasks/plans/multimodal-processing`, `media-and-entity-routing`, `19-20-batch-multimodal-mime` |
| [`enrich-engine`](enrich-engine/epic.md) | E-ENRICH-ENGINE | Async enrichment engine (submit/harvest) on the standalone qwen-batch service (ADR-0009; was Gemini batch) | Active (pivot) | dlc-worker | E-MEDIA | `tasks/plans/qwen-batch-enrichment`, `docs/adr/0009` (ADR-0007 shape), US-EENG-1/2 |
| [`enrich-labels`](enrich-labels/epic.md) | E-ENRICH-LABELS | Triage labels/admission, label_version, standout/underperformer | Active | dlc-worker | E-ENRICH-ENGINE | `tasks/plans/post-performance-observations-*` |
| [`enrich-facets`](enrich-facets/epic.md) | E-ENRICH-FACETS | **NEW** cross-modal growth facets (V3) + engagement-utility | Open (design) | dlc-worker | E-ENRICH-ENGINE, E-MEDIA, E-SERVING-ANALYTICS | `docs/enrichment-enhancement-design.md`, `tasks/plans/facet-list-experiment-design.md` |
| [`enrich-summaries`](enrich-summaries/epic.md) | E-ENRICH-SUMMARIES | **NEW** content_summary + per-image carousel summaries (folded call) | Open (design) | dlc-worker | E-ENRICH-ENGINE, E-MEDIA | `docs/enrichment-enhancement-design.md` §6 |
| [`enrich-transcripts`](enrich-transcripts/epic.md) | E-ENRICH-TRANSCRIPTS | **NEW** ffmpeg audio-extract → faster-whisper, incremental + backfill | Open (design) | dlc-worker | E-MEDIA | `docs/enrichment-enhancement-design.md` §5 |
| [`serving-analytics`](serving-analytics/epic.md) | E-SERVING-ANALYTICS | dims/views/metrics, creator analytics, observations, **four gold marts (owner's three questions, US-ESA-1)** | Active | dlc-worker | E-ENRICH-*, E-IDENTITY | `tasks/plans/phase-4`, `metrics-centralization`, `creator-*`, `follower-observations-*` |
| [`identity`](identity/epic.md) | E-IDENTITY | creators/profiles/merges + dim_profile creator linkage + UI | Active | dlc-worker / sdlc-worker | E-INGEST | `tasks/plans/creators-and-profiles`, `profile-management`, `curated-creator-consolidation` |
| [`dashboard`](dashboard/epic.md) | E-DASHBOARD | Product UI: tables, filters, thin server, creators page | Active | sdlc-worker | E-SERVING-ANALYTICS | `tasks/plans/dashboard-tables-filtering`, `creators-ui-redesign` |

## Spine dependency map

```
ingest-silver ─► media-capture ─► enrich-engine ─► enrich-labels ─► serving-analytics ─► dashboard
      │                │              │                  │                   ▲
      │                ├──────────────┴── enrich-facets ──┤                   │
      │                └──────────────► enrich-transcripts  (no Gemini)      │
      │                               enrich-summaries ──────────────────────┘
identity ────────────────────────────────────────────────────────────────────► dim_profile linkage
```

New user stories in this repo should generally attach to **enrich-facets /
enrich-summaries / enrich-transcripts / enrich-labels / serving-analytics** —
those are the live, value-driving seams.
