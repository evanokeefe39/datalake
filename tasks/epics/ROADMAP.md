# Roadmap — new-feature implementation & deferred governance

This is the coordination layer for the store (parallels how
`tasks/plans/post-performance-observations-workstreams.md` coordinates its
epics). Two parts:
- **Part 1** — build order for the new enrichment features (facets, summaries,
  transcripts) with quality/completeness gates.
- **Part 2** — deferred Epic/PR governance automation (hook + SQLite), decisions
  recorded so it can be picked up later without re-deriving them.

Ownership: these are **dlc-worker** (data/stateful) epics. Feature epics are
tracked here; detailed AC/DoD/tests live in each epic's `user-stories/*.md`.

---

## Part 1 — New-feature build order

New feature epics: **enrich-facets**, **enrich-summaries**, **enrich-transcripts**
(+ supporting **enrich-engine** universal call, **media-capture** cached bytes).

### Sequencing rationale
- **Engine backend (2026-09-09, ADR-0009):** the universal call + all media-backed
  passes run on the standalone **qwen batch service** (`~/repos/qwen-batch-service`,
  US-EENG-1), not Gemini — media is client-side frame-sampled and sent in-request
  (no File-API upload, no GCS mirror). qwen is ~$3 for the corpus. US-EFAC-3/
  US-ESUM-1 below are qwen-backed.
- Engagement-utility is **free** (no model spend) and tells us which facets to
  ship → it gates schema finalization, not after it.
- The **universal video call** is one physical integration shared by facets and
  summaries (US-EFAC-3 + US-ESUM-1) — build it once.
- **Transcripts are independent of Gemini** (local ffmpeg+whisper) → they are a
  parallel cheap enabler that upgrades the text layer; they do NOT block the
  caption-only text layer.
- **Dev/test ≠ full backfill.** Iteration runs on a *slice* (fast locally or via
  a cloud burst), never the full corpus; production is the overnight local run.

### Build order
| # | Work | Epic / US | Gate / decision before next |
|---|---|---|---|
| 1 | Engagement-utility validation (free) | enrich-facets · US-EFAC-2 | Report which facets discriminate standout/hot vs underperformer per media type; non-discriminators flagged monitor, NOT dropped (keep-bias) |
| 2 | Lock V3 facet schema (uses #1) | enrich-facets · US-EFAC-1 | value_medium + brand_logos/on_screen_claim resolved; reserved-key validator tests pass |
| 3 | Transcripts: backend + incremental + backfill | enrich-transcripts · US-ETR-1..4 | ffmpeg→whisper incremental + resumable overnight backfill; **US-ETR-4 pluggable backend** gives a fast GCP-spot burst for dev/test slices |
| 4 | Universal video call (visual facets + folded summaries) | enrich-facets · US-EFAC-3 + enrich-summaries · US-ESUM-1 | Single additive call; smoke on temp/isolated DB; carousel index-alignment validated |
| 5 | Text-layer facets (caption-only first, transcript later) | enrich-facets · US-EFAC-4 | Re-runnable cheap text call; video input never re-sent on schema change |
| 6 | Gold marts shaped to the owner's three questions | serving-analytics · US-ESA-1 | Four marts (`gold_post_enrichment`, `gold_creator_performance`, `gold_content_shape_performance`, `gold_top_posts`) materialized from the six SILVER tables (enrichment landing: `bronze_enrichment_raw`; key = `platform`, `domain` = content niche); idempotent, row-count reconciled |
| 7 | (Deferred) visual-faithfulness gate | enrich-summaries · US-ESUM-2 | ONLY if a summary becomes decision-grade |

### Quality & completeness gates (all work)
- Schema catalog + readiness green for any new column/table.
- `seam_violations()==[]` holds (defs untouched by harnesses).
- Scoped `uv run pytest` (enrichment + unit/scripts) green; no regression to
  the silver/gold path.
- Validator tests: rejects reserved classification keys (`silver_content_classification`
  body) / missing required fields;
- Additive + `prompt_hash` self-versioning; single-pass guard (adding a text
  facet never re-sends video). Enrichment validation/quality contract lives in
  SILVER (deterministic from `bronze_enrichment_raw`); gold is marts only.
- Smoke on a temp DB before any production wiring; production enablement
  reviewed.
- Engagement-utility report committed artifact (numbers grounded in lake state,
  honest coverage flags).

### Decisions deferred until triggered
- **Full-media GCS move** + Cloud Run/STT: only when re-enrich is recurring (not
  for first extraction). No GPU *purchase* — the one-time backfill uses local
  CPU or a rented spot GPU.
- **Transcript burst** (US-ETR-4): GCP spot GPU in the SAME region as the
  media/staging bucket (internal transfer $0, per-second billing). VPN does NOT
  reduce cloud egress — region is a GCP setting; media must be colocated with
  the compute.
- Transcript **run location** (laptop vs cx33) pending where the scrape lives.

---

## Part 2 — Epic/PR governance automation (DEFERRED, decisions recorded)

Goal: reliably relate PR work to Epics and shout loudly when a PR lacks an
Epic/User Story; store tabular state (categories, IDs, PR relations) in SQLite.

### Decided design (do not re-derive — implement as-is when picked up)
1. **Enforcement posture: warn + notify, NOT hard block.** A hook intercepting
   PR creation notifies loudly (`ctx.ui.notify`) and injects a reminder; it does
   not throw/block. Escape hatch available.
2. **Single source of truth: markdown as truth (git-backed).** `tasks/epics/`
   `.md` is canonical. SQLite is a **derived, rebuildable index** (loader parses
   `tasks/epics/**` → `epic`/`user_story` tables); it is NOT tracked as the
   source — if the DB is lost, re-run the loader. `pr` relations (high churn)
   live in the DB.
3. **No native PR lifecycle event in OMP** — hooks fire on session / agent-turn /
   tool events only. So enforcement intercepts `tool_call` on the `github` tool
   when `op === "pr_create"` (plus `bash` running `gh pr create`/merge push), and
   checks body/branch against the registry.
4. **Layering (native mechanisms compose):**
   - **Rule** (`.omp/rules/*.md`, always-on): every PR must cite an Epic (+US
     when one exists); if none fits, surface it. Makes the agent cooperate.
   - **Hook** (`.omp/hooks/pre/*.ts`, ExtensionAPI preferred over legacy
     HookAPI): automatic enforcement — warn/notify on `github pr_create` when
     the Epic/US reference is missing.
   - **Command** (`/.epic relate`): user-invoked manual assign of branch/PR to
     an epic.
   - **Skill** (`epic-governance`): on-demand workflow knowledge (how to query/
     write the registry); auto-loads on PR/merge work.
   - **Custom tool** (`record_pr_epic` / `epic_lookup`): agent-callable.
   - Retrospective PR→Epic mapping (the 54 merged PRs) is folded into the SQLite
     `pr` table when this lane is built — not a standalone markdown doc.
5. **Placement:** project-local `.omp/` in the datalake repo (repo-specific
   governance). SQLite DB stays **untracked** (operational); markdown is the
   committed record.

### Status
Deferred. Do not build until this roadmap item is explicitly started.

### Triggers to start it
- PRs start landing that cite no Epic/US (enforcement need is real), or
- Manual PR-MAPPING churn becomes painful, or
- The first new-feature PR (facet/transcript/summary) is about to be authored.
