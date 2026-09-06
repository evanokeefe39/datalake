# refactor-research — index & reproduction playbook

Living research area for the orchestration-refactor investigation (`chore/refactor-investigation`).
Working plan lives in `tasks/plans/refactor-architecture-investigation.md` (gitignored).

## Artifacts

| File | What |
|------|------|
| `enrichment-service-patterns.md` | Research note: enrichment-with-API-calls design patterns, Dagster-native building blocks, antipatterns, option-space mapping (grounded in `dagster-expert` skill + our ADRs). |
| `orchestration-two-world-split.html` | Editorial explainer ("One pipeline, two worlds") — a standalone static HTML article + inline SVG diagrams illustrating data flow, the World A/B seam, and the #23/#24/#22/#20/#5 failure zones. |
| `migration-batch-native-enrichment.md` | Migration plan: batch-native enrichment, deprecate the external worker (the branch's executable output). Ratified via [ADR-0007](../adr/0007-batch-native-enrichment-deprecate-worker.md) + [ADR-0008](../adr/0008-hermetic-with-explicit-api-seam.md). |
| `README.md` (this) | Index + the method used to build the article, for reproducible future visual explainers. |

## How the article was produced (reproducible recipe)

**Agent:** `frontend-craft` (the design-skilled frontend subagent — the `frontend` agent but judged on taste/craft as well as correctness).

**Method (do this in order):**
1. **Author a precise content brief first.** The agent is not a data architect; it must not invent mechanics. Give it: the exact narrative, the two-world model, every issue + its root cause, and a list of **source-of-truth file paths** to read (research note, `ISSUES.md`, the relevant ADRs, the real code seams). Write the brief in the `task` so accuracy is upstream of the visuals.
2. **Instruct it to read the sources before building**, then design, then write one file. Give design direction (editorial/data-journalism tone; encode World A vs World B in two locked accent colors; dark panels for failure zones; hand-drawn inline SVG > a diagram lib where fine control matters; scroll-spy; honor `prefers-reduced-motion`; fonts via Google Fonts CDN). Single static `.html`, no build step.
3. **Acceptance criteria in the brief:** single file; facts match sources; diagrams render; screenshots of the hero + a diagram.
4. **Resume-on-failure:** if the agent exits before writing (it did once — execution hiccup, not content), it stays idle at `history://<agent>`. DM it via `hub` to *resume and finish writing the file first*, then verify. Cheap and reliable.

**Verification (two gates — content beats pixels here):**
- **Content-accuracy gate (the one that matters).** Read the finished article and cross-check every concrete figure against the sources: the #24 numbers (593/543/535+8/9,035→9,570/~00:05), #5's 365-of-2,628 (13.9%), the dead-letter counts (six `429` + four empty-caption), the `FreshnessPolicy` warn-24h/fail-48h, and "#20 fixed in PR #43". All traced exactly — the agent had read ISSUES.md/code and used real numbers, not placeholders.
- **Structural gate (do this in the main loop, no image-capable model needed).** Tag-balance check with a real parser. **Gotcha:** naive HTML parsers report false errors on inline SVG (self-closing `<rect/>` etc.) — strip `<svg>…</svg>` bodies *first*, then check `section`/`figure`/`div`/`main`/`nav` balance and that every nav `href="#…"` resolves to a `section[id]`. This caught three real markup bugs: an unclosed `<figure>`, a stray `</div>` before `</main>`, and (from an edit-tool nav insert) an orphaned nav block.
- **Render/visual gate:** hand to a browser-capable agent for screenshots; the main model couldn't ingest images, so use DOM proxies (no horizontal overflow, SVG bounding boxes non-zero, fonts `loaded`, scroll-spy observers present) as the in-loop check and let `frontend-craft` do pixel QA.

## Lessons worth carrying forward
- **Feed the visual agent a factual brief + source paths**, then independently verify the figures. Never let diagrams carry invented mechanics.
- **Structural edits to HTML (nav insert) can fuzzy-match and consume surrounding open tags** — re-run the balance check after any edit-tool HTML change, not just before committing.
- **SVG-heavy HTML defeats naive tag parsers**; strip `<svg>` before balancing, or use a real HTML5 parser.
- **Inline SVG (hand-drawn) beat pulling a diagram library** here: full control over the two-world fencing + seam highlight, and no CDN/library runtime risk for a self-contained artifact.
