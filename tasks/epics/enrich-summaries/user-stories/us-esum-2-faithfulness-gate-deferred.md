---
id: US-ESUM-2
epic: E-ENRICH-SUMMARIES
persona: P5
status: Deferred
---
# US-ESUM-2 — Visual-faithfulness gate (deferred, decision-grade only)

- **Epic:** E-ENRICH-SUMMARIES
- **Status:** Deferred — no scheduled work
- **Source:** `docs/enrichment-enhancement-design.md` §6 (methodological boundary)

## Story
**As a** reviewer/auditor, **I want** visual faithfulness of the folded summary
re-validated **only if** a summary ever becomes decision-grade (audit verdict,
sponsorship recommendation), **so that** we don't trust fold-equivocation where
true visual grounding matters.

## Why deferred
The fold spike's A-vs-B_S judge is self-referential and caption-context: it
proves folded and dedicated summaries are equally *caption-plausible*, not
equally *visually faithful*. The intended use (embeddings/search + qualitative
themes) is non-decision-grade, so folding is sufficient and the faithfulness
risk is accepted. This story activates the moment a summary feeds a decision.

## Acceptance criteria (binary, when triggered)
- AC1: Spot-check faithfulness on a sample against the actual media (not
      caption), OR run summary-only calls on the decision-grade subset (triage).
- AC2: If faithfulness regresses under fold, switch that subset to a dedicated
      summary-only call at 4096.

## Definition of done (when triggered)
- [ ] Faithfulness check artifact produced; fold-vs-dedicated decision recorded.
