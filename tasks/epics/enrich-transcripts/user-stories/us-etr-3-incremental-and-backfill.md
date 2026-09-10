---
id: US-ETR-3
epic: E-ENRICH-TRANSCRIPTS
persona: P1
status: Open
---
# US-ETR-3 — Incremental-at-scrape + resumable overnight full backfill

- **Epic:** E-ENRICH-TRANSCRIPTS
- **Status:** Open
- **Relates to:** E-ENRICH-FACETS (US-EFAC-4 text-layer consumes transcript),
  infra decision (local laptop vs cx33 box)
- **Source:** `docs/architecture/enrichment-design-v1-superseded.md` §5, §8, §9

## Story
**As a** pipeline operator, **I want** new videos transcribed incrementally at
scrape and the existing ~7,029-video corpus backfilled as an unattended,
resumable overnight job, **so that** the corpus gains transcripts without a
human babysitting a ~10–35 h CPU run.

## Acceptance criteria (binary)
- AC1: New cached videos are transcribed as they land (incremental); per scrape =
      minutes.
- AC2: Full backfill is resumable — already-done videos are skipped on restart
      (idempotent by media/cache key), so an interrupt loses nothing.
- AC3: Backfill runs unattended on the chosen CPU target (local laptop or cx33
      box) — no GPU purchase for the one-time backfill.
- AC4: Progress + completion checkpointed so main can resume from a coherent
      state after any abort.

## Definition of done
- [ ] Incremental hook + resumable backfill job; idempotency proven by an
      interrupt-and-restart smoke.
- [ ] Decision recorded on run location (laptop vs cx33) tied to where the scrape
      lives.

## Tests
- Restart mid-backfill transcribes exactly the remaining set (zero redo).
- A transcript already present is not overwritten/duplicated.
