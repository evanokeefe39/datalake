# US-DISC-5 — Use the actor's date filter for incremental refresh

- **Epic:** E-DISCOVERY
- **Persona:** P1 (Pipeline Operator), P7 (Owner / Principal)
- **Status:** Open — **absorbed into US-DISC-7** (ingestion upgrade, next branch); kept for its AC rationale.
- **Depends on:** —

## Story

**As** the pipeline operator,
**I want** the weekly refresh to request **only posts newer than a date**,
**So that** we pay for genuinely new posts instead of re-billing the newest N
that we have already seen.

## Context — a capability the actor has and we do not use

The actor **`apify~instagram-scraper`** exposes **`onlyPostsNewerThan`** in its
input schema (`GET /v2/acts/apify~instagram-scraper/builds/default` →
`actorDefinition.input`):

> *"Filter by date. Limit how far back to scrape. Enter a date in `YYYY-MM-DD`,
> ISO format, or as a relative value, e.g. `1 day`, `2 months`, `3 years`."*
> `dateType: absoluteOrRelative`; the pattern accepts `7 days`.

**Our client does not send it.** `integration/apify_client.py:111-115` sends only
`directUrls`, `resultsType`, `resultsLimit`, and `proxy`.

**Why it matters — billing is per result written to the dataset:**

| | `resultsLimit: N` (today) | `onlyPostsNewerThan: "7 days"` |
|---|---|---|
| Low-frequency creator (1 post/wk) | returns **7**, 6 already seen → **billed again** | returns **1** |
| High-frequency creator (>N/wk) | older posts roll off **silently** | returns all in the window |
| Cost per run | fixed at N | proportional to actual new posts |
| Blind spot | prolific posters' middle posts | none (date-complete) |

At the measured **$0.0023/result**, a creator posting once a week costs
**$0.0161** per weekly run with the date filter, versus **$0.0161 × 7** with a
count filter — a 7× difference on exactly the creators the incremental path was
meant to make cheap.

## Acceptance criteria (binary)

1. **GIVEN** a refresh config with a date window,
   **WHEN** the run is triggered,
   **THEN** the request body includes `onlyPostsNewerThan` with the configured
   value.
2. **GIVEN** a relative value (e.g. `7 days`),
   **WHEN** it is passed,
   **THEN** it is transmitted verbatim (the actor accepts relative values; do not
   convert to an absolute date client-side).
3. **GIVEN** a creator who published 1 post since the last run,
   **WHEN** the refresh runs,
   **THEN** the dataset contains **1** item (not N) — proving the filter is
   server-side, not client-side dedupe.
4. **GIVEN** `resultsLimit` is also configured,
   **WHEN** both are sent,
   **THEN** `resultsLimit` acts as an upper **safety bound** and the date filter
   governs which posts qualify.
5. **GIVEN** the backfill path (first scrape of a creator),
   **WHEN** it runs,
   **THEN** it is unaffected — backfill uses depth, not a date window.
6. **GIVEN** a run with `max_charge_usd` configured,
   **WHEN** the charge cap is reached,
   **THEN** the run terminates gracefully with partial results rather than
   failing mid-scrape (the account is at its credit ceiling).

## Definition of done

- [ ] `onlyPostsNewerThan` plumbed through `ScrapeConfig` → `trigger_run` →
      request body, with a named constant for the default window.
- [ ] ACs 1–6 each have a test; AC 3 (1 item, not N) is load-bearing — it is the
      only one that proves the filter is server-side.
- [ ] Docs state that the DEFAULT remains count-based for backfill, and the date
      filter applies to the incremental path only.
- [ ] A real incremental run is measured (`itemCount` vs posts actually new) to
      confirm per-result behaviour — recorded in the epic.
- [ ] `maxTotalChargeUsd` set on refresh runs (AC 6).
- [ ] Reasoning trace + assumption log.

## Open questions

- Window length: `7 days` exactly, or wider (e.g. `10 days`) to overlap and
  avoid gaps for creators who post less often than the cadence? The actor
  documents that **pinned posts may still appear even with the filter set**, so
  a small amount of re-billing is expected regardless.
- Should the window be derived from the **last successful run's timestamp** per
  profile rather than a fixed relative value? A per-profile cursor is more
  precise but adds state; a fixed relative value is stateless and simple.
