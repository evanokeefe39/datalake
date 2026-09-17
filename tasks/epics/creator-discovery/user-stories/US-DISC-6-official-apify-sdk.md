# US-DISC-6 — Replace the hand-rolled Apify client with the official SDK

- **Epic:** E-DISCOVERY
- **Persona:** P2 (Platform Engineer), P1 (Pipeline Operator)
- **Status:** SUPERSEDED by **US-DISC-8** (`US-DISC-8-apify-sdk-migration.md`),
  on the `feat/us-disc-7-ingestion-upgrade` branch. This file is retained as the
  original statement of intent; US-DISC-8 is canonical and carries the ACs/DoD.
  Tracked as `ISSUES.md` #41.
- **Depends on:** US-DISC-5 (date filter) — or do them together

## Story

**As** the platform engineer,
**I want** the pipeline to use the maintained `apify-client` SDK instead of our
hand-rolled wrapper,
**So that** we stop paying for a thin reimplementation that is less capable than
the library it shadows, and so actor capabilities are discoverable from code.

## Context — verified comparison

`defs/integration/apify_client.py` is a hand-rolled client (~150 lines: auth,
tenacity retry, three calls). The official `apify-client` is **not** a dependency.
Verified by installing it (v3.2.0) and inspecting:

| Capability | Hand-rolled | Official `apify-client` |
|---|---|---|
| Trigger / poll | ✅ | ✅ `actor.call()` |
| Retries | ✅ tenacity | ✅ built in |
| `maxTotalChargeUsd` | ✅ (as a **query** param) | ✅ `call(max_total_charge_usd=Decimal)` |
| Dataset fetch | ❌ single blocking GET, **fully buffered in memory**, no pagination | ✅ `dataset.iterate_items()` / `stream_items()` |
| **Actor input schema** | ❌ **none** | ✅ `actor.get()` |
| **Input validation before spending** | ❌ | ✅ `actor.validate_input()` |
| Maintained by | us | Apify |

**Why this matters beyond tidiness — it caused a real defect.** The absence of
schema access is exactly how `onlyPostsNewerThan` went unnoticed: the actor
supports a date filter, our wrapper could not reveal it, and it was only found by
querying the API directly after repeated prompting. A client that can expose the
input schema makes the next such gap discoverable.

**Also note the naming collision:** `defs/integration/apify_client.py` shadows the
official `apify_client` package, so importing the real library inside that module
would read confusingly. Rename or delete the wrapper.

## The one thing NOT to lose

`stream_dataset` deliberately requests `format=json` (a JSON **array**) with a
documented reason: *"to avoid Apify's NDJSON newline bug."* That workaround
encodes hard-won knowledge. The migration MUST either verify the bug is fixed in
the current SDK or retain the workaround explicitly — and must state which, with
evidence. Losing this silently would reintroduce a data-corruption bug.

## Acceptance criteria (binary)

1. **GIVEN** the orchestration service,
   **WHEN** its dependencies are inspected,
   **THEN** `apify-client` is declared (via `uv add`) — no `pip` usage.
2. **GIVEN** a scrape config,
   **WHEN** a run is triggered,
   **THEN** the request body is **equivalent** to the current one
   (`directUrls`, `resultsType`, `resultsLimit`, proxy) — proven by asserting the
   payload, not by observing that a run succeeded.
3. **GIVEN** `max_charge_usd` is configured,
   **WHEN** the run is triggered,
   **THEN** the charge cap is applied through the SDK's own parameter, and the
   cap is honoured (not silently dropped in the port).
4. **GIVEN** a dataset download,
   **WHEN** it completes,
   **THEN** it does not buffer the entire dataset in memory — verified
   structurally (an iterator/stream path), including the `format=json`
   newline-bug handling.
5. **GIVEN** the wrapper is removed,
   **WHEN** the repo is searched,
   **THEN** no reference to `integration/apify_client` remains, and the name does
   not shadow the official package.
6. **GIVEN** `scrape.py` is the only production caller and `test_core_refresh.py`
   patches `_post`,
   **WHEN** the migration lands,
   **THEN** both call sites and the test's patch target are updated in the same
   change — the test must not be left patching a symbol that no longer exists.

## Definition of done

- [ ] `apify-client` added via `uv add`; wrapper removed or renamed to a
      non-shadowing name.
- [ ] The `format=json` newline-bug decision is re-verified and documented with
      evidence (either "fixed in vX" or "workaround retained because…").
- [ ] AC 2 proven by asserting the payload; AC 4 verified structurally.
- [ ] Blast radius enumerated before editing: `scrape.py` (production) and
      `test_core_refresh.py` (patches `_post`).
- [ ] A real smoke run against one profile confirms equivalent results (row
      count and field parity against the current path) — not just a green test.
- [ ] Reasoning trace + assumption log.

## Open questions

- Is the official SDK's `call()` (synchronous with `wait_duration`) a better fit
  than our trigger+poll split? Our Dagster assets rely on the trigger/poll
  separation (submit starts work, harvest polls later). `actor.start()` +
  `run.wait()` may preserve it — confirm before choosing.
- Does the SDK expose `onlyPostsNewerThan` via `run_input` (plain dict) or does
  it validate against the schema? If `validate_input` rejects unknown keys,
  US-DISC-5's parameter must be verified against it.
