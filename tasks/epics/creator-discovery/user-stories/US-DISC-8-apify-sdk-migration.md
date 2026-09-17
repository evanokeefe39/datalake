# US-DISC-8 — Official Apify SDK migration

**Epic:** E-DISCOVERY · **Branch:** `feat/us-disc-7-ingestion-upgrade` (same
branch as US-DISC-7; split for independent acceptance) · **Status:**
**Implemented** (PR #89, 2026-09-17) · **Depends on:** nothing
in US-DISC-7 · **Absorbs:** the former US-DISC-6, `ISSUES.md` #41 and #42.

## Story

**As** the pipeline owner, **I want** the hand-rolled Apify HTTP client replaced
by the official `apify-client` SDK, **so that** the actor's own input schema is
discoverable from code and a capability the actor exposes cannot be missed the
way `onlyPostsNewerThan` was.

**Why this is separate from US-DISC-7:** the migration is mechanical and
independently verifiable — it changes transport, not behaviour — and it does not
depend on the watermark or bucketing design. It ships on the same branch so the
one-branch decision stands, but it is accepted on its own criteria.

## Contracts to move (verified)

Migrate `defs/integration/apify_client.py` → `apify-client` (v3.2.0 verified).
Capabilities we lack: `actor.get()` (input schema), `actor.validate_input()`
(validate before spending), `dataset.iterate_items()` (pagination + no full
buffer), and it does **not shadow the package name**.

**Migration moves FOUR contracts (not a thin import swap):**

1. **Call shape** — `scrape.py:28` imports three *functions*; the SDK is OO
   (`ApifyClient(token).actor(id).call()`).
2. **Return contract** — `RunInfo` and `stream_dataset(...) -> int`.
3. **Patch target** — `tests/unit/instagram/test_core_refresh.py:42` patches
   `...apify_client._post` **by module path**; it must move or it silently stops
   intercepting.
4. **Idempotency + streaming** — `bronze_path(dataset_id)` + exists-check give
   write-once idempotency; preserve it.

**Do not lose:** `stream_dataset` uses `format=json` to *"avoid Apify's NDJSON
newline bug"* — re-verify against the SDK or retain explicitly, with evidence.

**Also fixes:** `stream_dataset` currently does **not stream** — it buffers the
whole response via `json.loads(resp.text)`. Not a correctness bug (the endpoint
is uncapped — see ISSUES #42) but a memory concern at scale; `iterate_items()`
resolves it.

**Submit/poll separation must survive:** Dagster assets rely on trigger now /
harvest later, so prefer `actor.start()` + `run.wait()` over `call()`.


## Acceptance criteria (binary)
1. **GIVEN** the SDK is adopted,
   **WHEN** a scrape is requested,
   **THEN** the request is issued through `apify-client` and the hand-rolled
   `apify_client.py` is removed (or renamed to a non-shadowing module) — no
   duplicate transport path remains.
2. **GIVEN** the actor's input schema is fetched,
   **WHEN** it is retrieved,
   **THEN** it is retrievable from code (`actor.get()`), so a capability like
   `onlyPostsNewerThan` is discoverable rather than invisible.
3. **GIVEN** the SDK is adopted,
   **WHEN** a scrape is requested,
   **THEN** the outgoing request is **equivalent to the pre-migration request**
   on all three of its parts, and the test asserts them **separately**, because
   a body-only diff would pass while silently dropping the spending guard:

   - **JSON body** — `directUrls`, `resultsType`, `resultsLimit`, `proxy`
   - **Query params** — **`maxTotalChargeUsd`** (from `max_charge_usd`): a
     QUERY param, NOT a body key
   - **`proxy` shape** — `{"useApifyProxy": True}`, compared **structurally**,
     never as a string

   Capturing the body alone cannot see `maxTotalChargeUsd`, and string-comparing
   `proxy` breaks on key order — so the test captures the **full outgoing
   request (URL + query params + JSON body)**. **This is the guard the split
   nearly lost:** dropping `proxy`, `resultsType`, or the charge cap silently
   changes what is billed and returned, and every other criterion here stays
   green.
4. **GIVEN** `max_charge_usd` comes from config/CLI,
   **WHEN** a run is started,
   **THEN** the configured value reaches the request as the charge cap —
   asserting the **plumbing from config to the request**, not the parameter
   shape itself (AC 3 owns the transport detail).
5. **GIVEN** a scrape completes,
   **WHEN** the dataset is read,
   **THEN** it is consumed via `iterate_items()` (streaming), so peak memory does
   not scale with dataset size — resolving the buffering residual of ISSUES #42.
6. **GIVEN** `test_core_refresh.py` patches the transport,
   **WHEN** the suite runs,
   **THEN** the patch target is updated to the new module path and still
   intercepts — a silently non-intercepting patch is a false green.
7. **GIVEN** a dataset was already ingested,
   **WHEN** the same `dataset_id` is fetched again,
   **THEN** the write-once idempotency (`bronze_path(dataset_id)` + exists-check)
   is preserved.
8. **GIVEN** the submit/poll separation,
   **WHEN** a run is triggered,
   **THEN** the asset still triggers now and harvests later (never a blocking
   `call()` inside a materialization).
9. **GIVEN** the `format=json` NDJSON workaround,
   **WHEN** the SDK path is used,
   **THEN** the decision is re-verified against the SDK and either retained with
   evidence or removed with evidence.

## Definition of done

- [ ] `apify-client` added via `uv add` (never `pip`).
- [ ] All four contracts migrated: call shape, `RunInfo`/return contract, patch
      target, idempotency.
- [ ] A **real smoke run** against one profile: row-count and field parity
      against the pre-migration path — not just a green suite.
- [ ] Submit/poll separation preserved; no blocking call inside a materialization.
- [ ] `ISSUES.md` #41 and #42 closed with the evidence that closed them.
- [ ] Conventional commits; branch per repo conventions.
- [ ] Reasoning trace + assumption log.

## Open questions

1. **`actor.start()` + `run.wait()` vs `call()`** — confirm the SDK's async shape
   matches the current trigger/harvest split before choosing.
2. **Does the SDK's item iteration preserve the `format=json` behaviour**, or does
   it reintroduce the NDJSON newline issue the workaround exists for?
