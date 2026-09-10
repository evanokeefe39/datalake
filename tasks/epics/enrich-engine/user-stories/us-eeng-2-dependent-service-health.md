---
id: US-EENG-2
epic: E-ENRICH-ENGINE
persona: P1
status: Open
---
# US-EENG-2 — qwen-batch service is a declared dependent service; loud failure if down

- **Epic:** E-ENRICH-ENGINE
- **Status:** Open
- **Source:** `tasks/plans/qwen-batch-enrichment.md`

## Story

**As a** pipeline operator, **I want** the enrichment step to treat the
standalone qwen-batch service as a hard dependency and **fail loudly** when it is
not up, **so that** a down service can never masquerade as "no work to do" and
silently skip enrichment.

## Acceptance criteria (binary)

- AC1: The repo declares the qwen-batch service as a dependent service (its
      run/local + docker invocation and health contract documented).
- AC2: Before any submit, the client pings the service `GET /health`; a non-OK or
      connection failure raises a clear error naming the service and how to start
      it (docker compose up), never returning an empty/quiet success.
- AC3: The failure surfaces as a loud CLI failure (QwenServiceError →
      non-zero exit from scripts/enrich_facets_batch.py), never a completed
      run with zero items.
- AC4: Health/readiness state is visible in the enrichment logs at run start.
- AC5 (workload-agnostic health): The health gate covers ALL workloads the
      service hosts (vision, text, whisper-STT) — one dependent service, one
      `/health` contract, one loud-failure rule; no workload may bypass the
      health check or fail quietly.

## Definition of done

- [ ] Health check wired into the submit path; AC2 verified with the service
      stopped (driver run fails loudly) and started (passes).
- [ ] Dependent-service + spin-up instructions documented in AGENTS.md.

## Tests

- Stop the service → trigger enrichment → step errors with the startup remedy,
      and no gold writes occur.
- Start the service → same trigger → enrichment proceeds.
