# P1 — Pipeline Operator

- **Canonical actor string:** pipeline operator
- **Consolidates:** pipeline operator, operator, orchestrator, profile-onboarding
  operator
- **Status:** active (internal)

## Who
Runs and configures the ingestion → enrichment pipeline. Owns scrapes,
backfills, migrations, schedules, and spend/tier gates. Cares that runs are
honest, idempotent, resumable, and don't silently mis-state state.

## Primary intent / goals
- Onboard and track profiles/creators; control scrape depth and enablement.
- Run incremental + backfill jobs that checkpoint and resume; never corrupt or
  double-process.
- Respect single-writer DBs, additive-only DDL, and Gemini spend/tier gates.
- See that "0 enqueued" means "0 candidates", never "job silently skipped".

## Agent-mediated mode
An AI agent (`dlc-worker`) is the *actual* operator for most runs today. It
needs: idempotent/resumable/checkpointed jobs; media captured at scrape;
single-writer serialization; agent-enforced spend + environment guardrails;
clear success/failure signals it can act on. The persona's intent is "run it
reliably and truthfully" — the agent must never be able to drift silently.

## Related
`enrich-engine`, `media-capture`, `enrich-transcripts`, `enrich-labels`,
`ingest-silver`, `identity`.
