# P2 — Platform Engineer

- **Canonical actor string:** platform engineer
- **Consolidates:** developer, maintainer
- **Status:** active (internal)

## Who
Builds and maintains the code, schema, seams, and tests. Cares about clean
architecture, hermetic boundaries, schema-catalog/readiness integrity, and
re-runnable, deterministic tooling.

## Primary intent / goals
- Keep `defs/` hermetic (no API calls in transform layers); `seam_violations()`
  empty; every new table/column in the schema catalog or readiness fails.
- Additive, self-versioned outputs; reserved-key/validator safety.
- Add a new facet → re-run only the cheap text call, never re-send video.
- Re-runnable scripts and deterministic tests that prove behavior.

## Agent-mediated mode
An AI agent (`sdlc-worker`, `implementer`, `reviewer`) writes the code and
opens PRs. It needs the seams and tests above so agent changes are provably
safe, plus governance so every agent-created PR cites its epic/user story. The
persona's intent is engineering quality; the agent is the hands that must be
kept honest by the platform's checks.

## Related
`enrich-engine`, `serving-analytics` (thin-projector discipline), schema
catalog, `AGENTS.md` conventions.
