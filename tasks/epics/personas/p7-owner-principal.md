# P7 — Owner / Principal

- **Canonical actor string:** owner
- **Consolidates:** "the user", the human running the factory
- **Status:** active (primary principal)

## Who
The human who owns the platform and the decisions it serves (creator-growth
strategy, research direction, spend). Sets direction and makes consequential
judgment calls; delegates execution to agents and workers.

## Primary intent / goals
- Steer what the platform studies (growth, what works vs bombs, niches) and what
  it must not touch.
- Make consequential decisions (tier upgrades, spend, schema/scope) with enough
  honest signal to judge — not be surprised by silent agent drift.
- Trust that work lands in a governed way: related to epics/stories, loudly
  surfaced, auditable.

## Agent-mediated mode
The Owner is the principal *behind* the other agents (`dlc-worker`,
`sdlc-worker`, reviewer) and their AI-agent operators. What the platform owes
the Owner is **governance + transparency**: agent-created PRs must cite their
epic/user story and be loudly flagged when they don't; versioned outputs must
let the Owner see what's stale; spend/guardrails must be enforced on the
agents. This is the deferred hook/SQLite lane's real beneficiary.

## Related
Governance automation (ROADMAP Part 2), every persona above (they act for P7),
`AGENTS.md` (agent operates this repo for the Owner).
