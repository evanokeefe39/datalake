# AI Agent — transitive actor (not a persona)

- **Status:** active — the datalake is already agent-operated (AGENTS.md, omp
  `dlc-worker`/`sdlc-worker`/`reviewer`)

## What it is
An AI agent (Claude Code / omp worker, ChatGPT, another coding agent) is a
**transitive actor**: it has no independent intent — it always acts *for a
principal* persona (P1–P7) across the platform's machine surfaces. It is the
executor, not the beneficiary, so it is **not** an 8th peer persona. Each
persona's Agent-mediated mode (§P1–P7 files) describes how an agent fulfills
that principal's intent.

## The 2026 reality for this platform
Agents are often the *actual operator*: the Owner runs enrichment via
`dlc-worker`; an analyst's agent runs the EDA; an engineer's agent opens the
PRs. So the platform must be designed to be operated by agents, not just read
by humans.

## What an agent-first surface requires (cross-cutting)
1. **Agent-first interfaces** — CLI, stable SQL views, JSON/API over human UI;
   the repo's thin-projector discipline already points here. An agent should
   reach the same canonical views a human dashboard reads.
2. **Verifiable, versioned outputs** — `label_version`, `prompt_hash`,
   self-versioning — so an agent can assert what changed and what's stale
   instead of trusting raw output.
3. **Governance for agent-created work** — agents (dlc/sdlc-worker) create the
   PRs; they are the ones most likely to skip the epic/US. The deferred
   hook/SQLite lane (ROADMAP Part 2: warn+notify on `github pr_create` without
   an epic reference) is really *agent* governance.
4. **Guardrails enforced on the agent** — spend caps, single-writer DBs,
   environment isolation — because an agent won't reflexively honor them the way
   a human would.

## Story semantics
Use `persona: P#` for the principal and note agent mediation in the body or a
`via: agent` field — never "As a dlc-worker" (executor ≠ beneficiary).

## Related
`personas/README.md` (model), ROADMAP Part 2 (governance), `AGENTS.md`.
