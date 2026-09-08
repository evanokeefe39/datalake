# Personas

Canonical, distinct user personas for this platform. User stories reference a
persona via `persona:` front matter (P1–P7) instead of free-text "As a …", so
roles are consistent and queryable across epics.

## Two actor kinds — principals and executors

- **Personas (P1–P7)** are *principals*: who wants the outcome and benefits from
  it. They set intent.
- **An AI Agent is a transitive actor, not a persona.** It has no independent
  intent — it always acts *for a principal* across the platform's machine
  surfaces. Treat it as the execution channel + executor; see
  [`ai-agent.md`](ai-agent.md). Each persona below carries an **Agent-mediated
  mode** describing how an agent fulfills that persona's intent and what the
  platform must expose for it.
- **Worker agents** (`dlc-worker`, `sdlc-worker`, `reviewer`) are the harness's
  executors. They are *ownership* (an epic's `Owner:`) — never a story's
  beneficiary. Stories written "As a dlc-worker" should be re-attributed to a
  principal persona.

## Registry

| ID | Persona | Consolidates (actor strings) | Agent-mediated mode? |
|---|---|---|---|
| [P1](p1-pipeline-operator.md) | Pipeline Operator | pipeline operator, operator, orchestrator | Yes — dlc-worker |
| [P2](p2-platform-engineer.md) | Platform Engineer | developer, maintainer | Yes — sdlc-worker/implementer |
| [P3](p3-growth-analyst.md) | Growth Analyst | analyst, creator-growth analyst, content analyst | Yes — query/EDA agent |
| [P4](p4-dashboard-user.md) | Dashboard User | dashboard user | Some (NL→SQL) |
| [P5](p5-reviewer-auditor.md) | Reviewer / Auditor | reviewer, auditor | Yes — reviewer/scout |
| [P6](p6-sponsor-brand.md) | Sponsor / Brand | (audit-layer client, buyer panel) | Yes — audit agent |
| [P7](p7-owner-principal.md) | Owner / Principal | "the user" | Yes — delegates to all others |

Cross-cutting: [AI Agent](ai-agent.md) (transitive actor + agent-first surface
requirements).
