# P5 — Reviewer / Auditor

- **Canonical actor string:** reviewer/auditor
- **Consolidates:** reviewer, auditor
- **Status:** active (verification + audit)

## Who
Independently verifies that work matches its acceptance criteria and that
derived signal is correct enough to trust. Where output is decision-grade
(brand-safety flags, faithfulness), the auditor is the guard against
over-prediction and silent staleness.

## Primary intent / goals
- Check agent/work output against binary AC/DoD — never trust self-reports.
- Audit brand-safety and visual faithfulness before any client-facing flag is
  believed; understand the residual risk (e.g. folded-summary faithfulness).
- See stale/versioned data loudly (label_version, prompt_hash) rather than
  trusting raw output.

## Agent-mediated mode
An AI agent (`reviewer`, fresh-context) does the verification pass. It needs:
evidence-backed, reproducible checks; self-versioned outputs it can assert
against; explicit precision gates on audit flags. The persona's intent is
independent confidence; the agent must be structured to *disprove* the work,
not rubber-stamp it.

## Related
`enrich-facets` (validator, reserved keys), `enrich-summaries` (US-ESUM-2
faithfulness gate), `enrich-labels` (self-versioning), `AGENTS.md` review
interface.
