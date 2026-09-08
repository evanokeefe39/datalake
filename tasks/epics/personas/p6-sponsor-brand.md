# P6 — Sponsor / Brand

- **Canonical actor string:** sponsor/brand
- **Consolidates:** audit-layer client; the buyer panel's sponsor perspective
- **Status:** placeholder (audit product not yet real)

## Who
An external client that evaluates creators/posts for sponsorship and brand
safety. Consumes the audit layer (brand-safety flags, disclosed sponsorship),
not the growth analytics.

## Primary intent / goals
- Select which posts/creators are brand-safe for their own risk profile
  (brand-selectable flags: political, medical, financial, sexualized, …).
- Know when a flag is high-confidence vs uncertain, so they don't over-block on
  raw, low-precision output.

## Agent-mediated mode
An AI agent runs the brand-safety audit on the sponsor's behalf. It needs the
**explicit, enumerable `brand_safety_*` set** (modality-agnostic, brand-
selectable) and a **precision gate** — client-facing audit flags must never be
raw LLM output (recall>precision risk is unmeasured without human gold). The
persona's intent is safe, brand-specific screening; the agent must be prevented
from auto-blocking on low-confidence flags.

## Related
`enrich-facets` (brand-safety set), the design doc's brand/audit layer. Held as
placeholder until the audit product is scoped — do not build the P6 surface yet.
