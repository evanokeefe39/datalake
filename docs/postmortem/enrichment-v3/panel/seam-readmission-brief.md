# BRIEF UNDER REVIEW — Bin Gemini, re-admit it through the seam

Owner proposal (2026-09-14, verbatim intent): *it's gonna be easier to temporarily bin
Gemini and then bring it back according to the interface / seam we defined.*

Status: PROPOSAL. This brief exists so the panel reviews the same artifact. It is not a
plan and carries no decisions.

---

## 1. What is being proposed

Two moves, in order:

1. **Bin Gemini *usage* now.** Retire the two Dagster entry points that drive the legacy
   Gemini batch path — `submit_gemini_batches_job` (`submit.py:227`) and
   `gemini_batch_harvest_sensor` (`harvest.py:419`). Delete the nine direct Gemini call
   sites that bypass the seam. Keep the Gemini *code*: `gemini_batch.py` (the client),
   `DirectBatchAdapter`, and `build_adapter("gemini")`.
2. **Bring Gemini back later through the seam**, i.e. re-admit it as a provider behind the
   ADR-0008/0009 interface, rather than restoring the nine call sites that were deleted.

The stated motivation: avoiding the per-site migration work AND avoiding years of
old/new coexistence. Instead of adapting nine call sites to the seam while both providers
run, remove the provider and re-admit it once, properly, through the interface that was
designed for exactly this.

## 2. What already exists (the interface being appealed to)

- **The seam** — three verbs over an HTTP contract: `submit` / `poll-to-terminal` /
  `retrieve`. Canonical terminal-state vocabulary: `pending` / `processing` /
  `completed` / `failed`. Reference: `src/datalake/defs/enrichment/seam.py`
  (`run_lifecycle` at :145).
- **`ProviderAdapter` Protocol**, with two implementations:
  - `ServiceBackedAdapter` — qwen-batch-service. The service is what makes the
    *synchronous* OpenRouter/qwen provider look async.
  - `DirectBatchAdapter` — Gemini, natively async.
- **`build_adapter(name)`** — the only place a provider is named. Swaps by config string.
  **CORRECTED 2026-09-14 by the panel**: the registered names are `service_backed` and
  `direct_batch` — there is **no** `"gemini"` key. `seam.register_adapter("service_backed", …)`
  is the only registration in the file (`seam.py:113,119`). This correction also applies to the
  remediation plan's W-FREEZE, which asked for a test that `build_adapter("gemini")` constructs —
  that test would fail on a name that does not exist.
- **ADR-0008 / ADR-0009** — one seam, one lifecycle, two verbs' worth of providers.
- **ADR-0013** — the seam keeps **no ledger**; the service owns its job store and Dagster
  polls it. No shared `external_jobs` table.
- **ADR-0014 (W0)** — dynamics: the harvest run reports `enrichment_harvested`; the same
  run mints round-N+1 retry keys. Partition key is
  `<workload>\x00r<N>\x00<post_id>` — self-describing, derivable both ways.

## 3. What is already known to be broken or missing

- `seam.run_lifecycle` has **ZERO production callers** today — the only occurrence of the
  name in `src/`. The provenance of the interface is a **spike**
  (`~/repos/enrichment-spike/spike_defs/adapter.py`), never wired into this repo.
- 9 of the 11 provider call sites bypass the seam; only `facets_batch.py:318,322` (qwen)
  go through `build_adapter`.
- The `max_tokens` defect: the seam's submit verb **cannot express a per-call parameter**,
  which the bypass comments cite as the reason for bypassing. To be fixed by adding
  `JobSpec` to the submit verb.
- The two providers have **divergent terminal predicates** today (`gemini_batch.is_terminal`
  vs qwen's state handling) — one of the named seam defects.
- Handle serialization diverges: `"|"` (`submit.py:170`) vs `","` (`adapters.py:280`).
  **Citations corrected by the panel**: the adapter side is `adapters.py:287`. The qwen bypass is
  `facets_batch.py:292` via `build_adapter` (an earlier draft cited :318,322).
- **Live media facts the proposal must reckon with** (read from `ops.sqlite`, 2026-09-14):
  `media_metadata` 5,613 rows of which **5,419 have EXPIRED File API URIs** (max `expires_at`
  2026-09-11T00:37Z), and `media_cache` holds 27,748 rows. An earlier draft of this brief said
  2,768 — it was wrong by 10×. Consequence for the proposal: re-admitting Gemini cannot reuse
  any existing File API upload; media must be re-uploaded, so the re-admission cost includes a
  full media re-seed, not merely a credential check.
- `gold_analyses` is the legacy path's write target; the old path has been dormant since
  ~2026-09-05 (zero instigator records; last writes 09-05/09-08/09-09).

## 4. Questions the panel must answer

Each seat should answer these, from its own discipline, with citations:

**Q1 — Is the interface actually sufficient?** Does the ADR-0008/0009 seam, *as specified*,
admit a second provider cleanly — or was it designed around one provider's shape and the
other only nominally? Name what would have to change in the interface for Gemini to be
re-admitted through it without re-introducing a bypass.

**Q2 — What must be preserved during the bin so the return is possible?** Concretely: which
artifacts, tests, fixtures, contracts and invariants must survive the removal — and what is
the minimum set whose loss would make "bring it back through the seam" a rewrite rather
than a re-admission?

**Q3 — What is the actual re-admission procedure?** Sketch it: what is added, where, gated
on what, verified how. Distinguish "the adapter constructs" from "the provider is live" —
they are not the same claim.

**Q4 — What rots in the interval?** The Gemini path will be unused for an unknown period.
Name the specific bit-rot risks (SDK drift, API version drift, credential expiry, fixture
staleness, untested code paths) and the cheapest mitigations that keep the return real
rather than aspirational.

**Q5 — Where does this proposal lose money, data, or correctness?** In particular: does
"temporarily bin" create a period where **no** provider is live (qwen-only), and what
happens to the corpus during it? Does it re-bill? Does it orphan provenance, partition
keys, or quarantine rows?

**Q6 — Is "temporarily" honest?** If the honest answer is "this is a permanent retirement
with a fig leaf", say so — a re-admission path that will never be exercised is dead weight
that looks like a capability. If it IS realistic, name the trigger that would bring Gemini
back and who decides.

## 5. Constraints on the review

- **Read-only.** This file is the deliverable per seat; do not modify `src/` or the plan.
- Ground every claim in a file:line or a live-DB observation; mark inference as inference.
- Where the proposal is right, say so plainly — a panel that only finds faults is as
  useless as one that finds none. But a finding must be evidenced, not balanced for form.
- The relevant rules are `edge-contract`, `migration-ships-with-code`, `dormant-vs-broken`,
  `contract-first-traceability`, `idempotent-replay`, and `new-source-rule`. The relevant
  hooks are `boundary-gate` and `migration-guard`.
- Do not re-litigate the post-mortem. The subject is this proposal only.
