# Harness coverage: rules vs hooks

Does the existing rule set cover the MECE learnings, and which hooks should exist?
All rules are KEPT. Nothing is deleted. This proposes additions only.

---

## 1. Method

The five MECE controls (`learnings-mece.md` §1) are the coverage grid. Each of the 20 existing
rules in `~/.omp/agent/rules/` was mapped onto them, plus the two worker configs
(`dlc-worker.md`, `sdlc-worker.md`). A control is covered only if a rule or config clause
**states an obligation whose violation is detectable**.

---

## 2. Coverage verdict

> **CORRECTED — see §7.** This verdict ignores rule→agent scoping. Six rules credited here are
> frontmatter-scoped to `[dlc-worker, main]` and never reach `sdlc-worker`. The claim below is
> accurate for `dlc-worker` and **overstated for `sdlc-worker`**. Read §7.2 for the per-agent
> matrix and §7.4 for the `agents:` frontmatter each new rule must carry.

**The rule set is strong on C4 and C5, and has real gaps in C1, C2 and C3.** It is not
complete. Five obligations are missing, and one existing rule is scoped too narrowly to bind.

### Covered — credit where due (do not duplicate these)

| Control | Rules that cover it |
|---|---|
| **C1 Specification** | `declared-dependencies` (every asset declares inputs/outputs), `contract-first-traceability` (name the contract + requirement), `andon-no-silent-failure`, `negative-space` |
| **C4 Sequencing** | `migration-ships-with-code`, `no-publish-then-fix`, `wap-audit-before-publish`, `idempotent-replay` — the strongest cluster in the set |
| **C5 Verification** | `run-dont-claim`, `validate-against-real-dev-run`, `qa-codified-every-layer`, `dormant-vs-broken`, `andon-no-silent-failure` |
| **C2 partly** | `change-review-blast-radius` (enumerate consumers and classify them) |

### The five gaps

**G1 — Every mechanism names its driver.** *Control: C1.*
No rule requires a retry, quarantine, reconciliation, or cleanup to name **an actor and a
trigger**. `declared-dependencies` orders assets by dependency; it does not ask "who fires
this?". This is exactly the ADR-0012 gap: a retry with `MAX_ATTEMPTS` and backoff and nothing
that mints a round, a quarantine table with no reader.
**Needed:** a rule — *every mechanism names the actor that drives it and the event that
triggers it; a mechanism with no driver does not exist yet.*

**G2 — Every created artifact names its consumer.** *Control: C1.*
`dlc-worker.md` step 2 requires enumerating consumers of what you **change**. Nothing requires
naming the consumer of what you **create**. A new module's consumer does not exist yet — and
that is precisely where all six system-level defects lived. `declared-dependencies` is the
nearest rule and is scoped to assets, not to modules or seams.
**Needed:** a rule — *a new component names its production consumer, or declares explicitly
that it has none yet and who will assign one.*

**G3 — Every contract has a named owner.** *Control: C2.*
No rule assigns ownership of a boundary. `change-review-blast-radius` enumerates consumers but
assigns nobody. This project had *owned seams* on paper (the plan §4 register) and still failed,
so ownership alone is insufficient — but it is still necessary, because an orphan check with no
owner has no addressee when it goes red.
**Needed:** a rule — *every contract/interface between units has a named owner; a contract
without an owner is a defect to surface, not a gap to leave.*

**G4 — One contract, one shape.** *Control: C3.*
No rule forbids two serializations or two implementations of the same boundary.
`metrics-derived-upstream` is the analogous rule for metrics (define once, consume everywhere)
but it is scoped to metrics only. The handle encoding (`'|'` vs `','`), the two qwen HTTP
clients, and the partition-key shape all live in this gap.
**Needed:** a rule — *one boundary has exactly one canonical representation; a second
serialization or a second client for the same service is a defect.*

**G5 — A gate declares what it cannot detect; existence is never acceptance.** *Control: C5.*
`run-dont-claim` is close but is framed as "run the thing", not "state your gate's blind spot"
or "an existence check is not an adoption check". `qa-codified-every-layer` requires checks but
not that they be falsifiable. The vacuous `test_state_compatibility.py` gate passed every
existing rule in the set. And §8.6 (the unreadable page certified by a verifier who could not
see) is uncovered entirely.
**Needed:** a rule — *every gate states the failure it cannot detect; a verifier that cannot
observe the artifact must not certify it; an existence claim is not an adoption claim.*

### One existing rule scoped too narrowly

`declared-dependencies` covers assets declaring inputs/outputs. The failure here was
**producer/consumer pairing at runtime** — a producer with no consumer and a consumer with no
producer. Widening that rule (or adding its sibling) to require an **orphan check** closes the
gap without changing what it already says.

---

## 3. Hooks to create

### Hard constraints from the OMP docs (these shape every design)

- **`ctx.hasUI` is `false` in headless, print, and subagent mode.** Every hook must work with no
  UI. No `confirm()`, no `select()` — those silently return `false`/`undefined` and would block
  unconditionally in a subagent.
- **`ExtensionAPI` is preferred over the legacy `HookAPI`.** The docs are explicit: "Use
  `ExtensionAPI` for new work; use `HookAPI` only if you are maintaining an existing hook module."
- **A `tool_call` handler returning `{ block: true, reason }` fails closed** — if a handler
  throws, the tool is blocked. So a buggy hook can halt all work. Hooks must be written
  defensively and must exit early and cheaply on non-matching input.
- **Discovery:** `.omp/hooks/pre/*.ts` and `.omp/hooks/post/*.ts` (project), or
  `~/.omp/agent/hooks/pre/*.ts` (user). Dedupe key is `${type}:${tool}:${name}`.
- These are **mechanical** checks only. Anything requiring judgment stays a rule or skill.

### H1 — Provider-name containment (catches all 12 bypass sites)

**Placement:** project (`<repo>/.omp/hooks/pre/provider-containment.ts`).
**Trigger:** `tool_call`, tools `edit` and `write`.
**Logic:** if the target path is not an adapter/seam module, scan the new content for
`gemini_batch.` or `qwen_client.`. Block with a message naming the seam entry point.
**Prevents:** the seam adopted by 1 of 4 production flows, 12 bypass sites.
**Risk:** false positives on legitimate adapter files — scope by path allowlist.

### H2 — Live-object materialization gate (catches code-only objects)

> **CORRECTED — see §8.4.** This spec named no source for "the declared new objects", so it was
> not implementable. The source is the **catalog** (`_DUCKDB_SPECS` keys + views), compared
> against the **live DB as an observation**. See §8.4 before implementing.

**Placement:** project. **Trigger:** `tool_call` on `bash` where the command matches
`git commit` or `gh pr create`.
**Logic:** read the declared new objects from a manifest; query the live database; block if any
declared object is missing or has zero rows.
**Prevents:** the 12 code-only objects; bronze with zero landed rows.
**Note:** this is the hook that would have blocked this migration's own "complete" claims.

### H3 — Contrast assertion on styled artifacts (catches the §8.6 class)

**Placement:** user-level (`~/.omp/agent/hooks/pre/report-contrast.ts`) — it applies to any repo.
**Trigger:** `tool_call` on `write`/`edit` where the path ends `.html`.
**Logic:** parse `<style>`, resolve `:root` custom properties, enumerate every
`color:` declaration, compute each against its background, block if any text colour is under
4.5:1 **or if any `var(--x)` is undefined**.
**Prevents:** the unreadable page; the undefined `--text`; the malformed `:root`; the two
low-contrast colours. Also catches the class where any `var()` has no definition.
**Why a hook and not a rule:** it is pure computation, needs no judgment, and the verifier in
that incident could not see.

### H4 — Destructive-DDL guard (catches the 391-row class and the zombie queue)

**Placement:** user-level.
**Trigger:** `tool_call` on `bash`.
**Logic:** block `DROP TABLE` / `TRUNCATE` / `DELETE FROM` (unqualified) when the target is a
live data path, unless the command carries an explicit approval marker.
**Prevents:** the documented incident where a numbered migration destroyed 391 real rows.
**Note:** the repo already has this rule in `tasks/lessons.md`; the rule did not bind because
nothing enforced it. Hooks are exactly where it belongs.

### H5 — Catalog target-vs-current reconciliation (catches the vacuous gate)

> **CORRECTED — see §8.3.** As written this was **circular**: if the catalog is the declaration,
> comparing it against "the target schema" passes in exactly the case it exists to catch — a
> vacuous hook built to detect a vacuous gate. The source of intent is the **DDL in the commit**,
> and check (a) is simply "no hand-written DDL". See §8.3.

**Placement:** project.
**Trigger:** `tool_call` on `bash` for `git commit`.
**Logic:** two checks, neither circular. **(a) No hand-written DDL:** every
`CREATE TABLE` / `CREATE VIEW` string in `src/` must be produced by the catalog generator
(`duckdb_ddl(...)`); fail on a raw SQL literal. **(b) Catalog follows the diff:** objects created
or dropped by DDL anywhere in this commit — including inside Python string constants — must be
added to / removed from `schemas.py` in the same commit.
**Prevents:** `test_state_compatibility.py` passing while asserting the old world — and it
catches the raw `CLASSIFICATION_DDL` literal (`classification.py:65`) that caused it.
**Design note — this is the corrected lesson.** A live-DB-only comparison would have PASSED in
this migration (catalog and live DB agreed). The source of intent is the **DDL in the commit**;
the catalog is never compared against itself. See §8.3.

### H6 — Acceptance-criteria shape lint (catches existence-shaped criteria)

**Placement:** project. **Trigger:** `tool_call` on `write`/`edit` for files under
`tasks/plans/` or `tasks/epics/`.
**Logic:** flag criteria matching existence patterns (`exists`, `is the only`, `is defined`,
`is created`) that have no paired assertion about data or adoption.
**Prevents:** the exit criteria that were satisfiable by a unit test of a registry.
**Caveat:** this one is heuristic and should WARN, not block.

### H7 — Unobservable-verification guard (catches the certified-but-unseen class)

**Placement:** user-level. **Trigger:** `tool_call` on `task` (a subagent dispatch).
**Logic:** if the dispatch prompt asks for visual/browser verification, require that the
dispatch also specifies a **mechanical assertion** (a ratio, a count, a parsed property) OR
block with a message that the acceptance is unobservable.
**Prevents:** "visual QA passed" certifying an unreadable page.
**Rationale:** this is the general form of §8.6 — the verifier's capability must cover the
acceptance.

### Priority order

Ranked by (damage prevented ÷ implementation risk):

1. **H4** destructive-DDL guard — highest damage, trivial logic, already a documented incident
2. **H1** provider containment — catches 12 defects, mechanically decidable
3. **H3** contrast assertion — caught this session's worst artifact, pure computation
4. **H5** catalog target reconciliation — catches the vacuous gate class
5. **H2** materialization gate — highest value but needs a manifest and DB access
6. **H7** unobservable-verification guard — general but heuristic
7. **H6** acceptance-shape lint — WARN only

---

## 4. What must stay a rule (hooks cannot do it)

Hooks are mechanical. These need judgment and belong in rules or skills:

- **G3 seam ownership** — assigning an owner is a human/design decision. No hook can decide it.
- **C2 contract ownership across units** — same.
- **The MECE control framing itself** — teaching, not enforcement.
- **"Name the production consumer of what you create"** — the *presence* of a name can be
  linted; the *correctness* of the name cannot.

**The division of labour:** a rule states the obligation; a hook enforces the mechanically
checkable part of it. A rule with no hook is a lesson that does not bind (this migration's
central finding). A hook with no rule is an unexplained failure.

---

## 5. Where each change lives

| Artifact | Location | Scope |
|---|---|---|
| New rules G1–G5 | `~/.omp/agent/rules/*.md` | User-wide (all repos) |
| H1, H2, H5, H6 | `<repo>/.omp/hooks/pre/*.ts` | Project (needs repo paths + DB) |
| H3, H4, H7 | `~/.omp/agent/hooks/pre/*.ts` | User-wide (repo-agnostic) |
| Orchestrator-side learnings | **no home yet — the gap** | See `postmortem-implementation-drift.md` §10.4 |

Note: `~/.omp/agent/hooks/` does not exist yet — it must be created.

---

## 6. The one thing no rule or hook fixes

`postmortem-implementation-drift.md` §10.3 established the structural gap: the learnings that
would have prevented this are **orchestrator-side**, and no agent config owns the orchestrator.
The `dlc-worker` and `sdlc-worker` configs both hand responsibility *up* (step 5: "the
ORCHESTRATOR owns the reviewer gate"). Adding rules and hooks helps, but until the orchestrator
itself is configured — its dispatch checklist and its acceptance gate — the same class of
failure remains available.

---

## 7. CORRECTION: coverage is per-agent, and §2 overstated it for `sdlc-worker`

§2 claimed "the rule set is strong on C4 and C5". That is true for `dlc-worker` and
**overstated for `sdlc-worker`**, because most of the rules credited are frontmatter-scoped via
`agents:` and never reach it.

### 7.1 Actual scoping (read from each rule's frontmatter)

| Rule | `agents:` | Reaches `sdlc-worker`? |
|---|---|---|
| `change-review-blast-radius` | dlc-worker, sdlc-worker, main | yes |
| `decompose-lean-units` | main, dlc-worker, sdlc-worker | yes |
| `metrics-derived-upstream` | frontend, ux-worker, sdlc-worker, dlc-worker | yes |
| `migration-ships-with-code` | dlc-worker, sdlc-worker, main | yes |
| `new-source-rule` | dlc-worker, sdlc-worker, main | yes |
| `offload-long-runs` | main, dlc-worker, sdlc-worker, implementer | yes |
| **`contract-first-traceability`** | dlc-worker, main | **NO** |
| **`declared-dependencies`** | dlc-worker, main | **NO** |
| **`provenance-on-derived`** | dlc-worker, main | **NO** |
| **`qa-codified-every-layer`** | dlc-worker, main | **NO** |
| **`validate-against-real-dev-run`** | dlc-worker, main | **NO** |
| **`wap-audit-before-publish`** | dlc-worker, main | **NO** |
| `andon-no-silent-failure` | (none — all) | yes |
| `dormant-vs-broken` | (none — all) | yes |
| `idempotent-replay` | (none — all) | yes |
| `negative-space` | (none — all) | yes |
| `no-publish-then-fix` | (none — all) | yes |
| `run-dont-claim` | (none — all) | yes |

### 7.2 Corrected coverage matrix

| Control | `dlc-worker` coverage | `sdlc-worker` coverage |
|---|---|---|
| **C1 Specification** | strong (`declared-dependencies`, `contract-first-traceability`, `andon-no-silent-failure`, `negative-space`) | **weak** — only the always-apply subset (`andon`, `negative-space`) |
| **C2 Accountability** | `change-review-blast-radius` | same (shared) |
| **C3 Interface** | `metrics-derived-upstream` (metrics only) | same |
| **C4 Sequencing** | strong (`migration-ships-with-code`, `no-publish-then-fix`, `wap-audit-before-publish`, `idempotent-replay`) | **moderate** — loses `wap-audit-before-publish` |
| **C5 Verification** | strong (`run-dont-claim`, `validate-against-real-dev-run`, `qa-codified-every-layer`, `dormant-vs-broken`, `andon`) | **moderate** — loses `validate-against-real-dev-run` and `qa-codified-every-layer`, arguably the two most important for a feature agent |

**The `sdlc-worker` finding is the sharper one.** A feature agent that lacks
`validate-against-real-dev-run` and `qa-codified-every-layer` is exactly an agent that will
report "tests pass" on an unrun path — the failure mode this whole exercise is about. Those two
rules were scoped to the data agent because their *wording* is data-shaped ("Asset checks on
every asset"), but their *obligation* is universal.

### 7.3 Corrections to the missing obligations (§3)

Two of the six scoping gaps are arguably misfiled, not missing:

- `validate-against-real-dev-run` — its obligation ("run the thing and observe") is universal.
  Proposed: **widen `agents:` to include `sdlc-worker`**, and reword the data-specific examples
  into a general obligation with a data example. This is an ADDITION to scope, not a deletion.
- `qa-codified-every-layer` — same. The data wording ("asset checks on every asset") has a
  feature analogue ("every behavioral contract has a test that fails on a plausible bug").
  Proposed: **widen + add the analogue clause.**

Genuinely data-scoped and correctly so — leave alone: `provenance-on-derived`,
`contract-first-traceability`, `declared-dependencies`. Their obligations do not have a meaningful
feature analogue.

### 7.4 Required `agents:` frontmatter for the new rules G1–G5

"Do not file them all as user-wide" is correct — placement must name the agents.

| New rule | Proposed `agents:` | Why |
|---|---|---|
| **G1 — mechanisms name their driver** | `[dlc-worker, sdlc-worker, main]` | Any agent that builds a retry/quarantine/reconciliation. Data agents especially. |
| **G2 — created artifacts name their consumer** | *(omit `agents:` — applies to all)* | Universal: any agent creating a component faces this. This is the gap that caused all six system-level defects. |
| **G3 — every contract has a named owner** | *(omit `agents:` — applies to all)* | Universal. Ownership is not technology-specific. |
| **G4 — one contract, one shape** | *(omit `agents:` — applies to all)* | Universal. Two serializations of one boundary is a defect everywhere. |
| **G5 — gates declare blind spots; unobservant verifiers must not certify** | *(omit `agents:` — applies to all)* | Universal, and must reach `reviewer` too, which is where certification happens. |
| **G6 — orphan check (runtime producer/consumer pairing)** | `[dlc-worker, main]` | Data-topology specific. Proposed as a *widening/companion* of `declared-dependencies`, not a new standalone rule. |

Rationale for the split: rules that encode a **technology-independent invariant** omit `agents:`
and apply everywhere; rules whose obligation only exists in a data pipeline name the data agents
explicitly. Filing G2–G5 as `[dlc-worker, main]` would have re-created this exact bug.

### 7.5 Consequence for §5

§5's table said "user-wide (all repos)" for G1–G5. That is the *location* (where the file lives),
not the *scope* (which agents load it). Both must be specified. Location: `~/.omp/agent/rules/`
for all new rules, since none are repo-specific. Scope: per §7.4 above.

---

## 8. CORRECTION: H2 and H5 had no declaration source — and H5 was circular

The advisory is correct on both counts, and the fix is better than expected because **this
codebase already has the right mechanism and the defect was a bypass of it.**

### 8.1 What the codebase actually does

`schemas.py` is designed as a **single source of truth**, with DDL *generated from it*:

- `duckdb_ddl("gold_analyses")` (`defs/enrichment/assets.py:27`) — DDL rendered from the catalog.
- `schemas.py:21`, verbatim: *"runtime assets execute, so the DDL can never drift from the
  catalog."*
- `schemas.py:385`: "Return DDL for every DuckDB table in the catalog."

So drift is designed to be **impossible** — if you declare an object, the DDL comes from the
catalog and the two cannot disagree.

### 8.2 The defect was a bypass, not a missing declaration

`defs/enrichment/classification.py:65` defines:

```python
CLASSIFICATION_DDL = """CREATE TABLE IF NOT EXISTS silver_content_classification (
```

That is a **raw, hand-written SQL literal** — not `duckdb_ddl("silver_content_classification")`.
It never entered `schemas.py`. This is precisely why the compatibility gate was vacuous: the
catalog had no knowledge of `silver_content_classification`, so "does the catalog match the live
DB?" was *true* — both lacked it.

**The invariant the codebase claims ("the DDL can never drift from the catalog") was violated by
the one DDL that didn't use the generator.** That is a mechanically detectable defect.

### 8.3 H5, corrected — non-circular, two checks

The circularity the advisory identified: if the catalog is both the declaration *and* the thing
compared against the target, the check passes in exactly the case it exists to catch. Resolved by
taking the **DDL in the commit as the source of intent**, and the catalog as the declaration that
must follow it.

**(a) No hand-written DDL.** Every `CREATE TABLE` / `CREATE VIEW` string in `src/` must be
produced by the catalog generator (`duckdb_ddl(...)`), not written as a raw SQL literal.
Mechanically: grep `src/` for DDL statements outside the catalog module; fail on a match.
**Catches `CLASSIFICATION_DDL` directly — the actual defect, on the day it was written.**

**(b) Catalog follows the diff.** Extract every object created or dropped by DDL anywhere in the
commit's diff — including inside Python string constants — and require `schemas.py` to list every
created object and to stop listing every dropped one, **in the same commit**.
**Catches "created but never registered."** This is the advisory's suggested source and it is the
right one.

This is non-circular because the two sides are different artifacts: the **DDL expresses intent**,
the **catalog expresses declaration**, and they are required to agree. Neither is derived from the
other at check time.

### 8.4 H2, corrected — the source is the catalog, and that is not circular

The declared object set is the **catalog** (`_DUCKDB_SPECS` keys plus the view list). The check
then queries the **live database** — an *observation*, not a declaration. Declaration-vs-observation
is not a circular comparison; the earlier formulation was circular only because it compared a
declaration against itself.

- For every catalog object: it must exist in the live DB.
- For every catalog object newly added in this commit: it must have rows, or an explicit,
  recorded justification for zero rows (a landing table that has never run is *unexercised*, not
  *broken* — cf. `panel-data.md` F1).
- H2 and H5(a) compose: H5 makes the catalog complete, H2 proves the catalog's objects were
  actually materialized.

### 8.5 Why this is a better hook than the original

| | Original H2/H5 | Corrected |
|---|---|---|
| Declaration source | unnamed ("a manifest") | the catalog, whose completeness H5(a) now enforces |
| Circularity | H5 compared a declaration against itself | DDL intent vs catalog declaration — independent artifacts |
| Catches the real defect | no | **yes — the raw `CLASSIFICATION_DDL` literal** |
| Enforces an existing invariant | no | **yes — `schemas.py:21`'s stated "can never drift" guarantee** |

The general lesson, which generalises past this repo: **when a codebase has a single-source-of-truth
generator, the mechanical check is not "is the output correct" — it is "did anything bypass the
generator".** Bypass detection is decidable, cheap, and catches the whole class. Checking output
correctness requires knowing the intended output, which is the circularity trap.
