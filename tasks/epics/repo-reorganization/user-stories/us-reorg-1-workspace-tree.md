---
id: US-REORG-1
epic: E-REORG
persona: P1
status: Open
---
# US-REORG-1 — The repository is a uv workspace and every module lives where its role says

- **Epic:** E-REORG
- **Status:** Open
- **Branch:** `refactor/workspace-tree`
- **Relates to:** E-ENRICH-ENGINE (the code being moved), ADR-0015 (the decision),
  ADR-0009 (the retired provider removed here)
- **Source:** `tasks/plans/repo-reorganization.md`

## Story

**As a** maintainer of this repository, **I want** the layout to state what each module is
and which component owns it, **so that** I can place new work without re-deriving the
system's boundaries — and so a retired provider can no longer be selected by configuration.

## Acceptance criteria (binary)

- AC1: `src/` does not exist; the code location is `services/orchestration/src/orchestration/`,
  and `services/orchestration/pyproject.toml` declares it a workspace member.
- AC2: `packages/opsdb/` holds the ops.sqlite contract: the SQLite half of the schema
  catalog (`schema.py`), the roster (`roster.py`), the media-cache row contract
  (`media_cache.py`), and a `merges.py` that documents where the ledger operations land.
  `packages/storage/` holds `filesystem.py` + `keys.py` as the R2 seam.
- AC3: The root `pyproject.toml` is a virtual workspace project
  (`[tool.uv] package = false`, `members = ["services/*", "packages/*"]`) and
  `[tool.dagster] module_name = "orchestration.definitions"`.
- AC4: Every `datalake.` import is gone repo-wide; `uv run python -c "import orchestration"`
  succeeds and all 64 `orchestration.defs` modules import.
- AC5: `orchestration.defs.engine/` holds the cross-domain machinery (provider, service
  adapter, partitions, submit, harvest, sensor, landing, media, silver_rt) and
  `orchestration.defs.ig_enriched/slv/` holds only Instagram payloads (prompts, schemas,
  classification, visual, text, audio, quarantine, workloads, checks).
- AC6: `orchestration.defs.serving/` is five modules — `dims.py`, `metrics.py`, `marts.py`,
  `views.py`, `checks.py` — each exporting `ASSETS`; `definitions.py` composes
  `[*dims.ASSETS, *metrics.ASSETS, *marts.ASSETS, *views.ASSETS]`.
- AC7: `orchestration.defs.ig_core/` holds the Instagram pipeline split by table family
  (`bnz/scrape.py`, `slv/{posts,profiles,comments,labels,checks}.py`).
- AC8: `gemini_batch.py`, `DirectBatchAdapter`, `GeminiResource`, `GeminiTier`,
  `GeminiTierConfig`, `detect_provider`, and `google-genai` are deleted;
  `engine/service_backed.py` exports `PROVIDER_NAME = "service_backed"` and the three seam
  callers use `build_adapter(service_backed.PROVIDER_NAME)`. No caller selects a provider
  from configuration.
- AC9: `src/datalake/cli/` and `scripts/run_pipeline.py` are deleted; the CLI's two unique
  operations (a read-only state dump and a watermark reset) are documented in
  `docs/operations/runbook.md` with their single-writer warning.
- AC10: Every `__init__.py` under `orchestration/defs/` and `packages/*/src/` is a docstring
  and nothing else — no imports, no assignments, no side effects — enforced by
  `tests/operational/test_thin_init_files.py`.
- AC11: `platform/paths.py` resolves the repo root by walking to the `.git` marker, and
  `tests/unit/orchestration/test_paths_anchor.py` asserts `DATA_DIR == <repo>/data` with
  `IG_DATA_DIR` unset.
- AC12: `migrations/migrate_drop_prompt_registry.py` archives the table to
  `data/lake/archive/prompt_registry/<utc>/`, asserts the export count equals the live
  count in the same run, then retires it; the `prompt_registry` spec is gone from the
  catalog and `check_prompt_currency` no longer takes an ops resource.
- AC13: `uv run dagster definitions validate` passes for code location
  `orchestration.definitions`.
- AC14: `uv run ruff check services/ packages/ tests/ scripts/ migrations/` is clean.
- AC15: ADR-0015 is Accepted, indexed in `docs/architecture/adr/README.md`, and records the
  tree, the eight conventions, the rename, and the import-time `load_dotenv()` exception.

## Definition of done

- [ ] Every AC above holds, each verified by its named command or test.
- [ ] `uv lock && uv sync --dev` succeeds from a clean environment.
- [ ] The residual-reference grep returns nothing:
      `grep -rn "src/datalake\|datalake\.defs\|datalake\.cli" services/ packages/ scripts/ migrations/ tests/ dashboard/ .github/`
- [ ] The pytest suite's import paths are retargeted (mechanical only — test assertions are
      the next phase's work, per the epic's scope note).
- [ ] `tasks/plans/repo-reorganization.md` committed; epic registered in `tasks/epics/README.md`.
- [ ] Squash-merged to `main` via PR.

## Tests

| AC | Test |
|---|---|
| AC10 | `tests/operational/test_thin_init_files.py` |
| AC11 | `tests/unit/orchestration/test_paths_anchor.py` |
| AC12 | `tests/operational/test_state_compatibility.py` (`_STALE_SQLITE_TABLES` carries `prompt_registry`) |
| AC4, AC13 | the import sweep + `dagster definitions validate` (the structural gate) |
| AC8 | `tests/unit/enrichment/test_seam_registration.py` (registration) and the seam-purity check |
