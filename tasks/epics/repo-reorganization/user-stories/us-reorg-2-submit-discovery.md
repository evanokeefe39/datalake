---
id: US-REORG-2
epic: E-REORG
persona: P1
status: Done
---
# US-REORG-2 — The drain folds into submit, and a sensor closes the trigger gap

- **Epic:** E-REORG
- **Status:** Done — branch `refactor/submit-discovery`
- **Branch:** `refactor/submit-discovery`
- **Relates to:** US-EENG-4 (the in-flight guard this preserves), ADR-0014 (D1, amended here),
  ADR-0016 (the decision), ADR-0012/0013 (orchestration state)
- **Source:** `tasks/plans/repo-reorganization.md`

## Story

**As a** pipeline operator, **I want** enrichment discovery to happen inside the submit run
rather than in a separate enqueue asset driven by a schedule, **so that** there is one
place that decides what to enrich and one place that guards against submitting it twice —
and so a pending backlog actually triggers a run instead of waiting to be noticed.

## Acceptance criteria (binary)

- AC1: `ig_posts_gen_batches` no longer exists — not as an asset, not in `definitions.py`,
  not in `platform/schedules.py`'s `daily_medallion` target list.
- AC2: `defs/ig_enriched/slv/workloads.py` defines a `Workload` dataclass
  (`name`, `platform`, `silver_table`, `prompt_hash`, `candidates`, `build_item`, `estimate`)
  and a `WORKLOADS: dict[str, Workload]` keyed by the landing workload name.
- AC3: `engine/submit.py` iterates `WORKLOADS`, and for each: runs `candidates`, reads the
  in-flight set ONCE via `partitions.in_flight_partitions(instance)`, materializes the
  `enrichment_submitted` placeholder for each new key BEFORE calling the provider, builds
  items via `build_item`, and issues ONE `adapter.submit(items, job_spec=...)` per run.
- AC4: The `MAX_ROUNDS` retry-budget guard moved out of `discover_pending` into the submit
  fold, with its existing message ("the retry budget is exhausted; refusing to submit
  again"). A key at `attempt_round >= MAX_ROUNDS` raises rather than being silently skipped.
- AC5: Discovery and the double-submit guard read the SAME `in_flight_partitions`
  derivation — no second code path derives in-flight state. ADR-0016 records this invariant.
- AC6: `SubmitConfig(Config)` replaces `GoldConfig`, carrying `post_ids: list[str] = []`,
  `whole_corpus: bool = False`, `limit: int = DEFAULT_SUBMIT_LIMIT`.
- AC7: The classification workload reproduces today's prompt byte-for-byte:
  `f"{IG_GOLD_PROMPT}\n{caption}"`.
- AC8: `engine/sensor.py` exports `enrichment_submit_sensor` with tags
  `{"adr": "0012", "driver": "submit"}`, a 300 s default interval overridable via
  `ENRICHMENT_SUBMIT_SENSOR_INTERVAL_SECONDS`. It returns without a run when the pending set
  is empty; when non-empty it fails LOUDLY if the service health gate fails, then yields
  exactly one `RunRequest` carrying an `enrichment/submit_partitions` tag listing the
  sorted pending keys. It is registered in `definitions.py`.
- AC9: `ADRs 0016` is Accepted and indexed; ADR-0014's index row is annotated and its D1
  section carries a pointer line. Its decision text is NOT edited.
- AC10: `check_approved_classification_coverage`'s operator-facing description names the
  submit sensor instead of the deleted asset.
- AC11: `uv run dagster definitions validate` passes with both sensors registered.

## Definition of done

- [ ] Every AC above holds.
- [ ] `uv run python -c "from orchestration import definitions as d; names=[a.name for a in d.all_assets]; assert 'ig_posts_gen_batches' not in names"` prints the asset count with no assertion error.
- [ ] With `dagster dev` running, one `enrichment_submit_sensor` tick is observed: it logs
      the pending count and either returns "nothing to do" or requests exactly one run whose
      tag lists the pending keys.
- [ ] `uv run ruff check services/ packages/ tests/` is clean.
- [ ] Squash-merged to `main` via PR.

## Tests

| AC | Test |
|---|---|
| AC2, AC3, AC6 | `tests/unit/enrichment/test_submit_discovery.py` |
| AC4 | `tests/unit/enrichment/test_partitions_retry.py` |
| AC5 | `tests/unit/instagram/test_drain_inflight_guard.py` |
| AC8 | `tests/unit/enrichment/test_sensor.py` (or `test_submit_sensor.py`) |
| AC1, AC11 | the graph-shape assertion + `dagster definitions validate` |
