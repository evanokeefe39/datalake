"""Dagster-native enrichment submit — the sole discovery actor (ADR-0012/0016).

Discovery and the in-flight guard are ONE derivation, and it lives here. A
submit run:

1. asks each workload in scope for its candidates (eligibility queries in
   ``ig_enriched/slv/workloads.py``);
2. takes ONE read of the in-flight set off the Dagster instance
   (``partitions.in_flight_partitions``) and reads it against those candidates
   — the guard and the discovery cannot disagree, because they are the same
   snapshot;
3. applies the ``MAX_ROUNDS`` guard, which is how a stuck partition surfaces:
   a key already at the ceiling is a loud failure, never a silent stall;
4. materializes the ``enrichment_submitted`` placeholder per surviving post —
   so the NEXT run's guard suppresses them — and builds one seam ``Item`` per
   post, whose ``custom_key`` IS the partition key so harvest maps results
   back with no stored mapping;
5. makes ONE ``adapter.submit(items, job_spec=...)`` call PER WORKLOAD (bounded)
   — each workload carries its own job-level options — and records the
   returned handle as metadata on each covered partition — Dagster-native
   state, no ledger (ADR-0013).

Ordering is load-bearing in two directions: the placeholder is materialized
AFTER building — a candidate that cannot be built must never get a partition,
or harvest waits forever on a handle that never covers it — and BEFORE the
POST, so a crash between them leaves a post visibly in flight (recoverable)
rather than invisible (lost).

There is NO queue read anywhere — ``batch_jobs``/``batch_items`` are retired
(ADR-0012) — and no drain asset: ``ig_posts_gen_batches`` is deleted (ADR-0016).
An unbuildable post becomes a TERMINAL FAILURE (an ``ok=False`` bronze row +
harvested partition + retry mint when the cause may clear) — never a silent
skip that strands its partition in flight forever.

Every provider call goes through the seam adapter; nothing here names a
provider.
"""

import logging

from dagster import AssetKey, AssetMaterialization, Nothing, Out, job, op

from orchestration.defs.engine import harvest, landing
from orchestration.defs.engine.partitions import (
    MAX_ROUNDS,
    SUBMITTED_ASSET_NAME,
    in_flight_partitions,
    parse_partition_key,
    partition_key,
    post_partition_state,
)
from orchestration.defs.engine.provider import DEFAULT_JOBSPEC, Item, JobSpec
from orchestration.defs.ig_enriched.slv import workloads as _workloads
from orchestration.defs.ig_enriched.slv.workloads import (
    SubmitConfig,
    UnbuildablePostError,
    Workload,
    workloads_for,
)
from orchestration.defs.platform.resources import DuckDBResource, SQLiteResource

logger = logging.getLogger("enrichment.submit")

#: Bounded runs: at most this many posts submit per run.
DEFAULT_SUBMIT_LIMIT = 250

_SUBMITTED_KEY = AssetKey(SUBMITTED_ASSET_NAME)


class _TerminalFailure:
    """A candidate that can never reach the provider this cycle."""

    __slots__ = ("key", "error", "retryable", "workload", "post_id")

    def __init__(
        self, *, key: str, workload: str, post_id: str, error: str, retryable: bool
    ) -> None:
        self.key = key
        self.workload = workload
        self.post_id = post_id
        self.error = error
        self.retryable = retryable


def _in_flight_by_post(instance) -> set[str]:
    """The in-flight set, projected to post_ids — ONE read per run.

    Raises on a malformed key: keys are the orchestration record, and an
    unreadable one means the key grammar was violated upstream.
    """
    posts: set[str] = set()
    for key in in_flight_partitions(instance):
        try:
            parsed = parse_partition_key(key)
        except ValueError as exc:
            raise RuntimeError(
                f"unparseable in-flight partition key {key!r}: {exc}"
            ) from exc
        posts.add(parsed.post_id)
    return posts


def _guard_round(instance, workload: str, post_id: str, key: str) -> None:
    """Refuse to submit a post whose retry budget is exhausted.

    ``post_partition_state`` derives the next round from the materialized
    keys, so this is the same derivation the retry driver mints from. The
    test keys on ``next_round`` alone: a post whose next round has reached
    the ceiling has spent its budget, whether or not anything is currently
    in flight for it. Gating this on ``suppressed`` as well would make the
    raise unreachable — the caller skips in-flight posts before calling here
    — so a partition stuck at the ceiling would be re-submitted forever.
    """
    state = post_partition_state(instance, workload, post_id)
    if state.next_round >= MAX_ROUNDS:
        raise RuntimeError(
            f"partition {key!r} is at round >={MAX_ROUNDS} "
            f"(MAX_ROUNDS={MAX_ROUNDS}) — the retry budget is exhausted; "
            "refusing to submit again"
        )


def build_items(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    work: list[tuple[Workload, dict, str]],
) -> tuple[list[tuple[Workload, Item]], list[_TerminalFailure]]:
    """Build a seam Item for each buildable candidate; failures stay loud.

    ``work`` is ``(workload, candidate, partition_key)``. Returns
    ``(built, failures)`` where each built entry CARRIES the workload that
    produced it.

    The workload travels WITH its item deliberately. An earlier version
    returned a bare item list and the caller zipped it against the candidate
    list positionally — which misaligns the moment any candidate fails to
    build (failures are filtered out of the item list but remain in the
    candidate list), handing a later item the wrong workload AND leaving a
    never-built partition materialized as submitted, so it looked in flight
    forever while harvest waited on a handle that never covered it.
    """
    built: list[tuple[Workload, Item]] = []
    failures: list[_TerminalFailure] = []
    with duckdb.get_connection() as conn:
        for workload, candidate, key in work:
            post_id = candidate["post_id"]
            try:
                item = workload.build_item(ops, conn, candidate)
            except UnbuildablePostError as exc:
                failures.append(
                    _TerminalFailure(
                        key=key,
                        workload=workload.name,
                        post_id=post_id,
                        error=str(exc),
                        retryable=exc.retryable,
                    )
                )
                continue
            except Exception as exc:  # noqa: BLE001 — accounted, then re-surfaced
                failures.append(
                    _TerminalFailure(
                        key=key,
                        workload=workload.name,
                        post_id=post_id,
                        error=f"{type(exc).__name__}: {exc}",
                        retryable=True,
                    )
                )
                continue
            # The submit stage owns the key (it owns the partition): the
            # builder declares the payload, not the addressing.
            built.append(
                (
                    workload,
                    Item(
                        custom_key=key,
                        prompt=item.prompt,
                        images=item.images,
                        post_id=item.post_id,
                        platform=item.platform,
                    ),
                )
            )
    return built, failures


def _fail_terminal(
    instance,
    ops: SQLiteResource,
    failure: _TerminalFailure,
    *,
    root: str | None = None,
) -> None:
    """Make a discovered post terminal WITHOUT provider work.

    Lands an ``ok=False`` bronze row (a failure is a landed row, never a
    dropped one), reports the harvested partition so the in-flight set
    shrinks, and mints a retry round when the cause is retryable.
    """
    landing.land_response(
        post_id=failure.post_id,
        platform="instagram",
        workload=failure.workload,
        provider="none",
        model="",
        prompt_hash="",
        schema_version="",
        run_id="submit-build-failure",
        response_text="",
        ok=False,
        error_message=failure.error,
        root=root,
    )
    harvest.report_harvested(instance, [failure.key])
    if failure.retryable:
        harvest.mint_retries(instance, {failure.key: failure.error})
    else:
        logger.error(
            "Post %s terminally unbuildable (no retry): %s — the "
            "landed∖conformed anti-join check owns its visibility.",
            failure.post_id,
            failure.error[:200],
        )


def submit_pending(
    instance,
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    adapter,  # seam.ProviderAdapter
    *,
    config: SubmitConfig | None = None,
    job_spec: JobSpec = DEFAULT_JOBSPEC,
    root: str | None = None,
) -> dict:
    """One submit pass: discover → guard → placeholder → build → submit.

    One provider job per workload (a workload owns its job-level options).
    Raises ``RuntimeError`` when the provider fails its readiness gate — never
    a quiet "nothing to do" (US-EENG-2). Nothing is written to the instance on
    a dry run, so a dry run does not change what the next real run sees.
    """
    cfg = config or SubmitConfig(limit=DEFAULT_SUBMIT_LIMIT)
    selected = workloads_for(cfg)

    if not adapter.health():
        raise RuntimeError(
            f"enrichment provider '{adapter.name}' failed its readiness gate "
            "— failing loudly, never a quiet 'nothing to do' (US-EENG-2)"
        )

    # ── Discover ────────────────────────────────────────────────────────
    discovered: list[tuple[Workload, dict, str, int]] = []
    with duckdb.get_connection() as conn:
        if len(selected) == 1 and selected[0].name == _workloads.WORKLOADS[0].name:
            _ensure_classification_table(conn)
        for workload in selected:
            for candidate in workload.candidates(conn, cfg):
                discovered.append((workload, candidate, "", 0))

    if not discovered:
        return {
            "submitted": 0,
            "failed": 0,
            "discovered": 0,
            "candidates": 0,
            "in_flight": 0,
            "handle": None,
            "dry_run": cfg.dry_run,
        }

    # ── ONE in-flight read; the guard and the enqueue share it ──────────
    in_flight = _in_flight_by_post(instance)
    candidates_seen = len(discovered)

    accepted: list[tuple[Workload, dict, str, int]] = []
    suppressed: list[str] = []
    for workload, candidate, _, _ in discovered:
        post_id = candidate["post_id"]
        if post_id in in_flight and not cfg.post_ids:
            # Suppressed ONLY on the discovery path: explicit post_ids
            # re-enrichment bypasses every guard.
            suppressed.append(post_id)
            continue
        state = post_partition_state(instance, workload.name, post_id)
        round_no = state.next_round
        key = partition_key(workload.name, round_no, [post_id])
        _guard_round(instance, workload.name, post_id, key)
        accepted.append((workload, candidate, key, round_no))

    if suppressed:
        logger.warning(
            "Submit: %d post(s) already IN FLIGHT — not re-submitted (%s)",
            len(suppressed),
            sorted(suppressed)[:20],
        )

    accepted = accepted[: cfg.limit]

    if cfg.dry_run:
        # Project cost with the SAME arithmetic a real run uses, and touch
        # nothing. Nothing is materialized, so nothing becomes "in flight".
        built, failures = build_items(
            ops, duckdb, [(w, c, "") for w, c, _, _ in accepted]
        )
        tokens, cost = _project(built)
        return {
            "submitted": 0,
            "failed": len(failures),
            "discovered": candidates_seen,
            "candidates": len(accepted),
            "in_flight": len(suppressed),
            "estimate_tokens": tokens,
            "estimate_usd": round(cost, 4),
            "handle": None,
            "dry_run": True,
        }

    # ── Build FIRST, so the placeholder covers only real work ───────────
    # Ordering note: the placeholder must exist BEFORE the POST (a crash
    # between them leaves the post visibly in flight rather than invisibly
    # lost), but it must be written AFTER building — a candidate that cannot
    # be built never reaches the provider, so materializing its key would
    # strand a partition that harvest waits on forever.
    built, failures = build_items(
        ops, duckdb, [(w, c, k) for w, c, k, _ in accepted]
    )

    keys = [item.custom_key for _, item in built]
    if keys:
        instance.add_dynamic_partitions(SUBMITTED_ASSET_NAME, keys)
        for key in keys:
            instance.report_runless_asset_event(
                AssetMaterialization(asset_key=_SUBMITTED_KEY, partition=key)
            )

    # Unbuildable candidates are terminal-loud: an ok=False bronze landing, a
    # harvested partition, and a retry round when the cause may clear.
    for failure in failures:
        _fail_terminal(instance, ops, failure, root=root)

    # ONE submit per WORKLOAD, not per run: a workload carries its own
    # job-level options (the facet passes need max_tokens/mode; the
    # classification pass needs neither), and job options belong to the whole
    # provider job. The provider job is the dedup boundary, so several jobs
    # in one run is still exactly-once per partition.
    items_by_workload: dict[str, list[Item]] = {}
    spec_by_workload: dict[str, JobSpec] = {}
    for workload, item in built:
        items_by_workload.setdefault(workload.name, []).append(item)
        spec_by_workload[workload.name] = workload.job_spec

    handles: dict[str, str] = {}
    for workload_name, batch in items_by_workload.items():
        spec = spec_by_workload[workload_name] or job_spec
        handle = adapter.submit(batch, job_spec=spec)
        handles[workload_name] = handle
        for item in batch:
            instance.report_runless_asset_event(
                AssetMaterialization(
                    asset_key=_SUBMITTED_KEY,
                    partition=item.custom_key,
                    metadata={
                        "handle": handle,
                        "provider": adapter.name,
                        "workload": workload_name,
                    },
                )
            )
        logger.info(
            "Submitted %d item(s) for workload %s under provider handle %s",
            len(batch), workload_name, handle,
        )

    return {
        "submitted": len(built),
        "failed": len(failures),
        "discovered": candidates_seen,
        "candidates": len(accepted),
        "in_flight": len(suppressed),
        "handle": next(iter(handles.values()), None),
        "handles": handles,
        "dry_run": False,
    }


def _project(built: list[tuple[Workload, Item]]) -> tuple[int, float]:
    """Cost projection, grouped by the workload that produced each item.

    Takes the built pairs directly: the workload travels with its item, so
    this cannot misattribute a batch the way a positional zip against the
    candidate list could.
    """
    by_name: dict[str, list[Item]] = {}
    for workload, item in built:
        by_name.setdefault(workload.name, []).append(item)
    tokens = 0
    cost = 0.0
    for workload in _workloads.WORKLOADS:
        batch = by_name.get(workload.name)
        if not batch:
            continue
        t, c = workload.estimate(batch)
        tokens += t
        cost += c
    return tokens, cost


def _ensure_classification_table(conn) -> None:
    """Create the classification table if the state DB has never run conform."""
    from orchestration.defs.ig_enriched.slv.classification import CLASSIFICATION_DDL

    conn.execute(CLASSIFICATION_DDL)


# ── Dagster op + job + asset ────────────────────────────────────────────────


@op(tags={"adr": "0016", "seam": "enrichment-api"}, out=Out(Nothing))
def submit_enrichment_op(
    context, config: SubmitConfig, ops: SQLiteResource, duckdb: DuckDBResource
) -> None:
    """One bounded submit pass: discovery, guard, placeholder, submit.

    ``out=Nothing`` because this op reports, it does not produce: orchestration
    state lives on the Dagster instance (materialized partitions) and payloads
    live in the lake, so there is nothing for an I/O manager to persist. Without
    it Dagster routes the returned report dict through ``PolarsIOManager``,
    which tries to resolve an asset key for an op that has none and fails the
    run at execution time — invisible to ``definitions validate``.
    """
    from orchestration.defs.engine import service_backed
    from orchestration.defs.engine.provider import build_adapter

    adapter = build_adapter(service_backed.PROVIDER_NAME)
    result = submit_pending(
        context.instance, ops, duckdb, adapter, config=config
    )
    context.log.info(
        "Submit: %d submitted, %d failed, %d candidate(s) of %d discovered "
        "(%d in flight)%s",
        result["submitted"],
        result["failed"],
        result["candidates"],
        result["discovered"],
        result["in_flight"],
        f" — DRY RUN, est {result.get('estimate_tokens', 0)} tok / "
        f"${result.get('estimate_usd', 0)}"
        if result["dry_run"]
        else f" — handles {result.get('handles', {}) or 'none'}",
    )


@job(name="enrichment_submit")
def enrichment_submit_job():
    """Discover eligible posts → guard → placeholder → submit through the seam."""
    submit_enrichment_op()
