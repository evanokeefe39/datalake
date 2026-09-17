"""Dagster-native enrichment submit — seam-mediated, partition-discovered
(ADR-0012/0013/0014).

The submit stage consumes the work the drain enqueued: the Dagster
instance's materialized ``enrichment_submitted`` partitions that have no
``enrichment_harvested`` counterpart yet (the in-flight set,
``partitions.in_flight_partitions``). There is NO queue read anywhere —
``batch_jobs``/``batch_items`` are retired (ADR-0012) and their helpers are
deleted from this module.

Flow per bounded run:

1. ``discover_pending`` — parse composite partition keys
   (``<workload>\\x00r<N>\\x00<post_id>``, ADR-0014 D1) out of the in-flight
   set, filter to this stage's workload. A key already at
   ``partitions.MAX_ROUNDS`` while still in flight is a stuck state and
   fails loudly.
2. ``build_items`` — resolve each post from silver (caption + cached media
   bytes) into a seam ``Item`` whose ``custom_key`` IS the partition key,
   so harvest maps results back with no stored mapping. A post that cannot
   be built becomes a TERMINAL FAILURE (an ``ok=False`` bronze row +
   harvested partition + retry mint) — never a silent skip that strands the
   partition in flight forever.
3. ONE ``adapter.submit(items, job_spec=...)`` call per run (bounded);
   the returned handle is recorded as metadata on every covered
   ``enrichment_submitted`` partition — Dagster-native state, no ledger
   (ADR-0013). Harvest reads it back via :mod:`harvest.discover_handles`.

Every provider call goes through the seam adapter. Gemini's direct
``gemini_batch.submit`` call site and the ``submit_gemini_batches_job``
entry point are DELETED (W-FREEZE / ADR-0014 remediation).
"""

import json
import logging
from dataclasses import dataclass

from dagster import AssetKey, AssetMaterialization, job, op

from orchestration.defs.engine import harvest, landing, partitions
from orchestration.defs.engine.landing import WORKLOAD_CONTENT_CLASSIFICATION
from orchestration.defs.engine.media import cached_local_path
from orchestration.defs.engine.partitions import (
    MAX_ROUNDS,
    SUBMITTED_ASSET_NAME,
    in_flight_partitions,
    parse_partition_key,
)
from orchestration.defs.engine.provider import DEFAULT_JOBSPEC, Item, JobSpec
from orchestration.defs.ig_enriched.slv.prompts import IG_GOLD_PROMPT
from orchestration.defs.platform.resources import DuckDBResource, SQLiteResource

logger = logging.getLogger("enrichment.submit")

#: Bounded runs: at most this many partitions (posts) submit per run.
DEFAULT_SUBMIT_LIMIT = 250

_SUBMITTED_KEY = AssetKey(SUBMITTED_ASSET_NAME)

#: Join key is PLATFORM, never ``domain`` (the ADR-0011 duplicate-name
#: rule). The classification workload reads Instagram silver posts.
_PLATFORM_BY_WORKLOAD: dict[str, str] = {
    WORKLOAD_CONTENT_CLASSIFICATION: "instagram",
}

_SILVER_TABLES: dict[str, str] = {"instagram": "silver_ig_posts"}


@dataclass(frozen=True)
class PendingPartition:
    """One unit of discovered work, parsed out of the in-flight set."""

    key: str
    workload: str
    round: int
    post_id: str


@dataclass(frozen=True)
class TerminalFailure:
    """A post that can never reach the provider this cycle.

    ``retryable`` — True when the cause may clear on a later cycle (e.g. a
    media cache miss); False for deterministic skips (empty caption, missing
    silver row), which mint no retry and surface via the anti-join check.
    """

    key: str
    error: str
    retryable: bool


# ── Discovery ───────────────────────────────────────────────────────────────


def discover_pending(
    instance: partitions.PartitionSnapshot,
    *,
    workload: str = WORKLOAD_CONTENT_CLASSIFICATION,
) -> list[PendingPartition]:
    """Discover work from the instance's materialized ``enrichment_submitted``
    partitions (the in-flight set) — NEVER from a queue table.

    A malformed partition key raises: keys are the orchestration record, and
    an unreadable one means the key grammar was violated somewhere upstream.
    A key at ``round >= MAX_ROUNDS`` still in flight is stuck (the retry
    driver never mints past the budget) and raises rather than stalling
    silently.
    """
    pending: list[PendingPartition] = []
    for key in sorted(in_flight_partitions(instance)):
        try:
            parsed = parse_partition_key(key)
        except ValueError as exc:
            raise RuntimeError(
                f"unparseable in-flight partition key {key!r}: {exc}"
            ) from exc
        if parsed.workload != workload:
            continue
        if parsed.attempt_round >= MAX_ROUNDS:
            raise RuntimeError(
                f"partition {key!r} is in flight at round {parsed.attempt_round} "
                f">= MAX_ROUNDS={MAX_ROUNDS} — the retry budget is exhausted; "
                "refusing to submit again"
            )
        pending.append(
            PendingPartition(
                key=key,
                workload=parsed.workload,
                round=parsed.attempt_round,
                post_id=parsed.post_id,
            )
        )
    return pending


# ── Item building ───────────────────────────────────────────────────────────


def _resolve_media_paths(ops: SQLiteResource, media_files_json: str | None) -> tuple[str, ...]:
    """Resolve a post's media URLs to CACHED local byte paths.

    Cache-only by design: the scrape-time byte cache is the reliable copy
    (CDN URLs die in ~4-5 days), and no provider transport is named here.
    A cache miss raises — the caller turns it into a terminal failure for
    that post alone; submitting partial media would silently change the
    analysis input.
    """
    if not media_files_json:
        return ()
    urls = [u for u in json.loads(media_files_json) if u]
    paths: list[str] = []
    for url in urls:
        path = cached_local_path(ops, url)
        if not path:
            raise FileNotFoundError(
                f"media cache miss for {url[:120]} — not submitting partial media"
            )
        paths.append(path)
    return tuple(paths)


def build_items(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    pending: list[PendingPartition],
) -> tuple[list[Item], list[TerminalFailure]]:
    """Build seam Items for discovered work; failures stay terminal-loud.

    Returns ``(items, failures)``. Every failure is reported by the caller
    (``ok=False`` bronze row + harvested partition), so the in-flight set
    always shrinks for every discovered partition — the drain's guard never
    suppresses on stranded work.
    """
    items: list[Item] = []
    failures: list[TerminalFailure] = []
    for entry in pending:
        platform = _PLATFORM_BY_WORKLOAD.get(entry.workload)
        table = _SILVER_TABLES.get(platform or "")
        if not table:
            failures.append(
                TerminalFailure(
                    key=entry.key,
                    error=f"no silver table for platform {platform!r}",
                    retryable=False,
                )
            )
            continue
        with duckdb.get_connection() as conn:
            row = conn.execute(
                f"SELECT caption, media_files FROM {table} WHERE post_id = ?",
                [entry.post_id],
            ).fetchone()
        caption = (row[0] if row else "") or ""
        if not caption.strip():
            failures.append(
                TerminalFailure(
                    key=entry.key,
                    error="empty caption — nothing to enrich",
                    retryable=False,
                )
            )
            continue
        try:
            images = _resolve_media_paths(ops, row[1] if row else None)
        except Exception as exc:
            failures.append(
                TerminalFailure(key=entry.key, error=str(exc), retryable=True)
            )
            continue
        items.append(
            Item(
                custom_key=entry.key,
                prompt=f"{IG_GOLD_PROMPT}\n{caption}",
                images=images,
                post_id=entry.post_id,
                platform=platform or "",
            )
        )
    return items, failures


# ── Core (mock-testable) ────────────────────────────────────────────────────


def _fail_terminal(
    instance: partitions.PartitionSnapshot,
    ops: SQLiteResource,
    failure: TerminalFailure,
    *,
    root: str | None = None,
) -> None:
    """Make a discovered partition terminal WITHOUT provider work.

    Lands an ``ok=False`` bronze row (failure is a landed row, never a
    dropped one), reports the harvested partition so the in-flight set
    shrinks, and mints a retry round when the cause is retryable.
    """
    parsed = parse_partition_key(failure.key)
    platform = _PLATFORM_BY_WORKLOAD[parsed.workload]
    landing.land_response(
        post_id=parsed.post_id,
        platform=platform,
        workload=parsed.workload,
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
            "Partition %s terminally unbuildable (no retry): %s — the "
            "landed∖conformed anti-join check owns its visibility.",
            failure.key, failure.error[:200],
        )


def submit_pending(
    instance: partitions.PartitionSnapshot,
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    adapter,  # seam.ProviderAdapter
    *,
    job_spec: JobSpec = DEFAULT_JOBSPEC,
    limit: int = DEFAULT_SUBMIT_LIMIT,
    root: str | None = None,
) -> dict:
    """One submit pass: discover → build → ONE seam submit → record handle.

    Raises ``RuntimeError`` when the provider fails its readiness gate —
    never a quiet "nothing to do" (US-EENG-2). Idempotent per partition: a
    partition already submitted (handle recorded) but not yet harvested is
    still in flight and is discovered again only to be re-submitted by THIS
    stage's bounded run — discovery caps work at ``limit`` and the provider
    job itself is the dedup boundary (the handle overwrites, one job per
    run). The unbuildable-failure path keeps the accounting identity closed.
    """
    if not adapter.health():
        raise RuntimeError(
            f"enrichment provider '{adapter.name}' failed its readiness gate "
            "— failing loudly, never a quiet 'nothing to do' (US-EENG-2)"
        )

    pending = discover_pending(instance)[:limit]
    items, failures = build_items(ops, duckdb, pending)

    for failure in failures:
        _fail_terminal(instance, ops, failure, root=root)

    handle: str | None = None
    if items:
        handle = adapter.submit(items, job_spec=job_spec)
        # Record the handle Dagster-natively: metadata on the submitted
        # partition materializations. No ledger table (ADR-0013).
        instance.add_dynamic_partitions(
            SUBMITTED_ASSET_NAME, [item.custom_key for item in items]
        )
        for item in items:
            instance.report_runless_asset_event(
                AssetMaterialization(
                    asset_key=_SUBMITTED_KEY,
                    partition=item.custom_key,
                    metadata={"handle": handle, "provider": adapter.name},
                )
            )
        logger.info(
            "Submitted %d item(s) under provider handle %s", len(items), handle
        )

    return {
        "submitted": len(items),
        "failed": len(failures),
        "discovered": len(pending),
        "handle": handle,
    }


# ── Dagster op + job ────────────────────────────────────────────────────────


@op(tags={"adr": "0014", "seam": "enrichment-api"})
def submit_enrichment_op(context, ops: SQLiteResource, duckdb: DuckDBResource) -> dict:
    """One bounded submit pass over the drain's enqueued partitions."""
    from orchestration.defs.engine import service_backed
    from orchestration.defs.engine.provider import build_adapter

    adapter = build_adapter(service_backed.PROVIDER_NAME)
    result = submit_pending(context.instance, ops, duckdb, adapter)
    context.log.info(
        "Submit pass: %d submitted, %d terminal failures, %d discovered "
        "(handle=%s)",
        result["submitted"], result["failed"], result["discovered"],
        result["handle"],
    )
    return result


@job(name="enrichment_submit")
def enrichment_submit_job():
    """Discover in-flight partitions → build → submit through the seam."""
    submit_enrichment_op()
