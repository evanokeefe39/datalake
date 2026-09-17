"""Dagster-native enrichment harvest — poll to terminal, land verbatim,
report the harvested partition, mint retries (ADR-0011/0012/0013/0014).

This module is the TARGET harvest stage of the Dagster-native loop::

    drain (ig_posts_gen_batches) → submit → harvest

Harvest owns two ADR-0014 dynamics:

* **D2 — the harvested driver.** When a provider job reaches terminal state
  and its responses are landed verbatim in bronze, this run materializes an
  ``enrichment_harvested`` partition per covered submitted partition —
  ``instance.report_runless_asset_event(AssetMaterialization(...))``, the
  exact mechanism the drain uses for ``enrichment_submitted``, on the SAME
  injected instance the in-flight guard reads. No ``DagsterInstance.get()``
  fallback exists anywhere: an uninjected instance fails loudly.

* **D3 — the retry driver.** For every terminal-failed item (``ok=False``),
  harvest mints round N+1 by registering the retry partition key on the
  submitted space (``add_dynamic_partitions``). The round-N key stays
  harvested and is NEVER re-materialized. The budget is
  ``partitions.MAX_ROUNDS`` read straight out of the key — no attempt
  column, no backoff timer (``MAX_ATTEMPTS`` is retired with the queue).

The provider handle travels on the ``enrichment_submitted`` partition
materialization the submit run emits (metadata ``handle``) — Dagster-native
state, no ledger table (ADR-0013). Poll loops are BOUNDED: one pass, one
poll per handle per run, capped handles, and a poll/retrieve error RAISES —
never ``logger.warning``-and-continue forever.

Gemini's direct ``gemini_batch`` call sites are DELETED (W-FREEZE / ADR-0014
remediation): every provider call in this module goes through the seam
adapter. The retired ``gemini_batch_harvest`` job and its sensor are gone —
``gemini_batch.py`` survives on disk as inert code only.
"""

import logging

from dagster import (
    AssetKey,
    AssetMaterialization,
    DynamicPartitionsDefinition,
    job,
    op,
)

from orchestration.defs.engine import landing, partitions
from orchestration.defs.engine.landing import WORKLOAD_CONTENT_CLASSIFICATION
from orchestration.defs.engine.partitions import (
    HARVESTED_ASSET_NAME,
    MAX_ROUNDS,
    SUBMITTED_ASSET_NAME,
    in_flight_partitions,
    parse_partition_key,
)
from orchestration.defs.engine.provider import ProviderAdapter
from orchestration.defs.ig_enriched.slv.prompts import (
    CURRENT_PROMPT_HASH,
    IG_GOLD_SCHEMA_VERSION,
)

logger = logging.getLogger("enrichment.harvest")

#: Cheap-tick bound: at most this many provider handles are polled per
#: harvest run. Anything beyond stays for the next run — the in-flight set
#: is not lost, only deferred.
_MAX_HANDLES_PER_RUN = 25

#: Join key is PLATFORM, never ``domain`` (the ADR-0011 duplicate-name rule).
#: One platform per workload; an unmapped workload fails loudly rather than
#: landing rows with a guessed platform.
_PLATFORM_BY_WORKLOAD: dict[str, str] = {
    WORKLOAD_CONTENT_CLASSIFICATION: "instagram",
}

_SUBMITTED_KEY = AssetKey(SUBMITTED_ASSET_NAME)
_HARVESTED_KEY = AssetKey(HARVESTED_ASSET_NAME)

_SUBMITTED_DYN: DynamicPartitionsDefinition = partitions.SUBMITTED_PARTITIONS
_HARVESTED_DYN: DynamicPartitionsDefinition = partitions.HARVESTED_PARTITIONS


# ── The harvested driver (ADR-0014 D2) ──────────────────────────────────────


def report_harvested(
    instance: partitions.PartitionSnapshot, keys: list[str]
) -> None:
    """Materialize ``enrichment_harvested`` per terminal submitted partition.

    Mirrors the drain's ``_materialize_submitted_partitions``: register the
    dynamic partitions, then report runless materializations on the SAME
    injected instance the in-flight guard reads. Re-running with the same
    keys is idempotent (a set membership, not a counter).
    """
    if not keys:
        return
    instance.add_dynamic_partitions(HARVESTED_ASSET_NAME, list(keys))
    for key in keys:
        instance.report_runless_asset_event(
            AssetMaterialization(asset_key=_HARVESTED_KEY, partition=key)
        )
    logger.info("Harvested %d partition(s): %s", len(keys), sorted(keys))


# ── The retry driver (ADR-0014 D3) ──────────────────────────────────────────


def mint_retries(
    instance: partitions.PartitionSnapshot, failed: dict[str, str]
) -> list[str]:
    """Mint round N+1 partition keys for terminal-failed posts.

    The retry key is REGISTERED on the submitted dynamic-partition space so
    the retry round is observable, but it is deliberately NOT materialized
    here: materializing ``enrichment_submitted`` at mint time would put the
    retry into the in-flight set before the drain admits the post, and the
    D6 cycle narrative requires the retry round to be "not yet submitted"
    when the next drain run derives it. The next drain run materializes the
    submitted partition at the minted key.

    The round-N key stays harvested — it is NEVER re-materialized.

    Returns the list of minted retry keys. A post already at
    ``round >= MAX_ROUNDS`` mints NOTHING: its bronze-landed (if any) row
    without a conformed silver counterpart holds the anti-join check red —
    the loud, derived replacement for the retired ``dead_letter`` table.
    """
    minted: list[str] = []
    for key, error in sorted(failed.items()):
        parsed = parse_partition_key(key)
        retry_round = parsed.attempt_round + 1
        if retry_round >= MAX_ROUNDS:
            logger.error(
                "Post %s exhausted retry budget (round %d >= MAX_ROUNDS=%d) "
                "after: %s — no retry minted; the landed∖conformed anti-join "
                "check owns its visibility.",
                parsed.post_id, parsed.attempt_round, MAX_ROUNDS,
                (error or "")[:200],
            )
            continue
        retry_key = partitions.partition_key(
            parsed.workload, retry_round, [parsed.post_id]
        )
        instance.add_dynamic_partitions(SUBMITTED_ASSET_NAME, [retry_key])
        minted.append(retry_key)
        logger.warning(
            "Terminal failure on %s (round %d): %s — minted retry key %s",
            parsed.post_id, parsed.attempt_round, (error or "")[:200], retry_key,
        )
    return minted


# ── Handle discovery (Dagster-native, no ledger) ────────────────────────────


def _latest_submitted_metadata(
    instance: partitions.PartitionSnapshot, key: str
) -> dict:
    """Latest ``enrichment_submitted`` materialization metadata for a key."""
    from dagster._core.event_api import AssetRecordsFilter

    records_filter = AssetRecordsFilter(
        asset_key=_SUBMITTED_KEY, asset_partitions=[key]
    )
    records = instance.fetch_materializations(records_filter, limit=1)
    if not records.records:
        return {}
    materialization = records.records[0].asset_materialization
    return {
        name: value.value
        for name, value in (materialization.metadata or {}).items()
    }


def discover_handles(
    instance: partitions.PartitionSnapshot,
) -> dict[str, str]:
    """Map in-flight partition key → provider handle, from instance state.

    The submit run records the provider job handle as metadata on the
    ``enrichment_submitted`` materialization of every partition it covered
    (a JSON array of provider job names via the adapter layer's one handle
    codec). A partition whose latest submitted materialization carries no
    handle has been enqueued by the drain but not yet submitted — it is
    skipped here (the submit stage owns it).
    """
    handles: dict[str, str] = {}
    for key in sorted(in_flight_partitions(instance)):
        metadata = _latest_submitted_metadata(instance, key)
        handle = metadata.get("handle")
        if handle:
            handles[key] = handle
    return handles


# ── Landing ─────────────────────────────────────────────────────────────────


def land_result(
    result,  # seam.Result
    *,
    run_id: str,
    root: str | None = None,
) -> str:
    """Land ONE seam result verbatim into bronze; return its partition key.

    ``result.custom_key`` IS the partition key (submit sets it so), so the
    post, round, and workload derive from the result itself — no stored
    mapping. ``ok=False`` results land verbatim too: failure is a landed
    bronze row, never a dropped one.
    """
    parsed = parse_partition_key(result.custom_key)
    platform = _PLATFORM_BY_WORKLOAD.get(parsed.workload)
    if platform is None:
        raise RuntimeError(
            f"no platform mapping for workload {parsed.workload!r} "
            f"(partition key {result.custom_key!r}) — refusing to land with "
            "a guessed platform (join key is platform, never domain)"
        )
    landing.land_response(
        post_id=parsed.post_id,
        platform=platform,
        workload=parsed.workload,
        provider=result.provider,
        model=result.model,
        prompt_hash=CURRENT_PROMPT_HASH,
        schema_version=IG_GOLD_SCHEMA_VERSION,
        run_id=run_id,
        response_text=result.response_text or "",
        ok=bool(result.ok),
        error_message=result.error,
        root=root,
    )
    return result.custom_key


# ── Harvest run core (one bounded pass) ─────────────────────────────────────


def harvest_pending(
    instance: partitions.PartitionSnapshot,
    adapter: ProviderAdapter,
    *,
    root: str | None = None,
    max_handles: int = _MAX_HANDLES_PER_RUN,
) -> dict:
    """One harvest pass: poll every known handle to (at most) terminal,
    land responses verbatim, report harvested partitions, mint retries.

    Bounded: at most ``max_handles`` handles polled, ONE poll per handle —
    non-terminal handles simply wait for the next run. A poll or retrieve
    error RAISES (fail loudly); it never warns-and-continues forever.
    """
    handles = discover_handles(instance)
    keys_by_handle: dict[str, list[str]] = {}
    for key, handle in handles.items():
        keys_by_handle.setdefault(handle, []).append(key)
    polled = sorted(keys_by_handle)
    if len(polled) > max_handles:
        logger.warning(
            "Harvest capped at %d of %d handles this run",
            max_handles, len(polled),
        )
        polled = polled[:max_handles]

    total_landed = 0
    total_harvested = 0
    total_minted = 0
    for handle in polled:
        keys = keys_by_handle[handle]
        # Fail loudly on transport errors — a swallowed poll error is the
        # silent-stall defect this loop exists to kill.
        state = adapter.normalize_state(adapter.poll(handle))
        if not adapter.is_terminal(state):
            logger.info("Handle %s still %s (%d partition(s))", handle, state, len(keys))
            continue
        results = adapter.retrieve(handle)
        landed_keys: list[str] = []
        for result in results:
            landed_keys.append(land_result(result, run_id=handle, root=root))
            total_landed += 1
        # Harvest is all-or-nothing per provider job (ADR-0012): every
        # in-flight partition the retrieved results cover flips together.
        terminal_keys = [k for k in landed_keys if k in handles]
        report_harvested(instance, terminal_keys)
        total_harvested += len(terminal_keys)
        failures = {
            r.custom_key: (r.error or "unknown failure")
            for r in results
            if not r.ok and r.custom_key in handles
        }
        total_minted += len(mint_retries(instance, failures))

    return {
        "landed": total_landed,
        "harvested": total_harvested,
        "retried": total_minted,
        "handles_polled": len(polled),
        "handles_pending": len(keys_by_handle) - len(polled),
    }


# ── Dagster op + job ────────────────────────────────────────────────────────


@op(tags={"adr": "0014", "seam": "enrichment-api"})
def harvest_enrichment_op(context) -> dict:
    """One bounded harvest pass over the seam (short, bounded)."""
    from orchestration.defs.engine import service_backed
    from orchestration.defs.engine.provider import build_adapter

    adapter = build_adapter(service_backed.PROVIDER_NAME)
    if not adapter.health():
        raise RuntimeError(
            f"enrichment provider '{adapter.name}' failed its readiness gate "
            "— failing loudly, never a quiet 'nothing to do' (US-EENG-2)"
        )
    result = harvest_pending(context.instance, adapter)
    context.log.info(
        "Harvest pass: %d landed, %d harvested, %d retry key(s) minted "
        "(%d/%d handles polled)",
        result["landed"], result["harvested"], result["retried"],
        result["handles_polled"],
        result["handles_polled"] + result["handles_pending"],
    )
    return result


@job(name="enrichment_harvest")
def enrichment_harvest_job():
    """Poll terminal enrichment jobs → land verbatim → report harvested →
    mint retries."""
    harvest_enrichment_op()
