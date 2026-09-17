"""The harvest driver sensor (ADR-0012 decision 2).

An INTERVAL sensor, never an ``@asset_sensor``: an asset sensor fires only on
a NEW materialization event, so work that was not terminal at that instant
would never be re-checked and would hang forever. This sensor re-derives the
FULL in-flight set from the Dagster instance on EVERY tick (the same
``partitions.in_flight_partitions`` derivation the drain uses — one definition
of in-flight, never a second one) and requests an ``enrichment_harvest_job``
run only for partitions whose provider-side state is terminal.

The sensor TRIGGERS only. It never lands responses, never reports the
``enrichment_harvested`` partition, and never mints retry keys — those are
ADR-0014 D2/D3 responsibilities of the harvest run itself, the only actor that
retrieves results. The sensor polls provider handles to *observe* terminal
state (read-only), then hands off to the run that owns the transition.
"""

import hashlib
import json
import logging
import os

from dagster import RunRequest, SensorEvaluationContext, sensor

from orchestration.defs.engine.harvest import (
    discover_handles,
    enrichment_harvest_job,
)
from orchestration.defs.engine.partitions import in_flight_partitions

logger = logging.getLogger("enrichment.sensor")

#: Default tick cadence. Overridable via ``ENRICHMENT_SENSOR_INTERVAL_SECONDS``
#: (the Dagster UI can also edit the interval per sensor at runtime).
DEFAULT_INTERVAL_SECONDS = 30

_sensor_tags = {"adr": "0012", "driver": "harvest"}


@sensor(
    job=enrichment_harvest_job,
    minimum_interval_seconds=int(
        os.environ.get(
            "ENRICHMENT_SENSOR_INTERVAL_SECONDS", str(DEFAULT_INTERVAL_SECONDS)
        )
    ),
    tags=_sensor_tags,
    description="Re-derives the full in-flight set every tick; requests a "
    "harvest run only for terminal partitions (ADR-0012 D2).",
)
def enrichment_harvest_sensor(context: SensorEvaluationContext):
    instance = context.instance

    # The FULL in-flight set, re-derived every tick — the same helper the
    # drain uses. Never a second, partial definition of in-flight.
    in_flight = in_flight_partitions(instance)
    if not in_flight:
        logger.info("enrichment: nothing in flight — no harvest needed")
        return
    # US-EENG-2: the provider is a hard dependency. With work in flight, a
    # down service must fail LOUDLY — never look like a quiet nothing-to-do.
    from orchestration.defs.engine import service_backed
    from orchestration.defs.engine.provider import build_adapter

    adapter = build_adapter(service_backed.PROVIDER_NAME)
    if not adapter.health():
        raise RuntimeError(
            f"enrichment provider '{adapter.name}' failed its readiness gate "
            f"with {len(in_flight)} partition(s) in flight — failing loudly, "
            "never a quiet 'nothing to do' (US-EENG-2)"
        )

    # Read-only observation of the seam: group in-flight partitions by the
    # provider handle the submit run recorded for them. Partitions the drain
    # enqueued but submit has not covered yet carry no handle — the submit
    # stage owns them, so they simply wait.
    handles = discover_handles(instance)
    keys_by_handle: dict[str, list[str]] = {}
    for key in sorted(in_flight):
        handle = handles.get(key)
        if handle:
            keys_by_handle.setdefault(handle, []).append(key)
    if not keys_by_handle:
        logger.info(
            "enrichment: %d in flight, none terminal yet "
            "(no provider handles yet — submit stage owns them)",
            len(in_flight),
        )
        return

    # A poll error RAISES — a swallowed poll error is the silent-stall defect
    # this loop exists to kill (same policy as the harvest run).
    terminal_keys: list[str] = []
    for handle in sorted(keys_by_handle):
        state = adapter.normalize_state(adapter.poll(handle))
        if adapter.is_terminal(state):
            terminal_keys.extend(keys_by_handle[handle])
    if not terminal_keys:
        # Distinct from "nothing in flight": work EXISTS, none of it terminal.
        logger.info(
            "enrichment: %d in flight, none terminal yet — "
            "no harvest run requested",
            len(in_flight),
        )
        return

    # ONE run request per tick, covering every terminal partition — the same
    # batching rule the harvest run applies to its own writes (ADR-0012 D3:
    # DuckDB is single-writer; batching removes contention by construction).
    # run_key deduplicates: the same still-unharvested terminal set observed
    # on consecutive ticks before the previous run lands requests nothing new.
    run_key = "harvest-" + hashlib.sha256(
        "\x00".join(sorted(terminal_keys)).encode()
    ).hexdigest()[:16]
    context.log.info(
        "enrichment: %d of %d in-flight partition(s) terminal — requesting "
        "harvest run (run_key=%s)",
        len(terminal_keys),
        len(in_flight),
        run_key,
    )
    yield RunRequest(
        run_key=run_key,
        tags={
            **_sensor_tags,
            "enrichment/harvest_partitions": json.dumps(sorted(terminal_keys)),
        },
    )
