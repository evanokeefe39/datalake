"""Apify actor runs — trigger, wait, stream, over the official ``apify-client``.

The three functions the pipeline uses are kept; a hand-rolled HTTP client is not.
This module is the transport seam between the pipeline and Apify: the assets call
``trigger_run`` / ``poll_run`` / ``stream_dataset`` and never touch the SDK
directly, so the SDK's client construction is confined to ``_client`` (the one
place tests intercept).

Why the SDK, rather than the previous hand-rolled client:

* the outgoing payload stops being opaque — ``run_input`` is a plain dict whose
  keys are the actor's documented input properties, so adding an input is adding
  a key rather than discovering a query parameter;
* retry policy is the SDK's (exponential backoff, 429/5xx), rather than four
  local helpers re-implementing it;
* ``memory_mbytes`` and ``max_total_charge_usd`` are first-class run options.

The previous client's ``format=json`` workaround is retired, deliberately. It
existed to dodge Apify's NDJSON newline bug because that client parsed a raw HTTP
text body; ``iterate_items()`` yields already-parsed dicts, so no newline
delimiting is involved at any point and the bug cannot reach this path.

All functions take an explicit ``token`` parameter (never a global).
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from apify_client import ApifyClient

log = logging.getLogger(__name__)

#: Terminal statuses that mean the run did not produce usable output.
_FAILED_STATUSES = frozenset({"FAILED", "ABORTED", "TIMED-OUT"})


@dataclass
class RunInfo:
    """Result of triggering an Apify actor run."""

    run_id: str
    dataset_id: str | None = None
    actor: str = ""
    estimated_cost_usd: float = 0.0


@dataclass
class RunOutcome:
    """Terminal state of a finished actor run.

    ``usage_total_usd`` is what the run actually cost — the SDK only exposes it
    once the run is terminal, which is why ``poll_run`` returns it and
    ``trigger_run`` can only report the start-time estimate in ``RunInfo``.
    """

    dataset_id: str
    usage_total_usd: float = 0.0


def _client(token: str) -> ApifyClient:
    """Construct the SDK client. The ONE interception point for tests."""
    return ApifyClient(token)


def trigger_run(
    actor: str,
    urls: list[str],
    *,
    token: str,
    results_limit: int = 1,
    results_type: str = "posts",
    max_charge_usd: float | None = None,
) -> RunInfo:
    """Start an actor run. Returns immediately with run_id and dataset_id.

    The dataset_id is available before the run finishes — Apify creates it up
    front. Use ``poll_run()`` to wait for completion.

    ``max_charge_usd`` maps to the run option ``maxTotalChargeUsd``, which caps
    what the run may bill. ``Decimal(str(...))``, not ``Decimal(...)``: the float
    constructor reproduces binary error and would send a cap like
    ``0.5000000000000000277...``.
    """
    run_input = {
        "directUrls": urls,
        "resultsType": results_type,
        "resultsLimit": results_limit,
        "proxy": {"useApifyProxy": True},
    }
    run = _client(token).actor(actor).start(
        run_input=run_input,
        max_total_charge_usd=(
            Decimal(str(max_charge_usd)) if max_charge_usd is not None else None
        ),
    )
    log.info("Triggered run %s (dataset %s)", run.id, run.default_dataset_id)
    return RunInfo(
        run_id=run.id,
        dataset_id=run.default_dataset_id,
        actor=actor,
        estimated_cost_usd=run.usage_total_usd or 0.0,
    )


#: How long to keep re-reading a finished run for its billable cost.
#: Apify finalizes ``usageTotalUsd`` a few seconds AFTER the status turns
#: terminal — measured 2026-09-17: it read 0.0 at finish and 0.0023 by +4s.
#: Without this, the .meta sidecar records $0.00 for every run.
_COST_SETTLE_SECS = 30
_COST_SETTLE_POLL_SECS = 2


def _settled_cost(client, run_id: str, finished) -> float:
    """The run's billable cost, re-read until Apify publishes it.

    Returns whatever is available when the window closes: a run that genuinely
    cost nothing (a total charge cap that stopped it immediately, say) is
    indistinguishable from one that has not settled, and blocking forever on
    that ambiguity would be worse than recording zero.
    """
    cost = finished.usage_total_usd or 0.0
    deadline = time.monotonic() + _COST_SETTLE_SECS
    while cost == 0.0 and time.monotonic() < deadline:
        time.sleep(_COST_SETTLE_POLL_SECS)
        refreshed = client.run(run_id).get()
        cost = (refreshed.usage_total_usd or 0.0) if refreshed is not None else 0.0
    if cost == 0.0:
        log.warning(
            "Run %s reported no billable usage after %ds — recording 0.0",
            run_id,
            _COST_SETTLE_SECS,
        )
    return cost


def poll_run(
    run_id: str, *, token: str, poll_secs: int = 5, timeout: int = 600
) -> RunOutcome:
    """Wait for a run to finish. Returns its dataset id and actual cost.

    Raises RuntimeError on failure or timeout.

    ``poll_secs`` is retained for callers that still pass it; the SDK's
    ``wait_for_finish`` does the waiting, so it is unused here.
    """
    client = _client(token)
    finished = client.run(run_id).wait_for_finish(
        wait_duration=timedelta(seconds=timeout)
    )
    if finished is None:
        raise RuntimeError(f"Run {run_id} not found")
    if finished.status == "SUCCEEDED":
        log.info("Run %s succeeded, dataset %s", run_id, finished.default_dataset_id)
        return RunOutcome(
            dataset_id=finished.default_dataset_id,
            usage_total_usd=_settled_cost(client, run_id, finished),
        )
    if finished.status in _FAILED_STATUSES:
        raise RuntimeError(
            f"Run {run_id} {finished.status}: {finished.status_message or ''}"
        )
    raise RuntimeError(
        f"Run {run_id} did not finish within {timeout}s (status {finished.status})"
    )


def stream_dataset(dataset_id: str, dest: Path, *, token: str) -> int:
    """Download dataset to ``dest`` as NDJSON. Returns the item count.

    Writes one JSON object per line so ``pl.read_ndjson`` can read it back.
    ``newline=""``: the default translation would rewrite the line endings this
    format is defined by.
    """
    count = 0
    with open(dest, "w", encoding="utf-8", newline="") as f:
        for item in _client(token).dataset(dataset_id).iterate_items():
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            count += 1
    log.info("Streamed %d items to %s", count, dest)
    return count
