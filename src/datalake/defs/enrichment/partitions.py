"""Dagster-native orchestration state for enrichment (ADR-0012/0013).

Orchestration state is DERIVED, never stored. There is no `external_jobs`
table, no queue, no status column (ADR-0013: the seam keeps no ledger). The
two lifecycle stages own one partition space each:

* ``enrichment_submitted`` — materialized BEFORE the billed provider call
  ("placeholder-before-POST"); it is the inspectable record that work was
  handed to the provider.
* ``enrichment_harvested`` — materialized when the responses for that
  partition are landed verbatim in bronze.

The complete in-flight set is the set difference::

    materialized(submitted) - materialized(harvested)

It is re-derived from the Dagster instance on every read, so it survives a
restart with no in-memory state and cannot drift from a stored copy — there
is no stored copy.

THE PARTITION KEY — the load-bearing design decision
----------------------------------------------------

The key is a PER-POST key. Every partition in the two spaces belongs to
exactly one post id::

    partition_key(workload, attempt_round, [post_id])
      = sha256(workload \\x00 attempt_round \\x00 sorted([post_id]))[:16]

Callers MUST pass exactly one post id. The signature accepts an iterable
only so it stays one pure function; ``[pid]`` is the tracking case.

Reasoning:

1. **Per-post is FORCED by ADR-0013's no-ledger decision, not a taste
   choice.** The seam keeps no ledger, so there is no stored mapping from a
   job handle to the post ids it covers. The only way the discovery drain
   can decide whether an INDIVIDUAL post is already in flight is to derive
   that post's partition key from data it already has (workload, round,
   the candidate's own id) and test membership in
   ``in_flight_partitions(instance)``. A per-batch key would require the
   drain to know the batch composition — i.e. to consult the ledger we
   banned. Per-post derivability is the only option. This is why a
   per-batch key documented in earlier revisions of this module silently
   never intersected the drain's per-post keys and would have hidden the
   double-submit the guard exists to prevent.

2. **Per-run (run_id) is wrong.** A run id changes on every Dagster
   re-materialization, so re-running the same work under a new run would
   mint a fresh partition invisible to the subtraction — the exact orphaning
   failure ADR-0012 decision 5 rejects. It is also NOT derivable at harvest
   time from the same inputs.

3. **Granularity split: tracking is per-post, API work is whole-job.** The
   provider API operation remains whole-job: one ``submit`` call covers N
   posts and ``harvest`` is all-or-nothing per job. At harvest, ALL of that
   job's post-partitions flip from ``enrichment_submitted`` to
   ``enrichment_harvested`` together. So the in-flight set temporarily
   shrinks by N on one harvest event — that is correct: tracking
   granularity (per post, so the drain can suppress individual candidates)
   is finer than the API granularity the orchestrator can actually act on.

4. **Determinism properties.**

   * *Derivable identically at submit and harvest time*: both stages know
     the workload, the retry round, and the covered post ids. No hidden
     state is shared.
   * *Stable across a re-run of the same work*: same inputs -> same digest,
     so a crash-and-retry of the same post materializes the SAME submitted
     partition (idempotent) rather than a new one. A post submitted but not
     yet harvested is visible in the in-flight set, and the drain refuses
     to resubmit it.
   * *Retry is a NEW key, deliberately*: bumping ``attempt_round`` mints a
     fresh partition, so a retry round is visible to the subtraction
     (ADR-0012 decision 5; the round number bounds the budget — the
     landing table's row count per post IS the attempt count).
   * *The subtraction means exactly "submitted but not yet harvested"*:
     a key enters the space at submit, leaves it when its harvest
     materializes. Every other state (done, failed, backlog) is derived
     from the lake, not from this space.

The post id list is sorted before hashing so iteration order cannot change
the key. Keys are one-way digests: mapping an in-flight key BACK to a post
id is impossible — the consumer re-derives ``partition_key(workload,
round, [pid])`` per candidate and tests membership (that mapping lives
with the consumer, which owns the workload constant; do not duplicate it
here).

"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from dagster import (
    AssetKey,
    DynamicPartitionsDefinition,
)

SUBMITTED_ASSET_NAME = "enrichment_submitted"
HARVESTED_ASSET_NAME = "enrichment_harvested"

#: Partition space of the submit stage. Materialized before the billed call.
SUBMITTED_PARTITIONS: DynamicPartitionsDefinition = DynamicPartitionsDefinition(
    name=SUBMITTED_ASSET_NAME
)

#: Partition space of the harvest stage. Materialized when responses are
#: landed verbatim in bronze for that partition.
HARVESTED_PARTITIONS: DynamicPartitionsDefinition = DynamicPartitionsDefinition(
    name=HARVESTED_ASSET_NAME
)


def partition_key(workload: str, attempt_round: int, post_ids: Iterable[str]) -> str:
    """Derive the deterministic PER-POST partition key.

    Callers MUST pass exactly one post id (``[pid]``): the tracking
    contract is per post, forced by ADR-0013's no-ledger decision — the
    drain can only test an individual candidate's in-flight state if that
    candidate's key is derivable from (workload, round, its own id) alone.
    The iterable signature is kept so this stays one pure function; do NOT
    pass a whole batch, a per-batch key can never intersect the drain's
    per-post keys.

    Preconditions:
        * ``workload`` is a non-empty string (e.g. a ``landing.WORKLOADS``
          member);
        * ``attempt_round`` >= 0 (0 = first attempt; retry round N uses N);
        * ``post_ids`` is a non-empty iterable of non-empty unique strings.

    Postconditions:
        * Pure and deterministic: the same inputs always return the same
          key, independent of ``post_ids`` order.
        * Distinct inputs (any workload, round, or set-of-ids difference)
          produce distinct keys with overwhelming probability (16 hex chars
          = 64 bits of the sha256 digest).

    Raises:
        ValueError: on an empty workload, a negative round, an empty id set,
            or an empty id within the set.
    """
    if not workload:
        raise ValueError("workload must be a non-empty string")
    if attempt_round < 0:
        raise ValueError(f"attempt_round must be >= 0, got {attempt_round}")
    ids = sorted(post_ids)
    if not ids:
        raise ValueError("post_ids must contain at least one post id")
    if any(not pid for pid in ids):
        raise ValueError("post_ids must not contain empty strings")
    payload = "\x00".join([workload, str(attempt_round), *ids])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# ------------------------------------------------------- the instance surface
# The ONLY instance interface this module depends on. The real
# dagster.DagsterInstance satisfies it (instance.get_materialized_partitions(
# asset_key) -> set[str]); tests inject a fake. Nothing here ever opens
# a live instance.


@runtime_checkable
class PartitionSnapshot(Protocol):
    """The partition-snapshot surface of a Dagster instance.

    The REAL ``DagsterInstance.get_materialized_partitions`` takes an
    ``AssetKey`` (NOT a ``PartitionsDefinition`` — that call raises
    ``AttributeError ... no attribute 'to_string'`` on Dagster 1.13.x) and
    returns the set of partition keys with at least one materialization
    event for that asset in the caller's instance.

    Postcondition: the result contains only keys actually materialized —
    NOT merely dynamically added. ADR-0012/0013's in-flight set is
    ``materialized(submitted) - materialized(harvested)`` over the two
    enrichment asset keys, so this surface must reflect materializations,
    not the dynamic-partition registry.
    """

    def get_materialized_partitions(
        self, asset_key: AssetKey
    ) -> set[str]: ...


def _require_snapshot(instance: PartitionSnapshot) -> None:
    """Explicit error path: reject an object without the snapshot surface.

    Raises:
        TypeError: if ``instance`` does not implement
            ``get_materialized_partitions``.
    """
    if not isinstance(instance, PartitionSnapshot):
        raise TypeError(
            "instance must implement get_materialized_partitions"
            f" (PartitionSnapshot); got {type(instance).__name__}"
        )


# ------------------------------------------------------------ the derivations


def in_flight_partitions(instance: PartitionSnapshot) -> set[str]:
    """Derive the complete in-flight set from the instance.

    ``materialized(submitted) - materialized(harvested)`` — exactly the
    posts handed to the provider (one partition key per post) whose
    responses have not been landed in bronze yet. This is the double-submit
    guard: a key present here means the sensor/drain MUST NOT resubmit that
    post. Keys are one-way digests; to map an in-flight key back to posts,
    re-derive ``partition_key(workload, round, [pid])`` per candidate and
    test membership.

    Preconditions:
        ``instance`` implements :class:`PartitionSnapshot` (the real
        ``DagsterInstance`` does; tests inject a fake).

    Postconditions:
        * A pure function of the instance snapshot — no stored state.
        * Returns a copy; mutating the result never affects the instance.
        * Empty when nothing is submitted, or when everything submitted has
          been harvested.

    Raises:
        TypeError: if ``instance`` lacks the snapshot surface.
    """
    _require_snapshot(instance)
    submitted = instance.get_materialized_partitions(
        AssetKey(SUBMITTED_ASSET_NAME)
    )
    harvested = instance.get_materialized_partitions(
        AssetKey(HARVESTED_ASSET_NAME)
    )
    return submitted - harvested


@dataclass(frozen=True)
class Account:
    """The enrichment accounting identity (ADR-0012/0013 guardrail).

    The identity::

        done + failed + in_flight + backlog == total_candidates

    must hold on every read so a metric that silently stopped detecting is
    itself detectable. ``done``/``failed``/``backlog`` are lake-derived
    (conformed silver rows, failed landings, and candidates not yet done or
    failed respectively); ``in_flight`` is instance-derived. The identity
    therefore CROSS-CHECKS the instance against the lake: losing a
    submitted partition materialization deflates ``in_flight`` and breaks
    the identity — exactly the failure the guardrail exists to catch.
    """

    done: int
    failed: int
    in_flight: int
    backlog: int
    total_candidates: int

    @property
    def holds(self) -> bool:
        """True iff the identity holds."""
        return self.done + self.failed + self.in_flight + self.backlog == (
            self.total_candidates
        )

    @property
    def deficit(self) -> int:
        """``total_candidates - (done + failed + in_flight + backlog)``.

        Zero when the identity holds; non-zero names the discrepancy the
        guardrail fired on.
        """
        return self.total_candidates - (
            self.done + self.failed + self.in_flight + self.backlog
        )


def account(
    instance: PartitionSnapshot,
    *,
    done: int,
    failed: int,
    backlog: int,
    total_candidates: int,
) -> Account:
    """Compute the four accounting numbers, with ``in_flight`` from the instance.

    Preconditions:
        * ``instance`` implements :class:`PartitionSnapshot`;
        * ``done``, ``failed``, ``backlog`` are lake-derived counts (silver
          conformed rows; failed landings; candidates not yet done or
          failed) for the same candidate population as
          ``total_candidates``;
        * all counts >= 0.

    Postconditions:
        * ``in_flight`` equals ``len(in_flight_partitions(instance))`` for
          the same instance snapshot.
        * ``account.holds`` is the caller-assertable identity; it is NOT
          guaranteed true — an instance/lake disagreement (e.g. a lost
          submitted materialization) makes it False, which is the point.

    Raises:
        TypeError: if ``instance`` lacks the snapshot surface.
        ValueError: if any count is negative.
    """
    _require_snapshot(instance)
    counts = {"done": done, "failed": failed, "backlog": backlog}
    for name, value in counts.items():
        if value < 0:
            raise ValueError(f"{name} must be >= 0, got {value}")
    if total_candidates < 0:
        raise ValueError(f"total_candidates must be >= 0, got {total_candidates}")
    in_flight = len(in_flight_partitions(instance))
    return Account(
        done=done,
        failed=failed,
        in_flight=in_flight,
        backlog=backlog,
        total_candidates=total_candidates,
    )


# ------------------------------------------------------- failure surfacing
# Replaces the `dead_letter` table (ADR-0012 decision 7): the failure set is
# an anti-join over tables that must exist anyway, so a stored dead_letter is
# a cache of a derivable query that can silently empty itself.


def failure_set(landed: set[str], conformed: set[str]) -> frozenset[str]:
    """The failure/backlog set: ``landed(bronze) - conformed(silver)``.

    This REPLACES ``dead_letter``. A post is failed-or-stuck when its
    verbatim response was landed in bronze but no conformed silver row
    exists for it — failure is inferred from absence, so the invariant
    "a failed item is NEVER conformed" is load-bearing (ADR-0012
    consequences; needs its own test in Phase 2's conform path).

    Preconditions:
        * ``landed`` contains the candidate keys (``(post_id, platform)``
          rendered as strings) with a landed bronze response;
        * ``conformed`` contains the keys with a conformed silver row for
          the CURRENT contract;
        * both sets are computed for the same candidate population and
          contract version — mixing contracts empties the anti-join
          spuriously.

    Postconditions:
        * Pure; returns a frozenset (safe to hand to an asset check event).
        * Disjoint from ``conformed`` by construction.

    Raises:
        TypeError: if either argument is not a set.
    """
    if not isinstance(landed, set) or not isinstance(conformed, set):
        raise TypeError("landed and conformed must be sets of candidate keys")
    return landed - conformed
