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
exactly one post id, and the key is a READABLE, self-describing composite
(ADR-0014 decision 1) — not a one-way digest::

    partition_key(workload, attempt_round, [post_id])
      = "<workload>\x00r<attempt_round>\x00<post_id>"
      # e.g. "content-classification\x00r0\x00Cabc123"

Callers MUST pass exactly one post id — the grammar has exactly one post-id
slot. The round is READ BACK OUT of a materialized key with
:func:`parse_partition_key`, so the retry driver (ADR-0014 D3) can derive
"what round is this key at?" without any stored state — the key IS the
record (ADR-0013: no ledger).

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
   * *Stable across a re-run of the same work*: same inputs -> same key,
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

Keys are composites, not digests: any consumer can split a materialized key
back into (workload, round, post_id) with :func:`parse_partition_key` — no
stored mapping exists or is needed (ADR-0013, ADR-0014 D1). The workload
constant lives with the consumer that owns it; this module does not
duplicate it.

"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from dagster import (
    AssetKey,
    DynamicPartitionsDefinition,
)
SUBMITTED_ASSET_NAME = "enrichment_submitted"
HARVESTED_ASSET_NAME = "enrichment_harvested"

MAX_ROUNDS: int = 5
"""Retry budget (ADR-0014 D3): "give up" is the derived condition
``round >= MAX_ROUNDS`` read straight out of the highest-round key for a
post. Replaces the retired queue's ``MAX_ATTEMPTS = 5``; no attempt column,
no backoff timer — rounds advance only when a real harvest cycle completes."""


_ROUND_PREFIX = "r"
_KEY_DELIMITER = "\x00"


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
    """Derive the deterministic PER-POST partition key (ADR-0014 D1).

    Callers MUST pass exactly one post id (``[pid]``): the tracking
    contract is per post, forced by ADR-0013's no-ledger decision — the
    drain can only test an individual candidate's in-flight state if that
    candidate's key is derivable from (workload, round, its own id) alone.
    The iterable signature is kept so this stays one pure function.

    Grammar: ``<workload>\\x00r<attempt_round>\\x00<post_id>`` — readable,
    delimiter-disambiguated, and reversible via :func:`parse_partition_key`.
    Round 0 keys keep the same shape (``r0``); there is no special case.

    Raises:
        ValueError: on an empty workload, a negative round, anything other
            than exactly one non-empty post id.
    """
    if not workload:
        raise ValueError("workload must be a non-empty string")
    if attempt_round < 0:
        raise ValueError(f"attempt_round must be >= 0, got {attempt_round}")
    ids = list(post_ids)
    if len(ids) != 1:
        raise ValueError(
            "partition_key is PER-POST: pass exactly one post id, "
            f"got {len(ids)}"
        )
    pid = ids[0]
    if not pid:
        raise ValueError("post id must be a non-empty string")
    return _KEY_DELIMITER.join([workload, f"{_ROUND_PREFIX}{attempt_round}", pid])


@dataclass(frozen=True)
class ParsedKey:
    """The dimensions read back OUT of a materialized partition key."""

    workload: str
    attempt_round: int
    post_id: str


def parse_partition_key(key: str) -> ParsedKey:
    """Split a composite partition key back into its dimensions (ADR-0014 D1).

    The inverse of :func:`partition_key`. This is what makes the retry
    driver possible with no ledger: the harvest run reads the round straight
    out of a terminal key and mints round N+1.

    Raises:
        ValueError: if ``key`` does not match the D1 grammar (wrong number
            of delimiter-separated fields, a non-``r<N>`` round token, or a
            non-integer round).
    """
    parts = key.split(_KEY_DELIMITER)
    if len(parts) != 3:
        raise ValueError(
            "partition key must be '<workload>\\x00r<N>\\x00<post_id>'; "
            f"got {key!r}"
        )
    workload, round_token, post_id = parts
    if not round_token.startswith(_ROUND_PREFIX):
        raise ValueError(f"round token must start with 'r'; got {round_token!r}")
    try:
        attempt_round = int(round_token[len(_ROUND_PREFIX):])
    except ValueError:
        raise ValueError(f"round token must be r<N>; got {round_token!r}") from None
    if attempt_round < 0:
        raise ValueError(f"round must be >= 0; got {attempt_round}")
    return ParsedKey(workload=workload, attempt_round=attempt_round, post_id=post_id)


def parse_partition_key_or_round0(key: str) -> ParsedKey | None:
    """Parse a key, grandfathering pre-ADR-0014 opaque digests as round 0.

    Keys materialized before the D1 grammar are 16-hex digests with no
    readable round. ADR-0014 D1 migration rule: treat an unparseable key as
    round 0 for SUPPRESSION ONLY. Returns ``None`` for keys that are not
    even plausible digests (empty), which callers skip.
    """
    try:
        return parse_partition_key(key)
    except ValueError:
        return ParsedKey(workload=key, attempt_round=0, post_id="") if key else None

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
    guard: a key present here means the drain MUST NOT resubmit that post.
    Keys are composites: map an in-flight key back to its post with
    :func:`parse_partition_key` (no stored mapping exists — ADR-0013).

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
class PostPartitionState:
    """A candidate post's orchestration state, derived from the instance.

    ``suppressed`` — some materialized key for this post is in flight
    (submitted, not yet harvested): the drain MUST NOT re-enqueue.
    ``next_round`` — the round the drain should enqueue at if not
    suppressed: one past the highest MATERIALIZED harvested round for the
    post. Pre-ADR-0014 digest keys cannot prove they belong to a post, so
    they neither suppress nor advance the round — they simply age out.
    """

    suppressed: bool
    next_round: int


def post_partition_state(
    instance: PartitionSnapshot, workload: str, post_id: str
) -> PostPartitionState:
    """Derive one post's suppression + next-round state from the instance.

    This is the drain guard's whole derivation (ADR-0014 D1: both sides
    derive the dimensions from the key itself). Suppression is a
    whole-post test over ALL materialized keys for the post — not just
    round 0 — so a retry round in flight suppresses exactly as a first
    attempt does, and a harvested round never suppresses.
    """
    _require_snapshot(instance)
    submitted = instance.get_materialized_partitions(AssetKey(SUBMITTED_ASSET_NAME))
    harvested = instance.get_materialized_partitions(AssetKey(HARVESTED_ASSET_NAME))
    own_submitted = _keys_for_post(submitted, workload, post_id)
    own_harvested = _keys_for_post(harvested, workload, post_id)
    in_flight = own_submitted - own_harvested
    rounds = [parsed.attempt_round for parsed in own_harvested]
    return PostPartitionState(
        suppressed=bool(in_flight),
        next_round=(max(rounds) + 1) if rounds else 0,
    )


def _keys_for_post(
    keys: set[str], workload: str, post_id: str
) -> set[ParsedKey]:
    """Materialized keys that belong to (workload, post_id), parsed."""
    out: set[ParsedKey] = set()
    for key in keys:
        try:
            parsed = parse_partition_key(key)
        except ValueError:
            continue
        if parsed.workload == workload and parsed.post_id == post_id:
            out.add(parsed)
    return out


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
