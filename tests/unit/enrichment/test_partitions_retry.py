"""ADR-0014 D1/D3: the composite partition key and the retry-budget math.

The key is a readable composite ``<workload>\\x00r<N>\\x00<post_id>`` —
derivable in BOTH directions, no one-way digest (the digest that fused the
round away is the defect this grammar replaces). ``MAX_ROUNDS`` replaces the
retired queue's ``MAX_ATTEMPTS``.
"""

import pytest
from orchestration.defs.engine.partitions import (
    HARVESTED_ASSET_NAME,
    MAX_ROUNDS,
    SUBMITTED_ASSET_NAME,
    ParsedKey,
    parse_partition_key,
    post_partition_state,
)

WORKLOAD = "content-classification"


class FakeInstance:
    """Minimal PartitionSnapshot double over two materialization spaces."""

    def __init__(self) -> None:
        self._materialized: dict[str, set[str]] = {
            SUBMITTED_ASSET_NAME: set(),
            HARVESTED_ASSET_NAME: set(),
        }

    def materialize(self, space: str, keys: set[str]) -> None:
        self._materialized[space].update(keys)

    def get_materialized_partitions(self, asset_key) -> set[str]:
        return set(self._materialized[asset_key.to_user_string()])


# ── D1: the key grammar ─────────────────────────────────────────────────────


def test_partition_key_is_the_read_composite():
    key = f"{WORKLOAD}\x00r0\x00Cabc123"
    from orchestration.defs.engine.partitions import partition_key

    assert partition_key(WORKLOAD, 0, ["Cabc123"]) == key


def test_round_zero_uses_the_same_grammar():
    from orchestration.defs.engine.partitions import partition_key

    assert "\x00r0\x00" in partition_key(WORKLOAD, 0, ["P1"])
    assert "\x00r3\x00" in partition_key(WORKLOAD, 3, ["P1"])


def test_parse_roundtrips_the_round():
    from orchestration.defs.engine.partitions import partition_key

    for round_n in (0, 1, 4):
        key = partition_key(WORKLOAD, round_n, ["P1"])
        parsed = parse_partition_key(key)
        assert parsed == ParsedKey(
            workload=WORKLOAD, attempt_round=round_n, post_id="P1"
        )


@pytest.mark.parametrize(
    "bad",
    ["no-delimiters", "a\x00b", "w\x00rN\x00p", "w\x00r-1\x00p", "w\x00r0\x00p\x00extra"],
)
def test_parse_rejects_malformed_keys(bad):
    with pytest.raises(ValueError):
        parse_partition_key(bad)


def test_partition_key_requires_exactly_one_post_id():
    from orchestration.defs.engine.partitions import partition_key

    with pytest.raises(ValueError):
        partition_key(WORKLOAD, 0, [])
    with pytest.raises(ValueError):
        partition_key(WORKLOAD, 0, ["a", "b"])


def test_max_rounds_replaces_max_attempts():
    # The retired queue's MAX_ATTEMPTS=5 is gone from batch.py; the budget
    # is this constant, read from the key itself.
    assert MAX_ROUNDS >= 1
    with pytest.raises(ImportError):
        from orchestration.defs.engine.batch import MAX_ATTEMPTS  # noqa: F401


# ── D3: the state math the drain guard and retry driver share ───────────────


def test_first_attempt_suppresses_and_enqueues_at_round_zero():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.suppressed is True
    assert state.next_round == 0


def test_harvested_round_releases_the_post():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    inst.materialize(HARVESTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.suppressed is False


def test_failed_round_advances_next_round():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    inst.materialize(HARVESTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.suppressed is False
    assert state.next_round == 1


def test_retry_round_in_flight_suppresses_whole_post():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    inst.materialize(HARVESTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r1\x00P1"})
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.suppressed is True


def test_other_workloads_and_posts_do_not_interfere():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {"other\x00r0\x00P1", f"{WORKLOAD}\x00r0\x00OTHER"})
    assert post_partition_state(inst, WORKLOAD, "P1").suppressed is False


# ── The submit-side retry guard ─────────────────────────────────────────────


def _drained_to_ceiling(inst: FakeInstance, post_id: str) -> None:
    """Give a post MAX_ROUNDS harvested rounds and nothing in flight.

    This is the state a post reaches when every attempt so far has been
    harvested and the next round would exceed the budget. ``suppressed`` is
    False here — nothing is in flight — which is exactly why the guard must
    key on the round alone.
    """
    for round_no in range(MAX_ROUNDS):
        key = f"{WORKLOAD}\x00r{round_no}\x00{post_id}"
        inst.materialize(SUBMITTED_ASSET_NAME, {key})
        inst.materialize(HARVESTED_ASSET_NAME, {key})


def test_guard_raises_when_retry_budget_is_exhausted():
    """GIVEN a post whose harvested rounds have reached MAX_ROUNDS
    WHEN submit's retry guard runs
    THEN it raises — an exhausted partition is never re-submitted.

    Regression: gating this on ``suppressed`` as well made the raise
    unreachable (the caller skips in-flight posts first), so a partition
    stuck at the ceiling was silently re-submitted forever.
    """
    from orchestration.defs.engine.partitions import (
        partition_key,
        read_materialized_sets,
    )
    from orchestration.defs.engine.submit import _guard_round

    inst = FakeInstance()
    _drained_to_ceiling(inst, "P1")
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.next_round == MAX_ROUNDS
    assert state.suppressed is False  # nothing in flight — the trap

    with pytest.raises(RuntimeError, match="retry budget is exhausted"):
        _guard_round(
            read_materialized_sets(inst),
            WORKLOAD,
            "P1",
            partition_key(WORKLOAD, MAX_ROUNDS, ["P1"]),
        )


def test_guard_allows_a_post_with_budget_left():
    """GIVEN a post one round short of the ceiling
    WHEN the guard runs
    THEN it does not raise — the budget is not yet spent.
    """
    from orchestration.defs.engine.partitions import (
        partition_key,
        read_materialized_sets,
    )
    from orchestration.defs.engine.submit import _guard_round

    inst = FakeInstance()
    for round_no in range(MAX_ROUNDS - 1):
        key = f"{WORKLOAD}\x00r{round_no}\x00P1"
        inst.materialize(SUBMITTED_ASSET_NAME, {key})
        inst.materialize(HARVESTED_ASSET_NAME, {key})

    _guard_round(
        read_materialized_sets(inst),
        WORKLOAD,
        "P1",
        partition_key(WORKLOAD, MAX_ROUNDS - 1, ["P1"]),
    )


def test_post_partition_state_reads_the_instance_once_per_snapshot():
    """GIVEN N candidates judged against one snapshot
    WHEN each is resolved through the snapshot
    THEN the instance is read exactly TWICE for the whole pass, not twice
    per candidate.

    Regression: ``post_partition_state`` re-read both materialized sets on
    every call, making a submit pass O(candidates x total_partitions) — two
    full instance reads per candidate, contending with the daemon on the
    instance store. Measured 0.56s per call on a 4-key store, which is
    ~76 minutes at the live candidate count and grows with the set. The
    read-once snapshot is also what submit.py's docstring already promised:
    every candidate judged against the same instant.
    """
    from orchestration.defs.engine.partitions import (
        post_partition_state_from_sets,
        read_materialized_sets,
    )

    class CountingInstance(FakeInstance):
        def __init__(self) -> None:
            super().__init__()
            self.reads = 0

        def get_materialized_partitions(self, asset_key) -> set[str]:
            self.reads += 1
            return super().get_materialized_partitions(asset_key)

    inst = CountingInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})

    sets = read_materialized_sets(inst)
    assert inst.reads == 2  # submitted + harvested, once
    for i in range(50):
        post_partition_state_from_sets(sets, WORKLOAD, f"P{i}")
    assert inst.reads == 2  # 50 candidates added ZERO further reads
